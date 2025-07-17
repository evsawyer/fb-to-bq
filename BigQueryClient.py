import os
import json
import io
import logging
from typing import List, Dict, Optional, Tuple, Any
from google.oauth2 import service_account
from google.cloud import bigquery
from dotenv import load_dotenv
from SchemaRegistry import SchemaRegistry

load_dotenv()

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Handles all BigQuery operations"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    def __init__(self, credentials_json: str = None, project_id: str = None):
        """Initialize BigQuery client with credentials
        
        Args:
            credentials_json: JSON string of service account credentials
                             (defaults to env var GOOGLE_CREDENTIALS)
            project_id: Google Cloud project ID (auto-detected from credentials)
        """
        self.client = self._initialize_client(credentials_json)
        self.project_id = project_id or self.client.project
        
    def _initialize_client(self, credentials_json: str = None) -> bigquery.Client:
        """Initialize BigQuery client with credentials from file or environment"""
        try:
            # First try to get credentials from parameter or environment variable
            creds_json = credentials_json or os.getenv('GOOGLE_CREDENTIALS')
            
            if creds_json:
                # Production: Use credentials from JSON string
                credentials_info = json.loads(creds_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info, 
                    scopes=self.SCOPES
                )
            else:
                # Local: Try to use credentials file
                credentials_path = "credentials.json"
                if not os.path.exists(credentials_path):
                    raise ValueError("No credentials found in environment variable or local file")
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path, 
                    scopes=self.SCOPES
                )
            
            return bigquery.Client(
                project=credentials.project_id,
                credentials=credentials
            )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse credentials JSON: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise
    
    def insert_records(self, 
                      dataset_id: str, 
                      table_id: str, 
                      records: List[dict], 
                      batch_size: int = 1000) -> Dict[str, int]:
        """Insert or update records in BigQuery table using optimized MERGE approach
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            records: List of records to insert/update
            batch_size: Number of records per batch (used for temp table loading)
            
        Returns:
            Dict with counts of records processed
        """
        if not records:
            logger.warning("No records to insert")
            return {'processed': 0, 'status': 'no_records'}
        
        # Ensure table exists if it's a meta_ads table
        if 'meta_ads' in table_id:
            logger.info(f"Ensuring {dataset_id}.{table_id} exists...")
            if not self.ensure_meta_ads_table_exists(dataset_id, table_id):
                raise Exception(f"Failed to ensure table {dataset_id}.{table_id} exists")
        
        # Use optimized MERGE approach
        logger.info(f"Using optimized MERGE approach for {len(records)} records")
        result = self._insert_records_using_merge(dataset_id, table_id, records, batch_size)
        
        return result
    
    def execute_query(self, query: str) -> bool:
        """Execute an arbitrary BigQuery query
        
        Args:
            query: SQL query to execute
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query_job = self.client.query(query)
            query_job.result()  # Wait for completion
            logger.info("Query executed successfully")
            return True
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return False
    
    def get_existing_records(self, 
                           dataset_id: str, 
                           table_id: str,
                           date_starts: List[str],
                           date_stops: List[str],
                           ad_ids: List[str]) -> List:
        """Get existing records for the given dates and ad_ids"""
        query = f"""
        SELECT date_start, date_stop, ad_id
        FROM `{dataset_id}.{table_id}`
        WHERE date_start IN UNNEST(@date_starts)
          AND date_stop IN UNNEST(@date_stops)
          AND ad_id IN UNNEST(@ad_ids)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("date_starts", "DATE", date_starts),
                bigquery.ArrayQueryParameter("date_stops", "DATE", date_stops),
                bigquery.ArrayQueryParameter("ad_ids", "STRING", ad_ids)
            ]
        )
        
        existing_records = list(self.client.query(query, job_config=job_config).result())
        logger.info(f"Found {len(existing_records)} existing records")
        return existing_records
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> List[str]:
        """Get the schema field names of a table, excluding primary key fields"""
        table = self.client.get_table(f"{dataset_id}.{table_id}")
        return [field.name for field in table.schema 
                if field.name not in ['date_start', 'date_stop', 'ad_id']]
    
    def _separate_records(self, 
                         new_records: List[dict], 
                         existing_records: List) -> Tuple[List[dict], List[dict]]:
        """Separate records into updates and inserts based on existing records"""
        # Create a set of existing keys for quick lookup
        existing_keys = {
            (row.date_start.strftime('%Y-%m-%d'), 
             row.date_stop.strftime('%Y-%m-%d'), 
             row.ad_id) 
            for row in existing_records
        }
        
        updates = []
        inserts = []
        
        for record in new_records:
            key = (record['date_start'], record['date_stop'], record['ad_id'])
            if key in existing_keys:
                updates.append(record)
            else:
                inserts.append(record)
        
        return updates, inserts
    
    def _process_updates(self, 
                        dataset_id: str, 
                        table_id: str, 
                        updates: List[dict], 
                        batch_size: int):
        """Process update records in batches"""
        updatable_fields = self.get_table_schema(dataset_id, table_id)
        update_clause = ",\n        ".join(f"{field} = S.{field}" for field in updatable_fields)
        
        main_table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            logger.info(f"Processing update batch {i // batch_size + 1} with {len(batch)} records")
            
            # Create temp table
            temp_table_id = f"temp_updates_{i}"
            temp_table_ref = f"{self.project_id}.{dataset_id}.{temp_table_id}"
            
            temp_table = bigquery.Table(temp_table_ref)
            temp_table.schema = self.client.get_table(main_table_ref).schema
            self.client.create_table(temp_table, exists_ok=True)
            
            # Load batch into temp table
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            json_data = '\n'.join(json.dumps(record) for record in batch)
            self.client.load_table_from_file(
                file_obj=io.StringIO(json_data),
                destination=temp_table_ref,
                job_config=job_config
            ).result()
            
            # Merge from temp table
            merge_query = f"""
            MERGE `{main_table_ref}` T
            USING `{temp_table_ref}` S
            ON T.date_start = S.date_start 
            AND T.date_stop = S.date_stop 
            AND T.ad_id = S.ad_id
            WHEN MATCHED THEN
                UPDATE SET
                    {update_clause}
            """
            self.client.query(merge_query).result()
            
            # Clean up temp table
            self.client.delete_table(temp_table_ref)
            logger.info(f"Update batch {i // batch_size + 1} completed")
    
    def _process_inserts(self, 
                        dataset_id: str, 
                        table_id: str, 
                        inserts: List[dict], 
                        batch_size: int):
        """Process insert records in batches"""
        for i in range(0, len(inserts), batch_size):
            batch = inserts[i:i + batch_size]
            logger.info(f"Processing insert batch {i // batch_size + 1} with {len(batch)} records")
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            json_data = '\n'.join(json.dumps(record) for record in batch)
            self.client.load_table_from_file(
                file_obj=io.StringIO(json_data),
                destination=f"{self.project_id}.{dataset_id}.{table_id}",
                job_config=job_config
            ).result()
            
            logger.info(f"Insert batch {i // batch_size + 1} completed")
    
    def append_to_table(self, dataset_id: str, table_id: str, rows: List[dict]) -> List:
        """Simple append operation for backward compatibility"""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        
        errors = self.client.insert_rows_json(table, rows)
        
        if errors:
            logger.error(f"Encountered errors while inserting rows: {errors}")
        else:
            logger.info(f"Successfully inserted {len(rows)} rows into {dataset_id}.{table_id}")
        
        return errors
    
    def create_table_if_not_exists(self, dataset_id: str, table_id: str, schema: List[bigquery.SchemaField]) -> bool:
        """Create a table if it doesn't exist
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            schema: List of BigQuery SchemaField objects defining the table schema
            
        Returns:
            True if table was created or already exists, False on error
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        try:
            # Check if table exists
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
            return True
        except Exception:
            # Table doesn't exist, create it
            logger.info(f"Table {table_ref} doesn't exist, creating it...")
            
            try:
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                logger.info(f"Created table {table_ref}")
                return True
            except Exception as e:
                logger.error(f"Failed to create table {table_ref}: {e}")
                return False
    
    def ensure_meta_ads_table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Ensure the meta_ads table exists with proper schema
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            
        Returns:
            True if table exists or was created, False on error
        """
        # Get schema from SchemaRegistry - single source of truth
        schema = SchemaRegistry.to_bigquery_schema('insights')
        
        return self.create_table_if_not_exists(dataset_id, table_id, schema)
    
    def _insert_records_using_merge(self, 
                                   dataset_id: str, 
                                   table_id: str, 
                                   records: List[dict],
                                   batch_size: int = 1000) -> Dict[str, Any]:
        """Optimized insert using temp table and MERGE statement
        
        This avoids pulling existing data into Python memory and lets BigQuery
        handle the update/insert logic internally.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: Target table ID
            records: Records to insert/update
            batch_size: Batch size for loading to temp table
            
        Returns:
            Dict with operation results
        """
        from datetime import datetime
        
        # Generate unique temp table name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        temp_table_id = f"{table_id}_temp_{timestamp}"
        temp_table_ref = f"{self.project_id}.{dataset_id}.{temp_table_id}"
        target_table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        try:
            # Step 1: Create temp table with same schema as target
            logger.info(f"Creating temp table {temp_table_ref}")
            target_table = self.client.get_table(target_table_ref)
            temp_table = bigquery.Table(temp_table_ref, schema=target_table.schema)
            self.client.create_table(temp_table)
            
            # Step 2: Load all records to temp table
            logger.info(f"Loading {len(records)} records to temp table")
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            # Load in batches to avoid memory issues
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                json_data = '\n'.join(json.dumps(record) for record in batch)
                
                load_job = self.client.load_table_from_file(
                    file_obj=io.StringIO(json_data),
                    destination=temp_table_ref,
                    job_config=job_config
                )
                load_job.result()  # Wait for completion
                logger.info(f"Loaded batch {i // batch_size + 1} ({len(batch)} records)")
                
                # After first batch, change to append mode
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            
            # Step 3: Execute MERGE statement
            logger.info("Executing MERGE statement")
            merge_query = self._build_merge_query(dataset_id, table_id, temp_table_id)
            merge_job = self.client.query(merge_query)
            merge_result = merge_job.result()
            
            # Get merge statistics
            num_rows_affected = merge_job.num_dml_affected_rows
            logger.info(f"MERGE completed: {num_rows_affected} rows affected")
            
            # Step 4: Clean up temp table
            logger.info(f"Dropping temp table {temp_table_ref}")
            self.client.delete_table(temp_table_ref)
            
            return {
                'processed': len(records),
                'rows_affected': num_rows_affected,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error in MERGE operation: {str(e)}")
            # Try to clean up temp table on error
            try:
                self.client.delete_table(temp_table_ref)
            except:
                pass
            raise
    
    def _build_merge_query(self, dataset_id: str, table_id: str, temp_table_id: str) -> str:
        """Build the MERGE query for meta_ads tables
        
        Args:
            dataset_id: Dataset ID
            table_id: Target table ID
            temp_table_id: Temp table ID
            
        Returns:
            MERGE SQL query
        """
        # Get all fields from SchemaRegistry
        schema = SchemaRegistry.get_schema('insights')
        all_field_names = list(schema.keys())
        
        # Define key fields that shouldn't be updated
        key_fields = {'account_id', 'ad_id', 'date_start'}
        
        # List of fields to update (excluding key fields)
        update_fields = [field for field in all_field_names if field not in key_fields]
        
        # Build UPDATE SET clause
        update_clause = ',\n        '.join([f"{field} = S.{field}" for field in update_fields])
        
        # Build INSERT column list
        all_fields = ['account_id', 'ad_id', 'date_start'] + update_fields
        insert_columns = ', '.join(all_fields)
        insert_values = ', '.join([f"S.{field}" for field in all_fields])
        
        merge_query = f"""
        MERGE `{self.project_id}.{dataset_id}.{table_id}` T
        USING `{self.project_id}.{dataset_id}.{temp_table_id}` S
        ON T.ad_id = S.ad_id 
           AND T.date_start = S.date_start
           AND T.account_id = S.account_id
        WHEN MATCHED THEN
            UPDATE SET
                {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        
        return merge_query 