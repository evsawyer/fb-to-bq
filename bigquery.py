from google.oauth2 import service_account
import os
from google.cloud import bigquery
import json
import io
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bigquery')

def get_bigquery_client():
    """Initialize BigQuery client with credentials from either file or environment variable."""
    try:
        # First try to get credentials from environment variable (production)
        credentials_json = os.getenv('GOOGLE_CREDENTIALS')
        
        if credentials_json:
            # Production: Use credentials from environment variable
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
        else:
            # Local: Try to use credentials file
            credentials_path = "credentials.json"
            if not os.path.exists(credentials_path):
                raise ValueError("No credentials found in environment variable or local file")
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        
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

client = get_bigquery_client()

def append_to_table(dataset_id: str, table_id: str, rows: list[dict]):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    errors = client.insert_rows_json(table, rows)
    
    if errors:
        logger.error(f"Encountered errors while inserting rows: {errors}")
    else:
        logger.info(f"Successfully inserted {len(rows)} rows into {dataset_id}.{table_id}")
    return errors

########################################################
# Get existing records
########################################################
def get_existing_records(dataset_id, table_id, date_starts, date_stops, ad_ids):
    """Get existing records for the given dates, date_stops, and ad_ids"""
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
    
    existing_records = list(client.query(query, job_config=job_config).result())
    logger.info(f"Number of existing records retrieved: {len(existing_records)}")
    return existing_records

########################################################
# Separate records into updates and inserts
########################################################
def separate_records(new_records, existing_records):
    """Separate records into updates and inserts based on existing records"""
    # Create a set of existing keys for quick lookup, converting dates to strings
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
    logger.info(f"Number of records to update: {len(updates)}")
    logger.info(f"Number of records to insert: {len(inserts)}")
    return updates, inserts

def get_table_schema(dataset_id, table_id):
    """Get the schema of the table, excluding the primary key fields"""
    table = client.get_table(f"{dataset_id}.{table_id}")
    # Get all field names except the primary key fields
    return [field.name for field in table.schema 
            if field.name not in ['date_start', 'date_stop', 'ad_id']]

########################################################
# Process records
########################################################
def process_records(dataset_id, table_id, new_records, batch_size=1000):
    """Process records in batches, handling updates and inserts"""
    
    logger.info("Fetching updatable fields from the table schema")
    updatable_fields = get_table_schema(dataset_id, table_id)
    logger.info(f"Updatable fields: {updatable_fields}")

    logger.info("Constructing update clause for SQL merge operation")
    update_clause = ",\n        ".join(f"{field} = S.{field}" for field in updatable_fields)
    logger.debug(f"Update clause: {update_clause}")

    logger.info("Extracting unique date_starts, date_stops, and ad_ids from new records")
    date_starts = list(set(r['date_start'] for r in new_records))
    date_stops = list(set(r['date_stop'] for r in new_records))
    ad_ids = list(set(r['ad_id'] for r in new_records))
    logger.debug(f"Unique date_starts: {date_starts}")
    logger.debug(f"Unique date_stops: {date_stops}")
    logger.debug(f"Unique ad_ids: {ad_ids}")

    logger.info("Retrieving existing records from BigQuery")
    existing_records = get_existing_records(dataset_id, table_id, date_starts, date_stops, ad_ids)
    logger.info(f"Number of existing records retrieved: {len(existing_records)}")

    logger.info("Separating new records into updates and inserts")
    updates, inserts = separate_records(new_records, existing_records)
    logger.info(f"Number of records to update: {len(updates)}")
    logger.info(f"Number of records to insert: {len(inserts)}")

    logger.info("Fetching project ID from BigQuery client")
    project_id = client.project  # Get the project ID from the client
    logger.debug(f"Project ID: {project_id}")
    logger.info(f"Project ID: {project_id}")
    logger.info(f"Total records to process: {len(new_records)}")
    logger.info(f"Records to update: {len(updates)}, Records to insert: {len(inserts)}")

    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        logger.info(f"Processing update batch {i // batch_size + 1} with {len(batch)} records")
        temp_table_id = f"temp_updates_{i}"
        temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
        main_table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Create the temp table with the same schema
        temp_table = bigquery.Table(temp_table_ref)
        temp_table.schema = client.get_table(main_table_ref).schema
        client.create_table(temp_table, exists_ok=True)
        logger.info(f"Temporary table {temp_table_ref} created")

        # Load the batch into temp table using load_table_from_file
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        json_data = '\n'.join(json.dumps(record) for record in batch)
        client.load_table_from_file(
            file_obj=io.StringIO(json_data),
            destination=temp_table_ref,
            job_config=job_config
        ).result()
        logger.info(f"Loaded update batch {i // batch_size + 1} into temporary table")

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
        client.query(merge_query).result()
        logger.info(f"Merge completed for update batch {i // batch_size + 1}")

        # Clean up temp table (use fully-qualified string)
        client.delete_table(temp_table_ref)
        logger.info(f"Temporary table {temp_table_ref} deleted")

    for i in range(0, len(inserts), batch_size):
        batch = inserts[i:i + batch_size]
        logger.info(f"Processing insert batch {i // batch_size + 1} with {len(batch)} records")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        json_data = '\n'.join(json.dumps(record) for record in batch)
        client.load_table_from_file(
            file_obj=io.StringIO(json_data),
            destination=f"{project_id}.{dataset_id}.{table_id}",
            job_config=job_config
        ).result()
        logger.info(f"Loaded insert batch {i // batch_size + 1} into main table")