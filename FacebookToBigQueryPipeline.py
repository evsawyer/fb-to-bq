import logging
import json
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

from FacebookClient import FacebookClient
from BigQueryClient import BigQueryClient
from DataValidator import DataValidator
from KPIMappingManager import KPIMappingManager
from SchemaRegistry import SchemaRegistry
from Config import Config

logger = logging.getLogger(__name__)


class FacebookToBigQueryPipeline:
    """Main orchestrator for the Facebook to BigQuery ETL pipeline"""
    
    def __init__(self, config: Config = None):
        """Initialize the pipeline with configuration
        
        Args:
            config: Configuration object (defaults to loading from environment)
        """
        self.config = config or Config.from_env()
        
        # Log configuration
        logger.info("Initializing Facebook to BigQuery Pipeline")
        self.config.log_config()
        
        # Initialize components
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize all pipeline components"""
        # Core clients
        self.fb_client = FacebookClient(
            access_token=self.config.facebook.access_token,
            app_id=self.config.facebook.app_id,
            app_secret=self.config.facebook.app_secret
        )
        
        self.bq_client = BigQueryClient(
            credentials_json=self.config.bigquery.credentials_json,
            project_id=self.config.bigquery.project_id
        )
        
        # Get schema for validation
        insights_schema = SchemaRegistry.get_schema_dict('insights')
        self.validator = DataValidator(insights_schema)
        
        # Business logic components
        self.kpi_manager = KPIMappingManager(self.fb_client, self.bq_client)
    
    def run_full_sync(self, 
                     time_range: Dict[str, str] = None,
                     dry_run: bool = False) -> Dict[str, Any]:
        """Execute complete ETL pipeline
        
        Args:
            time_range: Optional date range {'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD'}
                       If None, defaults to last 30 days
            dry_run: If True, validate data but don't write to BigQuery
            
        Returns:
            Dictionary with execution results and statistics
        """
        logger.info("Starting full sync pipeline")
        start_time = datetime.now()
        results = {
            'start_time': start_time.isoformat(),
            'status': 'started',
            'steps': {}
        }
        
        try:
            # Step 1: Update KPI mappings if enabled
            if self.config.pipeline.update_kpi_mappings and not dry_run:
                logger.info("Step 1: Updating KPI mappings")
                mapping_results = self.kpi_manager.update_mapping_table()
                results['steps']['kpi_mappings'] = {
                    'status': 'success',
                    'mappings': mapping_results
                }
            else:
                logger.info("Step 1: Skipping KPI mapping update")
                results['steps']['kpi_mappings'] = {'status': 'skipped'}
            
            # Step 2: Fetch Facebook insights
            logger.info("Step 2: Fetching Facebook insights")
            insights = self.fb_client.get_insights(
                ad_account_ids=self.config.facebook.ad_account_ids,
                time_range=time_range,
                chunk_days=self.config.pipeline.chunk_days,
                delay_between_chunks=self.config.pipeline.delay_between_chunks
            )
            
            results['steps']['fetch_insights'] = {
                'status': 'success',
                'total_records': len(insights)
            }
            
            if not insights:
                logger.warning("No insights data retrieved")
                results['status'] = 'completed_no_data'
                return results
            
            # Step 3: Validate and transform data
            if self.config.pipeline.enable_validation:
                logger.info("Step 3: Validating and transforming data")
                validation_results = self.validator.validate_batch(insights)
                
                valid_records = validation_results['valid']
                invalid_records = validation_results['invalid']
                
                results['steps']['validation'] = {
                    'status': 'success',
                    'valid_records': len(valid_records),
                    'invalid_records': len(invalid_records)
                }
                
                if invalid_records:
                    # Save invalid records for debugging
                    self._save_invalid_records(invalid_records)
                
                insights_to_upload = valid_records
            else:
                logger.info("Step 3: Skipping validation")
                insights_to_upload = insights
                results['steps']['validation'] = {'status': 'skipped'}
            
            # Step 4: Upload to BigQuery
            if not dry_run and insights_to_upload:
                logger.info("Step 4: Uploading to BigQuery")
                table_id = self.config.bigquery.get_full_table_id(
                    self.config.bigquery.meta_ads_table
                )
                
                upload_results = self.bq_client.insert_records(
                    dataset_id=self.config.bigquery.dataset_id,
                    table_id=self.config.bigquery.meta_ads_table,
                    records=insights_to_upload,
                    batch_size=self.config.pipeline.batch_size
                )
                
                results['steps']['upload'] = {
                    'status': upload_results.get('status', 'success'),
                    'processed': upload_results.get('processed', 0),
                    'rows_affected': upload_results.get('rows_affected', 0)
                }
            else:
                logger.info("Step 4: Skipping BigQuery upload (dry run)")
                results['steps']['upload'] = {'status': 'skipped'}
            

            
            # Complete
            end_time = datetime.now()
            results['end_time'] = end_time.isoformat()
            results['duration_seconds'] = (end_time - start_time).total_seconds()
            results['status'] = 'completed'
            
            logger.info(f"Pipeline completed successfully in {results['duration_seconds']:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}")
            results['status'] = 'failed'
            results['error'] = str(e)
            raise
        
        return results
    
    def run_incremental_sync(self, days_back: int = 7) -> Dict[str, Any]:
        """Run incremental sync for recent data
        
        Args:
            days_back: Number of days to sync (default: 7)
            
        Returns:
            Dictionary with execution results
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        time_range = {
            'since': start_date.strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d')
        }
        
        logger.info(f"Running incremental sync for {days_back} days: {time_range}")
        return self.run_full_sync(time_range=time_range)
    
    def run_date_range_sync(self, 
                           start_date: str, 
                           end_date: str) -> Dict[str, Any]:
        """Run sync for a specific date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with execution results
        """
        time_range = {
            'since': start_date,
            'until': end_date
        }
        
        logger.info(f"Running date range sync: {time_range}")
        return self.run_full_sync(time_range=time_range)
    
    def validate_only(self, 
                     time_range: Dict[str, str] = None) -> Dict[str, Any]:
        """Run pipeline in validation-only mode
        
        Args:
            time_range: Optional date range
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Running validation-only pipeline")
        return self.run_full_sync(time_range=time_range, dry_run=True)
    
    def _save_invalid_records(self, invalid_records: List[Dict]):
        """Save invalid records to a file for debugging"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"invalid_records_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(invalid_records, f, indent=2)
        
        logger.warning(f"Saved {len(invalid_records)} invalid records to {filename}")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline configuration and status"""
        return {
            'configuration': self.config.to_dict(),
            'components': {
                'facebook_client': 'initialized',
                'bigquery_client': 'initialized',
                'validator': 'initialized',
                'kpi_manager': 'initialized'
            }
        }


# Convenience functions for direct execution
def run_full_sync(config: Config = None, **kwargs) -> Dict[str, Any]:
    """Convenience function to run full sync"""
    pipeline = FacebookToBigQueryPipeline(config)
    return pipeline.run_full_sync(**kwargs)


def run_incremental_sync(days_back: int = 7, config: Config = None) -> Dict[str, Any]:
    """Convenience function to run incremental sync"""
    pipeline = FacebookToBigQueryPipeline(config)
    return pipeline.run_incremental_sync(days_back)


def run_date_range_sync(start_date: str, end_date: str, config: Config = None) -> Dict[str, Any]:
    """Convenience function to run date range sync"""
    pipeline = FacebookToBigQueryPipeline(config)
    return pipeline.run_date_range_sync(start_date, end_date)


# Command line interface
if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Facebook to BigQuery Pipeline')
    parser.add_argument('--mode', choices=['full', 'incremental', 'daterange', 'validate'],
                       default='incremental', help='Sync mode')
    parser.add_argument('--days-back', type=int, default=7,
                       help='Days to sync for incremental mode')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD) for daterange mode')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD) for daterange mode')
    parser.add_argument('--dry-run', action='store_true',
                       help='Run in dry-run mode (no writes)')
    
    args = parser.parse_args()
    
    try:
        pipeline = FacebookToBigQueryPipeline()
        
        if args.mode == 'full':
            results = pipeline.run_full_sync(dry_run=args.dry_run)
        elif args.mode == 'incremental':
            results = pipeline.run_incremental_sync(days_back=args.days_back)
        elif args.mode == 'daterange':
            if not args.start_date or not args.end_date:
                parser.error('--start-date and --end-date required for daterange mode')
            results = pipeline.run_date_range_sync(args.start_date, args.end_date)
        elif args.mode == 'validate':
            results = pipeline.validate_only()
        
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        exit(1) 