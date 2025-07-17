from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os
from typing import List, Dict, Any, Optional
import asyncio
import logging
import sys
import json

# Import the new pipeline class
from FacebookToBigQueryPipeline import FacebookToBigQueryPipeline
from Config import Config

# Configure root logger to capture all modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Important for Cloud Run
)

logger = logging.getLogger(__name__)
load_dotenv()

app = FastAPI(
    title="Facebook Ads to BigQuery Sync",
    description="API to sync Facebook Ads insights data to BigQuery"
)
@app.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint to verify the service is running.
    Returns a simple status message.
    """
    return {"status": "healthy"}

@app.post("/sync-ads-insights")
async def sync_ads_insights(
    mode: str = "incremental",
    days_back: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dry_run: bool = False,
    skip_kpi_update: bool = False,
    use_test_table: bool = False 
) -> Dict[str, Any]:
    """
    Test endpoint using the new class-based pipeline architecture.
    
    Args:
        mode: Sync mode - "incremental", "full", or "daterange"
        days_back: For incremental mode, number of days to sync (default: 7)
        start_date: For daterange mode, start date (YYYY-MM-DD)
        end_date: For daterange mode, end date (YYYY-MM-DD)
        dry_run: If True, validate data but don't write to BigQuery
        skip_kpi_update: If True, skip updating KPI mappings
        use_test_table: If True, write to meta_ads_test instead of meta_ads (default: False)
        
    Returns:
        Dictionary with execution results and statistics
    """
    try:
        logger.info(f"Received test sync request - mode: {mode}, dry_run: {dry_run}")
        
        # Create configuration with optional overrides
        config = Config.from_env()
        
        # Apply endpoint parameters to config
        if skip_kpi_update:
            config.pipeline.update_kpi_mappings = False
            
        # Use test table if requested
        if use_test_table:
            config.bigquery.meta_ads_table = config.bigquery.test_meta_ads_table
            
        # Initialize pipeline with config
        pipeline = FacebookToBigQueryPipeline(config)
        
        # Execute based on mode
        if mode == "incremental":
            logger.info(f"Running incremental sync for {days_back} days")
            results = await asyncio.to_thread(
                pipeline.run_incremental_sync,
                days_back=days_back
            )
            
        elif mode == "daterange":
            if not start_date or not end_date:
                raise HTTPException(
                    status_code=400,
                    detail="start_date and end_date are required for daterange mode"
                )
            logger.info(f"Running date range sync from {start_date} to {end_date}")
            results = await asyncio.to_thread(
                pipeline.run_date_range_sync,
                start_date=start_date,
                end_date=end_date
            )
            
        elif mode == "full":
            logger.info("Running full sync (last 30 days)")
            results = await asyncio.to_thread(
                pipeline.run_full_sync,
                dry_run=dry_run
            )
            
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid mode: {mode}. Must be 'incremental', 'full', or 'daterange'"
            )
        
        # Add endpoint parameters to results for transparency
        results['parameters'] = {
            'mode': mode,
            'dry_run': dry_run,
            'skip_kpi_update': skip_kpi_update,
            'use_test_table': use_test_table,
            'target_table': config.bigquery.meta_ads_table,
            'days_back': days_back if mode == 'incremental' else None,
            'date_range': {
                'start': start_date,
                'end': end_date
            } if mode == 'daterange' else None
        }
        
        return results
        
    except ValueError as e:
        # Configuration errors
        raise HTTPException(
            status_code=400,
            detail=f"Configuration error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Test sync failed with error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error in test sync: {str(e)}"
        )


@app.get("/test-sync-status")
async def test_sync_status() -> Dict[str, Any]:
    """
    Get the current status and configuration of the test pipeline.
    Useful for debugging and verifying configuration.
    """
    try:
        # Create pipeline to check configuration
        pipeline = FacebookToBigQueryPipeline()
        status = pipeline.get_pipeline_status()
        
        # Add environment info
        status['environment'] = {
            'dataset_id': os.getenv('DATASET_ID', 'Not set'),
            'table_id': os.getenv('TABLE_ID', 'Not set'),
            'fb_account_count': len(json.loads(os.getenv('FB_AD_ACCOUNT_ID', '[]'))),
            'pipeline_mode': 'class-based (new)'
        }
        
        return status
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error getting pipeline status: {str(e)}"
        )