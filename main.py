from fastapi import FastAPI, HTTPException
from bigquery import get_existing_records, separate_records, get_table_schema, process_records
from facebook import get_ads_insights_with_delay, get_all_ad_ids, get_ads_insights, get_all_ads_insights_bulk_simple
from validate import analyze_insights_structure, validate_insight, prepare_for_bigquery
from kpi_event_mapping_table import update_mapping_table_with_facebook_data
from rollup import execute_ads_rollup_query
from dotenv import load_dotenv
import os
from typing import List, Dict, Any
import asyncio
import logging
import sys
import json
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
async def sync_ads_insights() -> Dict[str, Any]:
    """
    Endpoint to fetch Facebook Ads insights and sync them to BigQuery.
    This endpoint:
    1. Fetches all ad IDs
    2. Gets insights for those ads
    3. Validates and prepares the data
    4. Syncs with BigQuery (updates/inserts as needed)
    """
    try:
        # Send back a confirmation that the request was received
        logger.info("Received request to sync ads insights")
        # return {"message": "Request to sync ads insights received successfully"}
        # 1. Get Facebook Ads data
        # logger.info("Fetching all Facebook ad IDs")
        # ad_ids = get_all_ad_ids()
        # logger.info(f"Retrieved {len(ad_ids)} ad IDs")

        # logger.info("Fetching insights for all ad IDs")
        # raw_insights = get_ads_insights_with_delay(ad_ids)
        # insights_list = [x for x in raw_insights]
        # logger.info(f"Retrieved insights for {len(insights_list)} ads")
        print("🚀 Starting bulk insights fetch with rate limit monitoring...")
        ad_account_ids = json.loads(os.getenv("FB_AD_ACCOUNT_ID"))
        # Use the bulk method
        insights = get_all_ads_insights_bulk_simple(ad_account_ids)
        insights_list = [x for x in insights]

        # 2. Validate and prepare records
        logger.info("Validating and preparing insights for BigQuery")
        valid_insights = []
        for insight in insights_list:
            if validate_insight(insight):
                prepared_data = prepare_for_bigquery(insight)
                if prepared_data:
                    valid_insights.append(prepared_data)

        if not valid_insights:
            raise HTTPException(
                status_code=400,
                detail="No valid insights found to process"
            )

        # 3. Get BigQuery configuration
        dataset_id = os.getenv('DATASET_ID')
        table_id = os.getenv('TABLE_ID')

        # 4. Get existing records
        # date_starts = list(set(r['date_start'] for r in valid_insights))
        # date_stops = list(set(r['date_stop'] for r in valid_insights))
        # ad_ids = list(set(r['ad_id'] for r in valid_insights))

        # existing = list(get_existing_records(dataset_id, table_id, date_starts, date_stops, ad_ids))

        # # 5. Separate updates and inserts
        # updates, inserts = separate_records(valid_insights, existing)

        # 6. Process records
        logger.info("Starting to process records for BigQuery")
        await asyncio.to_thread(
            process_records,
            dataset_id=dataset_id,
            table_id=table_id,
            new_records=valid_insights,
            batch_size=1000
        )
        logger.info("Finished processing records for BigQuery")
        
        # logger.info("Waiting for 1 minute before updating mapping table")
        # await asyncio.sleep(60)

        logger.info("Starting to update mapping table with Facebook data")
        await asyncio.to_thread(update_mapping_table_with_facebook_data)
        logger.info("Finished updating mapping table with Facebook data")
        
        # logger.info("Waiting for 1 minute before executing rollup query")
        # await asyncio.sleep(60)

        logger.info("Starting to execute ads rollup query")
        await asyncio.to_thread(execute_ads_rollup_query)
        logger.info("Finished executing ads rollup query")

        return {
            "status": "success",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ads insights: {str(e)}"
        )