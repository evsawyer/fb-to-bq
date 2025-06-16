from fastapi import FastAPI, HTTPException
from bigquery import get_existing_records, separate_records, get_table_schema, process_records
from facebook import get_ads_insights, get_all_ad_ids
from validate import analyze_insights_structure, validate_insight, prepare_for_bigquery
from dotenv import load_dotenv
import os
from typing import List, Dict, Any
import asyncio
import logging

logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(
    title="Facebook Ads to BigQuery Sync",
    description="API to sync Facebook Ads insights data to BigQuery"
)

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
        # 1. Get Facebook Ads data
        logger.info("Fetching all Facebook ad IDs")
        ad_ids = get_all_ad_ids()
        logger.info(f"Retrieved {len(ad_ids)} ad IDs")

        logger.info("Fetching insights for all ad IDs")
        raw_insights = get_ads_insights(ad_ids)
        insights_list = [x for x in raw_insights]
        logger.info(f"Retrieved insights for {len(insights_list)} ads")

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
        await asyncio.to_thread(
            process_records,
            dataset_id=dataset_id,
            table_id=table_id,
            new_records=valid_insights,
            batch_size=1000
        )

        return {
            "status": "success",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing ads insights: {str(e)}"
        )