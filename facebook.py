import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from dotenv import load_dotenv
import json
import logging
import time
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError

logger = logging.getLogger(__name__)

load_dotenv()

# Init Facebook API
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
AD_ACCOUNT_ID = os.getenv('FB_AD_ACCOUNT_ID')
APP_ID = os.getenv('FB_APP_ID')
APP_SECRET = os.getenv('FB_APP_SECRET')

# Log the tail of each variable
logger.info(f"ACCESS_TOKEN tail: {ACCESS_TOKEN[-4:] if ACCESS_TOKEN else 'None'}")
logger.info(f"AD_ACCOUNT_ID tail: {AD_ACCOUNT_ID[-4:] if AD_ACCOUNT_ID else 'None'}")
logger.info(f"APP_ID tail: {APP_ID[-4:] if APP_ID else 'None'}")
logger.info(f"APP_SECRET tail: {APP_SECRET[-4:] if APP_SECRET else 'None'}")

FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# def get_campaigns():
#   """Fetch and print all campaigns for the ad account."""
#   account = AdAccount(AD_ACCOUNT_ID)
#   campaigns = account.get_campaigns(fields=['id', 'name'])

#   print("Campaigns:")
#   for campaign in campaigns:
#       print(f"  ID: {campaign['id']} | Name: {campaign['name']}")


def get_all_ad_ids():   
  """Extract all ad IDs from all campaigns and return as a list."""

  ad_account_ids = AD_ACCOUNT_ID
  if ad_account_ids:
      logger.info(f"AD_ACCOUNT_ID: {ad_account_ids}")
      try:
        ad_account_ids = json.loads(ad_account_ids)
      except json.JSONDecodeError:
        logger.error(f"Failed to parse AD_ACCOUNT_ID: {ad_account_ids}")
        raise ValueError("Invalid JSON format for AD_ACCOUNT_ID")
  else:
      raise ValueError("No ad account IDs found in environment variable")

  all_ad_ids = []

  for ad_account_id in ad_account_ids:
    account = AdAccount(ad_account_id)
                # Get all ads directly from the account
    ads = account.get_ads(
        fields=['id', 'name'],
        params={'limit': 1000}  # Get more ads per request
    )
    
    for ad in ads:
        all_ad_ids.append(ad['id'])
        logger.info(f"Found ad: {ad['name']} (ID: {ad['id']})")

  logger.info(f"\nTotal ads found: {len(all_ad_ids)}")
  return all_ad_ids


def get_ads_insights(ad_ids: list[str]) -> list[dict]:
    """Fetch insights for multiple ads.
    
    Args:
        ad_ids: List of Facebook ad IDs to fetch insights for
        
    Returns:
        list[dict]: List of insights data for all ads
    """
    # Comprehensive list of available fields
    fields = [
        # Metadata
        'account_id', 'account_name', 'account_currency',
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 
        'campaign_id', 'campaign_name',
        'date_start', 'date_stop',

        # Basic performance
        'impressions', 'reach', 'frequency',
        'spend', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr',
        'unique_clicks', 'unique_ctr', 'cost_per_unique_click',

        # Website interactions
        'inline_link_clicks', 'inline_link_click_ctr', 'website_ctr',

        # Action metrics
        'actions', 'action_values',
        'unique_actions',
        'cost_per_action_type', 'cost_per_unique_action_type',

        # ROAS
        'purchase_roas',

        # Video engagement
        'video_play_actions', 'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p100_watched_actions',

        # Ad quality diagnostics
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',

        # Objectives
        'objective', 'optimization_goal'
    ]

    params = {
        'date_preset': 'last_month',
        'level': 'ad',
        'time_increment': 1
    }

    all_insights = []
    
    for ad_id in ad_ids:
        try:
            ad = Ad(ad_id)
            insights = ad.get_insights(fields=fields, params=params)
            all_insights.extend(insights)
            print(f"Successfully fetched insights for ad {ad_id}")
        
        except Exception as e:
            print(f"Error fetching insights for ad {ad_id}: {str(e)}")
            continue

    print(f"\nFetched insights for {len(all_insights)} entries across {len(ad_ids)} ads")

    return all_insights

def get_ads_insights_with_delay(ad_ids: list[str], delay_seconds: float = 0.75) -> list[dict]:
    """Fetch insights for multiple ads with hardcoded delay between requests.
    
    Args:
        ad_ids: List of Facebook ad IDs to fetch insights for
        delay_seconds: Delay between each request (default 0.75 seconds)
        
    Returns:
        list[dict]: List of insights data for all ads
    """
    # Comprehensive list of available fields
    fields = [
        # Metadata
        'account_id', 'account_name', 'account_currency',
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 
        'campaign_id', 'campaign_name',
        'date_start', 'date_stop',

        # Basic performance
        'impressions', 'reach', 'frequency',
        'spend', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr',
        'unique_clicks', 'unique_ctr', 'cost_per_unique_click',

        # Website interactions
        'inline_link_clicks', 'inline_link_click_ctr', 'website_ctr',

        # Action metrics
        'actions', 'action_values',
        'unique_actions',
        'cost_per_action_type', 'cost_per_unique_action_type',

        # ROAS
        'purchase_roas',

        # Video engagement
        'video_play_actions', 'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p100_watched_actions',

        # Ad quality diagnostics
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',

        # Objectives
        'objective', 'optimization_goal'
    ]

    params = {
        'date_preset': 'last_30d',
        'level': 'ad',
        'time_increment': 1
    }

    all_insights = []
    failed_ads = []
    total_ads = len(ad_ids)
    estimated_time = total_ads * delay_seconds / 60  # in minutes
    
    print(f"Processing {total_ads} ads with {delay_seconds}s delay between requests")
    print(f"Estimated completion time: {estimated_time:.1f} minutes")
    print("=" * 50)
    
    start_time = time.time()
    
    for i, ad_id in enumerate(ad_ids, 1):
        try:
            ad = Ad(ad_id)
            insights = ad.get_insights(fields=fields, params=params)
            
            # Convert to list and add to results
            insights_list = list(insights)
            all_insights.extend(insights_list)
            
            # Progress logging every 100 ads
            if i % 100 == 0 or i == total_ads:
                elapsed_time = (time.time() - start_time) / 60
                remaining_ads = total_ads - i
                estimated_remaining = (remaining_ads * delay_seconds) / 60
                
                print(f"Progress: {i}/{total_ads} ads ({i/total_ads*100:.1f}%) | "
                      f"Elapsed: {elapsed_time:.1f}min | "
                      f"Est. remaining: {estimated_remaining:.1f}min | "
                      f"Success rate: {(i-len(failed_ads))/i*100:.1f}%")
            
            # Only delay if not the last request
            if i < total_ads:
                time.sleep(delay_seconds)
        
        except FacebookRequestError as e:
            print(f"Facebook API error for ad {ad_id} (#{i}): {e}")
            failed_ads.append(ad_id)
            
            # Still delay even on errors to avoid hammering the API
            if i < total_ads:
                time.sleep(delay_seconds)
        
        except Exception as e:
            print(f"Unexpected error for ad {ad_id} (#{i}): {str(e)}")
            failed_ads.append(ad_id)
            
            # Still delay even on errors
            if i < total_ads:
                time.sleep(delay_seconds)

    # Final summary
    total_time = (time.time() - start_time) / 60
    success_count = len(ad_ids) - len(failed_ads)
    
    print("=" * 50)
    print(f"COMPLETED!")
    print(f"Total time: {total_time:.1f} minutes")
    print(f"Successfully processed: {success_count}/{total_ads} ads ({success_count/total_ads*100:.1f}%)")
    print(f"Failed ads: {len(failed_ads)}")
    print(f"Total insights entries: {len(all_insights)}")
    
    if failed_ads:
        print(f"First 10 failed ad IDs: {failed_ads[:10]}")

    return all_insights


def get_all_ads_insights_bulk(ad_account_ids: list[str]) -> list[dict]:
    """Fetch insights for ALL ads in account(s) with minimal API calls.
    
    Args:
        ad_account_ids: List of Facebook ad account IDs
        
    Returns:
        list[dict]: List of insights data for all ads across all accounts
    """
    # Comprehensive list of available fields
    fields = [
        # Metadata
        'account_id', 'account_name', 'account_currency',
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 
        'campaign_id', 'campaign_name',
        'date_start', 'date_stop',

        # Basic performance
        'impressions', 'reach', 'frequency',
        'spend', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr',
        'unique_clicks', 'unique_ctr', 'cost_per_unique_click',

        # Website interactions
        'inline_link_clicks', 'inline_link_click_ctr', 'website_ctr',

        # Action metrics
        'actions', 'action_values',
        'unique_actions',
        'cost_per_action_type', 'cost_per_unique_action_type',

        # ROAS
        'purchase_roas',

        # Video engagement
        'video_play_actions', 'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p100_watched_actions',

        # Ad quality diagnostics
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',

        # Objectives
        'objective', 'optimization_goal'
    ]

    params = {
        'date_preset': 'last_30d',
        'level': 'ad',  # This gets insights broken down by individual ads
        'time_increment': 1,
        'limit': 1000,  # Maximum results per page
    }

    all_insights = []
    failed_accounts = []
    
    print(f"Processing {len(ad_account_ids)} ad account(s) for bulk insights")
    print("=" * 60)
    
    start_time = time.time()
    
    for i, ad_account_id in enumerate(ad_account_ids, 1):
        try:
            print(f"Processing account {i}/{len(ad_account_ids)}: {ad_account_id}")
            
            account = AdAccount(ad_account_id)
            
            # Get insights for ALL ads in this account with pagination
            insights_cursor = account.get_insights(fields=fields, params=params)
            
            account_insights = []
            page_count = 0
            
            # Handle pagination correctly - iterate through pages, not individual insights
            try:
                for insights_batch in insights_cursor:
                    # insights_batch is a list of insight objects
                    for insight in insights_batch:
                        # Convert insight object to dictionary
                        insight_dict = dict(insight)
                        account_insights.append(insight_dict)
                    
                    page_count += 1
                    if page_count % 10 == 0:  # Less frequent updates
                        print(f"  📄 Page {page_count}: {len(account_insights)} insights processed...")
                        
            except StopIteration:
                # End of pagination
                pass
            
            all_insights.extend(account_insights)
            
            elapsed_time = (time.time() - start_time) / 60
            print(f"  ✅ Account {ad_account_id}: {len(account_insights)} insights")
            print(f"  📊 Total insights so far: {len(all_insights)}")
            print(f"  ⏱️  Elapsed time: {elapsed_time:.1f} minutes")
            print()
            
            # Small delay between accounts to be respectful
            if i < len(ad_account_ids):
                time.sleep(1)
        
        except FacebookRequestError as e:
            # Properly extract error codes
            try:
                error_code = e.api_error_code() if hasattr(e, 'api_error_code') else None
                error_subcode = e.api_error_subcode() if hasattr(e, 'api_error_subcode') else None
            except:
                error_code = None
                error_subcode = None
            
            print(f"❌ Facebook API error for account {ad_account_id}:")
            print(f"   Code: {error_code}, Subcode: {error_subcode}")
            print(f"   Message: {str(e)[:200]}...")  # Truncate long error messages
            
            # Handle specific error types
            if error_code == 1 and error_subcode == 99:
                print("   ℹ️  This is likely a temporary server error or account access issue")
            elif error_code == 17:
                print("   ⚠️  Rate limit exceeded - consider adding delays")
            elif error_code == 190:
                print("   🔑 Access token issue - check permissions")
            
            failed_accounts.append({
                'account_id': ad_account_id,
                'error_code': error_code,
                'error_subcode': error_subcode,
                'error_message': str(e)[:500]  # Limit error message length
            })
        
        except Exception as e:
            print(f"❌ Unexpected error for account {ad_account_id}: {str(e)[:200]}...")
            failed_accounts.append({
                'account_id': ad_account_id,
                'error_code': 'unknown',
                'error_message': str(e)[:500]  # Limit error message length
            })

    # Final summary
    total_time = (time.time() - start_time) / 60
    success_count = len(ad_account_ids) - len(failed_accounts)
    
    print("=" * 60)
    print(f"🎉 COMPLETED!")
    print(f"⏱️  Total time: {total_time:.1f} minutes")
    print(f"✅ Successfully processed: {success_count}/{len(ad_account_ids)} accounts")
    print(f"❌ Failed accounts: {len(failed_accounts)}")
    print(f"📊 Total insights entries: {len(all_insights)}")
    
    if failed_accounts:
        print("Failed accounts details:")
        for failed in failed_accounts:
            if isinstance(failed, dict):
                print(f"  {failed['account_id']}: {failed.get('error_code', 'unknown')} - {failed.get('error_message', 'No message')}")
            else:
                print(f"  {failed}: Legacy error format")

    return all_insights


def get_all_ads_insights_bulk_simple(ad_account_ids: list[str]) -> list[dict]:
    """Simple version - let Facebook SDK handle pagination automatically."""
    
    # Comprehensive list of available fields
    fields = [
        # Metadata
        'account_id', 'account_name', 'account_currency',
        'ad_id', 'ad_name', 'adset_id', 'adset_name', 
        'campaign_id', 'campaign_name',
        'date_start', 'date_stop',

        # Basic performance
        'impressions', 'reach', 'frequency',
        'spend', 'clicks', 'cpc', 'cpm', 'cpp', 'ctr',
        'unique_clicks', 'unique_ctr', 'cost_per_unique_click',

        # Website interactions
        'inline_link_clicks', 'inline_link_click_ctr', 'website_ctr',

        # Action metrics
        'actions', 'action_values',
        'unique_actions',
        'cost_per_action_type', 'cost_per_unique_action_type',

        # ROAS
        'purchase_roas',

        # Video engagement
        'video_play_actions', 'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p100_watched_actions',

        # Ad quality diagnostics
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',

        # Objectives
        'objective', 'optimization_goal'
    ]

    params = {
        'date_preset': 'last_30d',
        'level': 'ad',  # This gets insights broken down by individual ads
        'time_increment': 1,
        'limit': 1000,  # Maximum results per page
    }
    
    all_insights = []
    
    for ad_account_id in ad_account_ids:
        print(f"Processing {ad_account_id}...")
        
        try:
            account = AdAccount(ad_account_id)
            insights = account.get_insights(fields=fields, params=params)
            
            # Just convert to list - SDK handles all pagination behind the scenes
            account_insights = [dict(insight) for insight in insights]
            all_insights.extend(account_insights)
            
            print(f"  ✅ Got {len(account_insights)} insights")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            continue
    
    print(f"🎉 Total: {len(all_insights)} insights")
    return all_insights
