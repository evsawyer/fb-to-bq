import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Init Facebook API
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
AD_ACCOUNT_ID = os.getenv('FB_AD_ACCOUNT_ID')
APP_ID = os.getenv('FB_APP_ID')
APP_SECRET = os.getenv('FB_APP_SECRET')

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
  account = AdAccount(AD_ACCOUNT_ID)
  campaigns = account.get_campaigns(fields=['id', 'name'])
  
  all_ad_ids = []
  
  print("Extracting ad IDs from campaigns...")
  for campaign in campaigns:
      print(f"Processing campaign: {campaign['name']} (ID: {campaign['id']})")
      
      # Get ads for this campaign
      ads = campaign.get_ads(fields=['id', 'name'])
      
      for ad in ads:
          all_ad_ids.append(ad['id'])
          print(f"  Found ad: {ad['name']} (ID: {ad['id']})")
  
  print(f"\nTotal ads found: {len(all_ad_ids)}")
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

