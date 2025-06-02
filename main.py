import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad

# Init Facebook API
ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')
AD_ACCOUNT_ID = os.getenv('FB_AD_ACCOUNT_ID')
APP_ID = os.getenv('FB_APP_ID')
APP_SECRET = os.getenv('FB_APP_SECRET')

FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)


def get_campaigns():
    """Fetch and print all campaigns for the ad account."""
    account = AdAccount(AD_ACCOUNT_ID)
    campaigns = account.get_campaigns(fields=['id', 'name'])

    print("Campaigns:")
    for campaign in campaigns:
        print(f"  ID: {campaign['id']} | Name: {campaign['name']}")


def get_ad_insights(ad_id):
    """Fetch and print insights for a specific ad."""
    # Comprehensive list of available fields
    fields = [
        'account_id', 'ad_id', 'ad_name', 'adset_id', 'adset_name', 
        'campaign_id', 'campaign_name', 'date_start', 'date_stop',
        'impressions', 'reach', 'spend', 'clicks', 'cpc', 'cpm', 'cpp',
        'ctr', 'frequency', 'actions', 'action_values', 'cost_per_action_type',
        'cost_per_unique_action_type', 'unique_actions', 'video_play_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions', 
        'video_p75_watched_actions', 'video_p100_watched_actions',
        'objective', 'optimization_goal', 'quality_ranking', 'engagement_rate_ranking',
        'conversion_rate_ranking', 'cost_per_inline_link_click', 'inline_link_clicks',
        'inline_link_click_ctr', 'website_ctr'
    ]
    
    params = {
        'date_preset': 'this_month',
        'level': 'ad',
        'time_increment': 1
    }

    ad = Ad(ad_id)
    insights = ad.get_insights(fields=fields, params=params)

    for row in insights:
        print(f"\n--- Ad Insights ---")
        # Print all available fields and their values
        for key, value in row.items():
            print(f"{key}: {value}")


if __name__ == '__main__':
    print("Fetching campaigns...")
    get_campaigns()

    print("\nFetching insights for specific ad...")
    ad_id = '120228502463500226'  # Replace with a real ad ID
    try:
        get_ad_insights(ad_id)
    except Exception as e:
        print(f"Error fetching insights: {e}")
