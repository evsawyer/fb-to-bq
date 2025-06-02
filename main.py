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
    fields = [
        'ad_name',
        'campaign_name',
        'impressions',
        'reach',
        'spend',
        'clicks',
        'actions',
        'cost_per_action_type',
        'objective',
        'adset_name',
        'date_start',
        'date_stop'
    ]

    params = {
        'date_preset': 'this_month',
        'level': 'ad',
        'time_increment': 1
    }

    ad = Ad(ad_id)
    insights = ad.get_insights(fields=fields, params=params)

    for row in insights:
        print(f"\nAd: {row['ad_name']}")
        print(f"Impressions: {row['impressions']}")
        print(f"Reach: {row['reach']}")
        print(f"Spend: ${row['spend']}")
        print(f"Clicks: {row.get('clicks', 0)}")

        actions = row.get('actions', [])
        for action in actions:
            print(f"Action: {action['action_type']} — Value: {action['value']}")

        costs = row.get('cost_per_action_type', [])
        for cost in costs:
            print(f"Cost per {cost['action_type']}: ${cost['value']}")


if __name__ == '__main__':
    print("Fetching campaigns...")
    get_campaigns()

    print("\nFetching insights for specific ad...")
    ad_id = '120228502463500226'  # Replace with a real ad ID
    try:
        get_ad_insights(ad_id)
    except Exception as e:
        print(f"Error fetching insights: {e}")
