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

  