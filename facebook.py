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

if __name__ == '__main__':
    # Get all ad IDs
    ad_ids = get_all_ad_ids()
    print(f"\nAll Ad IDs: {ad_ids}")
