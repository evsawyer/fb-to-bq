import requests
import pandas as pd
from dotenv import load_dotenv
import os
from typing import List, Dict
from bigquery import get_bigquery_client
import pandas_gbq
import json
load_dotenv()

def fetch_custom_conversions_from_facebook(access_token: str, ad_account_id: str) -> List[Dict]:
    """Fetch custom conversions and their human-readable names from Facebook"""
    
    # Ensure we have the 'act_' prefix for the API call
    if not ad_account_id.startswith('act_'):
        formatted_account_id = f"act_{ad_account_id}"
    else:
        formatted_account_id = ad_account_id
    
    url = f"https://graph.facebook.com/v18.0/{formatted_account_id}/customconversions"
    params = {
        'access_token': access_token,
        'fields': 'id,name,custom_event_type'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        custom_mappings = []
        for conversion in data.get('data', []):
            custom_mappings.append({
                'user_friendly_name': conversion['name'],
                'meta_action_type': f"offsite_conversion.custom.{conversion['id']}",
                'mapping_type': 'custom',
                'ad_account_id': ad_account_id.replace('act_', ''),  # Store without act_ prefix
                'facebook_conversion_id': conversion['id']
            })
        
        return custom_mappings
        
    except requests.RequestException as e:
        print(f"Error fetching custom conversions for account {formatted_account_id}: {e}")
        return []

def get_standard_mappings() -> List[Dict]:
    """Get standard event mappings that apply to all accounts"""
    return [
        # Standard event mappings
        {'user_friendly_name': 'Lead', 'meta_action_type': 'lead', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Video View', 'meta_action_type': 'video_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Purchase', 'meta_action_type': 'purchase', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Page View', 'meta_action_type': 'page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Link Click', 'meta_action_type': 'link_click', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Page Engagement', 'meta_action_type': 'page_engagement', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Post Engagement', 'meta_action_type': 'post_engagement', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Landing Page View', 'meta_action_type': 'landing_page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Post Reaction', 'meta_action_type': 'post_reaction', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Post Save', 'meta_action_type': 'post_save', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        {'user_friendly_name': 'Web Lead', 'meta_action_type': 'web_lead', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        # {'user_friendly_name': 'Omni Landing Page View', 'meta_action_type': 'omni_landing_page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
        # Pixel event mappings  
        # {'user_friendly_name': 'Lead', 'meta_action_type': 'offsite_conversion.fb_pixel_lead', 'mapping_type': 'pixel', 'ad_account_id': 'all'},
        # {'user_friendly_name': 'Purchase', 'meta_action_type': 'offsite_conversion.fb_pixel_purchase', 'mapping_type': 'pixel', 'ad_account_id': 'all'},
    ]

def update_mapping_table_with_facebook_data():
    """Update the mapping table with fresh data from all Facebook ad accounts"""
    
    access_token = os.getenv('FB_ACCESS_TOKEN')
    if not access_token:
        raise ValueError("FB_ACCESS_TOKEN environment variable not found")
    
    # Get all ad account IDs
    ad_account_ids = json.loads(os.getenv("FB_AD_ACCOUNT_ID"))
    print(f"Processing {len(ad_account_ids)} ad accounts")
    
    # Start with standard mappings
    all_mappings = get_standard_mappings()
    
    # Fetch custom conversions from each ad account
    total_custom_conversions = 0
    for account_id in ad_account_ids:
        print(f"Fetching custom conversions for account: {account_id}")
        custom_mappings = fetch_custom_conversions_from_facebook(access_token, account_id)
        all_mappings.extend(custom_mappings)
        total_custom_conversions += len(custom_mappings)
        print(f"Found {len(custom_mappings)} custom conversions for account {account_id}")
    
    print(f"Total: {len(get_standard_mappings())} standard mappings + {total_custom_conversions} custom conversions = {len(all_mappings)} total mappings")
    
    if not all_mappings:
        print("Warning: No mappings found!")
        return
    
    # Update BigQuery table using pandas-gbq
    try:
        # Get BigQuery client for credentials
        client = get_bigquery_client()
        
        df = pd.DataFrame(all_mappings)
        
        # Add timestamp for tracking when mappings were last updated
        df['last_updated'] = pd.Timestamp.now()
        
        table_id = "raw_ads.kpi_event_mapping"
        
        # Use pandas_gbq.to_gbq instead of df.to_gbq
        pandas_gbq.to_gbq(
            df, 
            destination_table=table_id, 
            project_id='ivc-media-ads-warehouse',
            if_exists='replace',
            credentials=client._credentials  # Use the same credentials from your BigQuery client
        )
        
        print(f"Successfully updated mapping table with {len(all_mappings)} total mappings")
        
        # Print summary by account and type
        summary = df.groupby(['ad_account_id', 'mapping_type']).size().reset_index(name='count')
        print("\nMapping summary:")
        print(summary.to_string(index=False))
        
    except Exception as e:
        print(f"Error updating BigQuery table: {e}")
        raise