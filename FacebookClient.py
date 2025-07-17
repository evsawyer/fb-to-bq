import os
import json
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class FacebookClient:
    """Handles all Facebook API interactions"""
    
    # Comprehensive list of available fields for insights
    INSIGHT_FIELDS = [
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
        'video_thruplay_watched_actions',

        # Ad quality diagnostics
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',

        # Objectives
        'objective', 'optimization_goal'
    ]
    
    def __init__(self, access_token: str = None, app_id: str = None, app_secret: str = None):
        """Initialize Facebook API client
        
        Args:
            access_token: Facebook access token (defaults to env var FB_ACCESS_TOKEN)
            app_id: Facebook app ID (defaults to env var FB_APP_ID)
            app_secret: Facebook app secret (defaults to env var FB_APP_SECRET)
        """
        self.access_token = access_token or os.getenv('FB_ACCESS_TOKEN')
        self.app_id = app_id or os.getenv('FB_APP_ID')
        self.app_secret = app_secret or os.getenv('FB_APP_SECRET')
        
        # Log the tail of each variable for security
        logger.info(f"ACCESS_TOKEN tail: {self.access_token[-4:] if self.access_token else 'None'}")
        logger.info(f"APP_ID tail: {self.app_id[-4:] if self.app_id else 'None'}")
        logger.info(f"APP_SECRET tail: {self.app_secret[-4:] if self.app_secret else 'None'}")
        
        # Initialize Facebook API
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
    
    def get_ad_account_ids(self) -> List[str]:
        """Parse and return ad account IDs from environment or config"""
        ad_account_ids = os.getenv('FB_AD_ACCOUNT_ID')
        
        if ad_account_ids:
            logger.info(f"AD_ACCOUNT_ID: {ad_account_ids}")
            try:
                return json.loads(ad_account_ids)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse AD_ACCOUNT_ID: {ad_account_ids}")
                raise ValueError("Invalid JSON format for AD_ACCOUNT_ID")
        else:
            raise ValueError("No ad account IDs found in environment variable")
    
    def get_all_ad_ids(self, ad_account_ids: List[str] = None) -> List[str]:
        """Extract all ad IDs from specified ad accounts
        
        Args:
            ad_account_ids: List of ad account IDs (defaults to env var)
            
        Returns:
            List of all ad IDs found
        """
        if ad_account_ids is None:
            ad_account_ids = self.get_ad_account_ids()
        
        all_ad_ids = []
        
        for ad_account_id in ad_account_ids:
            account = AdAccount(ad_account_id)
            ads = account.get_ads(
                fields=['id', 'name'],
                params={'limit': 1000}
            )
            
            for ad in ads:
                all_ad_ids.append(ad['id'])
                logger.info(f"Found ad: {ad['name']} (ID: {ad['id']})")
        
        logger.info(f"Total ads found: {len(all_ad_ids)}")
        return all_ad_ids
    
    def get_insights(self, 
                    ad_account_ids: List[str] = None,
                    time_range: Dict[str, str] = None,
                    chunk_days: int = 7,
                    delay_between_chunks: float = 0.2,
                    fields: List[str] = None) -> List[dict]:
        """Unified method for fetching insights with smart batching
        
        Args:
            ad_account_ids: List of ad account IDs (defaults to env var)
            time_range: Dict with 'since' and 'until' dates (format: 'YYYY-MM-DD')
                       If None, defaults to last 30 days
            chunk_days: Number of days per chunk for date range queries
            delay_between_chunks: Delay between chunk requests in seconds
            fields: List of fields to retrieve (defaults to INSIGHT_FIELDS)
            
        Returns:
            List of insights data for all ads
        """
        if ad_account_ids is None:
            ad_account_ids = self.get_ad_account_ids()
            
        if fields is None:
            fields = self.INSIGHT_FIELDS
        
        if time_range:
            return self._get_insights_date_range(
                ad_account_ids, time_range, chunk_days, delay_between_chunks, fields
            )
        else:
            return self._get_insights_bulk(ad_account_ids, fields)
    
    def _get_insights_bulk(self, ad_account_ids: List[str], fields: List[str]) -> List[dict]:
        """Fetch insights for ALL ads in accounts with minimal API calls"""
        params = {
            'date_preset': 'last_30d',
            'level': 'ad',
            'time_increment': 1,
            'limit': 500,
        }
        
        all_insights = []
        
        for ad_account_id in ad_account_ids:
            logger.info(f"Processing {ad_account_id}...")
            
            try:
                account = AdAccount(ad_account_id)
                insights = account.get_insights(fields=fields, params=params)
                
                # Convert to list - SDK handles pagination
                account_insights = [dict(insight) for insight in insights]
                all_insights.extend(account_insights)
                
                logger.info(f"  ✅ Got {len(account_insights)} insights")
                
            except FacebookRequestError as e:
                logger.error(f"  ❌ Facebook API error: {e}")
                continue
            except Exception as e:
                logger.error(f"  ❌ Error: {e}")
                continue
        
        logger.info(f"🎉 Total: {len(all_insights)} insights")
        return all_insights
    
    def _get_insights_date_range(self,
                                ad_account_ids: List[str],
                                time_range: Dict[str, str],
                                chunk_days: int,
                                delay_between_chunks: float,
                                fields: List[str]) -> List[dict]:
        """Fetch insights for a specific date range, split into chunks"""
        # Parse dates
        start_date = datetime.strptime(time_range['since'], '%Y-%m-%d')
        end_date = datetime.strptime(time_range['until'], '%Y-%m-%d')
        
        total_days = (end_date - start_date).days + 1
        
        logger.info(f"📅 Date range: {time_range['since']} to {time_range['until']} ({total_days} days)")
        logger.info(f"📦 Splitting into {chunk_days}-day chunks with 1-day overlap")
        
        all_insights = []
        chunk_count = 0
        current_start = start_date
        
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
            
            chunk_time_range = {
                'since': current_start.strftime('%Y-%m-%d'),
                'until': chunk_end.strftime('%Y-%m-%d')
            }
            
            chunk_count += 1
            logger.info(f"\n📦 Chunk {chunk_count}: {chunk_time_range['since']} to {chunk_time_range['until']}")
            
            for ad_account_id in ad_account_ids:
                logger.info(f"  Processing account {ad_account_id}...")
                
                try:
                    params = {
                        'time_range': chunk_time_range,
                        'level': 'ad',
                        'time_increment': 1,
                        'limit': 500,
                    }
                    
                    account = AdAccount(ad_account_id)
                    insights = account.get_insights(fields=fields, params=params)
                    
                    chunk_insights = [dict(insight) for insight in insights]
                    all_insights.extend(chunk_insights)
                    
                    logger.info(f"    ✅ Got {len(chunk_insights)} insights")
                    
                except FacebookRequestError as e:
                    logger.error(f"    ❌ Facebook API error: {e}")
                except Exception as e:
                    logger.error(f"    ❌ Error: {e}")
            
            current_start = current_start + timedelta(days=chunk_days - 1)
            
            if current_start <= end_date and delay_between_chunks > 0:
                logger.info(f"  ⏳ Waiting {delay_between_chunks}s before next chunk...")
                time.sleep(delay_between_chunks)
        
        logger.info(f"✅ Fetched {len(all_insights)} total insights")
        return all_insights
    
    def get_custom_conversions(self, ad_account_id: str) -> List[dict]:
        """Fetch custom conversions for a specific ad account
        
        Args:
            ad_account_id: Facebook ad account ID
            
        Returns:
            List of custom conversion mappings
        """
        # Ensure we have the 'act_' prefix for the API call
        if not ad_account_id.startswith('act_'):
            formatted_account_id = f"act_{ad_account_id}"
        else:
            formatted_account_id = ad_account_id
        
        url = f"https://graph.facebook.com/v18.0/{formatted_account_id}/customconversions"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name,custom_event_type'
        }
        
        try:
            import requests
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            custom_mappings = []
            for conversion in data.get('data', []):
                custom_mappings.append({
                    'user_friendly_name': conversion['name'],
                    'meta_action_type': f"offsite_conversion.custom.{conversion['id']}",
                    'mapping_type': 'custom',
                    'ad_account_id': ad_account_id.replace('act_', ''),
                    'facebook_conversion_id': conversion['id']
                })
            
            return custom_mappings
            
        except Exception as e:
            logger.error(f"Error fetching custom conversions for account {formatted_account_id}: {e}")
            return [] 