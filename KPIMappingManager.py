import logging
import pandas as pd
import pandas_gbq
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class KPIMappingManager:
    """Manages KPI event mappings between user-friendly names and Facebook action types"""
    
    def __init__(self, facebook_client, bigquery_client):
        """Initialize KPI mapping manager
        
        Args:
            facebook_client: Instance of FacebookClient for API calls
            bigquery_client: Instance of BigQueryClient for database operations
        """
        self.fb_client = facebook_client
        self.bq_client = bigquery_client
        self.mapping_table = "rollup_reference.kpi_event_mapping"
        self._mappings_cache = None
    
    def get_standard_mappings(self) -> List[Dict]:
        """Get standard event mappings that apply to all accounts
        
        Returns:
            List of standard mapping dictionaries
        """
        return [
            # Standard event mappings
            # {'user_friendly_name': 'Lead', 'meta_action_type': 'lead', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Video View', 'meta_action_type': 'video_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Purchase', 'meta_action_type': 'purchase', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Page View', 'meta_action_type': 'page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Link Click', 'meta_action_type': 'link_click', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Page Engagement', 'meta_action_type': 'page_engagement', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Post Engagement', 'meta_action_type': 'post_engagement', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Landing Page View', 'meta_action_type': 'landing_page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Post Reaction', 'meta_action_type': 'post_reaction', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Post Save', 'meta_action_type': 'post_save', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Web Lead', 'meta_action_type': 'web_lead', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Click On Platform', 'meta_action_type': 'click', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Click To Website', 'meta_action_type': 'link_click', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Lead Website', 'meta_action_type': 'onsite_web_lead', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Video View 3sec', 'meta_action_type': 'video_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'View Content', 'meta_action_type': 'view_content', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Add To Cart', 'meta_action_type': 'add_to_cart', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Initiate Checkout', 'meta_action_type': 'initiate_checkout', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Complete Registration', 'meta_action_type': 'complete_registration', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Omni Landing Page View', 'meta_action_type': 'omni_landing_page_view', 'mapping_type': 'standard', 'ad_account_id': 'all'},
            # Pixel event mappings  
            # {'user_friendly_name': 'Lead', 'meta_action_type': 'offsite_conversion.fb_pixel_lead', 'mapping_type': 'pixel', 'ad_account_id': 'all'},
            # {'user_friendly_name': 'Purchase', 'meta_action_type': 'offsite_conversion.fb_pixel_purchase', 'mapping_type': 'pixel', 'ad_account_id': 'all'},
            {'user_friendly_name': 'offsite_conversion.fb_pixel_custom', 'meta_action_type': 'offsite_conversion.fb_pixel_custom', 'mapping_type': 'pixel', 'ad_account_id': '1052847603363129'},
        ]
    
    def fetch_custom_mappings(self, ad_account_ids: List[str] = None) -> List[Dict]:
        """Fetch custom conversions from Facebook for specified ad accounts
        
        Args:
            ad_account_ids: List of ad account IDs (defaults to env var)
            
        Returns:
            List of custom conversion mappings
        """
        if ad_account_ids is None:
            ad_account_ids = self.fb_client.get_ad_account_ids()
        
        all_custom_mappings = []
        
        for account_id in ad_account_ids:
            logger.info(f"Fetching custom conversions for account: {account_id}")
            
            try:
                custom_mappings = self.fb_client.get_custom_conversions(account_id)
                all_custom_mappings.extend(custom_mappings)
                logger.info(f"Found {len(custom_mappings)} custom conversions for account {account_id}")
            except Exception as e:
                logger.error(f"Error fetching custom conversions for account {account_id}: {e}")
                continue
        
        logger.info(f"Total custom conversions found: {len(all_custom_mappings)}")
        return all_custom_mappings
    
    def update_mapping_table(self, include_custom: bool = True) -> Dict[str, int]:
        """Update the BigQuery mapping table with fresh data
        
        Args:
            include_custom: Whether to fetch and include custom conversions
            
        Returns:
            Dictionary with counts of mappings by type
        """
        logger.info("Updating KPI event mapping table...")
        
        # Start with standard mappings
        all_mappings = self.get_standard_mappings()
        standard_count = len(all_mappings)
        
        # Add custom conversions if requested
        custom_count = 0
        if include_custom:
            custom_mappings = self.fetch_custom_mappings()
            all_mappings.extend(custom_mappings)
            custom_count = len(custom_mappings)
        
        if not all_mappings:
            logger.warning("No mappings found to update!")
            return {'standard': 0, 'custom': 0, 'total': 0}
        
        # Update BigQuery table
        try:
            df = pd.DataFrame(all_mappings)
            
            # Add timestamp for tracking
            df['last_updated'] = pd.Timestamp.now()
            
            # Use pandas_gbq to update the table
            pandas_gbq.to_gbq(
                df,
                destination_table=self.mapping_table,
                project_id='ivc-media-ads-warehouse',
                if_exists='replace',
                credentials=self.bq_client.client._credentials
            )
            
            logger.info(f"Successfully updated mapping table with {len(all_mappings)} total mappings")
            
            # Print summary
            summary = df.groupby(['ad_account_id', 'mapping_type']).size().reset_index(name='count')
            logger.info("\nMapping summary:")
            logger.info(summary.to_string(index=False))
            
            # Clear cache after update
            self._mappings_cache = None
            
            return {
                'standard': standard_count,
                'custom': custom_count,
                'total': len(all_mappings)
            }
            
        except Exception as e:
            logger.error(f"Error updating BigQuery table: {e}")
            raise
    
    def get_mapping_for_account(self, ad_account_id: str, kpi_name: str) -> Optional[str]:
        """Get the Facebook action type for a given KPI name and account
        
        Args:
            ad_account_id: Facebook ad account ID
            kpi_name: User-friendly KPI name
            
        Returns:
            Facebook action type or None if not found
        """
        if self._mappings_cache is None:
            self._load_mappings()
        
        # First check for account-specific mapping
        account_key = f"{ad_account_id}:{kpi_name}"
        if account_key in self._mappings_cache:
            return self._mappings_cache[account_key]
        
        # Fall back to 'all' account mapping
        all_key = f"all:{kpi_name}"
        return self._mappings_cache.get(all_key)
    
    def _load_mappings(self):
        """Load mappings from BigQuery into cache"""
        try:
            query = f"""
            SELECT ad_account_id, user_friendly_name, meta_action_type
            FROM `{self.mapping_table}`
            """
            
            result = self.bq_client.client.query(query).result()
            
            self._mappings_cache = {}
            for row in result:
                key = f"{row.ad_account_id}:{row.user_friendly_name}"
                self._mappings_cache[key] = row.meta_action_type
            
            logger.info(f"Loaded {len(self._mappings_cache)} mappings into cache")
            
        except Exception as e:
            logger.error(f"Error loading mappings from BigQuery: {e}")
            self._mappings_cache = {}
    
    def get_all_mappings(self) -> pd.DataFrame:
        """Get all mappings as a DataFrame
        
        Returns:
            DataFrame with all KPI mappings
        """
        query = f"SELECT * FROM `{self.mapping_table}`"
        return pd.read_gbq(
            query,
            project_id=self.bq_client.project_id,
            credentials=self.bq_client.client._credentials
        ) 