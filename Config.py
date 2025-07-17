import os
import json
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class FacebookConfig:
    """Facebook API configuration"""
    access_token: str
    app_id: str
    app_secret: str
    ad_account_ids: List[str]
    
    @classmethod
    def from_env(cls) -> 'FacebookConfig':
        """Create FacebookConfig from environment variables"""
        access_token = os.getenv('FB_ACCESS_TOKEN')
        app_id = os.getenv('FB_APP_ID')
        app_secret = os.getenv('FB_APP_SECRET')
        ad_account_ids_json = os.getenv('FB_AD_ACCOUNT_ID')
        
        if not all([access_token, app_id, app_secret, ad_account_ids_json]):
            missing = []
            if not access_token: missing.append('FB_ACCESS_TOKEN')
            if not app_id: missing.append('FB_APP_ID')
            if not app_secret: missing.append('FB_APP_SECRET')
            if not ad_account_ids_json: missing.append('FB_AD_ACCOUNT_ID')
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        try:
            ad_account_ids = json.loads(ad_account_ids_json)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in FB_AD_ACCOUNT_ID: {ad_account_ids_json}")
        
        return cls(
            access_token=access_token,
            app_id=app_id,
            app_secret=app_secret,
            ad_account_ids=ad_account_ids
        )


@dataclass
class BigQueryConfig:
    """BigQuery configuration"""
    project_id: Optional[str]
    credentials_json: Optional[str]
    dataset_id: str = 'raw_ads'
    
    # Table names - can be fully qualified (dataset.table) or just table name
    meta_ads_table: str = 'meta_ads'
    ad_grouping_table: str = 'rollup_reference.ad_grouping'  # This table is in rollup_reference
    kpi_mapping_table: str = 'rollup_reference.kpi_event_mapping'  # This is also in rollup_reference
    test_meta_ads_table: str = 'meta_ads_test'  # Test table for comparing implementations
    
    @classmethod
    def from_env(cls) -> 'BigQueryConfig':
        """Create BigQueryConfig from environment variables"""
        credentials_json = os.getenv('GOOGLE_CREDENTIALS')
        project_id = os.getenv('GCP_PROJECT_ID')
        
        # Table overrides from env
        dataset_id = os.getenv('BQ_DATASET_ID', 'raw_ads')
        meta_ads_table = os.getenv('BQ_META_ADS_TABLE', 'meta_ads')
        ad_grouping_table = os.getenv('BQ_AD_GROUPING_TABLE', 'rollup_reference.ad_grouping')
        kpi_mapping_table = os.getenv('BQ_KPI_MAPPING_TABLE', 'rollup_reference.kpi_event_mapping')
        test_meta_ads_table = os.getenv('BQ_TEST_META_ADS_TABLE', 'meta_ads_test')
        
        return cls(
            project_id=project_id,
            credentials_json=credentials_json,
            dataset_id=dataset_id,
            meta_ads_table=meta_ads_table,
            ad_grouping_table=ad_grouping_table,
            kpi_mapping_table=kpi_mapping_table,
            test_meta_ads_table=test_meta_ads_table
        )
    
    def get_full_table_id(self, table_name: str) -> str:
        """Get fully qualified table ID"""
        if '.' in table_name:
            # Already fully qualified
            return table_name
        return f"{self.dataset_id}.{table_name}"


@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    batch_size: int = 1000
    chunk_days: int = 7
    delay_between_chunks: float = 0.2
    enable_validation: bool = False
    update_kpi_mappings: bool = True
    
    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        """Create PipelineConfig from environment variables"""
        return cls(
            batch_size=int(os.getenv('PIPELINE_BATCH_SIZE', '1000')),
            chunk_days=int(os.getenv('PIPELINE_CHUNK_DAYS', '7')),
            delay_between_chunks=float(os.getenv('PIPELINE_DELAY', '0.2')),
            enable_validation=os.getenv('PIPELINE_ENABLE_VALIDATION', 'true').lower() == 'true',
            update_kpi_mappings=os.getenv('PIPELINE_UPDATE_KPI_MAPPINGS', 'true').lower() == 'true'
        )


class Config:
    """Central configuration management for the entire pipeline"""
    
    def __init__(self,
                 facebook_config: FacebookConfig = None,
                 bigquery_config: BigQueryConfig = None,
                 pipeline_config: PipelineConfig = None):
        """Initialize configuration
        
        Args:
            facebook_config: Facebook API configuration
            bigquery_config: BigQuery configuration
            pipeline_config: Pipeline execution configuration
        """
        self.facebook = facebook_config or FacebookConfig.from_env()
        self.bigquery = bigquery_config or BigQueryConfig.from_env()
        self.pipeline = pipeline_config or PipelineConfig.from_env()
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create complete configuration from environment variables"""
        return cls(
            facebook_config=FacebookConfig.from_env(),
            bigquery_config=BigQueryConfig.from_env(),
            pipeline_config=PipelineConfig.from_env()
        )
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """Create configuration from dictionary"""
        facebook_dict = config_dict.get('facebook', {})
        bigquery_dict = config_dict.get('bigquery', {})
        pipeline_dict = config_dict.get('pipeline', {})
        
        return cls(
            facebook_config=FacebookConfig(**facebook_dict) if facebook_dict else None,
            bigquery_config=BigQueryConfig(**bigquery_dict) if bigquery_dict else None,
            pipeline_config=PipelineConfig(**pipeline_dict) if pipeline_dict else None
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'facebook': {
                'access_token': '***' if self.facebook.access_token else None,  # Redact sensitive data
                'app_id': self.facebook.app_id[-4:] if self.facebook.app_id else None,  # Show only tail
                'app_secret': '***' if self.facebook.app_secret else None,  # Redact sensitive data
                'ad_account_ids': self.facebook.ad_account_ids
            },
            'bigquery': {
                'project_id': self.bigquery.project_id,
                'dataset_id': self.bigquery.dataset_id,
                'meta_ads_table': self.bigquery.meta_ads_table,
                'ad_grouping_table': self.bigquery.ad_grouping_table,
                'kpi_mapping_table': self.bigquery.kpi_mapping_table
            },
            'pipeline': {
                'batch_size': self.pipeline.batch_size,
                'chunk_days': self.pipeline.chunk_days,
                'delay_between_chunks': self.pipeline.delay_between_chunks,
                'enable_validation': self.pipeline.enable_validation,
                'update_kpi_mappings': self.pipeline.update_kpi_mappings
            }
        }
    
    def log_config(self):
        """Log the current configuration (with sensitive data redacted)"""
        logger.info("Current configuration:")
        config_dict = self.to_dict()
        for section, values in config_dict.items():
            logger.info(f"\n{section.upper()}:")
            for key, value in values.items():
                logger.info(f"  {key}: {value}") 