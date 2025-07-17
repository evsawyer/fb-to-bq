from typing import Dict, Any, Type, Optional


class FieldSchema:
    """Enhanced field schema with validation and metadata"""
    
    def __init__(self, 
                 field_type: Type,
                 nullable: bool = True,
                 nested: bool = False,
                 description: str = "",
                 default_value: Any = None):
        """Initialize field schema
        
        Args:
            field_type: Python type or 'date' for date fields
            nullable: Whether the field can be None
            nested: Whether this is a nested field (like actions)
            description: Field description for documentation
            default_value: Default value if field is missing
        """
        self.type = field_type
        self.nullable = nullable
        self.nested = nested
        self.description = description
        self.default_value = default_value
    
    def validate(self, value: Any) -> bool:
        """Validate a value against this field schema"""
        if value is None:
            return self.nullable
        
        if self.type == 'date':
            # Date validation is handled separately
            return isinstance(value, str)
        elif self.nested:
            # Nested fields should be lists
            return isinstance(value, list)
        else:
            # For regular types, check instance
            return isinstance(value, self.type)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for compatibility"""
        return {
            'type': self.type,
            'nullable': self.nullable,
            'nested': self.nested,
            'description': self.description,
            'default_value': self.default_value
        }


class SchemaRegistry:
    """Central registry for all data schemas"""
    
    # Facebook Insights Schema
    INSIGHTS_SCHEMA = {
        # Float fields
        'spend': FieldSchema(float, description="Ad spend amount"),
        'cpc': FieldSchema(float, description="Cost per click"),
        'cpm': FieldSchema(float, description="Cost per thousand impressions"),
        'cpp': FieldSchema(float, description="Cost per purchase"),
        'ctr': FieldSchema(float, description="Click-through rate"),
        'frequency': FieldSchema(float, description="Average frequency"),
        'unique_ctr': FieldSchema(float, description="Unique click-through rate"),
        'cost_per_unique_click': FieldSchema(float, description="Cost per unique click"),
        'inline_link_click_ctr': FieldSchema(float, description="Inline link click CTR"),
        
        # Integer fields
        'impressions': FieldSchema(int, description="Total impressions"),
        'reach': FieldSchema(int, description="Unique reach"),
        'clicks': FieldSchema(int, description="Total clicks"),
        'unique_clicks': FieldSchema(int, description="Unique clicks"),
        'inline_link_clicks': FieldSchema(int, description="Inline link clicks"),
        
        # Date fields
        'date_start': FieldSchema('date', description="Start date (YYYY-MM-DD)"),
        'date_stop': FieldSchema('date', description="End date (YYYY-MM-DD)"),
        
        # String fields
        'account_id': FieldSchema(str, description="Facebook account ID"),
        'account_name': FieldSchema(str, description="Facebook account name"),
        'account_currency': FieldSchema(str, description="Account currency"),
        'ad_id': FieldSchema(str, description="Facebook ad ID"),
        'ad_name': FieldSchema(str, description="Ad name"),
        'adset_id': FieldSchema(str, description="Ad set ID"),
        'adset_name': FieldSchema(str, description="Ad set name"),
        'campaign_id': FieldSchema(str, description="Campaign ID"),
        'campaign_name': FieldSchema(str, description="Campaign name"),
        'quality_ranking': FieldSchema(str, description="Ad quality ranking"),
        'engagement_rate_ranking': FieldSchema(str, description="Engagement rate ranking"),
        'conversion_rate_ranking': FieldSchema(str, description="Conversion rate ranking"),
        'objective': FieldSchema(str, description="Campaign objective"),
        'optimization_goal': FieldSchema(str, description="Optimization goal"),
        
        # Nested action fields
        'website_ctr': FieldSchema(float, nested=True, description="Website CTR by action type"),
        'actions': FieldSchema(int, nested=True, description="Actions by type"),
        'unique_actions': FieldSchema(int, nested=True, description="Unique actions by type"),
        'cost_per_action_type': FieldSchema(float, nested=True, description="Cost per action type"),
        'cost_per_unique_action_type': FieldSchema(float, nested=True, description="Cost per unique action"),
        'video_play_actions': FieldSchema(int, nested=True, description="Video play actions"),
        'video_avg_time_watched_actions': FieldSchema(int, nested=True, description="Avg video watch time"),
        'video_p100_watched_actions': FieldSchema(int, nested=True, description="100% video views"),
        'video_p25_watched_actions': FieldSchema(int, nested=True, description="25% video views"),
        'video_p50_watched_actions': FieldSchema(int, nested=True, description="50% video views"),
        'video_p75_watched_actions': FieldSchema(int, nested=True, description="75% video views"),
        'video_thruplay_watched_actions': FieldSchema(int, nested=True, description="Video thruplay views"),
    }
    
    # KPI Event Mapping Schema
    KPI_MAPPING_SCHEMA = {
        'user_friendly_name': FieldSchema(str, nullable=False, description="Human-readable KPI name"),
        'meta_action_type': FieldSchema(str, nullable=False, description="Facebook action type"),
        'mapping_type': FieldSchema(str, description="Type: standard, custom, or pixel"),
        'ad_account_id': FieldSchema(str, description="Ad account ID or 'all'"),
        'facebook_conversion_id': FieldSchema(str, description="Facebook conversion ID"),
        'last_updated': FieldSchema('date', description="Last update timestamp")
    }
    
    @classmethod
    def get_schema(cls, schema_name: str) -> Dict[str, FieldSchema]:
        """Get a specific schema by name"""
        schemas = {
            'insights': cls.INSIGHTS_SCHEMA,
            'kpi_mapping': cls.KPI_MAPPING_SCHEMA
        }
        
        if schema_name not in schemas:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        return schemas[schema_name]
    
    @classmethod
    def get_schema_dict(cls, schema_name: str) -> Dict[str, Dict[str, Any]]:
        """Get schema in dictionary format for backward compatibility"""
        schema = cls.get_schema(schema_name)
        return {field: info.to_dict() for field, info in schema.items()}
    
    @classmethod
    def get_field_lists(cls, schema_name: str) -> Dict[str, list]:
        """Get categorized field lists for a schema"""
        schema = cls.get_schema(schema_name)
        
        return {
            'float_fields': [
                field for field, info in schema.items() 
                if info.type == float and not info.nested
            ],
            'int_fields': [
                field for field, info in schema.items() 
                if info.type == int and not info.nested
            ],
            'date_fields': [
                field for field, info in schema.items() 
                if info.type == 'date' and not info.nested
            ],
            'string_fields': [
                field for field, info in schema.items() 
                if info.type == str and not info.nested
            ],
            'nested_fields': [
                field for field, info in schema.items() 
                if info.nested
            ]
        } 