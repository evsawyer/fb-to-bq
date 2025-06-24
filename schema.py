# schema.py

"""
Central schema definitions for all tables/fields.
This file is intended to be imported by validation and transformation modules.
"""

# Unified schema for all fields
SCHEMA = {
    # Float fields
    'spend': {'type': float, 'nullable': True},
    'cpc': {'type': float, 'nullable': True},
    'cpm': {'type': float, 'nullable': True},
    'cpp': {'type': float, 'nullable': True},
    'ctr': {'type': float, 'nullable': True},
    'frequency': {'type': float, 'nullable': True},
    'unique_ctr': {'type': float, 'nullable': True},
    'cost_per_unique_click': {'type': float, 'nullable': True},
    'inline_link_click_ctr': {'type': float, 'nullable': True},

    # Integer fields
    'impressions': {'type': int, 'nullable': True},
    'reach': {'type': int, 'nullable': True},
    'clicks': {'type': int, 'nullable': True},
    'unique_clicks': {'type': int, 'nullable': True},
    'inline_link_clicks': {'type': int, 'nullable': True},

    # Date fields (YYYY-MM-DD)
    'date_start': {'type': 'date', 'nullable': True},
    'date_stop': {'type': 'date', 'nullable': True},

    # Action fields with nested structure
    'website_ctr': {'type': float, 'nullable': True, 'nested': True},
    'actions': {'type': int, 'nullable': True, 'nested': True},
    'unique_actions': {'type': int, 'nullable': True, 'nested': True},
    'cost_per_action_type': {'type': float, 'nullable': True, 'nested': True},
    'cost_per_unique_action_type': {'type': float, 'nullable': True, 'nested': True},
    'video_play_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_avg_time_watched_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_p100_watched_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_p25_watched_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_p50_watched_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_p75_watched_actions': {'type': int, 'nullable': True, 'nested': True},
    'video_thruplay_watched_actions': {'type': int, 'nullable': True, 'nested': True},
}

# Helper lists for backward compatibility or quick access
FLOAT_FIELDS = [field for field, info in SCHEMA.items() if info['type'] == float and not info.get('nested')]
INT_FIELDS = [field for field, info in SCHEMA.items() if info['type'] == int and not info.get('nested')]
DATE_FIELDS = [field for field, info in SCHEMA.items() if info['type'] == 'date' and not info.get('nested')]
ACTION_FIELD_SCHEMAS = {field: info for field, info in SCHEMA.items() if info.get('nested') is True}
