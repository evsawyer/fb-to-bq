from collections import defaultdict
import datetime
from schema import ACTION_FIELD_SCHEMAS, FLOAT_FIELDS, INT_FIELDS, SCHEMA

def analyze_insights_structure(insights_data):
    """Analyze the structure of insights data to identify potential BigQuery compatibility issues"""
    field_types = defaultdict(set)
    nested_fields = set()
    
    def analyze_value(key, value):
        if isinstance(value, list) and value and isinstance(value[0], dict):
            nested_fields.add(key)
            for item in value:
                if 'action_type' in item and 'value' in item:
                    field_types[f"{key}_values"].add(type(item['value']).__name__)
        else:
            field_types[key].add(type(value).__name__)

    for insight in insights_data:
        for key, value in insight.items():
            analyze_value(key, value)
    
    return {
        'field_types': {k: list(v) for k, v in field_types.items()},
        'nested_fields': list(nested_fields)
    }

def validate_and_convert_action_field(field_name: str, actions: list) -> list:
    """Validate and convert a specific action field according to its schema requirements"""
    if not actions:
        return None
        
    if not isinstance(actions, list):
        raise ValueError(f"{field_name} must be a list, got {type(actions)}")
        
    schema = ACTION_FIELD_SCHEMAS[field_name]
    converted_actions = []
    
    for idx, action in enumerate(actions):
        if not isinstance(action, dict):
            raise ValueError(f"Item {idx} in {field_name} must be a dictionary, got {type(action)}")
            
        if not all(k in action for k in ['action_type', 'value']):
            raise ValueError(f"Item {idx} in {field_name} missing required keys: {action}")
            
        try:
            value = action['value']
            if value is None and schema['nullable']:
                converted_value = None
            else:
                converted_value = schema['type'](str(value))
                
            converted_actions.append({
                'action_type': str(action['action_type']),
                'value': converted_value
            })
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to convert value in {field_name}[{idx}]: {str(e)}")
            
    return converted_actions

def validate_insight(insight, verbose=True):
    """Validate a single insight record and show potential issues"""
    issues = []
    
    # First validate regular (non-nested) fields
    for field, info in SCHEMA.items():
        # Skip nested action fields - they have their own validation
        if info.get('nested'):
            continue
            
        if field in insight:
            if info['type'] == float:
                try:
                    float(str(insight[field]))
                except ValueError:
                    issues.append(f"Invalid float value in {field}: {insight[field]}")
            elif info['type'] == int:
                try:
                    int(str(insight[field]))
                except ValueError:
                    issues.append(f"Invalid integer value in {field}: {insight[field]}")
            elif info['type'] == 'date':
                try:
                    datetime.datetime.strptime(str(insight[field]), '%Y-%m-%d')
                except ValueError:
                    issues.append(f"Invalid date format in {field}: {insight[field]}")
    
    # Then validate nested action fields separately
    for field, info in ACTION_FIELD_SCHEMAS.items():
        if field in insight and insight[field]:
            if not isinstance(insight[field], list):
                issues.append(f"{field} is not a list: {type(insight[field])}")
            else:
                for idx, action in enumerate(insight[field]):
                    if not isinstance(action, dict):
                        issues.append(f"Item {idx} in {field} is not a dictionary: {type(action)}")
                    elif not all(k in action for k in ['action_type', 'value']):
                        issues.append(f"Item {idx} in {field} missing required keys: {action}")
    
    if verbose:
        if issues:
            print(f"\nIssues found in insight {insight.get('ad_id')} for date {insight.get('date_start')}:")
            for issue in issues:
                print(f"- {issue}")
        else:
            print(f"✓ Insight {insight.get('ad_id')} for date {insight.get('date_start')} looks valid")
    
    return len(issues) == 0

def prepare_for_bigquery(insight):
    """Transform a single insight into BigQuery-compatible format"""
    prepared = dict(insight)
    
    # Handle action fields with specific type requirements
    for field_name in ACTION_FIELD_SCHEMAS.keys():
        try:
            if field_name in prepared:
                prepared[field_name] = validate_and_convert_action_field(
                    field_name, 
                    prepared[field_name]
                )
        except ValueError as e:
            print(f"Error processing {field_name}: {str(e)}")
            return None
    
    # Convert float fields
    for field in FLOAT_FIELDS:
        if field in prepared and prepared[field] is not None:
            prepared[field] = float(str(prepared[field]))
    
    # Convert integer fields
    for field in INT_FIELDS:
        if field in prepared and prepared[field] is not None:
            prepared[field] = int(str(prepared[field]))

    return prepared
