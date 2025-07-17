import logging
import datetime
from typing import List, Dict, Tuple, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataValidator:
    """Handles data validation and transformation for BigQuery compatibility"""
    
    def __init__(self, schema: Dict[str, Dict[str, Any]]):
        """Initialize validator with schema definition
        
        Args:
            schema: Dictionary defining field types and constraints
                   Format: {field_name: {'type': type, 'nullable': bool, 'nested': bool}}
        """
        self.schema = schema
        self._build_field_lists()
    
    def _build_field_lists(self):
        """Build categorized field lists from schema"""
        self.float_fields = [
            field for field, info in self.schema.items() 
            if info['type'] == float and not info.get('nested')
        ]
        self.int_fields = [
            field for field, info in self.schema.items() 
            if info['type'] == int and not info.get('nested')
        ]
        self.date_fields = [
            field for field, info in self.schema.items() 
            if info['type'] == 'date' and not info.get('nested')
        ]
        self.action_fields = {
            field: info for field, info in self.schema.items() 
            if info.get('nested') is True
        }
    
    def validate_record(self, record: dict, verbose: bool = False) -> Tuple[bool, List[str]]:
        """Validate a single record against the schema
        
        Args:
            record: Record to validate
            verbose: Whether to log validation issues
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Validate regular (non-nested) fields
        for field, info in self.schema.items():
            if info.get('nested'):
                continue
                
            if field in record and record[field] is not None:
                try:
                    if info['type'] == float:
                        float(str(record[field]))
                    elif info['type'] == int:
                        int(str(record[field]))
                    elif info['type'] == 'date':
                        datetime.datetime.strptime(str(record[field]), '%Y-%m-%d')
                except ValueError as e:
                    issues.append(f"Invalid {info['type'].__name__ if hasattr(info['type'], '__name__') else info['type']} value in {field}: {record[field]}")
        
        # Validate nested action fields
        for field, info in self.action_fields.items():
            if field in record and record[field]:
                if not isinstance(record[field], list):
                    issues.append(f"{field} is not a list: {type(record[field])}")
                else:
                    for idx, action in enumerate(record[field]):
                        if not isinstance(action, dict):
                            issues.append(f"Item {idx} in {field} is not a dictionary: {type(action)}")
                        elif not all(k in action for k in ['action_type', 'value']):
                            issues.append(f"Item {idx} in {field} missing required keys: {action}")
        
        if verbose and issues:
            logger.warning(f"Validation issues for record {record.get('ad_id')} on {record.get('date_start')}: {issues}")
        
        return len(issues) == 0, issues
    
    def transform_for_bigquery(self, record: dict) -> Optional[dict]:
        """Transform a record into BigQuery-compatible format
        
        Args:
            record: Record to transform
            
        Returns:
            Transformed record or None if transformation fails
        """
        try:
            prepared = dict(record)
            
            # Handle action fields with specific type requirements
            for field_name, field_info in self.action_fields.items():
                if field_name in prepared:
                    prepared[field_name] = self._validate_and_convert_action_field(
                        field_name, 
                        prepared[field_name],
                        field_info
                    )
            
            # Convert float fields
            for field in self.float_fields:
                if field in prepared and prepared[field] is not None:
                    prepared[field] = float(str(prepared[field]))
            
            # Convert integer fields
            for field in self.int_fields:
                if field in prepared and prepared[field] is not None:
                    prepared[field] = int(str(prepared[field]))
            
            # Ensure date fields are properly formatted
            for field in self.date_fields:
                if field in prepared and prepared[field] is not None:
                    # Validate date format
                    datetime.datetime.strptime(str(prepared[field]), '%Y-%m-%d')
                    prepared[field] = str(prepared[field])
            
            return prepared
            
        except Exception as e:
            logger.error(f"Error transforming record: {str(e)}")
            return None
    
    def _validate_and_convert_action_field(self, 
                                          field_name: str, 
                                          actions: list,
                                          field_info: dict) -> Optional[list]:
        """Validate and convert a specific action field"""
        if not actions:
            return None
            
        if not isinstance(actions, list):
            raise ValueError(f"{field_name} must be a list, got {type(actions)}")
        
        converted_actions = []
        
        for idx, action in enumerate(actions):
            if not isinstance(action, dict):
                raise ValueError(f"Item {idx} in {field_name} must be a dictionary, got {type(action)}")
                
            if not all(k in action for k in ['action_type', 'value']):
                raise ValueError(f"Item {idx} in {field_name} missing required keys: {action}")
            
            try:
                value = action['value']
                if value is None and field_info.get('nullable', True):
                    converted_value = None
                else:
                    # Convert to the appropriate type
                    target_type = field_info['type']
                    converted_value = target_type(str(value))
                    
                converted_actions.append({
                    'action_type': str(action['action_type']),
                    'value': converted_value
                })
            except (ValueError, TypeError) as e:
                raise ValueError(f"Failed to convert value in {field_name}[{idx}]: {str(e)}")
        
        return converted_actions
    
    def validate_batch(self, 
                      records: List[dict], 
                      stop_on_first_error: bool = False) -> Dict[str, List[dict]]:
        """Validate and categorize a batch of records
        
        Args:
            records: List of records to validate
            stop_on_first_error: Whether to stop validation on first error
            
        Returns:
            Dictionary with 'valid' and 'invalid' lists of records
        """
        valid_records = []
        invalid_records = []
        
        for record in records:
            is_valid, issues = self.validate_record(record)
            
            if is_valid:
                # Transform the record for BigQuery
                transformed = self.transform_for_bigquery(record)
                if transformed:
                    valid_records.append(transformed)
                else:
                    invalid_records.append({
                        'record': record,
                        'issues': ['Transformation failed']
                    })
            else:
                invalid_records.append({
                    'record': record,
                    'issues': issues
                })
                
                if stop_on_first_error:
                    break
        
        logger.info(f"Batch validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")
        
        return {
            'valid': valid_records,
            'invalid': invalid_records
        }
    
    def analyze_data_structure(self, records: List[dict]) -> Dict[str, Any]:
        """Analyze the structure of records to identify potential issues
        
        Args:
            records: List of records to analyze
            
        Returns:
            Dictionary with field type analysis and nested field identification
        """
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
        
        for record in records:
            for key, value in record.items():
                analyze_value(key, value)
        
        return {
            'field_types': {k: list(v) for k, v in field_types.items()},
            'nested_fields': list(nested_fields),
            'total_records': len(records),
            'sample_record_keys': list(records[0].keys()) if records else []
        } 