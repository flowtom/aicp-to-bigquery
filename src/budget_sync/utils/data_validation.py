from datetime import datetime
from typing import Dict, Tuple, List, Union, Any

def safe_float_convert(value: Any) -> Union[float, None]:
    """Safely convert a value to float, returning None if conversion fails."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int_convert(value: Any) -> Union[int, None]:
    """Safely convert a value to integer, returning None if conversion fails."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def validate_budget_row(row_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates a single budget row before sending to BigQuery.
    Returns (is_valid, errors)
    """
    errors = []
    
    # Required fields with their expected types
    required_fields = {
        'upload_id': str,
        'user_email': str,
        'budget_name': str,
        'upload_timestamp': str,  # Will be converted to TIMESTAMP in BigQuery
        'class_code': str,
        'class_name': str,
        'line_item_number': int,
        'line_item_description': str
    }

    # Check required fields
    for field, expected_type in required_fields.items():
        if field not in row_data:
            errors.append(f"Missing required field: {field}")
            continue
            
        value = row_data[field]
        if value is None:
            errors.append(f"Required field cannot be NULL: {field}")
            continue

        # Special handling for integers
        if expected_type == int:
            if not isinstance(value, (int, float)) or not float(value).is_integer():
                errors.append(f"Invalid integer value for {field}: {value}")
        # Regular type checking for other types
        elif not isinstance(value, expected_type):
            errors.append(f"Invalid type for {field}: expected {expected_type.__name__}, got {type(value).__name__}")

    # Validate numeric fields (these can be NULL)
    numeric_fields = [
        'estimate_days', 'estimate_rate', 'estimate_total',
        'actual_days', 'actual_rate', 'actual_total'
    ]
    
    for field in numeric_fields:
        if field in row_data and row_data[field] is not None:
            value = row_data[field]
            if not isinstance(value, (int, float)):
                try:
                    float(value)  # Try to convert string to float
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric value for {field}: {value}")

    # Additional validation rules
    if 'class_code' in row_data and row_data['class_code']:
        if not row_data['class_code'].strip():
            errors.append("class_code cannot be empty string")

    if 'upload_timestamp' in row_data and row_data['upload_timestamp']:
        try:
            # Verify timestamp format
            datetime.fromisoformat(row_data['upload_timestamp'].replace('Z', '+00:00'))
        except (ValueError, TypeError):
            errors.append("Invalid upload_timestamp format. Expected ISO format")

    return len(errors) == 0, errors

def validate_budget_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validates a list of budget rows.
    Returns (valid_rows, error_rows)
    """
    valid_rows = []
    error_rows = []

    for i, row in enumerate(rows, 1):
        is_valid, errors = validate_budget_row(row)
        if is_valid:
            valid_rows.append(row)
        else:
            error_rows.append({
                'row_number': i,
                'data': row,
                'errors': errors
            })

    return valid_rows, error_rows 