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
    """Validate a budget row"""
    errors = []
    
    # Basic validation
    if not row_data:
        return False, ["Empty row data"]
        
    # Required fields
    if 'line_item_number' not in row_data or row_data['line_item_number'] is None:
        errors.append("Required field cannot be NULL: line_item_number")
            
    # Validate numeric fields
    numeric_fields = [
        'estimate_days', 'estimate_rate', 'estimate_total',
        'actual_days', 'actual_rate', 'actual_total'
    ]
    
    for field in numeric_fields:
        if field in row_data and row_data[field] is not None:
            value = row_data[field]
            if not isinstance(value, (int, float)):
                try:
                    float(value)
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric value for {field}: {value}")
                    
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