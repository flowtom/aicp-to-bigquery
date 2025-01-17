"""
Utility functions for processing budget data.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
import json
import re

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def safe_float_convert(value: Any) -> Optional[float]:
    """
    Safely convert a value to float, handling various formats:
    - Currency strings (e.g., '$1,234.56', '$0.00')
    - Percentage strings (e.g., '28%')
    - Numbers with commas (e.g., '1,234.56')
    - Regular numbers
    - Special values ('#N/A', '', None)
    """
    if value is None or value == '' or value == '#N/A':
        return None
        
    if isinstance(value, (int, float)):
        return float(value)
        
    if isinstance(value, str):
        # Remove currency symbols, commas, and whitespace
        cleaned = value.replace('$', '').replace(',', '').replace(' ', '')
        
        # Remove percentage sign if present
        cleaned = cleaned.replace('%', '')
        
        # Handle special case of '$0.00' or '0.00'
        if cleaned == '0.00' or cleaned == '0':
            return 0.0
            
        try:
            return float(cleaned)
        except ValueError:
            return None
            
    return None

def process_budget_row(row: List[Any], class_code: str, class_name: str, upload_id: str, timestamp: str) -> Optional[Dict[str, Any]]:
    """Process a single budget row into a standardized format"""
    try:
        # Basic validation
        if not row or len(row) < 5:
            return None
            
        # Skip header, empty, and total rows
        first_cell = str(row[0]).strip().lower()
        if first_cell in ['estimate', '', 'total a,c'] or not first_cell:
            return None
            
        # Skip rows without class information
        if not class_code or not class_name:
            print(f"Skipping row without class info: {row[0]}")
            return None
            
        # Extract values with proper type conversion using safe_float_convert
        line_item_number = int(row[0]) if row[0] and str(row[0]).isdigit() else None
        line_item_description = str(row[1]) if len(row) > 1 and row[1] else None
        estimate_days = safe_float_convert(row[3]) if len(row) > 3 else None
        estimate_rate = safe_float_convert(row[4]) if len(row) > 4 else None
        estimate_total = safe_float_convert(row[5]) if len(row) > 5 else None
        
        # Create standardized row
        return {
            'upload_id': upload_id,
            'budget_name': 'Test Budget',
            'upload_timestamp': timestamp,
            'class_code': class_code,
            'class_name': class_name,
            'line_item_number': line_item_number,
            'line_item_description': line_item_description,
            'estimate_days': estimate_days,
            'estimate_rate': estimate_rate,
            'estimate_total': estimate_total
        }
        
    except (ValueError, IndexError) as e:
        print(f"Error processing row: {str(e)}")
        return None

def validate_row(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a processed budget row.
    Returns (is_valid, errors).
    """
    errors = []
    required_fields = {
        'upload_id': str,
        'budget_name': str,
        'upload_timestamp': str,
        'class_code': str,
        'class_name': str,
        'line_item_number': int,
        'line_item_description': str
    }

    # Check required fields
    for field, expected_type in required_fields.items():
        if field not in row:
            errors.append(f"Missing required field: {field}")
        elif row[field] is None:
            errors.append(f"Required field cannot be NULL: {field}")
        elif not isinstance(row[field], expected_type):
            errors.append(f"Invalid type for {field}: expected {expected_type.__name__}, got {type(row[field]).__name__}")

    # Validate numeric fields
    numeric_fields = [
        'estimate_days', 'estimate_rate', 'estimate_total'
    ]
    for field in numeric_fields:
        if field in row and row[field] is not None:
            if not isinstance(row[field], (int, float)):
                errors.append(f"Invalid numeric value for {field}: {row[field]}")

    return len(errors) == 0, errors 