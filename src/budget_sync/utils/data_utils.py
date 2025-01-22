"""
Utility functions for processing budget data.
"""
from datetime import datetime
import json
from typing import Any, Dict, List, Optional, Union

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def safe_float_convert(value: Any) -> Optional[float]:
    """
    Safely convert a value to float, handling various formats:
    - Currency strings (e.g., '$1,234.56')
    - Numbers with commas (e.g., '1,234.56')
    - Percentage strings (e.g., '28%')
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

def process_budget_row(
    row: List[Any],
    class_code: str,
    class_name: str,
    upload_id: str,
    timestamp: str,
    budget_name: str = "Test Budget"
) -> Optional[Dict[str, Any]]:
    """
    Process a single budget row into a standardized format.
    
    Args:
        row: Raw row data from spreadsheet
        class_code: Budget class code (e.g., 'A', 'B', etc.)
        class_name: Budget class name
        upload_id: Unique identifier for this upload
        timestamp: Upload timestamp
        budget_name: Name of the budget
        
    Returns:
        Processed row as dictionary or None if row should be skipped
    """
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
            
        # Extract values with proper type conversion
        line_item_number = int(row[0]) if row[0] and str(row[0]).isdigit() else None
        line_item_description = str(row[1]) if len(row) > 1 and row[1] else None
        
        # Process estimate fields
        estimate_days = safe_float_convert(row[3]) if len(row) > 3 else None
        estimate_rate = safe_float_convert(row[4]) if len(row) > 4 else None
        estimate_total = safe_float_convert(row[5]) if len(row) > 5 else None
        
        # Process actual fields (if present)
        actual_days = safe_float_convert(row[6]) if len(row) > 6 else None
        actual_rate = safe_float_convert(row[7]) if len(row) > 7 else None
        actual_total = safe_float_convert(row[8]) if len(row) > 8 else None
        
        # Create standardized row
        return {
            'upload_id': upload_id,
            'budget_name': budget_name,
            'upload_timestamp': timestamp,
            'class_code': class_code,
            'class_name': class_name,
            'line_item_number': line_item_number,
            'line_item_description': line_item_description,
            'estimate_days': estimate_days,
            'estimate_rate': estimate_rate,
            'estimate_total': estimate_total,
            'actual_days': actual_days,
            'actual_rate': actual_rate,
            'actual_total': actual_total
        }
        
    except Exception as e:
        print(f"Error processing row: {str(e)}")
        return None

def extract_class_info(header: str) -> Optional[tuple[str, str]]:
    """
    Extract class code and name from a header row.
    Example: 'A: PREPRODUCTION & WRAP CREW' -> ('A', 'PREPRODUCTION & WRAP CREW')
    
    Args:
        header: Header string from spreadsheet
        
    Returns:
        Tuple of (class_code, class_name) or None if invalid format
    """
    try:
        if not header or ':' not in header:
            return None
            
        parts = header.split(':', 1)
        if len(parts) != 2:
            return None
            
        code = parts[0].strip()
        name = parts[1].strip()
        
        if not code or not name:
            return None
            
        return code, name
        
    except Exception as e:
        print(f"Error extracting class info: {str(e)}")
        return None 