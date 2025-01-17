from datetime import datetime
import json

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def safe_float_convert(value):
    """Safely convert value to float"""
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.replace(',', '').strip()
            if value.lower() in ['#n/a', 'n/a', '', 'total']:
                return None
            return float(value)
        return None
    except (ValueError, TypeError):
        return None

def process_budget_row(row, class_code, class_name, upload_id, timestamp):
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
        line_item_number = int(row[0]) if row[0] and row[0].isdigit() else None
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