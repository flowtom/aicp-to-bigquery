"""
Script to process budget data from Google Sheets and sync to BigQuery.
"""
import logging
import sys
from datetime import datetime, UTC, timedelta
from typing import Dict, Any, List, Optional, Tuple
import re
from src.budget_sync.services.budget_processor import BudgetProcessor
import os
from dotenv import load_dotenv
from pathlib import Path
import json

# Set up logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).parents[3] / '.env'
load_dotenv(env_path)

# Debug log environment variables
logger.info(f"Loading environment from: {env_path}")
logger.info(f"BIGQUERY_PROJECT_ID: {'Set' if os.getenv('BIGQUERY_PROJECT_ID') else 'Not Set'}")
logger.info(f"BIGQUERY_DATASET_ID: {'Set' if os.getenv('BIGQUERY_DATASET_ID') else 'Not Set'}")

# Configuration via environment variables for budget processing
BUDGET_SPREADSHEET_ID = os.environ.get('BUDGET_SPREADSHEET_ID', 'your-default-spreadsheet-id')
BUDGET_SHEET_GID = os.environ.get('BUDGET_SHEET_GID', 'your-default-sheet-gid')

# Debug: Print loaded configuration (remove or disable in production)
print(f"BUDGET_SPREADSHEET_ID: {BUDGET_SPREADSHEET_ID}")
print(f"BUDGET_SHEET_GID: {BUDGET_SHEET_GID}")

def parse_google_sheets_url(url: str) -> Dict[str, str]:
    """Extract spreadsheet ID and GID from Google Sheets URL."""
    logger.info(f"Parsed URL: {url}")
    
    # Extract spreadsheet ID
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        raise ValueError("Invalid Google Sheets URL format")
    spreadsheet_id = match.group(1)
    logger.info(f"Extracted spreadsheet_id: {spreadsheet_id}")
    
    # Extract GID
    match = re.search(r'[#&]gid=(\d+)', url)
    if not match:
        raise ValueError("No GID found in URL")
    gid = match.group(1)
    logger.info(f"Extracted GID: {gid}")
    
    return {
        'spreadsheet_id': spreadsheet_id,
        'gid': gid,
        'description': f"Budget from spreadsheet {spreadsheet_id}"
    }

def clean_money_value(value: Any) -> float:
    """Convert a monetary value (string or number) to float.
    
    Handles:
    - Dollar signs ($)
    - Commas in numbers
    - Percentage signs (%)
    - None values
    - Empty strings
    """
    if value is None or value == '':
        return 0.0
    
    if isinstance(value, (int, float)):
        return float(value)
    
    # Convert string to float
    try:
        # Remove $ and % signs
        value = str(value).replace('$', '').replace('%', '')
        # Remove commas
        value = value.replace(',', '')
        # Convert to float
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert value '{value}' to float, using 0.0")
        return 0.0

def clean_date_value(value: Any) -> str:
    """Convert a date value to YYYY-MM-DD format with fallback to today.
    
    Args:
        value: Date value to clean (string or datetime)
    
    Returns:
        str: Date in YYYY-MM-DD format
    """
    if not value:
        return datetime.now().date().isoformat()
    
    try:
        # If it's already a datetime
        if isinstance(value, datetime):
            return value.date().isoformat()
            
        # Try parsing common date formats
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y', '%Y/%m/%d']:
            try:
                return datetime.strptime(str(value), fmt).date().isoformat()
            except ValueError:
                continue
                
        # If no format matches, log warning and return today
        logger.warning(f"Could not parse date value '{value}', using today's date")
        return datetime.now().date().isoformat()
        
    except Exception as e:
        logger.warning(f"Error processing date value '{value}': {str(e)}, using today's date")
        return datetime.now().date().isoformat()

def safe_int_convert(value: Any, default: int = 0) -> int:
    """Safely convert a value to integer with fallback to default."""
    if value is None or value == '':
        return default
    try:
        if isinstance(value, str):
            # Remove any non-numeric characters (except negative sign)
            cleaned = ''.join(c for c in value if c.isdigit() or c == '-')
            return int(cleaned) if cleaned else default
        return int(value)
    except (ValueError, TypeError):
        return default

def process_budgets():
    """Process budgets from the Google Sheets using configuration from environment variables."""
    print(f"Processing budget using Spreadsheet ID: {BUDGET_SPREADSHEET_ID} and Sheet GID: {BUDGET_SHEET_GID}")
    try:
        processor = BudgetProcessor(spreadsheet_id=BUDGET_SPREADSHEET_ID, gid=BUDGET_SHEET_GID)
        raw_data = processor.fetch_raw_data()
        print("Raw data retrieved:", raw_data)
        return raw_data
    except Exception as e:
        print(f"Error processing budget: {str(e)}")
        return None

def main():
    """Main entry point."""
    try:
        # Get URL from command line argument
        if len(sys.argv) < 2:
            logger.error("❗ Error: Please provide a Google Sheets URL as an argument")
            sys.exit(1)
            
        url = sys.argv[1]
        
        # Parse URL to get spreadsheet ID and GID
        try:
            budget_info = parse_google_sheets_url(url)
        except ValueError as e:
            logger.error(f"❗ Error: {str(e)}")
            sys.exit(1)
            
        # Initialize budget processor
        processor = BudgetProcessor(
            spreadsheet_id=budget_info['spreadsheet_id'],
            gid=budget_info['gid']
        )
        
        logger.info(f"Starting budget processing at {datetime.now(UTC).isoformat()}")
        process_budgets()
    except Exception as e:
        logger.error(f"❗ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 