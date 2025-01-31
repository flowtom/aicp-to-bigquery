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
    """Process budgets from provided URLs."""
    # Initialize counters
    total_budgets = 0
    successful_budgets = 0
    failed_budgets = []
    
    try:
        # Ensure URL is provided
        if len(sys.argv) < 2:
            raise ValueError("Please provide a Google Sheets URL as an argument")
        
        url = sys.argv[1]
        total_budgets += 1
        
        try:
            # Parse URL
            budget_info = parse_google_sheets_url(url)
            
            # Process budget
            processor = BudgetProcessor(
                spreadsheet_id=budget_info['spreadsheet_id'],
                gid=budget_info['gid']
            )
            
            # Get sheet info first
            sheet_info = processor._get_sheet_info(budget_info['spreadsheet_id'], budget_info['gid'])
            budget = processor.process_budget()
            
            # Add detailed debug logging
            logger.info("Budget processing complete")
            logger.debug("Full budget structure:")
            logger.debug(f"Keys in budget: {budget.keys() if budget else 'No budget'}")
            logger.debug(f"Project Summary: {budget.get('project_summary') if budget else 'No project summary'}")
            
            if budget:
                # Get project_name from the processed budget and budget_name from sheet_info
                project_name = budget.get('project_summary', {}).get('project_info', {}).get('project_name')
                budget_name = sheet_info['title']  # Use sheet title directly

                logger.debug(f"Found project_name: {project_name}")
                logger.debug(f"Found budget_name: {budget_name}")

                if not project_name and not budget_name:
                    raise ValueError("Both project_name and budget_name are missing")

                # Use project_name or budget_name directly
                project_title = project_name if project_name else budget_name
                
                # Ensure budget_name is set correctly
                if not budget_name:
                    raise ValueError("Budget name is missing")
                
                # Generate upload_id using sheet info
                upload_id = processor._generate_upload_id(
                    spreadsheet_title=sheet_info['spreadsheet_title'],
                    sheet_title=sheet_info['title']
                )
                
                # Prepare budget data for BigQuery with all required fields
                budget_data = {
                    # Required fields
                    'budget_id': upload_id,
                    'project_id': budget_info['spreadsheet_id'],
                    'upload_timestamp': datetime.now(UTC).isoformat(),
                    'version_hash': str(hash(str(budget.get('classes', {})))),
                    'version_status': budget.get('version_status', 'draft'),
                    'spreadsheet_id': budget_info['spreadsheet_id'],
                    'sheet_name': budget_name,
                    'sheet_gid': budget_info['gid'],
                    'project_title': project_title,
                    'total_days': int(budget.get('project_summary', {}).get('timeline', {}).get('total_days', 0)),
                    'firm_bid_total_estimate': clean_money_value(budget.get('financials', {}).get('firm_bid', {}).get('estimated')),
                    'cost_plus_total_estimate': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('estimated')),
                    'grand_total_estimate': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('estimated')),
                    
                    # Optional fields
                    'version_notes': '',
                    'user_email': os.getenv('USER_EMAIL', 'user@example.com'),
                    'production_company': budget.get('project_summary', {}).get('project_info', {}).get('production_company'),
                    'contact_phone': budget.get('project_summary', {}).get('project_info', {}).get('contact_phone'),
                    'project_date': clean_date_value(budget.get('project_summary', {}).get('project_info', {}).get('date')),
                    'director': budget.get('project_summary', {}).get('core_team', {}).get('director'),
                    'producer': budget.get('project_summary', {}).get('core_team', {}).get('producer'),
                    'writer': budget.get('project_summary', {}).get('core_team', {}).get('writer'),
                    'pre_prod_days': int(budget.get('project_summary', {}).get('timeline', {}).get('pre_prod_days', 0)),
                    'build_days': int(budget.get('project_summary', {}).get('timeline', {}).get('build_days', 0)),
                    'pre_light_days': int(budget.get('project_summary', {}).get('timeline', {}).get('pre_light_days', 0)),
                    'studio_days': int(budget.get('project_summary', {}).get('timeline', {}).get('studio_days', 0)),
                    'location_days': int(budget.get('project_summary', {}).get('timeline', {}).get('location_days', 0)),
                    'wrap_days': int(budget.get('project_summary', {}).get('timeline', {}).get('wrap_days', 0)),
                    'firm_bid_total_actual': clean_money_value(budget.get('financials', {}).get('firm_bid', {}).get('actual')),
                    'cost_plus_total_actual': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('actual')),
                    'grand_total_actual': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('actual')),
                    'grand_total_variance': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('variance')),
                    'client_total_actual': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('client_actual')),
                    'client_total_variance': clean_money_value(budget.get('financials', {}).get('grand_total', {}).get('client_variance'))
                }
                
                # Add debug logging before upload
                logger.info("Preparing to upload budget data to BigQuery:")
                logger.info(f"Budget data structure: {json.dumps(budget_data, indent=2)}")
                
                # Prepare budget details data
                detail_rows = []
                classes = budget.get('classes', {})
                if not classes:
                    logger.warning("No classes data found in budget")
                
                logger.info(f"Processing {len(classes)} budget classes for upload")
                for class_code, budget_class in classes.items():
                    logger.info(f"Processing class {class_code} for upload")
                    logger.debug(f"Class data type: {type(budget_class)}")
                    
                    # Convert BudgetClass to dict if needed
                    if hasattr(budget_class, 'line_items'):
                        logger.info(f"Converting BudgetClass object to dictionary for {class_code}")
                        line_items = budget_class.line_items
                    else:
                        logger.info(f"Using existing line items for {class_code}")
                        line_items = budget_class.get('line_items', [])
                    
                    logger.info(f"Found {len(line_items)} line items in class {class_code}")
                    
                    for line_item in line_items:
                        if not isinstance(line_item, dict):
                            logger.warning(f"Skipping invalid line item in class {class_code}")
                            continue
                            
                        # Debug log raw values
                        logger.debug(f"Processing line item in class {class_code}:")
                        logger.debug(f"  Raw estimate_days: {line_item.get('estimate_days')}")
                        logger.debug(f"  Raw actual_days: {line_item.get('actual_days')}")
                        logger.debug(f"  Raw estimate_rate: {line_item.get('estimate_rate')}")
                        logger.debug(f"  Raw actual_rate: {line_item.get('actual_rate')}")
                            
                        detail_row = {
                            # Required fields
                            'budget_id': upload_id,
                            'project_id': budget_info['spreadsheet_id'],
                            'line_item_id': f"{upload_id}_{class_code}_{line_item.get('line_item_number', '')}",
                            'upload_timestamp': datetime.now(UTC).isoformat(),
                            'created_at': datetime.now(UTC).isoformat(),
                            'class_code': class_code,
                            'class_name': budget_class.class_name if hasattr(budget_class, 'class_name') else budget_class.get('class_name', ''),
                            'line_item_number': line_item.get('line_item_number', ''),
                            'line_item_description': line_item.get('line_item_description', ''),
                            'is_subtotal': False,
                            
                            # Nullable fields
                            'estimate_days': safe_int_convert(line_item.get('estimate_days')) if line_item.get('estimate_days') else None,
                            'estimate_rate': clean_money_value(line_item.get('estimate_rate')) if line_item.get('estimate_rate') else None,
                            'estimate_total': clean_money_value(line_item.get('estimate_total')) if line_item.get('estimate_total') else None,
                            'actual_days': safe_int_convert(line_item.get('actual_days')) if line_item.get('actual_days') else None,
                            'actual_rate': clean_money_value(line_item.get('actual_rate')) if line_item.get('actual_rate') else None,
                            'actual_total': clean_money_value(line_item.get('actual_total')) if line_item.get('actual_total') else None
                        }
                        detail_rows.append(detail_row)
                
                logger.info(f"Prepared {len(detail_rows)} detail rows for upload")
                
                # Upload to BigQuery
                if processor.bigquery_service:
                    logger.info("Uploading budget data to BigQuery...")
                    try:
                        processor.bigquery_service.upload_budget(budget_data)
                        processor.bigquery_service.upload_budget_details(detail_rows)
                        logger.info("✓ Successfully uploaded budget data to BigQuery")
                        successful_budgets += 1
                    except Exception as e:
                        logger.error("❌ Failed to upload to BigQuery:")
                        logger.error(f"  Error: {str(e)}")
                        failed_budgets.append(budget_info)
                else:
                    logger.warning("⚠️ BigQuery service not available - skipping upload")
                    successful_budgets += 1
            else:
                raise Exception("Budget processing returned None")
            
        except Exception as e:
            logger.error("\n" + "=" * 80)
            logger.error(f"❌ ERROR PROCESSING: {budget_info['description']}")
            logger.error(f"📄 Spreadsheet ID: {budget_info['spreadsheet_id']}")
            logger.error(f"📑 Sheet GID: {budget_info['gid']}")
            logger.error(f"❗ Error: {str(e)}")
            logger.error("=" * 80 + "\n")
            failed_budgets.append(budget_info)
    
    finally:
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("📋 PROCESSING SUMMARY")
        logger.info(f"📊 Total budgets: {total_budgets}")
        logger.info(f"✅ Successfully processed: {successful_budgets}")
        logger.info(f"❌ Failed to process: {len(failed_budgets)}")
        if failed_budgets:
            logger.info("\n❌ Failed budgets:")
            for budget in failed_budgets:
                logger.info(f"   - {budget['description']}")
        logger.info("\n🏁 Processing complete!")
        logger.info("=" * 80 + "\n")

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