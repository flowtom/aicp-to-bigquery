"""
Script to process budget data from Google Sheets and sync to BigQuery.
"""
import argparse
import json
import logging
import os
from pathlib import Path
from datetime import datetime
import hashlib
from src.budget_sync.services.budget_processor import BudgetProcessor
from src.budget_sync.services.bigquery_service import BigQueryService
from dotenv import load_dotenv
import sys
from typing import Any, Dict, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging for more detailed output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).resolve().parents[2] / 'src'))

def generate_version_hash(data: dict) -> str:
    """Generate a hash of the data for version tracking."""
    content = json.dumps(data, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()[:8]

def extract_project_info(metadata: dict) -> dict:
    """Extract project information from metadata."""
    # Get project info from metadata
    project_info = metadata['metadata']['project_info']
    financials = metadata['metadata']['financials']['grand_total']
    
    # Generate a project ID using spreadsheet title if project title is empty
    base_name = project_info['project_title'] or metadata['upload_info']['spreadsheet_title']
    project_id = f"{base_name}_{datetime.now().strftime('%Y%m%d')}"
    project_id = ''.join(c if c.isalnum() else '_' for c in project_id)
    
    # Get current timestamp in UTC
    current_time = datetime.utcnow().isoformat()
    
    return {
        'project_id': project_id,
        'job_name': project_info['project_title'] or metadata['upload_info']['spreadsheet_title'],
        'client_name': project_info.get('client_name', ''),
        'production_company': project_info.get('production_company', ''),
        'latest_budget_id': f"{project_id}_{datetime.now().strftime('%H%M%S')}",
        'latest_estimate_total': float(financials['estimated'].replace('$', '').replace(',', '')),
        'latest_actual_total': float(financials['actual'].replace('$', '').replace(',', '')),
        'latest_client_actual_total': float(financials['client_actual'].replace('$', '').replace(',', '')),
        'latest_variance': float(financials['variance'].replace('$', '').replace(',', '')),
        'latest_client_variance': float(financials['client_variance'].replace('$', '').replace(',', '')),
        'created_at': current_time,
        'updated_at': current_time,
        'status': 'active'
    }

def prepare_budget_record(metadata: dict, version_hash: str) -> dict:
    """Prepare budget record from metadata."""
    current_time = datetime.utcnow().isoformat()
    project_info = metadata['metadata']['project_info']
    project_date = project_info['date']
    if not project_date:
        # If no project date, use the upload date (just the date portion)
        project_date = current_time.split('T')[0]

    # Use same project ID generation logic as extract_project_info
    base_name = project_info['project_title'] or metadata['upload_info']['spreadsheet_title']
    project_id = f"{base_name}_{datetime.now().strftime('%Y%m%d')}"
    project_id = ''.join(c if c.isalnum() else '_' for c in project_id)
    budget_id = f"{project_id}_{datetime.now().strftime('%H%M%S')}"

    return {
        'budget_id': budget_id,
        'project_id': project_id,
        'upload_timestamp': current_time,
        'version_hash': version_hash,
        'user_email': os.getenv('USER_EMAIL'),
        'version_status': 'draft',
        'version_notes': os.getenv('VERSION_NOTES', ''),
        'spreadsheet_id': metadata['upload_info']['spreadsheet_id'],
        'sheet_name': metadata['upload_info']['sheet_title'],
        'sheet_gid': metadata['upload_info']['sheet_gid'],
        'project_title': project_info['project_title'] or metadata['upload_info']['spreadsheet_title'],
        'production_company': project_info.get('production_company', ''),
        'contact_phone': project_info.get('contact_phone', ''),
        'project_date': project_date,
        'director': metadata['metadata']['core_team'].get('director', ''),
        'producer': metadata['metadata']['core_team'].get('producer', ''),
        'writer': metadata['metadata']['core_team'].get('writer', ''),
        'pre_prod_days': int(metadata['metadata']['timeline'].get('pre_prod_days', 0)),
        'build_days': int(metadata['metadata']['timeline'].get('build_days', 0)),
        'pre_light_days': int(metadata['metadata']['timeline'].get('pre_light_days', 0)),
        'studio_days': int(metadata['metadata']['timeline'].get('studio_days', 0)),
        'location_days': int(metadata['metadata']['timeline'].get('location_days', 0)),
        'wrap_days': int(metadata['metadata']['timeline'].get('wrap_days', 0)),
        'total_days': sum([
            int(metadata['metadata']['timeline'].get('pre_prod_days', 0)),
            int(metadata['metadata']['timeline'].get('build_days', 0)),
            int(metadata['metadata']['timeline'].get('pre_light_days', 0)),
            int(metadata['metadata']['timeline'].get('studio_days', 0)),
            int(metadata['metadata']['timeline'].get('location_days', 0)),
            int(metadata['metadata']['timeline'].get('wrap_days', 0))
        ]),
        'firm_bid_total_estimate': float(metadata['metadata']['financials']['grand_total']['estimated'].replace('$', '').replace(',', '')),
        'firm_bid_total_actual': float(metadata['metadata']['financials']['grand_total']['actual'].replace('$', '').replace(',', '')),
        'cost_plus_total_estimate': 0.0,  # TODO: Add cost plus totals
        'cost_plus_total_actual': 0.0,
        'grand_total_estimate': float(metadata['metadata']['financials']['grand_total']['estimated'].replace('$', '').replace(',', '')),
        'grand_total_actual': float(metadata['metadata']['financials']['grand_total']['actual'].replace('$', '').replace(',', '')),
        'grand_total_variance': float(metadata['metadata']['financials']['grand_total']['variance'].replace('$', '').replace(',', '')),
        'client_total_actual': float(metadata['metadata']['financials']['grand_total']['client_actual'].replace('$', '').replace(',', '')),
        'client_total_variance': float(metadata['metadata']['financials']['grand_total']['client_variance'].replace('$', '').replace(',', ''))
    }

def _parse_number(value: Any) -> float:
    """Parse a number from a string, handling currency and percentage formats."""
    if not value:
        return 0.0
    
    # Convert to string and clean up
    str_value = str(value).strip()
    
    # Handle percentages
    if str_value.endswith('%'):
        return float(str_value.rstrip('%')) / 100.0
        
    # Handle parentheses notation for negative numbers
    is_negative = str_value.startswith('(') and str_value.endswith(')')
    if is_negative:
        str_value = str_value[1:-1]  # Remove parentheses
        
    # Handle currency and clean up
    cleaned = str_value.replace('$', '').replace(',', '')
    
    try:
        result = float(cleaned or 0)
        return -result if is_negative else result
    except ValueError:
        return 0.0

def prepare_detail_records(rows: list, metadata: dict, version_hash: str) -> list:
    """Prepare budget detail records from processed rows."""
    current_time = datetime.utcnow().isoformat()
    upload_timestamp = current_time
    
    # Use same project ID generation logic as extract_project_info
    base_name = metadata['metadata']['project_info']['project_title'] or metadata['upload_info']['spreadsheet_title']
    project_id = f"{base_name}_{datetime.now().strftime('%Y%m%d')}"
    project_id = ''.join(c if c.isalnum() else '_' for c in project_id)
    budget_id = f"{project_id}_{datetime.now().strftime('%H%M%S')}"
    
    detail_records = []
    for row in rows:
        if not row.get('class_code') or not row.get('line_item_number'):
            continue
            
        line_item_id = f"{budget_id}_{row['class_code']}_{row['line_item_number']}"
        
        detail_records.append({
            'budget_id': budget_id,
            'project_id': project_id,
            'line_item_id': line_item_id,
            'upload_timestamp': upload_timestamp,
            'created_at': current_time,  # TODO: Track actual creation time
            'class_code': row['class_code'],
            'class_name': row['class_name'],
            'line_item_number': int(row['line_item_number']),
            'line_item_description': row['line_item_description'],
            'estimate_days': _parse_number(row.get('estimate_days')),
            'estimate_rate': _parse_number(row.get('estimate_rate')),
            'estimate_ot_rate': _parse_number(row.get('estimate_ot_rate')),
            'estimate_ot_hours': _parse_number(row.get('estimate_ot_hours')),
            'estimate_total': _parse_number(row.get('estimate_total')),
            'calculated_estimate_total': _parse_number(row.get('calculated_estimate_total')),
            'estimate_variance': _parse_number(row.get('estimate_variance')),
            'actual_days': _parse_number(row.get('actual_days')),
            'actual_rate': _parse_number(row.get('actual_rate')),
            'actual_total': _parse_number(row.get('actual_total')),
            'calculated_actual_total': _parse_number(row.get('calculated_actual_total')),
            'actual_variance': _parse_number(row.get('actual_variance')),
            'class_total_estimate': _parse_number(row.get('class_total_estimate')),
            'class_total_actual': _parse_number(row.get('class_total_actual')),
            'class_pnw_estimate': _parse_number(row.get('class_pnw_estimate')),
            'class_pnw_actual': _parse_number(row.get('class_pnw_actual')),
            'class_pnw_rate': _parse_number(row.get('class_pnw_rate')),
            'notes': row.get('notes', ''),
            'is_subtotal': bool(row.get('is_subtotal', False))
        })
    
    return detail_records

def prepare_validation_records(validation_results: list, metadata: dict) -> list:
    """Prepare validation records."""
    project_id = metadata['upload_info']['id'].split('_')[0]
    budget_id = metadata['upload_info']['id']
    validation_timestamp = datetime.utcnow().isoformat()
    
    validation_records = []
    for result in validation_results:
        validation_records.append({
            'budget_id': budget_id,
            'project_id': project_id,
            'validation_id': f"{budget_id}_validation_{generate_version_hash(result)}",
            'upload_timestamp': metadata['upload_info']['timestamp'],
            'validation_timestamp': validation_timestamp,
            'validation_type': result['type'],
            'severity': result['severity'],
            'message': result['message'],
            'class_code': result.get('class_code'),
            'line_item_id': result.get('line_item_id'),
            'field_name': result.get('field_name'),
            'expected_value': str(result.get('expected_value', '')),
            'actual_value': str(result.get('actual_value', ''))
        })
    
    return validation_records

def process_single_budget(budget_processor: BudgetProcessor, bigquery_service: BigQueryService, budget_config: Dict[str, Any]) -> bool:
    """Process a single budget and upload to BigQuery."""
    try:
        logger.info("\n" + "="*80)
        logger.info(f"🔄 PROCESSING BUDGET: {budget_config['description']}")
        logger.info(f"📄 Spreadsheet ID: {budget_config['spreadsheet_id']}")
        logger.info(f"📑 Sheet GID: {budget_config['sheet_gid']}")
        logger.info("="*80 + "\n")

        # Process budget data
        processed_rows, metadata = budget_processor.process_budget(
            budget_config['spreadsheet_id'], 
            budget_config['sheet_gid']
        )
        
        # Generate version hash
        version_hash = generate_version_hash({
            'metadata': metadata,
            'rows': processed_rows
        })
        
        # Create/update project record
        project_info = extract_project_info(metadata)
        project_id = bigquery_service.create_or_update_project(project_info)
        
        # Upload budget record
        budget_record = prepare_budget_record(metadata, version_hash)
        budget_id = bigquery_service.upload_budget(budget_record)
        
        # Upload budget details
        detail_records = prepare_detail_records(processed_rows, metadata, version_hash)
        detail_count = bigquery_service.upload_budget_details(detail_records)
        
        # Upload validation results if any
        if metadata.get('validation_results'):
            validation_records = prepare_validation_records(
                metadata['validation_results'],
                metadata
            )
            validation_count = bigquery_service.upload_validations(validation_records)
        else:
            validation_count = 0
        
        # Generate output filename
        output_filename = f"output/processed_budget_{metadata['upload_info']['id']}.json"
        
        # Ensure output directory exists
        Path('output').mkdir(parents=True, exist_ok=True)
        
        # Save results locally
        with open(output_filename, 'w') as f:
            json.dump({
                'metadata': metadata,
                'rows': processed_rows,
                'bigquery_sync': {
                    'project_id': project_id,
                    'budget_id': budget_id,
                    'detail_rows_uploaded': detail_count,
                    'validation_rows_uploaded': validation_count,
                    'version_hash': version_hash,
                    'sync_timestamp': datetime.utcnow().isoformat()
                }
            }, f, indent=2)
        
        logger.info("\n" + "="*80)
        logger.info(f"✅ COMPLETED PROCESSING: {budget_config['description']}")
        logger.info(f"📁 Results saved to: {output_filename}")
        logger.info(f"🆔 Project ID: {project_id}")
        logger.info(f"📊 Budget ID: {budget_id}")
        logger.info(f"📝 Detail rows: {detail_count}")
        logger.info(f"⚠️  Validation rows: {validation_count}")
        logger.info("="*80 + "\n")
        
        return True
        
    except Exception as e:
        logger.error("\n" + "="*80)
        logger.error(f"❌ ERROR PROCESSING: {budget_config['description']}")
        logger.error(f"📄 Spreadsheet ID: {budget_config['spreadsheet_id']}")
        logger.error(f"📑 Sheet GID: {budget_config['sheet_gid']}")
        logger.error(f"❗ Error: {str(e)}")
        logger.error("="*80 + "\n")
        return False

def parse_google_sheets_url(url: str) -> Dict[str, str]:
    """Parse a Google Sheets URL into spreadsheet_id and gid.
    
    Example URL format:
    https://docs.google.com/spreadsheets/d/1340FUfPqPWIlQu2H6W5D5JTAwxJYCeYIbjVSTWUeepU/edit?gid=803920845
    """
    try:
        # Extract spreadsheet ID
        spreadsheet_id = url.split('/d/')[1].split('/')[0]
        
        # Extract GID
        gid = None
        if 'gid=' in url:
            gid = url.split('gid=')[1].split('&')[0].split('#')[0]
        
        logger.info(f"Parsed URL: {url}")
        logger.info(f"Extracted spreadsheet_id: {spreadsheet_id}")
        logger.info(f"Extracted GID: {gid}")
        
        return {
            "spreadsheet_id": spreadsheet_id,
            "sheet_gid": gid or "0",  # Default to first sheet if no GID
            "description": f"Budget from spreadsheet {spreadsheet_id}"
        }
    except Exception as e:
        raise ValueError(f"Invalid Google Sheets URL format: {url}") from e

def main():
    """Main entry point for budget processing script."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get required environment variables
        bq_project_id = os.getenv('BIGQUERY_PROJECT_ID')
        bq_dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not all([bq_project_id, bq_dataset_id]):
            raise ValueError("Missing required environment variables")
            
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Process AICP budgets and sync to BigQuery')
        parser.add_argument('urls', nargs='+', help='One or more Google Sheets URLs to process')
        args = parser.parse_args()
        
        # Parse URLs into budget configs
        budgets = []
        for url in args.urls:
            try:
                budget_config = parse_google_sheets_url(url)
                budgets.append(budget_config)
            except ValueError as e:
                logger.error(f"Skipping invalid URL: {url}")
                logger.error(f"Error: {str(e)}")
                continue
            
        if not budgets:
            raise ValueError("No valid Google Sheets URLs provided")
            
        # Initialize services
        budget_processor = BudgetProcessor()
        bigquery_service = BigQueryService(bq_project_id, bq_dataset_id)
        
        # Process each budget and track results
        total_budgets = len(budgets)
        successful_budgets = 0
        failed_budgets = []
        
        logger.info("\n" + "="*80)
        logger.info(f"🚀 STARTING BUDGET PROCESSING")
        logger.info(f"📊 Total budgets to process: {total_budgets}")
        logger.info("="*80 + "\n")
        
        # Process each budget
        for index, budget in enumerate(budgets, 1):
            logger.info(f"\n🔄 Processing budget {index} of {total_budgets}")
            
            if process_single_budget(budget_processor, bigquery_service, budget):
                successful_budgets += 1
            else:
                failed_budgets.append(budget['description'])
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("📋 PROCESSING SUMMARY")
        logger.info(f"📊 Total budgets: {total_budgets}")
        logger.info(f"✅ Successfully processed: {successful_budgets}")
        logger.info(f"❌ Failed to process: {len(failed_budgets)}")
        
        if failed_budgets:
            logger.info("\n❌ Failed budgets:")
            for budget in failed_budgets:
                logger.info(f"   - {budget}")
        
        logger.info("\n🏁 Processing complete!")
        logger.info("="*80 + "\n")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 