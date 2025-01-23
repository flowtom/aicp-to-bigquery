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
    project_id = metadata['upload_info']['id'].split('_')[0]
    project_info = metadata['metadata']['project_info']
    
    return {
        'project_id': project_id,
        'job_name': project_info['project_title'],
        'client_name': project_info.get('client_name', ''),
        'production_company': project_info.get('production_company', ''),
        'latest_budget_id': metadata['upload_info']['id'],
        'latest_estimate_total': float(metadata['grand_total']['estimated'].replace('$', '').replace(',', '')),
        'latest_actual_total': float(metadata['grand_total']['actual'].replace('$', '').replace(',', '')),
        'latest_client_actual_total': float(metadata['grand_total']['client_actual'].replace('$', '').replace(',', '')),
        'latest_variance': float(metadata['grand_total']['variance'].replace('$', '').replace(',', '')),
        'latest_client_variance': float(metadata['grand_total']['client_variance'].replace('$', '').replace(',', '')),
        'updated_at': datetime.utcnow().isoformat(),
        'status': 'active'
    }

def prepare_budget_record(metadata: dict, version_hash: str) -> dict:
    """Prepare budget record from metadata."""
    return {
        'budget_id': metadata['upload_info']['id'],
        'project_id': metadata['upload_info']['id'].split('_')[0],
        'upload_timestamp': metadata['upload_info']['timestamp'],
        'version_hash': version_hash,
        'user_email': os.getenv('USER_EMAIL'),
        'version_status': metadata.get('version_status', 'draft'),
        'version_notes': metadata.get('version_notes', ''),
        'spreadsheet_id': metadata['upload_info']['spreadsheet_id'],
        'sheet_name': metadata['upload_info']['sheet_title'],
        'sheet_gid': metadata['upload_info']['sheet_gid'],
        'project_title': metadata['metadata']['project_info']['project_title'],
        'production_company': metadata['metadata']['project_info'].get('production_company', ''),
        'contact_phone': metadata['metadata']['project_info'].get('contact_phone', ''),
        'project_date': metadata['metadata']['project_info'].get('date', ''),
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
        'firm_bid_total_estimate': float(metadata['grand_total']['estimated'].replace('$', '').replace(',', '')),
        'firm_bid_total_actual': float(metadata['grand_total']['actual'].replace('$', '').replace(',', '')),
        'cost_plus_total_estimate': 0.0,  # TODO: Add cost plus totals
        'cost_plus_total_actual': 0.0,
        'grand_total_estimate': float(metadata['grand_total']['estimated'].replace('$', '').replace(',', '')),
        'grand_total_actual': float(metadata['grand_total']['actual'].replace('$', '').replace(',', ''))
    }

def prepare_detail_records(rows: list, metadata: dict, version_hash: str) -> list:
    """Prepare budget detail records."""
    project_id = metadata['upload_info']['id'].split('_')[0]
    budget_id = metadata['upload_info']['id']
    upload_timestamp = metadata['upload_info']['timestamp']
    
    detail_records = []
    for row in rows:
        # Skip non-line items
        if not row.get('line_item_number'):
            continue
            
        detail_records.append({
            'budget_id': budget_id,
            'project_id': project_id,
            'line_item_id': f"{budget_id}_{row['class_code']}_{row['line_item_number']}",
            'upload_timestamp': upload_timestamp,
            'created_at': upload_timestamp,  # TODO: Track actual creation time
            'class_code': row['class_code'],
            'class_name': row['class_name'],
            'line_item_number': row['line_item_number'],
            'line_item_description': row['line_item_description'],
            'estimate_days': float(row.get('estimate_days', 0) or 0),
            'estimate_rate': float(row.get('estimate_rate', 0) or 0),
            'estimate_ot_rate': float(row.get('estimate_ot_rate', 0) or 0),
            'estimate_ot_hours': float(row.get('estimate_ot_hours', 0) or 0),
            'estimate_total': float(row.get('estimate_total', 0) or 0),
            'calculated_estimate_total': float(row.get('calculated_estimate_total', 0) or 0),
            'estimate_variance': float(row.get('estimate_variance', 0) or 0),
            'actual_days': float(row.get('actual_days', 0) or 0),
            'actual_rate': float(row.get('actual_rate', 0) or 0),
            'actual_total': float(row.get('actual_total', 0) or 0),
            'calculated_actual_total': float(row.get('calculated_actual_total', 0) or 0),
            'actual_variance': float(row.get('actual_variance', 0) or 0),
            'class_total_estimate': float(row.get('class_total_estimate', 0) or 0),
            'class_total_actual': float(row.get('class_total_actual', 0) or 0),
            'class_pnw_estimate': float(row.get('class_pnw_estimate', 0) or 0),
            'class_pnw_actual': float(row.get('class_pnw_actual', 0) or 0),
            'class_pnw_rate': float(row.get('class_pnw_rate', 0) or 0),
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

def main():
    """Main entry point for budget processing script."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get required environment variables
        spreadsheet_id = os.getenv('BUDGET_SPREADSHEET_ID')
        sheet_gid = os.getenv('BUDGET_SHEET_GID')
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        bq_project_id = os.getenv('BIGQUERY_PROJECT_ID')
        bq_dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not all([spreadsheet_id, credentials_path, bq_project_id, bq_dataset_id]):
            raise ValueError("Missing required environment variables")
        
        # Initialize services
        budget_processor = BudgetProcessor(credentials_path)
        bigquery_service = BigQueryService(bq_project_id, bq_dataset_id)
        
        # Process budget data
        metadata, processed_rows = budget_processor.process_budget(spreadsheet_id, sheet_gid)
        
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
        
        logger.info(f"Processing complete. Results saved to {output_filename}")
        logger.info(f"BigQuery sync complete:")
        logger.info(f"- Project: {project_id}")
        logger.info(f"- Budget: {budget_id}")
        logger.info(f"- Detail rows: {detail_count}")
        logger.info(f"- Validation rows: {validation_count}")
        
    except Exception as e:
        logger.error(f"Error processing budget: {str(e)}")
        raise

if __name__ == '__main__':
    main() 