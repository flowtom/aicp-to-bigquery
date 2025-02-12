"""Test script to run the budget processor."""
import json
import logging
from pathlib import Path
from budget_sync.services.budget_processor import BudgetProcessor
from datetime import datetime
import os

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
from dotenv import load_dotenv

env_path = Path(__file__).parents[3] / '.env'
load_dotenv(env_path)

# Debug log environment variables
logger.info(f"Loading environment from: {env_path}")
logger.info(f"PROJECT_ID: {'Set' if os.getenv('PROJECT_ID') else 'Not Set'}")
logger.info(f"SPREADSHEET_ID: {'Set' if os.getenv('SPREADSHEET_ID') else 'Not Set'}")

# Configuration via environment variables for test_run
PROJECT_ID = os.environ.get('PROJECT_ID', 'test-project')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', 'your-default-spreadsheet-id')

# Remove file-based configuration and use env variables
# with open('config/config.json', 'r') as f:
#     config = json.load(f)
# 
# PROJECT_ID = config.get('project_id', 'test-project')
# SPREADSHEET_ID = config.get('spreadsheet_id', 'your-default-spreadsheet-id')

def main():
    try:
        # Initialize budget processor using environment variable PROJECT_ID
        processor = BudgetProcessor(project_id=PROJECT_ID)
        logger.info("Budget processor initialized")
        
        # Define metadata for processing
        metadata = {
            'budget_name': 'Test Budget',
            'version_status': 'draft',
            'project_name': 'Test Project',
            'project_start_date': '2024-01-22',
            'project_end_date': '2024-02-22',
            'client_name': 'Test Client',
            'producer_name': 'Test Producer',
            'user_email': 'test@example.com',
            'version_notes': 'Test version',
            'previous_version_id': None
        }
        
        # Process budget using SPREADSHEET_ID from environment variable
        result = processor.process_budget(
            spreadsheet_id=SPREADSHEET_ID,
            metadata=metadata
        )
        
        # Create output directory if it doesn't exist
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # Save results to file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f'processed_budget_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
            
        logger.info(f"Processed {len(result.get('rows', []))} rows")
        logger.info(f"Found {len(result.get('validation_errors', []))} validation errors")
        logger.info(f"Results saved to {output_file}")
            
    except Exception as e:
        logger.error(f"Error processing budget: {str(e)}")
        raise

if __name__ == "__main__":
    main() 