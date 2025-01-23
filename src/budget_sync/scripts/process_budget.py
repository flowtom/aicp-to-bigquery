"""
Script to process budget data from Google Sheets.
"""
import argparse
import json
import logging
import os
from pathlib import Path
from src.budget_sync.services.budget_processor import BudgetProcessor
from dotenv import load_dotenv
import sys

# Configure logging for more detailed output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).resolve().parents[2] / 'src'))

def main():
    """Main entry point for the budget processing script."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get environment variables
        SPREADSHEET_ID = os.getenv('BUDGET_SPREADSHEET_ID')
        SHEET_GID = os.getenv('BUDGET_SHEET_GID')
        CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # Log configuration
        logging.info("=== Budget Processing Script ===")
        logging.info(f"Processing spreadsheet: {SPREADSHEET_ID}")
        logging.info(f"Sheet GID: {SHEET_GID}")
        logging.info(f"Using credentials from: {CREDENTIALS_PATH}")
        
        # Initialize processor
        budget_processor = BudgetProcessor(CREDENTIALS_PATH)
        
        # Create metadata
        metadata = {
            'version_status': 'draft',
            'user_email': os.getenv('USER_EMAIL', 'unknown'),
            'version_notes': os.getenv('VERSION_NOTES', '')
        }
        
        # Process the budget
        processed_rows, metadata = budget_processor.process_sheet(SPREADSHEET_ID, SHEET_GID)
        
        # Generate output filename
        output_filename = f"output/processed_budget_{metadata['upload_info']['id']}.json"
        
        # Ensure output directory exists
        Path('output').mkdir(parents=True, exist_ok=True)
        
        # Save results
        with open(output_filename, 'w') as f:
            json.dump({
                'metadata': metadata,
                'rows': processed_rows
            }, f, indent=2)
            
        # Log summary
        logging.info(f"\nFull results saved to: {output_filename}")
        logging.info(f"Version: {metadata['upload_info']['version']}")
        logging.info(f"\nFirst few processed rows:")
        for row in processed_rows[:3]:
            logging.info(json.dumps(row, indent=2))
            
    except Exception as e:
        logging.error(f"Error processing budget: {str(e)}")
        raise

if __name__ == "__main__":
    main() 