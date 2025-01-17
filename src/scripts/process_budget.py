"""
Script to process an AICP budget from Google Sheets.
"""
import os
import json
from datetime import datetime
import logging
from dotenv import load_dotenv
from ..services.budget_processor import BudgetProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Process budget data from Google Sheets."""
    # Enable debug logging if requested
    if os.getenv('DEBUG', '').lower() == 'true':
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")
    
    logger.info("=== Budget Processing Script ===")
    
    # Load environment variables
    load_dotenv(override=True)  # Force reload of environment variables
    
    # Get configuration
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    spreadsheet_id = os.getenv('BUDGET_SPREADSHEET_ID')
    sheet_gid = os.getenv('BUDGET_SHEET_GID', '590213670')
    
    if not spreadsheet_id:
        raise ValueError("BUDGET_SPREADSHEET_ID not found in environment variables")
    
    logger.info(f"Spreadsheet ID: {spreadsheet_id}")
    logger.info(f"Sheet GID: {sheet_gid}")
    logger.info(f"Using credentials from: {credentials_path}")
    logger.info("")
    
    # Initialize processor
    logger.info("Initializing budget processor...")
    processor = BudgetProcessor(credentials_path)
    
    # Process sheet
    logger.info("Fetching and processing sheet data...")
    processed_rows, metadata = processor.process_sheet(
        spreadsheet_id=spreadsheet_id,
        range_name='A1:Z1000',  # We'll determine actual range from the data
        target_gid=sheet_gid
    )
    
    # Save results
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(
        output_dir,
        f"processed_budget_{metadata['upload_id']}.json"
    )
    
    with open(output_file, 'w') as f:
        json.dump({
            'metadata': metadata,
            'rows': processed_rows
        }, f, indent=2)
    
    logger.info(f"\nFull results saved to: {output_file}")
    
    # Debug output
    if os.getenv('DEBUG', '').lower() == 'true':
        logger.debug("\nFirst few processed rows:")
        for i, row in enumerate(processed_rows[:3]):
            logger.debug(f"\nRow {i+1}:")
            logger.debug(json.dumps(row, indent=2))

if __name__ == '__main__':
    main() 