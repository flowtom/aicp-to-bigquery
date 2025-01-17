"""
Script to process an AICP budget from Google Sheets.
"""
from pathlib import Path
import json
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from ..services.budget_processor import BudgetProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    """Process a sample budget sheet."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Configuration
        CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'config/service-account-key.json')
        SPREADSHEET_ID = os.getenv('BUDGET_SPREADSHEET_ID')
        SHEET_NAME = os.getenv('BUDGET_SHEET_NAME', 'AICP BUDGET')
        DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
        
        if DEBUG:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Debug mode enabled")
        
        if not SPREADSHEET_ID:
            raise ValueError("Please set BUDGET_SPREADSHEET_ID in your .env file")
        
        if not Path(CREDENTIALS_PATH).exists():
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_PATH}")
        
        # Use a wide range to capture all data
        RANGE = f"'{SHEET_NAME}'!A1:Z1000"
        
        logger.info("=== Budget Processing Script ===")
        logger.info(f"Spreadsheet ID: {SPREADSHEET_ID}")
        logger.info(f"Sheet Name: {SHEET_NAME}")
        logger.info(f"Range: {RANGE}")
        logger.info(f"Using credentials from: {CREDENTIALS_PATH}")
        
        # Initialize processor
        logger.info("\nInitializing budget processor...")
        processor = BudgetProcessor(str(CREDENTIALS_PATH))
        
        # Process sheet
        logger.info("Fetching and processing sheet data...")
        rows, metadata = processor.process_sheet(SPREADSHEET_ID, RANGE)
        
        # Save results
        output_path = Path('output')
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = output_path / f'processed_budget_{timestamp}.json'
        
        with open(results_file, 'w') as f:
            json.dump({
                'metadata': metadata,
                'rows': rows
            }, f, indent=2)
        
        logger.info(f"\nFull results saved to: {results_file}")
        
        if DEBUG:
            logger.debug("\nFirst few processed rows:")
            for i, row in enumerate(rows[:3], 1):
                logger.debug(f"\nRow {i}:")
                logger.debug(json.dumps(row, indent=2))
        
    except Exception as e:
        logger.error(f"Error processing budget: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main() 