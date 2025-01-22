"""
Script to process budget data from Google Sheets.
"""
import argparse
import json
import logging
import os
from pathlib import Path
from src.services.budget_processor import BudgetProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for budget processing script."""
    parser = argparse.ArgumentParser(description='Process budget data from Google Sheets')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")

    logger.info("=== Budget Processing Script ===")

    # Configuration
    SPREADSHEET_ID = "1-DPBYMpn2NEirobksXa0sJvs3K0Xfhh6nM1lyWEyR0I"
    SHEET_GID = "590213670"
    CREDENTIALS_PATH = "config/service-account-key.json"

    logger.info(f"Processing spreadsheet: {SPREADSHEET_ID}")
    logger.info(f"Sheet GID: {SHEET_GID}")
    logger.info(f"Using credentials from: {CREDENTIALS_PATH}")

    try:
        # Initialize processor
        processor = BudgetProcessor(CREDENTIALS_PATH)

        # Process sheet
        processed_rows, metadata = processor.process_sheet(SPREADSHEET_ID, SHEET_GID)

        # Prepare output
        output = {
            'metadata': metadata.to_dict(),
            'rows': processed_rows
        }

        # Save results
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / f"processed_budget_{metadata.version_id}.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)

        logger.info(f"\nFull results saved to: {output_file}")
        logger.info(f"\nFirst few processed rows:")
        for row in processed_rows[:3]:
            logger.info(json.dumps(row, indent=2))

    except Exception as e:
        logger.error(f"Error processing budget: {str(e)}")
        raise

if __name__ == "__main__":
    main() 