#!/usr/bin/env python3
"""
Script to run the budget processor on a specific spreadsheet.
"""

import logging
from src.budget_sync.services.budget_processor import BudgetProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the budget processor."""
    try:
        # Initialize the processor
        spreadsheet_id = "1M3dvwJ97EaWzQeVVg1392I3IXoxtsFPg9J9NUX5mDN4"
        gid = "1704193613"
        
        logger.info(f"Processing spreadsheet {spreadsheet_id} with GID {gid}")
        processor = BudgetProcessor(spreadsheet_id=spreadsheet_id, gid=gid)
        
        # Process the sheet
        processed_rows, metadata = processor.process_sheet(spreadsheet_id, gid)
        
        # Log results
        logger.info("Processing completed successfully")
        logger.info(f"Processed {len(processed_rows)} rows")
        logger.info(f"Metadata: {metadata}")
        
    except Exception as e:
        logger.error(f"Error processing spreadsheet: {str(e)}")
        raise

if __name__ == "__main__":
    main() 