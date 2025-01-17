"""Script to process AICP budgets from Google Sheets."""
import os
import json
import logging
from datetime import datetime
from pathlib import Path

from src.services.budget_processor import BudgetProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config() -> dict:
    """Load configuration from config file"""
    try:
        with open('config/config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise

def save_processed_data(processed_rows: list, metadata: dict, output_path: str) -> None:
    """Save processed data to JSON file"""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output_data = {
            'metadata': metadata,
            'rows': processed_rows
        }
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"\nFull results saved to: {output_path}")
        
        # Preview first few rows
        if processed_rows:
            logger.info("\nFirst few processed rows:")
            for row in processed_rows[:3]:
                logger.info(json.dumps(row, indent=2))
                
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        raise

def main():
    # Enable debug logging if DEBUG environment variable is set
    if os.getenv('DEBUG'):
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")

    logger.info("=== Budget Processing Script ===")
    
    try:
        # Load config
        config = load_config()
        
        # Get credentials path
        credentials_path = config.get('service_account_path', 'config/service-account-key.json')
        spreadsheet_id = config.get('spreadsheet_id')
        target_gid = config.get('sheet_gid')
        
        logger.info(f"Processing spreadsheet: {spreadsheet_id}")
        logger.info(f"Sheet GID: {target_gid}")
        logger.info(f"Using credentials from: {credentials_path}")
        
        # Initialize processor
        processor = BudgetProcessor(credentials_path)
        
        # Process budget
        processed_rows, metadata = processor.process_sheet(
            spreadsheet_id=spreadsheet_id,
            target_gid=target_gid
        )
        
        if not processed_rows:
            logger.warning("No rows were processed")
            return
            
        # Save to file
        output_path = os.path.join(
            'output',
            f"processed_budget_{metadata['upload_id']}.json"
        )
        save_processed_data(processed_rows, metadata, output_path)
        
    except Exception as e:
        logger.error(f"Error processing budget: {e}")
        raise

if __name__ == "__main__":
    main() 