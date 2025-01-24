import os
import logging
import argparse
from dotenv import load_dotenv
from ..services.budget_sync_service import BudgetSyncService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Set up a new budget from a Clickup task')
    parser.add_argument('task_id', help='Clickup task ID to create budget for')
    args = parser.parse_args()
    
    try:
        # Initialize the sync service
        sync_service = BudgetSyncService()
        
        # Set up the new budget
        result = sync_service.setup_new_budget(args.task_id)
        
        # Log the results
        logger.info("Budget setup completed successfully:")
        logger.info(f"Budget URL: {result['budget_url']}")
        logger.info(f"AICP List ID: {result['aicp_list_id']}")
        
    except Exception as e:
        logger.error(f"Error setting up budget: {str(e)}")
        raise

if __name__ == "__main__":
    main() 