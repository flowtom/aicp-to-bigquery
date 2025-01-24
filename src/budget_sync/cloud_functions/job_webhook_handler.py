import os
import logging
import json
import functions_framework
from typing import Dict, Any
from ..services.job_setup_service import JobSetupService
from ..services.budget_template_service import BudgetTemplateService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@functions_framework.http
def handle_job_webhook(request):
    """Cloud Function to handle Clickup job creation webhooks."""
    try:
        # Verify webhook signature (TODO: Implement signature verification)
        
        # Parse webhook payload
        payload = request.get_json()
        
        # Verify this is a task creation event
        if not _is_job_task_creation(payload):
            return ('Not a job task creation event', 200)
            
        # Initialize services
        job_service = JobSetupService()
        budget_service = BudgetTemplateService()
        
        # Extract task information
        task_id = payload['task_id']
        
        # Create job structure in Clickup
        job_structure = job_service.create_job_structure(task_id)
        
        # Create budget template and folder structure
        budget_info = budget_service.setup_budget(
            task_id=task_id,
            client_name=job_structure['client_name'],
            job_name=job_structure['job_name']
        )
        
        # Update task with references
        job_service.update_task_references(
            task_id=task_id,
            budget_url=budget_info['budget_url'],
            budget_list_id=budget_info['list_id']
        )
        
        # Update audit log
        budget_service.update_audit_log(
            budget_id=budget_info['budget_id'],
            task_id=task_id
        )
        
        return ('Success', 200)
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return (str(e), 500)

def _is_job_task_creation(payload: Dict[str, Any]) -> bool:
    """Check if the webhook payload is for a job task creation event."""
    try:
        # Check event type
        if payload.get('event') != 'taskCreated':
            return False
            
        # Check if task is in the correct list/folder
        # TODO: Add your specific conditions here
        return True
        
    except Exception as e:
        logger.error(f"Error checking task type: {str(e)}")
        return False 