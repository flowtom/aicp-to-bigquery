import os
import logging
import json
from typing import Dict, Any
import functions_framework
from flask import Request
from ..services.job_setup_service import JobSetupService
from ..services.budget_template_service import BudgetTemplateService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@functions_framework.http
def handle_job_automation(request: Request):
    """Cloud Function to handle Clickup automation for job creation."""
    try:
        # Parse request payload
        try:
            payload = request.get_json()
            logger.info(f"Received automation payload: {json.dumps(payload, indent=2)}")
        except Exception as e:
            logger.error(f"Error parsing JSON payload: {str(e)}")
            return ('Invalid JSON payload', 400)
        
        # Extract task information from payload
        try:
            task_id = payload.get('task_id')
            if not task_id:
                return ('Missing task_id in payload', 400)
        except Exception as e:
            logger.error(f"Error extracting task information: {str(e)}")
            return ('Error processing task information', 400)
        
        # Initialize services
        job_service = JobSetupService()
        budget_service = BudgetTemplateService()
        
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
        
        # Return success response with created resources
        response = {
            'status': 'success',
            'task_id': task_id,
            'budget_url': budget_info['budget_url'],
            'list_id': budget_info['list_id']
        }
        
        return (json.dumps(response), 200, {'Content-Type': 'application/json'})
        
    except Exception as e:
        logger.error(f"Error processing automation: {str(e)}", exc_info=True)
        return (str(e), 500) 