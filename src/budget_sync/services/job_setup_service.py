import os
import logging
from typing import Dict, Any
from .clickup_service import ClickupService

logger = logging.getLogger(__name__)

class JobSetupService:
    """Service for setting up job infrastructure in Clickup."""
    
    def __init__(self):
        self.clickup = ClickupService()
        self.template_folder_id = os.getenv('CLICKUP_TEMPLATE_FOLDER_ID')
        if not self.template_folder_id:
            raise ValueError("CLICKUP_TEMPLATE_FOLDER_ID environment variable is required")
            
        self.budget_field_id = os.getenv('CLICKUP_BUDGET_FIELD_ID')
        if not self.budget_field_id:
            raise ValueError("CLICKUP_BUDGET_FIELD_ID environment variable is required")
            
        self.list_field_id = os.getenv('CLICKUP_LIST_FIELD_ID')
        if not self.list_field_id:
            raise ValueError("CLICKUP_LIST_FIELD_ID environment variable is required")
    
    def create_job_structure(self, task_id: str) -> Dict[str, Any]:
        """Create job folder structure from template."""
        # Get task details
        task = self.clickup.get_task(task_id)
        
        # Extract job information
        client_name = task['custom_fields'].get('client_name', 'Unknown Client')
        job_name = task['name']
        
        # Create job folder from template
        folder = self.clickup.create_folder(
            list_id=task['list']['id'],
            name=f"{job_name}_Job"
        )
        
        # Create budget list
        budget_list = self.clickup.create_list(
            folder_id=folder['id'],
            name="AICP Line Items"
        )
        
        # Copy template tasks (TODO: Implement template copying)
        
        return {
            'task_id': task_id,
            'client_name': client_name,
            'job_name': job_name,
            'folder_id': folder['id'],
            'list_id': budget_list['id']
        }
    
    def update_task_references(self, task_id: str, budget_url: str, budget_list_id: str) -> None:
        """Update task with budget and list references."""
        # Update budget URL custom field
        self.clickup.update_custom_field(
            task_id=task_id,
            field_id=self.budget_field_id,
            value=budget_url
        )
        
        # Update budget list reference
        self.clickup.update_custom_field(
            task_id=task_id,
            field_id=self.list_field_id,
            value=budget_list_id
        ) 