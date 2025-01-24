import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .clickup_service import ClickupService
from .google_drive_service import GoogleDriveService
from .bigquery_service import BigQueryService

logger = logging.getLogger(__name__)

class BudgetSyncService:
    """Service for orchestrating budget synchronization between Clickup and Google Drive."""
    
    def __init__(self):
        self.clickup = ClickupService()
        self.drive = GoogleDriveService()
        self.bigquery = BigQueryService()
        
        self.template_id = os.getenv('BUDGET_TEMPLATE_ID')
        if not self.template_id:
            raise ValueError("BUDGET_TEMPLATE_ID environment variable is required")
            
        self.budget_field_id = os.getenv('CLICKUP_BUDGET_FIELD_ID')
        if not self.budget_field_id:
            raise ValueError("CLICKUP_BUDGET_FIELD_ID environment variable is required")
    
    def setup_new_budget(self, task_id: str) -> Dict[str, Any]:
        """Set up a new budget for a Clickup task."""
        # Get task details from Clickup
        task = self.clickup.get_task(task_id)
        
        # Extract relevant information
        client_name = task['custom_fields'].get('client_name', 'Unknown Client')
        job_name = task['name']
        year = datetime.now().year
        
        # Create folder structure in Google Drive
        folder_structure = self.drive.create_budget_folder_structure(
            client_name=client_name,
            job_name=job_name,
            year=year
        )
        
        # Copy budget template
        template_name = f"{job_name}_Budget_{datetime.now().strftime('%Y%m%d')}"
        budget_url = self.drive.copy_template(
            template_id=self.template_id,
            name=template_name,
            parent_id=folder_structure['budget_folder']
        )
        
        # Update Clickup task with budget URL
        self.clickup.update_custom_field(
            task_id=task_id,
            field_id=self.budget_field_id,
            value=budget_url
        )
        
        # Create AICP Line Items list in Clickup
        folder = self.clickup.create_folder(
            list_id=task['list']['id'],
            name=f"{job_name}_Budget"
        )
        
        aicp_list = self.clickup.create_list(
            folder_id=folder['id'],
            name="AICP Line Items"
        )
        
        return {
            'task_id': task_id,
            'budget_url': budget_url,
            'folder_structure': folder_structure,
            'aicp_list_id': aicp_list['id']
        }
    
    def sync_estimates_to_clickup(self, budget_id: str, list_id: str) -> Dict[str, Any]:
        """Sync estimates from budget sheet to Clickup list."""
        # TODO: Implement estimate sync logic
        # This will involve:
        # 1. Reading estimates from the budget sheet
        # 2. Creating/updating tasks in the Clickup list
        # 3. Syncing data to BigQuery
        pass
    
    def sync_actuals_to_sheets(self, budget_id: str, list_id: str) -> Dict[str, Any]:
        """Sync actuals from Clickup list to budget sheet."""
        # TODO: Implement actuals sync logic
        # This will involve:
        # 1. Reading actuals from Clickup tasks
        # 2. Updating the budget sheet
        # 3. Syncing data to BigQuery
        pass 