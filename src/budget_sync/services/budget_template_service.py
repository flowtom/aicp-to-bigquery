import os
import logging
from typing import Dict, Any
from datetime import datetime
from .google_drive_service import GoogleDriveService
from .google_sheets_service import GoogleSheetsService

logger = logging.getLogger(__name__)

class BudgetTemplateService:
    """Service for managing budget templates in Google Drive/Sheets."""
    
    def __init__(self):
        self.drive = GoogleDriveService()
        self.sheets = GoogleSheetsService()
        
        self.template_id = os.getenv('GOOGLE_SHEETS_TEMPLATE_ID')
        if not self.template_id:
            raise ValueError("GOOGLE_SHEETS_TEMPLATE_ID environment variable is required")
            
        self.bids_root_id = os.getenv('GOOGLE_BIDS_ROOT_ID')
        if not self.bids_root_id:
            raise ValueError("GOOGLE_BIDS_ROOT_ID environment variable is required")
            
        self.workspace_domain = os.getenv('GOOGLE_WORKSPACE_DOMAIN')
        if not self.workspace_domain:
            raise ValueError("GOOGLE_WORKSPACE_DOMAIN environment variable is required")
    
    def setup_budget(self, task_id: str, client_name: str, job_name: str) -> Dict[str, Any]:
        """Set up budget template and folder structure."""
        # Create folder structure
        folder_structure = self.drive.create_budget_folder_structure(
            client_name=client_name,
            job_name=job_name,
            root_id=self.bids_root_id
        )
        
        # Copy budget template
        template_name = f"{job_name}_Budget_{datetime.now().strftime('%Y%m%d')}"
        budget_info = self.drive.copy_template(
            template_id=self.template_id,
            name=template_name,
            parent_id=folder_structure['budget_folder']
        )
        
        # Set up sharing permissions
        self._setup_sharing(budget_info['id'])
        
        return {
            'budget_id': budget_info['id'],
            'budget_url': budget_info['web_view_link'],
            'folder_structure': folder_structure
        }
    
    def update_audit_log(self, budget_id: str, task_id: str) -> None:
        """Update the audit log in the budget sheet."""
        try:
            self.sheets.update_audit_log(
                spreadsheet_id=budget_id,
                task_id=task_id,
                timestamp=datetime.now().isoformat(),
                event_type='CREATED'
            )
        except Exception as e:
            logger.error(f"Error updating audit log: {str(e)}")
            # Don't raise the error as this is not critical
    
    def _setup_sharing(self, file_id: str) -> None:
        """Set up sharing permissions for the budget file."""
        try:
            # Share with workspace domain
            self.drive.share_file(
                file_id=file_id,
                domain=self.workspace_domain,
                role='writer'
            )
        except Exception as e:
            logger.error(f"Error setting up sharing: {str(e)}")
            raise 