import os
import logging
from typing import Dict, Any, Optional, List
from googleapiclient.discovery import build
from datetime import datetime
from ..utils.google_auth import GoogleAuthManager

logger = logging.getLogger(__name__)

class GoogleDriveService:
    """Service for interacting with Google Drive API."""
    
    def __init__(self):
        auth_manager = GoogleAuthManager()
        self.service = build('drive', 'v3', credentials=auth_manager.get_credentials())
    
    def create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive and return its ID."""
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
            
        file = self.service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        logger.info(f"Created folder: {name} with ID: {file.get('id')}")
        return file.get('id')
    
    def copy_template(self, template_id: str, name: str, parent_id: str) -> Dict[str, str]:
        """Copy a template file to a new location."""
        file_metadata = {
            'name': name,
            'parents': [parent_id]
        }
        
        file = self.service.files().copy(
            fileId=template_id,
            body=file_metadata,
            fields='id, webViewLink'
        ).execute()
        
        logger.info(f"Copied template to: {name} with ID: {file.get('id')}")
        return {
            'id': file.get('id'),
            'web_view_link': file.get('webViewLink')
        }
    
    def create_budget_folder_structure(self, client_name: str, job_name: str, root_id: str, year: Optional[int] = None) -> Dict[str, str]:
        """Create the complete folder structure for a new budget."""
        if year is None:
            year = datetime.now().year
            
        # Create or find client folder under root
        client_folder = self._find_or_create_folder(client_name, root_id)
        
        # Create or find year folder
        year_folder = self._find_or_create_folder(str(year), client_folder)
        
        # Create job folder
        job_folder = self.create_folder(job_name, year_folder)
        
        # Create budget folder
        budget_folder = self.create_folder("Budget", job_folder)
        
        return {
            'client_folder': client_folder,
            'year_folder': year_folder,
            'job_folder': job_folder,
            'budget_folder': budget_folder
        }
    
    def share_file(self, file_id: str, domain: str, role: str = 'writer') -> None:
        """Share a file with a domain."""
        permission = {
            'type': 'domain',
            'role': role,
            'domain': domain
        }
        
        try:
            self.service.permissions().create(
                fileId=file_id,
                body=permission,
                fields='id'
            ).execute()
            logger.info(f"Shared file {file_id} with domain {domain}")
        except Exception as e:
            logger.error(f"Error sharing file: {str(e)}")
            raise
    
    def _find_or_create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        """Find a folder by name or create it if it doesn't exist."""
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}'"
        if parent_id:
            query += f" and '{parent_id}' in parents"
            
        results = self.service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        items = results.get('files', [])
        
        if items:
            logger.info(f"Found existing folder: {name}")
            return items[0]['id']
            
        return self.create_folder(name, parent_id) 