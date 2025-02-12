import os
import logging
from typing import Dict, Any, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class ClickupService:
    """Service for interacting with Clickup API."""
    
    def __init__(self):
        self.api_key = os.getenv('CLICKUP_API_KEY')
        if not self.api_key:
            raise ValueError("CLICKUP_API_KEY environment variable is required")
        self.base_url = "https://api.clickup.com/api/v2"
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
    
    def create_folder(self, list_id: str, name: str) -> Dict[str, Any]:
        """Create a new folder in a Clickup list."""
        url = f"{self.base_url}/list/{list_id}/folder"
        payload = {"name": name}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def create_list(self, folder_id: str, name: str) -> Dict[str, Any]:
        """Create a new list in a Clickup folder."""
        url = f"{self.base_url}/folder/{folder_id}/list"
        payload = {"name": name}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def update_custom_field(self, task_id: str, field_id: str, value: str) -> Dict[str, Any]:
        """Update a custom field value for a task."""
        url = f"{self.base_url}/task/{task_id}/field/{field_id}"
        payload = {"value": value}
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Get task details."""
        url = f"{self.base_url}/task/{task_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def create_task(self, list_id: str, name: str, description: Optional[str] = None) -> Dict[str, Any]:
        """Create a new task in a list."""
        url = f"{self.base_url}/list/{list_id}/task"
        payload = {
            "name": name,
            "description": description or ""
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json() 