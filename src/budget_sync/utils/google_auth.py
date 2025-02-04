import os
import logging
import pickle
from typing import Optional
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)

class GoogleAuthManager:
    """Manages Google OAuth credentials for all services."""
    
    # Combined scopes for all Google services
    SCOPES = [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    
    _instance = None
    _credentials = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GoogleAuthManager, cls).__new__(cls)
        return cls._instance
    
    def get_credentials(self) -> Credentials:
        """Get valid user credentials from storage."""
        if self._credentials and self._credentials.valid:
            return self._credentials
            
        self._credentials = self._load_or_refresh_credentials()
        return self._credentials
    
    def _load_or_refresh_credentials(self) -> Credentials:
        """Load credentials from file or refresh if expired."""
        creds = None
        
        # Load existing credentials
        if os.path.exists('token.json'):
            with open('token.json', 'rb') as token:
                creds = pickle.load(token)
        
        # If credentials exist but are expired, refresh them
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self._save_credentials(creds)
            return creds
            
        # If no valid credentials found, create new ones
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', self.SCOPES)
            creds = flow.run_local_server(port=0)
            self._save_credentials(creds)
        
        return creds
    
    def _save_credentials(self, creds: Credentials) -> None:
        """Save credentials to token.json."""
        with open('token.json', 'wb') as token:
            pickle.dump(creds, token) 