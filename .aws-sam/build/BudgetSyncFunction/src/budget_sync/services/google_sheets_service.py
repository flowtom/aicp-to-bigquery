import os
import logging
from typing import Dict, Any, List
from googleapiclient.discovery import build
from ..utils.google_auth import GoogleAuthManager

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    """Service for interacting with Google Sheets API."""
    
    def __init__(self):
        auth_manager = GoogleAuthManager()
        self.service = build('sheets', 'v4', credentials=auth_manager.get_credentials())
    
    def update_audit_log(self, spreadsheet_id: str, task_id: str, timestamp: str, event_type: str) -> None:
        """Update the audit log in the budget sheet."""
        try:
            # Get the audit log sheet ID
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            audit_sheet_id = None
            for sheet in sheet_metadata.get('sheets', []):
                if sheet['properties']['title'] == 'Audit Log':
                    audit_sheet_id = sheet['properties']['sheetId']
                    break
            
            if not audit_sheet_id:
                # Create audit log sheet if it doesn't exist
                audit_sheet_id = self._create_audit_log_sheet(spreadsheet_id)
            
            # Append the audit log entry
            range_name = 'Audit Log!A:D'
            values = [[timestamp, task_id, event_type, 'AUTO']]
            
            body = {
                'values': values
            }
            
            self.service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logger.info(f"Updated audit log for spreadsheet {spreadsheet_id}")
            
        except Exception as e:
            logger.error(f"Error updating audit log: {str(e)}")
            raise
    
    def _create_audit_log_sheet(self, spreadsheet_id: str) -> str:
        """Create an audit log sheet in the spreadsheet."""
        try:
            # Create new sheet
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': 'Audit Log',
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 4
                        }
                    }
                }
            }]
            
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
            
            sheet_id = result['replies'][0]['addSheet']['properties']['sheetId']
            
            # Add headers
            headers = [['Timestamp', 'Task ID', 'Event Type', 'Source']]
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='Audit Log!A1:D1',
                valueInputOption='RAW',
                body={'values': headers}
            ).execute()
            
            return sheet_id
            
        except Exception as e:
            logger.error(f"Error creating audit log sheet: {str(e)}")
            raise 