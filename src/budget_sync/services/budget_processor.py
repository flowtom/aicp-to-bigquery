from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import uuid
from typing import Dict, Any, List, Optional

from utils.data_validation import validate_budget_row, safe_float_convert, safe_int_convert

class AICPBudgetProcessor:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client()
        
        # Set up Google Sheets credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            'config/service-account-key.json',
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        self.sheets_service = build('sheets', 'v4', credentials=self.credentials)

    def process_budget(self, spreadsheet_id: str, metadata: Dict[str, Any]) -> str:
        """Process budget from Google Sheets"""
        upload_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        # Get spreadsheet data
        sheet = self.sheets_service.spreadsheets()
        result = sheet.get(spreadsheetId=spreadsheet_id, 
                         includeGridData=True).execute()
        
        processed_data = []
        error_rows = []
        current_class = None
        current_class_code = None
        
        # Process each sheet in the spreadsheet
        for sheet in result['sheets']:
            grid_data = sheet['data'][0]['rowData']
            
            for row_idx, row_data in enumerate(grid_data, 1):
                if 'values' not in row_data:
                    continue
                    
                row = self._extract_row_values(row_data['values'])
                
                # Skip empty rows
                if not any(row):
                    continue
                
                # Check for class header
                if row[0] and isinstance(row[0], str) and ': ' in row[0]:
                    current_class_code, current_class = self._parse_class_header(row[0])
                    continue
                
                # Process line items
                if current_class and self._is_line_item(row):
                    line_item = self._process_line_item(
                        row,
                        current_class_code,
                        current_class,
                        upload_id,
                        timestamp,
                        metadata['budget_name'],
                        metadata['user_email']
                    )
                    if line_item:
                        is_valid, validation_errors = validate_budget_row(line_item)
                        if is_valid:
                            processed_data.append(line_item)
                        else:
                            error_rows.append({
                                'row_number': row_idx,
                                'data': row,
                                'errors': validation_errors
                            })
        
        if error_rows:
            print("\nValidation errors found:")
            for error_row in error_rows:
                print(f"\nRow {error_row['row_number']}:")
                print(f"Data: {error_row['data']}")
                print(f"Errors: {error_row['errors']}")
            
            if not processed_data:
                raise ValueError("No valid rows found after validation")
        
        # Create DataFrame
        df = pd.DataFrame(processed_data)
        
        # Add metadata
        metadata_record = self._create_metadata_record(
            upload_id, timestamp, metadata
        )
        
        # Save to BigQuery
        self._save_to_bigquery(df, metadata_record)
        
        return upload_id

    def _extract_row_values(self, cells):
        """Extract values from Google Sheets cell data"""
        values = []
        for cell in cells:
            if 'effectiveValue' in cell:
                if 'numberValue' in cell['effectiveValue']:
                    values.append(cell['effectiveValue']['numberValue'])
                elif 'stringValue' in cell['effectiveValue']:
                    values.append(cell['effectiveValue']['stringValue'])
                else:
                    values.append(None)
            else:
                values.append(None)
        return values

    def _parse_class_header(self, header_text: str) -> tuple[Optional[str], str]:
        """Parse class code and name from header"""
        parts = header_text.split(': ', 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return None, header_text.strip()

    def _is_line_item(self, row: List[Any]) -> bool:
        """Check if row is a line item"""
        return bool(row[0]) and (
            str(row[0]).isdigit() or 
            (isinstance(row[0], str) and len(row[0]) > 1)
        )

    def _process_line_item(
        self, 
        row: List[Any], 
        class_code: str, 
        class_name: str, 
        upload_id: str, 
        timestamp: str,
        budget_name: str,
        user_email: str
    ) -> Optional[Dict[str, Any]]:
        """Process a single line item row"""
        try:
            return {
                'upload_id': upload_id,
                'user_email': user_email,
                'budget_name': budget_name,
                'upload_timestamp': timestamp,
                'class_code': class_code,
                'class_name': class_name,
                'line_item_number': safe_int_convert(row[0]),
                'line_item_description': str(row[1]).strip() if row[1] else None,
                'estimate_days': safe_float_convert(row[2]),
                'estimate_rate': safe_float_convert(row[3]),
                'estimate_total': safe_float_convert(row[4]),
                'actual_days': safe_float_convert(row[5]),
                'actual_rate': safe_float_convert(row[6]),
                'actual_total': safe_float_convert(row[7]),
                'raw_row_data': str(row)
            }
        except Exception as e:
            print(f"Error processing row: {row}. Error: {str(e)}")
            return None

    def _create_metadata_record(self, upload_id: str, timestamp: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Create metadata record"""
        return {
            'upload_id': upload_id,
            'upload_timestamp': timestamp,
            'budget_name': metadata['budget_name'],
            'version_status': metadata['version_status'],
            'project_name': metadata['project_name'],
            'project_start_date': metadata['project_start_date'],
            'project_end_date': metadata['project_end_date'],
            'client_name': metadata['client_name'],
            'producer_name': metadata['producer_name'],
            'user_email': metadata['user_email'],
            'version_notes': metadata.get('version_notes'),
            'previous_version_id': metadata.get('previous_version_id')
        }

    def _save_to_bigquery(self, df: pd.DataFrame, metadata_record: Dict[str, Any]) -> None:
        """Save processed data to BigQuery"""
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
        
        # Save to raw table
        raw_table_id = f"{self.project_id}.raw_budgets.raw_budget_data"
        job = self.client.load_table_from_dataframe(
            df, raw_table_id, job_config=job_config
        )
        job.result()

        # Save metadata
        metadata_table_id = f"{self.project_id}.raw_budgets.budget_metadata"
        metadata_df = pd.DataFrame([metadata_record])
        job = self.client.load_table_from_dataframe(
            metadata_df, metadata_table_id, job_config=job_config
        )
        job.result() 