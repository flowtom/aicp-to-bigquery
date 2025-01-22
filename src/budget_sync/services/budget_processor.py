from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import uuid
import json
from typing import Dict, Any, List, Optional
from src.budget_sync.services.cover_sheet_processor import CoverSheetProcessor
from budget_sync.utils.data_validation import validate_budget_row, safe_float_convert, safe_int_convert
import logging

# Load configuration
with open('config/config.json', 'r') as f:
    config = json.load(f)

class BudgetProcessor:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client()
        
        # Set up Google Sheets credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            'config/service-account-key.json',
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        self.sheets_service = build('sheets', 'v4', credentials=self.credentials)

    def process_budget(self, spreadsheet_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Process budget from Google Sheets"""
        try:
            # Get spreadsheet data
            sheet = self.sheets_service.spreadsheets()
            result = sheet.get(spreadsheetId=spreadsheet_id, 
                             includeGridData=True).execute()
            
            # Process cover sheet
            cover_sheet_processor = CoverSheetProcessor(self.sheets_service)
            cover_sheet_data = cover_sheet_processor.extract_cover_sheet(
                result['sheets'][0]['data'][0]['rowData']
            )
            
            # Process line items
            line_items_data = self._process_line_items(result['sheets'][1]['data'][0]['rowData'])
            
            # Create metadata
            metadata_record = {
                'upload_id': str(uuid.uuid4()),
                'upload_timestamp': datetime.now().isoformat(),
                'budget_name': metadata['budget_name'],
                'version_status': metadata['version_status'],
                'cover_sheet': cover_sheet_data,
                'project_info': {
                    'name': metadata['project_name'],
                    'start_date': metadata['project_start_date'],
                    'end_date': metadata['project_end_date'],
                    'client_name': metadata['client_name'],
                    'producer_name': metadata['producer_name']
                },
                'processing_info': {
                    'user_email': metadata['user_email'],
                    'version_notes': metadata.get('version_notes'),
                    'previous_version_id': metadata.get('previous_version_id')
                }
            }
            
            return {
                'metadata': metadata_record,
                'rows': line_items_data['line_items'],
                'validation_errors': line_items_data['errors']
            }
            
        except Exception as e:
            logging.error(f"Error processing budget: {str(e)}")
            raise

    def _process_line_items(self, grid_data):
        """Process line items from the grid data."""
        line_items = []
        errors = []
        current_class = None
        current_class_code = None
        row_count = len(grid_data)
        
        logging.info(f"Processing {row_count} rows")
        
        for row_idx, row in enumerate(grid_data):
            try:
                # Skip empty rows
                if not row or len(row) < 2:
                    logging.debug(f"Row {row_idx + 1}: Empty row")
                    continue
                    
                # Get first two columns which determine the type of row
                col_a = str(row[0] or '').strip()
                col_b = str(row[1] or '').strip()
                
                # Skip if both columns are empty
                if not col_a and not col_b:
                    logging.debug(f"Row {row_idx + 1}: Empty row")
                    continue
                    
                # Log row for debugging
                logging.debug(f"Row {row_idx + 1}: {row}")
                
                # Check for class headers
                if col_a and not col_b and ':' in col_a:
                    current_class = col_a.split(':')[0].strip()
                    current_class_code = None
                    logging.info(f"Found class: {current_class}")
                    continue
                    
                # Skip header rows
                if any(header in col_a.upper() for header in ['ESTIMATE', 'DAYS', 'RATE', 'TOTAL']):
                    continue
                    
                # Process line items only if we have a current class
                if current_class and len(row) >= 4:
                    # Try to extract line number from first column
                    if col_a.isdigit():
                        current_class_code = col_a
                    
                    # Only process if we have a class code and description
                    if current_class_code and col_b:
                        line_item = {
                            'class': current_class,
                            'line_number': current_class_code,
                            'description': col_b,
                            'values': self._extract_row_values(row)
                        }
                        line_items.append(line_item)
                        
            except Exception as e:
                error = f"Error processing row {row_idx + 1}: {str(e)}"
                logging.error(error)
                errors.append(error)
                
        logging.info(f"Processed {len(line_items)} line items with {len(errors)} errors")
        return {'line_items': line_items, 'errors': errors}

    def _extract_row_values(self, row):
        """Extract numeric values from a row."""
        values = {}
        
        # Define value mappings
        value_mappings = {
            'days': 2,
            'rate': 3,
            'total': 4,
            'actual_days': 5,
            'actual_rate': 6,
            'actual_total': 7
        }
        
        for key, col_idx in value_mappings.items():
            if col_idx < len(row):
                try:
                    value = row[col_idx]
                    if value:
                        # Handle different value types
                        if isinstance(value, (int, float)):
                            values[key] = float(value)
                        elif isinstance(value, str):
                            # Remove currency symbols and commas
                            clean_value = value.replace('$', '').replace(',', '').strip()
                            if clean_value:
                                try:
                                    values[key] = float(clean_value)
                                except ValueError:
                                    pass
                except Exception as e:
                    logging.warning(f"Error extracting {key} from row: {str(e)}")
                    
        return values

    def _safe_float(self, value) -> Optional[float]:
        """Safely convert a value to float."""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                # Remove currency formatting
                value = value.replace('$', '').replace(',', '')
            return float(value) if str(value).strip() else None
        except (ValueError, TypeError):
            return None

    def _parse_class_header(self, header_text: str) -> tuple[Optional[str], str]:
        """Parse class code and name from header"""
        parts = header_text.split(': ', 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return None, header_text.strip()

    def _is_line_item(self, row: list) -> bool:
        """Check if row is a valid line item."""
        if not row or len(row) < 2:
            return False
        
        # Skip header rows
        if isinstance(row[0], str):
            header_terms = [
                'PRODUCTION COSTS SUMMARY',
                'ESTIMATE',
                'DAYS',
                'TOTAL',
                'SUB TOTAL',
                'P&W',
                'PAGE',
                'SUMMARY'
            ]
            if any(term.lower() in str(row[0]).lower() for term in header_terms):
                return False
        
        # Check if first column has a line number (numeric or string)
        first_col = str(row[0]).strip()
        if not first_col:
            return False
        
        # Check if second column has a description
        second_col = str(row[1]).strip()
        if not second_col:
            return False
        
        return True

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

    def _process_rows(self, rows: List[List[Any]]) -> List[Dict[str, Any]]:
        """Process rows from the budget sheet."""
        processed_rows = []
        for i, row in enumerate(rows):
            # Skip completely empty rows
            if not any(cell is not None and str(cell).strip() != '' for cell in row):
                continue
                
            logging.debug(f"Processing row {i}: {row}")
            
            if not self._is_line_item(row):
                logging.debug(f"Skipping non-line item row {i}")
                continue

            processed_row = self._process_row(row)
            if processed_row:
                processed_rows.append(processed_row)
                
        return processed_rows 