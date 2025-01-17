"""
Budget processor service for handling AICP budget data.
"""
from datetime import datetime
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path
import json
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BudgetProcessor:
    """Processes AICP budget data from Google Sheets."""
    
    # Template structure constants
    CLASS_MAPPINGS = {
        'A': {
            'class_code_cell': 'L1',
            'class_name_cell': 'M1',
            'line_items_range': {'start': 'L4', 'end': 'S52'},
            'columns': {
                'estimate': {
                    'days': 'N',
                    'rate': 'O',
                    'total': 'P'
                },
                'actual': {
                    'days': 'Q',
                    'rate': 'R',
                    'total': 'S'
                }
            },
            'subtotal_cells': {'estimate': 'P53', 'actual': 'S53'},
            'pw_cells': {'label': 'O54', 'estimate': 'P54', 'actual': 'S54'},
            'total_cells': {'estimate': 'P55', 'actual': 'S55'},
            'required_fields': ['line_number', 'description']
        },
        'B': {
            'class_code_cell': 'T1',  # B: SHOOTING CREW
            'class_name_cell': 'U1',
            'line_items_range': {'start': 'T4', 'end': 'AC52'},
            'columns': {
                'estimate': {
                    'days': 'V',
                    'rate': 'W',
                    'ot_rate': 'X',
                    'ot_hours': 'Y',
                    'total': 'Z'
                },
                'actual': {
                    'days': 'AA',
                    'rate': 'AB',
                    'total': 'AC'
                }
            },
            'subtotal_cells': {
                'estimate': 'Z53',  # =SUM($Z$4:$Z$52)
                'actual': 'AC53'    # =SUM($AC$4:$AC$52)
            },
            'pw_cells': {
                'label': 'Y54',     # "P&W"
                'estimate': 'Z54',   # "28%"
                'actual': 'AC54'
            },
            'total_cells': {
                'estimate': 'Z55',   # =(Z53*Z54)+Z53
                'actual': 'AC55'     # =($AC$53+$AC$54)
            },
            'required_fields': ['line_number', 'description']
        }
    }
    
    def __init__(self, credentials_path: str):
        """Initialize with Google Sheets credentials."""
        try:
            logger.info(f"Loading credentials from: {credentials_path}")
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            logger.info(f"Using service account: {self.credentials.service_account_email}")
            
            self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
            logger.info("Successfully initialized Google Sheets service")
            
        except Exception as e:
            logger.error(f"Failed to initialize budget processor: {str(e)}")
            raise

    def _get_increment_number(self, file_name: str, sheet_name: str, date_str: str) -> int:
        """Get the next increment number for a given file/sheet combination."""
        try:
            # Create or load the increment tracking file
            tracking_file = Path('output/version_tracking.json')
            tracking_file.parent.mkdir(exist_ok=True)
            
            if tracking_file.exists():
                with open(tracking_file) as f:
                    tracking_data = json.load(f)
            else:
                tracking_data = {}
            
            # Create key for this file-sheet combination (without date)
            key = f"{file_name}-{sheet_name}"
            
            # If we have a new combination, start at 1
            if key not in tracking_data:
                tracking_data[key] = {
                    'current_increment': 1,
                    'first_seen': date_str,
                    'last_updated': date_str
                }
            else:
                tracking_data[key]['current_increment'] += 1
                tracking_data[key]['last_updated'] = date_str
            
            increment = tracking_data[key]['current_increment']
            
            # Save updated tracking data
            with open(tracking_file, 'w') as f:
                json.dump(tracking_data, f, indent=2)
            
            return increment
            
        except Exception as e:
            logger.warning(f"Error getting increment number: {e}. Using 1 as default.")
            return 1

    def _generate_upload_id(self, spreadsheet_title: str, sheet_title: str) -> str:
        """Generate a unique upload ID."""
        # Clean file and sheet names
        clean_file = '_'.join(''.join(c if c.isalnum() else ' ' for c in spreadsheet_title).split())
        clean_sheet = '_'.join(''.join(c if c.isalnum() else ' ' for c in sheet_title).split())
        
        # Generate date string
        date_str = datetime.now().strftime('%m-%d-%y')
        
        # Get increment number
        increment = self._get_increment_number(clean_file, clean_sheet, date_str)
        
        return f"{clean_file}-{clean_sheet}-{date_str}_{increment}"

    def _get_class_totals(self, spreadsheet_id: str, sheet_title: str, mapping: Dict) -> Dict:
        """Get class totals by reading directly from cells."""
        values = {}
        
        # Get subtotal values
        for key in ['estimate', 'actual']:
            cell = mapping['subtotal_cells'][key]
            cell_range = f"'{sheet_title}'!{cell}"
            result = self._get_range_values(spreadsheet_id, cell_range)
            value = result[0][0] if result and result[0] else "$0.00"
            values[f'class_{key}_subtotal'] = value
            if value == "$0.00":
                logger.debug(f"Empty or zero {key} subtotal value in cell {cell}")
        
        # Get P&W values
        for key in ['estimate', 'actual']:
            cell = mapping['pw_cells'][key]
            cell_range = f"'{sheet_title}'!{cell}"
            result = self._get_range_values(spreadsheet_id, cell_range)
            value = result[0][0] if result and result[0] else "$0.00"
            values[f'class_{key}_pnw'] = value
            if value == "$0.00":
                logger.debug(f"Empty or zero {key} P&W value in cell {cell}")
        
        # Get total values
        for key in ['estimate', 'actual']:
            cell = mapping['total_cells'][key]
            cell_range = f"'{sheet_title}'!{cell}"
            result = self._get_range_values(spreadsheet_id, cell_range)
            value = result[0][0] if result and result[0] else "$0.00"
            values[f'class_{key}_total'] = value
            if value == "$0.00":
                logger.debug(f"Empty or zero {key} total value in cell {cell}")
        
        return values

    def _validate_line_item(self, line_item: Dict, class_code: str, row_number: int) -> List[str]:
        """Validate a line item and return any validation messages."""
        messages = []
        required_fields = self.CLASS_MAPPINGS[class_code]['required_fields']
        
        # Check required fields
        for field in required_fields:
            if not line_item.get(field):
                messages.append(f"Missing required field: {field}")
        
        # Validate rate and days combinations
        if line_item.get('estimate_rate') and not line_item.get('estimate_days'):
            messages.append("Has estimate rate but missing days")
        
        if line_item.get('actual_rate') and not line_item.get('actual_days'):
            messages.append("Has actual rate but missing days")
        
        # Class B specific validations
        if class_code == 'B':
            if line_item.get('estimate_ot_rate') and not line_item.get('estimate_ot_hours'):
                messages.append("Has OT rate but missing hours")
            if line_item.get('estimate_ot_hours') and not line_item.get('estimate_ot_rate'):
                messages.append("Has OT hours but missing rate")
        
        # Log any validation issues
        if messages:
            logger.debug(f"Validation issues in row {row_number}: {', '.join(messages)}")
        
        return messages

    def _process_line_item(self, row: List[Any], class_code: str, class_name: str, class_totals: Dict, row_number: int) -> Optional[Dict]:
        """Process a single line item with class totals."""
        try:
            # Create base line item
            line_item = {
                'class_code': class_code,
                'class_name': class_name,
                'line_number': row[0],
                'description': row[1]
            }
            
            # Add values based on class type
            if class_code == 'A':
                line_item.update({
                    'estimate_days': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_days': row[5] if len(row) > 5 else None,
                    'actual_rate': row[6] if len(row) > 6 else None,
                    'actual_total': row[7] if len(row) > 7 else None
                })
            else:  # Class B
                line_item.update({
                    'estimate_days': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_ot_rate': row[4] if len(row) > 4 else None,
                    'estimate_ot_hours': row[5] if len(row) > 5 else None,
                    'estimate_total': row[6] if len(row) > 6 else None,
                    'actual_days': row[7] if len(row) > 7 else None,
                    'actual_rate': row[8] if len(row) > 8 else None,
                    'actual_total': row[9] if len(row) > 9 else None
                })
            
            # Add class totals
            line_item.update({
                'class_estimate_subtotal': class_totals['class_estimate_subtotal'],
                'class_estimate_pnw': class_totals['class_estimate_pnw'],
                'class_estimate_total': class_totals['class_estimate_total'],
                'class_actual_subtotal': class_totals['class_actual_subtotal'],
                'class_actual_pnw': class_totals['class_actual_pnw'],
                'class_actual_total': class_totals['class_actual_total']
            })
            
            # Validate line item
            validation_messages = self._validate_line_item(line_item, class_code, row_number)
            line_item['validation_status'] = 'valid' if not validation_messages else 'warning'
            line_item['validation_messages'] = validation_messages
            
            # Log empty or unusual values
            for key, value in line_item.items():
                if key not in ['validation_status', 'validation_messages'] and value in [None, '', '$0.00', '0', 0]:
                    logger.debug(f"Empty or zero value for {key} in row {row_number}")
            
            return line_item
            
        except Exception as e:
            logger.error(f"Error processing row {row_number}: {str(e)}")
            return None

    def _get_class_name(self, header_values: list, class_code: str, mapping: dict, spreadsheet_id: str, sheet_title: str) -> Optional[str]:
        """Extract class name from header values."""
        if not header_values:
            return None
            
        # First try to get from separate name cell
        if len(header_values[0]) > 1:
            return header_values[0][1]
            
        # If not found, try to extract from combined header
        header_text = header_values[0][0] if header_values[0] else ""
        if header_text.startswith(f"{class_code}:"):
            # Extract name part after the class code
            return header_text.split(":", 1)[1].strip()
            
        # If still not found, try reading name cell directly
        name_range = f"'{sheet_title}'!{mapping['class_name_cell']}"
        name_values = self._get_range_values(spreadsheet_id, name_range)
        if name_values and name_values[0]:
            return name_values[0][0]
            
        return None

    def process_sheet(self, spreadsheet_id: str, target_gid: str = None) -> tuple[list, dict]:
        """Process budget from Google Sheets"""
        try:
            # Get sheet info
            sheet_info = self._get_sheet_info(spreadsheet_id, target_gid)
            sheet_title = sheet_info['title']
            
            processed_rows = []
            validation_issues = []
            
            # Process each class section
            for class_code, mapping in self.CLASS_MAPPINGS.items():
                try:
                    # Get class header
                    header_range = f"'{sheet_title}'!{mapping['class_code_cell']}:{mapping['class_name_cell']}"
                    header_values = self._get_range_values(spreadsheet_id, header_range)
                    
                    # Debug header values
                    logger.info(f"Class {class_code} header range: {header_range}")
                    logger.info(f"Class {class_code} header values: {header_values}")
                    
                    if not header_values:
                        logger.warning(f"No header values found for class {class_code}")
                        continue
                    
                    # Get class name using the new helper method
                    class_name = self._get_class_name(header_values, class_code, mapping, spreadsheet_id, sheet_title)
                    if not class_name:
                        logger.warning(f"No class name found for class {class_code}")
                        continue
                        
                    # Get all data including calculated totals
                    data_range = f"'{sheet_title}'!{mapping['line_items_range']['start']}:{mapping['line_items_range']['end']}"
                    data_values = self._get_range_values(spreadsheet_id, data_range)
                    
                    if not data_values:
                        logger.warning(f"No data values found for class {class_code}")
                        continue
                    
                    # Get totals directly from cells
                    class_totals = self._get_class_totals(spreadsheet_id, sheet_title, mapping)
                    
                    # Process each line item
                    for row_number, row in enumerate(data_values, start=1):
                        if not row:  # Skip empty rows
                            continue
                            
                        if len(row) < 2:  # Need at least number and description
                            logger.debug(f"Incomplete row {row_number} in class {class_code}: {row}")
                            continue
                            
                        # Process line item with class totals
                        line_item = self._process_line_item(row, class_code, class_name, class_totals, row_number)
                        if line_item:
                            processed_rows.append(line_item)
                            if line_item['validation_messages']:
                                validation_issues.append(line_item['validation_messages'])
                        
                except Exception as e:
                    logger.error(f"Error processing class {class_code}: {str(e)}")
                    continue
                    
            # Create metadata
            metadata = {
                'upload_id': self._generate_upload_id(sheet_info['spreadsheet_title'], sheet_title),
                'upload_timestamp': datetime.now().isoformat(),
                'sheet_title': sheet_title,
                'sheet_gid': target_gid,
                'processed_rows': len(processed_rows),
                'processed_class_codes': list(set(row['class_code'] for row in processed_rows)),
                'validation_issues': len(validation_issues),
                'validation_messages': validation_issues if validation_issues else None
            }
            
            # Log processing summary
            logger.info(f"\nProcessing Summary:")
            logger.info(f"- Total rows processed: {len(processed_rows)}")
            logger.info(f"- Classes processed: {', '.join(metadata['processed_class_codes'])}")
            logger.info(f"- Validation issues: {len(validation_issues)}")
            
            return processed_rows, metadata
            
        except Exception as e:
            logger.error(f"Error processing sheet: {str(e)}")
            raise

    def _get_range_values(self, spreadsheet_id: str, range_name: str) -> list:
        """Fetch values for a specific range."""
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', [])

    def _get_sheet_info(self, spreadsheet_id: str, target_gid: str = None) -> dict:
        """Get sheet information including title and GID."""
        try:
            # Get spreadsheet info
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            # Get spreadsheet title
            spreadsheet_title = spreadsheet.get('properties', {}).get('title', 'Untitled')
            
            # Find sheet by GID if provided, otherwise use first sheet
            target_sheet = None
            if target_gid:
                for sheet in spreadsheet['sheets']:
                    if str(sheet['properties'].get('sheetId')) == target_gid:
                        target_sheet = sheet
                        break
            else:
                target_sheet = spreadsheet['sheets'][0]
            
            if not target_sheet:
                raise ValueError(f"Sheet with GID {target_gid} not found")
            
            return {
                'title': target_sheet['properties']['title'],
                'gid': str(target_sheet['properties']['sheetId']),
                'spreadsheet_title': spreadsheet_title
            }
            
        except Exception as e:
            logger.error(f"Error getting sheet info: {str(e)}")
            raise 