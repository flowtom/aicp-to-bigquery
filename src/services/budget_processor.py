"""
Budget processor service for handling AICP budget data.
"""
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ..utils.data_utils import safe_float_convert
from ..models.budget import Budget, BudgetClass, BudgetLineItem
import os
import json
from pathlib import Path

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
            'estimate_header_cells': {'start': 'N2', 'end': 'P2'},
            'actual_header_cells': {'start': 'Q2', 'end': 'S2'},
            'line_items_range': {'start': 'L4', 'end': 'S52'},
            'subtotal_cells': {'estimate': 'P53', 'actual': 'S53'},
            'pw_cells': {'label': 'O54', 'estimate': 'P54', 'actual': 'S54'},
            'total_cells': {'estimate': 'P55', 'actual': 'S55'}
        },
        'B': {
            'class_code_cell': 'T1',  # B: SHOOTING CREW
            'class_name_cell': 'U1',
            'estimate_header_cells': {'start': 'V2', 'end': 'Z2'},
            'actual_header_cells': {'start': 'AA2', 'end': 'AC2'},
            'line_items_range': {'start': 'T4', 'end': 'AC52'},
            'subtotal_cells': {'estimate': 'Z53', 'actual': 'AC53'},
            'pw_cells': {'label': 'Y54', 'estimate': 'Z54', 'actual': 'AC54'},
            'total_cells': {'estimate': 'Z55', 'actual': 'AC55'}
        }
    }
    
    # Column indices within line items range (0-based)
    COLUMN_INDICES = {
        'A': {
            'line_number': 0,      # L column
            'description': 1,      # M column
            'estimate_days': 2,    # N column
            'estimate_rate': 3,    # O column
            'estimate_total': 4,   # P column
            'actual_days': 5,      # Q column
            'actual_rate': 6,      # R column
            'actual_total': 7      # S column
        },
        'B': {
            'line_number': 0,      # T column
            'description': 1,      # U column
            'estimate_days': 2,    # V column
            'estimate_rate': 3,    # W column
            'estimate_ot_rate': 4, # X column
            'estimate_ot_hours': 5,# Y column
            'estimate_total': 6,   # Z column
            'actual_days': 7,      # AA column
            'actual_rate': 8,      # AB column
            'actual_total': 9      # AC column
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

    def _generate_upload_id(self, spreadsheet_title: str, sheet_title: str, version_status: str = 'Draft') -> str:
        """Generate a unique upload ID.
        
        Format: fileName-sheetName-MM-dd-yy-increment
        Example: GOOG0324PIXELDR_Estimate-Brand_&_DR_Combined-01-17-25_1
        """
        # Clean file and sheet names (replace spaces and special chars with underscores)
        clean_file = '_'.join(''.join(c if c.isalnum() else ' ' for c in spreadsheet_title).split())
        clean_sheet = '_'.join(''.join(c if c.isalnum() else ' ' for c in sheet_title).split())
        
        # Generate date string
        date_str = datetime.now().strftime('%m-%d-%y')
        
        # Get increment number (passing date for tracking purposes only)
        increment = self._get_increment_number(clean_file, clean_sheet, date_str)
        
        return f"{clean_file}-{clean_sheet}-{date_str}_{increment}"

    def process_sheet(self, spreadsheet_id: str, range_name: str, target_gid: str = "590213670") -> Tuple[List[Dict], Dict]:
        """Process the AICP budget sheet.
        
        Args:
            spreadsheet_id: The ID of the spreadsheet
            range_name: The range to process
            target_gid: The GID of the sheet to process (default: 590213670)
        """
        try:
            # Verify access and fetch data
            logger.info(f"Attempting to access spreadsheet: {spreadsheet_id}")
            try:
                spreadsheet = self.sheets_service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute()
                
                # Find sheet by GID
                target_sheet = None
                for sheet in spreadsheet['sheets']:
                    if str(sheet['properties'].get('sheetId')) == target_gid:
                        target_sheet = sheet
                        break
                
                if not target_sheet:
                    raise ValueError(f"Sheet with GID {target_gid} not found")
                
                sheet_title = target_sheet['properties']['title']
                spreadsheet_title = spreadsheet.get('properties', {}).get('title', 'Untitled')
                logger.info(f"Using sheet: {sheet_title} (GID: {target_gid})")
                
                logger.info(f"Successfully accessed spreadsheet: {spreadsheet_title}")
            except HttpError as e:
                if e.resp.status == 404:
                    logger.error(f"Spreadsheet not found. Please check the spreadsheet ID: {spreadsheet_id}")
                elif e.resp.status == 403:
                    logger.error(f"Access denied. Please share the spreadsheet with: {self.credentials.service_account_email}")
                raise
            
            # Initialize budget with enhanced upload_id
            version_status = 'Draft'  # This could come from config or parameters
            upload_id = self._generate_upload_id(spreadsheet_title, sheet_title, version_status)
            
            budget = Budget(
                upload_id=upload_id,
                budget_name=sheet_title,
                version_status=version_status,
                upload_timestamp=datetime.now(),
                classes={}
            )
            
            # Process each class
            for class_code in ['A', 'B']:  # Add classes in order
                section_data = self._process_class(spreadsheet_id, class_code, sheet_title)
                if section_data:
                    logger.info(f"Processing class {section_data['class_code']}: {section_data['class_name']}")
                    
                    # Create budget class
                    budget_class = BudgetClass(
                        code=section_data['class_code'],
                        name=section_data['class_name'],
                        line_items=[]
                    )
                    
                    # Process line items
                    for raw_item in section_data['line_items']:
                        if raw_item['id'] and raw_item['description']:
                            line_item = BudgetLineItem(
                                number=int(raw_item['id']),
                                description=raw_item['description'],
                                estimate_days=raw_item['estimate']['days'],
                                estimate_rate=raw_item['estimate']['rate'],
                                actual_days=raw_item['actual']['days'],
                                actual_rate=raw_item['actual']['rate'],
                                estimate_ot_rate=raw_item['estimate'].get('ot_rate'),
                                estimate_ot_hours=raw_item['estimate'].get('ot_hours')
                            )
                            budget_class.line_items.append(line_item)
                    
                    # Only add class if it has line items
                    if budget_class.line_items:
                        budget.classes[budget_class.code] = budget_class
                        logger.info(f"Added class {budget_class.code} with {len(budget_class.line_items)} line items")
            
            # Validate budget after all classes are added
            budget._validate()
            
            # Convert to BigQuery rows
            processed_rows = budget.to_bigquery_rows()
            
            # Create enhanced metadata
            validation_summary = budget.validation_summary
            metadata = {
                'upload_id': budget.upload_id,
                'upload_timestamp': budget.upload_timestamp.isoformat(),
                'version_status': budget.version_status,
                'processed_rows': len(processed_rows),
                'total_estimate': budget.total_estimate,
                'processed_class_codes': budget.processed_class_codes,
                'total_line_items': budget.total_line_items,
                'has_actuals': budget.has_actuals,
                'sheet_info': {
                    'title': sheet_title,
                    'gid': target_gid
                },
                'validation': {
                    'is_valid': validation_summary['is_valid'],
                    'missing_days': validation_summary['missing_days'],
                    'has_actuals': validation_summary['has_actuals'],
                    'messages': validation_summary['messages'],
                    'class_summaries': validation_summary['class_summaries']
                }
            }
            
            # Log processing summary
            logger.info("Processing complete:")
            logger.info(f"- Sheet: {sheet_title} (GID: {target_gid})")
            logger.info(f"- Processed rows: {metadata['processed_rows']}")
            logger.info(f"- Total estimate: ${metadata['total_estimate']:,.2f}")
            logger.info(f"- Processed classes: {', '.join(metadata['processed_class_codes'])}")
            logger.info(f"- Total line items: {metadata['total_line_items']}")
            
            # Log validation details and class summaries
            logger.info("\nValidation Summary:")
            if not validation_summary['is_valid']:
                for msg in validation_summary['messages']:
                    logger.warning(f"  - {msg}")
            
            # Always show class summaries
            for class_code, summary in validation_summary['class_summaries'].items():
                logger.info(f"\nClass {class_code} Summary:")
                logger.info(f"  Total items: {summary['total_items']}")
                logger.info(f"  Items with rates: {summary['items_with_rates']}")
                logger.info(f"  Items with days: {summary['items_with_days']}")
                logger.info(f"  Complete items: {summary['items_complete']}")
                logger.info(f"  Estimate total: ${summary['estimate_total']:,.2f}")
                logger.info(f"  Actual total: ${summary['actual_total']:,.2f}")
                if summary['messages']:
                    for msg in summary['messages']:
                        logger.warning(f"  - {msg}")
            
            return processed_rows, metadata
            
        except Exception as e:
            logger.error(f"Error processing sheet: {str(e)}", exc_info=True)
            raise

    def _process_class(self, spreadsheet_id: str, class_code: str, sheet_title: str) -> Optional[Dict]:
        """Process a specific class section using template structure."""
        try:
            mapping = self.CLASS_MAPPINGS[class_code]
            
            # Format ranges with sheet name properly
            class_range = f"'{sheet_title}'!{mapping['class_code_cell']}:{mapping['class_name_cell']}"
            class_header = self._get_range_values(spreadsheet_id, class_range)
            
            if not class_header or not class_header[0]:
                logger.warning(f"No header found for class {class_code}")
                return None
            
            # Extract class name, handling potential empty cells
            class_name = ''
            if len(class_header[0]) > 1:
                class_name = class_header[0][1]
            elif len(class_header[0]) == 1:
                # For Class B, sometimes the name is in the first cell
                class_name = class_header[0][0]
                if class_name == class_code:  # Don't use if it's just the class code
                    class_name = ''
            
            # Get line items with proper sheet name formatting
            items_range = f"'{sheet_title}'!{mapping['line_items_range']['start']}:{mapping['line_items_range']['end']}"
            line_items = self._get_range_values(spreadsheet_id, items_range)
            
            return {
                'class_code': class_code,
                'class_name': class_name.strip(),
                'line_items': self._process_line_items(line_items, class_code)
            }
            
        except Exception as e:
            logger.error(f"Error processing class {class_code}: {str(e)}")
            return None

    def _process_line_items(self, raw_items: List[List], class_code: str) -> List[Dict]:
        """Process line items using template column structure."""
        processed_items = []
        indices = self.COLUMN_INDICES[class_code]
        
        for row in raw_items:
            if not row or not row[0]:  # Skip empty rows
                continue
            
            try:
                # Extract raw values based on class
                if class_code == 'A':
                    processed_item = self._process_class_a_line_item(row, indices)
                elif class_code == 'B':
                    processed_item = self._process_class_b_line_item(row, indices)
                
                if processed_item:
                    processed_items.append(processed_item)
                    
            except (IndexError, ValueError) as e:
                logger.warning(f"Error processing line item row: {e}")
                continue
        
        return processed_items

    def _process_class_a_line_item(self, row: List, indices: Dict) -> Dict:
        """Process a Class A line item."""
        return {
            'id': row[indices['line_number']],
            'description': row[indices['description']],
            'estimate': {
                'days': safe_float_convert(row[indices['estimate_days']]),
                'rate': safe_float_convert(row[indices['estimate_rate']])
            },
            'actual': {
                'days': safe_float_convert(row[indices['actual_days']]),
                'rate': safe_float_convert(row[indices['actual_rate']])
            }
        }

    def _process_class_b_line_item(self, row: List, indices: Dict) -> Dict:
        """Process a Class B line item with OT calculations."""
        try:
            # Get all the values using indices
            estimate_days = safe_float_convert(row[indices['estimate_days']])
            estimate_rate = safe_float_convert(row[indices['estimate_rate']])
            estimate_ot_rate = safe_float_convert(row[indices['estimate_ot_rate']])
            estimate_ot_hours = safe_float_convert(row[indices['estimate_ot_hours']])
            
            return {
                'id': row[indices['line_number']],
                'description': row[indices['description']],
                'estimate': {
                    'days': estimate_days,
                    'rate': estimate_rate,
                    'ot_rate': estimate_ot_rate,
                    'ot_hours': estimate_ot_hours
                },
                'actual': {
                    'days': safe_float_convert(row[indices['actual_days']]),
                    'rate': safe_float_convert(row[indices['actual_rate']])
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing Class B line item: {str(e)}")
            return None

    def _get_range_values(self, spreadsheet_id: str, range_name: str) -> List[List]:
        """Fetch values for a specific range."""
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', []) 