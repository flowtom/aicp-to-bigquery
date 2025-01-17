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
        }
        # Add other class mappings as needed
    }
    
    # Column indices within line items range (0-based)
    COLUMN_INDICES = {
        'line_number': 0,      # L column
        'description': 1,      # M column
        'estimate_days': 2,    # N column
        'estimate_rate': 3,    # O column
        'estimate_total': 4,   # P column
        'actual_days': 5,      # Q column
        'actual_rate': 6,      # R column
        'actual_total': 7      # S column
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

    def process_sheet(self, spreadsheet_id: str, range_name: str) -> Tuple[List[Dict], Dict]:
        """Process the AICP budget sheet."""
        try:
            # Verify access and fetch data
            logger.info(f"Attempting to access spreadsheet: {spreadsheet_id}")
            try:
                spreadsheet = self.sheets_service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute()
                logger.info(f"Successfully accessed spreadsheet: {spreadsheet.get('properties', {}).get('title')}")
            except HttpError as e:
                if e.resp.status == 404:
                    logger.error(f"Spreadsheet not found. Please check the spreadsheet ID: {spreadsheet_id}")
                elif e.resp.status == 403:
                    logger.error(f"Access denied. Please share the spreadsheet with: {self.credentials.service_account_email}")
                raise
            
            # Initialize budget
            upload_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            budget = Budget(
                upload_id=upload_id,
                budget_name='Test Budget',  # This should come from config
                version_status='Draft',
                upload_timestamp=datetime.now(),
                classes={}  # Initialize empty dict that we'll populate
            )
            
            # Process Section A (for now)
            section_data = self._process_class(spreadsheet_id, 'A')
            if section_data:
                logger.info(f"Processing class {section_data['class_code']}: {section_data['class_name']}")
                
                # Create budget class
                budget_class = BudgetClass(
                    code=section_data['class_code'],
                    name=section_data['class_name'],
                    line_items=[]  # Initialize empty list
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
                            actual_rate=raw_item['actual']['rate']
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
            logger.info(f"- Processed rows: {metadata['processed_rows']}")
            logger.info(f"- Total estimate: ${metadata['total_estimate']:,.2f}")
            logger.info(f"- Processed classes: {', '.join(metadata['processed_class_codes'])}")
            logger.info(f"- Total line items: {metadata['total_line_items']}")
            
            # Log validation details
            if not validation_summary['is_valid']:
                logger.warning("Validation issues found:")
                for msg in validation_summary['messages']:
                    logger.warning(f"  - {msg}")
                
                # Log class-specific summaries
                for class_code, summary in validation_summary['class_summaries'].items():
                    logger.info(f"\nClass {class_code} Summary:")
                    logger.info(f"  Total items: {summary['total_items']}")
                    logger.info(f"  Items with rates: {summary['items_with_rates']}")
                    logger.info(f"  Items with days: {summary['items_with_days']}")
                    logger.info(f"  Complete items: {summary['items_complete']}")
                    if summary['messages']:
                        for msg in summary['messages']:
                            logger.warning(f"  - {msg}")
            
            return processed_rows, metadata
            
        except Exception as e:
            logger.error(f"Error processing sheet: {str(e)}", exc_info=True)
            raise

    def _process_class(self, spreadsheet_id: str, class_code: str) -> Optional[Dict]:
        """Process a specific class section using template structure."""
        try:
            mapping = self.CLASS_MAPPINGS[class_code]
            
            # Get class header info
            class_range = f"{mapping['class_code_cell']}:{mapping['class_name_cell']}"
            class_header = self._get_range_values(spreadsheet_id, class_range)
            
            if not class_header:
                logger.warning(f"No header found for class {class_code}")
                return None
            
            # Get line items
            items_range = f"{mapping['line_items_range']['start']}:{mapping['line_items_range']['end']}"
            line_items = self._get_range_values(spreadsheet_id, items_range)
            
            return {
                'class_code': class_code,
                'class_name': class_header[0][1] if len(class_header[0]) > 1 else '',
                'line_items': self._process_line_items(line_items)
            }
            
        except Exception as e:
            logger.error(f"Error processing class {class_code}: {str(e)}")
            return None

    def _process_line_items(self, raw_items: List[List]) -> List[Dict]:
        """Process line items using template column structure."""
        processed_items = []
        
        for row in raw_items:
            if not row or not row[0]:  # Skip empty rows
                continue
            
            try:
                # Extract raw values
                days = safe_float_convert(row[self.COLUMN_INDICES['estimate_days']])
                rate = safe_float_convert(row[self.COLUMN_INDICES['estimate_rate']])
                
                # Handle actual values
                actual_days = safe_float_convert(row[self.COLUMN_INDICES['actual_days']])
                actual_rate = safe_float_convert(row[self.COLUMN_INDICES['actual_rate']])
                
                processed_items.append({
                    'id': row[self.COLUMN_INDICES['line_number']],
                    'description': row[self.COLUMN_INDICES['description']],
                    'estimate': {
                        'days': days,
                        'rate': rate
                    },
                    'actual': {
                        'days': actual_days,
                        'rate': actual_rate
                    }
                })
            except (IndexError, ValueError) as e:
                logger.warning(f"Error processing line item row: {e}")
                continue
        
        return processed_items

    def _get_range_values(self, spreadsheet_id: str, range_name: str) -> List[List]:
        """Fetch values for a specific range."""
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        return result.get('values', []) 