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
import time

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
        },
        'C': {
            'class_code_cell': 'AD1',
            'class_name_cell': 'AD1',  # Combined in same cell as code
            'line_items_range': {'start': 'AD3', 'end': 'AJ15'},
            'columns': {
                'estimate': {
                    'number': 'AF',
                    'days': 'AG',
                    'rate': 'AH',
                    'total': 'AI'
                },
                'actual': {
                    'total': 'AJ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AI16',
                'actual': 'AJ16'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class C
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AI16',  # Same as subtotal since no P&W
                'actual': 'AJ16'
            },
            'required_fields': ['line_number', 'description']
        },
        'D': {
            'class_code_cell': 'AD18',
            'class_name_cell': 'AD18',  # Combined in same cell as code
            'line_items_range': {'start': 'AD20', 'end': 'AJ44'},
            'columns': {
                'estimate': {
                    'number': 'AF',
                    'days': 'AG',
                    'rate': 'AH',
                    'total': 'AI'
                },
                'actual': {
                    'total': 'AJ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AI45',
                'actual': 'AJ45'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class D
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AI45',  # Same as subtotal since no P&W
                'actual': 'AJ45'
            },
            'required_fields': ['line_number', 'description']
        },
        'E': {
            'class_code_cell': 'AD47',
            'class_name_cell': 'AD47',  # Combined in same cell as code
            'line_items_range': {'start': 'AD49', 'end': 'AJ59'},
            'columns': {
                'estimate': {
                    'number': 'AF',
                    'days': 'AG',
                    'rate': 'AH',
                    'total': 'AI'
                },
                'actual': {
                    'total': 'AJ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AI60',
                'actual': 'AJ60'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class E
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AI60',  # Same as subtotal since no P&W
                'actual': 'AJ60'
            },
            'required_fields': ['line_number', 'description']
        },
        'F': {
            'class_code_cell': 'AK1',
            'class_name_cell': 'AL1',  # Combined in same cell as code
            'line_items_range': {'start': 'AK19', 'end': 'AR19'},
            'columns': {
                'estimate': {
                    'number': 'AM',  # # column
                    'rate': 'AN',
                    'total': 'AO'
                },
                'actual': {
                    'total': 'AR'
                }
            },
            'subtotal_cells': {
                'estimate': 'AO20',
                'actual': 'AR20'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class F
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AO20',  # Same as subtotal since no P&W
                'actual': 'AR20'
            },
            'required_fields': ['line_number', 'description']
        },
        'G': {
            'class_code_cell': 'AK22',
            'class_name_cell': 'AL22',  # "G: ART DEPT LABOR"
            'line_items_range': {'start': 'AK24', 'end': 'AR35'},
            'columns': {
                'estimate': {
                    'days': 'AM',
                    'rate': 'AN',
                    'total': 'AO'
                },
                'actual': {
                    'total': 'AR'
                }
            },
            'subtotal_cells': {
                'estimate': 'AO36',
                'actual': 'AR36'
            },
            'pw_cells': {
                'label': 'AN37',  # P&W label
                'estimate': 'AO37',  # Estimate P&W
                'actual': 'AR37'   # Actual P&W
            },
            'total_cells': {
                'estimate': 'AO38',  # Total after P&W
                'actual': 'AR38'    # Total after P&W
            },
            'required_fields': ['line_number', 'description']
        },
        'H': {
            'class_code_cell': 'AK40',
            'class_name_cell': 'AL40',  # Combined in same cell as code
            'line_items_range': {'start': 'AK42', 'end': 'AR53'},
            'columns': {
                'estimate': {
                    'number': 'AM',
                    'rate': 'AN',
                    'total': 'AO'
                },
                'actual': {
                    'total': 'AR'
                }
            },
            'subtotal_cells': {
                'estimate': 'AO54',
                'actual': 'AR54'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class H
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AO54',  # Same as subtotal since no P&W
                'actual': 'AR54'
            },
            'required_fields': ['line_number', 'description']
        },
        'I': {
            'class_code_cell': 'AS1',
            'class_name_cell': 'AT1',  # Combined in same cell as code
            'line_items_range': {'start': 'AS3', 'end': 'AZ20'},
            'columns': {
                'estimate': {
                    'number': 'AU',
                    'days': 'AV',
                    'rate': 'AW',
                    'total': 'AX'
                },
                'actual': {
                    'total': 'AZ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AX21',
                'actual': 'AZ21'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class I
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AX21',  # Same as subtotal since no P&W
                'actual': 'AZ21'
            },
            'required_fields': ['line_number', 'description']
        },
        'J': {
            'class_code_cell': 'AS23',
            'class_name_cell': 'AT23',  # Combined in same cell as code
            'line_items_range': {'start': 'AS25', 'end': 'AZ30'},
            'columns': {
                'estimate': {
                    'number': 'AU',
                    'days': 'AV',
                    'rate': 'AW',
                    'total': 'AX'
                },
                'actual': {
                    'total': 'AZ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AX31',
                'actual': 'AZ31'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class J
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AX31',  # Same as subtotal since no P&W
                'actual': 'AZ31'
            },
            'required_fields': ['line_number', 'description']
        },
        'K': {
            'class_code_cell': 'AS33',
            'class_name_cell': 'AT33',  # Combined in same cell as code
            'line_items_range': {'start': 'AS35', 'end': 'BA46'},  # Updated end row to 46
            'columns': {
                'estimate': {
                    'hours': 'AU',
                    'rate': 'AV',
                    'total': 'AW'
                },
                'actual': {
                    'hours': 'AX',
                    'total': 'AY'
                }
            },
            'subtotal_cells': {
                'estimate': 'AX47',  # Updated to correct cell
                'actual': 'AZ47'     # Updated to correct cell
            },
            'pw_cells': {
                'label': None,  # No P&W for Class K
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AX47',  # Same as subtotal since no P&W
                'actual': 'AZ47'     # Same as subtotal since no P&W
            },
            'client_total_cell': 'BA47',  # Client subtotal
            'required_fields': ['line_number', 'description']
        },
        'L': {
            'class_code_cell': 'AS49',
            'class_name_cell': 'AT49',  # Combined in same cell as code
            'line_items_range': {'start': 'AS51', 'end': 'BA55'},
            'columns': {
                'estimate': {
                    'days': 'AV',
                    'rate': 'AW',
                    'total': 'AX'
                },
                'actual': {
                    'hours': 'AY',
                    'total': 'AZ'
                }
            },
            'subtotal_cells': {
                'estimate': 'AX56',
                'actual': 'AZ56'
            },
            'pw_cells': {
                'label': None,  # No P&W for Class L
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'AX56',  # Same as subtotal since no P&W
                'actual': 'AZ56'
            },
            'client_total_cell': 'BA56',  # Client subtotal
            'required_fields': ['line_number', 'description']
        },
        'M': {
            'class_code_cell': 'BB1',
            'class_name_cell': 'BB1',  # Combined in same cell as code
            'line_items_range': {'start': 'BB3', 'end': 'BH33'},
            'columns': {
                'estimate': {
                    'number': 'BD',  # # column
                    'days': 'BE',    # DAYS
                    'rate': 'BF',    # RATE
                    'total': 'BG'    # Total
                },
                'actual': {
                    'total': 'BH'    # Total
                }
            },
            'subtotal_cells': {
                'estimate': 'BG34',  # Estimated subtotal
                'actual': 'BH34'     # Actual subtotal
            },
            'pw_cells': {
                'label': 'BF35',     # P&W label
                'estimate': 'BG35',  # P&W estimate
                'actual': 'BH35'     # P&W actual
            },
            'total_cells': {
                'estimate': 'BG36',  # Estimate Total after P&W
                'actual': 'BH36'     # Actual Total after P&W
            },
            'required_fields': ['line_number', 'description']
        },
        'M2': {  # Additional Talent Expenses
            'class_code_cell': 'BB38',
            'class_name_cell': 'BB38',  # Combined in same cell
            'line_items_range': {'start': 'BB40', 'end': 'BH44'},  # Line items range
            'columns': {
                'estimate': {
                    'number': 'BH35',  # Number column
                    'days': 'BH36',    # Days column
                    'rate': 'BH37',    # Rate column
                    'total': 'BH38'    # Total column
                },
                'actual': {
                    'total': 'BH39'    # Actual total column
                }
            },
            'subtotal_cells': {
                'estimate': 'BG45',  # Estimated Total
                'actual': 'BH45'     # Actual Total
            },
            'pw_cells': {
                'label': None,  # No P&W for Additional Talent
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'BG45',  # Same as subtotal since no P&W
                'actual': 'BH45'     # Same as subtotal since no P&W
            },
            'required_fields': ['line_number', 'description']
        },
        'N': {  # Talent Expenses
            'class_code_cell': 'BB47',
            'class_name_cell': 'BB47',  # Combined in same cell
            'line_items_range': {'start': 'BB49', 'end': 'BH54'},  # Line items range
            'columns': {
                'estimate': {
                    'number': 'BD',  # # column
                    'days': 'BE',    # Days column
                    'rate': 'BF',    # Rate column
                    'total': 'BG'    # Total column
                },
                'actual': {
                    'total': 'BH'    # Total column
                }
            },
            'subtotal_cells': {
                'estimate': 'BG55',  # Estimated subtotal
                'actual': 'BH55'     # Actual subtotal
            },
            'pw_cells': {
                'label': None,  # No P&W for Class N
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'BG55',  # Same as subtotal since no P&W
                'actual': 'BH55'     # Same as subtotal since no P&W
            },
            'required_fields': ['line_number', 'description']
        },
        'O': {  # Agency Services
            'class_code_cell': 'BI1',
            'class_name_cell': 'BI1',  # Combined in same cell
            'line_items_range': {'start': 'BI3', 'end': 'BP37'},  # Line items range
            'columns': {
                'estimate': {
                    'hours': 'BK',    # Hours column
                    'rate': 'BL',     # Rate column
                    'total': 'BM'     # Total column
                },
                'actual': {
                    'hours': 'BN',    # Hours column
                    'total': 'BO'     # Total column
                }
            },
            'subtotal_cells': {
                'estimate': 'BM38',  # Estimated subtotal
                'actual': 'BO38'     # Actual subtotal
            },
            'pw_cells': {
                'label': None,  # No P&W for Class O
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'BM38',  # Same as subtotal since no P&W
                'actual': 'BO38'     # Same as subtotal since no P&W
            },
            'client_total_cell': 'BP38',  # Client total
            'required_fields': ['line_number', 'description']
        },
        'P': {  # Post Expenses
            'class_code_cell': 'BI40',
            'class_name_cell': 'BI40',  # Combined in same cell
            'line_items_range': {'start': 'BI42', 'end': 'BO51'},  # Line items range
            'columns': {
                'estimate': {
                    'hours': 'BK',    # Hours column
                    'rate': 'BL',     # Rate column
                    'total': 'BM'     # Total column
                },
                'actual': {
                    'hours': 'BN',    # Hours column
                    'total': 'BO'     # Total column
                }
            },
            'subtotal_cells': {
                'estimate': 'BM52',  # Estimated subtotal
                'actual': 'BO52'     # Actual subtotal
            },
            'pw_cells': {
                'label': None,  # No P&W for Class P
                'estimate': None,
                'actual': None
            },
            'total_cells': {
                'estimate': 'BM52',  # Same as subtotal since no P&W
                'actual': 'BO52'     # Same as subtotal since no P&W
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
        """Get class totals using batch request."""
        values = {}
        ranges = []
        
        # Collect all ranges we need to fetch
        for key in ['estimate', 'actual']:
            # Subtotal cells
            ranges.append(f"'{sheet_title}'!{mapping['subtotal_cells'][key]}")
            
            # P&W cells if they exist
            if mapping['pw_cells'].get('estimate') is not None:
                ranges.append(f"'{sheet_title}'!{mapping['pw_cells'][key]}")
                
            # Total cells
            ranges.append(f"'{sheet_title}'!{mapping['total_cells'][key]}")
        
        # Add client total cell for Class K
        if mapping.get('client_total_cell'):
            ranges.append(f"'{sheet_title}'!{mapping['client_total_cell']}")
        
        # Fetch all ranges in one batch request
        results = self._get_range_values_batch(spreadsheet_id, ranges)
        
        # Process results
        result_index = 0
        
        # Get subtotal values
        for key in ['estimate', 'actual']:
            value = results[result_index][0][0] if results[result_index] and results[result_index][0] else "$0.00"
            # Ensure proper dollar sign formatting
            if not isinstance(value, str) or not value.startswith('$'):
                value = f"${value}" if value else "$0.00"
            values[f'class_{key}_subtotal'] = value
            result_index += 1
        
        # Get P&W values if they exist
        if mapping['pw_cells'].get('estimate') is not None:
            for key in ['estimate', 'actual']:
                value = results[result_index][0][0] if results[result_index] and results[result_index][0] else "$0.00"
                # Ensure proper dollar sign formatting
                if not isinstance(value, str) or not value.startswith('$'):
                    value = f"${value}" if value else "$0.00"
                values[f'class_{key}_pnw'] = value
                result_index += 1
        else:
            values['class_estimate_pnw'] = None
            values['class_actual_pnw'] = None
        
        # Get total values
        for key in ['estimate', 'actual']:
            value = results[result_index][0][0] if results[result_index] and results[result_index][0] else "$0.00"
            # Ensure proper dollar sign formatting
            if not isinstance(value, str) or not value.startswith('$'):
                value = f"${value}" if value else "$0.00"
            values[f'class_{key}_total'] = value
            result_index += 1
            
        # Get client total for Class K
        if mapping.get('client_total_cell'):
            value = results[result_index][0][0] if results[result_index] and results[result_index][0] else "$0.00"
            # Ensure proper dollar sign formatting
            if not isinstance(value, str) or not value.startswith('$'):
                value = f"${value}" if value else "$0.00"
            values['class_client_total'] = value
        
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
            # Clean class name - remove class code prefix if present
            if class_name and ':' in class_name:
                class_name = class_name.split(':', 1)[1].strip()

            # Create base line item
            line_item = {
                'class_code': class_code,
                'class_name': class_name,
                'line_number': row[0],
                'description': row[1]
            }
            
            # Add values based on class type
            if class_code == 'M2':  # Additional Talent Expenses
                # Get raw values first
                estimate_number = row[2] if len(row) > 2 else None
                estimate_days = row[3] if len(row) > 3 else None
                estimate_rate = row[4] if len(row) > 4 else None
                estimate_total = row[5] if len(row) > 5 else None
                actual_total = row[6] if len(row) > 6 else None
                
                # Update line item with properly formatted values
                line_item.update({
                    'estimate_number': estimate_number,  # Keep as is for number
                    'estimate_days': estimate_days,      # Keep as is for days
                    'estimate_rate': estimate_rate,      # Keep as is for rate
                    'estimate_total': estimate_total if isinstance(estimate_total, str) and estimate_total.startswith('$') else f"${estimate_total}" if estimate_total else None,
                    'actual_total': actual_total if isinstance(actual_total, str) and actual_total.startswith('$') else f"${actual_total}" if actual_total else None
                })
                
                # Don't validate days for percentage rates
                if line_item.get('estimate_rate') and not line_item.get('estimate_days') and not str(line_item.get('estimate_rate')).endswith('%'):
                    line_item.setdefault('validation_messages', []).append("Has estimate rate but missing days")
            elif class_code == 'L':  # Class L has days and hours
                # Get raw values first
                estimate_days = row[2] if len(row) > 2 else None
                estimate_rate = row[3] if len(row) > 3 else None
                estimate_total = row[4] if len(row) > 4 else None
                actual_hours = row[5] if len(row) > 5 else None
                actual_total = row[6] if len(row) > 6 else None
                
                # Update line item with properly formatted values
                line_item.update({
                    'estimate_days': estimate_days,  # Keep as is for days
                    'estimate_rate': estimate_rate,  # Keep as is for rate
                    'estimate_total': estimate_total if isinstance(estimate_total, str) and estimate_total.startswith('$') else f"${estimate_total}" if estimate_total else None,
                    'actual_hours': actual_hours,    # Keep as is for hours
                    'actual_total': actual_total if isinstance(actual_total, str) and actual_total.startswith('$') else f"${actual_total}" if actual_total else None
                })
                
                # Add class client total if available
                if 'class_client_total' in class_totals:
                    line_item['class_client_total'] = class_totals['class_client_total']
                
                # Validate days for Class L
                if line_item.get('estimate_rate') and not line_item.get('estimate_days'):
                    line_item.setdefault('validation_messages', []).append("Has estimate rate but missing days")
            elif class_code == 'K':
                line_item.update({
                    'estimate_hours': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_hours': row[5] if len(row) > 5 else None,
                    'actual_total': row[6] if len(row) > 6 else None
                })
            elif class_code == 'A':
                line_item.update({
                    'estimate_days': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_days': row[5] if len(row) > 5 else None,
                    'actual_rate': row[6] if len(row) > 6 else None,
                    'actual_total': row[7] if len(row) > 7 else None
                })
            elif class_code == 'B':
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
            elif class_code in ['F', 'H']:  # Classes F and H have number, rate, total
                line_item.update({
                    'estimate_number': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_total': row[5] if len(row) > 5 else None
                })
            elif class_code == 'G':  # Class G has days, rate, total in estimate
                line_item.update({
                    'estimate_days': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_total': row[5] if len(row) > 5 else None
                })
            elif class_code == 'I':  # Class I has number, days, rate, total
                line_item.update({
                    'estimate_number': row[2] if len(row) > 2 else None,
                    'estimate_days': row[3] if len(row) > 3 else None,
                    'estimate_rate': row[4] if len(row) > 4 else None,
                    'estimate_total': row[5] if len(row) > 5 else None,
                    'actual_total': row[6] if len(row) > 6 else None
                })
            elif class_code == 'J':  # Class J has number, days, rate, total
                line_item.update({
                    'estimate_number': row[2] if len(row) > 2 else None,
                    'estimate_days': row[3] if len(row) > 3 else None,
                    'estimate_rate': row[4] if len(row) > 4 else None,
                    'estimate_total': row[5] if len(row) > 5 else None,
                    'actual_total': row[6] if len(row) > 6 else None
                })
            elif class_code == 'O':  # Class O has hours instead of days
                line_item.update({
                    'estimate_hours': row[2] if len(row) > 2 else None,
                    'estimate_rate': row[3] if len(row) > 3 else None,
                    'estimate_total': row[4] if len(row) > 4 else None,
                    'actual_hours': row[5] if len(row) > 5 else None,
                    'actual_total': row[6] if len(row) > 6 else None
                })
                # Add client total if available
                if 'class_client_total' in class_totals:
                    line_item['class_client_total'] = class_totals['class_client_total']
            else:  # Classes C, D, E
                line_item.update({
                    'estimate_number': row[2] if len(row) > 2 else None,
                    'estimate_days': row[3] if len(row) > 3 else None,
                    'estimate_rate': row[4] if len(row) > 4 else None,
                    'estimate_total': row[5] if len(row) > 5 else None,
                    'actual_total': row[6] if len(row) > 6 else None
                })
            
            # Add class totals
            line_item.update({
                'class_estimate_subtotal': class_totals['class_estimate_subtotal'],
                'class_estimate_pnw': class_totals.get('class_estimate_pnw', None),  # May be None for Class C-I
                'class_estimate_total': class_totals['class_estimate_total'],
                'class_actual_subtotal': class_totals['class_actual_subtotal'],
                'class_actual_pnw': class_totals.get('class_actual_pnw', None),  # May be None for Class C-I
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
                'project_info': {
                    'name': sheet_info['spreadsheet_title'],
                    'sheet': sheet_title,
                },
                'summary': {
                    'total_rows': len(processed_rows),
                    'processed_classes': {
                        class_code: {
                            'estimate_total': next((row['class_estimate_total'] for row in processed_rows if row['class_code'] == class_code), '$0.00'),
                            'actual_total': next((row['class_actual_total'] for row in processed_rows if row['class_code'] == class_code), '$0.00'),
                            'row_count': sum(1 for row in processed_rows if row['class_code'] == class_code)
                        }
                        for class_code in set(row['class_code'] for row in processed_rows)
                    },
                    'validation_count': len(validation_issues)
                },
                'upload_info': {
                    'id': self._generate_upload_id(sheet_info['spreadsheet_title'], sheet_title),
                    'timestamp': datetime.now().isoformat(),
                    'sheet_gid': target_gid,
                    'spreadsheet_id': spreadsheet_id
                }
            }
            
            # Log processing summary
            logger.info(f"\nProcessing Summary:")
            logger.info(f"- Total rows processed: {metadata['summary']['total_rows']}")
            logger.info(f"- Classes processed: {', '.join(metadata['summary']['processed_classes'].keys())}")
            logger.info(f"- Validation issues: {metadata['summary']['validation_count']}")
            
            return processed_rows, metadata
            
        except Exception as e:
            logger.error(f"Error processing sheet: {str(e)}")
            raise

    def _get_range_values(self, spreadsheet_id: str, range_name: str) -> list:
        """Fetch values for a specific range with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=range_name
                ).execute()
                return result.get('values', [])
            except Exception as e:
                if 'RATE_LIMIT_EXCEEDED' in str(e):
                    if attempt < max_retries - 1:
                        logger.info(f"Rate limit hit, waiting before retry {attempt + 1}/{max_retries}")
                        time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                        continue
                raise
                
    def _get_range_values_batch(self, spreadsheet_id: str, ranges: List[str]) -> List[List]:
        """Fetch multiple ranges in a single batch request."""
        try:
            result = self.sheets_service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges
            ).execute()
            
            # Extract values from each range
            values = []
            for value_range in result.get('valueRanges', []):
                values.append(value_range.get('values', []))
            return values
            
        except Exception as e:
            if 'RATE_LIMIT_EXCEEDED' in str(e):
                logger.warning("Rate limit hit in batch request, retrying with delay...")
                time.sleep(2)
                return self._get_range_values_batch(spreadsheet_id, ranges)
            logger.error(f"Error in batch request: {str(e)}")
            raise
            
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