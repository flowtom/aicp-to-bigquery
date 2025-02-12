"""
Budget processor service for handling AICP budget data.
"""
from datetime import datetime, UTC
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pathlib import Path
import json
from typing import Dict, List, Any, Optional, Tuple
import time
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from src.budget_sync.models.budget import Budget, BudgetClass
from src.budget_sync.services.bigquery_service import BigQueryService
from googleapiclient.errors import HttpError
import re


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BudgetValidationError(Exception):
    """Exception raised for errors in the budget validation."""
    pass


class BudgetProcessor:
    """Processes AICP budget data from Google Sheets."""
    
    # Template structure constants
    CLASS_MAPPINGS = {
        'COVER_SHEET': {
            'project_info': {
                'project_title': 'C5',
                'production_company': 'C6',
                'contact_phone': 'C7',
                'date': 'H4'
            },
            'core_team': {
                'director': 'C9',
                'producer': 'C10',
                'writer': 'C11'
            },
            'timeline': {
                'pre_prod_days': 'D12',
                'build_days': 'D13',
                'pre_light_days': 'D14',
                'studio_days': 'D15',
                'location_days': 'D16',
                'wrap_days': 'D17'
            },
            'firm_bid_summary': {
                'pre_production_wrap': {
                    'description': 'Pre-production and wrap costs',
                    'categories': 'Total A,C',
                    'estimated': 'G22',
                    'actual': 'H22',
                    'variance': 'I22',
                    'client_actual': 'J22',
                    'client_variance': 'K22'
                },
                'shooting_crew_labor': {
                    'description': 'Shooting crew labor',
                    'categories': 'Total B',
                    'estimated': 'G23',
                    'actual': 'H23',
                    'variance': 'I23',
                    'client_actual': 'J23',
                    'client_variance': 'K23'
                }
            },
            'grand_total': {
                'description': 'GRAND BID TOTAL',
                'estimated': 'G47',
                'actual': 'H47',
                'variance': 'I47',
                'client_actual': 'J47',
                'client_variance': 'K47'
            }
        },
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
    
    def __init__(self, spreadsheet_id: str, gid: str = None):
        """Initialize the budget processor with spreadsheet ID and sheet GID."""
        self.spreadsheet_id = spreadsheet_id
        self.gid = gid
        
        try:
            # If modifying these scopes, delete the file token.json.
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = None
            # The file token.json stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first time.
            if os.path.exists('token.json'):
                with open('token.json', 'r') as token:
                    creds_data = json.load(token)
                    creds = Credentials.from_authorized_user_info(creds_data, SCOPES)

            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())

            logger.info(f"Using OAuth2 credentials for spreadsheet {spreadsheet_id}")
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            # Initialize BigQuery service
            project_id = os.getenv('BIGQUERY_PROJECT_ID')
            dataset_id = os.getenv('BIGQUERY_DATASET_ID')
            
            if project_id and dataset_id:
                try:
                    self.bigquery_service = BigQueryService(project_id, dataset_id)
                    logger.info("✓ BigQuery service initialized successfully")
                except Exception as e:
                    logger.error(f"❌ Failed to initialize BigQuery service: {str(e)}")
                    self.bigquery_service = None
            else:
                logger.warning("⚠️ BigQuery service not initialized - missing environment variables")
                self.bigquery_service = None

        except Exception as e:
            logger.error(f"Error initializing budget processor: {str(e)}")
            raise

    def _extract_row_from_cell(self, cell_ref: str) -> int:
        """Extract the row number from a cell reference (e.g., 'L4' returns 4)."""
        match = re.search(r'\d+', cell_ref)
        if match:
            return int(match.group())
        return 0

    def _get_version_numbers(self, file_name: str, sheet_name: str, date_str: str, current_data: dict) -> tuple[int, int, int]:
        """Get version numbers based on file history and content changes."""
        try:
            tracking_file = Path('output/version_tracking.json')
            tracking_file.parent.mkdir(exist_ok=True)
            
            if tracking_file.exists():
                with open(tracking_file) as f:
                    tracking_data = json.load(f)
            else:
                tracking_data = {}
            
            key = f"{file_name}-{sheet_name}"
            current_hash = hash(str(current_data))
            
            # Handle old format migration
            if key in tracking_data and isinstance(tracking_data[key], dict):
                if "current_increment" in tracking_data[key]:
                    # Convert old increment to semantic version
                    old_increment = tracking_data[key]["current_increment"]
                    tracking_data[key] = {
                        'major_version': 1,
                        'minor_version': 0,
                        'patch_version': old_increment,
                        'first_seen': tracking_data[key].get('first_seen', date_str),
                        'last_updated': date_str,
                        'last_data_hash': current_hash
                    }
            
            # If new combination, start at version 1.0.1
            if key not in tracking_data:
                previous_versions = [k for k in tracking_data.keys() 
                                   if k.startswith(file_name) and k != key]
                
                tracking_data[key] = {
                    'major_version': len(previous_versions) + 1,  # New sheet name = new major version
                    'minor_version': 0,
                    'patch_version': 1,
                    'first_seen': date_str,
                    'last_updated': date_str,
                    'last_data_hash': current_hash
                }
            else:
                last_hash = tracking_data[key].get('last_data_hash')
                
                if current_hash == last_hash:
                    # No changes in content - increment patch
                    tracking_data[key]['patch_version'] += 1
                else:
                    # Content changed - increment minor, reset patch
                    tracking_data[key]['minor_version'] += 1
                    tracking_data[key]['patch_version'] = 1
                    tracking_data[key]['last_data_hash'] = current_hash
                
                tracking_data[key]['last_updated'] = date_str
            
            # Save updated tracking data
            with open(tracking_file, 'w') as f:
                json.dump(tracking_data, f, indent=2)
            
            return (
                tracking_data[key]['major_version'],
                tracking_data[key]['minor_version'],
                tracking_data[key]['patch_version']
            )
            
        except Exception as e:
            logger.warning(f"Error getting version numbers: {e}. Using 1.0.1 as default.")
            return 1, 0, 1

    def _generate_upload_id(self, spreadsheet_title: str, sheet_title: str) -> str:
        """Generate a unique upload ID."""
        # Clean file and sheet names
        clean_file = '_'.join(''.join(c if c.isalnum() else ' ' for c in spreadsheet_title).split())
        clean_sheet = '_'.join(''.join(c if c.isalnum() else ' ' for c in sheet_title).split())
        
        # Generate date string
        date_str = datetime.now().strftime('%m-%d-%y')
        
        # Get version numbers
        major, minor, patch = self._get_version_numbers(clean_file, clean_sheet, date_str, {})
        version_str = f"{major}.{minor}.{patch}"
        
        return f"{clean_file}-{clean_sheet}-{date_str}_{version_str}"

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

            # Process line item with class totals
            line_item = {
                'line_item_number': row[0],
                'line_item_description': row[1]
            }

            # Skip if no line number or description
            if not line_item['line_item_number'] or not line_item['line_item_description']:
                logger.debug(f"Skipping row {row_number} in class {class_code} - missing line number or description")
                return None
            
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

    def _batch_get_values(self, spreadsheet_id: str, ranges: List[str], sheet_title: str) -> Dict[str, List]:
        """Fetch multiple cell ranges in a batch request to reduce API calls."""
        max_retries = 5
        base_delay = 2
        
        # Ensure budget_name is valid before requesting data
        budget_name = sheet_title if sheet_title else "Fallback_Sheet_Name"
        formatted_ranges = []
        for cell in ranges:
            if '!' in cell:  # If it already contains a sheet title, don't prepend
                formatted_ranges.append(cell)
            else:
                formatted_ranges.append(f"'{budget_name}'!{cell}")

        # Debug log to check the constructed ranges
        logging.debug(f"Batch request using sheet_title: {sheet_title}")
        logging.debug(f"Ranges requested: {formatted_ranges}")
        
        for attempt in range(max_retries):
            try:
                result = self.sheets_service.spreadsheets().values().batchGet(
                    spreadsheetId=spreadsheet_id,
                    ranges=formatted_ranges
                ).execute()
                
                # Convert to dictionary for easier access
                return {
                    entry['range']: entry.get('values', [['']])[0] 
                    for entry in result.get('valueRanges', [])
                }
            except Exception as e:
                if 'RATE_LIMIT_EXCEEDED' in str(e) and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Rate limit hit, waiting {delay}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(delay)
                    continue
                logger.error(f"Error in batch request: {str(e)}")
                raise

    def _process_cover_sheet(self, spreadsheet_id: str, sheet_title: str) -> Dict:
        """Process cover sheet data using batch request."""
        mapping = self.CLASS_MAPPINGS['COVER_SHEET']
        
        # Collect all ranges we need to fetch
        ranges_to_fetch = []
        
        # Project info ranges
        for cell in mapping['project_info'].values():
            ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
            
        # Core team ranges
        for cell in mapping['core_team'].values():
            ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
            
        # Timeline ranges
        for cell in mapping['timeline'].values():
            ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
            
        # Firm bid ranges
        for category in mapping['firm_bid_summary'].values():
            for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
                if field in category:
                    ranges_to_fetch.append(f"'{sheet_title}'!{category[field]}")
                    
        # Grand total ranges
        for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
            if field in mapping['grand_total']:
                ranges_to_fetch.append(f"'{sheet_title}'!{mapping['grand_total'][field]}")
        
        # Fetch all values in one batch request
        batch_values = self._batch_get_values(spreadsheet_id, ranges_to_fetch, sheet_title)
        
        # Process project info
        project_info = {}
        for field, cell in mapping['project_info'].items():
            range_key = f"'{sheet_title}'!{cell}"
            value = batch_values.get(range_key, [''])[0] or ""
            project_info[field] = value
        
        # Process core team
        core_team = {}
        for role, cell in mapping['core_team'].items():
            range_key = f"'{sheet_title}'!{cell}"
            value = batch_values.get(range_key, [''])[0] or ""
            core_team[role] = value
        
        # Process timeline
        timeline = {}
        for milestone, cell in mapping['timeline'].items():
            range_key = f"'{sheet_title}'!{cell}"
            value = batch_values.get(range_key, ['0'])[0] or "0"
            timeline[milestone] = value
        
        # Process firm bid summary
        firm_bid = {}
        for category, details in mapping['firm_bid_summary'].items():
            firm_bid[category] = {
                'description': details['description'],
                'categories': details['categories']
            }
            for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
                if field in details:
                    range_key = f"'{sheet_title}'!{details[field]}"
                    value = batch_values.get(range_key, ['$0.00'])[0]
                    firm_bid[category][field] = self._format_money(value)
        
        # Process grand total
        grand_total = {'description': mapping['grand_total']['description']}
        for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
            if field in mapping['grand_total']:
                range_key = f"'{sheet_title}'!{mapping['grand_total'][field]}"
                value = batch_values.get(range_key, ['$0.00'])[0]
                grand_total[field] = self._format_money(value)
        
        return {
            'project_summary': {
                'project_info': project_info,
                'core_team': core_team,
                'timeline': timeline
            },
            'financials': {
                'firm_bid': firm_bid,
                'grand_total': grand_total
            }
        }

    def _format_money(self, value: Any) -> str:
        """Format number as money string."""
        if value is None:
            return "$0.00"
        try:
            if isinstance(value, str):
                if value.startswith('$'):
                    return value  # Already formatted
                value = float(value.replace(',', ''))  # Convert to float safely
            return "${:,.2f}".format(float(value))  # Format as currency
        except (ValueError, TypeError):
            return "$0.00"  # Default if formatting fails

    def _validate_cover_sheet(self, cover_sheet_data: Dict, spreadsheet_title: str) -> Dict:
        """Validates the cover sheet data and applies fallback defaults."""
        if not isinstance(cover_sheet_data, dict):
            raise BudgetValidationError("Cover sheet data must be a dictionary")
            
        # Extract project info properly
        project_info = cover_sheet_data.get('project_summary', {}).get('project_info', {})

        # Apply defaults for missing required fields
        if not project_info.get('project_title'):
            logger.warning("⚠️ Missing project_title, defaulting to spreadsheet name.")
            project_info['project_title'] = spreadsheet_title

        if not project_info.get('production_company'):
            logger.warning("⚠️ Missing production_company, defaulting to 'Newfangled Studios'.")
            project_info['production_company'] = "Newfangled Studios"

        # Update the project info in the cover sheet data
        cover_sheet_data['project_summary']['project_info'] = project_info

        logger.info("✓ Cover sheet validated successfully (fallbacks applied if needed)")
        return cover_sheet_data

    def fetch_raw_data(self) -> dict:
        """Fetch raw data from the Google Sheet using a batchGet call for debugging purposes."""
        try:
            sheet_info = self._get_sheet_info(self.spreadsheet_id, self.gid)
            sheet_title = sheet_info['title']
            mapping = self.CLASS_MAPPINGS['COVER_SHEET']

            # Build list of ranges - using project_info cells as example
            ranges_to_fetch = []
            for cell in mapping['project_info'].values():
                ranges_to_fetch.append(f"'{sheet_title}'!{cell}")

            logger.info(f"Fetching raw data with ranges: {ranges_to_fetch}")
            raw_data = self._batch_get_values(self.spreadsheet_id, ranges_to_fetch, sheet_title)
            logger.info(f"Raw data retrieved: {raw_data}")
            return raw_data
        except Exception as e:
            logger.error(f"Error fetching raw data: {str(e)}")
            return {}

    def process_sheet(self, spreadsheet_id: str, sheet_gid: str) -> Tuple[List[Dict], Dict]:
        """Process a single sheet from the spreadsheet."""
        try:
            processed_rows = []
            processed_classes = 0
            pause_time = 10  # Reduced from 30 to 10 seconds
            
            # Get sheet title
            sheet_title = self._get_sheet_title(spreadsheet_id, sheet_gid)
            
            # Process each budget class
            for class_code, class_info in self.CLASS_MAPPINGS.items():
                logger.info(f"🔄 Processing Class {class_code}...")
                
                # Process class data
                class_rows = self._process_class(
                    spreadsheet_id,
                    sheet_gid,
                    sheet_title,
                    class_code,
                    class_info
                )
                processed_rows.extend(class_rows)
                processed_classes += 1
                
                # Pause after every 2 classes to avoid rate limits
                if processed_classes % 2 == 0:
                    logger.info(f"⏳ Pausing for {pause_time}s after processing {processed_classes} classes...")
                    for i in range(pause_time, 0, -1):
                        logger.info(f"⌛ Resuming in {i} seconds...")
                        time.sleep(1)
            
            # Get version info
            clean_file = '_'.join(''.join(c if c.isalnum() else ' ' for c in sheet_title).split())
            date_str = datetime.now().strftime('%m-%d-%y')
            
            # Get version numbers
            major, minor, patch = self._get_version_numbers(clean_file, sheet_title, date_str, {})
            version_str = f"{major}.{minor}.{patch}"
            
            # Generate metadata with reorganized structure
            metadata = {
                'upload_info': {
                    'id': f"{clean_file}-{sheet_title}-{date_str}_{version_str}",
                    'spreadsheet_id': spreadsheet_id,
                    'spreadsheet_title': sheet_title,
                    'sheet_title': sheet_title,
                    'sheet_gid': sheet_gid,
                    'timestamp': datetime.now().isoformat(),
                    'version': version_str,
                    'first_seen': self._get_first_seen_date(clean_file, sheet_title),
                    'last_updated': date_str
                },
                'metadata': {
                    'project_info': self._process_cover_sheet(spreadsheet_id, sheet_title)['project_summary']['project_info'],
                    'core_team': self._process_cover_sheet(spreadsheet_id, sheet_title)['project_summary']['core_team'],
                    'timeline': self._process_cover_sheet(spreadsheet_id, sheet_title)['project_summary']['timeline'],
                    'financials': self._process_cover_sheet(spreadsheet_id, sheet_title)['financials']
                },
                'processing_summary': {
                    'total_rows': len(processed_rows),
                    'processed_classes': sorted(list(set(r['class_code'] for r in processed_rows))),
                    'validation_issues': sum(1 for r in processed_rows if r['validation_status'] != 'valid')
                }
            }
            
            return processed_rows, metadata
            
        except Exception as e:
            logger.error(f"Error processing sheet: {str(e)}")
            raise

    def _get_first_seen_date(self, clean_file: str, clean_sheet: str) -> str:
        """Get the first seen date for a file/sheet combination."""
        try:
            with open('output/version_tracking.json', 'r') as f:
                tracking_data = json.load(f)
                
            key = f"{clean_file}-{clean_sheet}"
            if key in tracking_data:
                return tracking_data[key].get('first_seen', datetime.now().strftime('%m-%d-%y'))
            return datetime.now().strftime('%m-%d-%y')
        except:
            return datetime.now().strftime('%m-%d-%y')

    def _get_range_values(self, spreadsheet_id: str, range_name: str) -> list:
        """Fetch values for a specific range with retry logic."""
        max_retries = 5  # Increased from 3
        base_delay = 2   # Base delay in seconds
        
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
                        delay = base_delay * (2 ** attempt)  # Exponential backoff: 2s, 4s, 8s, 16s, 32s
                        logger.info(f"Rate limit hit, waiting {delay}s before retry {attempt + 1}/{max_retries}")
                        time.sleep(delay)
                        continue
                raise

    def _get_range_values_batch(self, spreadsheet_id: str, ranges: List[str]) -> List[List]:
        """Fetch multiple ranges in a single batch request."""
        max_retries = 5  # Increased from implicit 1
        base_delay = 2   # Base delay in seconds
        
        for attempt in range(max_retries):
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
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.info(f"Rate limit hit in batch request, waiting {delay}s before retry {attempt + 1}/{max_retries}")
                        time.sleep(delay)
                        continue
                logger.error(f"Error in batch request: {str(e)}")
                raise
            
    def _get_sheet_info(self, spreadsheet_id: str, gid: str) -> Dict[str, str]:
        """Get sheet information including title."""
        try:
            logger.info(f"Fetching sheet metadata for spreadsheet {spreadsheet_id} and GID {gid}")
            response = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            sheets = response.get("sheets", [])
            for sheet in sheets:
                if str(sheet["properties"]["sheetId"]) == str(gid):
                    logger.info(f"✅ Found sheet title: {sheet['properties']['title']}")
                    return {
                        "spreadsheet_title": response.get("properties", {}).get("title", "Unknown Spreadsheet"),
                        "title": sheet["properties"]["title"],
                        "gid": gid
                    }

            # If we get here, no matching sheet was found
            logger.warning(f"⚠️ No sheet found for GID {gid}, defaulting to unknown")
            return {
                "spreadsheet_title": response.get("properties", {}).get("title", "Unknown Spreadsheet"),
                "title": f"Sheet_{gid}",
                "gid": gid
            }

        except Exception as e:
            logger.error(f"❌ Error fetching sheet metadata: {str(e)}")
            raise ValueError(f"Failed to access sheet: {str(e)}")

    def _get_cell_value(self, spreadsheet_id: str, sheet_title: str, cell_ref: str) -> Any:
        """Get value from a cell."""
        try:
            range_name = f"'{sheet_title}'!{cell_ref}"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            values = result.get('values', [])
            return values[0][0] if values and values[0] else None
        except Exception as e:
            logging.warning(f"Error getting cell {cell_ref}: {str(e)}")
            return None

    def _process_class(self, spreadsheet_id: str, sheet_title: str, class_code: str, mapping: Dict) -> Optional[BudgetClass]:
        """Process a single class section and return a BudgetClass object."""
        # Get class header
        header_range = f"'{sheet_title}'!{mapping['class_code_cell']}:{mapping['class_name_cell']}"
        header_values = self._get_range_values(spreadsheet_id, header_range)
        
        # Debug header values
        logger.debug(f"Class {class_code} header range: {header_range}")
        logger.debug(f"Class {class_code} header values: {header_values}")
        
        if not header_values:
            logger.warning(f"⚠️ No header values found for class {class_code}, skipping...")
            return None
        
        # Get class name
        class_name = self._get_class_name(header_values, class_code, mapping, spreadsheet_id, sheet_title)
        if not class_name:
            logger.warning(f"⚠️ No class name found for class {class_code}, skipping...")
            return None
            
        # Get all data including calculated totals
        data_range = f"'{sheet_title}'!{mapping['line_items_range']['start']}:{mapping['line_items_range']['end']}"
        starting_row = self._extract_row_from_cell(mapping['line_items_range']['start'])
        logger.debug(f"Class {class_code} starting row from mapping: {starting_row}")
        data_values = self._get_range_values(spreadsheet_id, data_range)
        logger.debug(f"Class {class_code} data range: {data_range} retrieved {len(data_values)} rows")
        
        if not data_values:
            logger.warning(f"⚠️ No data values found for class {class_code}, skipping...")
            return None
        
        # Get totals directly from cells
        class_totals = self._get_class_totals(spreadsheet_id, sheet_title, mapping)
        logger.debug(f"Class {class_code} totals: {class_totals}")
        
        line_items = []
        # Process each line item
        for row_number, row in enumerate(data_values, start=1):
            if not row:  # Skip empty rows
                continue
                
            if len(row) < 2:  # Need at least number and description
                logger.debug(f"Skipping incomplete row {row_number} in class {class_code}")
                continue
                
            # Process line item with class totals
            line_item = self._process_line_item(row, class_code, class_name, class_totals, row_number)
            if line_item:
                line_items.append(line_item)
        
        if not line_items:
            logger.warning(f"⚠️ Class {class_code} has no valid line items, skipping...")
            return None

        def clean_money_value(value: str) -> float:
            """Convert money or percentage string to float."""
            if not value:
                return 0.0
            try:
                # Handle percentage values
                if isinstance(value, str):
                    # Remove $ if present (some percentage cells might have $ prefix)
                    value = value.replace('$', '')
                    
                    if value.endswith('%'):
                        # Convert percentage to decimal (e.g., "28%" -> 0.28)
                        return float(value.rstrip('%')) / 100
                    
                    # Handle regular money values
                    return float(value.replace(',', ''))
                    
                return float(value)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert value '{value}' to float, using 0.0")
                return 0.0
            
        # Create BudgetClass object with cleaned values
        return BudgetClass(
            class_code=class_code,
            class_name=class_name,
            estimate_subtotal=clean_money_value(class_totals['class_estimate_subtotal']),
            estimate_pnw=clean_money_value(class_totals.get('class_estimate_pnw', '0')),
            estimate_total=clean_money_value(class_totals['class_estimate_total']),
            actual_subtotal=clean_money_value(class_totals['class_actual_subtotal']),
            actual_pnw=clean_money_value(class_totals.get('class_actual_pnw', '0')),
            actual_total=clean_money_value(class_totals['class_actual_total']),
            line_items=line_items
        )

    def process_budget(self) -> Optional[Dict]:
        """Process budget with enhanced validation and error handling."""
        try:
            logger.info(f"Processing budget from sheet {self.spreadsheet_id}")
            
            try:
                sheet_info = self._get_sheet_info(self.spreadsheet_id, self.gid)
            except ValueError as e:
                logger.error(f"Failed to get sheet info: {str(e)}")
                return None

            try:
                # Process cover sheet
                cover_sheet_data = self._process_cover_sheet(self.spreadsheet_id, sheet_info['title'])
                
                # Double-check cover_sheet_data exists before using it
                if not cover_sheet_data:
                    logger.warning("⚠️ cover_sheet_data is empty! Using default values.")
                    cover_sheet_data = {}  # Prevents it from being None
                
                validated_data = self._validate_cover_sheet(cover_sheet_data, sheet_info['spreadsheet_title'])

                if not isinstance(validated_data, dict):
                    raise ValueError("Cover sheet validation returned a non-dict type")

                logger.info("✓ Cover sheet processed successfully")
                
                # Process budget classes
                classes = {}
                logger.info(f"Starting to process budget classes...")
                logger.info(f"Found {len([k for k in self.CLASS_MAPPINGS.keys() if k != 'COVER_SHEET'])} classes to process")
                
                for class_code, mapping in self.CLASS_MAPPINGS.items():
                    if class_code == 'COVER_SHEET':  # Skip cover sheet mapping
                        continue
                        
                    try:
                        logger.info(f"🔍 Processing budget class: {class_code}...")
                        logger.debug(f"Using mapping: {json.dumps(mapping, indent=2)}")
                        
                        # Get class header to check if class exists
                        header_range = f"'{sheet_info['title']}'!{mapping['class_code_cell']}:{mapping['class_name_cell']}"
                        header_values = self._get_range_values(self.spreadsheet_id, header_range)
                        
                        if not header_values:
                            logger.info(f"⚠️ Class {class_code} header not found at {header_range}, skipping.")
                            continue
                            
                        logger.info(f"Found class header: {header_values}")
                        
                        class_data = self._process_class(
                            self.spreadsheet_id,
                            sheet_info['title'],
                            class_code,
                            mapping
                        )
                        
                        if class_data:
                            classes[class_code] = class_data
                            logger.info(f"✅ Successfully processed class {class_code}")
                            if hasattr(class_data, 'line_items'):
                                logger.info(f"   Found {len(class_data.line_items)} line items")
                        else:
                            logger.warning(f"⚠️ Class {class_code} returned no data")

                    except Exception as e:
                        logger.error(f"❌ Error processing class {class_code}: {str(e)}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                
                logger.info(f"Completed class processing. Found {len(classes)} valid classes.")
                
                # Combine all data
                budget_data = {
                    'upload_id': self._generate_upload_id(sheet_info['spreadsheet_title'], sheet_info['title']),
                    'upload_timestamp': datetime.now(UTC),
                    'version_status': 'draft',
                    'sheet_title': sheet_info['title'],
                    'project_summary': validated_data.get('project_summary', {}),
                    'financials': validated_data.get('financials', {}),
                    'classes': classes
                }

                return budget_data

            except ValueError as e:
                logger.error(f"❌ Cover sheet validation failed: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            return None

def retry_on_http_error(max_retries=5, backoff_factor=2, status_codes=(429, 503)):
    """Decorator to retry a function on specific HTTP errors with exponential backoff.

    Args:
        max_retries: Maximum number of retries.
        backoff_factor: Factor for exponential backoff.
        status_codes: Tuple of HTTP status codes to retry on.

    Returns:
        Decorated function with retry logic.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except HttpError as error:
                    if error.resp.status in status_codes:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"HTTP error {error.resp.status} occurred. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"HttpError occurred: {error}")
                        raise
            logger.error("Max retries exceeded.")
            return None
        return wrapper
    return decorator

# Example usage of the decorator
@retry_on_http_error()
def get_sheet_data(service, spreadsheet_id, ranges):
    """Fetch data from Google Sheets using batchGet with retry logic.

    Args:
        service: The Sheets API service instance.
        spreadsheet_id: The ID of the spreadsheet to retrieve data from.
        ranges: A list of ranges to retrieve from the spreadsheet.

    Returns:
        A dictionary containing the requested data.
    """
    request = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=ranges
    )
    response = request.execute()
    return response 