import time
import logging

logger = logging.getLogger(__name__)

# Default mapping for Cover Sheet data
COVER_SHEET_MAPPING = {
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
}

def _batch_get_values(sheets_service, spreadsheet_id, ranges):
    """Helper to fetch batch values with retry logic."""
    max_retries = 5
    base_delay = 2
    for attempt in range(max_retries):
        try:
            logger.debug(f"[_batch_get_values] Attempt {attempt+1}: Fetching ranges: {ranges}")
            result = sheets_service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges
            ).execute()
            logger.debug(f"[_batch_get_values] Fetched result: {result}")
            # Convert result to a dict with range as key
            batch_values = {entry['range']: entry.get('values', [['']])[0] for entry in result.get('valueRanges', [])}
            logger.debug(f"[_batch_get_values] Retrieved values for {len(batch_values)} ranges.")
            return batch_values
        except Exception as e:
            if 'RATE_LIMIT_EXCEEDED' in str(e) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"[_batch_get_values] Rate limit exceeded; attempt {attempt+1}/{max_retries}. Retrying in {delay} seconds.")
                time.sleep(delay)
                continue
            logger.error(f"[_batch_get_values] Error on attempt {attempt+1}/{max_retries}: {e}")
            raise


def _format_money(value):
    """Format a number as a money string."""
    logger.debug(f"[_format_money] Received value: {value}")
    if value is None:
        logger.debug("[_format_money] Value is None, returning default '$0.00'")
        return "$0.00"
    try:
        if isinstance(value, str):
            v = value.strip()
            negative = False
            # Check if the value is in parentheses indicating a negative number
            if v.startswith('(') and v.endswith(')'):
                negative = True
                v = v[1:-1].strip()
            # Remove any leading '$'
            if v.startswith('$'):
                v = v[1:]
            # Remove commas and spaces
            v = v.replace(',', '').replace(' ', '')
            num = float(v)
            if negative:
                num = -num
            formatted_value = "${:,.2f}".format(num)
            logger.debug(f"[_format_money] Formatted value: {formatted_value}")
            return formatted_value
        else:
            num = float(value)
            formatted_value = "${:,.2f}".format(num)
            logger.debug(f"[_format_money] Formatted value: {formatted_value}")
            return formatted_value
    except (ValueError, TypeError) as e:
        logger.error(f"[_format_money] Error formatting value {value}: {e}")
        return "$0.00"


def process_cover_sheet(sheets_service, spreadsheet_id, sheet_title, mapping=COVER_SHEET_MAPPING):
    """Process Cover Sheet data using batchGet in a separate module.

    Args:
        sheets_service: An authorized Google Sheets API service instance.
        spreadsheet_id: The ID of the Google Spreadsheet.
        sheet_title: The title of the sheet to process.
        mapping: (Optional) A mapping for the cover sheet cells; defaults to COVER_SHEET_MAPPING.

    Returns:
        A dictionary with processed cover sheet data.
    """
    # Collect ranges from the mapping
    ranges_to_fetch = []
    for cell in mapping['project_info'].values():
        ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
    for cell in mapping['core_team'].values():
        ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
    for cell in mapping['timeline'].values():
        ranges_to_fetch.append(f"'{sheet_title}'!{cell}")
    for category in mapping['firm_bid_summary'].values():
        for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
            if field in category:
                ranges_to_fetch.append(f"'{sheet_title}'!{category[field]}")
    for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
        if field in mapping['grand_total']:
            ranges_to_fetch.append(f"'{sheet_title}'!{mapping['grand_total'][field]}")
    
    logger.info(f"[Cover_Sheet] Fetching ranges: {ranges_to_fetch}")
    batch_values = _batch_get_values(sheets_service, spreadsheet_id, ranges_to_fetch)
    logger.debug(f"[Cover_Sheet] Raw batch values: {batch_values}")
    
    # Process project info
    project_info = {}
    for field, cell in mapping['project_info'].items():
        range_key = f"'{sheet_title}'!{cell}"
        project_info[field] = batch_values.get(range_key, [''])[0] or ""
    
    # Process core team
    core_team = {}
    for role, cell in mapping['core_team'].items():
        range_key = f"'{sheet_title}'!{cell}"
        core_team[role] = batch_values.get(range_key, [''])[0] or ""
    
    # Process timeline
    timeline = {}
    for milestone, cell in mapping['timeline'].items():
        range_key = f"'{sheet_title}'!{cell}"
        timeline[milestone] = batch_values.get(range_key, ['0'])[0] or "0"
    
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
                firm_bid[category][field] = _format_money(value)
    
    # Process grand total
    grand_total = {'description': mapping['grand_total']['description']}
    for field in ['estimated', 'actual', 'variance', 'client_actual', 'client_variance']:
        if field in mapping['grand_total']:
            range_key = f"'{sheet_title}'!{mapping['grand_total'][field]}"
            value = batch_values.get(range_key, ['$0.00'])[0]
            grand_total[field] = _format_money(value)
    
    processed_data = {
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
    logger.info(f"[Cover_Sheet] Processed data: {processed_data}")
    return processed_data 