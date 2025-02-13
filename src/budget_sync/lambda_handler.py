import json
import logging
import re

from src.budget_sync.services.budget_processor import BudgetProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_spreadsheet_details(url: str) -> tuple:
    """Extracts the spreadsheet ID and sheet GID from a Google Sheets URL."""
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        raise ValueError(f"Invalid URL: Spreadsheet ID not found in '{url}'")
    spreadsheet_id = match.group(1)
    
    match_gid = re.search(r'(?:\?|#)gid=([0-9]+)', url)
    if not match_gid:
        raise ValueError(f"Invalid URL: Sheet GID not found in '{url}'")
    sheet_gid = match_gid.group(1)
    return spreadsheet_id, sheet_gid


def lambda_handler(event, context):
    """AWS Lambda handler for processing budget data from a Google Sheets URL."""
    logger.info("Received event: " + json.dumps(event))
    try:
        url = None
        if event.get('queryStringParameters') and event['queryStringParameters'].get('url'):
            url = event['queryStringParameters']['url']
        elif event.get('body'):
            body = event['body']
            if isinstance(body, str):
                body = json.loads(body)
            url = body.get('url')

        if not url:
            raise ValueError("URL not provided in the event payload")

        spreadsheet_id, sheet_gid = extract_spreadsheet_details(url)
        logger.info(f"Extracted spreadsheet_id: {spreadsheet_id}, sheet_gid: {sheet_gid}")

        processor = BudgetProcessor(spreadsheet_id, sheet_gid)
        processed_data = processor.process_budget()
        
        if processed_data:
            response = {
                "status": "success",
                "data": processed_data
            }
            status_code = 200
        else:
            response = {
                "status": "error",
                "message": "Failed to process budget."
            }
            status_code = 500
    except Exception as e:
        logger.exception("Error processing budget: " + str(e))
        response = {"status": "error", "message": str(e)}
        status_code = 500

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response)
    } 