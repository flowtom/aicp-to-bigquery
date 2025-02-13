import json
import logging
import re
import traceback
from datetime import datetime

from src.budget_sync.services.budget_processor import BudgetProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_spreadsheet_details(url: str) -> tuple:
    """Extracts the spreadsheet ID and sheet GID from a Google Sheets URL."""
    logger.debug(f"Attempting to extract details from URL: {url}")
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        logger.error(f"Failed to extract spreadsheet ID from URL: {url}")
        raise ValueError(f"Invalid URL: Spreadsheet ID not found in '{url}'")
    spreadsheet_id = match.group(1)
    
    match_gid = re.search(r'(?:\?|#)gid=([0-9]+)', url)
    if not match_gid:
        logger.error(f"Failed to extract GID from URL: {url}")
        raise ValueError(f"Invalid URL: Sheet GID not found in '{url}'")
    sheet_gid = match_gid.group(1)
    
    logger.debug(f"Successfully extracted spreadsheet_id: {spreadsheet_id}, sheet_gid: {sheet_gid}")
    return spreadsheet_id, sheet_gid


class BudgetEncoder(json.JSONEncoder):
    """Custom JSON encoder for Budget-related classes."""
    def default(self, obj):
        # Handle objects with to_dict method
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        # Handle datetime objects
        elif isinstance(obj, datetime):
            return obj.isoformat()
        # Handle any other objects with __dict__
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)


def lambda_handler(event, context):
    """AWS Lambda handler for processing budget data from a Google Sheets URL."""
    logger.info("Lambda execution started")
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"Context: RequestId: {context.aws_request_id}")
    logger.info(f"Function timeout: {context.get_remaining_time_in_millis()}ms")

    try:
        # Extract URL from event
        logger.debug("Attempting to extract URL from event")
        url = None
        if event.get('queryStringParameters') and event['queryStringParameters'].get('url'):
            url = event['queryStringParameters']['url']
            logger.info("URL extracted from query parameters")
        elif event.get('body'):
            body = event['body']
            if isinstance(body, str):
                body = json.loads(body)
            url = body.get('url')
            logger.info("URL extracted from request body")

        if not url:
            logger.error("No URL provided in the request")
            raise ValueError("URL not provided in the event payload")

        # Extract spreadsheet details
        logger.info("Extracting spreadsheet details")
        spreadsheet_id, sheet_gid = extract_spreadsheet_details(url)
        logger.info(f"Successfully extracted spreadsheet_id: {spreadsheet_id}, sheet_gid: {sheet_gid}")

        # Process budget
        logger.info("Initializing BudgetProcessor")
        processor = BudgetProcessor(spreadsheet_id, sheet_gid)
        logger.info("Starting budget processing")
        
        try:
            logger.info(f"Remaining time before processing: {context.get_remaining_time_in_millis()}ms")
            processed_data = processor.process_budget()
            logger.info(f"Remaining time after processing: {context.get_remaining_time_in_millis()}ms")
        except Exception as proc_error:
            logger.error(f"Error in process_budget: {str(proc_error)}")
            logger.error(f"Process budget traceback: {traceback.format_exc()}")
            raise

        if processed_data:
            logger.info("Budget processing completed successfully")
            # Use the custom encoder for all JSON operations
            logger.debug(f"Processed data: {json.dumps(processed_data, cls=BudgetEncoder)}")
            response = {
                "status": "success",
                "data": processed_data
            }
            status_code = 200
        else:
            logger.error("Budget processing failed - no data returned")
            response = {
                "status": "error",
                "message": "Failed to process budget."
            }
            status_code = 500
    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        response = {"status": "error", "message": str(ve)}
        status_code = 400
    except json.JSONDecodeError as je:
        logger.error(f"JSON parsing error: {str(je)}")
        response = {"status": "error", "message": "Invalid JSON in request body"}
        status_code = 400
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        response = {
            "status": "error", 
            "message": "Internal server error",
            "detail": str(e)
        }
        status_code = 500

    logger.info(f"Returning response with status code: {status_code}")
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"  # Add CORS if needed
        },
        # Use the custom encoder for the response body
        "body": json.dumps(response, cls=BudgetEncoder)
    } 