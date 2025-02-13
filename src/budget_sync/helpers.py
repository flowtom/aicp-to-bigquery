"""
Helpers Module

This module centralizes functions for parameter extraction and core task processing for AICP-to-BigQuery.

Future implementations will extract parameters from Lambda events and process tasks accordingly.
"""

from src.budget_sync.logger_config import logger
import json
import re

# TODO: Implement helper functions such as extract_task_id_from_event and process_task

def extract_task_id_from_event(event):
    """Extract task ID from the given event.

    This function checks for a task_id in pathParameters and then in the JSON body if not found.
    """
    try:
        path_params = event.get('pathParameters', {})
        if path_params and 'task_id' in path_params:
            logger.info('Task ID found in pathParameters.')
            return path_params['task_id']

        if 'body' in event:
            try:
                body_data = json.loads(event['body'])
                if 'task_id' in body_data:
                    logger.info('Task ID found in JSON body.')
                    return body_data['task_id']
                else:
                    logger.warning('No task ID found in JSON body.')
            except json.JSONDecodeError as jde:
                logger.error('Invalid JSON in event body: %s', jde)

        logger.warning('No task ID found in event.')
        return None
    except Exception as e:
        logger.error('Error extracting task ID: %s', e)
        return None


def process_task(task_id):
    """Process the given task_id by invoking the core business logic functions.

    This function will call create_job_from_task from the job creator module and handle errors accordingly.
    """
    try:
        from src.budget_sync.clickup.job_creator import create_job_from_task
        result = create_job_from_task(task_id)
        return result
    except Exception as e:
        logger.error('Error processing task %s: %s', task_id, e)
        return None

def get_secret(secret_name, region_name="us-east-1"):
    """Retrieve a secret from AWS Secrets Manager.
    
    Args:
        secret_name (str): The name of the secret.
        region_name (str): The AWS region where the secret is stored (default 'us-east-1').
    
    Returns:
        dict or str: The secret value parsed as a JSON dictionary if possible, or the raw string value.
    """
    import boto3
    from botocore.exceptions import ClientError
    
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            secret = response['SecretString']
            try:
                # Try to parse the secret as JSON
                return json.loads(secret)
            except json.JSONDecodeError:
                return secret
        else:
            # If the secret is binary
            return response['SecretBinary']
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        return None 

def create_error_response(status_code, message, task_id=None):
    """Create a standardized error response for API Gateway.

    Args:
        status_code (int): The HTTP status code for the response.
        message (str): The error message to include in the response.
        task_id (str, optional): The task ID related to the error, if applicable.

    Returns:
        dict: A dictionary representing the API Gateway response.
    """
    response_body = {"error": message}
    if task_id:
        response_body["task_id"] = task_id
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response_body)
    } 

def extract_budget_url_from_event(event):
    """Extract budget URL from the webhook event.
    
    This function checks for a 'budget_url' key in the event's JSON payload,
    validates its format (it should start with 'https://docs.google.com/spreadsheets/d/'),
    and returns the URL if valid. Returns None if missing or malformed.
    """
    try:
        if "body" in event:
            body_data = json.loads(event["body"])
            budget_url = body_data.get("budget_url")
        else:
            budget_url = event.get("budget_url")
        if not budget_url:
            logger.error("Budget URL is missing in the event.")
            return None
        # Validate the URL format
        pattern = r"^https:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9-_]+"
        if not re.match(pattern, budget_url):
            logger.error("Budget URL is not correctly formatted: %s", budget_url)
            return None
        logger.info("Budget URL extracted: %s", budget_url)
        return budget_url
    except Exception as e:
        logger.error("Error extracting budget URL: %s", e)
        return None

def parse_spreadsheet_url(budget_url):
    """Parse the budget URL to extract the spreadsheet ID and GID.
    
    Returns a tuple of (spreadsheet_id, gid). If GID is not present, returns None for gid.
    """
    try:
        id_match = re.search(r"/d/([a-zA-Z0-9-_]+)", budget_url)
        if not id_match:
            logger.error("No Spreadsheet ID found in the budget URL: %s", budget_url)
            return None, None
        spreadsheet_id = id_match.group(1)
        gid_match = re.search(r"[?&]gid=([0-9]+)", budget_url)
        gid = gid_match.group(1) if gid_match else None
        logger.info("Extracted Spreadsheet ID: %s and GID: %s", spreadsheet_id, gid)
        return spreadsheet_id, gid
    except Exception as e:
        logger.error("Error parsing spreadsheet URL: %s", e)
        return None, None

def process_task_from_budget_url(budget_url):
    """Process a task from the provided budget URL.
    
    This function extracts the spreadsheet ID and GID from the budget URL
    and then calls the job creation logic to process the task.
    """
    try:
        # Validate URL domain to ensure it is from docs.google.com
        if not budget_url.startswith("https://docs.google.com/spreadsheets/d/"):
            logger.error("Budget URL does not start with expected domain: %s", budget_url)
            return create_error_response(400, "Invalid budget URL provided.")
        
        spreadsheet_id, gid = parse_spreadsheet_url(budget_url)
        if not spreadsheet_id:
            logger.error("Failed to extract a valid spreadsheet ID from the provided budget URL.")
            return create_error_response(400, "Invalid budget URL provided.")
        from src.budget_sync.clickup.job_creator import create_job_from_task
        # Call the job creation logic with the extracted spreadsheet info
        result = create_job_from_task({"spreadsheet_id": spreadsheet_id, "gid": gid})
        return result
    except Exception as e:
        logger.error("Error processing task from budget URL: %s", e)
        return create_error_response(500, "Error processing the provided budget URL.") 