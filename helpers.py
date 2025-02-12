"""
Helpers Module

This module centralizes functions for parameter extraction and core task processing for AICP-to-BigQuery.

Future implementations will extract parameters from Lambda events and process tasks accordingly.
"""

from src.budget_sync.logger_config import logger
import json

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