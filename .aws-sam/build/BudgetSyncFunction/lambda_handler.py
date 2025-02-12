import json
from src.budget_sync.logger_config import logger

from helpers import (
    extract_task_id_from_event,
    process_task,
    extract_budget_url_from_event,
    process_task_from_budget_url,
    create_error_response
)


def lambda_handler(event, context):
    """AWS Lambda handler function.

    This function processes incoming events. It first checks for a 'budget_url' in the event using
    extract_budget_url_from_event. If found, it processes the task using process_task_from_budget_url. Otherwise,
    it attempts to extract a task_id and uses process_task.
    """
    logger.info("Received event: %s", json.dumps(event))
    
    # Check if event contains a budget_url
    budget_url = extract_budget_url_from_event(event)
    if budget_url:
        logger.info("Processing event as budget URL event.")
        result = process_task_from_budget_url(budget_url)
        # If result indicates an error response, return it directly
        if isinstance(result, dict) and result.get("statusCode") is not None:
            return result
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "success", "budget_url": budget_url, "result": result})
        }
    
    # Fallback: process as task_id event
    task_id = extract_task_id_from_event(event)
    if not task_id:
        logger.warning("No task ID or budget_url provided in event.")
        return create_error_response(400, "Task ID or Budget URL not provided in event")
    
    try:
        result = process_task(task_id)
        if result is None:
            logger.error("Processing task %s failed.", task_id)
            return create_error_response(500, "Task processing failed", task_id)
        else:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"status": "success", "task_id": task_id, "result": result})
            }
    except Exception as e:
        logger.exception("Unexpected error processing task %s: %s", task_id, e)
        return create_error_response(500, "Unexpected error", task_id) 