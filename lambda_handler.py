import json
from src.budget_sync.logger_config import logger

from helpers import extract_task_id_from_event, process_task, create_error_response


def lambda_handler(event, context):
    """AWS Lambda handler function.

    This function processes incoming events, extracts the task_id, calls the process_task function,
    and returns a response formatted for API Gateway with statusCode, headers, and body.
    """
    logger.info("Received event: %s", json.dumps(event))

    task_id = extract_task_id_from_event(event)
    if not task_id:
        logger.warning("Task ID not found in event")
        return create_error_response(400, "Task ID not provided in event")

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