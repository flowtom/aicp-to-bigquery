import json
from src.budget_sync.logger_config import logger

from helpers import (
    extract_task_id_from_event,
    process_task,
    extract_budget_url_from_event,
    process_task_from_budget_url,
    create_error_response,
    parse_spreadsheet_url
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
    if not budget_url:
        logger.warning("No budget URL provided in event.")
        return create_error_response(400, "Budget URL not provided in event")

    logger.info("Processing event as budget URL event.")

    # Extract spreadsheet ID and GID from the budget_url for response details
    try:
        spreadsheet_id, gid = parse_spreadsheet_url(budget_url)
    except Exception as e:
        logger.exception("Error parsing spreadsheet URL: %s", e)
        return create_error_response(500, "Error parsing budget URL.")

    try:
        result = process_task_from_budget_url(budget_url)
    except Exception as e:
        logger.exception("Error processing budget URL: %s", e)
        return create_error_response(500, "Error processing budget URL.")

    if isinstance(result, dict) and result.get("statusCode") is not None:
        return result

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "status": "success",
            "budget_url": budget_url,
            "spreadsheet_id": spreadsheet_id,
            "gid": gid,
            "result": result
        })
    } 