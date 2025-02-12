import json
import logging
from lambda_handler import lambda_handler
from unittest.mock import patch

# Configure logging to capture output for testing
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('budget_sync')

# Test scenarios

def test_lambda_handler_success():
    event = {
        "pathParameters": {"task_id": "123"},
        "body": json.dumps({})
    }
    context = {}
    response = lambda_handler(event, context)
    assert response['statusCode'] == 200
    logger.info("Test success scenario passed.")


def test_lambda_handler_missing_task_id():
    event = {
        "body": json.dumps({})
    }
    context = {}
    response = lambda_handler(event, context)
    assert response['statusCode'] == 400
    logger.info("Test missing task ID scenario passed.")


@patch('lambda_handler.process_task', side_effect=Exception("Simulated error"))
def test_lambda_handler_unexpected_error(mock_process_task):
    event = {
        "pathParameters": {"task_id": "error"},
        "body": json.dumps({})
    }
    context = {}
    
    response = lambda_handler(event, context)
    assert response['statusCode'] == 500
    logger.info("Test unexpected error scenario passed.")

# Run tests
if __name__ == "__main__":
    test_lambda_handler_success()
    test_lambda_handler_missing_task_id()
    test_lambda_handler_unexpected_error()
    logger.info("All tests completed.") 