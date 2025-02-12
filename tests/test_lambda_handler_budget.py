import json
import pytest
from lambda_handler import lambda_handler
from unittest.mock import patch


def test_lambda_handler_valid_budget_url():
    valid_url = "https://docs.google.com/spreadsheets/d/test123?gid=456"
    event = {
        "body": json.dumps({"budget_url": valid_url})
    }
    context = {}
    
    # Patch parse_spreadsheet_url to return ("test123", "456")
    # and patch process_task_from_budget_url to return a dummy successful response
    with patch("helpers.parse_spreadsheet_url", return_value=("test123", "456")) as mock_parse, \
         patch("helpers.process_task_from_budget_url", return_value={"task_id": {"spreadsheet_id": "test123", "gid": "456"}, "status": "created", "message": "Job creation placeholder - actual implementation pending"}) as mock_process:
        response = lambda_handler(event, context)
        body = json.loads(response["body"])
        assert response["statusCode"] == 200
        assert body["status"] == "success"
        assert body["spreadsheet_id"] == "test123"
        assert body["gid"] == "456"
        assert "result" in body
        mock_parse.assert_called_once_with(valid_url)
        mock_process.assert_called_once_with(valid_url)


def test_lambda_handler_missing_budget_url():
    # Event without budget_url in the JSON body
    event = {"body": json.dumps({})}
    context = {}
    response = lambda_handler(event, context)
    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "Budget URL not provided" in body["error"]


def test_lambda_handler_parse_exception():
    valid_url = "https://docs.google.com/spreadsheets/d/test123?gid=456"
    event = {"body": json.dumps({"budget_url": valid_url})}
    context = {}
    with patch("helpers.parse_spreadsheet_url", side_effect=Exception("Parse error")) as mock_parse:
        response = lambda_handler(event, context)
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "Error parsing budget URL" in body["error"]
        mock_parse.assert_called_once_with(valid_url)


def test_lambda_handler_processing_exception():
    valid_url = "https://docs.google.com/spreadsheets/d/test123?gid=456"
    event = {"body": json.dumps({"budget_url": valid_url})}
    context = {}
    with patch("helpers.parse_spreadsheet_url", return_value=("test123", "456")) as mock_parse, \
         patch("helpers.process_task_from_budget_url", side_effect=Exception("Processing error")) as mock_process:
        response = lambda_handler(event, context)
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "Error processing budget URL" in body["error"]
        mock_parse.assert_called_once_with(valid_url)
        mock_process.assert_called_once_with(valid_url)


if __name__ == "__main__":
    pytest.main(["-v"]) 