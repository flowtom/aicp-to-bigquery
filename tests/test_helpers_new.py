import json
import pytest
from unittest.mock import patch

from helpers import (
    extract_budget_url_from_event,
    parse_spreadsheet_url,
    process_task_from_budget_url,
    create_error_response
)


def test_extract_budget_url_success():
    valid_url = "https://docs.google.com/spreadsheets/d/abc123?gid=456"
    event = {"body": json.dumps({"budget_url": valid_url})}
    url = extract_budget_url_from_event(event)
    assert url == valid_url


def test_extract_budget_url_missing():
    event = {"body": json.dumps({})}
    url = extract_budget_url_from_event(event)
    assert url is None


def test_extract_budget_url_invalid():
    event = {"body": json.dumps({"budget_url": "invalid_url"})}
    url = extract_budget_url_from_event(event)
    assert url is None


def test_parse_spreadsheet_url_success():
    url = "https://docs.google.com/spreadsheets/d/abc123?gid=456"
    spreadsheet_id, gid = parse_spreadsheet_url(url)
    assert spreadsheet_id == "abc123"
    assert gid == "456"


def test_parse_spreadsheet_url_no_gid():
    url = "https://docs.google.com/spreadsheets/d/xyz789"
    spreadsheet_id, gid = parse_spreadsheet_url(url)
    assert spreadsheet_id == "xyz789"
    assert gid is None


@patch("helpers.create_error_response")
@patch("src.budget_sync.clickup.job_creator.create_job_from_task")
def test_process_task_from_budget_url_success(mock_create, mock_error_resp):
    # Simulate create_job_from_task to return a success response
    mock_create.return_value = {"status": "created", "msg": "Success"}
    url = "https://docs.google.com/spreadsheets/d/testid?gid=999"
    result = process_task_from_budget_url(url)
    # It should call create_job_from_task with a dict
    mock_create.assert_called_once_with({"spreadsheet_id": "testid", "gid": "999"})
    assert result == {"status": "created", "msg": "Success"}


@patch("helpers.create_error_response")
@patch("src.budget_sync.clickup.job_creator.create_job_from_task")
def test_process_task_from_budget_url_invalid(mock_create, mock_error_resp):
    # For an invalid URL, process_task_from_budget_url should call create_error_response
    url = "https://invalid.com/d/testid"
    error_response = {"statusCode": 400, "body": json.dumps({"error": "Invalid budget URL provided."})}
    mock_error_resp.return_value = error_response
    result = process_task_from_budget_url(url)
    # Ensure create_job_from_task is not called
    mock_create.assert_not_called()
    assert result == error_response 