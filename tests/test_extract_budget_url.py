import json
import pytest
from helpers import extract_budget_url_from_event


def test_extract_budget_url_valid():
    # Simulated event with a valid budget URL
    event = {"body": json.dumps({"budget_url": "https://docs.google.com/spreadsheets/d/abc123"})}
    result = extract_budget_url_from_event(event)
    assert result == "https://docs.google.com/spreadsheets/d/abc123"


def test_extract_budget_url_missing():
    # Simulated event with missing budget_url key
    event = {"body": json.dumps({})}
    result = extract_budget_url_from_event(event)
    assert result is None


def test_extract_budget_url_invalid_format():
    # Simulated event with an incorrectly formatted URL
    event = {"body": json.dumps({"budget_url": "http://notvalid.com/spreadsheets/d/abc123"})}
    result = extract_budget_url_from_event(event)
    assert result is None


def test_extract_budget_url_invalid_json():
    # Simulated event with an invalid JSON body
    event = {"body": "Not a valid JSON"}
    result = extract_budget_url_from_event(event)
    assert result is None


if __name__ == "__main__":
    pytest.main(["-v"]) 