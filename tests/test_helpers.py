import json
import logging
import pytest
from unittest.mock import patch

from helpers import extract_task_id_from_event, process_task

logger = logging.getLogger('budget_sync')


# Tests for extract_task_id_from_event

def test_extract_task_id_with_path_params():
    event = {
        "pathParameters": {"task_id": "123"}
    }
    task_id = extract_task_id_from_event(event)
    assert task_id == "123"


def test_extract_task_id_with_body():
    event = {
        "body": json.dumps({"task_id": "456"})
    }
    task_id = extract_task_id_from_event(event)
    assert task_id == "456"


def test_extract_task_id_invalid_json():
    event = {
        "body": "Not a JSON"
    }
    task_id = extract_task_id_from_event(event)
    assert task_id is None


def test_extract_task_id_missing():
    event = {}
    task_id = extract_task_id_from_event(event)
    assert task_id is None


# Tests for process_task

@patch('src.budget_sync.clickup.job_creator.create_job_from_task', return_value={"status": "created"})
def test_process_task_success(mock_create_job):
    task_id = "789"
    result = process_task(task_id)
    assert result == {"status": "created"}
    mock_create_job.assert_called_once_with(task_id)

@patch('src.budget_sync.clickup.job_creator.create_job_from_task', side_effect=Exception("Simulated failure"))
def test_process_task_failure(mock_create_job):
    task_id = "fail"
    result = process_task(task_id)
    assert result is None


if __name__ == "__main__":
    test_extract_task_id_with_path_params()
    test_extract_task_id_with_body()
    test_extract_task_id_invalid_json()
    test_extract_task_id_missing()
    test_process_task_success()
    test_process_task_failure()
    logger.info("All helper tests completed.") 