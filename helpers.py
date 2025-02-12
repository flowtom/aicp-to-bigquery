"""
Helpers Module

This module centralizes functions for parameter extraction and core task processing for AICP-to-BigQuery.

Future implementations will extract parameters from Lambda events and process tasks accordingly.
"""

import logging
import json

logger = logging.getLogger(__name__)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

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