import functions_framework
import logging
import json
from flask import Request
from src.budget_sync.clickup.job_creator import create_job_from_task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@functions_framework.http
def handle_job_automation(request: Request):
    """HTTP Cloud Function that accepts task_id via URL path or request body."""
    logger.info("Function triggered!")
    
    # Log everything about the request
    logger.info(f"Request method: {request.method}")
    logger.info(f"Request path: {request.path}")
    logger.info(f"Request URL: {request.url}")
    
    # Log headers
    logger.info("Request headers:")
    for header, value in request.headers.items():
        logger.info(f"{header}: {value}")
    
    # Log raw data
    try:
        logger.info("Raw request data:")
        raw_data = request.get_data()
        logger.info(raw_data)
        
        # Try to parse as JSON if possible
        if raw_data:
            try:
                json_data = json.loads(raw_data)
                logger.info(f"Parsed JSON data: {json.dumps(json_data, indent=2)}")
            except json.JSONDecodeError:
                logger.info("Raw data is not JSON format")
    except Exception as e:
        logger.error(f"Error reading raw data: {str(e)}")
    
    try:
        # First try to get task_id from URL path
        path = request.path
        task_id = None
        
        if path and path != '/':
            # Extract task_id from path (remove leading slash if present)
            task_id = path.strip('/')
            logger.info(f"Found task_id in URL path: {task_id}")
            
        if not task_id:
            # Try request body
            try:
                request_json = request.get_json(silent=True)
                if request_json and 'task_id' in request_json:
                    task_id = request_json['task_id']
                    logger.info(f"Found task_id in request body: {task_id}")
            except Exception as e:
                logger.error(f"Error parsing JSON body: {str(e)}")
            
            # Try form data
            if not task_id:
                try:
                    form_data = request.form
                    if form_data and 'task_id' in form_data:
                        task_id = form_data['task_id']
                        logger.info(f"Found task_id in form data: {task_id}")
                except Exception as e:
                    logger.error(f"Error parsing form data: {str(e)}")
        
        if task_id:
            # Call the job creation logic
            try:
                logger.info(f"Creating job for task ID: {task_id}")
                result = create_job_from_task(task_id)
                return {
                    'status': 'success',
                    'task_id': task_id,
                    'job_created': result
                }
            except Exception as e:
                logger.error(f"Error creating job: {str(e)}")
                return {
                    'status': 'error',
                    'message': f'Failed to create job: {str(e)}',
                    'task_id': task_id
                }
        
        # No task_id found anywhere
        logger.warning("No task_id provided in request")
        return {'status': 'error', 'message': 'No task_id provided'}
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {'status': 'error', 'message': str(e)} 