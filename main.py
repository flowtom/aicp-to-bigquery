import functions_framework

@functions_framework.http
def handle_job_automation(request):
    """HTTP Cloud Function."""
    request_json = request.get_json(silent=True)
    
    if request_json and 'task_id' in request_json:
        return {'status': 'success', 'task_id': request_json['task_id']}
    else:
        return {'status': 'error', 'message': 'No task_id provided'} 