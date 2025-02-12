from flask import Flask, request, jsonify
from services.budget_processor import AICPBudgetProcessor
from utils.data_utils import DateTimeEncoder
import tempfile
import os
import json

# Load API configuration from environment variables
API_HOST = os.environ.get('API_HOST', '0.0.0.0')
API_PORT = int(os.environ.get('API_PORT', '8080'))

# Replace file-based configuration with environment variables
PROJECT_ID = os.environ.get('PROJECT_ID', 'your-default-project-id')
PROJECT_NUMBER = os.environ.get('PROJECT_NUMBER', 'your-default-project-number')

app = Flask(__name__)
processor = AICPBudgetProcessor(project_id=PROJECT_ID)

@app.route('/process-budget', methods=['POST'])
def process_budget():
    try:
        # Validate request
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
            
        if 'metadata' not in request.form:
            return jsonify({'error': 'No metadata provided'}), 400
            
        file = request.files['file']
        metadata = json.loads(request.form['metadata'])
        
        # Save file temporarily
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            file.save(temp_file.name)
            
            # Process budget
            try:
                # Convert data using DateTimeEncoder for JSON serialization
                processed_data = processor.process_budget(temp_file.name, metadata)
                response_data = json.loads(json.dumps(processed_data, cls=DateTimeEncoder))
                
                return jsonify({
                    'status': 'success',
                    'data': response_data
                })
            finally:
                os.unlink(temp_file.name)
                
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host=API_HOST, port=API_PORT) 