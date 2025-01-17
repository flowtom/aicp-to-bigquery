from flask import Flask, request, jsonify
from services.budget_processor import AICPBudgetProcessor
from utils.data_utils import DateTimeEncoder
import tempfile
import os
import json

def load_config():
    with open('config/config.json', 'r') as f:
        return json.load(f)

config = load_config()
PROJECT_ID = config['project_id']
PROJECT_NUMBER = config['project_number']

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
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080))) 