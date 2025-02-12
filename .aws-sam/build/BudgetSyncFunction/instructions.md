# AWS Lambda Portability & Refactoring Task List

This document outlines an extremely detailed, step-by-step plan to refactor the AICP-to-BigQuery repository for portability and AWS Lambda deployment. Each major story point is broken down into numbered tasks with detailed checkboxes for all substeps. We are using python3, AWS Lambda, Google Sheets API, BigQuery, and API Gateway.

---

1. **Decouple Core Business Logic from Request Handling**  
   - [x] **Create a new helper module (`helpers.py`):**  
     - [x] Create a file named `helpers.py` in the project root or a shared module folder.  
     - [x] Add documentation at the top explaining that this module will centralize functions for parameter extraction and core task processing.
   - [x] **Extract Task ID from Incoming Events:**  
     - [x] Write a function `extract_task_id_from_event(event)` that:
       - [x] Checks if `event['pathParameters']` exists and contains `task_id`.
       - [x] If not found, parses the JSON body (`json.loads(event['body'])`) to retrieve `task_id`.
       - [x] Optionally checks for form data if required.
       - [x] Logs the extraction process using a shared logger.
   - [x] **Wrap Core Task Processing:**  
     - [x] Write a function `process_task(task_id)` that:
       - [x] Invokes the existing `create_job_from_task(task_id)` from `src/budget_sync/clickup/job_creator.py`.
       - [x] Implements try/except blocks to catch and log any exceptions.
       - [x] Returns the result from `create_job_from_task` for further processing.
   - [x] **Integrate Consistent Logging:**  
     - [x] Configure a shared logger (or import the existing logger) in `helpers.py` so that logging is consistent across all modules.

2. **Develop an AWS Lambda Handler**  
   - [x] **Create a New File for the Lambda Handler (`lambda_handler.py`):**  
     - [x] Create `lambda_handler.py` in the project root.  
     - [x] Include necessary imports such as `json`, `logging`, and functions from `helpers.py`.
   - [x] **Define the `lambda_handler` Function:**  
     - [x] Implement the `lambda_handler(event, context)` function that:
       - [x] Logs the incoming event (convert the event to JSON for readability).
       - [x] Calls `extract_task_id_from_event(event)` to retrieve the `task_id`.
       - [x] Checks if `task_id` exists; if not, logs a warning and returns a 400 response with an error message.
       - [x] If `task_id` is found, calls `process_task(task_id)` and properly handles exceptions.
       - [x] Returns a response formatted for API Gateway with:
         - `statusCode`: (e.g., 200, 400, or 500).
         - `body`: A JSON-stringified object containing status, `task_id`, and either the job result or an error message.
   - [x] **Ensure API Gateway Compatibility:**  
     - [x] Confirm that the returned object from the Lambda handler matches the API Gateway expectations (i.e., includes both `statusCode` and `body`).

3. **Migrate Configuration Management to Environment Variables**  
   - [x ] **Identify All Local Configurations:**  
     - [ x] Review `.env.template` and any configuration JSON files (e.g., `config/budget_list.json`) for settings such as:
       - `GOOGLE_APPLICATION_CREDENTIALS`
       - `BIGQUERY_PROJECT_ID`
       - `BIGQUERY_DATASET_ID`
       - Any additional API keys or service-specific configurations.
   - [x ] **Modify Code to Use Environment Variables:**  
     - [x ] Replace file-based configuration reads with `os.environ.get('VARIABLE_NAME')` in all relevant modules.
     - [x ] Document the expected values and formats in the project README.
   - [x ] **Plan for AWS Secrets Manager Integration (Optional):**  
     - [x ] Outline a method to store sensitive data (like service account keys) in AWS Secrets Manager.
     - [x ] Add a placeholder function in `helpers.py` or a new module to demonstrate how secrets could be retrieved at runtime.

4. **Refactor Google Sheets API Calls to Optimize for Rate Limiting**  
   - [x ] **Review Current API Call Implementation:**  
     - [x ] Identify where individual cell calls are made (likely in `src/budget_sync/budget_processor.py`).
   - [x ] **Implement Batch API Calls Using `batchGet`:**  
     - [ ] Write or refactor a function (e.g., `get_sheet_data(service, spreadsheet_id, ranges, max_retries=5)`) that:
       - [x ] Uses the Google Sheets API's `batchGet` method to fetch multiple ranges in a single request.
       - [x ] Accepts a list of cell ranges (e.g., `['A1', 'B2', 'C3']`) and returns the combined results.
   - [x ] **Integrate Exponential Backoff:**  
     - [x ] Within the `get_sheet_data` function, add retry logic that:
       - [x ] Catches `HttpError` exceptions.
       - [x ] Checks for status codes 429 or 503.
       - [x ] Implements exponential backoff using `time.sleep(2 ** retry)`.
       - [x ] Raises an error if maximum retries are exceeded.
   - [x ] **Optionally Create a Decorator for Retry Logic:**  
     - [x ] Develop a reusable decorator that can be applied to any external API call to centralize retry logic.

5. **Enhance Logging and Error Handling Across the Application**  
   - [x ] **Consolidate Logging Setup:**  
     - [x ] Create a shared logging configuration file (e.g., `logger_config.py`) to set up and export a unified logger.
     - [x ] Ensure that all modules (including `helpers.py`, `lambda_handler.py`, etc.) import and use this logger.
   - [x ] **Standardize Error Handling:**  
     - [x ] Define common error response formats in helper functions.
     - [x ] Refactor try/except blocks to log errors consistently at the appropriate levels (INFO, WARNING, ERROR).
   - [x ] **Test Logging Output:**  
     - [x ] Simulate both successful and failure scenarios locally to verify that logging outputs are comprehensive and clear.

6. **Set Up Unit Tests and Local Simulation for AWS Lambda**  
   - [x ] **Expand the Test Suite in the `tests/` Directory:**  
     - [x ] Write unit tests for `extract_task_id_from_event(event)` using various simulated event formats (API Gateway event, raw JSON, form data).
     - [x ] Create tests for `process_task(task_id)` ensuring it correctly calls `create_job_from_task` and handles exceptions.
     - [x ] Develop tests for the `lambda_handler` function to simulate different event inputs and expected HTTP responses.
   - [x ] **Simulate AWS Lambda Environment Locally:**  
     - [x ] Set up AWS SAM CLI or the Serverless Framework to run the Lambda function locally.
     - [x ] Create sample event JSON files that mimic API Gateway events for testing.
   - [x ] **Integrate Continuous Testing:**  
     - [x ] Ensure that tests run automatically via a CI pipeline whenever code changes are made.

7. **Package and Deploy the Application to AWS Lambda**  
   - [ ] **Prepare the Deployment Package:**  
     - [ ] Ensure all necessary code files (`lambda_handler.py`, `helpers.py`, modules under `src/`, etc.) are included in the deployment package.
     - [ ] Update `requirements.txt` (or `pyproject.toml`) with all dependencies.
   - [ ] **Choose a Deployment Method:**  
     - [ ] Decide whether to use AWS SAM, the Serverless Framework, or a manual zip package for deployment.
     - [ ] If using AWS SAM, create a `template.yaml` file that defines:
       - The Lambda function (handler, runtime, and memory settings).
       - Required environment variables.
       - IAM role permissions for accessing BigQuery and other services.
   - [ ] **Set Environment Variables in AWS Lambda:**  
     - [ ] In the AWS Lambda console or within your deployment configuration, set environment variables such as:
       - `GOOGLE_APPLICATION_CREDENTIALS`
       - `BIGQUERY_PROJECT_ID`
       - `BIGQUERY_DATASET_ID`
     - [ ] Document the required values and indicate the source (or use AWS Secrets Manager for sensitive keys).
   - [ ] **Deploy and Verify the Lambda Function:**  
     - [ ] Deploy using the chosen method.
     - [ ] Test the Lambda function using sample events in the AWS Lambda console.

8. **Integrate with N8N Workflow**  
   - [ ] **Configure API Gateway Trigger:**  
     - [ ] Set up an API Gateway that triggers the Lambda function.
     - [ ] Map HTTP methods and path parameters correctly so that the event contains the required `pathParameters` (e.g., `{ "task_id": "..." }`).
   - [ ] **Set Up the N8N Workflow:**  
     - [ ] Create an HTTP Request node in N8N that points to the API Gateway endpoint.
     - [ ] Ensure the HTTP request includes the `task_id` either as a path parameter or in the JSON body, in line with the Lambda handler's expectations.
     - [ ] Test the workflow to confirm that the Lambda function is triggered and returns the expected response.
   - [ ] **Document the Integration:**  
     - [ ] Update the project documentation with detailed instructions on how to configure N8N to trigger the Lambda function.

9. **Documentation and Finalization**  
   - [ ] **Update the README File:**  
     - [ ] Add a new section detailing the AWS Lambda deployment process.
     - [ ] Include instructions for setting environment variables, deploying via AWS SAM or the Serverless Framework, and running tests locally.
     - [ ] Document how to trigger the function (e.g., via N8N) and provide troubleshooting tips.
   - [ ] **Update the PROJECT_STRUCTURE.md (if applicable):**  
     - [ ] Reflect the new file structure, including the addition of `lambda_handler.py` and `helpers.py`, and note any changes in configuration management.
   - [ ] **Create a Change Log or Deployment Notes:**  
     - [ ] Summarize all the changes made for portability, including refactoring details and integration steps for AWS Lambda and API Gateway.

---

This detailed checklist should serve as a comprehensive guide for refactoring and deploying the AICP-to-BigQuery application to AWS Lambda. Each task is designed to be a discrete story point that can be individually tracked and completed by an AI coding agent or a development team.

Happy coding!
