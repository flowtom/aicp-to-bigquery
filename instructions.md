# AWS Lambda Portability & Refactoring Task List

This document outlines an extremely detailed, step-by-step plan to refactor the AICP-to-BigQuery repository for portability and AWS Lambda deployment. Each major story point is broken down into numbered tasks with detailed checkboxes for all substeps. We are using python3, AWS Lambda, Google Sheets API, BigQuery, and API Gateway.

---

1. **Decouple Core Business Logic from Request Handling**  
   - [x] **Create a new helper module (`helpers.py`):**  
     - [x] Create a file named `helpers.py` in the project root or a shared module folder.  
     - [x] Add documentation at the top explaining that this module will centralize functions for parameter extraction and core task processing.
   - - [x ] **Update Extraction Logic for Webhook Payload:**  
     - [x ] Write (or update) a function `extract_budget_url_from_event(event)` that:
       - [x ] Checks for the presence of a key (e.g., `"budget_url"`) in the JSON payload of the webhook.
       - [x ] Logs and validates that the URL is present and correctly formatted.
     - [x ] If the URL is missing or malformed, log an appropriate error and return an error response.
   - [ X] **Extract Spreadsheet ID and GID from the URL:**  
     - [x ] Write a function `parse_spreadsheet_url(budget_url)` that:
       - [x ] Uses regular expressions or URL parsing methods to extract the spreadsheet ID (the portion following `/d/` and before the next slash).
       - [x ] Extracts the GID, typically found after `gid=` in the URL query string.
       - [x ] Validates the extracted values (e.g., checks that the spreadsheet ID is non-empty and the GID is numeric or matches expected patterns).
       - [x ] Logs the extracted spreadsheet ID and GID.
   - [x ] **Wrap Core Task Processing:**  
     - [x ] Write a function `process_task(budget_url)` that:
       - [x ] Calls `parse_spreadsheet_url(budget_url)` to obtain the spreadsheet ID and GID.
       - [x ] Passes these values into the job creation logic (i.e., calls `create_job_from_task` or a similarly named function), which now accepts the spreadsheet ID and GID.
       - [x ] Catches and logs any exceptions and returns the processing result.
   - [x ] **Integrate Consistent Logging:**  
     - [x ] Ensure that all helper functions use a shared logger to maintain consistent log output across modules.

2. **Develop an AWS Lambda Handler**  
   - [x] **Create a New File for the Lambda Handler (`lambda_handler.py`):**  
     - [x] Create `lambda_handler.py` in the project root.  
     - [x] Include necessary imports such as `json`, `logging`, and functions from `helpers.py`.
   - [x ] **Define the `lambda_handler` Function:**  
     - [x ] Implement `lambda_handler(event, context)` that:
       - [x ] Logs the incoming event (convert to JSON for readability).
       - [x ] Calls `extract_budget_url_from_event(event)` to retrieve the `budget_url`.
       - [x ] Validates that `budget_url` is present; if not, logs a warning and returns a 400 response.
       - [x ] Calls `process_task(budget_url)` and handles any exceptions.
       - [x ] Returns a response formatted for API Gateway with:
         - `statusCode` (e.g., 200, 400, 500)
         - `body` containing a JSON-stringified object with processing status, extracted spreadsheet ID and GID, and job creation results or error messages.
   - [ ] **Ensure API Gateway Compatibility:**  
     - [ ] Confirm that the returned object from the Lambda handler matches the API Gateway expectations (i.e., includes both `statusCode` and `body`).

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
     - [ ] Write unit tests for `extract_budget_url_from_event(event)` using simulated event payloads.
     - [ ] Write unit tests for `parse_spreadsheet_url(budget_url)` to verify correct extraction of spreadsheet ID and GID from various URL formats.
     - [ ] Develop tests for `process_task(budget_url)` ensuring it correctly processes the URL and handles exceptions. 
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


# Next Steps: Implementing Full Processing Logic in process_budgets()

1. **Read Data from the Google Sheet**
   - [X ] Update the `process_budgets()` function to call a method on your `BudgetProcessor` that fetches data from the Google Sheet.
     - [X ] Use batched API calls (e.g., via `batchGet`) to minimize the number of API calls.
     - [X ] Log the raw data retrieved for debugging purposes.

2. **Process the Cell Mappings and Extract Information**
   - [X ] In your `BudgetProcessor` class, implement methods that:
     - [X ] Identify the starting points for each budget class (A through P) based on the non-standard layout.
     - [X ] Parse line items for each class, reading estimates, actuals, subtotals, etc.
     - [X ] Log intermediate results to verify that the correct cells are being processed.
   - [X ] Ensure that the mapping logic accommodates any template changes or variations.

3. **Validate and Transform Data into JSON Format**
   - [X ] Add validation steps that check for:
     - [X ] Missing or invalid fields.
     - [X ] Consistency in numeric formats (e.g., ensuring money values are floats).
     - [X ] Correct date formats using helper functions.
   - [X ] Transform the validated data into your standardized JSON structure.
     - [x ] Include metadata such as timestamps, versioning information, and any processing statistics.

4. **Upload Processed Data to BigQuery**
   - [x ] **Review Processed JSON Format:**
     - [x ] Ensure that your final JSON output includes distinct sections for:
       - Cover Sheet data (e.g., under the key `"cover_sheet"` or `"project_summary"`).
       - Line items data (e.g., under the key `"line_items"` or similar).
     - [x ] Confirm that a unique `budget_id` is generated and included in both sections.
   - [x ] **Design Table Schemas:**
     - [x ] Verify the schema for the "budgets" table (should include columns for all Cover Sheet fields plus a `budget_id`).
     - [x ] Verify the schema for the "budget_details" table (should include columns for line item details plus a `budget_id` to join with the "budgets" table).
   - [x ] **Document the Data Mapping:**
     - [x ] Write documentation (or inline comments) mapping JSON keys to BigQuery table columns.
     
  4.1     **Format Processed Data for BigQuery Upload**
    - [x ] **Develop a Function to Format Cover Sheet Data:**
      - [x ] Create a function (e.g., `format_cover_sheet_for_bq(processed_data)`) that:
        - [x ] Extracts the Cover Sheet portion from the processed JSON.
        - [x ] Maps each field to the corresponding BigQuery column.
        - [x ] Includes the common `budget_id` field.
        - [x ] Returns a dictionary or row object formatted for BigQuery.
    - [x ] **Develop a Function to Format Line Items Data:**
      - [x ] Create a function (e.g., `format_line_items_for_bq(processed_data)`) that:
        - [x ] Extracts the line items data from the processed JSON.
        - [x ] Iterates over each line item, mapping the fields to the corresponding columns.
        - [x ] Attaches the same `budget_id` to each row.
        - [x ] Returns a list of dictionaries/row objects ready for batch insertion.

  4.2  **Implement BigQuery Upload Logic**
    - [x ] **Integrate with BigQuery Client Library:**
      - [x ] Import and initialize the Google Cloud BigQuery client.
      - [x ] Ensure that your environment variables (like `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID`) are correctly loaded.
    - [x ] **Implement Upload Function for Cover Sheet:**
      - [x ] Create a function (e.g., `upload_cover_sheet_to_bq(bq_client, cover_sheet_row)`) that:
        - [x ] Uses `insert_rows_json` (or a similar API) to insert the cover sheet data into the "budgets" table.
        - [x ] Implements error handling:
          - [x ] If an error occurs (e.g., network issues, schema mismatch), log the error.
          - [x ] Optionally implement retry logic with exponential backoff.
        - [x ] Logs a success message upon successful upload.
    - [x ] **Implement Upload Function for Line Items:**
      - [x ] Create a function (e.g., `upload_line_items_to_bq(bq_client, line_items_rows)`) that:
        - [x ] Uses `insert_rows_json` to batch insert line item rows into the "budget_details" table.
        - [x ] Implements similar error handling and retry logic as for the cover sheet.
        - [x ] Logs the number of rows inserted and any warnings or errors.
    - [x ] **Integrate Upload Steps in Main Processing Flow:**
      - [x ] In your main processing function (or `BudgetProcessor`), after processing and formatting the data:
        - [x ] Call the cover sheet upload function and capture the result.
        - [x ] Call the line items upload function and capture the result.
        - [x ] Log the overall status and any relevant details for monitoring.

  4.3 **Testing and Validation**
    - [ x] **Local Testing:**
      - [ x] Simulate the full processing pipeline locally (using SAM/Docker or direct command-line execution).
      - [ x] Verify that both the cover sheet and line items data are correctly formatted and uploaded.
    - [ x] **Mock BigQuery API Calls:**
      - [ x] Write unit tests that mock BigQuery API calls to simulate insertion success and failure.
      - [ x] Ensure that the error handling and retry logic behave as expected.
    - [ x] **Review Logs:**
      - [ x] Confirm that detailed logs are generated for each step of the upload process.
      - [ x] Verify that errors, if any, are clearly logged with sufficient context for troubleshooting.

  4.4  **Documentation and Finalization**
    - [x ] **Update Project Documentation:**
      - [x ] Add a section in the README or internal docs detailing:
        - The data flow from processing to BigQuery upload.
        - How the cover sheet and line items are mapped and related via `budget_id`.
        - Any configuration needed for BigQuery (table names, schema details, etc.).
    - [x ] **Deployment Notes:**
      - [x ] Document how to set environment variables and credentials for BigQuery.
      - [x ] Provide troubleshooting tips for common upload issues.
    - [x ] **Version Control:**
      - [x ] Commit your changes and document them in your change log.




5. **Testing and Verification**
   - [ ] Run local tests using SAM/Docker to simulate the entire flow.
   - [ ] Verify that each step logs meaningful output and that the final output in BigQuery (or locally printed JSON) matches the expected format.
   - [ ] Write unit tests for each of the newly implemented functions to ensure robustness.


   BUG BUGS BUGS


   Cover Sheet is not being processed.

   1. **Review Current Processing Logic**
   - [x ] Open the current `BudgetProcessor` (or equivalent) module.
   - [x ] Identify existing logic that processes budget classes (A–P).
   - [x ] Locate any existing code (or log statements) related to Cover_Sheet processing.
   - [x ] Document where and how Cover_Sheet data was intended to be processed before it was inadvertently omitted.

2. **Identify the Correct Range for Cover_Sheet**
   - [x ] Confirm the cell range for the Cover_Sheet in your Google Sheet template.
     - [x ] Verify the starting and ending cells (e.g., "Cover_Sheet!A1:D10" or similar).
     - [x ] Note any specific formatting or key data points required for the Cover_Sheet.
   - [x ] Update any configuration or constants (if applicable) to include the Cover_Sheet range.

3. **Implement Separate Processing for Cover_Sheet**
   - [x ] Create or update a function in `BudgetProcessor` (or in a helper module) called, for example, `_process_cover_sheet()`.
     - [x ] The function should:
       - [x ] Connect to the Google Sheets API using the appropriate range for the Cover_Sheet.
       - [x ] Extract raw data from that range.
       - [ ] Parse and transform the raw data into a structured JSON format (e.g., mapping specific cells to keys).
       - [x ] Log the raw values and the processed output for debugging.
   - [x ] Ensure that the function returns a JSON object (or dictionary) representing the Cover_Sheet data.
   - [x ] Add unit tests for `_process_cover_sheet()` to confirm it returns the expected structure given a sample input.

4. **Integrate Cover_Sheet Data into Final JSON Output**
   - [x ] Update the main processing function (e.g., `process_budgets()`) to call the new `_process_cover_sheet()` method.
     - [x ] Capture the returned JSON from `_process_cover_sheet()`.
   - [x ] Modify the final output JSON object to include a key such as `"cover_sheet"` with the processed data.
     - [x ] Example:
       ```python
       final_output = {
           "cover_sheet": cover_sheet_data,   # Data from _process_cover_sheet()
           "line_items": processed_line_items,  # Existing data for classes A–P
           "metadata": { ... }                  # Additional processing metadata
       }
       ```
   - [x ] Verify that when the processing logic runs, the final JSON now includes the Cover_Sheet data.

5. **Add Detailed Logging and Error Handling**
   - [x ] In `_process_cover_sheet()`, add logging statements that:
     - [x ] Log the entry into the function and the specified range.
     - [x ] Log raw data received from the API.
     - [x ] Log the transformed output before returning it.
   - [x ] Ensure any errors (e.g., if the range is empty or data is malformed) are caught and logged, and that the function returns a default structure or error message as needed.

6. **Test the Updated Processing Flow Locally**
   - [x ] Run your processing script (e.g., using SAM/Docker) with a sample Google Sheets URL that includes valid Cover_Sheet data.
   - [x ] Examine the logs to verify that:
     - [x ] "Cover_Sheet processed" is logged along with actual data.
     - [x ] The final JSON output includes a non-empty `"cover_sheet"` field.
   - [x ] If the Cover_Sheet data is still missing, review the defined cell range and the parsing logic.
   - [x ] Update unit tests or add additional tests to simulate different Cover_Sheet scenarios.

7. **Document the Changes**
   - [x ] Update your project README and any internal documentation to describe:
     - [x ] How Cover_Sheet data is now processed.
     - [x ] The expected range and structure for the Cover_Sheet.
     - [x ] Any new configuration values or environmental changes.
   - [x ] Note any assumptions or limitations in the current implementation for future reference.

