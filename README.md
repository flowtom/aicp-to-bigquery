# Budget Sync

A tool for processing AICP (Association of Independent Commercial Producers) budgets from Google Sheets into a standardized JSON format.

## Core Functionality

This tool transforms unstructured AICP budget data from Google Sheets into a standardized, validated JSON format that can be:
- Easily analyzed
- Tracked for changes
- Validated for accuracy
- Used by other systems (like BigQuery)

### Input
- One or more Google Sheets containing AICP budget data
- Structured with budget classes A through P
- Contains estimates and actuals for budget line items

### Output
- Standardized JSON files with processed budget data
- Version tracking information
- Processing metadata
- BigQuery tables for analytics

## Prerequisites
- Python 3.8 or higher
- Google Sheets API access
- Google Cloud service account
- BigQuery project and dataset
- Basic understanding of AICP budget structure

## Getting Started

1. **Clone the Repository**
```bash
git clone [repository-url]
cd budget-sync
```

2. **Set Up Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure Google Sheets and BigQuery Access**
- Create a Google Cloud Project
- Enable Google Sheets API and BigQuery API
- Create a service account with necessary permissions:
  - Google Sheets API (roles/sheets.reader)
  - BigQuery Data Editor (roles/bigquery.dataEditor)
- Download service account key to `config/service-account-key.json`
- Share your Google Sheets with the service account email

5. **Configure Environment Variables**
Create `.env` file in the project root:
```bash
# Google Sheets Configuration
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json

# BigQuery Configuration
BIGQUERY_PROJECT_ID=your-bigquery-project-id
BIGQUERY_DATASET_ID=budget_data

# User Configuration
USER_EMAIL=your.email@example.com
VERSION_NOTES="Initial version"  # Optional
```

6. **Configure Budget List**
Create `config/budget_list.json`:
```json
{
    "budgets": [
        {
            "spreadsheet_id": "your-spreadsheet-id-1",
            "sheet_gid": "your-sheet-gid-1",
            "description": "Budget 1 Description",
            "version_notes": "Optional notes"
        },
        {
            "spreadsheet_id": "your-spreadsheet-id-2",
            "sheet_gid": "your-sheet-gid-2",
            "description": "Budget 2 Description"
        }
    ]
}
```

## Running the Script

1. **Process budgets and sync to BigQuery:**
```bash
# Use default config file (config/budget_list.json)
python src/budget_sync/scripts/process_budget.py

# Or specify a custom config file
python src/budget_sync/scripts/process_budget.py --config path/to/config.json
```

The script will:
- Process each budget spreadsheet
- Create/update project records
- Upload budget data
- Upload line item details
- Save validation results
- Generate local JSON output

## Output

1. **Local Files**
- `output/processed_budget_[ID].json` - Processed budget data and metadata for each budget

2. **BigQuery Tables**
- `projects` - Core project information
- `budgets` - Budget versions and cover sheet data
- `budget_details` - Line item details
- `budget_validations` - Validation results

## Development Guide

### Adding New Features
1. Create a new branch
2. Update relevant components
3. Add tests
4. Update documentation
5. Submit pull request

### Testing
Run tests:
```bash
pytest tests/
```

### Common Issues

1. **Authentication Errors**
```
google.auth.exceptions.DefaultCredentialsError
```
Solution: Check service account key path in GOOGLE_APPLICATION_CREDENTIALS

2. **Sheet Access Errors**
```
googleapiclient.errors.HttpError: 404
```
Solution: Verify spreadsheet IDs and sharing permissions

3. **BigQuery Errors**
```
google.api_core.exceptions.NotFound
```
Solution: Verify BIGQUERY_PROJECT_ID and BIGQUERY_DATASET_ID

## Components

### services/budget_processor.py

The core service that processes AICP budgets:

1. **Class Mappings**
   - Maps 16 budget classes (A through P)
   - Defines cell locations for each class:
     - Class code/name locations
     - Line item ranges
     - Column mappings for estimates/actuals
     - Subtotal cells
     - P&W (Payroll & Wrap) cells
     - Total cells

2. **Processing Functions**
   - `process_sheet`: Processes entire budget
   - `_process_class`: Handles individual classes
   - `_process_line_item`: Processes line items
   - `_process_cover_sheet`: Extracts cover sheet data

3. **Data Validation**
   - Validates line items
   - Checks required fields
   - Validates rates and days
   - Logs validation messages

### models/budget.py

Core data models that define the structure of:
- Budget line items (with estimates and actuals)
- Budget classes (A through P)
- Validation results
- Complete AICP budget structure

### api/routes.py

Web service that:
- Provides API endpoints for budget processing
- Handles file uploads and validation
- Returns processed JSON data

### scripts/test_run.py

Development utility script that:
- Tests the budget processor with sample data
- Uses test metadata and configuration
- Outputs timestamped results
- Logs processing statistics and validation errors

## Version Control System

Uses semantic versioning (MAJOR.MINOR.PATCH):
- **MAJOR** (X.0.0): Sheet name changes
- **MINOR** (0.X.0): Budget content changes
- **PATCH** (0.0.X): Reprocessing unchanged content

### Version Tracking Example
```json
{
  "GOOG0324PIXELDR_Estimate-Brand_DR_Combined": {
    "major_version": 1,
    "minor_version": 1,
    "patch_version": 2,
    "first_seen": "01-17-25",
    "last_updated": "01-22-25"
  }
}
```

## Metadata

Each processed budget includes metadata:
```json
{
  "project_info": {
    "name": "GOOG0324PIXELDR_Estimate",
    "sheet": "Brand & DR Combined",
    "version": "1.0.73",
    "source_url": "https://docs.google.com/spreadsheets/d/[SHEET_ID]/edit#gid=[GID]",
    "processed_at": "2024-01-22T15:30:45.123456"
  }
}
```

## Running

1. Process a budget spreadsheet:
```bash
python src/scripts/process_budget.py
```

2. Run the web service:
```bash
python src/budget_sync/api/routes.py
```

3. Access the API at `http://localhost:8080/process-budget`

## Output Files

- `output/processed_budget_[FILENAME]_[VERSION].json` - Processed budget data
- `output/version_tracking.json` - Version history

## BigQuery Setup

### 1. Create Google Cloud Project
```bash
# Create new project
gcloud projects create budget-sync-db-dev

# Set as default project
gcloud config set project budget-sync-db-dev

# Enable BigQuery API
gcloud services enable bigquery.googleapis.com
```

### 2. Create Service Account
```bash
# Create service account
gcloud iam service-accounts create budget-sync-sa \
    --display-name="Budget Sync Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding budget-sync-db-dev \
    --member="serviceAccount:budget-sync-sa@budget-sync-db-dev.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor" \
    --condition=None

# Download service account key
gcloud iam service-accounts keys create config/bigquery-service-account-key.json \
    --iam-account=budget-sync-sa@budget-sync-db-dev.iam.gserviceaccount.com
```

### 3. Create BigQuery Dataset
```bash
# Create dataset
bq mk --dataset budget-sync-db:budget_data

# Verify dataset creation
bq ls
```

### 4. Configure Dataset Access
The dataset ID for use in code will be: `budget-sync-db.budget_data`

### Alternative Setup via Google Cloud Console
1. **Create Project**
   - Go to Google Cloud Console
   - Click "New Project"
   - Enter project name and ID

2. **Create Service Account**
   - Go to IAM & Admin > Service Accounts
   - Click "CREATE SERVICE ACCOUNT"
   - Add "BigQuery Data Editor" role

3. **Create Dataset**
   - Go to BigQuery UI
   - Click on your project
   - Click "CREATE DATASET"
   - Enter dataset ID: `budget_data`

4. **Download Credentials**
   - Go to service account details
   - Create new key (JSON)
   - Save as `config/bigquery-service-account-key.json`

## Helper Module Refactoring

The core business logic has been decoupled from request handling by creating a new helper module `helpers.py`. This module centralizes parameter extraction, task processing, and logging to ensure consistency across the application.

### Key Changes:

- **helpers.py Module:**
  - Contains the function `extract_task_id_from_event(event)` which extracts the `task_id` from incoming Lambda events by:
    - Checking `event['pathParameters']` for a `task_id`.
    - Parsing the JSON body (`json.loads(event['body'])`) if necessary.
    - Logging the extraction process using a shared logger.

  - Contains the function `process_task(task_id)` which:
    - Invokes `create_job_from_task(task_id)` from `src/budget_sync/clickup/job_creator.py`.
    - Uses a try/except block to catch, log, and handle any exceptions, returning the result for further processing.

- **Consistent Logging:**
  - A shared logger is configured in `helpers.py` with a default StreamHandler and a consistent log format to ensure uniform logging throughout the modules.

This refactoring step is an essential part of preparing the codebase for AWS Lambda deployment and improved maintainability.
