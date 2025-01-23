# Budget Sync

A tool for processing AICP (Association of Independent Commercial Producers) budgets from Google Sheets into a standardized JSON format.

## Core Functionality

This tool transforms unstructured AICP budget data from Google Sheets into a standardized, validated JSON format that can be:
- Easily analyzed
- Tracked for changes
- Validated for accuracy
- Used by other systems (like BigQuery)

### Input
- Google Sheet containing AICP budget data
- Structured with budget classes A through P
- Contains estimates and actuals for budget line items

### Output
- Standardized JSON files with processed budget data
- Version tracking information
- Processing metadata

## Prerequisites
- Python 3.8 or higher
- Google Sheets API access
- Google Cloud service account
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

4. **Configure Google Sheets Access**
- Create a Google Cloud Project
- Enable Google Sheets API
- Create a service account
- Download service account key to `config/service-account-key.json`
- Share your Google Sheet with the service account email

5. **Configure the Project**
Create `config/config.json`:
```json
{
  "project_id": "your-project-id",
  "spreadsheet_id": "your-spreadsheet-id",
  "sheet_gid": "your-sheet-gid"
}
```

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

Run development test script:
```bash
python src/budget_sync/scripts/test_run.py
```

### Common Issues

1. **Authentication Errors**
```
google.auth.exceptions.DefaultCredentialsError
```
Solution: Check service account key path and permissions

2. **Sheet Access Errors**
```
googleapiclient.errors.HttpError: 404
```
Solution: Verify spreadsheet ID and sharing permissions

3. **Processing Errors**
```
KeyError: 'class_code_cell'
```
Solution: Verify sheet structure matches expected format

## Contributing
1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create pull request

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
