# Budget Sync

A tool for processing and syncing AICP budget data with BigQuery.

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure:
- Place your service account key in `config/service-account-key.json`
- Update project settings in `config/config.json`

## Components

### budget_processor.py

The core service that handles reading and processing budget data from Google Sheets. Key features:
- Maps and processes 16 different budget classes (A through P)
- Handles various data formats including days, hours, rates, and totals
- Validates line items and tracks validation issues
- Supports batch processing to handle API rate limits
- Maintains version tracking for processed files

### process_budget.py

The script that orchestrates the budget processing workflow:
- Reads configuration settings
- Initializes the budget processor service
- Processes the specified spreadsheet
- Saves results to JSON files with versioned output names
- Provides detailed logging of the processing steps

### budget.py

The main application file that:
- Sets up the Flask web server
- Provides API endpoints for budget processing
- Handles authentication and request validation
- Manages the processing queue
- Implements enhanced metadata tracking system

## Metadata System

The budget processing system includes comprehensive metadata tracking:

### Core Metadata Fields
- `version_id`: Unique identifier for each budget version
- `previous_version_id`: Reference to previous version (if any)
- `creation_timestamp`: When the budget was first created
- `last_modified_timestamp`: Last modification time
- `modified_by`: User who made the last modification
- `status`: Current status (draft/review/approved)

### Tracking Features
- **Approval Chain**: Tracks the approval workflow
- **Change Log**: Detailed history of modifications
- **Validation Status**: Current validation state and issues
- **Source Information**: Original data source details
- **Processing Statistics**: Performance and processing metrics

### Example Metadata Structure
```json
{
  "version_id": "GOOG0324PIXELDR_v2",
  "previous_version_id": "GOOG0324PIXELDR_v1",
  "creation_timestamp": "2024-01-22T10:30:00Z",
  "last_modified_timestamp": "2024-01-22T15:45:00Z",
  "modified_by": "john.doe@agency.com",
  "status": "review",
  "approval_chain": [
    {
      "step": "initial_review",
      "approver": "jane.smith@agency.com",
      "timestamp": "2024-01-22T16:00:00Z",
      "status": "approved"
    }
  ],
  "change_log": [
    {
      "timestamp": "2024-01-22T15:45:00Z",
      "type": "update",
      "details": {
        "class": "A",
        "field": "estimate_total",
        "old_value": "50000",
        "new_value": "55000"
      }
    }
  ]
}
```

## Running

1. Process a budget spreadsheet:
```bash
python src/scripts/process_budget.py
```

2. Run the web service:
```bash
python app.py
```

3. Access the API at `http://localhost:5000/api/v1/budget`

## Output

The processed budget data is saved to:
- `output/processed_budget_[FILENAME]_[DATE]_[VERSION].json` - Contains the processed line items and metadata
- `output/version_tracking.json` - Tracks processing versions for each file
