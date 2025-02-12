# Budget Sync Project Structure

```
budget-sync/
├── Dockerfile                      # Container configuration
├── PROJECT_STRUCTURE.md           # This file - project structure documentation
├── README.md                      # Project overview and setup instructions
│
├── config/                        # Configuration files
│   ├── config.json               # Main configuration settings
│   └── service-account-key.json  # Google service account credentials (not in git)
│
├── output/                       # Processed budget output files
│   ├── processed_budget_*.json   # Individual processed budgets with versioning
│   └── version_tracking.json     # Tracks version history of processed budgets
│
├── poetry.lock                   # Poetry dependency lock file
├── pyproject.toml               # Project metadata and dependencies
├── pytest.ini                   # PyTest configuration
├── requirements.txt             # Direct pip dependencies
│
├── src/                         # Source code
│   └── budget_sync/            # Main package
│       ├── api/                # API components
│       │   ├── __init__.py
│       │   └── routes.py       # API endpoints and request handling
│       │
│       ├── constants/          # Constant values and mappings
│       │   └── cover_sheet_mappings.py  # Cover sheet cell mappings
│       │
│       ├── models/            # Data models and schemas
│       │   ├── budget.py     # Core budget data structures
│       │   └── schemas/      # JSON validation schemas
│       │       ├── budget_metadata_schema.json
│       │       ├── processed_schema.json
│       │       └── raw_schema.json
│       │
│       ├── scripts/          # Utility and test scripts
│       │   ├── process_budget.py          # Main processing script
│       │   ├── test_cover_sheet_processor.py  # Cover sheet testing
│       │   └── test_run.py                # Development testing utility
│       │
│       ├── services/         # Core business logic
│       │   ├── __init__.py
│       │   └── budget_processor.py  # Main budget processing logic
│       │
│       └── utils/           # Utility functions
│           ├── data_utils.py       # Data processing utilities
│           └── data_validation.py  # Validation functions
│
└── tests/                    # Test suite
    ├── conftest.py          # Test configuration and fixtures
    ├── fixtures/            # Test data
    │   ├── sample_budget.csv
    │   ├── test_budget_data.json
    │   └── test_credentials.json
    │
    ├── test_api/           # API tests
    │   └── __init__.py
    │
    ├── test_services/      # Service layer tests
    │   └── test_budget_processor.py
    │
    └── test_utils/        # Utility function tests
        ├── test_budget_validation.py
        └── test_data_utils.py
```

## Key Components

### Configuration
- `config/`: Contains configuration files and credentials
- `pyproject.toml`: Project dependencies and metadata
- `pytest.ini`: Test configuration

### Source Code (`src/budget_sync/`)
- `api/`: REST API implementation
- `constants/`: System-wide constants and mappings
- `models/`: Data structures and schemas
- `scripts/`: Utility and test scripts
- `services/`: Core business logic
- `utils/`: Helper functions

### Testing (`tests/`)
- Organized by component (api, services, utils)
- Includes fixtures and sample data
- Comprehensive test coverage

### Output
- `output/`: Contains processed budgets
- Uses semantic versioning (MAJOR.MINOR.PATCH)
- Maintains version history

## File Naming Conventions

### Processed Budgets
Format: `processed_budget_[FILENAME]_[VERSION].json`
Example: `processed_budget_GOOG0324PIXELDR_Estimate-Brand_DR_Combined-01-22-25_1.0.76.json`

Components:
- Project identifier (e.g., GOOG0324PIXELDR)
- Sheet name (e.g., Estimate-Brand_DR_Combined)
- Date (e.g., 01-22-25)
- Version number (e.g., 1.0.76)

### Version Tracking
`version_tracking.json`: Maintains history of all processed versions 