# Budget Sync Project Structure

```
budget-sync/
├── config/                          # Configuration files
│   ├── __init__.py
│   ├── config.json                  # Main configuration file
│   └── .gitkeep                     # Placeholder (service account key should not be committed)
│
├── src/                            # Source code
│   ├── __init__.py
│   │
│   ├── api/                        # API related code
│   │   ├── __init__.py
│   │   ├── routes.py              # API endpoints
│   │   └── middleware.py          # API middleware
│   │
│   ├── models/                    # Data models and schemas
│   │   ├── __init__.py
│   │   └── schemas/              # JSON schemas
│   │       ├── __init__.py
│   │       ├── raw_schema.json
│   │       ├── processed_schema.json
│   │       └── budget_metadata_schema.json
│   │
│   ├── services/                  # Business logic
│   │   ├── __init__.py
│   │   ├── budget_processor.py    # Budget processing logic
│   │   └── bigquery_service.py    # BigQuery interaction logic
│   │
│   └── utils/                     # Utility functions
│       ├── __init__.py
│       ├── config.py              # Configuration management
│       ├── data_utils.py          # Data processing utilities
│       ├── env.py                 # Environment variable management
│       └── errors.py              # Custom exceptions
│
├── tests/                         # Test files
│   ├── __init__.py
│   ├── conftest.py               # Test configuration
│   ├── test_data/               # Test data files
│   │   └── sample_budget.xlsx
│   ├── test_api/
│   │   └── test_routes.py
│   ├── test_services/
│   │   └── test_budget_processor.py
│   └── test_utils/
│       └── test_data_utils.py
│
├── scripts/                       # Utility scripts
│   ├── setup_bigquery.py         # BigQuery setup script
│   └── generate_schemas.py        # Schema generation script
│
├── .env.example                   # Example environment variables
├── .gitignore                    # Git ignore file
├── Dockerfile                    # Docker configuration
├── docker-compose.yml            # Docker compose configuration
├── pyproject.toml               # Project metadata and dependencies
├── README.md                    # Project documentation
└── setup.py                     # Package setup file
```

## Key Directories and Files

### `/config`
- Configuration files
- Service account credentials (not committed to git)
- Environment-specific settings

### `/src`
Main source code directory with modular organization:

#### `/src/api`
- REST API implementation
- Route handlers
- Middleware

#### `/src/models`
- Data schemas
- Data validation
- Type definitions

#### `/src/services`
- Core business logic
- External service integrations
- Data processing

#### `/src/utils`
- Helper functions
- Common utilities
- Error handling

### `/tests`
- Unit tests
- Integration tests
- Test data and fixtures

### `/scripts`
- Development utilities
- Database setup
- Schema management

## Key Files

### Configuration Files
- `pyproject.toml`: Project dependencies and metadata
- `Dockerfile`: Container configuration
- `.env.example`: Template for environment variables

### Documentation
- `README.md`: Project overview and setup instructions
- `PROJECT_STRUCTURE.md`: This file, explaining the project organization

### Setup
- `setup.py`: Package installation
- `requirements.txt`: (Optional) Direct pip dependencies

## Environment Variables
Create a `.env` file based on `.env.example` with:
```
PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
FLASK_ENV=development
FLASK_APP=src/api/routes.py
``` 