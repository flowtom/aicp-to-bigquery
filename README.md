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

## Running

3. Run the script:

```bash
python test_sheets_integration.py
```

4. Run the Flask app:

```bash
python app.py
```

5. Access the API at `http://localhost:5000/api/v1/budget`
