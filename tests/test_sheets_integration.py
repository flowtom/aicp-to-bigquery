# test_sheets_integration.py
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import uuid
import json
from src.utils.data_utils import process_budget_row, DateTimeEncoder, validate_row

def load_test_data_to_bigquery():
    """Load sample data from Google Sheet to BigQuery with validated processing"""
    try:
        # Initialize clients
        CREDENTIALS = service_account.Credentials.from_service_account_file(
            'service-account-key.json',
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        )
        
        bq_client = bigquery.Client(project='budgetsync-2025', credentials=CREDENTIALS)
        sheets_service = build('sheets', 'v4', credentials=CREDENTIALS)
        
        # Test parameters
        SPREADSHEET_ID = '1Rh3z8u1qYU496UQNXabZYEB6QqiggdzRgOAgzaZj7Bw'
        SHEET_NAME = 'AICP BUDGET - toolkit'
        upload_id = str(uuid.uuid4())
        timestamp = datetime.now()
        
        print("Fetching data from Google Sheets...")
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1:S1000"
        ).execute()
        
        rows = result.get('values', [])
        print(f"Retrieved {len(rows)} rows from sheet")
        
        # Process rows using validated utilities
        processed_rows = []
        current_class_code = None
        current_class_name = None
        skipped_rows = 0
        processed_count = 0
        invalid_rows = 0
        
        print("\nProcessing rows...")
        for i, row in enumerate(rows, 1):
            try:
                if not row:
                    skipped_rows += 1
                    continue
                    
                first_cell = str(row[0]) if row else ''
                
                # Debug print for class headers
                if ': ' in first_cell:
                    current_class_code, current_class_name = first_cell.split(': ', 1)
                    print(f"\nFound class: {current_class_code}: {current_class_name}")
                    continue
                
                # Process line item
                processed_row = process_budget_row(
                    row, 
                    current_class_code, 
                    current_class_name, 
                    upload_id, 
                    timestamp
                )
                
                if processed_row:
                    # Validate row before adding
                    if validate_row(processed_row, i):
                        processed_rows.append(processed_row)
                        processed_count += 1
                        if processed_count % 10 == 0:
                            print(f"Processed {processed_count} valid rows...")
                    else:
                        invalid_rows += 1
                        print(f"Invalid row {i}: {row}")
                else:
                    skipped_rows += 1
                    
            except Exception as e:
                invalid_rows += 1
                print(f"Error processing row {i}: {str(e)}")
                continue
        
        print(f"\nProcessing complete:")
        print(f"- Valid rows processed: {processed_count}")
        print(f"- Rows skipped: {skipped_rows}")
        print(f"- Invalid rows: {invalid_rows}")
        
        if not processed_rows:
            print("No valid rows to load to BigQuery")
            return
        
        # Debug print first few rows
        print("\nSample of processed rows:")
        for i, row in enumerate(processed_rows[:3]):
            print(f"Row {i}:")
            print(json.dumps(row, indent=2, cls=DateTimeEncoder))
        
        print("\nLoading to BigQuery...")
        table_id = 'budgetsync-2025.raw_budgets.raw_budget_data'
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("upload_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("budget_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("upload_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("class_code", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("class_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("line_item_number", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("line_item_description", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("estimate_days", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("estimate_rate", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("estimate_total", "FLOAT", mode="NULLABLE")
            ]
        )
        
        # Convert to JSON with datetime handling
        json_data = json.dumps(processed_rows, cls=DateTimeEncoder)
        rows_to_insert = json.loads(json_data)
        
        job = bq_client.load_table_from_json(
            rows_to_insert,
            table_id,
            job_config=job_config
        )
        
        try:
            job.result()
            print(f"\nSuccessfully loaded {len(processed_rows)} rows to BigQuery")
        except Exception as e:
            print(f"\nError loading to BigQuery: {str(e)}")
            if job.errors:
                for error in job.errors:
                    print(f"- {error['message']}")
            raise
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    load_test_data_to_bigquery()