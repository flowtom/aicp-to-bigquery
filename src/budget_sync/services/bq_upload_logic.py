import time
import logging

logger = logging.getLogger(__name__)


def upload_cover_sheet_to_bq(bq_client, dataset_id, table_id, cover_sheet_row, max_retries=3):
    """
    Uploads a single cover sheet row to BigQuery using insert_rows_json.

    Args:
      bq_client: An instance of google.cloud.bigquery.Client.
      dataset_id: The BigQuery dataset ID.
      table_id: The BigQuery table ID for budgets.
      cover_sheet_row: A dictionary representing a row for the budgets table.
      max_retries: Maximum number of retries before failing.

    Returns:
      True if upload is successful, False otherwise.
    """
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    for attempt in range(max_retries):
        try:
            errors = bq_client.insert_rows_json(table_ref, [cover_sheet_row])
            if errors:
                logger.error("Errors uploading cover sheet row: %s", errors)
                raise Exception(f"BigQuery insertion errors: {errors}")
            logger.info("Uploaded cover sheet data to BigQuery successfully.")
            return True
        except Exception as e:
            wait_time = 2 ** attempt
            logger.error("Error uploading cover sheet to BigQuery (attempt %s): %s. Retrying in %s seconds...", attempt+1, e, wait_time)
            time.sleep(wait_time)
    return False


def upload_line_items_to_bq(bq_client, dataset_id, table_id, line_items_rows, max_retries=3):
    """
    Uploads line items rows to BigQuery using insert_rows_json.

    Args:
      bq_client: An instance of google.cloud.bigquery.Client.
      dataset_id: The BigQuery dataset ID.
      table_id: The BigQuery table ID for budget details.
      line_items_rows: A list of dictionaries, each representing a row for the budget_details table.
      max_retries: Maximum number of retries before failing.

    Returns:
      True if upload is successful, False otherwise.
    """
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    for attempt in range(max_retries):
        try:
            errors = bq_client.insert_rows_json(table_ref, line_items_rows)
            if errors:
                logger.error("Errors uploading line items: %s", errors)
                raise Exception(f"BigQuery insertion errors: {errors}")
            logger.info("Uploaded %d line items to BigQuery successfully.", len(line_items_rows))
            return True
        except Exception as e:
            wait_time = 2 ** attempt
            logger.error("Error uploading line items to BigQuery (attempt %s): %s. Retrying in %s seconds...", attempt+1, e, wait_time)
            time.sleep(wait_time)
    return False 