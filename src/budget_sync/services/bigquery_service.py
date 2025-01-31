"""
Service for handling BigQuery operations.
"""
from typing import List, Dict, Any, Optional
import logging
from google.cloud import bigquery
from google.api_core import retry
from datetime import datetime
import json
from pathlib import Path
import os

logger = logging.getLogger(__name__)

class BigQueryService:
    """Handles interactions with Google BigQuery."""
    
    def __init__(self, project_id: str, dataset_id: str):
        """Initialize BigQuery client with project and dataset."""
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Initialize client with just the project ID
        self.client = bigquery.Client(project=project_id)
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
        
        # Load schemas
        self.projects_schema = self._load_schema('projects_table_schema.json')
        self.budget_schema = self._load_schema('budget_table_schema.json')
        self.budget_detail_schema = self._load_schema('budget_detail_table_schema.json')
        self.validation_schema = self._load_schema('budget_validation_table_schema.json')
        
        # Table references - use fully qualified IDs
        self.projects_table_id = f"{self.project_id}.{self.dataset_id}.projects"
        self.budget_table_id = f"{self.project_id}.{self.dataset_id}.budgets"
        self.budget_detail_table_id = f"{self.project_id}.{self.dataset_id}.budget_details"
        self.validation_table_id = f"{self.project_id}.{self.dataset_id}.budget_validations"
        
        # Ensure tables exist
        self._ensure_tables_exist()
    
    def _load_schema(self, schema_file: str) -> List[Dict[str, Any]]:
        """Load BigQuery schema from JSON file."""
        schema_path = Path(__file__).parent.parent / 'models' / 'schemas' / schema_file
        try:
            with open(schema_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading schema {schema_file}: {str(e)}")
            raise
    
    def _ensure_dataset_exists(self):
        """Create dataset if it doesn't exist."""
        try:
            dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
            dataset.location = "US"
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_id} is ready")
        except Exception as e:
            logger.error(f"Error ensuring dataset exists: {str(e)}")
            raise
    
    def _recreate_table(self, table_id: str, schema: List[bigquery.SchemaField], time_partition_field: Optional[str] = None) -> None:
        """Delete and recreate a table with the given schema."""
        try:
            # Use fully qualified table ID
            if '.' not in table_id:
                table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
                
            self.client.delete_table(table_id, not_found_ok=True)
            logger.info(f"Deleted table {table_id}")
            
            table = bigquery.Table(table_id, schema=schema)
            if time_partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=time_partition_field
                )
            self.client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            logger.error(f"Error recreating table {table_id}: {str(e)}")
            raise
    
    def _ensure_tables_exist(self):
        """Create tables if they don't exist."""
        try:
            # Create projects table
            projects_table = bigquery.Table(
                self.projects_table_id,
                schema=self._create_schema(self.projects_schema)
            )
            self.client.create_table(projects_table, exists_ok=True)
            logger.info("Projects table is ready")
            
            # Create budget table
            budget_table = bigquery.Table(
                self.budget_table_id,
                schema=self._create_schema(self.budget_schema)
            )
            budget_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="upload_timestamp"
            )
            self.client.create_table(budget_table, exists_ok=True)
            logger.info("Budget table is ready")
            
            # Create budget detail table
            budget_detail_table = bigquery.Table(
                self.budget_detail_table_id,
                schema=self._create_schema(self.budget_detail_schema)
            )
            budget_detail_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="upload_timestamp"
            )
            self.client.create_table(budget_detail_table, exists_ok=True)
            logger.info("Budget detail table is ready")
            
            # Create validation table
            validation_table = bigquery.Table(
                self.validation_table_id,
                schema=self._create_schema(self.validation_schema)
            )
            validation_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="validation_timestamp"
            )
            self.client.create_table(validation_table, exists_ok=True)
            logger.info("Validation table is ready")
            
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {str(e)}")
            raise
    
    def _create_schema(self, schema_def: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """Convert schema definition to BigQuery SchemaField objects."""
        return [
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field['mode'],
                description=field.get('description', '')
            )
            for field in schema_def
        ]
    
    def _extract_project_id(self, budget_name: str) -> str:
        """Extract project ID from budget name (e.g. GOOG0324PIXELDR from GOOG0324PIXELDR_Estimate)."""
        return budget_name.split('_')[0]
    
    @retry.Retry()
    def create_or_update_project(self, project_data: Dict[str, Any]) -> str:
        """Create or update a project record."""
        try:
            project_id = project_data['project_id']
            
            # Check if project exists
            query = f"""
            SELECT project_id
            FROM `{self.projects_table_id}`
            WHERE project_id = @project_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("project_id", "STRING", project_id)
                ]
            )
            
            results = self.client.query(query, job_config=job_config).result()
            exists = len(list(results)) > 0
            
            if exists:
                # Update existing project
                update_fields = [f"{k} = @{k}" for k in project_data.keys()]
                query = f"""
                UPDATE `{self.projects_table_id}`
                SET {', '.join(update_fields)}
                WHERE project_id = @project_id
                """
            else:
                # Insert new project
                fields = list(project_data.keys())
                query = f"""
                INSERT INTO `{self.projects_table_id}`
                ({', '.join(fields)})
                VALUES ({', '.join([f'@{f}' for f in fields])})
                """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(k, "STRING" if isinstance(v, str) else "FLOAT", v)
                    for k, v in project_data.items()
                ]
            )
            
            self.client.query(query, job_config=job_config).result()
            logger.info(f"{'Updated' if exists else 'Created'} project {project_id}")
            
            return project_id
            
        except Exception as e:
            logger.error(f"Error creating/updating project: {str(e)}")
            raise
    
    @retry.Retry()
    def upload_budget(self, budget_data: Dict[str, Any]) -> str:
        """Upload budget data and return budget_id."""
        try:
            if not budget_data:
                logger.warning("No budget data to upload")
                return None
            
            errors = self.client.insert_rows_json(self.budget_table_id, [budget_data])
            if errors:
                logger.error(f"Errors uploading budget data: {errors}")
                raise Exception(f"Failed to upload budget data: {errors}")
            
            logger.info(f"Successfully uploaded budget {budget_data['budget_id']}")
            return budget_data['budget_id']
            
        except Exception as e:
            logger.error(f"Error uploading budget: {str(e)}")
            raise
    
    @retry.Retry()
    def upload_budget_details(self, detail_rows: List[Dict[str, Any]]) -> int:
        """Upload budget detail rows and return count of rows uploaded."""
        try:
            if not detail_rows:
                logger.warning("No detail rows to upload")
                return 0
            
            errors = self.client.insert_rows_json(self.budget_detail_table_id, detail_rows)
            if errors:
                logger.error(f"Errors uploading budget details: {errors}")
                raise Exception(f"Failed to upload budget details: {errors}")
            
            logger.info(f"Successfully uploaded {len(detail_rows)} budget detail rows")
            return len(detail_rows)
            
        except Exception as e:
            logger.error(f"Error uploading budget details: {str(e)}")
            raise
    
    @retry.Retry()
    def upload_validations(self, validation_rows: List[Dict[str, Any]]) -> int:
        """Upload validation results and return count of rows uploaded."""
        try:
            if not validation_rows:
                logger.warning("No validation rows to upload")
                return 0
            
            errors = self.client.insert_rows_json(self.validation_table_id, validation_rows)
            if errors:
                logger.error(f"Errors uploading validations: {errors}")
                raise Exception(f"Failed to upload validations: {errors}")
            
            logger.info(f"Successfully uploaded {len(validation_rows)} validation rows")
            return len(validation_rows)
            
        except Exception as e:
            logger.error(f"Error uploading validations: {str(e)}")
            raise 