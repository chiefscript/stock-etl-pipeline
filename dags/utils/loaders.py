import os
import logging
import pandas as pd
import tempfile
from typing import Optional, Dict, List, Any, Union
from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


def load_to_bigquery(
        data_path: str,
        table_id: str,
        schema: Optional[List] = None,
        project_id: Optional[str] = None,
        write_disposition: str = 'WRITE_APPEND'
) -> Dict[str, Any]:
    """
    Load data from a CSV file to BigQuery.

    Args:
        data_path: Path to the CSV file
        table_id: BigQuery table ID in the format 'dataset.table'
        schema: BigQuery table schema (optional)
        project_id: GCP project ID (optional, defaults to environment)
        write_disposition: BigQuery write disposition

    Returns:
        Dict with load results
    """
    logger.info(f"Loading data from {data_path} to BigQuery table {table_id}")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Configure job options
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=schema is None,
        schema=schema,
        write_disposition=write_disposition,
    )

    # Get file size for logging
    file_size = os.path.getsize(data_path) / (1024 * 1024)  # Size in MB
    logger.info(f"File size: {file_size:.2f} MB")

    try:
        # Open the file and load to BigQuery
        with open(data_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )

            # Wait for job to complete
            load_job.result()

            # Get the table
            table = client.get_table(table_id)

            # Return results
            results = {
                'job_id': load_job.job_id,
                'rows_loaded': load_job.output_rows,
                'errors': load_job.errors,
                'table_rows': table.num_rows,
                'status': 'success'
            }

            logger.info(f"Loaded {results['rows_loaded']} rows to {table_id}")
            return results

    except GoogleCloudError as e:
        logger.error(f"Error loading to BigQuery: {e}")
        return {
            'status': 'error',
            'errors': str(e)
        }


def create_bigquery_schema(columns_config: Dict[str, Dict[str, Any]]) -> List[bigquery.SchemaField]:
    """
    Create a BigQuery schema from a dictionary configuration.

    Args:
        columns_config: Dictionary of column configurations

    Returns:
        List of BigQuery SchemaField objects
    """
    schema = []

    for column_name, config in columns_config.items():
        field = bigquery.SchemaField(
            name=column_name,
            field_type=config.get('type', 'STRING'),
            mode=config.get('mode', 'NULLABLE'),
            description=config.get('description', None)
        )
        schema.append(field)

    return schema


def upsert_to_bigquery(
        data_path: str,
        table_id: str,
        temp_table_id: str,
        key_columns: List[str],
        schema: Optional[List] = None,
        project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Perform an upsert (update/insert) operation to BigQuery.

    This function first loads data to a temporary table, then
    performs a MERGE operation to update existing records and
    insert new ones.

    Args:
        data_path: Path to the CSV file
        table_id: BigQuery target table ID in the format 'dataset.table'
        temp_table_id: BigQuery temporary table ID
        key_columns: List of columns to use as the merge key
        schema: BigQuery table schema (optional)
        project_id: GCP project ID (optional, defaults to environment)

    Returns:
        Dict with upsert results
    """
    logger.info(f"Upserting data from {data_path} to BigQuery table {table_id}")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # First, load data to temporary table
    temp_load_result = load_to_bigquery(
        data_path=data_path,
        table_id=temp_table_id,
        schema=schema,
        project_id=project_id,
        write_disposition='WRITE_TRUNCATE'  # Overwrite temp table
    )

    if temp_load_result.get('status') != 'success':
        logger.error("Failed to load data to temporary table")
        return temp_load_result

    try:
        # Get all columns from the temp table
        temp_table = client.get_table(temp_table_id)
        columns = [field.name for field in temp_table.schema]

        # Remove key columns from the list of columns to update
        update_columns = [col for col in columns if col not in key_columns]

        # Build MERGE query
        merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON {' AND '.join([f'T.{col} = S.{col}' for col in key_columns])}
        WHEN MATCHED THEN
          UPDATE SET {', '.join([f'T.{col} = S.{col}' for col in update_columns])}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(columns)})
          VALUES ({', '.join([f'S.{col}' for col in columns])})
        """

        # Execute MERGE query
        query_job = client.query(merge_query)
        query_result = query_job.result()

        # Return results
        results = {
            'job_id': query_job.job_id,
            'rows_affected': query_job.num_dml_affected_rows,
            'status': 'success'
        }

        logger.info(f"Upserted {results['rows_affected']} rows to {table_id}")
        return results

    except GoogleCloudError as e:
        logger.error(f"Error upserting to BigQuery: {e}")
        return {
            'status': 'error',
            'errors': str(e)
        }


def load_dataframe_to_bigquery(
        df: pd.DataFrame,
        table_id: str,
        schema: Optional[List] = None,
        project_id: Optional[str] = None,
        write_disposition: str = 'WRITE_APPEND'
) -> Dict[str, Any]:
    """
    Load data from a pandas DataFrame directly to BigQuery.

    Args:
        df: DataFrame to load
        table_id: BigQuery table ID in the format 'dataset.table'
        schema: BigQuery table schema (optional)
        project_id: GCP project ID (optional, defaults to environment)
        write_disposition: BigQuery write disposition

    Returns:
        Dict with load results
    """
    logger.info(f"Loading DataFrame with {len(df)} rows to BigQuery table {table_id}")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Configure job options
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
    )

    try:
        # Load the DataFrame
        load_job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        # Wait for job to complete
        load_job.result()

        # Get the table
        table = client.get_table(table_id)

        # Return results
        results = {
            'job_id': load_job.job_id,
            'rows_loaded': load_job.output_rows,
            'errors': load_job.errors,
            'table_rows': table.num_rows,
            'status': 'success'
        }

        logger.info(f"Loaded {results['rows_loaded']} rows to {table_id}")
        return results

    except GoogleCloudError as e:
        logger.error(f"Error loading DataFrame to BigQuery: {e}")
        return {
            'status': 'error',
            'errors': str(e)
        }


def load_to_gcs(
        data: Union[str, pd.DataFrame],
        bucket_name: str,
        blob_name: str,
        project_id: Optional[str] = None,
        content_type: Optional[str] = None
) -> Dict[str, Any]:
    """
    Load data to Google Cloud Storage.

    Args:
        data: Either a path to a file or a pandas DataFrame
        bucket_name: GCS bucket name
        blob_name: Name of the blob in the bucket
        project_id: GCP project ID (optional, defaults to environment)
        content_type: Content type of the uploaded file (optional)

    Returns:
        Dict with upload results
    """
    logger.info(f"Uploading data to gs://{bucket_name}/{blob_name}")

    # Initialize GCS client
    client = storage.Client(project=project_id)

    try:
        # Get bucket
        bucket = client.bucket(bucket_name)

        # Create blob
        blob = bucket.blob(blob_name)

        # Set content type if provided
        if content_type:
            blob.content_type = content_type

        # Upload data
        if isinstance(data, pd.DataFrame):
            # Create a temporary file for the DataFrame
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
                temp_path = temp_file.name
                data.to_csv(temp_path, index=False)

            # Upload the file
            blob.upload_from_filename(temp_path)

            # Clean up
            os.remove(temp_path)

        elif isinstance(data, str) and os.path.isfile(data):
            # Upload from file path
            blob.upload_from_filename(data)

        else:
            raise ValueError("Data must be either a DataFrame or a path to a file")

        # Return results
        results = {
            'bucket': bucket_name,
            'blob': blob_name,
            'size': blob.size,
            'md5_hash': blob.md5_hash,
            'public_url': blob.public_url,
            'status': 'success'
        }

        logger.info(f"Successfully uploaded {results['size']} bytes to gs://{bucket_name}/{blob_name}")
        return results

    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        return {
            'status': 'error',
            'errors': str(e)
        }


def create_bigquery_table_if_not_exists(
        table_id: str,
        schema: List[bigquery.SchemaField],
        project_id: Optional[str] = None,
        partition_field: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
        description: Optional[str] = None
):
    """
    Create a BigQuery table if it doesn't exist.

    Args:
        table_id: BigQuery table ID in the format 'dataset.table'
        schema: BigQuery table schema
        project_id: GCP project ID (optional, defaults to environment)
        partition_field: Field to partition the table by (optional)
        cluster_fields: Fields to cluster the table by (optional)
        description: Table description (optional)

    Returns:
        Dict with results
    """
    logger.info(f"Creating BigQuery table {table_id} if it doesn't exist")

    # Initialize BigQuery client
    client = bigquery.Client
