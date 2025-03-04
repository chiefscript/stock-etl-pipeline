from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

from utils.extractors import extract_alpha_vantage_data, extract_yahoo_finance_data
from utils.transformers import transform_stock_data, merge_stock_datasets
from utils.validators import validate_raw_data, validate_transformed_data
from plugins.custom_operators.data_quality_operator import DataQualityOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Load configuration from Airflow variables
config = Variable.get("stock_etl_config", deserialize_json=True)
STOCK_SYMBOLS = config.get('stock_symbols', ['AAPL', 'MSFT', 'GOOGL', 'AMZN'])
BUCKET_NAME = config.get('gcs_bucket')
BQ_DATASET = config.get('bigquery_dataset')
BQ_TABLE = config.get('bigquery_table')

with DAG(
    'stock_data_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for stock data from multiple sources to BigQuery',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stocks', 'etl', 'finance'],
    max_active_runs=1,
) as dag:

    # Extract data from Alpha Vantage API
    extract_alpha_vantage_task = PythonOperator(
        task_id='extract_alpha_vantage_data',
        python_callable=extract_alpha_vantage_data,
        op_kwargs={
            'symbols': STOCK_SYMBOLS,
            'output_path': '/tmp/alpha_vantage_data.csv',
            'api_key': Variable.get("alpha_vantage_api_key"),
        },
    )

    # Extract data from Yahoo Finance API
    extract_yahoo_finance_task = PythonOperator(
        task_id='extract_yahoo_finance_data',
        python_callable=extract_yahoo_finance_data,
        op_kwargs={
            'symbols': STOCK_SYMBOLS,
            'output_path': '/tmp/yahoo_finance_data.csv',
            'period': '1mo'  # Get 1 month of data
        },
    )

    # Validate raw data
    validate_raw_alpha_vantage_data = DataQualityOperator(
        task_id='validate_raw_alpha_vantage_data',
        data_path='/tmp/alpha_vantage_data.csv',
        validation_callable=validate_raw_data,
        validation_type='alpha_vantage'
    )

    validate_raw_yahoo_data = DataQualityOperator(
        task_id='validate_raw_yahoo_data',
        data_path='/tmp/yahoo_finance_data.csv',
        validation_callable=validate_raw_data,
        validation_type='yahoo_finance'
    )

    # Transform Alpha Vantage data
    transform_alpha_vantage_task = PythonOperator(
        task_id='transform_alpha_vantage_data',
        python_callable=transform_stock_data,
        op_kwargs={
            'input_path': '/tmp/alpha_vantage_data.csv',
            'output_path': '/tmp/transformed_alpha_vantage_data.csv',
            'source': 'alpha_vantage'
        },
    )

    # Transform Yahoo Finance data
    transform_yahoo_finance_task = PythonOperator(
        task_id='transform_yahoo_finance_data',
        python_callable=transform_stock_data,
        op_kwargs={
            'input_path': '/tmp/yahoo_finance_data.csv',
            'output_path': '/tmp/transformed_yahoo_finance_data.csv',
            'source': 'yahoo_finance'
        },
    )

    # Merge transformed datasets
    merge_datasets_task = PythonOperator(
        task_id='merge_datasets',
        python_callable=merge_stock_datasets,
        op_kwargs={
            'input_paths': [
                '/tmp/transformed_alpha_vantage_data.csv',
                '/tmp/transformed_yahoo_finance_data.csv'
            ],
            'output_path': '/tmp/merged_stock_data.csv'
        },
    )

    # Validate transformed data
    validate_transformed_data_task = DataQualityOperator(
        task_id='validate_transformed_data',
        data_path='/tmp/merged_stock_data.csv',
        validation_callable=validate_transformed_data
    )

    # Upload merged data to GCS
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/merged_stock_data.csv',
        dst=f'stock_data/{datetime.today().strftime("%Y-%m-%d")}/merged_stock_data.csv',
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
    )

    # Load data from GCS to BigQuery
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[f'stock_data/{datetime.today().strftime("%Y-%m-%d")}/merged_stock_data.csv'],
        destination_project_dataset_table=f'{BQ_DATASET}.{BQ_TABLE}',
        schema_fields=[
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'open', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'high', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'low', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'close', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'data_source', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        ],
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        gcp_conn_id='google_cloud_default',
    )
