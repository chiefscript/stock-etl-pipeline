import logging
import tempfile
from typing import Callable, Dict, List, Any, Optional

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)


class APIToGCSOperator(BaseOperator):
    """
    Operator that fetches data from an API and saves it to Google Cloud Storage.
    """
    template_fields = ('gcs_path',)

    @apply_defaults
    def __init__(
            self,
            fetch_callable: Callable,
            gcs_bucket: str,
            gcs_path: str,
            fetch_args: Optional[List] = None,
            fetch_kwargs: Optional[Dict[str, Any]] = None,
            gcp_conn_id: str = 'google_cloud_default',
            file_format: str = 'csv',
            *args, **kwargs
    ):
        """
        Initialize the operator.

        Args:
            fetch_callable: Function to call for fetching data
            gcs_bucket: GCS bucket name
            gcs_path: Path within the bucket
            fetch_args: Positional arguments to pass to fetch_callable
            fetch_kwargs: Keyword arguments to pass to fetch_callable
            gcp_conn_id: Airflow connection to use for GCS
            file_format: Format to save data (csv, json, parquet)
        """
        super().__init__(*args, **kwargs)
        self.fetch_callable = fetch_callable
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.fetch_args = fetch_args or []
        self.fetch_kwargs = fetch_kwargs or {}
        self.gcp_conn_id = gcp_conn_id
        self.file_format = file_format.lower()

    def execute(self, context):
        """
        Execute the operator.

        Args:
            context: Airflow context

        Returns:
            Dict with execution results
        """
        self.log.info(f"Fetching data from API and saving to gs://{self.gcs_bucket}/{self.gcs_path}")

        # Fetch data from API
        data = self.fetch_callable(*self.fetch_args, **self.fetch_kwargs)

        # Convert to DataFrame if not already
        if not isinstance(data, pd.DataFrame):
            try:
                # Try to convert dict or list to DataFrame
                if isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    df = pd.DataFrame(data)
            except Exception as e:
                self.log.error(f"Could not convert data to DataFrame: {e}")
                raise ValueError(f"API data must be convertible to DataFrame: {e}")
        else:
            df = data

        # Save to a temporary file
        with tempfile.NamedTemporaryFile(
                prefix=f"airflow_api_data_",
                suffix=f".{self.file_format}",
                delete=False
        ) as temp_file:
            temp_path = temp_file.name

            # Save in the specified format
            if self.file_format == 'csv':
                df.to_csv(temp_path, index=False)
            elif self.file_format == 'json':
                df.to_json(temp_path, orient='records', lines=True)
            elif self.file_format == 'parquet':
                df.to_parquet(temp_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")

        # Upload to GCS
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=self.gcs_path,
            filename=temp_path
        )

        self.log.info(f"Successfully uploaded data to gs://{self.gcs_bucket}/{self.gcs_path}")

        # Push metrics to XCom
        metrics = {
            'rows': len(df),
            'columns': df.columns.tolist(),
            'file_format': self.file_format,
            'gcs_path': f"gs://{self.gcs_bucket}/{self.gcs_path}"
        }
        context['ti'].xcom_push(key='api_to_gcs_metrics', value=metrics)

        return metrics
