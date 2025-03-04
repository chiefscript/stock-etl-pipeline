import os
import logging
from typing import Callable, Dict, Any, Optional

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Operator that runs data quality checks on a dataset.

    This operator reads data from a file, runs a validation function on it,
    and raises an exception if the validation fails.
    """

    @apply_defaults
    def __init__(
            self,
            data_path: str,
            validation_callable: Callable,
            validation_type: Optional[str] = None,
            validation_kwargs: Optional[Dict[str, Any]] = None,
            *args, **kwargs
    ):
        """
        Initialize the operator.

        Args:
            data_path: Path to the data file to validate
            validation_callable: Function to call for validation
            validation_type: Type of validation to perform (passed to validation_callable)
            validation_kwargs: Additional keyword arguments to pass to validation_callable
        """
        super().__init__(*args, **kwargs)
        self.data_path = data_path
        self.validation_callable = validation_callable
        self.validation_type = validation_type
        self.validation_kwargs = validation_kwargs or {}

    def execute(self, context):
        """
        Execute the operator.

        Args:
            context: Airflow context

        Returns:
            Dict with validation results

        Raises:
            ValueError: If validation fails
        """
        self.log.info(f"Running data quality validation on {self.data_path}")

        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"Data file not found: {self.data_path}")

        # Read the data file based on file extension
        file_ext = os.path.splitext(self.data_path)[1].lower()

        if file_ext == '.csv':
            df = pd.read_csv(self.data_path)
        elif file_ext in ['.parquet', '.pq']:
            df = pd.read_parquet(self.data_path)
        elif file_ext == '.json':
            df = pd.read_json(self.data_path)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        # Run validation
        kwargs = {**self.validation_kwargs}
        if self.validation_type:
            kwargs['validation_type'] = self.validation_type

        is_valid, results = self.validation_callable(df, **kwargs)

        # Log validation results
        self.log.info(f"Validation results: {results}")

        # Record metrics in task instance XCom
        context['ti'].xcom_push(key='validation_metrics', value=results.get('metrics', {}))

        # Handle warnings
        for warning in results.get('warnings', []):
            self.log.warning(f"Validation warning: {warning}")

        # If validation failed, raise an exception
        if not is_valid:
            error_message = "\n".join(results.get('errors', ['Validation failed']))
            raise ValueError(f"Data quality validation failed: {error_message}")

        self.log.info("Data quality validation passed")

        return results
