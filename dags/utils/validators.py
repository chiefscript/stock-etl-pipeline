import logging
import pandas as pd
from typing import Dict, List, Tuple, Any
import pandera as pa

logger = logging.getLogger(__name__)


# Define schemas for data validation

def get_raw_alpha_vantage_schema() -> pa.DataFrameSchema:
    """
    Get pandera schema for raw Alpha Vantage data.
    """
    return pa.DataFrameSchema({
        'date': pa.Column(pa.String, nullable=False),
        'symbol': pa.Column(pa.String, nullable=False),
        'open': pa.Column(pa.Float, nullable=True),
        'high': pa.Column(pa.Float, nullable=True),
        'low': pa.Column(pa.Float, nullable=True),
        'close': pa.Column(pa.Float, nullable=False),
        'volume': pa.Column(pa.Int, nullable=True),
        'data_source': pa.Column(pa.String, nullable=False),
        'extracted_at': pa.Column(pa.String, nullable=False)
    })


def get_raw_yahoo_finance_schema() -> pa.DataFrameSchema:
    """
    Get pandera schema for raw Yahoo Finance data.
    """
    return pa.DataFrameSchema({
        'date': pa.Column(pa.String, nullable=False),
        'symbol': pa.Column(pa.String, nullable=False),
        'open': pa.Column(pa.Float, nullable=True),
        'high': pa.Column(pa.Float, nullable=True),
        'low': pa.Column(pa.Float, nullable=True),
        'close': pa.Column(pa.Float, nullable=False),
        'volume': pa.Column(pa.Int, nullable=True),
        'data_source': pa.Column(pa.String, nullable=False),
        'extracted_at': pa.Column(pa.String, nullable=False)
    })


def get_transformed_data_schema() -> pa.DataFrameSchema:
    """
    Get pandera schema for transformed data.
    """
    return pa.DataFrameSchema({
        'date': pa.Column(pa.String, nullable=False),
        'symbol': pa.Column(pa.String, nullable=False),
        'open': pa.Column(pa.Float, nullable=True),
        'high': pa.Column(pa.Float, nullable=True),
        'low': pa.Column(pa.Float, nullable=True),
        'close': pa.Column(pa.Float, nullable=False),
        'volume': pa.Column(pa.Int, nullable=True),
        'data_source': pa.Column(pa.String, nullable=False),
        'processed_at': pa.Column(pa.String, nullable=False),
        'daily_change_pct': pa.Column(pa.Float, nullable=True),
        'daily_volatility': pa.Column(pa.Float, nullable=True)
    })


def validate_raw_data(df: pd.DataFrame, validation_type: str = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate raw data from various sources.

    Args:
        df: DataFrame to validate
        validation_type: Type of validation to perform ('alpha_vantage' or 'yahoo_finance')

    Returns:
        Tuple of (is_valid, validation_results)
    """
    validation_results = {
        'passed': True,
        'errors': [],
        'warnings': [],
        'metrics': {}
    }

    try:
        # Basic validation checks

        # Check for empty dataframe
        if df.empty:
            validation_results['passed'] = False
            validation_results['errors'].append("DataFrame is empty")
            return False, validation_results

        # Check for required columns
        required_columns = ['date', 'symbol', 'close', 'data_source']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            validation_results['passed'] = False
            validation_results['errors'].append(f"Missing required columns: {missing_columns}")
            return False, validation_results

        # Source-specific schema validation
        if validation_type == 'alpha_vantage':
            schema = get_raw_alpha_vantage_schema()
        elif validation_type == 'yahoo_finance':
            schema = get_raw_yahoo_finance_schema()
        else:
            # Use a generic schema if no specific type provided
            schema = None

        if schema:
            try:
                schema.validate(df)
            except pa.errors.SchemaError as e:
                validation_results['passed'] = False
                validation_results['errors'].append(f"Schema validation failed: {str(e)}")
                return False, validation_results

        # Check for negative prices
        if (df['close'] < 0).any():
            validation_results['passed'] = False
            validation_results['errors'].append("Found negative close prices")
            return False, validation_results

        # Check for future dates
        if 'date' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['date']):
                df['date_temp'] = pd.to_datetime(df['date'])
            else:
                df['date_temp'] = df['date']

            today = pd.Timestamp.now().normalize()
            future_dates = df[df['date_temp'] > today]

            if not future_dates.empty:
                validation_results['passed'] = False
                validation_results['errors'].append(f"Found {len(future_dates)} records with future dates")
                return False, validation_results

        # Calculate metrics
        validation_results['metrics'] = {
            'record_count': len(df),
            'symbol_count': df['symbol'].nunique(),
            'date_range': [df['date'].min(), df['date'].max()],
            'missing_values': df.isnull().sum().to_dict()
        }

        # Check for reasonable date range (e.g., not too old)
        if 'date_temp' in df.columns:
            oldest_date = pd.to_datetime(df['date'].min())
            if oldest_date < (pd.Timestamp.now() - pd.Timedelta(days=365)):
                validation_results['warnings'].append("Data contains records older than one year")

        # Check for duplicate records
        duplicates = df.duplicated(subset=['date', 'symbol']).sum()
        if duplicates > 0:
            validation_results['warnings'].append(f"Found {duplicates} duplicate records")

        return validation_results['passed'], validation_results

    except Exception as e:
        validation_results['passed'] = False
        validation_results['errors'].append(f"Validation error: {str(e)}")
        return False, validation_results


def validate_transformed_data(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate transformed data.

    Args:
        df: DataFrame to validate

    Returns:
        Tuple of (is_valid, validation_results)
    """
    validation_results = {
        'passed': True,
        'errors': [],
        'warnings': [],
        'metrics': {}
    }

    try:
        # Schema validation
        schema = get_transformed_data_schema()
        try:
            schema.validate(df)
        except pa.errors.SchemaError as e:
            validation_results['passed'] = False
            validation_results['errors'].append(f"Schema validation failed: {str(e)}")
            return False, validation_results

        # Check for negative prices
        if (df['close'] < 0).any():
            validation_results['passed'] = False
            validation_results['errors'].append("Found negative close prices")
            return False, validation_results

        # Check for reasonable values
        # Price range check
        max_price = df['close'].max()
        if max_price > 10000:  # Arbitrary threshold for suspicious values
            validation_results['warnings'].append(f"Found unusually high price: {max_price}")

        # Volume range check
        if 'volume' in df.columns:
            max_volume = df['volume'].max()
            if max_volume > 1000000000:  # 1 billion shares
                validation_results['warnings'].append(f"Found unusually high volume: {max_volume}")

        # Check daily volatility is not extremely high
        if 'daily_volatility' in df.columns:
            high_volatility = df[df['daily_volatility'] > 20]  # 20% volatility threshold
            if not high_volatility.empty:
                validation_results['warnings'].append(
                    f"Found {len(high_volatility)} records with high volatility (>20%)")

        # Calculate metrics
        validation_results['metrics'] = {
            'record_count': len(df),
            'symbol_count': df['symbol'].nunique(),
            'date_range': [df['date'].min(), df['date'].max()],
            'missing_values': df.isnull().sum().to_dict(),
            'sources': df['data_source'].value_counts().to_dict()
        }

        # Check for duplicate records
        duplicates = df.duplicated(subset=['date', 'symbol', 'data_source']).sum()
        if duplicates > 0:
            validation_results['warnings'].append(f"Found {duplicates} duplicate records")

        # Check for data consistency across sources
        # Group by date and symbol, check for significant price differences
        if df['data_source'].nunique() > 1:
            for symbol in df['symbol'].unique():
                symbol_data = df[df['symbol'] == symbol]
                for date in symbol_data['date'].unique():
                    date_data = symbol_data[symbol_data['date'] == date]
                    if len(date_data) > 1:  # We have data from multiple sources
                        close_prices = date_data['close'].values
                        max_diff_pct = (max(close_prices) - min(close_prices)) / min(close_prices) * 100
                        if max_diff_pct > 5:  # 5% threshold
                            validation_results['warnings'].append(
                                f"Price inconsistency for {symbol} on {date}: {max_diff_pct:.2f}% difference"
                            )

        return validation_results['passed'], validation_results

    except Exception as e:
        validation_results['passed'] = False
        validation_results['errors'].append(f"Validation error: {str(e)}")
        return False, validation_results


def validate_data_freshness(df: pd.DataFrame, max_age_days: int = 1) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate that the data is fresh (not too old).

    Args:
        df: DataFrame to validate
        max_age_days: Maximum allowed age of data in days

    Returns:
        Tuple of (is_valid, validation_results)
    """
    validation_results = {
        'passed': True,
        'errors': [],
        'warnings': [],
        'metrics': {}
    }

    try:
        # Check for date column
        if 'date' not in df.columns:
            validation_results['passed'] = False
            validation_results['errors'].append("Date column missing")
            return False, validation_results

        # Convert date to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            dates = pd.to_datetime(df['date'])
        else:
            dates = df['date']

        # Check data freshness
        today = pd.Timestamp.now().normalize()
        cutoff_date = today - pd.Timedelta(days=max_age_days)
        newest_date = dates.max()

        validation_results['metrics'] = {
            'newest_date': newest_date.strftime('%Y-%m-%d'),
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
            'days_behind': (today - newest_date).days
        }

        if newest_date < cutoff_date:
            validation_results['passed'] = False
            validation_results['errors'].append(
                f"Data is too old. Newest date is {newest_date.strftime('%Y-%m-%d')}, "
                f"should be at least {cutoff_date.strftime('%Y-%m-%d')}"
            )
            return False, validation_results

        return validation_results['passed'], validation_results

    except Exception as e:
        validation_results['passed'] = False
        validation_results['errors'].append(f"Data freshness validation error: {str(e)}")
        return False, validation_results


def validate_symbol_coverage(df: pd.DataFrame, required_symbols: List[str]) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate that all required symbols are present in the data.

    Args:
        df: DataFrame to validate
        required_symbols: List of symbols that must be present

    Returns:
        Tuple of (is_valid, validation_results)
    """
    validation_results = {
        'passed': True,
        'errors': [],
        'warnings': [],
        'metrics': {}
    }

    try:
        # Check for symbol column
        if 'symbol' not in df.columns:
            validation_results['passed'] = False
            validation_results['errors'].append("Symbol column missing")
            return False, validation_results

        # Get unique symbols in data
        actual_symbols = set(df['symbol'].unique())
        required_symbols_set = set(required_symbols)

        # Check coverage
        missing_symbols = required_symbols_set - actual_symbols

        validation_results['metrics'] = {
            'actual_symbol_count': len(actual_symbols),
            'required_symbol_count': len(required_symbols_set),
            'actual_symbols': sorted(list(actual_symbols)),
            'missing_symbols': sorted(list(missing_symbols))
        }

        if missing_symbols:
            validation_results['passed'] = False
            validation_results['errors'].append(
                f"Missing {len(missing_symbols)} required symbols: {', '.join(sorted(missing_symbols))}"
            )
            return False, validation_results

        # Check for unexpected symbols
        extra_symbols = actual_symbols - required_symbols_set
        if extra_symbols:
            validation_results['warnings'].append(
                f"Found {len(extra_symbols)} unexpected symbols: {', '.join(sorted(extra_symbols))}"
            )

        return validation_results['passed'], validation_results

    except Exception as e:
        validation_results['passed'] = False
        validation_results['errors'].append(f"Symbol coverage validation error: {str(e)}")
        return False, validation_results


def validate_bigquery_schema_compatibility(
        df: pd.DataFrame,
        bq_schema: List[Dict[str, Any]]
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate that the DataFrame is compatible with the BigQuery schema.

    Args:
        df: DataFrame to validate
        bq_schema: BigQuery schema definition

    Returns:
        Tuple of (is_valid, validation_results)
    """
    validation_results = {
        'passed': True,
        'errors': [],
        'warnings': [],
        'metrics': {}
    }

    try:
        # Create mapping of BigQuery schema fields
        bq_fields = {field['name']: field for field in bq_schema}
        bq_required_fields = [field['name'] for field in bq_schema if field.get('mode') == 'REQUIRED']

        # Check for required fields
        df_columns = set(df.columns)
        missing_required = [field for field in bq_required_fields if field not in df_columns]

        if missing_required:
            validation_results['passed'] = False
            validation_results['errors'].append(
                f"Missing required BigQuery fields: {', '.join(missing_required)}"
            )
            return False, validation_results

        # Check for data type compatibility
        type_issues = []

        for col in df.columns:
            if col in bq_fields:
                bq_type = bq_fields[col]['type']

                # Check numeric types
                if bq_type in ['FLOAT', 'FLOAT64'] and not pd.api.types.is_numeric_dtype(df[col]):
                    type_issues.append(f"{col} should be numeric for BigQuery type {bq_type}")

                # Check integer types
                elif bq_type in ['INTEGER', 'INT64'] and not (
                        pd.api.types.is_integer_dtype(df[col]) or
                        (pd.api.types.is_numeric_dtype(df[col]) and df[col].apply(float.is_integer).all())
                ):
                    type_issues.append(f"{col} should be integer for BigQuery type {bq_type}")

                # Check date types
                elif bq_type == 'DATE' and not (
                        pd.api.types.is_datetime64_any_dtype(df[col]) or
                        (pd.api.types.is_string_dtype(df[col]) and
                         pd.to_datetime(df[col], errors='coerce').notna().all())
                ):
                    type_issues.append(f"{col} should be a valid date for BigQuery type {bq_type}")

        if type_issues:
            validation_results['warnings'].extend(type_issues)

        # Check for extra columns
        extra_columns = df_columns - set(bq_fields.keys())
        if extra_columns:
            validation_results['warnings'].append(
                f"DataFrame contains columns not in BigQuery schema: {', '.join(extra_columns)}"
            )

        validation_results['metrics'] = {
            'df_column_count': len(df_columns),
            'bq_field_count': len(bq_fields),
            'required_field_count': len(bq_required_fields),
            'matching_field_count': len(df_columns.intersection(set(bq_fields.keys())))
        }

        return validation_results['passed'], validation_results

    except Exception as e:
        validation_results['passed'] = False
        validation_results['errors'].append(f"BigQuery schema compatibility validation error: {str(e)}")
        return False, validation_results
