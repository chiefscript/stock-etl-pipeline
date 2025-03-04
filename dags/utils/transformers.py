import os
import logging
import pandas as pd
from datetime import datetime
from typing import List

logger = logging.getLogger(__name__)


def transform_stock_data(input_path: str, output_path: str, source: str) -> str:
    """
    Transform raw stock data into a standardized format.

    Args:
        input_path: Path to the input CSV file
        output_path: Path to save the transformed data
        source: Source of the data ('alpha_vantage' or 'yahoo_finance')

    Returns:
        Path to the saved transformed data file
    """
    logger.info(f"Transforming {source} data from {input_path}")

    # Read the raw data
    data = pd.read_csv(input_path)

    # Handle source-specific transformations
    if source == 'alpha_vantage':
        # Alpha Vantage specific transformations
        # Convert date format if needed
        if 'date' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['date']):
            data['date'] = pd.to_datetime(data['date'])

    elif source == 'yahoo_finance':
        # Yahoo Finance specific transformations
        # Convert date format if needed
        if 'date' in data.columns and not pd.api.types.is_datetime64_any_dtype(data['date']):
            data['date'] = pd.to_datetime(data['date'])

    else:
        raise ValueError(f"Unknown source: {source}")

    # Common transformations

    # Format date as YYYY-MM-DD
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')

    # Ensure all numeric columns are float
    for col in ['open', 'high', 'low', 'close']:
        if col in data.columns:
            data[col] = data[col].astype(float)

    # Ensure volume is integer
    if 'volume' in data.columns:
        data['volume'] = data['volume'].fillna(0).astype(int)

    # Add processed timestamp
    data['processed_at'] = datetime.now().isoformat()

    # Calculate additional metrics
    # Daily change percentage
    data['daily_change_pct'] = ((data['close'] - data['open']) / data['open'] * 100).round(2)

    # Daily volatility (high-low range as percentage of open price)
    data['daily_volatility'] = ((data['high'] - data['low']) / data['open'] * 100).round(2)

    # Sort by date and symbol
    data = data.sort_values(['symbol', 'date'])

    # Save transformed data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data.to_csv(output_path, index=False)

    logger.info(f"Transformed {len(data)} records to {output_path}")

    return output_path


def merge_stock_datasets(input_paths: List[str], output_path: str) -> str:
    """
    Merge multiple transformed stock datasets into a single dataset.

    Args:
        input_paths: List of paths to the input CSV files
        output_path: Path to save the merged data

    Returns:
        Path to the saved merged data file
    """
    logger.info(f"Merging datasets: {input_paths}")

    all_data = []

    for path in input_paths:
        try:
            data = pd.read_csv(path)
            all_data.append(data)
        except Exception as e:
            logger.error(f"Error reading file {path}: {e}")
            continue

    if not all_data:
        raise ValueError("No valid data files to merge")

    # Concatenate all datasets
    merged_data = pd.concat(all_data, ignore_index=True)

    # Remove duplicates based on date, symbol, and data_source
    merged_data = merged_data.drop_duplicates(subset=['date', 'symbol', 'data_source'])

    # If we have the same date and symbol from different sources, we can either:
    # 1. Keep both records (what we're doing now)
    # 2. Combine them (e.g., average the values)
    # 3. Choose one source as the "source of truth"

    # Sort by date and symbol
    merged_data = merged_data.sort_values(['date', 'symbol', 'data_source'])

    # Save merged data
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged_data.to_csv(output_path, index=False)

    logger.info(f"Merged {len(merged_data)} records to {output_path}")

    return output_path


def calculate_moving_averages(data: pd.DataFrame, windows: List[int] = [5, 10, 20, 50]) -> pd.DataFrame:
    """
    Calculate moving averages for each stock symbol.

    Args:
        data: DataFrame containing stock data
        windows: List of window sizes for moving averages

    Returns:
        DataFrame with added moving average columns
    """
    result = data.copy()

    # For each symbol, calculate moving averages
    for symbol in result['symbol'].unique():
        symbol_data = result[result['symbol'] == symbol].copy()

        for window in windows:
            ma_col = f'ma_{window}'
            symbol_data[ma_col] = symbol_data['close'].rolling(window=window).mean().round(2)

        result.loc[result['symbol'] == symbol] = symbol_data

    return result
