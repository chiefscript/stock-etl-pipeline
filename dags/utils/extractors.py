import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List

import yfinance as yf
from alpha_vantage.timeseries import TimeSeries

logger = logging.getLogger(__name__)


def extract_alpha_vantage_data(symbols: List[str], output_path: str, api_key: str) -> str:
    """
    Extract daily stock data from Alpha Vantage API.

    Args:
        symbols: List of stock symbols to extract data for
        output_path: Path to save the extracted data
        api_key: Alpha Vantage API key

    Returns:
        Path to the saved data file
    """
    logger.info(f"Extracting Alpha Vantage data for symbols: {symbols}")

    # Initialize TimeSeries client
    ts = TimeSeries(key=api_key, output_format='pandas')

    # Get data for each symbol and combine into a single DataFrame
    all_data = []

    for symbol in symbols:
        try:
            # Get daily time series data
            data, meta_data = ts.get_daily(symbol=symbol, outputsize='compact')

            # Reset index to make date a column and add symbol
            data = data.reset_index()
            data['symbol'] = symbol

            # Rename columns to standardized format
            data = data.rename(columns={
                'date': 'date',
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })

            all_data.append(data)

            # Respect API rate limits
            import time
            time.sleep(12)  # Alpha Vantage free tier allows 5 calls per minute

        except Exception as e:
            logger.error(f"Error extracting data for {symbol}: {e}")
            continue

    if not all_data:
        raise ValueError("Failed to extract any data from Alpha Vantage")

    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)

    # Add source and timestamp
    combined_data['data_source'] = 'alpha_vantage'
    combined_data['extracted_at'] = datetime.now().isoformat()

    # Save to CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_data.to_csv(output_path, index=False)

    logger.info(f"Extracted {len(combined_data)} records to {output_path}")

    return output_path


def extract_yahoo_finance_data(symbols: List[str], output_path: str, period: str = '1mo') -> str:
    """
    Extract stock data from Yahoo Finance API.

    Args:
        symbols: List of stock symbols to extract data for
        output_path: Path to save the extracted data
        period: Time period to extract (e.g., '1d', '1mo', '1y')

    Returns:
        Path to the saved data file
    """
    logger.info(f"Extracting Yahoo Finance data for symbols: {symbols}")

    # Get data for each symbol and combine into a single DataFrame
    all_data = []

    for symbol in symbols:
        try:
            # Get data from Yahoo Finance
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period)

            # Reset index to make date a column and add symbol
            data = data.reset_index()
            data['symbol'] = symbol

            # Rename columns to standardized format (Yahoo Finance columns are already capitalized)
            data = data.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })

            all_data.append(data)

        except Exception as e:
            logger.error(f"Error extracting data for {symbol}: {e}")
            continue

    if not all_data:
        raise ValueError("Failed to extract any data from Yahoo Finance")

    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)

    # Add source and timestamp
    combined_data['data_source'] = 'yahoo_finance'
    combined_data['extracted_at'] = datetime.now().isoformat()

    # Drop unnecessary columns
    columns_to_keep = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'data_source', 'extracted_at']
    combined_data = combined_data[columns_to_keep]

    # Save to CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_data.to_csv(output_path, index=False)

    logger.info(f"Extracted {len(combined_data)} records to {output_path}")

    return output_path
