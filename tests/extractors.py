import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from dags.utils.extractors import extract_alpha_vantage_data, extract_yahoo_finance_data


class TestExtractors(unittest.TestCase):
    """Test cases for data extraction utilities."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temp directory for output files
        self.temp_dir = "/tmp/extractor_tests/"
        os.makedirs(self.temp_dir, exist_ok=True)

        # Sample stock symbols
        self.symbols = ['AAPL', 'MSFT']

        # Sample Alpha Vantage data
        self.alpha_vantage_data = pd.DataFrame({
            'date': ['2023-09-01', '2023-09-02'],
            '1. open': [180.31, 181.22],
            '2. high': [182.05, 183.55],
            '3. low': [179.22, 180.13],
            '4. close': [181.15, 182.92],
            '5. volume': [52123400, 48726500]
        })
        self.alpha_vantage_data.set_index('date', inplace=True)

        # Sample Yahoo Finance data
        self.yahoo_finance_data = pd.DataFrame({
            'Date': [pd.Timestamp('2023-09-01'), pd.Timestamp('2023-09-02')],
            'Open': [180.31, 181.22],
            'High': [182.05, 183.55],
            'Low': [179.22, 180.13],
            'Close': [181.15, 182.92],
            'Volume': [52123400, 48726500],
            'Dividends': [0, 0],
            'Stock Splits': [0, 0]
        })

    def tearDown(self):
        """Clean up after tests."""
        # Remove any test files
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)

    @patch('dags.utils.extractors.TimeSeries')
    def test_extract_alpha_vantage_data(self, mock_time_series):
        """Test extraction from Alpha Vantage API."""
        # Mock the TimeSeries class
        mock_ts_instance = MagicMock()
        mock_time_series.return_value = mock_ts_instance

        # Configure the mock to return our sample data
        mock_ts_instance.get_daily.return_value = (self.alpha_vantage_data, {
            "1. Information": "Daily Prices",
            "2. Symbol": "AAPL",
            "3. Last Refreshed": "2023-09-02",
            "4. Output Size": "Compact",
            "5. Time Zone": "US/Eastern"
        })

        # Test the function
        output_path = os.path.join(self.temp_dir, "alpha_vantage_test.csv")
        result = extract_alpha_vantage_data(
            symbols=self.symbols,
            output_path=output_path,
            api_key="dummy_key"
        )

        # Assertions
        self.assertEqual(result, output_path)
        self.assertTrue(os.path.exists(output_path))

        # Check that the function was called with correct parameters
        mock_ts_instance.get_daily.assert_called()

        # Read the output file and check its contents
        output_data = pd.read_csv(output_path)
        self.assertIn('symbol', output_data.columns)
        self.assertIn('data_source', output_data.columns)
        self.assertEqual(output_data['data_source'].unique()[0], 'alpha_vantage')

    @patch('dags.utils.extractors.yf.Ticker')
    def test_extract_yahoo_finance_data(self, mock_ticker):
        """Test extraction from Yahoo Finance API."""
        # Mock the Ticker class
        mock_ticker_instance = MagicMock()
        mock_ticker.return_value = mock_ticker_instance

        # Configure the mock to return our sample data
        mock_ticker_instance.history.return_value = self.yahoo_finance_data

        # Test the function
        output_path = os.path.join(self.temp_dir, "yahoo_finance_test.csv")
        result = extract_yahoo_finance_data(
            symbols=self.symbols,
            output_path=output_path,
            period="1mo"
        )

        # Assertions
        self.assertEqual(result, output_path)
        self.assertTrue(os.path.exists(output_path))

        # Check that the function was called with correct parameters
        mock_ticker_instance.history.assert_called_with(period="1mo")

        # Read the output file and check its contents
        output_data = pd.read_csv(output_path)
        self.assertIn('symbol', output_data.columns)
        self.assertIn('data_source', output_data.columns)
        self.assertEqual(output_data['data_source'].unique()[0], 'yahoo_finance')


if __name__ == '__main__':
    unittest.main()
