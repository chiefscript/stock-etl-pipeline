# Create BigQuery tables
CREATE_STOCKS_TABLE = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table}` (
  date DATE NOT NULL,
  symbol STRING NOT NULL,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64 NOT NULL,
  volume INT64,
  data_source STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  daily_change_pct FLOAT64,
  daily_volatility FLOAT64
)
PARTITION BY date
CLUSTER BY symbol
;
"""

# Create aggregated view for daily analytics
CREATE_DAILY_METRICS_VIEW = """
CREATE OR REPLACE VIEW `{project}.{dataset}.stock_daily_metrics` AS
SELECT
  date,
  symbol,
  ANY_VALUE(close) AS close_price,
  AVG(daily_volatility) AS avg_volatility,
  COUNT(DISTINCT data_source) AS source_count
FROM `{project}.{dataset}.{table}`
GROUP BY date, symbol
ORDER BY date DESC, symbol
;
"""

# Query to get inconsistencies between data sources
INCONSISTENCY_CHECK_QUERY = """
WITH source_prices AS (
  SELECT
    date,
    symbol,
    data_source,
    close,
    processed_at
  FROM `{project}.{dataset}.{table}`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

symbol_date_stats AS (
  SELECT
    date,
    symbol,
    MIN(close) AS min_close,
    MAX(close) AS max_close,
    AVG(close) AS avg_close,
    COUNT(DISTINCT data_source) AS source_count,
    ARRAY_AGG(STRUCT(data_source, close, processed_at)) AS source_details
  FROM source_prices
  GROUP BY date, symbol
  HAVING COUNT(DISTINCT data_source) > 1
)

SELECT
  date,
  symbol,
  (max_close - min_close) / min_close * 100 AS price_diff_pct,
  source_count,
  source_details
FROM symbol_date_stats
WHERE (max_close - min_close) / min_close > 0.02  -- More than 2% difference
ORDER BY price_diff_pct DESC
;
"""

# Query to get rolling averages for stock prices
ROLLING_AVERAGES_QUERY = """
WITH daily_prices AS (
  SELECT
    date,
    symbol,
    ANY_VALUE(close) AS close_price
  FROM `{project}.{dataset}.{table}`
  WHERE symbol IN UNNEST({symbols})
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY date, symbol
)

SELECT
  date,
  symbol,
  close_price,
  AVG(close_price) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
  ) AS ma_5d,
  AVG(close_price) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS ma_10d,
  AVG(close_price) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS ma_20d,
  AVG(close_price) OVER (
    PARTITION BY symbol
    ORDER BY date
    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
  ) AS ma_50d
FROM daily_prices
ORDER BY symbol, date
;
"""

# Query to get data quality metrics
DATA_QUALITY_METRICS_QUERY = """
SELECT
  -- Record counts
  COUNT(*) AS total_records,
  COUNT(DISTINCT date) AS unique_dates,
  COUNT(DISTINCT symbol) AS unique_symbols,
  COUNT(DISTINCT data_source) AS unique_sources,

  -- Date range
  MIN(date) AS oldest_date,
  MAX(date) AS newest_date,

  -- Missing values
  COUNTIF(open IS NULL) AS null_open_count,
  COUNTIF(high IS NULL) AS null_high_count,
  COUNTIF(low IS NULL) AS null_low_count,
  COUNTIF(volume IS NULL) AS null_volume_count,

  -- Price statistics
  MIN(close) AS min_close_price,
  MAX(close) AS max_close_price,
  AVG(close) AS avg_close_price,

  -- Source distribution
  ARRAY_AGG(STRUCT(data_source, COUNT(*) AS count) ORDER BY data_source) AS source_distribution
FROM `{project}.{dataset}.{table}`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY ROLLUP(())
;
"""

# Query to get daily ingestion statistics
INGESTION_STATS_QUERY = """
SELECT
  DATE(processed_at) AS ingestion_date,
  COUNT(*) AS records_processed,
  COUNT(DISTINCT symbol) AS symbols_processed,
  COUNT(DISTINCT data_source) AS sources_processed,
  MIN(date) AS oldest_data_date,
  MAX(date) AS newest_data_date
FROM `{project}.{dataset}.{table}`
WHERE DATE(processed_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
GROUP BY ingestion_date
ORDER BY ingestion_date DESC
;
"""