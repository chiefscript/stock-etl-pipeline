# Stock ETL Pipeline

An end-to-end ETL (Extract, Transform, Load) pipeline for stock data using Apache Airflow, Python and Google BigQuery.

This project demonstrates a complete ETL pipeline that:

1. **Extracts** data from multiple public stock APIs (Alpha Vantage and Yahoo Finance)
2. **Transforms** the data using pandas
3. **Loads** processed data into Google BigQuery
4. **Orchestrates** the entire workflow using Apache Airflow
5. **Validates** data quality at each step of the pipeline

## Features

- **Multi-source Data Extraction**: Extract stock data from Alpha Vantage and Yahoo Finance APIs
- **Data Transformation**: Clean, validate, and standardize data from multiple sources
- **Data Loading**: Load processed data to Google BigQuery
- **Workflow Orchestration**: Schedule and monitor jobs with Apache Airflow
- **Data Quality Checks**: Validate data at each step of the pipeline
- **Error Handling**: Comprehensive error handling and retry mechanisms
- **Logging and Monitoring**: Track pipeline execution and data metrics
- **Containerization**: Docker-based deployment for easy setup and portability


## Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account with BigQuery enabled
- Alpha Vantage API key

## Setup and Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/stock-etl-pipeline.git
cd stock-etl-pipeline
```

**Create, start & install requirements to virtual environment**
```bash
virtualenv venv -p python3
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Set up Google Cloud credentials

- Create a GCP service account with BigQuery and GCS permissions
- Download the service account key JSON file
- Save it to `config/google-credentials.json`

### 3. Configure Airflow variables

Create a file `config/variables.json` with the following content:

```json
{
  "stock_etl_config": {
    "stock_symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "META"],
    "gcs_bucket": "your-gcs-bucket-name",
    "bigquery_dataset": "your_dataset",
    "bigquery_table": "stock_data"
  },
  "alpha_vantage_api_key": "YOUR_ALPHA_VANTAGE_API_KEY"
}
```

### 4. Start the Airflow services

```bash
docker-compose up -d
```

### 5. Access the Airflow UI

Open your browser and navigate to `http://localhost:8080`

Default credentials:
- Username: `airflow`
- Password: `airflow`

### 6. Configure connections and import variables

In the Airflow UI:

1. Go to Admin > Connections
2. Add a new Google Cloud connection named `google_cloud_default`
3. Go to Admin > Variables
4. Import your variables from `config/variables.json`

### 7. Enable and trigger the DAG

In the Airflow UI, enable the `stock_data_etl_pipeline` DAG and trigger it manually for the first run.

## Pipeline Workflow

1. **Extract data from Alpha Vantage API**
   - Fetch daily stock price data for configured symbols
   - Save raw data to local CSV file

2. **Extract data from Yahoo Finance API**
   - Fetch historical stock price data for configured symbols
   - Save raw data to local CSV file

3. **Validate raw data**
   - Check data quality and completeness
   - Ensure required fields are present
   - Verify data types and ranges

4. **Transform data**
   - Clean and standardize data from both sources
   - Calculate additional metrics (daily change, volatility)
   - Ensure consistent date formats

5. **Merge datasets**
   - Combine data from multiple sources
   - Handle duplicates and conflicts

6. **Validate transformed data**
   - Ensure transformation succeeded
   - Check for data completeness and accuracy

7. **Upload to Google Cloud Storage**
   - Store processed data in GCS bucket
   - Use date-based partitioning

8. **Load to BigQuery**
   - Append data to BigQuery table
   - Define schema with appropriate types

## Data Quality Checks

This pipeline implements several levels of data quality validation:

- **Schema validation**: Ensure data adheres to expected schema
- **Completeness checks**: Verify required fields are present
- **Range checks**: Ensure numeric values are within expected ranges
- **Cross-source validation**: Compare data between sources for consistency
- **Temporal checks**: Verify date sequences and ranges

## Customization

### Changing the Schedule

Modify the `schedule_interval` parameter in the DAG definition to change when the pipeline runs.

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run with coverage report
pytest --cov=. tests/
```

## Monitoring and Maintenance

- Monitor job success/failure in the Airflow UI
- Check logs in the Airflow UI or in `logs/` directory
- Set up email notifications for job failures
- Review data quality metrics in BigQuery

## Acknowledgements

- [Apache Airflow](https://airflow.apache.org/)
- [Alpha Vantage API](https://www.alphavantage.co/)
- [Yahoo Finance API](https://pypi.org/project/yfinance/)
- [Google BigQuery](https://cloud.google.com/bigquery)
