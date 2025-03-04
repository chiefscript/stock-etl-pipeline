FROM apache/airflow:2.7.1-python3.9

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Set environment variables
ENV PYTHONPATH=/opt/airflow

# Create directories
USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/logs /opt/airflow/config
RUN chown -R airflow:root /opt/airflow/dags /opt/airflow/plugins /opt/airflow/logs /opt/airflow/config

USER airflow
