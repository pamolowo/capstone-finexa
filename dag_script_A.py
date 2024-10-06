from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from Alpha_extraction import extract_and_upload
from Alpha_loading import load_data_to_postgres
from datetime import datetime, timedelta
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 25),
    'email': ['pamela.olowojebutu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'finexa_capital_pipeline_A',
    default_args=default_args,
    description='ETL for Finexa Capital Alpha_Vantage',
    schedule_interval=timedelta(days=1),
)

# Extraction Layer
extraction = PythonOperator(
    task_id='extraction_layer',
    python_callable=extract_and_upload,
    op_kwargs={
        'api_key': os.getenv('ALPHA_VANTAGE_API_KEY'),
        'symbols': ['AAPL', 'MSFT', 'TSLA', 'IBM', 'AMZN'],
        'blob_name': 'rawdata/raw_data.csv',
    },
    dag=dag,
)

# Databricks Transformation Layer with Job ID
run_databricks_job = DatabricksRunNowOperator(
    task_id='databricks_transformation',
    databricks_conn_id='databricks_default',
    job_id=978581139423905,  # Use Databricks Job ID
    notebook_params={
        'user': 'Airflow_user',
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
    do_xcom_push=True  # Ensure XCom push is enabled to return values
)

# Load to PostgreSQL
load_to_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,  # Directly call the loading function
    dag=dag,
)

# Task dependencies
extraction >> run_databricks_job >> load_to_postgres

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("DAG finexa_capital_pipeline_A is set up.")
