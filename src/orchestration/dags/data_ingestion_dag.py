"""
data_ingestion_dag.py - DAG for ingesting raw data from multiple sources

This DAG handles the ingestion of data from three main sources:
1. Web logs (JSON format, ingested via Kafka)
2. CRM data (customer and order CSVs)
3. Advertising data (Google Ads, Social Media, Influencers)

The DAG stores all data in the Bronze zone of the data lake.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import os
import glob
import json
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='Ingest data from web logs, CRM, and advertising sources',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'bronze'],
)

# Define file paths
DATA_PATH = '/data'
RAW_DATA_PATH = f'{DATA_PATH}/raw'
WEB_DATA_PATH = f'{RAW_DATA_PATH}/web'
CRM_DATA_PATH = "/data/raw/crm"
AD_DATA_PATH = f'{RAW_DATA_PATH}/advertising'

# Task functions
def check_web_logs(**kwargs):
    """Check for new web log files and return file paths for processing"""
    
    # Look for web log files for the execution date
    log_files = glob.glob(f"{WEB_DATA_PATH}/web_logs_*.json")
    
    if not log_files:
        logging.info(f"No web logs found")
        return []
    
    logging.info(f"Found {len(log_files)} web log files")
    return log_files

def check_crm_data(**kwargs):
    """Check for new CRM data files and return file paths for processing"""
    logging.info(f"Checking for CRM data in {CRM_DATA_PATH}")

    # List all files in the directory to debug
    all_files = glob.glob(f"{CRM_DATA_PATH}/*")
    logging.info(f"All files in directory: {all_files}")

    # Look for customer and order files for the execution date
    customer_files = glob.glob(f"{CRM_DATA_PATH}/customers_*.csv")
    order_files = glob.glob(f"{CRM_DATA_PATH}/orders_*.csv")

    logging.info(f"Found {len(customer_files)} customer files and {len(order_files)} order files")
    
    all_files = customer_files + order_files
    
    if not all_files:
        logging.info(f"No CRM data found ")
        return []
    
    logging.info(f"Found {len(all_files)} CRM files ")
    return all_files

def check_advertising_data(**kwargs):
    """Check for new advertising data files and return file paths for processing"""
    execution_date = kwargs['execution_date']
    
    # Look for advertising files for the execution date
    google_files = glob.glob(f"{AD_DATA_PATH}/google_ads_*.csv")
    social_files = glob.glob(f"{AD_DATA_PATH}/social_ads_*.csv")
    influencer_files = glob.glob(f"{AD_DATA_PATH}/influencer_data_*.csv")
    all_platform_files = glob.glob(f"{AD_DATA_PATH}/all_platforms_*.csv")
    
    all_files = google_files + social_files + influencer_files + all_platform_files
    
    if not all_files:
        logging.info(f"No advertising data found ")
        return []
    
    logging.info(f"Found {len(all_files)} advertising files")
    return all_files

# Define tasks
check_web_logs_task = PythonOperator(
    task_id='check_web_logs',
    python_callable=check_web_logs,
    provide_context=True,
    dag=dag,
)

check_crm_data_task = PythonOperator(
    task_id='check_crm_data',
    python_callable=check_crm_data,
    provide_context=True,
    dag=dag,
)

check_advertising_data_task = PythonOperator(
    task_id='check_advertising_data',
    python_callable=check_advertising_data,
    provide_context=True,
    dag=dag,
)

# Spark jobs for data ingestion
ingest_web_logs = SparkSubmitOperator(
    task_id='ingest_web_logs_to_kafka',
    application='/src/ingestion/streaming/ingest_web_logs.py',
    name='web_logs_to_kafka',
    conn_id='spark_default',
    application_args=["{{ ','.join(ti.xcom_pull(task_ids='check_web_logs')) }}"],
    verbose=True,
    conf={
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

ingest_web_logs_to_bronze = SparkSubmitOperator(
    task_id='ingest_web_logs_to_bronze',
    application='/src/ingestion/streaming/web_logs_to_bronze.py',
    name='web_logs_to_bronze',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.master': 'spark://spark-master:7077', 
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

ingest_crm_to_bronze = SparkSubmitOperator(
    task_id='ingest_crm_to_bronze',
    application='/src/ingestion/batch/crm_to_bronze.py',
    name='crm_to_bronze',
    conn_id='spark_default',
    verbose=True,
    application_args=["{{ ','.join(ti.xcom_pull(task_ids='check_crm_data')) }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',  
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.1',
    },
    dag=dag,
    
)

ingest_advertising_to_bronze = SparkSubmitOperator(
    task_id='ingest_advertising_to_bronze',
    application='/src/ingestion/batch/advertising_to_bronze.py',
    name='advertising_to_bronze',
    conn_id='spark_default',
    verbose=True,
    application_args=["{{ ','.join(ti.xcom_pull(task_ids='check_advertising_data')) }}"],
    conf={
        'spark.master': 'spark://spark-master:7077', 
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
    
)

# Set task dependencies
check_web_logs_task >> ingest_web_logs >> ingest_web_logs_to_bronze
check_crm_data_task >> ingest_crm_to_bronze
check_advertising_data_task >> ingest_advertising_to_bronze