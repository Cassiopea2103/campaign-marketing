"""
data_transformation_dag.py - DAG for transforming raw data into cleaned and enriched format

This DAG handles the transformation of data from the Bronze layer to the Silver layer:
1. Clean and standardize web log data
2. Clean and enrich CRM data
3. Clean and normalize advertising data
4. Join datasets where appropriate
5. Apply business rules and calculate basic metrics

The DAG stores transformed data in the Silver zone of the data lake.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    'data_transformation_dag',
    default_args=default_args,
    description='Transform data from Bronze to Silver',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['transformation', 'silver'],
)

# Wait for the ingestion DAG to complete
wait_for_ingestion = ExternalTaskSensor(
    task_id='wait_for_ingestion',
    external_dag_id='data_ingestion_dag',
    external_task_id=None,  # Wait for the entire DAG to complete
    mode='reschedule',  # Reschedule the task until the condition is met
    timeout=3600,  # Timeout after 1 hour
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Spark jobs for data transformation
clean_web_logs = SparkSubmitOperator(
    task_id='clean_web_logs',
    application='/src/etl/transformation/clean_web_logs.py',
    name='clean_web_logs',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

clean_crm_data = SparkSubmitOperator(
    task_id='clean_crm_data',
    application='/src/etl/transformation/clean_crm_data.py',
    name='clean_crm_data',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

clean_advertising_data = SparkSubmitOperator(
    task_id='clean_advertising_data',
    application='/src/etl/transformation/clean_advertising_data.py',
    name='clean_advertising_data',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Create enriched customer profiles including order history
create_customer_profiles = SparkSubmitOperator(
    task_id='create_customer_profiles',
    application='/src/etl/transformation/create_customer_profiles.py',
    name='create_customer_profiles',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Join web events with customer data for identified sessions
join_web_customer_data = SparkSubmitOperator(
    task_id='join_web_customer_data',
    application='/src/etl/transformation/join_web_customer_data.py',
    name='join_web_customer_data',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Calculate attribution models
create_attribution_models = SparkSubmitOperator(
    task_id='create_attribution_models',
    application='/src/etl/transformation/create_attribution_models.py',
    name='create_attribution_models',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Quality check on Silver data
run_silver_quality_checks = SparkSubmitOperator(
    task_id='run_silver_quality_checks',
    application='/src/etl/quality/check_silver_data_quality.py',
    name='silver_data_quality',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Wait for ingestion to complete before starting transformation
wait_for_ingestion >> [clean_web_logs, clean_crm_data, clean_advertising_data]

# Create customer profiles after cleaning CRM data
clean_crm_data >> create_customer_profiles

# Join web and customer data after both datasets are cleaned
[clean_web_logs, create_customer_profiles] >> join_web_customer_data

# Create attribution models after ad data is cleaned and web/customer data is joined
[clean_advertising_data, join_web_customer_data] >> create_attribution_models

# Run quality checks after all transformations are complete
[create_customer_profiles, join_web_customer_data, create_attribution_models] >> run_silver_quality_checks