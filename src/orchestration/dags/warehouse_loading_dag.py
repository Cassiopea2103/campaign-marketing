"""
warehouse_loading_dag.py - DAG for loading data to the data warehouse

This DAG handles:
1. Loading data from the Gold zone into the data warehouse
2. Running dbt models to transform data for analytics
3. Creating final reporting tables and views

The DAG supports the analytics team by making data available for dashboards and BI.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.operators.bash import BashOperator
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
    'warehouse_loading_dag',
    default_args=default_args,
    description='Load data from Gold to Data Warehouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['warehouse', 'dbt'],
)

# Wait for the aggregation DAG to complete
wait_for_aggregation = ExternalTaskSensor(
    task_id='wait_for_aggregation',
    external_dag_id='data_aggregation_dag',
    external_task_id=None,  # Wait for the entire DAG to complete
    mode='reschedule',
    timeout=3600,  # Timeout after 1 hour
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Load gold data to Snowflake
load_marketing_performance = SparkSubmitOperator(
    task_id='load_marketing_performance',
    application='/src/warehouse/load_marketing_performance.py',
    name='load_marketing_performance',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    jars='/opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/jars/wildfly-openssl-1.0.7.Final.jar,/opt/jars/snowflake-jdbc-3.13.14.jar,/opt/jars/spark-snowflake_2.12-2.9.3-spark_3.0.jar',
    dag=dag,
)

load_product_performance = SparkSubmitOperator(
    task_id='load_product_performance',
    application='/src/warehouse/load_product_performance.py',
    name='load_product_performance',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    jars='/opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/jars/wildfly-openssl-1.0.7.Final.jar',
    dag=dag,
)

load_customer_segments = SparkSubmitOperator(
    task_id='load_customer_segments',
    application='/src/warehouse/load_customer_segments.py',
    name='load_customer_segments',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    jars='/opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/jars/wildfly-openssl-1.0.7.Final.jar',
    dag=dag,
)

load_campaign_roi = SparkSubmitOperator(
    task_id='load_campaign_roi',
    application='/src/warehouse/load_campaign_roi.py',
    name='load_campaign_roi',
    conn_id='spark_default',
    application_args=["{{ ds }}"],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    jars='/opt/jars/hadoop-aws-3.3.1.jar,/opt/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/jars/wildfly-openssl-1.0.7.Final.jar',
    dag=dag,
)

# Run dbt models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /dbt && dbt run --profiles-dir /dbt/profiles --target prod',
    dag=dag,
)

# Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /dbt && dbt test --profiles-dir /dbt/profiles --target prod',
    dag=dag,
)

# Refresh Metabase dashboards via API
refresh_dashboards = PythonOperator(
    task_id='refresh_dashboards',
    python_callable=lambda: logging.info("Refreshing Metabase dashboards via API"),
    dag=dag,
)

# Set task dependencies
wait_for_aggregation >> [
    load_marketing_performance,
    load_product_performance,
    load_customer_segments,
    load_campaign_roi
]

# Run dbt after data is loaded to warehouse
[
    load_marketing_performance,
    load_product_performance,
    load_customer_segments,
    load_campaign_roi
] >> dbt_run

# Test after dbt models are run
dbt_run >> dbt_test

# Refresh dashboards after tests pass
dbt_test >> refresh_dashboards