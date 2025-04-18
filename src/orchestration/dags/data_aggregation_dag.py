"""
data_aggregation_dag.py - DAG for creating business-ready aggregated data

This DAG handles the transformation of data from Silver to Gold layer:
1. Create marketing performance aggregations
2. Create product performance aggregations
3. Create customer segmentation
4. Create time-based analytics

The DAG stores aggregated data in the Gold zone of the data lake.
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
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_aggregation_dag',
    default_args=default_args,
    description='Aggregate data from Silver to Gold',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['aggregation', 'gold'],
)

# Wait for the transformation DAG to complete
wait_for_transformation = ExternalTaskSensor(
    task_id='wait_for_transformation',
    external_dag_id='data_transformation_dag',
    external_task_id=None,  # Wait for the entire DAG to complete
    mode='reschedule',
    timeout=3600,  # Timeout after 1 hour
    poke_interval=60,  # Check every minute
    dag=dag,
)

# Spark jobs for aggregation

# Marketing performance metrics by channel and campaign
create_marketing_performance = SparkSubmitOperator(
    task_id='create_marketing_performance',
    application='/src/aggregation/marketing_performance.py',
    name='marketing_performance',
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

# Product performance metrics
create_product_performance = SparkSubmitOperator(
    task_id='create_product_performance',
    application='/src/aggregation/product_performance.py',
    name='product_performance',
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

# Customer segmentation
create_customer_segments = SparkSubmitOperator(
    task_id='create_customer_segments',
    application='/src/aggregation/customer_segments.py',
    name='customer_segments',
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

# Customer acquisition metrics
create_acquisition_metrics = SparkSubmitOperator(
    task_id='create_acquisition_metrics',
    application='/src/aggregation/acquisition_metrics.py',
    name='acquisition_metrics',
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

# Campaign ROI analysis
create_campaign_roi = SparkSubmitOperator(
    task_id='create_campaign_roi',
    application='/src/aggregation/campaign_roi.py',
    name='campaign_roi',
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

# Sales forecast and trends
create_sales_forecast = SparkSubmitOperator(
    task_id='create_sales_forecast',
    application='/src/aggregation/sales_forecast.py',
    name='sales_forecast',
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

# Quality checks on Gold data
run_gold_quality_checks = SparkSubmitOperator(
    task_id='run_gold_quality_checks',
    application='/src/quality/check_gold_data_quality.py',
    name='gold_data_quality',
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

# Define dependencies
wait_for_transformation >> [
    create_marketing_performance, 
    create_product_performance, 
    create_customer_segments,
    create_acquisition_metrics
]

# ROI analysis depends on marketing performance and acquisition metrics
[create_marketing_performance, create_acquisition_metrics] >> create_campaign_roi

# Sales forecast depends on product performance
create_product_performance >> create_sales_forecast

# Quality checks happen after all aggregations
[
    create_marketing_performance,
    create_product_performance,
    create_customer_segments,
    create_acquisition_metrics,
    create_campaign_roi,
    create_sales_forecast
] >> run_gold_quality_checks