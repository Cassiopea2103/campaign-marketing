"""
monitoring_dag.py - DAG for monitoring pipeline health and data quality

This DAG handles:
1. Running comprehensive data quality checks
2. Monitoring pipeline performance metrics
3. Validating data freshness
4. Alerting on anomalies or failures
5. Generating data quality reports

The DAG ensures data quality and system reliability.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging
import os
import json

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,  # Enable email on failure for monitoring
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'monitoring_dag',
    default_args=default_args,
    description='Monitor data pipeline health and quality',
    schedule_interval=timedelta(hours=1),  # Run more frequently than data pipelines
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['monitoring', 'quality'],
)

# Wait for specific time to ensure all other DAGs have had time to run
wait_for_other_dags = TimeDeltaSensor(
    task_id='wait_for_time_sensor',
    delta=timedelta(minutes=15),  # Run 15 minutes after the hour
    dag=dag,
)

# Check disk usage on data lake
check_disk_usage = BashOperator(
    task_id='check_disk_usage',
    bash_command='df -h /data | tail -n 1 | awk \'{print "Disk usage: " $5}\' > /data/monitoring/disk_usage.txt',
    dag=dag,
)

# Monitor Kafka lag
monitor_kafka_lag = BashOperator(
    task_id='monitor_kafka_lag',
    bash_command='echo "Checking Kafka consumer lag" && '
                'docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 '
                '--describe --group web_logs_consumer | '
                'tee /data/monitoring/kafka_lag.txt',
    dag=dag,
)

# Check data volume and anomalies in recent data
check_data_volumes = SparkSubmitOperator(
    task_id='check_data_volumes',
    application='/src/etl/monitoring/check_data_volumes.py',
    name='check_data_volumes',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Generate data quality report
generate_quality_report = SparkSubmitOperator(
    task_id='generate_quality_report',
    application='/src/etl/monitoring/generate_quality_report.py',
    name='generate_quality_report',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)



# Check overall data freshness
check_data_freshness = SparkSubmitOperator(
    task_id='check_data_freshness',
    application='/src/etl/monitoring/check_data_freshness.py',
    name='check_data_freshness',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Validate data completeness and quality
validate_data_quality = SparkSubmitOperator(
    task_id='validate_data_quality',
    application='/src/etl/monitoring/validate_data_quality.py',
    name='validate_data_quality',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Check for schema drift
check_schema_drift = SparkSubmitOperator(
    task_id='check_schema_drift',
    application='/src/etl/monitoring/check_schema_drift.py',
    name='check_schema_drift',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
    },
    dag=dag,
)

# Monitor pipeline execution metrics
def get_pipeline_metrics(**kwargs):
    """Collect pipeline execution metrics from Airflow metadata database"""
    from airflow.models import DagRun, TaskInstance
    import pandas as pd
    
    # Get execution date
    execution_date = kwargs['execution_date']
    
    # List of DAGs to monitor
    dags_to_monitor = [
        'data_ingestion_dag',
        'data_transformation_dag',
        'data_aggregation_dag',
        'warehouse_loading_dag'
    ]
    
    metrics = {}
    
    # Collect metrics for each DAG
    for dag_id in dags_to_monitor:
        # Get latest dag run
        dag_runs = DagRun.find(dag_id=dag_id)
        if not dag_runs:
            metrics[dag_id] = {
                'status': 'No runs found',
                'duration': None,
                'task_success_rate': None
            }
            continue
            
        latest_run = dag_runs[0]  # Most recent run
        
        # Calculate duration if run is complete
        if latest_run.end_date:
            duration = (latest_run.end_date - latest_run.start_date).total_seconds() / 60.0  # minutes
        else:
            duration = None
            
        # Get task instances
        task_instances = TaskInstance.find(dag_id=dag_id, execution_date=latest_run.execution_date)
        
        # Calculate success rate
        if task_instances:
            success_count = sum(1 for ti in task_instances if ti.state == 'success')
            total_count = len(task_instances)
            success_rate = success_count / total_count if total_count > 0 else 0
        else:
            success_rate = None
            
        metrics[dag_id] = {
            'status': latest_run.state,
            'duration': duration,
            'task_success_rate': success_rate,
            'start_time': latest_run.start_date.isoformat() if latest_run.start_date else None,
            'end_time': latest_run.end_date.isoformat() if latest_run.end_date else None
        }
    
    # Save metrics for Prometheus to scrape
    metrics_file = '/data/monitoring/pipeline_metrics.json'
    os.makedirs(os.path.dirname(metrics_file), exist_ok=True)
    
    with open(metrics_file, 'w') as f:
        json.dump({
            'timestamp': execution_date.isoformat(),
            'metrics': metrics
        }, f)
    
    return metrics

monitor_pipeline_metrics = PythonOperator(
    task_id='monitor_pipeline_metrics',
    python_callable=get_pipeline_metrics,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
wait_for_other_dags >> [
    check_data_freshness,
    validate_data_quality,
    check_schema_drift,
    monitor_pipeline_metrics,
    check_disk_usage,
    monitor_kafka_lag,
    check_data_volumes
]

# Generate report after all checks complete
[
    check_data_freshness,
    validate_data_quality,
    check_schema_drift,
    monitor_pipeline_metrics,
    check_disk_usage,
    monitor_kafka_lag,
    check_data_volumes
] >> generate_quality_report