"""
ingest_web_logs.py - Spark script to ingest web log data into Kafka

This script:
1. Reads web log JSON files
2. Performs basic validations
3. Publishes events to Kafka for streaming processing

Usage:
    spark-submit ingest_web_logs.py [file_paths_json]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, MapType, ArrayType
import json
import sys
import os
from datetime import datetime

# Initialize Spark Session with Kafka
spark = SparkSession.builder \
    .appName("Web Logs Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

def define_web_log_schema():
    """Define the schema for web log data"""
    # User schema
    user_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("authenticated", BooleanType(), True),
        StructField("registration_date", StringType(), True),
        StructField("user_segment", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("favorite_category", StringType(), True)
    ])
    
    # Device schema
    device_schema = StructType([
        StructField("type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("operator", StringType(), True),
        StructField("is_mobile", BooleanType(), True)
    ])
    
    # Location schema
    location_schema = StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("language", StringType(), True)
    ])
    
    # Marketing schema
    marketing_schema = StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("campaign_id", StringType(), True)
    ])
    
    # Page schema
    page_schema = StructType([
        StructField("url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("title", StringType(), True),
        StructField("visit_duration", StringType(), True)
    ])
    
    # Main web log schema
    web_log_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user", user_schema, True),
        StructField("device", device_schema, True),
        StructField("location", location_schema, True),
        StructField("marketing", marketing_schema, True),
        StructField("page", page_schema, True)
    ])
    
    return web_log_schema

def ingest_logs_to_kafka(file_paths):
    """Ingest log files to Kafka"""
    # Parse file paths if provided as string
    if isinstance(file_paths, str):
        try:
            file_paths = json.loads(file_paths)
        except json.JSONDecodeError:
            file_paths = [file_paths]  # Single file path
    
    if not file_paths:
        print("No files to process")
        return
    
    print(f"Processing {len(file_paths)} files")
    
    # Define schema
    web_log_schema = define_web_log_schema()
    
    # Read JSON files with the schema
    df = spark.read.schema(web_log_schema).json(file_paths)
    
    # Print some statistics about the data
    row_count = df.count()
    print(f"Loaded {row_count} events")
    
    # Basic data validation
    if row_count == 0:
        print("No data found in the input files")
        return
    
    # Filter out records with missing critical fields
    validated_df = df.filter(
        col("event_id").isNotNull() &
        col("event_type").isNotNull() &
        col("timestamp").isNotNull() &
        col("session_id").isNotNull()
    )
    
    invalid_count = row_count - validated_df.count()
    if invalid_count > 0:
        print(f"Filtered out {invalid_count} invalid records")
    
    # Convert the DataFrame to JSON for Kafka
    kafka_df = validated_df.select(
        col("event_id").alias("key"),
        to_json(struct("*")).alias("value")
    )
    
    # Write to Kafka topic
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "web-logs") \
        .save()
    
    print(f"Successfully ingested {validated_df.count()} events to Kafka topic 'web-logs'")

if __name__ == "__main__":
    # Get file paths from command line arguments
    file_paths = sys.argv[1] if len(sys.argv) > 1 else None
    
    # If no specific files provided, find latest logs
    if not file_paths:
        # Default to latest files in the web data directory
        data_dir = "/data/raw/web"
        today = datetime.now().strftime("%Y%m%d")
        
        # Find files for today
        import glob
        file_paths = glob.glob(f"{data_dir}/web_logs_{today}*.json")
        
        if not file_paths:
            print(f"No log files found for {today}")
            sys.exit(0)
    
    ingest_logs_to_kafka(file_paths)