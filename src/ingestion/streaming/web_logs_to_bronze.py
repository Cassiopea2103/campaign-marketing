"""
web_logs_to_bronze.py - Spark Streaming script to process web logs from Kafka to Bronze storage

This script:
1. Consumes web log events from Kafka
2. Performs initial validation and parsing
3. Writes the data to the Bronze zone of the data lake in partitioned format

Usage:
    spark-submit web_logs_to_bronze.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, to_timestamp, year, month, day, hour, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, MapType, ArrayType
import json
import os
import sys
from datetime import datetime

# Initialize Spark Session with Kafka
spark = SparkSession.builder \
    .appName("Web Logs Streaming to Bronze") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints/web_logs_bronze") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
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

def process_web_logs_stream():
    """Process web logs from Kafka and save to Bronze storage"""
    # Define schema
    web_log_schema = define_web_log_schema()
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "web-logs") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON data with schema
    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), web_log_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("key", "data.*", "kafka_timestamp")
    
    # Convert timestamp string to timestamp type for partitioning
    df_with_timestamp = parsed_df \
        .withColumn("processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    
    # Add partition columns
    partitioned_df = df_with_timestamp \
        .withColumn("year", year("event_timestamp")) \
        .withColumn("month", month("event_timestamp")) \
        .withColumn("day", day("event_timestamp")) \
        .withColumn("hour", hour("event_timestamp"))
    
    # Stream to Bronze storage in parquet format, partitioned by time
    query = partitioned_df \
        .writeStream \
        .format("parquet") \
        .option("path", "/data/bronze/web_logs") \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "/data/checkpoints/web_logs_bronze") \
        .start()
    
    # Wait for the streaming query to finish
    query.awaitTermination()

if __name__ == "__main__":
    process_web_logs_stream()