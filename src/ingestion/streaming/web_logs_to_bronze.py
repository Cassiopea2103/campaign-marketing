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
from pyspark.sql.functions import (
    col, from_json, expr, window, to_timestamp, 
    year, month, dayofmonth, hour, minute, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    BooleanType, MapType, ArrayType
)
import os
import sys
from datetime import datetime

# Initialize Spark Session with Kafka
spark = SparkSession.builder \
    .appName("Web Logs Streaming to Bronze") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
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
    try:
        # Create checkpoint directory
        checkpoint_dir = "/data/checkpoints/web_logs_bronze"
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        # Define Bronze output path
        bronze_path = "/data/bronze/web_logs"
        os.makedirs(bronze_path, exist_ok=True)
        
        # Define schema
        web_log_schema = define_web_log_schema()

        print ("Starting Spark Streaming to process web logs...")
        
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "web-logs") \
            .option("startingOffsets", "earliest") \
            .load()
            
        print("Connected to Kafka stream")
        
        # Parse JSON from Kafka
        print ("Parsing JSON from Kafka stream...")
        parsed_df = kafka_df.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), web_log_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("key", "data.*", "kafka_timestamp")
        print ("Parsed JSON from Kafka stream")
        
        # Convert timestamp string to timestamp type for partitioning
        print ("Converting timestamp to timestamp type...")
        df_with_timestamp = parsed_df \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("processing_timestamp", current_timestamp())
        print ("Converted timestamp to timestamp type")
        
        # Add partition columns
        print("adding partition columns...")
        partitioned_df = df_with_timestamp \
            .withColumn("year", year("event_timestamp")) \
            .withColumn("month", month("event_timestamp")) \
            .withColumn("day", dayofmonth("event_timestamp")) \
            .withColumn("hour", hour("event_timestamp"))
        print ("Added partition columns")
        
        # Add source info
        print ("Adding source info...")
        final_df = partitioned_df \
            .withColumn("data_source", lit("kafka")) \
            .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        print ("Added source info")
            
        # Stream to Bronze storage in parquet format, partitioned by time
        print ("Starting streaming to Bronze storage...")
        query = final_df \
            .writeStream \
            .format("parquet") \
            .option("path", bronze_path) \
            .partitionBy("year", "month", "day", "hour") \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime="1 minute") \
            .start()
            
        print(f"Started streaming data to {bronze_path}")
        
        # Wait for the streaming query to finish
        print("Waiting for streaming query to finish...")
        query.awaitTermination()
        print("Streaming query finished")
    
    except Exception as e:
        print(f"Error processing web logs stream: {e}")
        
        # Try an alternative approach using a batch write
        try:
            print("Attempting fallback batch processing approach...")
            
            # Read a small batch from Kafka
            batch_df = spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("subscribe", "web-logs") \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
                
            # Proceed with processing if data exists
            if batch_df.count() > 0:
                print(f"Found {batch_df.count()} records in Kafka")
                
                # Parse JSON
                parsed_batch_df = batch_df.select(
                    col("key").cast("string"),
                    from_json(col("value").cast("string"), web_log_schema).alias("data"),
                    col("timestamp").alias("kafka_timestamp")
                ).select("key", "data.*", "kafka_timestamp")
                
                # Add timestamps and partitioning
                batch_with_ts = parsed_batch_df \
                    .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
                    .withColumn("processing_timestamp", current_timestamp()) \
                    .withColumn("data_source", lit("kafka_batch")) \
                    .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \
                    .withColumn("year", year("event_timestamp")) \
                    .withColumn("month", month("event_timestamp")) \
                    .withColumn("day", dayofmonth("event_timestamp")) \
                    .withColumn("hour", hour("event_timestamp"))
                
                # Write batch to bronze
                local_bronze_path = "/data/bronze/web_logs_batch"
                os.makedirs(local_bronze_path, exist_ok=True)
                
                batch_with_ts.write \
                    .format("parquet") \
                    .mode("append") \
                    .partitionBy("year", "month", "day", "hour") \
                    .save(local_bronze_path)
                    
                print(f"Successfully wrote {batch_with_ts.count()} records as batch to {local_bronze_path}")
            else:
                print("No data found in Kafka topic")
        
        except Exception as e2:
            print(f"Fallback approach also failed: {e2}")
            sys.exit(1)

if __name__ == "__main__":
    process_web_logs_stream()