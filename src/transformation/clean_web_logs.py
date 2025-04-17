"""
clean_web_logs.py - Spark Streaming script to clean web logs data from Kafka to Silver

This script:
1. Consumes web log events from Kafka as a stream
2. Cleans and standardizes data
3. Enriches with additional metadata
4. Writes processed data to the Silver zone of the data lake in MinIO

Usage:
    spark-submit clean_web_logs.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, lower, trim, 
    to_timestamp, date_format, datediff, current_timestamp,
    explode, split, from_json, struct, to_json, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
import sys
import os
from datetime import datetime

# Initialize Spark Session with Kafka and S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Clean Web Logs Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define MinIO paths
MINIO_SILVER_PATH = "s3a://silver/web_logs"
MINIO_SILVER_SESSIONS_PATH = "s3a://silver/web_sessions"
CHECKPOINT_LOCATION = "s3a://bronze/checkpoints/web_logs_cleaning"

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
    """Process web logs stream from Kafka and save to Silver storage"""
    try:
        print("Starting Spark Streaming to clean web logs...")
        
        # Define schema
        web_log_schema = define_web_log_schema()
        
        # Read from Kafka as a stream
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "web-logs") \
            .option("startingOffsets", "earliest") \
            .load()
            
        print("Connected to Kafka stream for web logs")
        
        # Parse JSON from Kafka
        parsed_df = kafka_df.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), web_log_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("key", "data.*", "kafka_timestamp")
        
        # Standardize timestamps
        cleaned_df = parsed_df \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        
        # Clean and normalize user data
        cleaned_df = cleaned_df \
            .withColumn("user_id", col("user.user_id")) \
            .withColumn("is_authenticated", col("user.authenticated")) \
            .withColumn("user_email", when(col("user.email").isNotNull(), lower(trim(col("user.email")))).otherwise(None)) \
            .withColumn("user_segment", col("user.user_segment"))
        
        # Clean device data
        cleaned_df = cleaned_df \
            .withColumn("device_type", col("device.type")) \
            .withColumn("browser", col("device.browser")) \
            .withColumn("is_mobile", col("device.is_mobile"))
        
        # Clean location data
        cleaned_df = cleaned_df \
            .withColumn("country", col("location.country")) \
            .withColumn("city", col("location.city")) \
            .withColumn("region", col("location.region"))
        
        # Clean marketing attribution data
        cleaned_df = cleaned_df \
            .withColumn("utm_source", col("marketing.source")) \
            .withColumn("utm_medium", col("marketing.medium")) \
            .withColumn("utm_campaign", col("marketing.campaign")) \
            .withColumn("campaign_id", col("marketing.campaign_id"))
        
        # Extract page data
        cleaned_df = cleaned_df \
            .withColumn("page_url", col("page.url")) \
            .withColumn("referrer", col("page.referrer")) \
            .withColumn("visit_duration", col("page.visit_duration"))
        
        # Create event category field
        cleaned_df = cleaned_df \
            .withColumn("event_category", when(col("event_type").isin(["page_view", "scroll", "exit"]), "navigation")
                .when(col("event_type").isin(["product_view", "add_to_cart", "remove_from_cart"]), "product")
                .when(col("event_type").isin(["begin_checkout", "purchase"]), "conversion")
                .when(col("event_type").isin(["search", "filter_products"]), "search")
                .otherwise("other"))
        
        # Add silver metadata
        cleaned_df = cleaned_df \
            .withColumn("silver_processed_at", current_timestamp()) \
            .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Select final columns
        silver_df = cleaned_df.select(
            "event_id",
            "event_type",
            "event_category",
            "event_timestamp",
            "date",
            "session_id",
            "user_id",
            "is_authenticated",
            "user_email",
            "user_segment",
            "device_type",
            "browser",
            "is_mobile",
            "country",
            "city",
            "region",
            "utm_source",
            "utm_medium", 
            "utm_campaign",
            "campaign_id",
            "page_url",
            "referrer",
            "visit_duration",
            "silver_processed_at",
            "silver_batch_id"
        )
        
        # Add partition columns
        silver_df = silver_df \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp")))
        
        # Write to Silver zone as a stream with partitioning
        query = silver_df \
            .writeStream \
            .format("parquet") \
            .option("path", MINIO_SILVER_PATH) \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .partitionBy("date") \
            .trigger(processingTime="2 minutes") \
            .outputMode("append") \
            .start()
        
        print(f"Started streaming clean data to Silver zone at {MINIO_SILVER_PATH}")
        
        # Keep the stream running until manually terminated
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in web logs streaming process: {e}")
        return False
    
    return True

if __name__ == "__main__":
    # Process web logs as a stream
    success = process_web_logs_stream()
    
    # Set exit code based on success
    sys.exit(0 if success else 1)