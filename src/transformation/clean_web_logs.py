"""
clean_web_logs.py - Spark script to clean web logs data from Bronze to Silver

This script:
1. Reads web log data from the Bronze zone
2. Cleans and standardizes data
3. Enriches with additional metadata
4. Writes processed data to the Silver zone of the data lake

Usage:
    spark-submit clean_web_logs.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, lower, trim, 
    to_timestamp, date_format, datediff, current_date,
    explode, split, from_json, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Clean Web Logs") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

def get_date_to_process(date_arg=None):
    """Determine the date to process from argument or default to yesterday"""
    if date_arg:
        try:
            # Try to parse the date argument
            process_date = datetime.strptime(date_arg, '%Y-%m-%d')
            return process_date
        except ValueError:
            print(f"Invalid date format: {date_arg}. Using yesterday's date.")
    
    # Default to yesterday
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday

def clean_web_logs(process_date):
    """Clean web logs data for the given date"""
    date_str = process_date.strftime('%Y-%m-%d')
    year = process_date.strftime('%Y')
    month = process_date.strftime('%m')
    day = process_date.strftime('%d')
    
    print(f"Processing web logs for {date_str}")
    
    # Read data from Bronze zone - using partitioning
    bronze_path = f"/data/bronze/web_logs/year={year}/month={month}/day={day}"
    
    # Check if data exists
    if not os.path.exists(bronze_path):
        print(f"No data found at {bronze_path}")
        # Try to read all data for the day without hour partitioning
        bronze_path = f"/data/bronze/web_logs/year={year}/month={month}/day={day}"
    
    try:
        # Read the bronze data
        bronze_df = spark.read.parquet(bronze_path)
        
        # Log record count
        record_count = bronze_df.count()
        print(f"Loaded {record_count} web log records from Bronze")
        
        if record_count == 0:
            print("No records to process")
            return
    except Exception as e:
        print(f"Error reading Bronze data: {e}")
        return
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_df \
        .dropDuplicates(["event_id"]) \
        .filter(col("event_id").isNotNull() & col("timestamp").isNotNull() & col("session_id").isNotNull())
    
    # 2. Standardize timestamps
    cleaned_df = cleaned_df \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    
    # 3. Clean and normalize user data
    cleaned_df = cleaned_df \
        .withColumn("user_id", col("user.user_id")) \
        .withColumn("is_authenticated", col("user.authenticated")) \
        .withColumn("user_email", when(col("user.email").isNotNull(), lower(trim(col("user.email")))).otherwise(None)) \
        .withColumn("user_segment", col("user.user_segment"))
    
    # 4. Clean device data
    cleaned_df = cleaned_df \
        .withColumn("device_type", col("device.type")) \
        .withColumn("browser", col("device.browser")) \
        .withColumn("is_mobile", col("device.is_mobile"))
    
    # 5. Clean location data
    cleaned_df = cleaned_df \
        .withColumn("country", col("location.country")) \
        .withColumn("city", col("location.city")) \
        .withColumn("region", col("location.region"))
    
    # 6. Clean marketing attribution data
    cleaned_df = cleaned_df \
        .withColumn("utm_source", col("marketing.source")) \
        .withColumn("utm_medium", col("marketing.medium")) \
        .withColumn("utm_campaign", col("marketing.campaign")) \
        .withColumn("campaign_id", col("marketing.campaign_id"))
    
    # 7. Clean event-specific data
    
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
    
    # 8. Extract E-commerce data
    
    # 9. Enrich with session information
    # Calculate time since first event in session - will be added in a separate process
    
    # 10. Select and rename columns for final output
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
        "visit_duration"
    )
    
    # Write to Silver zone in Parquet format with date partition
    silver_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("date") \
        .save("/data/silver/web_logs")
    
    print(f"Successfully wrote {silver_df.count()} web log records to Silver")
    
    # Create an optimized view for sessions
    session_df = silver_df \
        .groupBy("session_id", "date") \
        .agg(
            {"event_timestamp": "min", "event_timestamp": "max"}, 
            {"event_id": "count"}, 
            {"is_authenticated": "max"},
            {"user_id": "first"},
            {"utm_source": "first"},
            {"utm_medium": "first"},
            {"utm_campaign": "first"},
            {"campaign_id": "first"},
            {"device_type": "first"},
            {"is_mobile": "first"},
            {"country": "first"},
            {"city": "first"}
        ) \
        .withColumnRenamed("min(event_timestamp)", "session_start") \
        .withColumnRenamed("max(event_timestamp)", "session_end") \
        .withColumnRenamed("count(event_id)", "event_count") \
        .withColumnRenamed("max(is_authenticated)", "is_authenticated") \
        .withColumnRenamed("first(user_id)", "user_id") \
        .withColumnRenamed("first(utm_source)", "utm_source") \
        .withColumnRenamed("first(utm_medium)", "utm_medium") \
        .withColumnRenamed("first(utm_campaign)", "utm_campaign") \
        .withColumnRenamed("first(campaign_id)", "campaign_id") \
        .withColumnRenamed("first(device_type)", "device_type") \
        .withColumnRenamed("first(is_mobile)", "is_mobile") \
        .withColumnRenamed("first(country)", "country") \
        .withColumnRenamed("first(city)", "city")
    
    # Add session duration
    session_df = session_df.withColumn(
        "session_duration_seconds", 
        (col("session_end").cast("long") - col("session_start").cast("long"))
    )
    
    # Write sessions to Silver
    session_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("date") \
        .save("/data/silver/web_sessions")
    
    print(f"Successfully wrote {session_df.count()} web session records to Silver")

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    clean_web_logs(process_date)