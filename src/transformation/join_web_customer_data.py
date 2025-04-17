"""
join_web_customer_data.py - Spark script to join web events with customer data

This script:
1. Reads web logs data from Silver zone
2. Reads customer data from Silver zone
3. Joins authenticated web sessions with customer profiles
4. Creates enriched web sessions for better analytics
5. Writes joined data to Silver zone for downstream analytics

Usage:
    spark-submit join_web_customer_data.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, lower, trim, 
    to_timestamp, date_format, datediff, current_timestamp,
    explode, split, from_json, struct, to_json,
    concat, expr, year, month, dayofmonth, count, sum, 
    first, last, collect_list, array, array_contains, size,row_number, hour, minute, second
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Join Web Customer Data") \
    .master("spark://spark-master:7077") \
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
MINIO_SILVER_BUCKET = "s3a://silver"
MINIO_SILVER_WEB_LOGS = f"{MINIO_SILVER_BUCKET}/web_logs"
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_WEB_SESSIONS = f"{MINIO_SILVER_BUCKET}/web_sessions"
MINIO_SILVER_CUSTOMER_SESSIONS = f"{MINIO_SILVER_BUCKET}/customer_sessions"

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

def load_web_logs(date_str=None):
    """Load web logs from Silver zone"""
    try:
        print("Loading web logs from Silver zone")
        
        # If date specified, load only that partition
        if date_str:
            web_logs_path = f"{MINIO_SILVER_WEB_LOGS}/date={date_str}"
            if not spark._jvm.org.apache.hadoop.fs.Path(web_logs_path).getFileSystem(
                spark._jsc.hadoopConfiguration()).exists(
                spark._jvm.org.apache.hadoop.fs.Path(web_logs_path)):
                print(f"No data found for date {date_str}, loading all available data")
                web_logs_path = MINIO_SILVER_WEB_LOGS
        else:
            web_logs_path = MINIO_SILVER_WEB_LOGS
            
        web_logs = spark.read.parquet(web_logs_path)
        
        # Log count and sample data
        log_count = web_logs.count()
        print(f"Loaded {log_count} web log events from Silver")
        
        return web_logs
    except Exception as e:
        print(f"Error loading web logs from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/web_logs"
            print(f"Attempting to read from local path: {local_silver_path}")
            web_logs = spark.read.parquet(local_silver_path)
            log_count = web_logs.count()
            print(f"Loaded {log_count} web log events from local Silver")
            return web_logs
        except Exception as e2:
            print(f"Error reading local Silver web logs: {e2}")
            return None

def load_customer_data():
    """Load customer data from Silver zone"""
    try:
        print("Loading customer data from Silver zone")
        
        customers = spark.read.parquet(MINIO_SILVER_CUSTOMERS)
        
        # Log count
        customer_count = customers.count()
        print(f"Loaded {customer_count} customer records from Silver")
        
        return customers
    except Exception as e:
        print(f"Error loading customer data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/customers"
            print(f"Attempting to read from local path: {local_silver_path}")
            customers = spark.read.parquet(local_silver_path)
            customer_count = customers.count()
            print(f"Loaded {customer_count} customer records from local Silver")
            return customers
        except Exception as e2:
            print(f"Error reading local Silver customer data: {e2}")
            return None

def create_web_sessions(web_logs):
    """Create web sessions from web log events"""
    print("Creating web sessions from event data")
    
    if web_logs is None:
        print("No web logs data available")
        return None
    
    try:
        # Group events by session ID
        session_window = Window.partitionBy("session_id").orderBy("event_timestamp")
        
        # Create session-level metrics
        session_metrics = web_logs \
            .withColumn("event_sequence", row_number().over(session_window)) \
            .groupBy("session_id") \
            .agg(
                first("event_timestamp").alias("session_start"),
                last("event_timestamp").alias("session_end"),
                first("date").alias("date"),
                first("user_id").alias("user_id"),
                first("is_authenticated").alias("is_authenticated"),
                first("user_email").alias("user_email"),
                first("user_segment").alias("user_segment"),
                first("device_type").alias("device_type"),
                first("browser").alias("browser"),
                first("is_mobile").alias("is_mobile"),
                first("country").alias("country"),
                first("city").alias("city"),
                first("region").alias("region"),
                first("utm_source").alias("utm_source"),
                first("utm_medium").alias("utm_medium"),
                first("utm_campaign").alias("utm_campaign"),
                first("campaign_id").alias("campaign_id"),
                collect_list("event_type").alias("event_types"),
                collect_list("page_url").alias("pages_viewed"),
                count("event_id").alias("event_count"),
                sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
                sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
                sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
                sum(when(col("event_type") == "begin_checkout", 1).otherwise(0)).alias("begin_checkout_count"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
            )
        
        # Calculate session duration in seconds
        session_metrics = session_metrics \
            .withColumn("session_duration_seconds", 
                       datediff(col("session_end"), col("session_start")) * 86400 +
                       (hour(col("session_end")) - hour(col("session_start"))) * 3600 +
                       (minute(col("session_end")) - minute(col("session_start"))) * 60 +
                       (second(col("session_end")) - second(col("session_start"))))
        
        # Add derived session metrics
        session_metrics = session_metrics \
            .withColumn("is_bounce", 
                        when(col("event_count") == 1, True)
                        .otherwise(False)) \
            .withColumn("conversion_session", 
                        when(col("purchase_count") > 0, True)
                        .otherwise(False)) \
            .withColumn("session_year", year(col("session_start"))) \
            .withColumn("session_month", month(col("session_start"))) \
            .withColumn("session_day", dayofmonth(col("session_start")))
        
        # Determine session source
        session_metrics = session_metrics \
            .withColumn("session_source", 
                        when(col("utm_source").isNotNull(), col("utm_source"))
                        .when(col("utm_medium").isNotNull(), col("utm_medium"))
                        .when(col("utm_campaign").isNotNull(), col("utm_campaign"))
                        .otherwise("direct"))
            
        print(f"Created {session_metrics.count()} web sessions")
        
        return session_metrics
        
    except Exception as e:
        print(f"Error creating web sessions: {e}")
        return None

def join_customer_web_data(web_sessions, customers):
    """Join web sessions with customer data for authenticated sessions"""
    print("Joining web sessions with customer data")
    
    if web_sessions is None or customers is None:
        print("Missing required data for joining")
        return None
    
    try:
        # Filter for authenticated sessions with user_id
        authenticated_sessions = web_sessions.filter(col("is_authenticated") == True) \
            .filter(col("user_id").isNotNull())
        
        auth_session_count = authenticated_sessions.count()
        print(f"Found {auth_session_count} authenticated sessions with user_id")

        # Print sample IDs for debugging the join mismatch
        print("Sample web session user_ids:")
        authenticated_sessions.select("user_id").distinct().limit(5).show(truncate=False)
    
        print("Sample customer IDs:")
        customers.select("customer_id").distinct().limit(5).show(truncate=False)

        # Print additional details about the data types
        print("User ID data type:")
        authenticated_sessions.select("user_id").printSchema()

        print("Customer ID data type:")
        customers.select("customer_id").printSchema()

        

        # Check if any exact matches exist at all
        common_ids = authenticated_sessions.select("user_id").intersect(customers.select("customer_id"))
        common_count = common_ids.count()
        print(f"Found {common_count} IDs that are common between web sessions and customers")

        if common_count > 0:
            print("Sample matching IDs:")
            common_ids.show(truncate=False)
        else:
            print("No matching IDs found. Adding additional debug information:")
            # Try to find potential matches with string manipulations
            print("User IDs with added/removed dashes:")
            authenticated_sessions.withColumn(
                "user_id_no_dash", regexp_replace(col("user_id"), "-", "")
            ).select("user_id", "user_id_no_dash").distinct().limit(5).show(truncate=False)
        
            print("Customer IDs with added/removed dashes:")
            customers.withColumn(
                "customer_id_no_dash", regexp_replace(col("customer_id"), "-", "")
            ).select("customer_id", "customer_id_no_dash").distinct().limit(5).show(truncate=False)
        
            # Check for case-sensitivity issues
            print("User IDs lowercase:")
            authenticated_sessions.withColumn(
                "user_id_lower", lower(col("user_id"))
            ).select("user_id", "user_id_lower").distinct().limit(5).show(truncate=False)
        
            print("Customer IDs lowercase:")
            customers.withColumn(
                "customer_id_lower", lower(col("customer_id"))
            ).select("customer_id", "customer_id_lower").distinct().limit(5).show(truncate=False)

        
        # Try normalizing IDs before joining (example for UUIDs without dashes)
        authenticated_sessions = authenticated_sessions.withColumn(
            "normalized_user_id", 
            regexp_replace(col("user_id"), "-", "")
        )

        
        auth_session_count = authenticated_sessions.count()
        print(f"Found {auth_session_count} authenticated sessions with user_id")

        customers = customers.withColumn(
            "normalized_customer_id", 
            regexp_replace(col("customer_id"), "-", "")
        )
        
        # Join with customer data
        customer_sessions = authenticated_sessions \
            .join(
                customers.select(
                    "customer_id", 
                    "normalized_customer_id",
                    "first_name", 
                    "last_name", 
                    "email", 
                    "phone", 
                    "favorite_category",
                    "total_orders",
                    "lifetime_value",
                    "lifecycle_stage",
                    "value_segment"
                ),
                authenticated_sessions["normalized_user_id"] == customers["normalized_customer_id"],
                "left"
            ) \
            .select(
                authenticated_sessions["*"],
                customers["first_name"],
                customers["last_name"],
                customers["email"].alias("crm_email"),
                customers["phone"],
                customers["favorite_category"],
                customers["total_orders"],
                customers["lifetime_value"],
                customers["lifecycle_stage"],
                customers["value_segment"]
            )
        
        # Add customer match flag
        customer_sessions = customer_sessions \
            .withColumn("customer_match", col("crm_email").isNotNull())
        
        joined_count = customer_sessions.count()
        print(f"Joined {joined_count} sessions with customer data")
        
        # Get counts for matched vs. unmatched
        matched_count = customer_sessions.filter(col("customer_match") == True).count()
        unmatched_count = customer_sessions.filter(col("customer_match") == False).count()
        print(f"Matched: {matched_count}, Unmatched: {unmatched_count}")
        
        return customer_sessions
    
    except Exception as e:
        print(f"Error joining customer and web data: {e}")
        return None

def save_joined_data(web_sessions, customer_sessions, process_date):
    """Save web sessions and customer sessions to Silver zone"""
    # Format date for metadata
    date_str = process_date.strftime('%Y-%m-%d')
    
    # Save web sessions
    if web_sessions is not None:
        try:
            # Add processing metadata
            web_sessions_with_meta = web_sessions \
                .withColumn("processing_date", lit(date_str)) \
                .withColumn("silver_updated_at", current_timestamp())
            
            # Write to Silver
            web_sessions_with_meta.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("session_year", "session_month") \
                .save(MINIO_SILVER_WEB_SESSIONS)
            
            print(f"Successfully wrote {web_sessions_with_meta.count()} web sessions to Silver at {MINIO_SILVER_WEB_SESSIONS}")
        except Exception as e:
            print(f"Error saving web sessions to MinIO Silver: {e}")
            # Try local filesystem as fallback
            try:
                local_silver_path = "/data/silver/web_sessions"
                print(f"Attempting to write to local path: {local_silver_path}")
                web_sessions_with_meta.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("session_year", "session_month") \
                    .save(local_silver_path)
                print(f"Successfully wrote {web_sessions_with_meta.count()} web sessions to local Silver")
            except Exception as e2:
                print(f"Error writing web sessions to local Silver: {e2}")
    
    # Save customer sessions
    if customer_sessions is not None:
        try:
            # Add processing metadata
            customer_sessions_with_meta = customer_sessions \
                .withColumn("processing_date", lit(date_str)) \
                .withColumn("silver_updated_at", current_timestamp())
            
            # Write to Silver
            customer_sessions_with_meta.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("session_year", "session_month") \
                .save(MINIO_SILVER_CUSTOMER_SESSIONS)
            
            print(f"Successfully wrote {customer_sessions_with_meta.count()} customer sessions to Silver at {MINIO_SILVER_CUSTOMER_SESSIONS}")
        except Exception as e:
            print(f"Error saving customer sessions to MinIO Silver: {e}")
            # Try local filesystem as fallback
            try:
                local_silver_path = "/data/silver/customer_sessions"
                print(f"Attempting to write to local path: {local_silver_path}")
                customer_sessions_with_meta.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("session_year", "session_month") \
                    .save(local_silver_path)
                print(f"Successfully wrote {customer_sessions_with_meta.count()} customer sessions to local Silver")
            except Exception as e2:
                print(f"Error writing customer sessions to local Silver: {e2}")

def process_web_customer_data(date_arg=None):
    """Main function to process and join web and customer data"""
    # Get date to process
    process_date = get_date_to_process(date_arg)
    date_str = process_date.strftime('%Y-%m-%d')
    
    print(f"Processing web and customer data for {date_str}")
    
    # Load data
    web_logs = load_web_logs(date_str)
    customers = load_customer_data()
    
    # Create web sessions
    web_sessions = create_web_sessions(web_logs)
    
    # Join with customer data
    customer_sessions = join_customer_web_data(web_sessions, customers)
    
    # Save results
    save_joined_data(web_sessions, customer_sessions, process_date)
    
    return web_sessions is not None and customer_sessions is not None

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    
    success = process_web_customer_data(date_arg)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)