"""
crm_to_bronze.py - Spark script to load CRM data to Bronze storage

This script:
1. Loads customer and order data from CSV files
2. Performs initial validation
3. Writes the data to the Bronze zone of the data lake in Parquet format

Usage:
    spark-submit crm_to_bronze.py [file_paths_json]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, regexp_replace, when, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType , BooleanType
import json
import sys
import os
import glob
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CRM Data to Bronze") \
    .master("spark://spark-master:7077")  \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.speculation", "true") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

bronze_dir = "/data/bronze/customers"
if not os.path.exists(bronze_dir):
    os.makedirs(bronze_dir, exist_ok=True)


def define_customer_schema():
    """Define the schema for customer data"""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("address", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("first_purchase_date", TimestampType(), True),
        StructField("last_purchase_date", TimestampType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("lifetime_value", DoubleType(), True),
        StructField("favorite_category", StringType(), True),
        StructField("skin_type", StringType(), True),
        StructField("skin_concerns", StringType(), True),
        StructField("preferred_ingredients", StringType(), True),
        StructField("allergies", StringType(), True),
        StructField("age", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("is_subscribed", BooleanType(), True),
        StructField("email_engagement", StringType(), True),
        StructField("acquisition_source", StringType(), True)
    ])

def define_order_schema():
    """Define the schema for order data"""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("order_total", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("discount_code", StringType(), True),
        StructField("final_total", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("utm_source", StringType(), True),
        StructField("utm_medium", StringType(), True),
        StructField("utm_campaign", StringType(), True),
        StructField("items", StringType(), True),  # This is a JSON string that will be parsed
        StructField("season", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone", StringType(), True)
    ])

def load_customer_data(file_paths):
    """Load customer data from CSV files with improved error handling"""
    # Define schema
    customer_schema = define_customer_schema()
        
    # Make sure file_paths is a list
    if isinstance(file_paths, str):
        file_paths = [file_paths]
        
    print(f"Attempting to load {len(file_paths)} customer files")
        
    # Check if files exist before attempting to load
    existing_files = [f for f in file_paths if os.path.exists(f)]
    print(f"Found {len(existing_files)} existing customer files")
        
    if not existing_files:
        print("No customer files found!")
        return None
        
    # Process in smaller batches to avoid overwhelming Spark
    batch_size = 10
    all_data_frames = []
        
    for i in range(0, len(existing_files), batch_size):
        batch = existing_files[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(existing_files) + batch_size - 1)//batch_size} with {len(batch)} files")
            
        try:
            # Read CSV files with the schema
            batch_df = spark.read.schema(customer_schema).csv(batch, header=True)
            row_count = batch_df.count()
            print(f"Loaded {row_count} customer records from batch")
                
            all_data_frames.append(batch_df)
        except Exception as e:
            print(f"Error processing batch: {e}")
            # Continue with next batch
        
    if not all_data_frames:
        print("No data could be loaded from any batch")
        return None
        
    # Union all batches
    df = all_data_frames[0]
    for batch_df in all_data_frames[1:]:
        df = df.union(batch_df)
        
    # Basic validation
    row_count = df.count()
    print(f"Loaded {row_count} customer records in total")
        
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("data_source", lit("crm")) \
        .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \
        .withColumn("ingestion_timestamp", current_timestamp())
        
    return df_with_metadata

def load_order_data(file_paths):
    """Load order data from CSV files"""
    # Define schema
    order_schema = define_order_schema()
    
    # Read CSV files with the schema
    df = spark.read.schema(order_schema).csv(file_paths, header=True)
    
    # Print statistics
    row_count = df.count()
    print(f"Loaded {row_count} order records")
    
    # Basic validation
    if row_count == 0:
        print("No data found in the input files")
        return None
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("data_source", lit("crm")) \
        .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \
        .withColumn("ingestion_timestamp", current_timestamp())
    
    return df_with_metadata

def process_crm_data(file_paths):
    """Process customer and order data and save to Bronze storage"""
    # Parse file paths if provided as string
    if isinstance(file_paths, str):
        if ',' in file_paths:
            file_paths = file_paths.split(',')
        else:
            file_paths = [file_paths]

            
    
    if not file_paths:
        # Default to latest files or check for a date range
        data_dir = "/data/raw/crm"
        today = datetime.now().strftime("%Y%m%d")
        
        # Try today's files
        today_files = glob.glob(f"{data_dir}/customers_{today}*.csv")
        today_files += glob.glob(f"{data_dir}/orders_{today}*.csv")

        # If no today files, try any files in the directory
        if not today_files:
            all_files = glob.glob(f"{data_dir}/customers_*.csv")
            all_files += glob.glob(f"{data_dir}/orders_*.csv")
            
            if all_files:
                print(f"No files found for today, using most recent files instead.")
                file_paths = all_files
            else:
                print(f"No CRM files found in the directory")
                sys.exit(0)
        else:
            file_paths = today_files
    
    print(f"Processing {len(file_paths)} files")
    
    # Separate customer and order files
    customer_files = [f for f in file_paths if 'customers' in f]
    order_files = [f for f in file_paths if 'orders' in f]
    
    # Process customer data
    if customer_files:
        print(f"Processing {len(customer_files)} customer files")
        customer_df = load_customer_data(customer_files)
        
        if customer_df is not None:
            # Write to Bronze storage in Parquet format
            customer_df.write \
                .format("parquet") \
                .mode("append") \
                .save("/data/bronze/customers")
            
            print(f"Successfully wrote {customer_df.count()} customer records to Bronze")
    
    # Process order data
    if order_files:
        print(f"Processing {len(order_files)} order files")
        order_df = load_order_data(order_files)
        
        if order_df is not None:
            # Write to Bronze storage in Parquet format
            order_df.write \
                .format("parquet") \
                .mode("append") \
                .save("/data/bronze/orders")
            
            print(f"Successfully wrote {order_df.count()} order records to Bronze")

if __name__ == "__main__":
    # Get file paths from command line arguments
    file_paths = sys.argv[1] if len(sys.argv) > 1 else None
    
    # If no specific files provided, find latest files
    if not file_paths:
        # Default to latest files in the CRM data directory
        data_dir = "/data/raw/crm"
        today = datetime.now().strftime("%Y%m%d")
        
        # Find files for today
        file_paths = glob.glob(f"{data_dir}/customers_{today}*.csv")
        file_paths += glob.glob(f"{data_dir}/orders_{today}*.csv")
        
        if not file_paths:
            print(f"No CRM files found for {today}")
            sys.exit(0)
    
    process_crm_data(file_paths)