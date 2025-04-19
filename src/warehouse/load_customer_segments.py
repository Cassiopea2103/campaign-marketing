"""
load_customer_segments.py - Spark script to load customer segmentation data from Gold to warehouse

This script:
1. Reads customer segmentation data from the Gold zone
2. Prepares the data for loading into the data warehouse
3. Applies any final transformations needed for reporting
4. Loads the data into the warehouse tables

Usage:
    spark-submit load_customer_segments.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, date_format,
    year, month, dayofmonth, coalesce, concat, expr
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Load Customer Segments to Warehouse") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("spark.jars.packages", 
"net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define storage paths
MINIO_GOLD_BUCKET = "s3a://gold"
MINIO_GOLD_CUSTOMER_SEGMENTS = f"{MINIO_GOLD_BUCKET}/customer_segments"
MINIO_GOLD_RFM_SEGMENTS = f"{MINIO_GOLD_BUCKET}/rfm_segments"
MINIO_GOLD_LIFECYCLE_SEGMENTS = f"{MINIO_GOLD_BUCKET}/lifecycle_segments"
WAREHOUSE_OUTPUT_DIR = "/data/warehouse/customer_segments"

# Define Snowflake connection properties
SNOWFLAKE_OPTIONS = {
    "sfURL": "ZCYQNOS-IA60918.snowflakecomputing.com",
    "sfUser": "CASSIOPEA2103",
    "sfPassword": "Saliou2103wade@", 
    "sfDatabase": "MARKETING_ANALYTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

# JDBC URL for Snowflake
SNOWFLAKE_URL = "net.snowflake.spark.snowflake"

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

def load_data_from_gold(path, description):
    """Load data from Gold zone with error handling"""
    try:
        df = spark.read.parquet(path)
        print(f"Loaded {df.count()} {description} records from {path}")
        return df
    except Exception as e:
        print(f"Error reading {description} from Gold: {e}")
        # Try local filesystem as fallback
        try:
            local_path = f"/data{path[path.find('/', 5):]}"
            print(f"Attempting to read from local path: {local_path}")
            df = spark.read.parquet(local_path)
            print(f"Loaded {df.count()} {description} records from local path")
            return df
        except Exception as e2:
            print(f"Error reading {description} from local path: {e2}")
            return None

def save_to_warehouse(dataframe, table_name):
    """Save dataframe directly to Snowflake"""
    if dataframe is None:
        print(f"No data to save to warehouse table {table_name}")
        return False
        
    # Write directly to Snowflake
    try:
        # Write to Snowflake using the Snowflake connector
        dataframe.write \
          .format("snowflake") \
          .options(**{
              "sfUrl": SNOWFLAKE_OPTIONS["sfURL"],
              "sfUser": SNOWFLAKE_OPTIONS["sfUser"],
              "sfPassword": SNOWFLAKE_OPTIONS["sfPassword"],
              "sfDatabase": SNOWFLAKE_OPTIONS["sfDatabase"] , 
              "sfSchema": SNOWFLAKE_OPTIONS["sfSchema"],    
              "sfWarehouse": SNOWFLAKE_OPTIONS["sfWarehouse"],
              "dbtable": table_name
          }) \
          .mode("overwrite") \
          .save()
            
        print(f"Successfully wrote {dataframe.count()} records to Snowflake table {table_name}")
        return True
    except Exception as e:
        print(f"Error writing to Snowflake: {e}")
        import traceback
        traceback.print_exc()
        return False

def prepare_combined_segments_table(combined_segments):
    """Prepare combined customer segments fact table for the warehouse"""
    if combined_segments is None:
        print("No combined segments data available")
        return None
    
    try:
        # Check available columns
        available_columns = combined_segments.columns
        print(f"Available columns in combined_segments: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "customer_id", "first_name", "last_name", "email", "city", "region",
            "rfm_segment", "r_score", "f_score", "m_score", "rfm_score",
            "primary_category", "secondary_category", "product_affinity_segment",
            "lifecycle_stage", "purchase_frequency_segment", "customer_value_segment",
            "days_since_first_order", "days_since_last_order", "customer_tenure_days",
            "marketing_recommendation", "preferred_channel_recommendation",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        segments_table = combined_segments.select(*columns_to_select)
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            segments_table = segments_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a customer_segment_key
        segments_table = segments_table.withColumn(
            "customer_segment_key", 
            concat(
                col("customer_id"),
                lit("_"),
                lit (str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month))
            )
        )
        
        # Add data warehouse metadata
        segments_table = segments_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return segments_table
    except Exception as e:
        print(f"Error preparing combined segments table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_rfm_segments_table(rfm_segments):
    """Prepare RFM segments fact table for the warehouse"""
    if rfm_segments is None:
        print("No RFM segments data available")
        return None
    
    try:
        # Check available columns
        available_columns = rfm_segments.columns
        print(f"Available columns in rfm_segments: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "customer_id", "recency", "frequency", "monetary",
            "r_score", "f_score", "m_score", "rfm_score", "rfm_segment",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        rfm_table = rfm_segments.select(*columns_to_select)
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            rfm_table = rfm_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a rfm_segment_key
        rfm_table = rfm_table.withColumn(
            "rfm_segment_key", 
            concat(
                col("customer_id"),
                lit("_rfm_"),
                lit (str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month))
            )
        )
        
        # Add data warehouse metadata
        rfm_table = rfm_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return rfm_table
    except Exception as e:
        print(f"Error preparing RFM segments table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_lifecycle_segments_table(lifecycle_segments):
    """Prepare lifecycle segments fact table for the warehouse"""
    if lifecycle_segments is None:
        print("No lifecycle segments data available")
        return None
    
    try:
        # Check available columns
        available_columns = lifecycle_segments.columns
        print(f"Available columns in lifecycle_segments: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "customer_id", "first_order_date", "last_order_date",
            "total_spend", "avg_order_value", "order_count",
            "days_since_first_order", "days_since_last_order", "customer_tenure_days",
            "lifecycle_stage", "purchase_frequency_segment", "customer_value_segment",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        lifecycle_table = lifecycle_segments.select(*columns_to_select)
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            lifecycle_table = lifecycle_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a lifecycle_segment_key
        lifecycle_table = lifecycle_table.withColumn(
            "lifecycle_segment_key", 
            concat(
                col("customer_id"),
                lit("_lifecycle_"),
                lit (str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month))
            )
        )
        
        # Add data warehouse metadata
        lifecycle_table = lifecycle_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return lifecycle_table
    except Exception as e:
        print(f"Error preparing lifecycle segments table: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_customer_dimension(combined_segments):
    """Create customer dimension for the warehouse"""
    if combined_segments is None:
        print("No customer data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = combined_segments.columns
        if "customer_id" not in available_columns:
            print("Required column customer_id not found in data")
            return None
        
        # Select customer demographic information
        customer_columns = [
            "customer_id", "first_name", "last_name", "email", 
            "city", "region"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in customer_columns if col in available_columns]
        
        # Extract distinct customer information
        customer_dim = combined_segments.select(*columns_to_select).distinct()
        
        # Create customer_key (using customer_id directly as key)
        customer_dim = customer_dim.withColumn("customer_key", col("customer_id"))
        
        # Extract location data if available
        if "city" in available_columns and "region" in available_columns:
            # Add location grouping
            customer_dim = customer_dim \
                .withColumn(
                    "location_group", 
                    when(col("region").isNotNull(), col("region"))
                    .otherwise("Unknown")
                )
        else:
            customer_dim = customer_dim \
                .withColumn("city", lit(None).cast("string")) \
                .withColumn("region", lit(None).cast("string")) \
                .withColumn("location_group", lit("Unknown"))
        
        # Add data warehouse metadata
        customer_dim = customer_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_updated_ts", current_date())
        
        return customer_dim
    except Exception as e:
        print(f"Error creating customer dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_segment_dimension():
    """Create segment reference dimension for the warehouse"""
    try:
        # Create static dimension for segment types and descriptions
        data = [
            # RFM Segments
            ("Champions", "RFM", "High value customers who bought recently", "High", "High"),
            ("Loyal Customers", "RFM", "Bought recently and regularly", "Medium", "High"),
            ("Potential Loyalists", "RFM", "Recent customers with average frequency", "Medium", "Medium"),
            ("Recent Customers", "RFM", "Bought most recently, but not often", "Low", "Medium"),
            ("Promising", "RFM", "Recent customers with low spend", "Low", "Low"),
            ("Customers Needing Attention", "RFM", "Above average recency and frequency, low spend", "Medium", "Medium"),
            ("At Risk", "RFM", "Below average recency and frequency, any spend", "High", "Low"),
            ("Can't Lose Them", "RFM", "Haven't purchased for some time, but high overall spend", "High", "Low"),
            ("Hibernating", "RFM", "Last purchase was long time ago, low spend", "Medium", "Low"),
            ("Lost", "RFM", "Lowest recency, frequency and value", "Low", "Low"),
            
            # Lifecycle Segments
            ("New Customer", "Lifecycle", "First purchase within 30 days", "Medium", "High"),
            ("Active Customer", "Lifecycle", "Purchased within 90 days", "Low", "High"),
            ("At-Risk Customer", "Lifecycle", "Not purchased in 91-180 days", "Medium", "Medium"),
            ("Lapsed Customer", "Lifecycle", "Not purchased in 181-365 days", "High", "Medium"),
            ("Lost Customer", "Lifecycle", "Not purchased in >365 days", "High", "Low"),
            
            # Product Affinity Segments
            ("Facial Care Enthusiast", "Product", "Primary purchases are facial care products", "Low", "Medium"),
            ("Body Care Focused", "Product", "Primary purchases are body care products", "Low", "Medium"),
            ("Hair Care Passionate", "Product", "Primary purchases are hair care products", "Low", "Medium"),
            ("Makeup Aficionado", "Product", "Primary purchases are makeup products", "Low", "Medium"),
            ("Fragrance Collector", "Product", "Primary purchases are fragrance products", "Low", "Medium"),
            ("Mixed Category Shopper", "Product", "No dominant product category preference", "Low", "Low"),
            
            # Frequency Segments
            ("One-Time Buyer", "Frequency", "Made only one purchase", "High", "Medium"),
            ("Occasional Buyer", "Frequency", "Made 2-3 purchases", "Medium", "Medium"),
            ("Regular Buyer", "Frequency", "Made 4-6 purchases", "Low", "Medium"),
            ("Frequent Buyer", "Frequency", "Made more than 6 purchases", "Low", "High"),
            
            # Value Segments
            ("Premium Value", "Value", "Top 25% in customer lifetime value", "Low", "High"),
            ("High Value", "Value", "Top 25-50% in customer lifetime value", "Low", "High"),
            ("Medium Value", "Value", "Middle 25-50% in customer lifetime value", "Medium", "Medium"),
            ("Low Value", "Value", "Bottom 25% in customer lifetime value", "High", "Low")
        ]
        
        # Create DataFrame
        segment_dim = spark.createDataFrame(data, [
            "segment_name", "segment_type", "segment_description", 
            "churn_risk", "upsell_potential"
        ])
        
        # Add segment_key (using segment_name directly as key)
        segment_dim = segment_dim.withColumn(
            "segment_key", 
            concat(col("segment_type"), lit("_"), col("segment_name"))
        )
        
        # Add data warehouse metadata
        segment_dim = segment_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_updated_ts", current_date())
        
        return segment_dim
    except Exception as e:
        print(f"Error creating segment dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_customer_segments_to_warehouse(process_date):
    """Main function to load customer segmentation data to the warehouse"""
    print(f"Loading customer segmentation data to warehouse for {process_date.strftime('%Y-%m-%d')}")
    
    # Determine year and month to load from gold
    year = process_date.year
    month = process_date.month
    
    # Try to load combined segments data from Gold
    try:
        # Try different path patterns
        combined_segments_path = f"{MINIO_GOLD_CUSTOMER_SEGMENTS}/year={year}/month={month}"
        combined_segments = load_data_from_gold(combined_segments_path, "combined segments")
        
        # If that fails, try with just the base path
        if combined_segments is None:
            combined_segments_path = f"{MINIO_GOLD_CUSTOMER_SEGMENTS}"
            combined_segments = load_data_from_gold(combined_segments_path, "combined segments")
    except Exception as e:
        print(f"Error loading combined segments: {e}")
        combined_segments = None
    
    # Try to load RFM segments data from Gold
    try:
        rfm_segments_path = f"{MINIO_GOLD_RFM_SEGMENTS}/year={year}/month={month}"
        rfm_segments = load_data_from_gold(rfm_segments_path, "RFM segments")
        
        if rfm_segments is None:
            rfm_segments_path = f"{MINIO_GOLD_RFM_SEGMENTS}"
            rfm_segments = load_data_from_gold(rfm_segments_path, "RFM segments")
    except Exception as e:
        print(f"Error loading RFM segments: {e}")
        rfm_segments = None
    
    # Try to load lifecycle segments data from Gold
    try:
        lifecycle_segments_path = f"{MINIO_GOLD_LIFECYCLE_SEGMENTS}/year={year}/month={month}"
        lifecycle_segments = load_data_from_gold(lifecycle_segments_path, "lifecycle segments")
        
        if lifecycle_segments is None:
            lifecycle_segments_path = f"{MINIO_GOLD_LIFECYCLE_SEGMENTS}"
            lifecycle_segments = load_data_from_gold(lifecycle_segments_path, "lifecycle segments")
    except Exception as e:
        print(f"Error loading lifecycle segments: {e}")
        lifecycle_segments = None
    
    # Prepare data for warehouse
    success = True
    
    # 1. Prepare combined segments fact table
    combined_segments_table = prepare_combined_segments_table(combined_segments)
    if combined_segments_table is not None:
        success = success and save_to_warehouse(combined_segments_table, "FACT_CUSTOMER_SEGMENTS")
    
    # 2. Prepare RFM segments fact table
    rfm_segments_table = prepare_rfm_segments_table(rfm_segments)
    if rfm_segments_table is not None:
        success = success and save_to_warehouse(rfm_segments_table, "FACT_RFM_SEGMENTS")
    
    # 3. Prepare lifecycle segments fact table
    lifecycle_segments_table = prepare_lifecycle_segments_table(lifecycle_segments)
    if lifecycle_segments_table is not None:
        success = success and save_to_warehouse(lifecycle_segments_table, "FACT_LIFECYCLE_SEGMENTS")
    
    # 4. Create customer dimension
    customer_dimension = create_customer_dimension(combined_segments or rfm_segments or lifecycle_segments)
    if customer_dimension is not None:
        success = success and save_to_warehouse(customer_dimension, "DIM_CUSTOMER")
    
    # 5. Create segment reference dimension
    segment_dimension = create_segment_dimension()
    if segment_dimension is not None:
        success = success and save_to_warehouse(segment_dimension, "DIM_SEGMENT")
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    print(f"Starting customer segmentation data load to Snowflake for date: {process_date.strftime('%Y-%m-%d')}")
    
    # Add Snowflake JDBC driver dependency if needed
    try:
        spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/snowflake-jdbc-3.13.30.jar")
        spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.0-spark_3.3/spark-snowflake_2.12-2.11.0-spark_3.3.jar")
        print("Added Snowflake JDBC dependencies to SparkContext")
    except Exception as e:
        print(f"Warning: Could not add Snowflake JDBC dependencies: {e}")
    
    # Load data to warehouse
    success = load_customer_segments_to_warehouse(process_date)
    
    if success:
        print("Customer segmentation data successfully loaded to Snowflake warehouse")
    else:
        print("Warning: Some customer segmentation data could not be loaded to Snowflake warehouse")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)