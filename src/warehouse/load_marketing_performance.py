"""
load_marketing_performance.py - Extracts marketing performance data from Gold layer and loads it to Snowflake

This script:
1. Reads the marketing performance data from the Gold zone using the correct paths
2. Performs any necessary final transformations
3. Loads the data into Snowflake data warehouse tables
4. Creates a summary of the load process

Usage:
    spark-submit load_marketing_performance.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, 
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank
)
import sys
import os
from datetime import datetime, timedelta
import logging

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Load Marketing Performance to Warehouse") \
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

# Define storage paths based on the actual structure in MinIO
MINIO_GOLD_BUCKET = "s3a://gold"
# Paths discovered in MinIO console
MINIO_GOLD_ACQUISITION_METRICS = f"{MINIO_GOLD_BUCKET}/acquisition_metrics"
MINIO_GOLD_MARKETING_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/marketing_performance"
MINIO_GOLD_CAMPAIGN_ROI = f"{MINIO_GOLD_BUCKET}/campaign_roi"

# Snowflake connection parameters (simulation for this project)
SNOWFLAKE_OPTIONS = {
    "sfUrl": "your-snowflake-account.snowflakecomputing.com",
    "sfUser": "MARKETING_ETL_USER",
    "sfPassword": "{{snowflake_password}}",  # In production, use Airflow Variable or secret
    "sfDatabase": "MARKETING_DATA",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "MARKETING_WH"
}

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
    """Load data from the gold bucket with error handling"""
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

def load_to_snowflake(dataframe, table_name, mode="append"):
    """Load dataframe to Snowflake table"""
    if dataframe is None:
        print(f"No data to load to {table_name}")
        return False
    
    try:
        print(f"Loading {dataframe.count()} records to Snowflake table {table_name}")
        
        # In a real environment, you would use Snowflake connector
        # This is a simulation for the project
        dataframe.write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        
        print(f"Successfully loaded data to Snowflake table {table_name}")
        return True
    except Exception as e:
        print(f"Error loading to Snowflake table {table_name}: {e}")
        
        # For simulation purposes, write to local file system
        try:
            output_path = f"/data/warehouse/{table_name}"
            print(f"Simulating Snowflake load by writing to {output_path}")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            dataframe.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(output_path)
            
            print(f"Simulated load to {output_path}")
            return True
        except Exception as e2:
            print(f"Error in simulated load: {e2}")
            return False

def load_marketing_performance(process_date):
    """Main function to load marketing performance data to the warehouse"""
    date_str = process_date.strftime('%Y-%m-%d')
    year_str = str(process_date.year)
    month_str = str(process_date.month).zfill(2)
    
    print(f"Loading marketing performance data for {date_str}")
    
    # Load channel comparison data from Gold (using the correct path structure)
    channel_comparison_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison/attribution_model=first_touch/year={year_str}/month={month_str}"
    channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
    
    # Try alternate attribution models if first_touch doesn't exist
    if channel_comparison is None:
        alt_paths = [
            f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison/attribution_model=last_touch/year={year_str}/month={month_str}",
            f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison/attribution_model=linear/year={year_str}/month={month_str}"
        ]
        for path in alt_paths:
            channel_comparison = load_data_from_gold(path, "channel comparison (alternate model)")
            if channel_comparison is not None:
                break
    
    # Load campaign ROI channel comparison data
    campaign_roi_channel_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison/time_period=daily/year={year_str}/month={month_str}"
    campaign_roi_channel = load_data_from_gold(campaign_roi_channel_path, "campaign ROI channel comparison")
    
    # Try other time periods if daily doesn't exist
    if campaign_roi_channel is None:
        alt_periods = ["monthly", "weekly", "quarterly"]
        for period in alt_periods:
            path = f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison/time_period={period}/year={year_str}/month={month_str}"
            campaign_roi_channel = load_data_from_gold(path, f"campaign ROI channel comparison ({period})")
            if campaign_roi_channel is not None:
                break
    
    # Load acquisition metrics
    acquisition_path = f"{MINIO_GOLD_ACQUISITION_METRICS}/acquisition_source/time_period=daily/year={year_str}/month={month_str}"
    acquisition_metrics = load_data_from_gold(acquisition_path, "acquisition metrics")
    
    # Try other time periods if daily doesn't exist
    if acquisition_metrics is None:
        alt_periods = ["monthly", "weekly", "quarterly"]
        for period in alt_periods:
            path = f"{MINIO_GOLD_ACQUISITION_METRICS}/acquisition_source/time_period={period}/year={year_str}/month={month_str}"
            acquisition_metrics = load_data_from_gold(path, f"acquisition metrics ({period})")
            if acquisition_metrics is not None:
                break
    
    success = True
    
    # Process and load channel comparison data to Snowflake
    if channel_comparison is not None:
        # Select and process relevant columns
        # Adjust column selection based on actual schema
        try:
            channel_comparison_fact = channel_comparison.select(
                "*"  # Initially select all columns to see what's available
            )
            # Show schema to understand the actual data structure
            print("Channel comparison schema:")
            channel_comparison_fact.printSchema()
            
            # Now select specific columns based on the schema
            available_columns = channel_comparison_fact.columns
            required_columns = [
                "utm_source", "utm_medium", "revenue", "conversions", 
                "total_cost", "total_impressions", "total_clicks",
                "roas", "roi", "processing_date"
            ]
            
            # Filter to only columns that actually exist
            selected_columns = [col for col in required_columns if col in available_columns]
            
            # Add any missing columns as nulls
            for column in required_columns:
                if column not in selected_columns:
                    channel_comparison_fact = channel_comparison_fact.withColumn(column, lit(None))
            
            # Final select with all required columns
            channel_comparison_fact = channel_comparison_fact.select(*required_columns)
            
            # Load to Snowflake
            success = success and load_to_snowflake(
                channel_comparison_fact,
                "MARKETING_CHANNEL_PERFORMANCE_FACT",
                "append"
            )
        except Exception as e:
            print(f"Error processing channel comparison data: {e}")
    
    # Process and load campaign ROI data to Snowflake
    if campaign_roi_channel is not None:
        try:
            # Show schema to understand the actual data structure
            print("Campaign ROI channel schema:")
            campaign_roi_channel.printSchema()
            
            # Select columns based on the schema
            campaign_roi_fact = campaign_roi_channel.select("*")
            
            # Load to Snowflake
            success = success and load_to_snowflake(
                campaign_roi_fact,
                "MARKETING_CAMPAIGN_ROI_FACT",
                "append"
            )
        except Exception as e:
            print(f"Error processing campaign ROI data: {e}")
    
    # Process and load acquisition metrics to Snowflake
    if acquisition_metrics is not None:
        try:
            # Show schema to understand the actual data structure
            print("Acquisition metrics schema:")
            acquisition_metrics.printSchema()
            
            # Select columns based on the schema
            acquisition_fact = acquisition_metrics.select("*")
            
            # Load to Snowflake
            success = success and load_to_snowflake(
                acquisition_fact,
                "MARKETING_ACQUISITION_FACT",
                "append"
            )
        except Exception as e:
            print(f"Error processing acquisition metrics: {e}")
    
    # Create a summary if we have any data
    if channel_comparison is not None or campaign_roi_channel is not None or acquisition_metrics is not None:
        try:
            # Create combined metrics for summary
            metrics = []
            
            if channel_comparison is not None:
                channel_metrics = channel_comparison.agg(
                    sum("revenue").alias("total_revenue"),
                    sum("total_cost").alias("total_channel_cost"),
                    avg("roi").alias("avg_channel_roi")
                ).collect()[0]
                
                metrics.append(("Channel Revenue", channel_metrics["total_revenue"]))
                metrics.append(("Channel Cost", channel_metrics["total_channel_cost"]))
                metrics.append(("Average ROI", channel_metrics["avg_channel_roi"]))
            
            if campaign_roi_channel is not None:
                # Adjust column names based on actual schema
                campaign_roi_metrics = campaign_roi_channel.agg(
                    sum("channel_revenue").alias("total_campaign_revenue") if "channel_revenue" in campaign_roi_channel.columns else lit(0).alias("total_campaign_revenue"),
                    sum("channel_cost").alias("total_campaign_cost") if "channel_cost" in campaign_roi_channel.columns else lit(0).alias("total_campaign_cost")
                ).collect()[0]
                
                metrics.append(("Campaign Revenue", campaign_roi_metrics["total_campaign_revenue"]))
                metrics.append(("Campaign Cost", campaign_roi_metrics["total_campaign_cost"]))
            
            if acquisition_metrics is not None:
                # Adjust column names based on actual schema
                acquisition_summary = acquisition_metrics.agg(
                    sum("customer_count").alias("total_customers") if "customer_count" in acquisition_metrics.columns else lit(0).alias("total_customers"),
                    avg("cost_per_acquisition").alias("avg_cpa") if "cost_per_acquisition" in acquisition_metrics.columns else lit(0).alias("avg_cpa")
                ).collect()[0]
                
                metrics.append(("Total Customers", acquisition_summary["total_customers"]))
                metrics.append(("Average CPA", acquisition_summary["avg_cpa"]))
            
            # Create a summary dataframe
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType
            
            schema = StructType([
                StructField("metric_name", StringType(), True),
                StructField("metric_value", DoubleType(), True),
                StructField("report_date", StringType(), True)
            ])
            
            summary_data = [(name, float(value) if value is not None else 0.0, date_str) for name, value in metrics]
            summary_df = spark.createDataFrame(summary_data, schema)
            
            # Load the summary to Snowflake
            success = success and load_to_snowflake(
                summary_df,
                "MARKETING_PERFORMANCE_SUMMARY",
                "append"
            )
            
        except Exception as e:
            print(f"Error creating summary: {e}")
    else:
        print("No data found for creating summary")
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Load marketing performance data to warehouse
    success = load_marketing_performance(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)