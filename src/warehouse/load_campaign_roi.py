"""
load_campaign_roi.py - Spark script to load campaign ROI data from Gold to warehouse

This script:
1. Reads campaign ROI data from the Gold zone
2. Prepares the data for loading into the data warehouse
3. Applies any final transformations needed for reporting
4. Loads the data into the warehouse tables

Usage:
    spark-submit load_campaign_roi.py [date_string]
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
    .appName("Load Campaign ROI to Warehouse") \
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
MINIO_GOLD_CAMPAIGN_ROI = f"{MINIO_GOLD_BUCKET}/campaign_roi"
WAREHOUSE_OUTPUT_DIR = "/data/warehouse/campaign_roi"

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

def prepare_campaign_performance_table(campaign_performance):
    """Prepare campaign performance fact table for the warehouse"""
    if campaign_performance is None:
        print("No campaign performance data available")
        return None
    
    try:
        # Check available columns
        available_columns = campaign_performance.columns
        print(f"Available columns in campaign_performance: {available_columns}")
        
        # Select relevant columns based on what's available
        base_columns = [
            "campaign_name", "platform", "category", "attribution_model",
            "time_period", "start_date", "end_date", 
            "period_revenue", "period_cost", "period_orders", 
            "period_customers", "period_impressions", "period_clicks",
            "period_tracked_conversions", "period_roas", "period_roi",
            "roi_rank", "revenue_rank", "roi_classification",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in base_columns if col in available_columns]
        
        # Select relevant columns and rename for clarity
        campaign_facts = campaign_performance.select(*columns_to_select)
        
        # Add any missing required columns
        if "time_period" not in available_columns:
            campaign_facts = campaign_facts.withColumn("time_period", lit("monthly"))
            
        if "attribution_model" not in available_columns:
            campaign_facts = campaign_facts.withColumn("attribution_model", lit("last_touch"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            campaign_facts = campaign_facts.withColumnRenamed("processing_date", "etl_date")
            
        # Create a campaign_performance_key
        campaign_facts = campaign_facts.withColumn(
            "campaign_performance_key", 
            concat(
                col("campaign_name"),
                lit("_"),
                coalesce(col("platform"), lit("unknown")),
                lit("_"),
                col("time_period"),
                lit("_"),
                lit(str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month)),
            )
        )
        
        # Add data warehouse metadata
        campaign_facts = campaign_facts \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return campaign_facts
    except Exception as e:
        print(f"Error preparing campaign performance table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_channel_comparison_table(channel_comparison):
    """Prepare channel comparison facts for the warehouse"""
    if channel_comparison is None:
        print("No channel comparison data available")
        return None
    
    try:
        # Check available columns
        available_columns = channel_comparison.columns
        print(f"Available columns in channel_comparison: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "platform", "time_period", "start_date", "end_date",
            "channel_cost", "channel_revenue", "channel_impressions",
            "channel_clicks", "channel_orders", "campaign_count",
            "avg_campaign_roi", "channel_roi", "channel_ctr",
            "channel_cpa", "channel_conversion_rate", "roi_rank",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        channel_facts = channel_comparison.select(*columns_to_select)
        
        # Add any missing required columns with default values
        if "time_period" not in available_columns:
            channel_facts = channel_facts.withColumn("time_period", lit("monthly"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            channel_facts = channel_facts.withColumnRenamed("processing_date", "etl_date")
            
        # Create a channel_comparison_key
        channel_facts = channel_facts.withColumn(
            "channel_comparison_key", 
            concat(
                coalesce(col("platform"), lit("unknown")),
                lit("_"),
                col("time_period"),
                lit("_"),
                lit(str (datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month)),
            )
        )
        
        # Add data warehouse metadata
        channel_facts = channel_facts \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return channel_facts
    except Exception as e:
        print(f"Error preparing channel comparison table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_roi_trends_table(roi_trends):
    """Prepare ROI trends facts for the warehouse"""
    if roi_trends is None:
        print("No ROI trends data available")
        return None
    
    try:
        # Check available columns
        available_columns = roi_trends.columns
        print(f"Available columns in roi_trends: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "campaign_name", "platform", "category", "attribution_model",
            "period_roi", "period_revenue", "period_cost", 
            "time_period", "start_date", "end_date",
            "roi_rank_in_channel", "roi_quality",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        trends_facts = roi_trends.select(*columns_to_select)
        
        # Add missing columns with default values
        if "time_period" not in available_columns:
            trends_facts = trends_facts.withColumn("time_period", lit("monthly"))
            
        if "attribution_model" not in available_columns:
            trends_facts = trends_facts.withColumn("attribution_model", lit("last_touch"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            trends_facts = trends_facts.withColumnRenamed("processing_date", "etl_date")
            
        # Create a roi_trend_key
        trends_facts = trends_facts.withColumn(
            "roi_trend_key", 
            concat(
                col("campaign_name"),
                lit("_"),
                coalesce(col("platform"), lit("unknown")),
                lit("_"),
                lit(str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month)),
            )
        )
        
        # Add data warehouse metadata
        trends_facts = trends_facts \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return trends_facts
    except Exception as e:
        print(f"Error preparing ROI trends table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_budget_allocation_table(budget_allocation):
    """Prepare budget allocation facts for the warehouse"""
    if budget_allocation is None:
        print("No budget allocation data available")
        return None
    
    try:
        # Check available columns
        available_columns = budget_allocation.columns
        print(f"Available columns in budget_allocation: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "campaign_name", "platform", "category", "period_cost",
            "period_revenue", "period_roi", "current_budget_share",
            "revenue_contribution", "roi_rank", "budget_reallocation_factor",
            "adjusted_budget", "suggested_budget_share", "suggested_budget_change",
            "allocation_recommendation", "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        budget_facts = budget_allocation.select(*columns_to_select)
        
        # Add missing columns with default values
        if "time_period" not in available_columns:
            budget_facts = budget_facts.withColumn("time_period", lit("monthly"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            budget_facts = budget_facts.withColumnRenamed("processing_date", "etl_date")
            
        # Create a budget_allocation_key
        budget_facts = budget_facts.withColumn(
            "budget_allocation_key", 
            concat(
                col("campaign_name"),
                lit("_"),
                coalesce(col("platform"), lit("unknown")),
                lit("_"),
                lit(str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month)),
            )
        )
        
        # Add data warehouse metadata
        budget_facts = budget_facts \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return budget_facts
    except Exception as e:
        print(f"Error preparing budget allocation table: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_campaign_dimension(campaign_performance):
    """Create campaign dimension for the warehouse"""
    if campaign_performance is None:
        print("No campaign data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = campaign_performance.columns
        if "campaign_name" not in available_columns or "platform" not in available_columns:
            print("Required columns campaign_name or platform not found in data")
            # Create a simple default campaign dimension if data not available
            campaign_dim = spark.createDataFrame([
                ("spring_sale_2025", "google", "cpc", "Promotional"),
                ("new_product_launch", "facebook", "social", "Product Launch"),
                ("summer_collection", "instagram", "social", "Seasonal"),
                ("retargeting_campaign", "google", "display", "Retargeting")
            ], ["campaign_name", "platform", "campaign_medium", "campaign_type"])
        else:
            # Extract distinct campaign information
            campaign_dim = campaign_performance.select(
                "campaign_name", 
                "platform", 
                "category"
            ).distinct()
            
            # Add campaign type based on campaign name patterns
            campaign_dim = campaign_dim \
                .withColumn("campaign_type", 
                    when(col("campaign_name").like("%promo%"), "Promotional")
                    .when(col("campaign_name").like("%launch%"), "Product Launch")
                    .when(col("campaign_name").like("%season%"), "Seasonal")
                    .when(col("campaign_name").like("%retarg%"), "Retargeting")
                    .when(col("campaign_name").like("%brand%"), "Branding")
                    .otherwise("General")
                )
        
        # Create campaign key (using campaign name directly as key)
        campaign_dim = campaign_dim.withColumn(
            "campaign_key", 
            concat(
                col("campaign_name"),
                lit("_"),
                coalesce(col("platform"), lit("unknown"))
            )
        )
        
        # Add data warehouse metadata
        campaign_dim = campaign_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return campaign_dim
    except Exception as e:
        print(f"Error creating campaign dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_platform_dimension(campaign_performance):
    """Create platform dimension for the warehouse"""
    if campaign_performance is None:
        print("No platform data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = campaign_performance.columns
        if "platform" not in available_columns:
            print("Required column platform not found in data")
            # Create a simple default platform dimension if data not available
            platform_dim = spark.createDataFrame([
                ("google", "Search", "Paid Search"),
                ("facebook", "Social", "Paid Social"),
                ("instagram", "Social", "Paid Social"),
                ("tiktok", "Social", "Paid Social"),
                ("email", "Direct", "Email Marketing"),
                ("influencer", "Social", "Influencer Marketing")
            ], ["platform", "platform_type", "channel_group"])
        else:
            # Extract distinct platform information
            platform_dim = campaign_performance.select("platform").distinct()
            
            # Add platform type and channel group
            platform_dim = platform_dim \
                .withColumn("platform_type", 
                    when(col("platform") == "google", "Search")
                    .when(col("platform").isin("facebook", "instagram", "tiktok"), "Social")
                    .when(col("platform") == "email", "Direct")
                    .when(col("platform") == "influencer", "Social")
                    .otherwise("Other")
                ) \
                .withColumn("channel_group", 
                    when(col("platform") == "google", "Paid Search")
                    .when(col("platform").isin("facebook", "instagram", "tiktok"), "Paid Social")
                    .when(col("platform") == "email", "Email Marketing")
                    .when(col("platform") == "influencer", "Influencer Marketing")
                    .otherwise("Other")
                )
        
        # Use platform directly as key
        platform_dim = platform_dim.withColumn("platform_key", col("platform"))
        
        # Add data warehouse metadata
        platform_dim = platform_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return platform_dim
    except Exception as e:
        print(f"Error creating platform dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_campaign_roi_to_warehouse(process_date):
    """Main function to load campaign ROI data to the warehouse"""
    print(f"Loading campaign ROI data to warehouse for {process_date.strftime('%Y-%m-%d')}")
    
    # Determine year and month to load from gold
    year = process_date.year
    month = process_date.month
    
    # Try to load campaign performance data from Gold
    try:
        # Try different path patterns
        campaign_performance_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/campaign_performance/time_period=monthly/year={year}/month={month}"
        campaign_performance = load_data_from_gold(campaign_performance_path, "campaign performance")
        
        # If that fails, try without time_period partition
        if campaign_performance is None:
            campaign_performance_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/campaign_performance/year={year}/month={month}"
            campaign_performance = load_data_from_gold(campaign_performance_path, "campaign performance")
            
        # If that fails too, try with just the base path
        if campaign_performance is None:
            campaign_performance_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/campaign_performance"
            campaign_performance = load_data_from_gold(campaign_performance_path, "campaign performance")
    except Exception as e:
        print(f"Error loading campaign performance: {e}")
        import traceback
        traceback.print_exc()
        campaign_performance = None
    
    # Try to load channel comparison data from Gold with similar fallback pattern
    try:
        channel_comparison_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison/time_period=monthly/year={year}/month={month}"
        channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
        
        if channel_comparison is None:
            channel_comparison_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison/year={year}/month={month}"
            channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
            
        if channel_comparison is None:
            channel_comparison_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison"
            channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
    except Exception as e:
        print(f"Error loading channel comparison: {e}")
        channel_comparison = None
    
    # Try to load ROI trends data from Gold
    try:
        roi_trends_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/roi_trends/platform=*/year={year}/month={month}"
        roi_trends = load_data_from_gold(roi_trends_path, "ROI trends")
        
        if roi_trends is None:
            roi_trends_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/roi_trends/platform=*"
            roi_trends = load_data_from_gold(roi_trends_path, "ROI trends")
            
        if roi_trends is None:
            roi_trends_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/roi_trends"
            roi_trends = load_data_from_gold(roi_trends_path, "ROI trends")
    except Exception as e:
        print(f"Error loading ROI trends: {e}")
        roi_trends = None
    
    # Try to load budget allocation data from Gold
    try:
        budget_allocation_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/budget_allocation/platform=*/year={year}/month={month}"
        budget_allocation = load_data_from_gold(budget_allocation_path, "budget allocation")
        
        if budget_allocation is None:
            budget_allocation_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/budget_allocation/platform=*"
            budget_allocation = load_data_from_gold(budget_allocation_path, "budget allocation")
            
        if budget_allocation is None:
            budget_allocation_path = f"{MINIO_GOLD_CAMPAIGN_ROI}/budget_allocation"
            budget_allocation = load_data_from_gold(budget_allocation_path, "budget allocation")
    except Exception as e:
        print(f"Error loading budget allocation: {e}")
        budget_allocation = None
    
    # Prepare data for warehouse
    success = True
    
    # 1. Prepare campaign performance table
    campaign_facts = prepare_campaign_performance_table(campaign_performance)
    if campaign_facts is not None:
        success = success and save_to_warehouse(campaign_facts, "FACT_CAMPAIGN_PERFORMANCE")
    
    # 2. Prepare channel comparison table
    channel_facts = prepare_channel_comparison_table(channel_comparison)
    if channel_facts is not None:
        success = success and save_to_warehouse(channel_facts, "FACT_CAMPAIGN_CHANNEL_COMPARISON")
    
    # 3. Prepare ROI trends table
    trends_facts = prepare_roi_trends_table(roi_trends)
    if trends_facts is not None:
        success = success and save_to_warehouse(trends_facts, "FACT_CAMPAIGN_ROI_TRENDS")
    
    # 4. Prepare budget allocation table
    budget_facts = prepare_budget_allocation_table(budget_allocation)
    if budget_facts is not None:
        success = success and save_to_warehouse(budget_facts, "FACT_CAMPAIGN_BUDGET_ALLOCATION")
    
    # 5. Create campaign dimension
    campaign_dimension = create_campaign_dimension(campaign_performance)
    if campaign_dimension is not None:
        success = success and save_to_warehouse(campaign_dimension, "DIM_CAMPAIGN")
    
    # 6. Create platform dimension
    platform_dimension = create_platform_dimension(campaign_performance)
    if platform_dimension is not None:
        success = success and save_to_warehouse(platform_dimension, "DIM_PLATFORM")
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    print(f"Starting campaign ROI data load to Snowflake for date: {process_date.strftime('%Y-%m-%d')}")
    
    # Add Snowflake JDBC driver dependency if needed
    
    # Load data to warehouse
    success = load_campaign_roi_to_warehouse(process_date)
    
    if success:
        print("Campaign ROI data successfully loaded to Snowflake warehouse")
    else:
        print("Warning: Some campaign ROI data could not be loaded to Snowflake warehouse")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)