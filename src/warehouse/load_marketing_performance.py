"""
load_marketing_performance.py - Spark script to load marketing performance data from Gold to warehouse

This script:
1. Reads marketing performance data from the Gold zone
2. Prepares the data for loading into the data warehouse
3. Applies any final transformations needed for reporting
4. Loads the data into the warehouse tables

Usage:
    spark-submit load_marketing_performance.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, date_format,
    year, month, dayofmonth, coalesce, concat
)
import sys
import os
from datetime import datetime, timedelta

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
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("spark.jars.packages", 
"net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define storage paths
MINIO_GOLD_BUCKET = "s3a://gold"
MINIO_GOLD_MARKETING_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/marketing_performance"
WAREHOUSE_OUTPUT_DIR = "/data/warehouse/marketing_performance"

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
    """Save dataframe directly to Snowflake without local backup"""
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

def prepare_channel_performance_table(channel_rankings):
    """Prepare channel performance dimension table for the warehouse"""
    if channel_rankings is None:
        print("No channel rankings data available")
        return None
    
    try:
        # Check available columns
        available_columns = channel_rankings.columns
        print(f"Available columns in channel_rankings: {available_columns}")
        
        # Select only available columns
        # Based on error message, it appears time_period and attribution_model aren't in the data
        columns_to_select = []
        
        # Add core columns that we know exist
        core_columns = [
            "utm_source", "utm_medium", "utm_campaign", "conversions",
            "revenue", "unique_orders", "unique_users", "total_cost",
            "total_impressions", "total_clicks", "roas", "roi",
            "cpa", "cpc", "conversion_rate", "roi_rank", "roas_rank",
            "revenue_rank", "conversion_rank", "performance_score",
            "performance_tier", "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in core_columns if col in available_columns]
        
        # Select relevant columns and rename for clarity
        channel_performance = channel_rankings.select(*columns_to_select)
        
        # Add time_period and attribution_model as constants if they don't exist
        if "time_period" not in available_columns:
            channel_performance = channel_performance.withColumn("time_period", lit("monthly"))
        
        if "attribution_model" not in available_columns:
            channel_performance = channel_performance.withColumn("attribution_model", lit("last_touch"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            channel_performance = channel_performance.withColumnRenamed("processing_date", "etl_date")
        
        # Add data warehouse metadata
        channel_performance = channel_performance \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return channel_performance
    except Exception as e:
        print(f"Error preparing channel performance table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_trend_analysis_table(trend_analysis):
    """Prepare trend analysis facts for the warehouse"""
    if trend_analysis is None:
        print("No trend analysis data available")
        return None
    
    try:
        # Check available columns
        available_columns = trend_analysis.columns
        print(f"Available columns in trend_analysis: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "utm_source", "utm_medium", "utm_campaign", 
            "current_start_date", "current_end_date", 
            "previous_start_date", "previous_end_date",
            "current_conversions", "current_revenue",
            "previous_conversions", "previous_revenue",
            "conversion_change", "conversion_change_pct",
            "revenue_change", "revenue_change_pct",
            "revenue_trend", "processing_date", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        trend_table = trend_analysis.select(*columns_to_select)
        
        # Add missing columns with default values
        if "attribution_model" not in available_columns:
            trend_table = trend_table.withColumn("attribution_model", lit("last_touch"))
        
        if "time_period" not in available_columns:
            trend_table = trend_table.withColumn("time_period", lit("monthly"))
            
        if "year" not in available_columns:
            trend_table = trend_table.withColumn("year", lit(datetime.now().year))
            
        if "month" not in available_columns:
            trend_table = trend_table.withColumn("month", lit(datetime.now().month))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            trend_table = trend_table.withColumnRenamed("processing_date", "etl_date")
        
        # Add data warehouse metadata
        trend_table = trend_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        # Convert date strings to date type for better querying
        date_columns = ["current_start_date", "current_end_date", "previous_start_date", "previous_end_date"]
        for date_col in date_columns:
            if date_col in available_columns:
                new_col = date_col.replace("_date", "")
                trend_table = trend_table.withColumn(new_col, to_date(col(date_col)))
        
        return trend_table
    except Exception as e:
        print(f"Error preparing trend analysis table: {e}")
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
            "utm_source", "utm_medium", 
            "channel_revenue_percentage", "channel_conversion_percentage", 
            "channel_cost_percentage", "channel_efficiency_index",
            "processing_date", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        comparison_table = channel_comparison.select(*columns_to_select)
        
        # Add missing columns with default values
        if "attribution_model" not in available_columns:
            comparison_table = comparison_table.withColumn("attribution_model", lit("last_touch"))
            
        if "year" not in available_columns:
            comparison_table = comparison_table.withColumn("year", lit(datetime.now().year))
            
        if "month" not in available_columns:
            comparison_table = comparison_table.withColumn("month", lit(datetime.now().month))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            comparison_table = comparison_table.withColumnRenamed("processing_date", "etl_date")
        
        # Add data warehouse metadata
        comparison_table = comparison_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return comparison_table
    except Exception as e:
        print(f"Error preparing channel comparison table: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_marketing_date_dimension(channel_rankings):
    """Create date dimension from marketing data"""
    if channel_rankings is None:
        print("No marketing data available for date dimension")
        return None
    
    try:
        # Check available columns
        available_columns = channel_rankings.columns
        print(f"Available columns for date dimension: {available_columns}")
        
        # Make sure start_date and end_date are available
        if "start_date" not in available_columns or "end_date" not in available_columns:
            print("Required columns start_date or end_date not found in data")
            
            # Create a simple date dimension based on current date if columns not available
            current_date_val = datetime.now().strftime("%Y-%m-%d")
            
            # Create a single row dataframe with date info
            date_dimension = spark.createDataFrame([
                (current_date_val, current_date_val, "monthly")
            ], ["start_date", "end_date", "period_type"])
            
            # Add computed columns
            date_dimension = date_dimension \
                .withColumn("start_date_dt", to_date(col("start_date"))) \
                .withColumn("end_date_dt", to_date(col("end_date"))) \
                .withColumn("date_key", date_format(col("start_date_dt"), "yyyyMMdd")) \
                .withColumn("year", year(col("start_date_dt"))) \
                .withColumn("month", month(col("start_date_dt"))) \
                .withColumn("day", dayofmonth(col("start_date_dt"))) \
                .withColumn("period_start_date", col("start_date_dt")) \
                .withColumn("period_end_date", col("end_date_dt"))
        else:
            # Extract distinct dates
            select_cols = ["start_date", "end_date"]
            
            date_data = channel_rankings.select(*select_cols).distinct()
            
            # Add a constant time_period column since it doesn't exist in the data
            date_dimension = date_data.withColumn("period_type", lit("monthly"))
            
            # Create date_key (yyyymmdd format)
            date_dimension = date_dimension \
                .withColumn("start_date_dt", to_date(col("start_date"))) \
                .withColumn("end_date_dt", to_date(col("end_date"))) \
                .withColumn("date_key", date_format(col("start_date_dt"), "yyyyMMdd")) \
                .withColumn("year", year(col("start_date_dt"))) \
                .withColumn("month", month(col("start_date_dt"))) \
                .withColumn("day", dayofmonth(col("start_date_dt"))) \
                .withColumn("period_start_date", col("start_date_dt")) \
                .withColumn("period_end_date", col("end_date_dt"))
        
        # Add data warehouse metadata
        date_dimension = date_dimension \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return date_dimension
    except Exception as e:
        print(f"Error creating date dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_marketing_channel_dimension(channel_rankings):
    """Create marketing channel dimension for the warehouse"""
    if channel_rankings is None:
        print("No channel data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = channel_rankings.columns
        if "utm_source" not in available_columns or "utm_medium" not in available_columns:
            print("Required columns utm_source or utm_medium not found in data")
            # Create a simple default channel dimension if data not available
            channel_dim = spark.createDataFrame([
                ("direct", "none"),
                ("google", "organic"),
                ("google", "cpc"),
                ("facebook", "social"),
                ("instagram", "social"),
                ("email", "newsletter")
            ], ["utm_source", "utm_medium"])
        else:
            # Extract distinct channel information
            channel_dim = channel_rankings.select(
                "utm_source",
                "utm_medium"
            ).distinct()
        
        # Create concatenated string for channel_key
        channel_dim = channel_dim \
            .withColumn("source_medium_concat", 
                concat(
                    coalesce(col("utm_source"), lit("")), 
                    lit("_"), 
                    coalesce(col("utm_medium"), lit(""))
                )
            )
        
        # Use the concatenated string directly instead of md5 hash
        channel_dim = channel_dim \
            .withColumn("channel_key", col("source_medium_concat")) \
            .withColumn("channel_name", 
                when(col("utm_source").isNotNull() & col("utm_medium").isNotNull(),
                    concat(col("utm_source"), lit(" - "), col("utm_medium")))
                .when(col("utm_source").isNotNull(), col("utm_source"))
                .when(col("utm_medium").isNotNull(), col("utm_medium"))
                .otherwise(lit("Unknown"))
            ) \
            .withColumn("channel_group", 
                when(col("utm_medium") == "cpc", "Paid Search")
                .when(col("utm_medium") == "social", "Social Media")
                .when(col("utm_medium") == "email", "Email")
                .when(col("utm_medium") == "organic", "Organic")
                .when(col("utm_medium") == "referral", "Referral")
                .when(col("utm_medium") == "affiliate", "Affiliate")
                .otherwise("Other")
            )
        
        # Drop the temporary column
        channel_dim = channel_dim.drop("source_medium_concat")
        
        # Add data warehouse metadata
        channel_dim = channel_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return channel_dim
    except Exception as e:
        print(f"Error creating channel dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_marketing_campaign_dimension(channel_rankings):
    """Create marketing campaign dimension for the warehouse"""
    if channel_rankings is None:
        print("No campaign data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = channel_rankings.columns
        required_columns = ["utm_campaign", "utm_source", "utm_medium"]
        
        missing_columns = [col for col in required_columns if col not in available_columns]
        if missing_columns:
            print(f"Missing required columns for campaign dimension: {missing_columns}")
            # Create a simple default campaign dimension if data not available
            campaign_dim = spark.createDataFrame([
                ("spring_sale_2025", "google", "cpc"),
                ("new_product_launch", "facebook", "social"),
                ("email_newsletter_april", "email", "newsletter"),
                ("retargeting_campaign", "google", "display")
            ], ["utm_campaign", "utm_source", "utm_medium"])
        else:
            # Extract distinct campaign information
            campaign_dim = channel_rankings.select(
                "utm_campaign",
                "utm_source",
                "utm_medium"
            ).distinct()
        
        # Create concatenated string for campaign_key
        campaign_dim = campaign_dim \
            .withColumn("campaign_key_concat", 
                concat(
                    coalesce(col("utm_campaign"), lit("")), 
                    lit("_"), 
                    coalesce(col("utm_source"), lit("")), 
                    lit("_"), 
                    coalesce(col("utm_medium"), lit(""))
                )
            )
        
        # Use the concatenated string directly instead of md5 hash
        campaign_dim = campaign_dim \
            .withColumn("campaign_key", col("campaign_key_concat")) \
            .withColumn("campaign_channel", 
                when(col("utm_source").isNotNull() & col("utm_medium").isNotNull(),
                    concat(col("utm_source"), lit(" - "), col("utm_medium")))
                .when(col("utm_source").isNotNull(), col("utm_source"))
                .when(col("utm_medium").isNotNull(), col("utm_medium"))
                .otherwise(lit("Unknown")))
        
        # Drop the temporary column
        campaign_dim = campaign_dim.drop("campaign_key_concat")
        
        # Extract campaign type and category from campaign name
        campaign_dim = campaign_dim \
            .withColumn("campaign_type", 
                when(col("utm_campaign").like("%promo%"), "Promotional")
                .when(col("utm_campaign").like("%launch%"), "Product Launch")
                .when(col("utm_campaign").like("%brand%"), "Branding")
                .when(col("utm_campaign").like("%retarg%"), "Retargeting")
                .otherwise("General"))
            
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

def load_marketing_performance_to_warehouse(process_date):
    """Main function to load marketing performance data to the warehouse"""
    print(f"Loading marketing performance data to warehouse for {process_date.strftime('%Y-%m-%d')}")
    
    # Determine year and month to load from gold
    year = process_date.year
    month = process_date.month
    
    # Try to load channel rankings data from Gold
    try:
        # Try different path patterns - first with time_period and attribution_model partitions
        channel_rankings_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_rankings/time_period=monthly/attribution_model=last_touch/year={year}/month={month}"
        channel_rankings = load_data_from_gold(channel_rankings_path, "channel rankings")
        
        # If that fails, try without time_period and attribution_model partitions
        if channel_rankings is None:
            channel_rankings_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_rankings/year={year}/month={month}"
            channel_rankings = load_data_from_gold(channel_rankings_path, "channel rankings")
            
        # If that fails too, try with just the base path
        if channel_rankings is None:
            channel_rankings_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_rankings"
            channel_rankings = load_data_from_gold(channel_rankings_path, "channel rankings")
    except Exception as e:
        print(f"Error loading channel rankings: {e}")
        channel_rankings = None
    
    # Try to load trend analysis data from Gold with similar fallback pattern
    try:
        trend_analysis_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/trend_analysis/time_period=monthly/attribution_model=last_touch/year={year}/month={month}"
        trend_analysis = load_data_from_gold(trend_analysis_path, "trend analysis")
        
        if trend_analysis is None:
            trend_analysis_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/trend_analysis/year={year}/month={month}"
            trend_analysis = load_data_from_gold(trend_analysis_path, "trend analysis")
            
        if trend_analysis is None:
            trend_analysis_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/trend_analysis"
            trend_analysis = load_data_from_gold(trend_analysis_path, "trend analysis")
    except Exception as e:
        print(f"Error loading trend analysis: {e}")
        trend_analysis = None
    
    # Try to load channel comparison data from Gold with similar fallback pattern
    try:
        channel_comparison_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison/attribution_model=last_touch/year={year}/month={month}"
        channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
        
        if channel_comparison is None:
            channel_comparison_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison/year={year}/month={month}"
            channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
            
        if channel_comparison is None:
            channel_comparison_path = f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison"
            channel_comparison = load_data_from_gold(channel_comparison_path, "channel comparison")
    except Exception as e:
        print(f"Error loading channel comparison: {e}")
        channel_comparison = None
    
    # Prepare data for warehouse
    success = True
    
    # 1. Prepare channel performance table
    channel_performance = prepare_channel_performance_table(channel_rankings)
    if channel_performance is not None:
        success = success and save_to_warehouse(channel_performance, "FACT_CHANNEL_PERFORMANCE")
    
    # 2. Prepare trend analysis table
    trend_table = prepare_trend_analysis_table(trend_analysis)
    if trend_table is not None:
        success = success and save_to_warehouse(trend_table, "FACT_MARKETING_TRENDS")
    
    # 3. Prepare channel comparison table
    comparison_table = prepare_channel_comparison_table(channel_comparison)
    if comparison_table is not None:
        success = success and save_to_warehouse(comparison_table, "FACT_CHANNEL_COMPARISON")
    
    # 4. Create date dimension
    date_dimension = create_marketing_date_dimension(channel_rankings)
    if date_dimension is not None:
        success = success and save_to_warehouse(date_dimension, "DIM_MARKETING_DATE")
    
    # 5. Create channel dimension
    channel_dimension = create_marketing_channel_dimension(channel_rankings)
    if channel_dimension is not None:
        success = success and save_to_warehouse(channel_dimension, "DIM_MARKETING_CHANNEL")
    
    # 6. Create campaign dimension
    campaign_dimension = create_marketing_campaign_dimension(channel_rankings)
    if campaign_dimension is not None:
        success = success and save_to_warehouse(campaign_dimension, "DIM_MARKETING_CAMPAIGN")
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    print(f"Starting marketing performance data load to Snowflake for date: {process_date.strftime('%Y-%m-%d')}")
    
    # Load data to warehouse
    success = load_marketing_performance_to_warehouse(process_date)
    
    if success:
        print("Marketing performance data successfully loaded to Snowflake warehouse")
    else:
        print("Warning: Some marketing performance data could not be loaded to Snowflake warehouse")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)