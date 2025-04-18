"""
marketing_performance.py - Spark script to create marketing performance analytics for Gold layer

This script:
1. Processes marketing performance data from Silver
2. Creates aggregated metrics by channel, campaign, time period
3. Calculates ROI, conversion, and efficiency metrics
4. Creates cross-channel comparison metrics
5. Writes results to the Gold zone for reporting and dashboards

Usage:
    spark-submit marketing_performance.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, 
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank, lag, row_number
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Marketing Performance Analytics") \
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

# Define storage paths
MINIO_SILVER_BUCKET = "s3a://silver"
MINIO_GOLD_BUCKET = "s3a://gold"
MINIO_SILVER_ATTRIBUTION = f"{MINIO_SILVER_BUCKET}/marketing_attribution"
MINIO_SILVER_CHANNEL_PERFORMANCE = f"{MINIO_SILVER_BUCKET}/channel_performance"
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_GOLD_MARKETING_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/marketing_performance"

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

def get_date_ranges(process_date):
    """Get various date ranges for the analysis"""
    # Daily: just the process date
    daily_date = process_date
    
    # Weekly: 7 days ending on process date
    weekly_start = process_date - timedelta(days=6)
    weekly_end = process_date
    
    # Monthly: 30 days ending on process date
    monthly_start = process_date - timedelta(days=29)
    monthly_end = process_date
    
    # Quarterly: 90 days ending on process date
    quarterly_start = process_date - timedelta(days=89)
    quarterly_end = process_date
    
    return {
        "daily": (daily_date, daily_date),
        "weekly": (weekly_start, weekly_end),
        "monthly": (monthly_start, monthly_end),
        "quarterly": (quarterly_start, quarterly_end)
    }

def load_data_from_path(path, description):
    """Try to load data from the given path with fallback to local path"""
    try:
        df = spark.read.parquet(path)
        print(f"Loaded {df.count()} {description} records from {path}")
        return df
    except Exception as e:
        print(f"Error reading {description} from MinIO: {e}")
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

def load_attribution_data():
    """Load marketing attribution data from Silver"""
    return load_data_from_path(MINIO_SILVER_ATTRIBUTION, "attribution")

def load_channel_performance_data():
    """Load channel performance data from Silver"""
    return load_data_from_path(MINIO_SILVER_CHANNEL_PERFORMANCE, "channel performance")

def load_orders_data():
    """Load orders data from Silver"""
    return load_data_from_path(MINIO_SILVER_ORDERS, "orders")

def load_ad_data():
    """Load advertising data from Silver"""
    google_ads = load_data_from_path(MINIO_SILVER_GOOGLE_ADS, "Google Ads")
    social_ads = load_data_from_path(MINIO_SILVER_SOCIAL_ADS, "Social Media Ads")
    influencer = load_data_from_path(MINIO_SILVER_INFLUENCER, "Influencer")
    
    return {
        "google_ads": google_ads,
        "social_ads": social_ads, 
        "influencer": influencer
    }

def create_time_period_metrics(attribution_data, channel_performance, date_ranges):
    """Create metrics aggregated by different time periods"""
    print("Creating time period marketing metrics")
    
    if attribution_data is None:
        print("Missing attribution data for time period metrics")
        return None
    
    time_period_metrics = []
    
    # Process each time period
    for period_name, (start_date, end_date) in date_ranges.items():
        print(f"Processing {period_name} metrics from {start_date} to {end_date}")
        
        # Convert dates to strings for filtering
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter attribution data for this period
        period_attribution = attribution_data \
            .filter(
                (to_date(col("order_date")) >= start_date_str) & 
                (to_date(col("order_date")) <= end_date_str)
            )
        
        # Aggregate by model, source, medium, campaign
        period_metrics = period_attribution \
            .groupBy("attribution_model", "utm_source", "utm_medium", "utm_campaign") \
            .agg(
                count("order_id").alias("conversions"),
                sum("attributed_revenue").alias("revenue"),
                expr("COUNT(DISTINCT order_id)").alias("unique_orders"),
                expr("COUNT(DISTINCT user_id)").alias("unique_users")
            ) \
            .withColumn("time_period", lit(period_name)) \
            .withColumn("start_date", lit(start_date_str)) \
            .withColumn("end_date", lit(end_date_str))
        
        # If channel performance data is available, join to get cost data
        if channel_performance is not None:
            try:
                # Join with channel performance for cost data
                period_metrics = period_metrics \
                    .join(
                        channel_performance.select(
                            "attribution_model", "utm_source", "utm_medium", "utm_campaign",
                            "total_cost", "total_impressions", "total_clicks"
                        ),
                        ["attribution_model", "utm_source", "utm_medium", "utm_campaign"],
                        "left"
                    ) \
                    .withColumn("roas", when(col("total_cost").isNotNull() & (col("total_cost") > 0), 
                        col("revenue") / col("total_cost")).otherwise(None)
                    ) \
                    .withColumn("roi", when(col("total_cost").isNotNull() & (col("total_cost") > 0), 
                        (col("revenue") - col("total_cost")) / col("total_cost")).otherwise(None)
                    ) \
                    .withColumn("cpa", when(col("conversions") > 0,
                        col("total_cost") / col("conversions")).otherwise(None)
                    ) \
                    .withColumn("cpc", when(col("total_clicks") > 0, 
                        col("total_cost") / col("total_clicks")).otherwise(None)
                    ) \
                    .withColumn("conversion_rate", when(col("total_clicks") > 0,
                        col("conversions") / col("total_clicks")).otherwise(None)
                    )
            except Exception as e:
                print(f"Error joining with channel performance: {e}")
                # Continue without cost metrics
                period_metrics = period_metrics \
                    .withColumn("total_cost", lit(None).cast("double")) \
                    .withColumn("total_impressions", lit(None).cast("long")) \
                    .withColumn("total_clicks", lit(None).cast("long")) \
                    .withColumn("roas", lit(None).cast("double")) \
                    .withColumn("roi", lit(None).cast("double")) \
                    .withColumn("cpa", lit(None).cast("double")) \
                    .withColumn("cpc", lit(None).cast("double")) \
                    .withColumn("conversion_rate", lit(None).cast("double"))
        else:
            # Add empty columns for cost metrics
            period_metrics = period_metrics \
                .withColumn("total_cost", lit(None).cast("double")) \
                .withColumn("total_impressions", lit(None).cast("long")) \
                .withColumn("total_clicks", lit(None).cast("long")) \
                .withColumn("roas", lit(None).cast("double")) \
                .withColumn("roi", lit(None).cast("double")) \
                .withColumn("cpa", lit(None).cast("double")) \
                .withColumn("cpc", lit(None).cast("double")) \
                .withColumn("conversion_rate", lit(None).cast("double"))
        
        print(f"Created {period_metrics.count()} records for {period_name} period")
        time_period_metrics.append(period_metrics)
    
    # Union all time periods
    if time_period_metrics:
        all_periods = time_period_metrics[0]
        for df in time_period_metrics[1:]:
            all_periods = all_periods.union(df)
        
        print(f"Created {all_periods.count()} time period metric records in total")
        return all_periods
    else:
        print("No time period metrics created")
        return None

def create_channel_rankings(channel_metrics):
    """Create channel rankings based on performance metrics"""
    print("Creating channel rankings")
    
    if channel_metrics is None:
        print("Missing channel metrics for rankings")
        return None
    
    try:
        # Define ranking windows for different metrics
        # Each window partitions by time period and attribution model
        # to create rankings within each group
        roi_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("roi").desc_nulls_last())
        roas_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("roas").desc_nulls_last())
        revenue_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("revenue").desc_nulls_last())
        conversion_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("conversions").desc_nulls_last())
        
        # Add various rankings
        channel_rankings = channel_metrics \
            .withColumn("roi_rank", dense_rank().over(roi_window)) \
            .withColumn("roas_rank", dense_rank().over(roas_window)) \
            .withColumn("revenue_rank", dense_rank().over(revenue_window)) \
            .withColumn("conversion_rank", dense_rank().over(conversion_window))
        
        # Create an overall performance score (average of rankings)
        channel_rankings = channel_rankings \
            .withColumn("performance_score", 
                (col("roi_rank") + col("roas_rank") + col("revenue_rank") + col("conversion_rank")) / 4
            )
        
        # Create performance tier based on score
        channel_rankings = channel_rankings \
            .withColumn("performance_tier", 
                when(col("performance_score") <= 3, "Top Performer")
                .when(col("performance_score") <= 7, "Strong Performer")
                .when(col("performance_score") <= 12, "Average Performer")
                .otherwise("Under Performer")
            )
        
        print(f"Added rankings to {channel_rankings.count()} channel records")
        return channel_rankings
        
    except Exception as e:
        print(f"Error creating channel rankings: {e}")
        return channel_metrics  # Return original metrics if rankings fail

def create_trend_analysis(attribution_data, date_ranges):
    """Create trend analysis to track performance changes over time"""
    print("Creating trend analysis")
    
    if attribution_data is None:
        print("Missing attribution data for trend analysis")
        return None
    
    try:
        # We'll analyze the weekly and monthly periods for trends
        periods_to_analyze = ["weekly", "monthly"]
        trend_metrics = []
        
        for period_name in periods_to_analyze:
            start_date, end_date = date_ranges[period_name]
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Get previous period for comparison
            if period_name == "weekly":
                prev_start_date = start_date - timedelta(days=7)
                prev_end_date = end_date - timedelta(days=7)
            else:  # monthly
                prev_start_date = start_date - timedelta(days=30)
                prev_end_date = end_date - timedelta(days=30)
            
            prev_start_date_str = prev_start_date.strftime('%Y-%m-%d')
            prev_end_date_str = prev_end_date.strftime('%Y-%m-%d')
            
            # Calculate current period metrics
            current_metrics = attribution_data \
                .filter(
                    (to_date(col("order_date")) >= start_date_str) & 
                    (to_date(col("order_date")) <= end_date_str)
                ) \
                .groupBy("attribution_model", "utm_source", "utm_medium", "utm_campaign") \
                .agg(
                    count("order_id").alias("current_conversions"),
                    sum("attributed_revenue").alias("current_revenue")
                )
            
            # Calculate previous period metrics
            previous_metrics = attribution_data \
                .filter(
                    (to_date(col("order_date")) >= prev_start_date_str) & 
                    (to_date(col("order_date")) <= prev_end_date_str)
                ) \
                .groupBy("attribution_model", "utm_source", "utm_medium", "utm_campaign") \
                .agg(
                    count("order_id").alias("previous_conversions"),
                    sum("attributed_revenue").alias("previous_revenue")
                )
            
            # Join current and previous period metrics
            period_trends = current_metrics \
                .join(
                    previous_metrics,
                    ["attribution_model", "utm_source", "utm_medium", "utm_campaign"],
                    "full_outer"
                ) \
                .na.fill(0, ["current_conversions", "current_revenue", 
                            "previous_conversions", "previous_revenue"])
            
            # Calculate trend metrics
            period_trends = period_trends \
                .withColumn("conversion_change", 
                    col("current_conversions") - col("previous_conversions")
                ) \
                .withColumn("conversion_change_pct",
                    when(col("previous_conversions") > 0,
                        (col("current_conversions") - col("previous_conversions")) / col("previous_conversions") * 100
                    ).otherwise(
                        when(col("current_conversions") > 0, lit(100)).otherwise(lit(0))
                    )
                ) \
                .withColumn("revenue_change",
                    col("current_revenue") - col("previous_revenue")
                ) \
                .withColumn("revenue_change_pct",
                    when(col("previous_revenue") > 0,
                        (col("current_revenue") - col("previous_revenue")) / col("previous_revenue") * 100
                    ).otherwise(
                        when(col("current_revenue") > 0, lit(100)).otherwise(lit(0))
                    )
                ) \
                .withColumn("time_period", lit(period_name)) \
                .withColumn("current_start_date", lit(start_date_str)) \
                .withColumn("current_end_date", lit(end_date_str)) \
                .withColumn("previous_start_date", lit(prev_start_date_str)) \
                .withColumn("previous_end_date", lit(prev_end_date_str))
            
            # Classify the trend
            period_trends = period_trends \
                .withColumn("revenue_trend", 
                    when(col("revenue_change_pct") >= 20, "Strong Growth")
                    .when(col("revenue_change_pct") >= 5, "Growth")
                    .when(col("revenue_change_pct") >= -5, "Stable")
                    .when(col("revenue_change_pct") >= -20, "Decline")
                    .otherwise("Strong Decline")
                )
            
            print(f"Created {period_trends.count()} trend metrics for {period_name} period")
            trend_metrics.append(period_trends)
        
        # Union all trend metrics
        if trend_metrics:
            all_trends = trend_metrics[0]
            for df in trend_metrics[1:]:
                all_trends = all_trends.union(df)
            
            print(f"Created {all_trends.count()} trend metric records in total")
            return all_trends
        else:
            print("No trend metrics created")
            return None
            
    except Exception as e:
        print(f"Error creating trend analysis: {e}")
        return None

def create_channel_comparison(attribution_data, channel_performance):
    """Create cross-channel comparison metrics"""
    print("Creating channel comparison")
    
    if attribution_data is None:
        print("Missing attribution data for channel comparison")
        return None
    
    try:
        # Group by attribution model, source, and medium
        channel_metrics = attribution_data \
            .groupBy("attribution_model", "utm_source", "utm_medium") \
            .agg(
                count("order_id").alias("conversions"),
                sum("attributed_revenue").alias("revenue"),
                expr("COUNT(DISTINCT utm_campaign)").alias("campaign_count"),
                expr("COUNT(DISTINCT order_id)").alias("unique_orders"),
                expr("COUNT(DISTINCT user_id)").alias("unique_users")
            )
        
        # If channel performance data is available, join to get cost data
        if channel_performance is not None:
            # Aggregate channel performance to source/medium level
            channel_costs = channel_performance \
                .groupBy("attribution_model", "utm_source", "utm_medium") \
                .agg(
                    sum("total_cost").alias("total_cost"),
                    sum("total_impressions").alias("total_impressions"),
                    sum("total_clicks").alias("total_clicks")
                )
            
            # Join with channel performance for cost data
            channel_metrics = channel_metrics \
                .join(
                    channel_costs,
                    ["attribution_model", "utm_source", "utm_medium"],
                    "left"
                ) \
                .withColumn("roas", when(col("total_cost").isNotNull() & (col("total_cost") > 0), 
                    col("revenue") / col("total_cost")).otherwise(None)
                ) \
                .withColumn("roi", when(col("total_cost").isNotNull() & (col("total_cost") > 0), 
                    (col("revenue") - col("total_cost")) / col("total_cost")).otherwise(None)
                ) \
                .withColumn("cpa", when(col("conversions") > 0,
                    col("total_cost") / col("conversions")).otherwise(None)
                ) \
                .withColumn("cpc", when(col("total_clicks") > 0, 
                    col("total_cost") / col("total_clicks")).otherwise(None)
                ) \
                .withColumn("conversion_rate", when(col("total_clicks") > 0,
                    col("conversions") / col("total_clicks")).otherwise(None)
                )
        else:
            # Add empty columns for cost metrics
            channel_metrics = channel_metrics \
                .withColumn("total_cost", lit(None).cast("double")) \
                .withColumn("total_impressions", lit(None).cast("long")) \
                .withColumn("total_clicks", lit(None).cast("long")) \
                .withColumn("roas", lit(None).cast("double")) \
                .withColumn("roi", lit(None).cast("double")) \
                .withColumn("cpa", lit(None).cast("double")) \
                .withColumn("cpc", lit(None).cast("double")) \
                .withColumn("conversion_rate", lit(None).cast("double"))
        
        # Calculate channel metrics percentages (market share)
        # Create a window for the attribution model
        model_window = Window.partitionBy("attribution_model")
        
        # Add percentage metrics
        channel_metrics = channel_metrics \
            .withColumn("channel_revenue_percentage", 
                when(sum("revenue").over(model_window) > 0,
                    col("revenue") / sum("revenue").over(model_window) * 100
                ).otherwise(0)
            ) \
            .withColumn("channel_conversion_percentage",
                when(sum("conversions").over(model_window) > 0,
                    col("conversions") / sum("conversions").over(model_window) * 100
                ).otherwise(0)
            ) \
            .withColumn("channel_cost_percentage",
                when(
                    (col("total_cost").isNotNull()) & 
                    (sum("total_cost").over(model_window) > 0),
                    col("total_cost") / sum("total_cost").over(model_window) * 100
                ).otherwise(0)
            )
            
        # Calculate channel efficiency indices
        # Higher value = more efficient than average
        channel_metrics = channel_metrics \
            .withColumn("channel_efficiency_index",
                when(
                    (col("channel_revenue_percentage") > 0) & 
                    (col("channel_cost_percentage") > 0),
                    col("channel_revenue_percentage") / col("channel_cost_percentage")
                ).otherwise(
                    when(col("channel_revenue_percentage") > 0, lit(999)).otherwise(lit(0))
                )
            )
        
        print(f"Created {channel_metrics.count()} channel comparison metrics")
        return channel_metrics
        
    except Exception as e:
        print(f"Error creating channel comparison: {e}")
        return None

def save_to_gold(dataframe, path, partition_cols):
    """Save dataframe to Gold zone with error handling and partitioning"""
    if dataframe is None:
        print(f"No data to save to {path}")
        return False
    
    try:
        # Write to Gold zone
        dataframe.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .save(path)
        
        print(f"Successfully wrote {dataframe.count()} records to Gold at {path}")
        return True
    except Exception as e:
        print(f"Error writing to Gold at {path}: {e}")
        
        # Try local filesystem as fallback
        try:
            local_path = f"/data{path[path.find('/', 5):]}"
            print(f"Attempting to write to local path: {local_path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            dataframe.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy(*partition_cols) \
                .save(local_path)
            
            print(f"Successfully wrote {dataframe.count()} records to local Gold at {local_path}")
            return True
        except Exception as e2:
            print(f"Error writing to local Gold: {e2}")
            return False

def create_marketing_performance_gold(process_date):
    """Main function to create Gold-level marketing performance analytics"""
    print(f"Creating marketing performance Gold layer for {process_date.strftime('%Y-%m-%d')}")
    
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    attribution_data = load_attribution_data()
    channel_performance = load_channel_performance_data()
    orders_data = load_orders_data()
    ad_data = load_ad_data()
    
    # Process data and create gold tables
    
    # 1. Create time period metrics
    time_period_metrics = create_time_period_metrics(attribution_data, channel_performance, date_ranges)
    
    # 2. Create channel rankings
    channel_rankings = create_channel_rankings(time_period_metrics)
    
    # 3. Create trend analysis
    trend_analysis = create_trend_analysis(attribution_data, date_ranges)
    
    # 4. Create channel comparison
    channel_comparison = create_channel_comparison(attribution_data, channel_performance)
    
    # Add processing date information to all datasets
    processing_year = process_date.year
    processing_month = process_date.month
    processing_day = process_date.day
    
    # Format for partition paths
    processing_date_str = process_date.strftime('%Y-%m-%d')
    
    success = True
    
    # Save time period metrics to Gold
    if channel_rankings is not None:
        channel_rankings = channel_rankings \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            channel_rankings,
            f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_rankings",
            ["time_period", "attribution_model", "year", "month"]
        )
    
    # Save trend analysis to Gold
    if trend_analysis is not None:
        trend_analysis = trend_analysis \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            trend_analysis,
            f"{MINIO_GOLD_MARKETING_PERFORMANCE}/trend_analysis",
            ["time_period", "attribution_model", "year", "month"]
        )
    
    # Save channel comparison to Gold
    if channel_comparison is not None:
        channel_comparison = channel_comparison \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            channel_comparison,
            f"{MINIO_GOLD_MARKETING_PERFORMANCE}/channel_comparison",
            ["attribution_model", "year", "month"]
        )
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Create marketing performance Gold layer
    success = create_marketing_performance_gold(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)