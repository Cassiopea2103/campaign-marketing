"""
marketing_performance.py - Spark script to create marketing performance analytics for Gold layer

This script:
1. Reads attribution data, orders, and advertising costs from Silver
2. Creates aggregated marketing performance metrics
3. Calculates ROI, ROAS, and efficiency metrics
4. Writes aggregated data to the Gold zone for BI/reporting

Usage:
    spark-submit marketing_performance.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, 
    datediff, date_format, to_date, current_date, 
    expr, round, lag, lead, row_number, rank, dense_rank
)
from pyspark.sql.window import Window
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Marketing Performance Analytics") \
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

def load_attribution_data():
    """Load marketing attribution data from Silver"""
    try:
        attribution_data = spark.read.parquet("/data/silver/channel_performance")
        print(f"Loaded {attribution_data.count()} attribution data records from Silver")
        return attribution_data
    except Exception as e:
        print(f"Error loading attribution data: {e}")
        return None

def load_channel_spend_data():
    """Load channel spend data from Silver"""
    try:
        # Combine all ad platform data
        google_ads = spark.read.parquet("/data/silver/google_ads")
        social_ads = spark.read.parquet("/data/silver/social_ads")
        influencer = spark.read.parquet("/data/silver/influencer")
        
        # Create a unified view with standardized schema
        google_spend = google_ads.select(
            col("date"), 
            col("platform"),
            col("campaign_id"),
            col("campaign_name"),
            col("ad_type"),
            col("cost").alias("spend"),
            col("impressions"),
            col("clicks"),
            col("conversions")
        )
        
        social_spend = social_ads.select(
            col("date"), 
            col("platform"),
            col("campaign_id"),
            col("campaign_name"),
            col("ad_type"),
            col("cost").alias("spend"),
            col("impressions"),
            col("clicks"),
            col("conversions")
        )
        
        influencer_spend = influencer.select(
            col("date"),
            lit("Influencer").alias("platform"),
            col("campaign_id"),
            col("campaign_name"),
            col("content_type").alias("ad_type"),
            col("fee").alias("spend"),
            col("impressions"),
            col("clicks"),
            col("conversions")
        )
        
        # Union all spend data
        all_spend = google_spend.union(social_spend).union(influencer_spend)
        
        spend_count = all_spend.count()
        print(f"Loaded {spend_count} channel spend records")
        
        return all_spend
    except Exception as e:
        print(f"Error loading channel spend data: {e}")
        return None

def load_order_data():
    """Load order data from Silver"""
    try:
        orders = spark.read.parquet("/data/silver/orders")
        print(f"Loaded {orders.count()} order records from Silver")
        return orders
    except Exception as e:
        print(f"Error loading order data: {e}")
        return None

def create_time_period_metrics(attribution_data, spend_data, date_ranges):
    """Create metrics aggregated by different time periods"""
    if attribution_data is None or spend_data is None:
        print("Missing required data for time period metrics")
        return None
    
    time_period_metrics = []
    
    # Process each time period
    for period_name, (start_date, end_date) in date_ranges.items():
        print(f"Processing {period_name} metrics from {start_date} to {end_date}")
        
        # Convert dates to strings for filtering
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter attribution data for this period - use attribution_date
        period_attribution = attribution_data.filter(
            (col("attribution_date") >= start_date_str) & 
            (col("attribution_date") <= end_date_str)
        )
        
        # Filter spend data for this period
        period_spend = spend_data.filter(
            (to_date(col("date")) >= start_date_str) & 
            (to_date(col("date")) <= end_date_str)
        )
        
        # Aggregate attribution by channel and model
        channel_metrics = period_attribution.groupBy(
            "attribution_model", "utm_source", "utm_medium", "utm_campaign"
        ).agg(
            sum("attributed_revenue").alias("revenue"),
            count("conversions").alias("conversions"),
            count("unique_orders").alias("orders"),
            count("unique_users").alias("users")
        )
        
        # Aggregate spend by channel
        channel_spend = period_spend.groupBy(
            "platform", "campaign_id", "campaign_name"
        ).agg(
            sum("spend").alias("cost"),
            sum("impressions").alias("impressions"),
            sum("clicks").alias("clicks"),
            sum("conversions").alias("tracked_conversions")
        )
        
        # Join attribution with spend
        channel_performance = channel_metrics.join(
            channel_spend,
            (channel_metrics["utm_campaign"] == channel_spend["campaign_name"]),
            "left"
        ).select(
            channel_metrics["attribution_model"],
            channel_metrics["utm_source"],
            channel_metrics["utm_medium"], 
            channel_metrics["utm_campaign"],
            channel_spend["platform"],
            channel_spend["campaign_id"],
            channel_metrics["revenue"],
            channel_metrics["conversions"],
            channel_metrics["orders"],
            channel_metrics["users"],
            channel_spend["cost"],
            channel_spend["impressions"],
            channel_spend["clicks"],
            channel_spend["tracked_conversions"]
        )
        
        # Calculate performance metrics
        channel_performance = channel_performance.withColumn("time_period", lit(period_name)) \
            .withColumn("start_date", lit(start_date_str)) \
            .withColumn("end_date", lit(end_date_str)) \
            .withColumn("roas", when(col("cost") > 0, col("revenue") / col("cost")).otherwise(None)) \
            .withColumn("roi", when(col("cost") > 0, (col("revenue") - col("cost")) / col("cost")).otherwise(None)) \
            .withColumn("cpa", when(col("conversions") > 0, col("cost") / col("conversions")).otherwise(None)) \
            .withColumn("cpc", when(col("clicks") > 0, col("cost") / col("clicks")).otherwise(None)) \
            .withColumn("cvr", when(col("clicks") > 0, col("conversions") / col("clicks")).otherwise(None))
        
        time_period_metrics.append(channel_performance)
    
    # Union all time periods
    if time_period_metrics:
        all_periods = time_period_metrics[0]
        for df in time_period_metrics[1:]:
            all_periods = all_periods.union(df)
        
        print(f"Created {all_periods.count()} time period metric records")
        return all_periods
    else:
        print("No time period metrics created")
        return None

def create_channel_rankings(channel_metrics):
    """Create channel rankings based on performance metrics"""
    if channel_metrics is None:
        print("Missing channel metrics for rankings")
        return None
    
    # Define ranking windows
    roi_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("roi").desc_nulls_last())
    roas_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("roas").desc_nulls_last())
    revenue_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("revenue").desc_nulls_last())
    conversion_window = Window.partitionBy("time_period", "attribution_model").orderBy(col("conversions").desc_nulls_last())
    
    # Add rankings
    channel_rankings = channel_metrics \
        .withColumn("roi_rank", dense_rank().over(roi_window)) \
        .withColumn("roas_rank", dense_rank().over(roas_window)) \
        .withColumn("revenue_rank", dense_rank().over(revenue_window)) \
        .withColumn("conversion_rank", dense_rank().over(conversion_window))
    
    # Add overall performance score (average of all ranks)
    channel_rankings = channel_rankings \
        .withColumn("performance_score", 
            (col("roi_rank") + col("roas_rank") + col("revenue_rank") + col("conversion_rank")) / 4
        )
    
    # Add performance tier based on score
    channel_rankings = channel_rankings \
        .withColumn("performance_tier", 
            when(col("performance_score") <= 3, "Top Performer")
            .when(col("performance_score") <= 7, "Strong Performer")
            .when(col("performance_score") <= 12, "Average Performer")
            .otherwise("Under Performer")
        )
    
    return channel_rankings

def create_marketing_performance_gold(process_date):
    """Create Gold-level marketing performance analytics"""
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    attribution_data = load_attribution_data()
    spend_data = load_channel_spend_data()
    order_data = load_order_data()
    
    # Create time period metrics
    time_period_metrics = create_time_period_metrics(attribution_data, spend_data, date_ranges)
    
    # Create channel rankings
    channel_rankings = create_channel_rankings(time_period_metrics)
    
    # Save to Gold zone
    if channel_rankings is not None:
        processing_year = process_date.year
        processing_month = process_date.month
        processing_day = process_date.day
        
        # Add processing date
        channel_rankings = channel_rankings \
            .withColumn("processing_date", lit(process_date.strftime('%Y-%m-%d'))) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        # Write to Gold with appropriate partitioning
        channel_rankings.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("time_period", "attribution_model", "year", "month") \
            .save("/data/gold/marketing_performance")
        
        print(f"Saved {channel_rankings.count()} marketing performance records to Gold")
        return True
    else:
        print("No marketing performance data to save")
        return False

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    create_marketing_performance_gold(process_date)