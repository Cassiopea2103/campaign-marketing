"""
campaign_roi.py - Spark script to analyze campaign ROI for Gold layer

This script:
1. Processes marketing attribution and ad spend data from Silver
2. Calculates ROI, ROAS, and other performance metrics by campaign
3. Creates profitability analysis across channels and campaigns
4. Analyzes campaign efficiency and cost metrics
5. Writes results to the Gold zone for reporting and dashboards

Usage:
    spark-submit campaign_roi.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, 
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank, lag, first, coalesce,
    countDistinct, year, month, dayofmonth
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Campaign ROI Analytics") \
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
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_SILVER_ALL_PLATFORMS = f"{MINIO_SILVER_BUCKET}/all_platforms"
MINIO_GOLD_CAMPAIGN_ROI = f"{MINIO_GOLD_BUCKET}/campaign_roi"

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
    
    # Year to date: from beginning of year to process date
    ytd_start = datetime(process_date.year, 1, 1)
    ytd_end = process_date
    
    return {
        "daily": (daily_date, daily_date),
        "weekly": (weekly_start, weekly_end),
        "monthly": (monthly_start, monthly_end),
        "quarterly": (quarterly_start, quarterly_end),
        "ytd": (ytd_start, ytd_end)
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

def load_customers_data():
    """Load customers data from Silver"""
    return load_data_from_path(MINIO_SILVER_CUSTOMERS, "customers")

def load_ad_data():
    """Load advertising data from Silver"""
    google_ads = load_data_from_path(MINIO_SILVER_GOOGLE_ADS, "Google Ads")
    social_ads = load_data_from_path(MINIO_SILVER_SOCIAL_ADS, "Social Media Ads")
    influencer = load_data_from_path(MINIO_SILVER_INFLUENCER, "Influencer")
    all_platforms = load_data_from_path(MINIO_SILVER_ALL_PLATFORMS, "All Platforms")
    
    return {
        "google_ads": google_ads,
        "social_ads": social_ads,
        "influencer": influencer,
        "all_platforms": all_platforms
    }

def create_campaign_performance_metrics(attribution_data, ad_data, date_ranges):
    """Create campaign performance metrics by time period"""
    print("Creating campaign performance metrics")

    if 'ytd' in date_ranges:
        print("Temporarily skipping YTD metrics due to performance concerns")
        del date_ranges['ytd']
    
    if attribution_data is None and ad_data is None:
        print("Missing required data for campaign performance metrics")
        return None
    
    try:
        # Process attribution data if available
        campaign_metrics = None
        
        if attribution_data is not None:
            # Get campaign performance from attribution data
            campaign_metrics = attribution_data \
                .filter(col("utm_campaign").isNotNull()) \
                .groupBy("utm_campaign", "attribution_model") \
                .agg(
                    countDistinct("order_id").alias("orders"),
                    countDistinct("user_id").alias("customers"),
                    sum("attributed_revenue").alias("attributed_revenue"),
                    avg("attributed_revenue").alias("avg_order_value")
                )
        
        # Join with ad spend data if available
        if ad_data is not None and "all_platforms" in ad_data and ad_data["all_platforms"] is not None:
            # Aggregate ad spend by campaign
            ad_spend = ad_data["all_platforms"] \
                .filter(col("campaign_name").isNotNull()) \
                .groupBy("campaign_name") \
                .agg(
                    sum("cost").alias("total_cost"),
                    sum("impressions").alias("impressions"),
                    sum("clicks").alias("clicks"),
                    sum("conversions").alias("tracked_conversions"),
                    first("platform").alias("platform"),
                    first("category").alias("category")
                )
            
            # Join with attribution metrics if available
            if campaign_metrics is not None:
                # First normalize campaign names for joining
                campaign_metrics = campaign_metrics.withColumnRenamed("utm_campaign", "campaign_name")
                
                # Join with ad spend
                campaign_performance = campaign_metrics.join(
                    ad_spend,
                    "campaign_name",
                    "full_outer"  # Use full outer join to include all campaigns
                )
            else:
                # Use ad spend data only
                campaign_performance = ad_spend
            
            # Calculate ROI and other performance metrics
            campaign_performance = campaign_performance \
                .withColumn(
                    "roas",
                    when(col("total_cost").isNotNull() & (col("total_cost") > 0) & col("attributed_revenue").isNotNull(),
                        col("attributed_revenue") / col("total_cost")
                    ).otherwise(None)
                ) \
                .withColumn(
                    "roi",
                    when(col("total_cost").isNotNull() & (col("total_cost") > 0) & col("attributed_revenue").isNotNull(),
                        (col("attributed_revenue") - col("total_cost")) / col("total_cost")
                    ).otherwise(None)
                ) \
                .withColumn(
                    "ctr",
                    when(col("impressions").isNotNull() & (col("impressions") > 0) & col("clicks").isNotNull(),
                        col("clicks") / col("impressions") * 100
                    ).otherwise(None)
                ) \
                .withColumn(
                    "cpc",
                    when(col("clicks").isNotNull() & (col("clicks") > 0) & col("total_cost").isNotNull(),
                        col("total_cost") / col("clicks")
                    ).otherwise(None)
                ) \
                .withColumn(
                    "cpa",
                    when(col("orders").isNotNull() & (col("orders") > 0) & col("total_cost").isNotNull(),
                        col("total_cost") / col("orders")
                    ).when(col("tracked_conversions").isNotNull() & (col("tracked_conversions") > 0) & col("total_cost").isNotNull(),
                        col("total_cost") / col("tracked_conversions")
                    ).otherwise(None)
                ) \
                .withColumn(
                    "conversion_rate",
                    when(col("clicks").isNotNull() & (col("clicks") > 0) & col("orders").isNotNull(),
                        col("orders") / col("clicks") * 100
                    ).when(col("clicks").isNotNull() & (col("clicks") > 0) & col("tracked_conversions").isNotNull(),
                        col("tracked_conversions") / col("clicks") * 100
                    ).otherwise(None)
                )
        else:
            # Just use attribution data
            campaign_performance = campaign_metrics.withColumnRenamed("utm_campaign", "campaign_name")
            
        # Calculate time period metrics
        time_period_metrics = []
        
        for period_name, (start_date, end_date) in date_ranges.items():
            if period_name == "quarterly":
                print(f"Skipping {period_name} metrics due to performance concerns")
                continue
    
            print(f"Processing {period_name} metrics from {start_date} to {end_date}")
            
            # Convert dates to strings for filtering
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            # Pre-filter the data before expensive operations
            if attribution_data is not None:
                filtered_attribution = attribution_data \
                    .filter(
                        (to_date(col("order_date")) >= start_date_str) & 
                        (to_date(col("order_date")) <= end_date_str) &
                        col("utm_campaign").isNotNull()
                    )
            
            # Filter attribution data for this period
            import pyspark.sql.functions as F
            period_attribution = filtered_attribution \
                .filter(
                    (to_date(col("order_date")) >= start_date_str) & 
                    (to_date(col("order_date")) <= end_date_str) &
                    col("utm_campaign").isNotNull()
                ) \
                .groupBy("utm_campaign", "attribution_model") \
                .agg(
                    F.approx_count_distinct("order_id").alias("period_orders"),
                    F.approx_count_distinct("user_id").alias("period_customers"),
                    sum("attributed_revenue").alias("period_revenue"),
                    avg("attributed_revenue").alias("period_aov")
                )
            
            # Filter ad spend data for this period
            period_ad_spend = None
            if ad_data is not None and "all_platforms" in ad_data and ad_data["all_platforms"] is not None:
                period_ad_spend = ad_data["all_platforms"] \
                    .filter(
                        (to_date(col("ad_date")) >= start_date_str) & 
                        (to_date(col("ad_date")) <= end_date_str) &
                        col("campaign_name").isNotNull()
                    ) \
                    .groupBy("campaign_name") \
                    .agg(
                        sum("cost").alias("period_cost"),
                        sum("impressions").alias("period_impressions"),
                        sum("clicks").alias("period_clicks"),
                        sum("conversions").alias("period_tracked_conversions"),
                        first("platform").alias("platform"),
                        first("category").alias("category")
                    )
            
            # Join period data
            from pyspark.sql.functions import broadcast

            if period_attribution is not None and period_ad_spend is not None:
                # Rename for consistent joining
                period_attribution = period_attribution.withColumnRenamed("utm_campaign", "campaign_name")
                

                
                # Join attribution and ad spend
                period_metrics = period_attribution.join(
                    period_ad_spend,
                    ["campaign_name"],
                    "full_outer"
                )
            elif period_attribution is not None:
                period_metrics = period_attribution.withColumnRenamed("utm_campaign", "campaign_name")
            elif period_ad_spend is not None:
                period_metrics = period_ad_spend
            else:
                # Skip this period if no data
                continue
            
            # Calculate period performance metrics
            period_metrics = period_metrics \
                .withColumn(
                    "period_roas",
                    when(col("period_cost").isNotNull() & (col("period_cost") > 0) & col("period_revenue").isNotNull(),
                        col("period_revenue") / col("period_cost")
                    ).otherwise(None)
                ) \
                .withColumn(
                    "period_roi",
                    when(col("period_cost").isNotNull() & (col("period_cost") > 0) & col("period_revenue").isNotNull(),
                        (col("period_revenue") - col("period_cost")) / col("period_cost")
                    ).otherwise(None)
                ) \
                .withColumn("time_period", lit(period_name)) \
                .withColumn("start_date", lit(start_date_str)) \
                .withColumn("end_date", lit(end_date_str))
            
            time_period_metrics.append(period_metrics)
        
        # Combine all time periods
        if time_period_metrics:
            all_periods = time_period_metrics[0]
            for df in time_period_metrics[1:]:
                all_periods = all_periods.unionByName(df, allowMissingColumns=True)
            
            print(f"Created {all_periods.count()} time period campaign metrics")
            
            # Join period metrics with overall campaign performance
            campaign_roi = campaign_performance.join(
                all_periods.select(
                    "campaign_name",
                    col("attribution_model").alias("period_attribution_model"),  # Rename to avoid ambiguity
                    "time_period",
                    "start_date",
                    "end_date",
                    "period_revenue",
                    "period_cost",
                    "period_orders",
                    "period_customers",
                    "period_impressions",
                    "period_clicks",
                    "period_tracked_conversions",
                    "period_roas",
                    "period_roi"
                ),
                ["campaign_name"],
                "left_outer"
            )
            
            # Calculate performance rank
            ## Specify the source of attribution_model
            roi_window = Window.partitionBy("time_period", "period_attribution_model").orderBy(col("period_roi").desc_nulls_last())
            #roi_window = Window.partitionBy("time_period", "all_periods.attribution_model").orderBy(col("period_roi").desc_nulls_last())
            revenue_window = Window.partitionBy("time_period", "period_attribution_model").orderBy(col("period_revenue").desc_nulls_last())
            
            # Add rankings
            campaign_roi = campaign_roi \
                .withColumn("roi_rank", dense_rank().over(roi_window)) \
                .withColumn("revenue_rank", dense_rank().over(revenue_window))
            
            # Add performance classification
            campaign_roi = campaign_roi \
                .withColumn(
                    "roi_classification",
                    when(col("period_roi").isNull(), "Unknown")
                    .when(col("period_roi") >= 2.0, "Excellent (200%+ ROI)")
                    .when(col("period_roi") >= 1.0, "Very Good (100-200% ROI)")
                    .when(col("period_roi") >= 0.5, "Good (50-100% ROI)")
                    .when(col("period_roi") >= 0.0, "Break-even (0-50% ROI)")
                    .when(col("period_roi") >= -0.5, "Poor (-50-0% ROI)")
                    .otherwise("Very Poor (< -50% ROI)")
                )
            
            return campaign_roi
        else:
            return campaign_performance
        
    except Exception as e:
        print(f"Error creating campaign performance metrics: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_channel_campaign_comparison(campaign_roi):
    """Create comparison of campaign performance across different channels"""
    print("Creating channel-campaign comparison")
    
    if campaign_roi is None:
        print("Missing campaign ROI data for channel comparison")
        return None
    
    try:
        # Group by platform/channel and time period
        channel_metrics = campaign_roi \
            .filter(col("platform").isNotNull() & col("time_period").isNotNull()) \
            .groupBy("platform", "time_period", "start_date", "end_date") \
            .agg(
                sum("period_cost").alias("channel_cost"),
                sum("period_revenue").alias("channel_revenue"),
                sum("period_impressions").alias("channel_impressions"),
                sum("period_clicks").alias("channel_clicks"),
                sum("period_orders").alias("channel_orders"),
                count("campaign_name").alias("campaign_count"),
                avg("period_roi").alias("avg_campaign_roi")
            )
        
        # Calculate channel performance metrics
        channel_comparison = channel_metrics \
            .withColumn(
                "channel_roi",
                when(col("channel_cost") > 0,
                    (col("channel_revenue") - col("channel_cost")) / col("channel_cost")
                ).otherwise(None)
            ) \
            .withColumn(
                "channel_ctr",
                when(col("channel_impressions") > 0,
                    col("channel_clicks") / col("channel_impressions") * 100
                ).otherwise(None)
            ) \
            .withColumn(
                "channel_cpa",
                when(col("channel_orders") > 0,
                    col("channel_cost") / col("channel_orders")
                ).otherwise(None)
            ) \
            .withColumn(
                "channel_conversion_rate",
                when(col("channel_clicks") > 0,
                    col("channel_orders") / col("channel_clicks") * 100
                ).otherwise(None)
            )
        
        # Create rankings by ROI within each time period
        roi_window = Window.partitionBy("time_period").orderBy(col("channel_roi").desc_nulls_last())
        
        channel_comparison = channel_comparison \
            .withColumn("roi_rank", dense_rank().over(roi_window))
        
        return channel_comparison
    
    except Exception as e:
        print(f"Error creating channel-campaign comparison: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_campaign_roi_time_trends(campaign_roi):
    """Analyze ROI trends over time for campaigns"""
    print("Creating campaign ROI time trends")
    
    if campaign_roi is None:
        print("Missing campaign ROI data for time trends")
        return None
    
    try:
        # Filter for monthly data with valid ROI
        monthly_data = campaign_roi \
            .filter(
                (col("time_period") == "monthly") & 
                (col("campaign_name").isNotNull()) & 
                (col("period_roi").isNotNull())
            )
        
        
        if monthly_data.count() == 0:
            print("No monthly data available for trend analysis")
            return None
        
        # Get previous month's data (can be simulated since we're processing a single date)
        # In a real-world scenario, this would come from historical data
        
        # For this example, we'll create a derived metric to simulate trend
        roi_trends = monthly_data.select(
            "campaign_name",
            "platform",
            "category",
            "attribution_model",
            "period_roi",
            "period_revenue",
            "period_cost",
            "time_period",
            "start_date",
            "end_date"
        )
        
        # Get top campaigns by ROI
        roi_window = Window.partitionBy("platform").orderBy(col("period_roi").desc())
        
        roi_trends = roi_trends \
            .withColumn("roi_rank_in_channel", dense_rank().over(roi_window)) \
            .withColumn(
                "roi_quality",
                when(col("period_roi") >= 1.0, "High ROI")
                .when(col("period_roi") >= 0, "Positive ROI")
                .when(col("period_roi") >= -0.5, "Negative ROI")
                .otherwise("Poor ROI")
            )
        
        # Calculate ROI distribution by platform
        platform_distribution = roi_trends \
            .groupBy("platform") \
            .agg(
                count("campaign_name").alias("campaign_count"),
                sum(when(col("period_roi") >= 1.0, 1).otherwise(0)).alias("high_roi_campaigns"),
                sum(when((col("period_roi") < 1.0) & (col("period_roi") >= 0), 1).otherwise(0)).alias("positive_roi_campaigns"),
                sum(when((col("period_roi") < 0) & (col("period_roi") >= -0.5), 1).otherwise(0)).alias("negative_roi_campaigns"),
                sum(when(col("period_roi") < -0.5, 1).otherwise(0)).alias("poor_roi_campaigns")
            ) \
            .withColumn(
                "high_roi_percentage", 
                when(col("campaign_count") > 0,
                    col("high_roi_campaigns") / col("campaign_count") * 100
                ).otherwise(0)
            ) \
            .withColumn(
                "positive_roi_percentage", 
                when(col("campaign_count") > 0,
                    col("positive_roi_campaigns") / col("campaign_count") * 100
                ).otherwise(0)
            ) \
            .withColumn(
                "platform_health",
                when(col("high_roi_percentage") >= 50, "Excellent")
                .when(col("high_roi_percentage") >= 30, "Good")
                .when(col("high_roi_percentage") + col("positive_roi_percentage") >= 50, "Average")
                .otherwise("Poor")
            )
        
        # Add time information
        platform_distribution = platform_distribution \
            .withColumn("analysis_period", lit("monthly")) \
            .withColumn("start_date", lit(monthly_data.first()["start_date"])) \
            .withColumn("end_date", lit(monthly_data.first()["end_date"]))
        
        return {
            "roi_trends": roi_trends,
            "platform_distribution": platform_distribution
        }
    
    except Exception as e:
        print(f"Error creating campaign ROI time trends: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_campaign_efficiency_analysis(campaign_roi, orders_data, customers_data):
    """Analyze campaign efficiency metrics"""
    print("Creating campaign efficiency analysis")
    
    if campaign_roi is None:
        print("Missing campaign ROI data for efficiency analysis")
        return None
    
    try:
        # Focus on monthly performance
        monthly_campaigns = campaign_roi.filter(col("time_period") == "monthly")
        
        if monthly_campaigns.count() == 0:
            print("No monthly campaign data available for efficiency analysis")
            return None
        
        # Calculate efficiency metrics
        efficiency_metrics = monthly_campaigns.select(
            "campaign_name",
            "platform",
            "category",
            "attribution_model",
            "period_cost",
            "period_revenue",
            "period_impressions",
            "period_clicks",
            "period_orders",
            "period_customers",
            "period_roi",
            "time_period",
            "start_date",
            "end_date"
        )
        
        # Add calculated metrics
        efficiency_metrics = efficiency_metrics \
            .withColumn(
                "cost_per_impression",
                when(col("period_impressions").isNotNull() & (col("period_impressions") > 0) & col("period_cost").isNotNull(),
                    col("period_cost") / col("period_impressions") * 1000  # CPM (Cost per 1000 impressions)
                ).otherwise(None)
            ) \
            .withColumn(
                "cost_per_click",
                when(col("period_clicks").isNotNull() & (col("period_clicks") > 0) & col("period_cost").isNotNull(),
                    col("period_cost") / col("period_clicks")
                ).otherwise(None)
            ) \
            .withColumn(
                "cost_per_order",
                when(col("period_orders").isNotNull() & (col("period_orders") > 0) & col("period_cost").isNotNull(),
                    col("period_cost") / col("period_orders")
                ).otherwise(None)
            ) \
            .withColumn(
                "cost_per_customer",
                when(col("period_customers").isNotNull() & (col("period_customers") > 0) & col("period_cost").isNotNull(),
                    col("period_cost") / col("period_customers")
                ).otherwise(None)
            ) \
            .withColumn(
                "conversion_rate",
                when(col("period_clicks").isNotNull() & (col("period_clicks") > 0) & col("period_orders").isNotNull(),
                    col("period_orders") / col("period_clicks") * 100
                ).otherwise(None)
            ) \
            .withColumn(
                "revenue_per_click",
                when(col("period_clicks").isNotNull() & (col("period_clicks") > 0) & col("period_revenue").isNotNull(),
                    col("period_revenue") / col("period_clicks")
                ).otherwise(None)
            ) \
            .withColumn(
                "revenue_per_customer",
                when(col("period_customers").isNotNull() & (col("period_customers") > 0) & col("period_revenue").isNotNull(),
                    col("period_revenue") / col("period_customers")
                ).otherwise(None)
            )
        
        # Calculate efficiency scores
        # Lower is better for cost metrics, higher is better for conversion and revenue metrics
        # Normalize scores relative to platform averages
        
        # Calculate platform averages
        platform_avgs = efficiency_metrics \
            .filter(
                col("cost_per_impression").isNotNull() &
                col("cost_per_click").isNotNull() &
                col("cost_per_order").isNotNull() &
                col("conversion_rate").isNotNull()
            ) \
            .groupBy("platform") \
            .agg(
                avg("cost_per_impression").alias("avg_cpm"),
                avg("cost_per_click").alias("avg_cpc"),
                avg("cost_per_order").alias("avg_cpo"),
                avg("conversion_rate").alias("avg_cvr"),
                avg("revenue_per_customer").alias("avg_rpc")
            )
        
        # Join with platform averages
        efficiency_metrics = efficiency_metrics.join(
            platform_avgs,
            "platform",
            "left"
        )
        
        # Calculate relative performance scores (1.0 = average, >1.0 = better than average)
        efficiency_metrics = efficiency_metrics \
            .withColumn(
                "cpm_score",
                when(col("cost_per_impression").isNotNull() & col("avg_cpm").isNotNull() & (col("avg_cpm") > 0),
                    col("avg_cpm") / col("cost_per_impression")  # Lower CPM is better
                ).otherwise(None)
            ) \
            .withColumn(
                "cpc_score",
                when(col("cost_per_click").isNotNull() & col("avg_cpc").isNotNull() & (col("avg_cpc") > 0),
                    col("avg_cpc") / col("cost_per_click")  # Lower CPC is better
                ).otherwise(None)
            ) \
            .withColumn(
                "cpo_score",
                when(col("cost_per_order").isNotNull() & col("avg_cpo").isNotNull() & (col("avg_cpo") > 0),
                    col("avg_cpo") / col("cost_per_order")  # Lower CPO is better
                ).otherwise(None)
            ) \
            .withColumn(
                "cvr_score",
                when(col("conversion_rate").isNotNull() & col("avg_cvr").isNotNull() & (col("avg_cvr") > 0),
                    col("conversion_rate") / col("avg_cvr")  # Higher CVR is better
                ).otherwise(None)
            ) \
            .withColumn(
                "rpc_score",
                when(col("revenue_per_customer").isNotNull() & col("avg_rpc").isNotNull() & (col("avg_rpc") > 0),
                    col("revenue_per_customer") / col("avg_rpc")  # Higher RPC is better
                ).otherwise(None)
            )
        
        # Calculate overall efficiency score (average of individual scores)
        efficiency_metrics = efficiency_metrics \
            .withColumn(
                "efficiency_score",
                (coalesce(col("cpm_score"), lit(1.0)) +
                 coalesce(col("cpc_score"), lit(1.0)) +
                 coalesce(col("cpo_score"), lit(1.0)) +
                 coalesce(col("cvr_score"), lit(1.0)) +
                 coalesce(col("rpc_score"), lit(1.0))) / 5
            )
        
        # Create efficiency rating
        efficiency_metrics = efficiency_metrics \
            .withColumn(
                "efficiency_rating",
                when(col("efficiency_score") >= 1.5, "Excellent")
                .when(col("efficiency_score") >= 1.2, "Good")
                .when(col("efficiency_score") >= 0.8, "Average")
                .when(col("efficiency_score") >= 0.5, "Below Average")
                .otherwise("Poor")
            )
        
        # Add efficiency recommendations
        efficiency_metrics = efficiency_metrics \
            .withColumn(
                "efficiency_focus_area",
                when(col("cpc_score").isNotNull() & (col("cpc_score") < 0.8), "Optimize for lower CPC")
                .when(col("cvr_score").isNotNull() & (col("cvr_score") < 0.8), "Improve conversion rate")
                .when(col("cpm_score").isNotNull() & (col("cpm_score") < 0.8), "Refine targeting for better CPM")
                .when(col("cpo_score").isNotNull() & (col("cpo_score") < 0.8), "Reduce cost per order")
                .when(col("rpc_score").isNotNull() & (col("rpc_score") < 0.8), "Increase revenue per customer")
                .otherwise("Maintain current performance")
            )
        
        return efficiency_metrics
    
    except Exception as e:
        print(f"Error creating campaign efficiency analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_budget_allocation_analysis(campaign_roi):
    """Create optimal budget allocation analysis based on ROI"""
    print("Creating budget allocation analysis")
    
    if campaign_roi is None:
        print("Missing campaign ROI data for budget allocation")
        return None
    
    try:
        # Use monthly performance data with valid ROI and cost
        monthly_data = campaign_roi.filter(
            (col("time_period") == "monthly") & 
            (col("period_roi").isNotNull()) & 
            (col("period_cost").isNotNull())
        )
        
        if monthly_data.count() == 0:
            print("No valid monthly data for budget allocation")
            return None
        
        # Calculate current budget allocation
        total_monthly_cost = monthly_data.agg(sum("period_cost")).collect()[0][0]
        
        if total_monthly_cost is None or total_monthly_cost == 0:
            print("No spending data available for budget allocation")
            return None
        
        # Calculate current budget share and suggested new share based on ROI
        allocation_analysis = monthly_data.select(
            "campaign_name",
            "platform",
            "category",
            "period_cost",
            "period_revenue",
            "period_roi"
        )
        
        # Get total revenue to calculate ROAS contribution
        total_revenue = monthly_data.agg(sum("period_revenue")).collect()[0][0] or 0
        
        allocation_analysis = allocation_analysis \
            .withColumn(
                "current_budget_share",
                when(col("period_cost").isNotNull() & (total_monthly_cost > 0),
                    col("period_cost") / lit(total_monthly_cost) * 100
                ).otherwise(0)
            ) \
            .withColumn(
                "revenue_contribution",
                when(col("period_revenue").isNotNull() & (total_revenue > 0),
                    col("period_revenue") / lit(total_revenue) * 100
                ).otherwise(0)
            )
        
        # Calculate the ROI rank for budget allocation
        roi_window = Window.orderBy(col("period_roi").desc_nulls_last())
        
        allocation_analysis = allocation_analysis \
            .withColumn("roi_rank", dense_rank().over(roi_window))
        
        # Calculate suggested budget shares based on ROI performance
        # Complex budget allocation model would typically be implemented here
        # This is a simplified version where high ROI campaigns get more budget
        allocation_analysis = allocation_analysis \
            .withColumn(
                "budget_reallocation_factor",
                when(col("period_roi") > 2.0, lit(1.5))  # 50% increase for excellent ROI
                .when(col("period_roi") > 1.0, lit(1.25))  # 25% increase for very good ROI
                .when(col("period_roi") > 0.5, lit(1.1))  # 10% increase for good ROI
                .when(col("period_roi") > 0.0, lit(1.0))  # No change for break-even
                .when(col("period_roi") > -0.5, lit(0.75))  # 25% decrease for poor ROI
                .otherwise(lit(0.5))  # 50% decrease for very poor ROI
            )
        
        # Calculate adjusted budget (this is just an estimate, would need normalization in practice)
        allocation_analysis = allocation_analysis \
            .withColumn(
                "adjusted_budget",
                col("period_cost") * col("budget_reallocation_factor")
            )
        
        # Calculate total adjusted budget for normalization
        total_adjusted = allocation_analysis.agg(sum("adjusted_budget")).collect()[0][0] or total_monthly_cost
        
        # Normalize to ensure total budget remains the same
        allocation_analysis = allocation_analysis \
            .withColumn(
                "suggested_budget_share",
                when(total_adjusted > 0,
                    col("adjusted_budget") / lit(total_adjusted) * 100
                ).otherwise(col("current_budget_share"))
            ) \
            .withColumn(
                "suggested_budget_change",
                col("suggested_budget_share") - col("current_budget_share")
            ) \
            .withColumn(
                "allocation_recommendation",
                when(col("suggested_budget_change") > 5, "Significantly Increase Budget")
                .when(col("suggested_budget_change") > 2, "Increase Budget")
                .when(col("suggested_budget_change") < -5, "Significantly Decrease Budget")
                .when(col("suggested_budget_change") < -2, "Decrease Budget")
                .otherwise("Maintain Current Budget")
            )
        
        return allocation_analysis
    
    except Exception as e:
        print(f"Error creating budget allocation analysis: {e}")
        import traceback
        traceback.print_exc()
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

def create_campaign_roi_gold(process_date):
    """Main function to create Gold-level campaign ROI analytics"""
    print(f"Creating campaign ROI Gold layer for {process_date.strftime('%Y-%m-%d')}")
    
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    attribution_data = load_attribution_data()
    ad_data = load_ad_data()
    orders_data = load_orders_data()
    customers_data = load_customers_data()

    if attribution_data is not None:
        attribution_data = attribution_data.repartition(col("order_date"))

    
    # Process data and create gold tables
    
    # 1. Create campaign performance metrics
    campaign_roi = create_campaign_performance_metrics(attribution_data, ad_data, date_ranges)
    
    # 2. Create channel-campaign comparison
    channel_comparison = create_channel_campaign_comparison(campaign_roi)
    
    # 3. Create campaign ROI time trends
    time_trends = create_campaign_roi_time_trends(campaign_roi)
    
    # 4. Create campaign efficiency analysis
    efficiency_analysis = create_campaign_efficiency_analysis(campaign_roi, orders_data, customers_data)
    
    # 5. Create budget allocation analysis
    budget_allocation = create_budget_allocation_analysis(campaign_roi)
    
    # Add processing date information to all datasets
    processing_year = process_date.year
    processing_month = process_date.month
    processing_day = process_date.day
    
    # Format for partition paths
    processing_date_str = process_date.strftime('%Y-%m-%d')
    
    success = True
    
    # Save campaign ROI to Gold
    if campaign_roi is not None:
        campaign_roi = campaign_roi \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            campaign_roi,
            f"{MINIO_GOLD_CAMPAIGN_ROI}/campaign_performance",
            ["time_period", "year", "month"]
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
            f"{MINIO_GOLD_CAMPAIGN_ROI}/channel_comparison",
            ["time_period", "year", "month"]
        )
    
    # Save ROI time trends to Gold
    if time_trends is not None:
        roi_trends = time_trends.get("roi_trends")
        if roi_trends is not None:
            roi_trends = roi_trends \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("year", lit(processing_year)) \
                .withColumn("month", lit(processing_month)) \
                .withColumn("day", lit(processing_day))
            
            success = success and save_to_gold(
                roi_trends,
                f"{MINIO_GOLD_CAMPAIGN_ROI}/roi_trends",
                ["platform", "year", "month"]
            )
        
        platform_distribution = time_trends.get("platform_distribution")
        if platform_distribution is not None:
            platform_distribution = platform_distribution \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("year", lit(processing_year)) \
                .withColumn("month", lit(processing_month)) \
                .withColumn("day", lit(processing_day))
            
            success = success and save_to_gold(
                platform_distribution,
                f"{MINIO_GOLD_CAMPAIGN_ROI}/platform_distribution",
                ["analysis_period", "year", "month"]
            )
    
    # Save efficiency analysis to Gold
    if efficiency_analysis is not None:
        efficiency_analysis = efficiency_analysis \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            efficiency_analysis,
            f"{MINIO_GOLD_CAMPAIGN_ROI}/efficiency_analysis",
            ["platform", "year", "month"]
        )
    
    # Save budget allocation to Gold
    if budget_allocation is not None:
        budget_allocation = budget_allocation \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            budget_allocation,
            f"{MINIO_GOLD_CAMPAIGN_ROI}/budget_allocation",
            ["platform", "year", "month"]
        )
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Create campaign ROI Gold layer
    success = create_campaign_roi_gold(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)