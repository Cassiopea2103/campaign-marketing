"""
create_attribution_models.py - Spark script to create marketing attribution models

This script:
1. Reads web sessions, orders, and advertising data from Silver
2. Creates different attribution models (first-touch, last-touch, linear, time-decay)
3. Calculates ROI and performance metrics for each channel
4. Writes attribution data to Silver zone

Usage:
    spark-submit create_attribution_models.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, row_number, lag, lead, sum, count, avg, max, min, explode,
    datediff, date_add, to_date, current_date, expr, round, concat, array
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Marketing Attribution Models") \
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
MINIO_SILVER_WEB_SESSIONS = f"{MINIO_SILVER_BUCKET}/web_sessions"
MINIO_SILVER_CUSTOMER_SESSIONS = f"{MINIO_SILVER_BUCKET}/customer_sessions"
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_SILVER_ALL_PLATFORMS = f"{MINIO_SILVER_BUCKET}/all_platforms"
MINIO_SILVER_ATTRIBUTION = f"{MINIO_SILVER_BUCKET}/marketing_attribution"
MINIO_SILVER_CHANNEL_PERFORMANCE = f"{MINIO_SILVER_BUCKET}/channel_performance"

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

def get_date_range(process_date, days_back=30):
    """Get date range for processing attribution data"""
    end_date = process_date
    start_date = process_date - timedelta(days=days_back)
    return start_date, end_date

def load_web_sessions(start_date, end_date):
    """Load web sessions from Silver for the date range"""
    print(f"Loading web sessions from {start_date} to {end_date}")
    
    try:
        # Read all sessions from silver
        all_sessions = spark.read.parquet(MINIO_SILVER_WEB_SESSIONS)
        
        # Convert dates to strings for filtering
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter for date range
        filtered_sessions = all_sessions.filter(
            (col("date") >= start_date_str) & 
            (col("date") <= end_date_str)
        )
        
        session_count = filtered_sessions.count()
        print(f"Loaded {session_count} web sessions from Silver")
        
        return filtered_sessions
    except Exception as e:
        print(f"Error loading web sessions from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/web_sessions"
            print(f"Attempting to read from local path: {local_silver_path}")
            
            all_sessions = spark.read.parquet(local_silver_path)
            
            # Convert dates to strings for filtering
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Filter for date range
            filtered_sessions = all_sessions.filter(
                (col("date") >= start_date_str) & 
                (col("date") <= end_date_str)
            )
            
            session_count = filtered_sessions.count()
            print(f"Loaded {session_count} web sessions from local Silver")
            
            return filtered_sessions
        except Exception as e2:
            print(f"Error loading web sessions from local Silver: {e2}")
            return None

def load_customer_sessions(start_date, end_date):
    """Load customer sessions from Silver for the date range"""
    print(f"Loading customer sessions from {start_date} to {end_date}")
    
    try:
        # Read all sessions from silver
        all_sessions = spark.read.parquet(MINIO_SILVER_CUSTOMER_SESSIONS)
        
        # Convert dates to strings for filtering
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter for date range
        filtered_sessions = all_sessions.filter(
            (col("date") >= start_date_str) & 
            (col("date") <= end_date_str)
        )
        
        session_count = filtered_sessions.count()
        print(f"Loaded {session_count} customer sessions from Silver")
        
        return filtered_sessions
    except Exception as e:
        print(f"Error loading customer sessions from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/customer_sessions"
            print(f"Attempting to read from local path: {local_silver_path}")
            
            all_sessions = spark.read.parquet(local_silver_path)
            
            # Convert dates to strings for filtering
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Filter for date range
            filtered_sessions = all_sessions.filter(
                (col("date") >= start_date_str) & 
                (col("date") <= end_date_str)
            )
            
            session_count = filtered_sessions.count()
            print(f"Loaded {session_count} customer sessions from local Silver")
            
            return filtered_sessions
        except Exception as e2:
            print(f"Error loading customer sessions from local Silver: {e2}")
            return None

def load_orders(start_date, end_date):
    """Load orders from Silver for the date range"""
    print(f"Loading orders from {start_date} to {end_date}")
    
    try:
        # Read all orders from silver
        all_orders = spark.read.parquet(MINIO_SILVER_ORDERS)
        
        # Convert dates to strings for comparison
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter for date range
        filtered_orders = all_orders.filter(
            (to_date(col("order_date")) >= start_date_str) &
            (to_date(col("order_date")) <= end_date_str)
        )
        
        order_count = filtered_orders.count()
        print(f"Loaded {order_count} orders from Silver")
        
        return filtered_orders
    except Exception as e:
        print(f"Error loading orders from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/orders"
            print(f"Attempting to read from local path: {local_silver_path}")
            
            all_orders = spark.read.parquet(local_silver_path)
            
            # Convert dates to strings for comparison
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Filter for date range
            filtered_orders = all_orders.filter(
                (to_date(col("order_date")) >= start_date_str) &
                (to_date(col("order_date")) <= end_date_str)
            )
            
            order_count = filtered_orders.count()
            print(f"Loaded {order_count} orders from local Silver")
            
            return filtered_orders
        except Exception as e2:
            print(f"Error loading orders from local Silver: {e2}")
            return None

def load_ad_data(start_date, end_date):
    """Load advertising data from Silver for the date range"""
    print(f"Loading advertising data from {start_date} to {end_date}")
    
    try:
        # Read from silver
        google_ads = spark.read.parquet(MINIO_SILVER_GOOGLE_ADS)
        social_ads = spark.read.parquet(MINIO_SILVER_SOCIAL_ADS)
        influencer = spark.read.parquet(MINIO_SILVER_INFLUENCER)
        all_platforms = spark.read.parquet(MINIO_SILVER_ALL_PLATFORMS)
        
        # Convert dates to strings for comparison
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter for date range
        filtered_google = google_ads.filter(
            (to_date(col("ad_date")) >= start_date_str) &
            (to_date(col("ad_date")) <= end_date_str)
        )
        
        filtered_social = social_ads.filter(
            (to_date(col("ad_date")) >= start_date_str) &
            (to_date(col("ad_date")) <= end_date_str)
        )
        
        filtered_influencer = influencer.filter(
            (to_date(col("ad_date")) >= start_date_str) &
            (to_date(col("ad_date")) <= end_date_str)
        )
        
        filtered_all = all_platforms.filter(
            (to_date(col("ad_date")) >= start_date_str) &
            (to_date(col("ad_date")) <= end_date_str)
        )
        
        # Print counts
        google_count = filtered_google.count()
        social_count = filtered_social.count()
        influencer_count = filtered_influencer.count()
        all_count = filtered_all.count()
        
        print(f"Loaded advertising data: Google={google_count}, Social={social_count}, Influencer={influencer_count}, All Platforms={all_count}")
        
        return {
            "google_ads": filtered_google,
            "social_ads": filtered_social,
            "influencer": filtered_influencer,
            "all_platforms": filtered_all
        }
    except Exception as e:
        print(f"Error loading advertising data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            # Load from local paths
            local_paths = {
                "google_ads": "/data/silver/google_ads",
                "social_ads": "/data/silver/social_ads",
                "influencer": "/data/silver/influencer",
                "all_platforms": "/data/silver/all_platforms"
            }
            
            result = {}
            
            for key, path in local_paths.items():
                print(f"Attempting to read {key} from local path: {path}")
                df = spark.read.parquet(path)
                
                # Convert dates to strings for comparison
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = end_date.strftime('%Y-%m-%d')
                
                # Filter for date range
                if "ad_date" in df.columns:
                    filtered_df = df.filter(
                        (to_date(col("ad_date")) >= start_date_str) &
                        (to_date(col("ad_date")) <= end_date_str)
                    )
                else:
                    # Date column might be different
                    filtered_df = df.filter(
                        (to_date(col("date")) >= start_date_str) &
                        (to_date(col("date")) <= end_date_str)
                    )
                
                result[key] = filtered_df
                print(f"Loaded {filtered_df.count()} {key} records from local Silver")
            
            return result
        except Exception as e2:
            print(f"Error loading advertising data from local Silver: {e2}")
            return None

def create_customer_journeys(web_sessions, customer_sessions, orders):
    """Create customer journeys by joining web sessions with orders"""
    print("Creating customer journeys")
    
    if web_sessions is None or orders is None:
        print("Missing required data for customer journeys")
        return None
    
    try:
        # Combine web sessions and customer sessions
        all_sessions = web_sessions
        if customer_sessions is not None:
            all_sessions = web_sessions.union(customer_sessions)
        
        # Filter for sessions with marketing attribution data
        sessions_with_attribution = all_sessions.filter(
            col("utm_source").isNotNull() | 
            col("utm_medium").isNotNull() | 
            col("utm_campaign").isNotNull() |
            col("campaign_id").isNotNull()
        )
        
        attr_session_count = sessions_with_attribution.count()
        print(f"Found {attr_session_count} sessions with marketing attribution data")
        
        # Window for ordering sessions by user and timestamp
        user_window = Window.partitionBy("user_id").orderBy("session_start")
        
        # Add sequence number to sessions
        sequenced_sessions = sessions_with_attribution \
            .withColumn("session_seq", row_number().over(user_window))
        
        # Get orders
        valid_orders = orders.filter(col("customer_id").isNotNull())
        
        # Window for ordering orders by user and date
        order_window = Window.partitionBy("customer_id").orderBy("order_date")
        
        # Add sequence number to orders
        sequenced_orders = valid_orders \
            .withColumn("order_seq", row_number().over(order_window))
        
        # Join sessions and orders for user journeys
        # Look for sessions that happened before orders (within 30 day window)
        journeys = sequenced_sessions \
            .join(
                sequenced_orders,
                (sequenced_sessions["user_id"] == sequenced_orders["customer_id"]) &
                (sequenced_sessions["session_start"] <= sequenced_orders["order_date"]) &
                (datediff(sequenced_orders["order_date"], sequenced_sessions["session_start"]) <= 30),
                "inner"
            ) \
            .select(
                sequenced_orders["order_id"],
                sequenced_orders["customer_id"].alias("user_id"),
                sequenced_orders["order_date"],
                sequenced_orders["final_total"].alias("order_value"),
                sequenced_orders["order_seq"],
                sequenced_sessions["session_id"],
                sequenced_sessions["session_start"],
                sequenced_sessions["utm_source"],
                sequenced_sessions["utm_medium"],
                sequenced_sessions["utm_campaign"],
                sequenced_sessions["campaign_id"],
                sequenced_sessions["session_seq"],
                datediff(
                    sequenced_orders["order_date"],
                    sequenced_sessions["session_start"]
                ).alias("days_before_purchase")
            )
        
        # Calculate journey metrics
        journey_metrics = journeys \
            .groupBy("order_id", "user_id", "order_date", "order_value") \
            .agg(
                count("session_id").alias("touchpoint_count"),
                min("session_start").alias("first_touchpoint_date"),
                max("session_start").alias("last_touchpoint_date"),
                min("days_before_purchase").alias("min_days_before_purchase"),
                max("days_before_purchase").alias("max_days_before_purchase"),
                collect_list(
                    struct(
                        "session_id", "session_start", "utm_source", 
                        "utm_medium", "utm_campaign", "campaign_id",
                        "days_before_purchase"
                    )
                ).alias("touchpoints")
            )
        
        journey_count = journey_metrics.count()
        print(f"Created {journey_count} customer journeys")
        
        return journey_metrics
    except Exception as e:
        print(f"Error creating customer journeys: {e}")
        return None

def create_attribution_models(journey_metrics):
    """Create different attribution models from journey metrics"""
    print("Creating attribution models")
    
    if journey_metrics is None:
        print("Missing journey metrics for attribution")
        return None
    
    # 1. First-touch attribution
    first_touch = journey_metrics \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            "first_touchpoint_date",
            "last_touchpoint_date",
            expr("touchpoints[0]").alias("first_touchpoint")
        ) \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            "first_touchpoint_date",
            "last_touchpoint_date",
            col("first_touchpoint.utm_source").alias("first_touch_source"),
            col("first_touchpoint.utm_medium").alias("first_touch_medium"),
            col("first_touchpoint.utm_campaign").alias("first_touch_campaign"),
            col("first_touchpoint.campaign_id").alias("first_touch_campaign_id"),
            col("first_touchpoint.days_before_purchase").alias("first_touch_days_before_purchase")
        ) \
        .withColumn("attribution_model", lit("first_touch")) \
        .withColumn("attributed_revenue", col("order_value"))
    
    # 2. Last-touch attribution
    last_touch = journey_metrics \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            "first_touchpoint_date",
            "last_touchpoint_date",
            expr("touchpoints[size(touchpoints) - 1]").alias("last_touchpoint")
        ) \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            "first_touchpoint_date",
            "last_touchpoint_date",
            col("last_touchpoint.utm_source").alias("last_touch_source"),
            col("last_touchpoint.utm_medium").alias("last_touch_medium"),
            col("last_touchpoint.utm_campaign").alias("last_touch_campaign"),
            col("last_touchpoint.campaign_id").alias("last_touch_campaign_id"),
            col("last_touchpoint.days_before_purchase").alias("last_touch_days_before_purchase")
        ) \
        .withColumn("attribution_model", lit("last_touch")) \
        .withColumn("attributed_revenue", col("order_value"))
    
    # 3. Linear attribution (equal credit to all touchpoints)
    # This requires exploding the touchpoints array and giving each touchpoint equal credit
    linear_attribution = journey_metrics \
        .select(
            "order_id",
            "user_id", 
            "order_date",
            "order_value",
            "touchpoint_count",
            explode("touchpoints").alias("touchpoint")
        ) \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            col("touchpoint.session_id").alias("session_id"),
            col("touchpoint.utm_source").alias("utm_source"),
            col("touchpoint.utm_medium").alias("utm_medium"),
            col("touchpoint.utm_campaign").alias("utm_campaign"),
            col("touchpoint.campaign_id").alias("campaign_id"),
            col("touchpoint.days_before_purchase").alias("days_before_purchase")
        ) \
        .withColumn("attribution_model", lit("linear")) \
        .withColumn("touchpoint_weight", lit(1) / col("touchpoint_count")) \
        .withColumn("attributed_revenue", col("order_value") / col("touchpoint_count"))
    
    # 4. Time-decay attribution (more credit to touchpoints closer to purchase)
    # Use an exponential decay function based on days before purchase
    time_decay_attribution = journey_metrics \
        .select(
            "order_id",
            "user_id", 
            "order_date",
            "order_value",
            "touchpoint_count",
            explode("touchpoints").alias("touchpoint")
        ) \
        .select(
            "order_id",
            "user_id",
            "order_date",
            "order_value",
            "touchpoint_count",
            col("touchpoint.session_id").alias("session_id"),
            col("touchpoint.utm_source").alias("utm_source"),
            col("touchpoint.utm_medium").alias("utm_medium"),
            col("touchpoint.utm_campaign").alias("utm_campaign"),
            col("touchpoint.campaign_id").alias("campaign_id"),
            col("touchpoint.days_before_purchase").alias("days_before_purchase")
        ) \
        .withColumn("decay_factor", expr("pow(0.7, days_before_purchase)")) \
        .withColumn("attribution_model", lit("time_decay"))
    
    # Calculate sum of decay factors for each order to normalize
    decay_sums = time_decay_attribution \
        .groupBy("order_id") \
        .agg(sum("decay_factor").alias("total_decay_factor"))
    
    # Join and calculate weighted revenue
    time_decay_attribution = time_decay_attribution \
        .join(decay_sums, "order_id") \
        .withColumn("touchpoint_weight", col("decay_factor") / col("total_decay_factor")) \
        .withColumn("attributed_revenue", col("order_value") * col("touchpoint_weight")) \
        .drop("decay_factor", "total_decay_factor")
    
    # Union all attribution models
    # Need to align schemas first
    
    # First touch schema alignment
    first_touch_aligned = first_touch \
        .withColumn("session_id", lit(None).cast("string")) \
        .withColumn("utm_source", col("first_touch_source")) \
        .withColumn("utm_medium", col("first_touch_medium")) \
        .withColumn("utm_campaign", col("first_touch_campaign")) \
        .withColumn("campaign_id", col("first_touch_campaign_id")) \
        .withColumn("days_before_purchase", col("first_touch_days_before_purchase")) \
        .withColumn("touchpoint_weight", lit(1.0)) \
        .select(
            "order_id", "user_id", "order_date", "order_value", "touchpoint_count",
            "session_id", "utm_source", "utm_medium", "utm_campaign", "campaign_id",
            "days_before_purchase", "attribution_model", "touchpoint_weight", "attributed_revenue"
        )
    
    # Last touch schema alignment
    last_touch_aligned = last_touch \
        .withColumn("session_id", lit(None).cast("string")) \
        .withColumn("utm_source", col("last_touch_source")) \
        .withColumn("utm_medium", col("last_touch_medium")) \
        .withColumn("utm_campaign", col("last_touch_campaign")) \
        .withColumn("campaign_id", col("last_touch_campaign_id")) \
        .withColumn("days_before_purchase", col("last_touch_days_before_purchase")) \
        .withColumn("touchpoint_weight", lit(1.0)) \
        .select(
            "order_id", "user_id", "order_date", "order_value", "touchpoint_count",
            "session_id", "utm_source", "utm_medium", "utm_campaign", "campaign_id",
            "days_before_purchase", "attribution_model", "touchpoint_weight", "attributed_revenue"
        )
    
    # Linear model already has correct schema
    linear_aligned = linear_attribution.select(
        "order_id", "user_id", "order_date", "order_value", "touchpoint_count",
        "session_id", "utm_source", "utm_medium", "utm_campaign", "campaign_id",
        "days_before_purchase", "attribution_model", "touchpoint_weight", "attributed_revenue"
    )
    
    # Time decay model already has correct schema
    time_decay_aligned = time_decay_attribution.select(
        "order_id", "user_id", "order_date", "order_value", "touchpoint_count",
        "session_id", "utm_source", "utm_medium", "utm_campaign", "campaign_id",
        "days_before_purchase", "attribution_model", "touchpoint_weight", "attributed_revenue"
    )
    
    # Union all models
    all_attributions = first_touch_aligned.union(last_touch_aligned) \
        .union(linear_aligned) \
        .union(time_decay_aligned)
    
    print(f"Created {all_attributions.count()} attribution records across all models")
    
    return all_attributions

def calculate_channel_performance(attribution_data, ad_data):
    """Calculate performance metrics for each channel based on attribution"""
    print("Calculating channel performance metrics")
    
    if attribution_data is None:
        print("Missing attribution data for channel performance")
        return None
    
    try:
        # Aggregate attribution by channel (source, medium, campaign) and model
        channel_performance = attribution_data \
            .filter(col("utm_source").isNotNull()) \
            .groupBy("attribution_model", "utm_source", "utm_medium", "utm_campaign", "campaign_id") \
            .agg(
                count("order_id").alias("conversions"),
                sum("attributed_revenue").alias("attributed_revenue"),
                avg("touchpoint_weight").alias("avg_touchpoint_weight"),
                count(distinct("order_id")).alias("unique_orders"),
                count(distinct("user_id")).alias("unique_users")
            ) \
            .withColumn("attribution_date", current_date())
        
        # Join with ad spend data if available to calculate ROI
        # First combine all ad data sources and create a standard schema
        if ad_data and ("all_platforms" in ad_data) and (ad_data["all_platforms"] is not None):
            ad_spend = ad_data["all_platforms"] \
                .groupBy("campaign_id", "platform", "campaign_name") \
                .agg(
                    sum("cost").alias("total_cost"),
                    sum("impressions").alias("total_impressions"),
                    sum("clicks").alias("total_clicks"),
                    sum("conversions").alias("tracked_conversions")
                )
            
            # Join attribution data with ad spend
            channel_performance_with_spend = channel_performance \
                .join(
                    ad_spend,
                    channel_performance["campaign_id"] == ad_spend["campaign_id"],
                    "left"
                ) \
                .withColumn("roi", 
                    when(col("total_cost").isNotNull() & (col("total_cost") > 0),
                        (col("attributed_revenue") - col("total_cost")) / col("total_cost")
                    ).otherwise(None)
                ) \
                .withColumn("cpa", 
                    when(col("conversions") > 0,
                        col("total_cost") / col("conversions")
                    ).otherwise(None)
                ) \
                .withColumn("cost_per_click",
                    when(col("total_clicks") > 0,
                        col("total_cost") / col("total_clicks")
                    ).otherwise(None)
                ) \
                .withColumn("conversion_rate",
                    when(col("total_clicks") > 0,
                        col("conversions") / col("total_clicks")
                    ).otherwise(None)
                )
            
            print(f"Calculated performance metrics with spend data for {channel_performance_with_spend.count()} channel combinations")
            return channel_performance_with_spend
        else:
            print("Ad spend data not available, returning attribution data only")
            return channel_performance
    except Exception as e:
        print(f"Error calculating channel performance: {e}")
        return None

def create_marketing_attribution(process_date, days_back=30):
    """Main function to create attribution models"""
    date_str = process_date.strftime('%Y-%m-%d')
    print(f"Creating attribution models for date {date_str} with {days_back} days lookback")
    
    # Get date range
    start_date, end_date = get_date_range(process_date, days_back)
    
    # Load data
    web_sessions = load_web_sessions(start_date, end_date)
    customer_sessions = load_customer_sessions(start_date, end_date)
    orders = load_orders(start_date, end_date)
    ad_data = load_ad_data(start_date, end_date)
    
    # Create customer journeys
    journey_metrics = create_customer_journeys(web_sessions, customer_sessions, orders)
    
    # Create attribution models
    attribution_data = create_attribution_models(journey_metrics)
    
    # Calculate channel performance
    channel_performance = calculate_channel_performance(attribution_data, ad_data)
    
    # Save attribution data to Silver
    if attribution_data is not None:
        try:
            # Partition by model and date
            attribution_data \
                .withColumn("year", year(col("order_date"))) \
                .withColumn("month", month(col("order_date"))) \
                .withColumn("day", dayofmonth(col("order_date"))) \
                .write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("attribution_model", "year", "month")