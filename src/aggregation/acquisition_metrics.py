"""
acquisition_metrics.py - Spark script to create customer acquisition analytics for Gold layer

This script:
1. Processes customer acquisition data from Silver
2. Creates metrics by channel, campaign, time period
3. Calculates CAC, CLV/CAC ratio, and other acquisition metrics 
4. Performs cohort analysis by acquisition date
5. Writes results to the Gold zone for reporting and dashboards

Usage:
    spark-submit acquisition_metrics.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, concat, 
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank, lag, lead, first, row_number,
    year, month, dayofmonth, quarter, explode, array, collect_list,
    countDistinct, lower  # Added missing functions
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Customer Acquisition Analytics") \
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
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_ATTRIBUTION = f"{MINIO_SILVER_BUCKET}/marketing_attribution"
MINIO_SILVER_WEB_SESSIONS = f"{MINIO_SILVER_BUCKET}/web_sessions"
MINIO_SILVER_CUSTOMER_PROFILES = f"{MINIO_SILVER_BUCKET}/customer_profiles"
MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_SILVER_ALL_PLATFORMS = f"{MINIO_SILVER_BUCKET}/all_platforms"
MINIO_GOLD_ACQUISITION_METRICS = f"{MINIO_GOLD_BUCKET}/acquisition_metrics"

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
    
    # Yearly: 365 days ending on process date
    yearly_start = process_date - timedelta(days=364)
    yearly_end = process_date
    
    return {
        "daily": (daily_date, daily_date),
        "weekly": (weekly_start, weekly_end),
        "monthly": (monthly_start, monthly_end),
        "quarterly": (quarterly_start, quarterly_end),
        "yearly": (yearly_start, yearly_end)
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

def load_customers_data():
    """Load customers data from Silver"""
    return load_data_from_path(MINIO_SILVER_CUSTOMERS, "customers")

def load_orders_data():
    """Load orders data from Silver"""
    return load_data_from_path(MINIO_SILVER_ORDERS, "orders")

def load_attribution_data():
    """Load marketing attribution data from Silver"""
    return load_data_from_path(MINIO_SILVER_ATTRIBUTION, "marketing attribution")

def load_web_sessions_data():
    """Load web sessions data from Silver"""
    return load_data_from_path(MINIO_SILVER_WEB_SESSIONS, "web sessions")

def load_customer_profiles_data():
    """Load customer profiles data from Silver"""
    return load_data_from_path(MINIO_SILVER_CUSTOMER_PROFILES, "customer profiles")

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

def create_acquisition_source_metrics(customers, orders, ad_data, date_ranges):
    """Create acquisition metrics by source"""
    print("Creating acquisition source metrics")
    
    if customers is None:
        print("Missing customers data for acquisition metrics")
        return None
    
    try:
        # Filter customers with acquisition source info
        acquisition_data = customers.filter(col("acquisition_source").isNotNull())
        
        # Count customers by acquisition source
        source_metrics = acquisition_data.groupBy("acquisition_source").agg(
            count("customer_id").alias("customer_count")
        )
        
        # Calculate value metrics if orders data is available
        if orders is not None:
            # Get first order date for each customer
            first_orders = orders.groupBy(col("customer_id").alias("order_customer_id")).agg(
                min("order_date").alias("first_order_date"),
                count("order_id").alias("order_count"),
                sum("final_total").alias("total_spend")
            )
            
            # Join with customers - use explicit column names to avoid ambiguity
            customer_value = acquisition_data.join(
                first_orders,
                acquisition_data["customer_id"] == first_orders["order_customer_id"],
                "left"
            )
            
            # Aggregate by acquisition source
            source_metrics = customer_value.groupBy("acquisition_source").agg(
                count("customer_id").alias("customer_count"),
                sum(when(col("order_count").isNotNull() & (col("order_count") > 0), 1).otherwise(0)).alias("converting_customers"),
                avg("order_count").alias("avg_orders_per_customer"),
                avg("total_spend").alias("avg_customer_value"),
                sum("total_spend").alias("total_revenue")
            )
            
            # Calculate conversion rate
            source_metrics = source_metrics.withColumn(
                "conversion_rate",
                when(col("customer_count") > 0, 
                    col("converting_customers") / col("customer_count")
                ).otherwise(0)
            )
        
        # Calculate acquisition costs if ad data is available
        if ad_data and ad_data["all_platforms"] is not None:
            ad_spend = ad_data["all_platforms"]
            
            # Normalize source names for joining
            ad_spend_by_source = ad_spend.withColumn(
                "acquisition_source",
                when(col("platform") == "Google", "google")
                .when(col("platform") == "Facebook", "facebook")
                .when(col("platform") == "Instagram", "instagram")
                .when(col("platform") == "TikTok", "tiktok")
                .when(col("platform").startswith("Influencer"), "influencer")
                .otherwise(lower(col("platform")))
            ).groupBy("acquisition_source").agg(
                sum("cost").alias("total_cost")
            )
            
            # Join with source metrics
            source_metrics = source_metrics.join(
                ad_spend_by_source,
                "acquisition_source",
                "left"
            )
            
            # Calculate CAC
            source_metrics = source_metrics.withColumn(
                "cost_per_acquisition",
                when((col("total_cost").isNotNull()) & (col("customer_count") > 0),
                    col("total_cost") / col("customer_count")
                ).otherwise(None)
            ).withColumn(
                "clv_cac_ratio",
                when((col("cost_per_acquisition").isNotNull()) & 
                     (col("cost_per_acquisition") > 0) & 
                     (col("avg_customer_value").isNotNull())),
                    col("avg_customer_value") / col("cost_per_acquisition")
                ).otherwise(None)
            
        
        # Calculate dates for time series analysis
        time_period_metrics = []
        
        for period_name, (start_date, end_date) in date_ranges.items():
            # Convert dates to strings for filtering
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Filter customers registered in this period
            period_customers = acquisition_data.filter(
                (to_date(col("registration_date")) >= start_date_str) & 
                (to_date(col("registration_date")) <= end_date_str)
            )
            
            # Aggregate by acquisition source
            period_metrics = period_customers.groupBy("acquisition_source").agg(
                count("customer_id").alias("new_customers")
            )
            
            # Add period info
            period_metrics = period_metrics \
                .withColumn("time_period", lit(period_name)) \
                .withColumn("start_date", lit(start_date_str)) \
                .withColumn("end_date", lit(end_date_str))
            
            time_period_metrics.append(period_metrics)
        
        # Combine all time periods
        if time_period_metrics:
            all_periods = time_period_metrics[0]
            for df in time_period_metrics[1:]:
                all_periods = all_periods.union(df)
            
            print(f"Created {all_periods.count()} time period acquisition metrics")
            
            # Join with overall source metrics
            acquisition_metrics = all_periods.join(
                source_metrics,
                "acquisition_source",
                "left"
            )
            
            # Calculate acquisition share
            period_totals = all_periods.groupBy("time_period").agg(
                sum("new_customers").alias("total_new_customers")
            )
            
            acquisition_metrics = acquisition_metrics.join(
                period_totals,
                "time_period",
                "left"
            ).withColumn(
                "acquisition_share",
                when(col("total_new_customers") > 0,
                    col("new_customers") / col("total_new_customers") * 100
                ).otherwise(0)
            )
            
            return acquisition_metrics
        else:
            return source_metrics
    
    except Exception as e:
        print(f"Error creating acquisition source metrics: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_cohort_analysis(customers, orders, date_ranges):
    """Create cohort analysis for customer acquisition"""
    print("Creating cohort analysis")
    
    if customers is None or orders is None:
        print("Missing required data for cohort analysis")
        return None
    
    try:
        # Filter customers with acquisition date info
        customer_data = customers.filter(col("registration_date").isNotNull())
        
        # Extract cohort periods (month and year of registration)
        cohort_data = customer_data.withColumn(
            "cohort_year", year(col("registration_date"))
        ).withColumn(
            "cohort_month", month(col("registration_date"))
        ).withColumn(
            "cohort_id", concat(col("cohort_year"), lit("-"), col("cohort_month"))
        )
        
        # Get orders with customer info
        orders_with_customer = orders.filter(col("customer_id").isNotNull()).join(
            cohort_data.select("customer_id", "cohort_id", "registration_date"),
            "customer_id",
            "inner"
        )
        
        # Calculate months since registration for each order
        orders_with_cohort = orders_with_customer.withColumn(
            "months_since_registration",
            (year(col("order_date")) - year(col("registration_date"))) * 12 +
            (month(col("order_date")) - month(col("registration_date")))
        ).filter(col("months_since_registration") >= 0)  # Only include orders after registration
        
        # Calculate cohort metrics
        # 1. Count customers in each cohort
        cohort_sizes = cohort_data.groupBy("cohort_id").agg(
            count("customer_id").alias("cohort_size"),
            min("registration_date").alias("cohort_start_date")
        )
        
        # 2. Calculate retention by month since registration
        retention_data = orders_with_cohort.groupBy("cohort_id", "months_since_registration").agg(
            countDistinct("customer_id").alias("active_customers"),
            sum("final_total").alias("revenue")
        )
        
        # Join with cohort sizes to calculate retention rate
        cohort_retention = retention_data.join(
            cohort_sizes,
            "cohort_id",
            "inner"
        ).withColumn(
            "retention_rate",
            when(col("cohort_size") > 0,
                col("active_customers") / col("cohort_size") * 100
            ).otherwise(0)
        ).withColumn(
            "revenue_per_customer",
            when(col("active_customers") > 0,
                col("revenue") / col("active_customers")
            ).otherwise(0)
        )
        
        # Add month numbers for readability
        retention_months = {
            0: "Month 0 (Acquisition)",
            1: "Month 1",
            2: "Month 2",
            3: "Month 3",
            6: "Month 6",
            12: "Month 12"
        }
        
        mapping_expr = expr(
            "CASE " + 
            " ".join([f"WHEN months_since_registration = {k} THEN '{v}'" for k, v in retention_months.items()]) + 
            " ELSE concat('Month ', cast(months_since_registration as string)) END"
        )
        
        cohort_retention = cohort_retention.withColumn("month_label", mapping_expr)
        
        # Calculate first purchase metrics
        first_purchase_window = Window.partitionBy("customer_id").orderBy("order_date")
        
        first_purchases = orders \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("purchase_rank", row_number().over(first_purchase_window)) \
            .filter(col("purchase_rank") == 1) \
            .select(
                "customer_id",
                "order_date",
                "final_total"
            )
        
        # Join with customer registration data
        first_purchase_metrics = first_purchases.join(
            cohort_data.select("customer_id", "registration_date", "cohort_id"),
            "customer_id",
            "inner"
        ).withColumn(
            "days_to_first_purchase",
            datediff(col("order_date"), col("registration_date"))
        )
        
        # Aggregate by cohort
        first_purchase_cohort = first_purchase_metrics.groupBy("cohort_id").agg(
            avg("days_to_first_purchase").alias("avg_days_to_first_purchase"),
            avg("final_total").alias("avg_first_order_value"),
            count("customer_id").alias("converting_customers")
        )
        
        # Join with cohort sizes to calculate conversion rate
        first_purchase_cohort = first_purchase_cohort.join(
            cohort_sizes,
            "cohort_id",
            "inner"
        ).withColumn(
            "conversion_rate",
            when(col("cohort_size") > 0,
                col("converting_customers") / col("cohort_size") * 100
            ).otherwise(0)
        )
        
        # Sort by cohort date
        cohort_retention = cohort_retention.orderBy("cohort_start_date", "months_since_registration")
        first_purchase_cohort = first_purchase_cohort.orderBy("cohort_start_date")
        
        return {
            "retention": cohort_retention,
            "first_purchase": first_purchase_cohort
        }
    
    except Exception as e:
        print(f"Error creating cohort analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_ltv_projections(customers, orders):
    """Create customer lifetime value projections"""
    print("Creating lifetime value projections")
    
    if customers is None or orders is None:
        print("Missing required data for LTV projections")
        return None
    
    try:
        # Join customer acquisition source with orders
        customer_orders = orders.filter(col("customer_id").isNotNull()).join(
            customers.select("customer_id", "acquisition_source", "registration_date"),
            "customer_id",
            "inner"
        )
        
        # Calculate customer tenure
        customer_orders = customer_orders.withColumn(
            "customer_tenure_days",
            datediff(col("order_date"), col("registration_date"))
        )
        
        # Group orders into tenure buckets
        customer_orders = customer_orders.withColumn(
            "tenure_bucket",
            when(col("customer_tenure_days") < 30, "0-30 days")
            .when(col("customer_tenure_days") < 90, "31-90 days")
            .when(col("customer_tenure_days") < 180, "91-180 days")
            .when(col("customer_tenure_days") < 365, "181-365 days")
            .otherwise("365+ days")
        )
        
        # Calculate metrics by acquisition source and tenure bucket
        ltv_data = customer_orders.groupBy("acquisition_source", "tenure_bucket").agg(
            countDistinct("customer_id").alias("customer_count"),
            sum("final_total").alias("total_revenue"),
            avg("final_total").alias("avg_order_value"),
            count("order_id").alias("order_count")
        ).withColumn(
            "orders_per_customer",
            when(col("customer_count") > 0,
                col("order_count") / col("customer_count")
            ).otherwise(0)
        ).withColumn(
            "revenue_per_customer",
            when(col("customer_count") > 0,
                col("total_revenue") / col("customer_count")
            ).otherwise(0)
        )
        
        # Order tenure buckets for time series analysis
        tenure_order = {
            "0-30 days": 1,
            "31-90 days": 2,
            "91-180 days": 3,
            "181-365 days": 4,
            "365+ days": 5
        }
        
        mapping_expr = expr(
            "CASE " + 
            " ".join([f"WHEN tenure_bucket = '{k}' THEN {v}" for k, v in tenure_order.items()]) + 
            " ELSE 99 END"
        )
        
        ltv_data = ltv_data.withColumn("tenure_order", mapping_expr).orderBy("acquisition_source", "tenure_order")
        
        return ltv_data
    
    except Exception as e:
        print(f"Error creating LTV projections: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_acquisition_channel_effectiveness(attribution_data, web_sessions, ad_data):
    """Analyze effectiveness of acquisition channels"""
    print("Creating acquisition channel effectiveness analysis")
    
    if attribution_data is None and web_sessions is None:
        print("Missing required data for channel effectiveness analysis")
        return None
    
    try:
        channel_metrics = []
        
        # Use web sessions data if available
        if web_sessions is not None:
            # Get conversion metrics by utm source
            web_conversions = web_sessions.filter(
                col("utm_source").isNotNull() & 
                col("conversion_session").isNotNull()
            ).groupBy("utm_source").agg(
                count("session_id").alias("session_count"),
                sum(when(col("conversion_session") == True, 1).otherwise(0)).alias("conversion_count")
            ).withColumn(
                "conversion_rate",
                when(col("session_count") > 0,
                    col("conversion_count") / col("session_count") * 100
                ).otherwise(0)
            ).withColumn(
                "channel_type", 
                lit("Web Traffic")
            )
            
            channel_metrics.append(web_conversions)
        
        # Use attribution data if available
        if attribution_data is not None:
            # Filter for first touch attribution
            first_touch = attribution_data.filter(col("attribution_model") == "first_touch")
            
            # Aggregate by source
            attribution_metrics = first_touch.filter(col("utm_source").isNotNull()).groupBy("utm_source").agg(
                count("order_id").alias("attributed_conversions"),
                sum("attributed_revenue").alias("attributed_revenue"),
                avg("attributed_revenue").alias("avg_order_value")
            ).withColumn(
                "channel_type",
                lit("Marketing Attribution")
            )
            
            channel_metrics.append(attribution_metrics)
        
        # Use ad spend data if available
        if ad_data and ad_data["all_platforms"] is not None:
            # Aggregate by platform
            ad_spend = ad_data["all_platforms"].groupBy("platform").agg(
                sum("cost").alias("total_cost"),
                sum("impressions").alias("total_impressions"),
                sum("clicks").alias("total_clicks"),
                sum("conversions").alias("tracked_conversions")
            ).withColumn(
                "utm_source",
                lower(col("platform"))
            ).withColumn(
                "channel_type",
                lit("Paid Media")
            ).withColumn(
                "cost_per_click",
                when(col("total_clicks") > 0,
                    col("total_cost") / col("total_clicks")
                ).otherwise(None)
            ).withColumn(
                "conversion_rate",
                when(col("total_clicks") > 0,
                    col("tracked_conversions") / col("total_clicks") * 100
                ).otherwise(None)
            ).withColumn(
                "cost_per_conversion",
                when(col("tracked_conversions") > 0,
                    col("total_cost") / col("tracked_conversions")
                ).otherwise(None)
            )
            
            channel_metrics.append(ad_spend)
        
        # Combine all metrics
        if channel_metrics:
            # Need to align the schemas first
            # We'll create a comprehensive schema and fill missing columns with nulls
            
            final_metrics = []
            for df in channel_metrics:
                # Rename utm_source or platform to channel_source for consistency
                if "utm_source" in df.columns:
                    df = df.withColumnRenamed("utm_source", "channel_source")
                elif "platform" in df.columns:
                    df = df.withColumnRenamed("platform", "channel_source") 
                
                final_metrics.append(df)
            
            # Union all metrics
            all_channels = final_metrics[0]
            for df in final_metrics[1:]:
                all_channels = all_channels.unionByName(df, allowMissingColumns=True)
                                           
            # Fill nulls in numeric columns
            numeric_cols = [
                "session_count", "conversion_count", "conversion_rate",
                "attributed_conversions", "attributed_revenue", "avg_order_value",
                "total_cost", "total_impressions", "total_clicks", "tracked_conversions",
                "cost_per_click", "cost_per_conversion"
            ]
            
            all_channels = all_channels.na.fill(0, numeric_cols)
            
            # Calculate effectiveness score (simplified version)
            # This is just an example - real effectiveness would combine multiple metrics
            all_channels = all_channels.withColumn(
                "effectiveness_score",
                when(col("channel_type") == "Paid Media" & col("cost_per_conversion").isNotNull() & (col("cost_per_conversion") > 0),
                    1000 / col("cost_per_conversion")  # Higher score for lower acquisition cost
                ).when(col("channel_type") == "Web Traffic" & col("conversion_rate").isNotNull(),
                    col("conversion_rate") / 10  # Scale conversion rate to comparable range
                ).when(col("channel_type") == "Marketing Attribution" & col("attributed_revenue").isNotNull() & col("attributed_conversions").isNotNull() & (col("attributed_conversions") > 0),
                    col("attributed_revenue") / col("attributed_conversions") / 1000  # Scale average revenue to comparable range
                ).otherwise(0)
            )
            
            # Create a window to rank channels by effectiveness
            effectiveness_window = Window.orderBy(col("effectiveness_score").desc())
            
            all_channels = all_channels.withColumn(
                "effectiveness_rank",
                row_number().over(effectiveness_window)
            )
            
            print(f"Created effectiveness analysis for {all_channels.count()} channels")
            return all_channels
        else:
            return None
    
    except Exception as e:
        print(f"Error creating channel effectiveness analysis: {e}")
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

def create_acquisition_metrics_gold(process_date):
    """Main function to create Gold-level acquisition metrics"""
    print(f"Creating acquisition metrics Gold layer for {process_date.strftime('%Y-%m-%d')}")
    
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    customers = load_customers_data()
    orders = load_orders_data()
    attribution_data = load_attribution_data()
    web_sessions = load_web_sessions_data()
    customer_profiles = load_customer_profiles_data()
    ad_data = load_ad_data()
    
    # Process data and create gold tables
    
    # 1. Create acquisition source metrics
    source_metrics = create_acquisition_source_metrics(customers, orders, ad_data, date_ranges)
    
    # 2. Create cohort analysis
    cohort_analysis = create_cohort_analysis(customers, orders, date_ranges)
    
    # 3. Create LTV projections
    ltv_projections = create_ltv_projections(customers, orders)
    
    # 4. Create channel effectiveness analysis
    channel_effectiveness = create_acquisition_channel_effectiveness(attribution_data, web_sessions, ad_data)
    
    # Add processing date information to all datasets
    processing_year = process_date.year
    processing_month = process_date.month
    processing_day = process_date.day
    
    # Format for partition paths
    processing_date_str = process_date.strftime('%Y-%m-%d')
    
    success = True
    
    # Save acquisition source metrics to Gold
    if source_metrics is not None:
        source_metrics = source_metrics \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            source_metrics,
            f"{MINIO_GOLD_ACQUISITION_METRICS}/acquisition_source",
            ["time_period", "year", "month"]
        )
    
    # Save cohort analysis to Gold
    if cohort_analysis is not None:
        cohort_retention = cohort_analysis.get("retention")
        if cohort_retention is not None:
            cohort_retention = cohort_retention \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("year", lit(processing_year)) \
                .withColumn("month", lit(processing_month)) \
                .withColumn("day", lit(processing_day))
            
            success = success and save_to_gold(
                cohort_retention,
                f"{MINIO_GOLD_ACQUISITION_METRICS}/cohort_retention",
                ["cohort_id", "year", "month"]
            )
        
        first_purchase = cohort_analysis.get("first_purchase")
        if first_purchase is not None:
            first_purchase = first_purchase \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("year", lit(processing_year)) \
                .withColumn("month", lit(processing_month)) \
                .withColumn("day", lit(processing_day))
            
            success = success and save_to_gold(
                first_purchase,
                f"{MINIO_GOLD_ACQUISITION_METRICS}/first_purchase",
                ["cohort_id", "year", "month"]
            )
    
    # Save LTV projections to Gold
    if ltv_projections is not None:
        ltv_projections = ltv_projections \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            ltv_projections,
            f"{MINIO_GOLD_ACQUISITION_METRICS}/ltv_projections",
            ["acquisition_source", "tenure_bucket", "year", "month"]
        )
    
    # Save channel effectiveness to Gold
    if channel_effectiveness is not None:
        channel_effectiveness = channel_effectiveness \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            channel_effectiveness,
            f"{MINIO_GOLD_ACQUISITION_METRICS}/channel_effectiveness",
            ["channel_type", "year", "month"]
        )
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Create acquisition metrics Gold layer
    success = create_acquisition_metrics_gold(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)