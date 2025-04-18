"""
product_performance.py - Spark script to create product performance analytics for Gold layer

This script:
1. Reads order items, orders, and customer data from Silver
2. Calculates product performance metrics (sales, revenue, conversion)
3. Creates aggregated metrics by product, category, time period
4. Adds seasonality and trend analysis
5. Identifies top-performing and underperforming products
6. Writes results to the Gold zone for reporting and dashboards

Usage:
    spark-submit product_performance.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, countDistinct, avg, max, min, 
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank, lag, row_number, regexp_extract,
    year, month, dayofmonth, quarter, dayofweek, 
    weekofyear, from_unixtime, unix_timestamp,
    explode, collect_list, size, explode_outer
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Product Performance Analytics") \
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
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_ORDER_ITEMS = f"{MINIO_SILVER_BUCKET}/order_items"
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_WEB_LOGS = f"{MINIO_SILVER_BUCKET}/web_logs"
MINIO_GOLD_PRODUCT_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/product_performance"

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
    
    # Previous month for year-over-year comparison
    prev_month_start = process_date - timedelta(days=365+29)
    prev_month_end = process_date - timedelta(days=365)
    
    return {
        "daily": (daily_date, daily_date),
        "weekly": (weekly_start, weekly_end),
        "monthly": (monthly_start, monthly_end),
        "quarterly": (quarterly_start, quarterly_end),
        "prev_month": (prev_month_start, prev_month_end)
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

def load_orders_data():
    """Load orders data from Silver"""
    return load_data_from_path(MINIO_SILVER_ORDERS, "orders")

def load_order_items_data():
    """Load order items data from Silver"""
    return load_data_from_path(MINIO_SILVER_ORDER_ITEMS, "order items")

def load_customers_data():
    """Load customers data from Silver"""
    return load_data_from_path(MINIO_SILVER_CUSTOMERS, "customers")

def load_web_logs_data():
    """Load web logs data from Silver"""
    return load_data_from_path(MINIO_SILVER_WEB_LOGS, "web logs")

def create_product_metrics(order_items, orders, web_logs, date_ranges):
    """Create core product performance metrics"""
    print("Creating core product performance metrics")
    
    if order_items is None:
        print("Missing order items data for product metrics")
        return None
    
    # Join order items with orders to get order date and customer info
    order_metrics = None
    
    if orders is not None:
        try:
            # Be explicit about which columns to select to avoid ambiguity
            order_metrics = order_items.select(
                "order_id", 
                "product_name",
                "category",
                "price",
                "quantity",
                "item_total"
            ).join(
                orders.select(
                    "order_id", 
                    col("customer_id").alias("order_customer_id"), 
                    col("order_date").alias("order_date_clean"), 
                    "order_status"
                ),
                "order_id", 
                "left"
            )
        except Exception as e:
            print(f"Error joining order items with orders: {e}")
            order_metrics = order_items
    else:
        order_metrics = order_items
    
    # Calculate basic product metrics
    product_metrics = order_metrics.groupBy("product_name", "category").agg(
        count("order_id").alias("order_count"),
        sum("quantity").alias("total_quantity"),
        sum("item_total").alias("total_revenue"),
        avg("price").alias("avg_price"),
        count("order_customer_id").alias("customer_count"),  # Use renamed column
        avg("quantity").alias("avg_quantity_per_order")
    )
    
    # Calculate additional metrics
    product_metrics = product_metrics.withColumn(
        "avg_revenue_per_order", 
        col("total_revenue") / col("order_count")
    )
    
    # Add date ranges
    time_period_metrics = []
    
    for period_name, (start_date, end_date) in date_ranges.items():
        print(f"Processing {period_name} metrics from {start_date} to {end_date}")
        
        # Convert dates to strings for filtering
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Filter order metrics for this period
        if order_metrics is not None and "order_date_clean" in order_metrics.columns:
            # Use the clean column we created specifically for filtering
            period_order_metrics = order_metrics.filter(
                (col("order_date_clean") >= start_date_str) & 
                (col("order_date_clean") <= end_date_str)
            )
            
            # Calculate product metrics for this period
            period_product_metrics = period_order_metrics.groupBy("product_name", "category").agg(
                count("order_id").alias("order_count"),
                sum("quantity").alias("total_quantity"),
                sum("item_total").alias("total_revenue"),
                count("order_customer_id").alias("customer_count")
            ).withColumn("time_period", lit(period_name)) \
             .withColumn("start_date", lit(start_date_str)) \
             .withColumn("end_date", lit(end_date_str))
            
            time_period_metrics.append(period_product_metrics)
    
    # Combine all time periods
    if time_period_metrics:
        all_periods = time_period_metrics[0]
        for df in time_period_metrics[1:]:
            all_periods = all_periods.union(df)
        
        print(f"Created {all_periods.count()} time period metric records in total")
        return all_periods
    else:
        print("No time period metrics created")
        return None

def create_product_categories_analysis(product_metrics):
    """Analyze product performance by category"""
    print("Creating product category analysis")
    
    if product_metrics is None:
        print("Missing product metrics for category analysis")
        return None
    
    try:
        # Group by category and time period
        category_metrics = product_metrics.groupBy("category", "time_period", "start_date", "end_date").agg(
            sum("order_count").alias("category_order_count"),
            sum("total_quantity").alias("category_quantity"),
            sum("total_revenue").alias("category_revenue"),
            count("product_name").alias("product_count")
        )
        
        # Calculate totals for each time period to get percentages
        period_totals = product_metrics.groupBy("time_period", "start_date", "end_date").agg(
            sum("total_revenue").alias("period_total_revenue"),
            sum("total_quantity").alias("period_total_quantity")
        )
        
        # Join with totals to calculate percentages
        category_metrics = category_metrics.join(
            period_totals,
            ["time_period", "start_date", "end_date"],
            "left"
        ).withColumn(
            "revenue_percentage", 
            when(col("period_total_revenue") > 0, 
                (col("category_revenue") / col("period_total_revenue")) * 100
            ).otherwise(0)
        ).withColumn(
            "quantity_percentage", 
            when(col("period_total_quantity") > 0, 
                (col("category_quantity") / col("period_total_quantity")) * 100
            ).otherwise(0)
        )
        
        # Create category rankings
        window_spec = Window.partitionBy("time_period").orderBy(col("category_revenue").desc())
        
        category_metrics = category_metrics.withColumn(
            "revenue_rank", 
            dense_rank().over(window_spec)
        )
        
        print(f"Created {category_metrics.count()} category analysis records")
        return category_metrics
    
    except Exception as e:
        print(f"Error creating category analysis: {e}")
        return None

def create_product_rankings(product_metrics):
    """Create product rankings based on revenue and quantity"""
    print("Creating product rankings")
    
    if product_metrics is None:
        print("Missing product metrics for rankings")
        return None
    
    try:
        # Create ranking windows for each time period
        revenue_window = Window.partitionBy("time_period", "category").orderBy(col("total_revenue").desc())
        quantity_window = Window.partitionBy("time_period", "category").orderBy(col("total_quantity").desc())
        
        # Add rankings
        product_rankings = product_metrics \
            .withColumn("revenue_rank", dense_rank().over(revenue_window)) \
            .withColumn("quantity_rank", dense_rank().over(quantity_window))
        
        # Create ranking by time period (across all categories)
        overall_revenue_window = Window.partitionBy("time_period").orderBy(col("total_revenue").desc())
        overall_quantity_window = Window.partitionBy("time_period").orderBy(col("total_quantity").desc())
        
        product_rankings = product_rankings \
            .withColumn("overall_revenue_rank", dense_rank().over(overall_revenue_window)) \
            .withColumn("overall_quantity_rank", dense_rank().over(overall_quantity_window))
        
        # Add top performer flags
        product_rankings = product_rankings \
            .withColumn("is_top_revenue_product", col("revenue_rank") <= 3) \
            .withColumn("is_top_quantity_product", col("quantity_rank") <= 3) \
            .withColumn("is_overall_top_product", col("overall_revenue_rank") <= 10)
        
        print(f"Created rankings for {product_rankings.count()} product records")
        return product_rankings
    
    except Exception as e:
        print(f"Error creating product rankings: {e}")
        return None

def create_trend_analysis(product_metrics, date_ranges):
    """Create trend analysis to track performance changes over time"""
    print("Creating product trend analysis")
    
    if product_metrics is None:
        print("Missing product metrics for trend analysis")
        return None
    
    try:
        # Focus on monthly trend analysis
        monthly_metrics = product_metrics.filter(col("time_period") == "monthly")
        prev_month_metrics = product_metrics.filter(col("time_period") == "prev_month")
        
        if monthly_metrics.count() == 0 or prev_month_metrics.count() == 0:
            print("Insufficient data for trend analysis (missing monthly or previous month data)")
            return None
        
        # Join current month with previous month
        trend_analysis = monthly_metrics.select(
            "product_name", 
            "category", 
            col("total_revenue").alias("current_revenue"),
            col("total_quantity").alias("current_quantity"),
            col("order_count").alias("current_orders")
        ).join(
            prev_month_metrics.select(
                "product_name", 
                "category", 
                col("total_revenue").alias("previous_revenue"),
                col("total_quantity").alias("previous_quantity"),
                col("order_count").alias("previous_orders")
            ),
            ["product_name", "category"],
            "full_outer"
        )
        
        # Fill nulls with zeros
        trend_analysis = trend_analysis.na.fill(0, [
            "current_revenue", "current_quantity", "current_orders",
            "previous_revenue", "previous_quantity", "previous_orders"
        ])
        
        # Calculate trend metrics
        trend_analysis = trend_analysis \
            .withColumn(
                "revenue_change", 
                col("current_revenue") - col("previous_revenue")
            ) \
            .withColumn(
                "revenue_growth_pct", 
                when(col("previous_revenue") > 0,
                    (col("current_revenue") - col("previous_revenue")) / col("previous_revenue") * 100
                ).otherwise(
                    when(col("current_revenue") > 0, lit(100)).otherwise(lit(0))
                )
            ) \
            .withColumn(
                "quantity_change",
                col("current_quantity") - col("previous_quantity")
            ) \
            .withColumn(
                "quantity_growth_pct",
                when(col("previous_quantity") > 0,
                    (col("current_quantity") - col("previous_quantity")) / col("previous_quantity") * 100
                ).otherwise(
                    when(col("current_quantity") > 0, lit(100)).otherwise(lit(0))
                )
            ) \
            .withColumn(
                "orders_change",
                col("current_orders") - col("previous_orders")
            ) \
            .withColumn(
                "orders_growth_pct",
                when(col("previous_orders") > 0,
                    (col("current_orders") - col("previous_orders")) / col("previous_orders") * 100
                ).otherwise(
                    when(col("current_orders") > 0, lit(100)).otherwise(lit(0))
                )
            )
        
        # Classify the trend
        trend_analysis = trend_analysis \
            .withColumn(
                "revenue_trend", 
                when(col("revenue_growth_pct") >= 20, "Strong Growth")
                .when(col("revenue_growth_pct") >= 5, "Growth")
                .when(col("revenue_growth_pct") >= -5, "Stable")
                .when(col("revenue_growth_pct") >= -20, "Decline")
                .otherwise("Strong Decline")
            )
        
        # Add time period info
        current_period = date_ranges["monthly"]
        previous_period = date_ranges["prev_month"]
        
        trend_analysis = trend_analysis \
            .withColumn("current_period", lit("monthly")) \
            .withColumn("current_start_date", lit(current_period[0].strftime('%Y-%m-%d'))) \
            .withColumn("current_end_date", lit(current_period[1].strftime('%Y-%m-%d'))) \
            .withColumn("previous_period", lit("prev_month")) \
            .withColumn("previous_start_date", lit(previous_period[0].strftime('%Y-%m-%d'))) \
            .withColumn("previous_end_date", lit(previous_period[1].strftime('%Y-%m-%d')))
        
        print(f"Created trend analysis for {trend_analysis.count()} products")
        return trend_analysis
    
    except Exception as e:
        print(f"Error creating trend analysis: {e}")
        return None

def analyze_product_seasonality(order_items, orders):
    """Analyze product seasonality patterns"""
    print("Analyzing product seasonality")
        
    if order_items is None or orders is None:
        print("Missing order data for seasonality analysis")
        return None
        
    try:
        # Join order items with orders to get dates with explicit column references
        order_items_renamed = order_items.select(
            col("order_id").alias("item_order_id"),
            "product_name", 
            "category", 
            "quantity", 
            "item_total"
        )
            
        orders_renamed = orders.select(
            col("order_id").alias("order_order_id"),
            "order_date", 
            "season"
        )
            
        order_data = order_items_renamed.join(
            orders_renamed,
            col("item_order_id") == col("order_order_id"),
            "inner"
        )
            
        # Add date components for seasonality analysis
        order_data = order_data \
            .withColumn("order_date_dt", to_date(col("order_date"))) \
            .withColumn("year", year(col("order_date_dt"))) \
            .withColumn("month", month(col("order_date_dt"))) \
            .withColumn("quarter", quarter(col("order_date_dt"))) \
            .withColumn("day_of_week", dayofweek(col("order_date_dt"))) \
            .withColumn("week_of_year", weekofyear(col("order_date_dt")))
            
        # Analyze by season
        seasonal_analysis = order_data.groupBy("product_name", "category", "season").agg(
            count("item_order_id").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            sum("item_total").alias("total_revenue")
        )
            
        # Get total metrics by product to calculate percentages
        product_totals = order_data.groupBy("product_name", "category").agg(
            count("item_order_id").alias("product_total_orders"),
            sum("quantity").alias("product_total_quantity"),
            sum("item_total").alias("product_total_revenue")
        )
            
        # Join seasonal analysis with product totals
        seasonal_analysis = seasonal_analysis.join(
            product_totals,
            ["product_name", "category"],
            "left"
        ).withColumn(
            "season_revenue_pct",
            when(col("product_total_revenue") > 0,
                (col("total_revenue") / col("product_total_revenue")) * 100
            ).otherwise(0)
        ).withColumn(
            "season_quantity_pct",
            when(col("product_total_quantity") > 0,
                (col("total_quantity") / col("product_total_quantity")) * 100
            ).otherwise(0)
        ).withColumn(
            "season_order_pct",
            when(col("product_total_orders") > 0,
                (col("order_count") / col("product_total_orders")) * 100
            ).otherwise(0)
        )
            
        # Find the peak season for each product
        seasonal_window = Window.partitionBy("product_name").orderBy(col("total_revenue").desc())
            
        seasonal_analysis = seasonal_analysis.withColumn(
            "season_rank", 
            row_number().over(seasonal_window)
        ).withColumn(
            "is_peak_season",
            col("season_rank") == 1
        )
            
        # Monthly analysis
        monthly_analysis = order_data.groupBy("product_name", "category", "month").agg(
            count("item_order_id").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            sum("item_total").alias("total_revenue")
        )
            
        # Join with product totals
        monthly_analysis = monthly_analysis.join(
            product_totals,
            ["product_name", "category"],
            "left"
        ).withColumn(
            "month_revenue_pct",
            when(col("product_total_revenue") > 0,
                (col("total_revenue") / col("product_total_revenue")) * 100
            ).otherwise(0)
        )
            
        # Find peak month
        month_window = Window.partitionBy("product_name").orderBy(col("total_revenue").desc())
            
        monthly_analysis = monthly_analysis.withColumn(
            "month_rank", 
            row_number().over(month_window)
        ).withColumn(
            "is_peak_month",
            col("month_rank") == 1
        )
            
        # Create month name column for readability
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
            7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
        }
            
        mapping_expr = expr("CASE " + " ".join([f"WHEN month = {k} THEN '{v}'" for k, v in month_names.items()]) + " END")
        monthly_analysis = monthly_analysis.withColumn("month_name", mapping_expr)
            
        # Create day of week analysis
        dow_analysis = order_data.groupBy("product_name", "category", "day_of_week").agg(
            count("item_order_id").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            sum("item_total").alias("total_revenue")
        )
            
        # Join with product totals
        dow_analysis = dow_analysis.join(
            product_totals,
            ["product_name", "category"],
            "left"
        ).withColumn(
            "dow_revenue_pct",
            when(col("product_total_revenue") > 0,
                (col("total_revenue") / col("product_total_revenue")) * 100
            ).otherwise(0)
        )
            
        # Create day name column for readability
        day_names = {
            1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 
            5: "Thursday", 6: "Friday", 7: "Saturday"
        }
            
        mapping_expr = expr("CASE " + " ".join([f"WHEN day_of_week = {k} THEN '{v}'" for k, v in day_names.items()]) + " END")
        dow_analysis = dow_analysis.withColumn("day_name", mapping_expr)
            
        print(f"Created seasonal analysis for {seasonal_analysis.count()} product-season combinations")
        print(f"Created monthly analysis for {monthly_analysis.count()} product-month combinations")
        print(f"Created day of week analysis for {dow_analysis.count()} product-day combinations")
            
        return {
            "seasonal": seasonal_analysis,
            "monthly": monthly_analysis,
            "dow": dow_analysis
        }
        
    except Exception as e:
        print(f"Error analyzing product seasonality: {e}")
        return None

def calculate_product_combinations(order_items):
    """Calculate frequently purchased product combinations"""
    print("Calculating product combination analysis")
        
    if order_items is None:
        print("Missing order items data for combination analysis")
        return None
        
    try:
        # Import needed functions
        from pyspark.sql.functions import collect_list, explode, size, countDistinct
            
        # Group items by order to get combinations
        order_products = order_items.groupBy("order_id").agg(
            collect_list("product_name").alias("products")
        )
            
        # Skip SQL approach and use DataFrame API instead
        # Filter to orders with at least 2 products
        orders_with_multiple_products = order_products.filter(size(col("products")) > 1)
            
        # Create pairs directly
        product_pairs_df = orders_with_multiple_products.select(
            "order_id", 
            explode("products").alias("product1")
        )
            
        pairs_with_remaining = product_pairs_df.join(
            orders_with_multiple_products,
            "order_id"
        )
            
        # Create arrays with products after the current one
        from pyspark.sql.functions import array, array_position, slice
            
        # Create a custom UDF to get remaining products after index
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, StringType
            
        @udf(returnType=ArrayType(StringType()))
        def get_remaining_products(all_products, current_product):
            if current_product in all_products:
                index = all_products.index(current_product)
                return all_products[index+1:]
            return []
            
        # Apply UDF to get remaining products
        pairs_with_remaining = pairs_with_remaining.withColumn(
            "remaining_products", 
            get_remaining_products("products", "product1")
        )
            
        # Now explode the remaining products
        product_pairs = pairs_with_remaining.select(
            "order_id",
            "product1",
            explode_outer("remaining_products").alias("product2")
        ).filter(col("product2").isNotNull())
            
        # Count frequency of each product pair
        product_pairs = product_pairs.groupBy("product1", "product2").agg(
            count("*").alias("pair_frequency")
        )
            
        # Calculate total orders for each product
        product_order_counts = order_items.groupBy("product_name").agg(
            countDistinct("order_id").alias("order_count")
        )
            
        # Join to get order counts for each product in the pair
        product_pairs = product_pairs.join(
            product_order_counts.withColumnRenamed("product_name", "product1")
                            .withColumnRenamed("order_count", "product1_orders"),
            "product1",
            "left"
        ).join(
            product_order_counts.withColumnRenamed("product_name", "product2")
                            .withColumnRenamed("order_count", "product2_orders"),
            "product2",
            "left"
        )
            
        # Calculate conditional probability and lift
        total_orders = order_items.select("order_id").distinct().count()
            
        product_pairs = product_pairs \
            .withColumn(
                "conditional_probability", 
                col("pair_frequency") / col("product1_orders")
            ) \
            .withColumn(
                "lift", 
                when((col("product1_orders") > 0) & (col("product2_orders") > 0),
                    (col("pair_frequency") * total_orders) / 
                    (col("product1_orders") * col("product2_orders"))
                ).otherwise(0)
            )
            
        # Rank by lift
        lift_window = Window.partitionBy("product1").orderBy(col("lift").desc())
            
        product_combinations = product_pairs \
            .withColumn("combination_rank", row_number().over(lift_window)) \
            .withColumn("is_strong_combination", col("lift") > 2.0) \
            .withColumn("is_top_combination", col("combination_rank") <= 3)
            
        print(f"Created {product_combinations.count()} product combination records")
        return product_combinations
        
    except Exception as e:
        print(f"Error calculating product combinations: {e}")
        import traceback
        traceback.print_exc()
        return None

def analyze_product_conversion(order_items, orders, web_logs):
    """Analyze product view-to-purchase conversion rates"""
    print("Analyzing product conversion rates")
        
    if order_items is None or web_logs is None:
        print("Missing data for product conversion analysis")
        return None
        
    try:
        # Count product views from web logs
        from pyspark.sql.functions import countDistinct, regexp_extract
            
        # First check what we're working with
        if "event_type" in web_logs.columns:
            product_views = web_logs.filter(col("event_type") == "product_view")
                
            # Show sample data for debugging - use built-in min function correctly
            view_count = web_logs.filter(col("event_type") == "product_view").count()
            sample_size = 5 if view_count > 5 else view_count
                
            if sample_size > 0:
                print("Sample product view URLs:")
                web_logs.filter(col("event_type") == "product_view").select("page_url").limit(sample_size).show(truncate=False)
                
            product_views = product_views \
                .withColumn(
                    "product_name", 
                    regexp_extract(col("page_url"), "/products/([^/]+)", 1)
                ) \
                .filter(col("product_name").isNotNull()) \
                .groupBy("product_name") \
                .agg(count("*").alias("total_views"))
        else:
            # Create empty dataframe with correct schema if no product views available
            from pyspark.sql.types import StructType, StructField, StringType, LongType
            schema = StructType([
                StructField("product_name", StringType(), True),
                StructField("total_views", LongType(), True)
            ])
            product_views = spark.createDataFrame([], schema)
            print("No event_type column found in web_logs or no product views found")
            
        # Count product purchases
        product_purchases = order_items.groupBy("product_name").agg(
            sum("quantity").alias("total_purchases"),
            countDistinct("order_id").alias("order_count")
        )
            
        # Print sample counts
        print(f"Found {product_views.count()} products with views")
        print(f"Found {product_purchases.count()} products with purchases")
            
        # Join views with purchases
        conversion_analysis = product_views.join(
            product_purchases,
            "product_name",
            "full_outer"  # Include products with only views or only purchases
        )
            
        # Fill null values with 0
        conversion_analysis = conversion_analysis.na.fill({
            "total_views": 0,
            "total_purchases": 0,
            "order_count": 0
        })
            
        # Calculate conversion rates
        conversion_analysis = conversion_analysis \
            .withColumn(
                "view_to_purchase_rate", 
                when(col("total_views") > 0,
                    col("total_purchases") / col("total_views")
                ).otherwise(0)
            ) \
            .withColumn(
                "view_to_order_rate",
                when(col("total_views") > 0,
                    col("order_count") / col("total_views")
                ).otherwise(0)
            )
            
        # Add category information
        category_info = order_items.select("product_name", "category").distinct()
            
        conversion_analysis = conversion_analysis.join(
            category_info,
            "product_name",
            "left"
        )
            
        # Calculate category averages
        category_averages = conversion_analysis.filter(col("category").isNotNull()).groupBy("category").agg(
            avg("view_to_purchase_rate").alias("category_avg_purchase_rate"),
            avg("view_to_order_rate").alias("category_avg_order_rate")
        )
            
        # Join with category averages
        conversion_analysis = conversion_analysis.join(
            category_averages,
            "category",
            "left"
        )
            
        # Compare product to category average
        conversion_analysis = conversion_analysis \
            .withColumn(
                "purchase_rate_vs_category", 
                when(col("category_avg_purchase_rate") > 0,
                    (col("view_to_purchase_rate") - col("category_avg_purchase_rate")) / col("category_avg_purchase_rate")
                ).otherwise(0)
            ) \
            .withColumn(
                "relative_performance",
                when(col("purchase_rate_vs_category") > 0.2, "Above Average")
                .when(col("purchase_rate_vs_category") >= -0.2, "Average")
                .otherwise("Below Average")
            )
            
        print(f"Created conversion analysis for {conversion_analysis.count()} products")
        return conversion_analysis
        
    except Exception as e:
        print(f"Error analyzing product conversion: {e}")
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

def create_product_performance_gold(process_date):
    """Main function to create Gold-level product performance analytics"""
    print(f"Creating product performance Gold layer for {process_date.strftime('%Y-%m-%d')}")
    
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    orders = load_orders_data()
    order_items = load_order_items_data()
    customers = load_customers_data()
    web_logs = load_web_logs_data()
    
    # Process data and create gold tables
    
    # 1. Create core product metrics by time period
    product_metrics = create_product_metrics(order_items, orders, web_logs, date_ranges)
    
    # 2. Create product category analysis
    category_analysis = create_product_categories_analysis(product_metrics)
    
    # 3. Create product rankings
    product_rankings = create_product_rankings(product_metrics)
    
    # 4. Create trend analysis
    trend_analysis = create_trend_analysis(product_metrics, date_ranges)
    
    # 5. Analyze product seasonality
    seasonality_analysis = analyze_product_seasonality(order_items, orders)
    
    # 6. Calculate product combinations
    product_combinations = calculate_product_combinations(order_items)
    
    # 7. Analyze product conversion rates
    conversion_analysis = analyze_product_conversion(order_items, orders, web_logs)
    
    # Add processing date information to all datasets
    processing_year = process_date.year
    processing_month = process_date.month
    processing_day = process_date.day
    
    # Format for partition paths
    processing_date_str = process_date.strftime('%Y-%m-%d')
    
    success = True
    
    # Save product metrics to Gold
    if product_rankings is not None:
        product_rankings = product_rankings \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            product_rankings,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/product_rankings",
            ["time_period", "category", "year", "month"]
        )
    
    # Save category analysis to Gold
    if category_analysis is not None:
        category_analysis = category_analysis \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            category_analysis,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/category_analysis",
            ["time_period", "year", "month"]
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
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/trend_analysis",
            ["year", "month"]
        )
    
    # Save seasonality analysis to Gold
    if seasonality_analysis is not None:
        # Save seasonal analysis
        seasonal = seasonal \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("processing_year", lit(processing_year)) \
            .withColumn("processing_month", lit(processing_month)) \
            .withColumn("processing_day", lit(processing_day))
        
        success = success and save_to_gold(
            seasonal,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/seasonal_analysis",
            ["season", "processing_year", "processing_month"]
        )
    
    # Save monthly analysis - avoid duplicate column names
    monthly = seasonality_analysis.get("monthly")
    if monthly is not None:
        monthly = monthly \
            .withColumnRenamed("month", "month_number") \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("processing_year", lit(processing_year)) \
            .withColumn("processing_month", lit(processing_month)) \
            .withColumn("processing_day", lit(processing_day))
        
        success = success and save_to_gold(
            monthly,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/monthly_analysis",
            ["month_number", "processing_year", "processing_month"]
        )
    
    # Save product combinations to Gold
    if product_combinations is not None:
        product_combinations = product_combinations \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            product_combinations,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/product_combinations",
            ["year", "month"]
        )
    
    # Save conversion analysis to Gold
    if conversion_analysis is not None:
        conversion_analysis = conversion_analysis \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        success = success and save_to_gold(
            conversion_analysis,
            f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/conversion_analysis",
            ["category", "year", "month"]
        )
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Create product performance Gold layer
    success = create_product_performance_gold(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)