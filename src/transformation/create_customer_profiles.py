"""
create_customer_profiles.py - Spark script to create enriched customer profiles

This script:
1. Loads customer and order data from Silver
2. Enriches customer profiles with order history and behavior patterns
3. Calculates customer metrics like lifetime value, purchase frequency, etc.
4. Writes enriched customer profiles to Silver for further analysis

Usage:
    spark-submit create_customer_profiles.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, datediff, 
    to_date, current_date, explode, split, from_json, struct, 
    expr, array_contains, first, last, collect_list, collect_set,
    date_format, months_between, year, month, dayofmonth, rank,coalesce,
)
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType, DoubleType
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Create Customer Profiles") \
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
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_ORDER_ITEMS = f"{MINIO_SILVER_BUCKET}/order_items"
MINIO_SILVER_PROFILES = f"{MINIO_SILVER_BUCKET}/customer_profiles"

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

def load_customer_data():
    """Load customer data from Silver"""
    try:
        customers = spark.read.parquet(MINIO_SILVER_CUSTOMERS)
        print(f"Loaded {customers.count()} customer records from Silver")
        return customers
    except Exception as e:
        print(f"Error loading customer data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/customers"
            print(f"Attempting to read from local path: {local_silver_path}")
            customers = spark.read.parquet(local_silver_path)
            print(f"Loaded {customers.count()} customer records from local Silver")
            return customers
        except Exception as e2:
            print(f"Error loading customer data from local Silver: {e2}")
            return None

def load_order_data():
    """Load order data from Silver"""
    try:
        orders = spark.read.parquet(MINIO_SILVER_ORDERS)
        print(f"Loaded {orders.count()} order records from Silver")
        return orders
    except Exception as e:
        print(f"Error loading order data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/orders"
            print(f"Attempting to read from local path: {local_silver_path}")
            orders = spark.read.parquet(local_silver_path)
            print(f"Loaded {orders.count()} order records from local Silver")
            return orders
        except Exception as e2:
            print(f"Error loading order data from local Silver: {e2}")
            return None

def load_order_items_data():
    """Load order items data from Silver"""
    try:
        order_items = spark.read.parquet(MINIO_SILVER_ORDER_ITEMS)
        print(f"Loaded {order_items.count()} order item records from Silver")
        return order_items
    except Exception as e:
        print(f"Error loading order items data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/order_items"
            print(f"Attempting to read from local path: {local_silver_path}")
            order_items = spark.read.parquet(local_silver_path)
            print(f"Loaded {order_items.count()} order item records from local Silver")
            return order_items
        except Exception as e2:
            print(f"Error loading order items data from local Silver: {e2}")
            return None

def create_enriched_customer_profiles(customers, orders, order_items, process_date):
    """Create enriched customer profiles with order history and behavior patterns"""
    print("Creating enriched customer profiles")
    
    if customers is None or orders is None or order_items is None:
        print("Missing required data for customer profiles")
        return None
    
    # Get current date for recency calculations
    current_date_str = process_date.strftime('%Y-%m-%d')
    
    try:
        # Calculate order metrics by customer
        order_metrics = orders \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("order_count"),
                sum("final_total").alias("total_spent"),
                avg("final_total").alias("avg_order_value"),
                min("order_date").alias("first_order_date"),
                max("order_date").alias("last_order_date"),
                sum(when(col("has_promotion"), 1).otherwise(0)).alias("promo_order_count"),
                sum(when(col("has_marketing_attribution"), 1).otherwise(0)).alias("attributed_order_count"),
                collect_set("payment_method").alias("payment_methods_used")
            ) \
            .withColumn("days_since_first_order", datediff(lit(current_date_str), col("first_order_date"))) \
            .withColumn("days_since_last_order", datediff(lit(current_date_str), col("last_order_date"))) \
            .withColumn("avg_order_frequency_days", 
                        when(col("order_count") > 1, 
                            col("days_since_first_order") / (col("order_count") - 1))
                        .otherwise(None)) \
            .withColumn("promo_sensitivity", 
                        when(col("order_count") > 0, 
                            col("promo_order_count") / col("order_count"))
                        .otherwise(0))
        
        # Get product category preferences by customer
        category_preferences = order_items \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id", "category") \
            .agg(
                sum("item_total").alias("category_spend"),
                count("*").alias("category_purchase_count")
            )
        
        # Get total spend by customer for calculating percentages
        total_spend_by_customer = category_preferences \
            .groupBy("customer_id") \
            .agg(sum("category_spend").alias("total_all_categories"))
        
        # Join to calculate category percentage
        category_preferences = category_preferences \
            .join(total_spend_by_customer, "customer_id") \
            .withColumn("category_percentage", 
                       when(col("total_all_categories") > 0,
                            (col("category_spend") / col("total_all_categories")) * 100)
                       .otherwise(0))
        
        # Create a window to rank categories by customer
        category_window = Window.partitionBy("customer_id").orderBy(col("category_spend").desc())
        
        # Rank categories for each customer
        ranked_categories = category_preferences \
            .withColumn("category_rank", rank().over(category_window))
        
        # Get primary and secondary categories
        primary_categories = ranked_categories \
            .filter(col("category_rank") == 1) \
            .select(
                col("customer_id"),
                col("category").alias("primary_category"),
                col("category_percentage").alias("primary_category_pct"),
                col("category_purchase_count").alias("primary_category_count")
            )
        
        secondary_categories = ranked_categories \
            .filter(col("category_rank") == 2) \
            .select(
                col("customer_id"),
                col("category").alias("secondary_category"),
                col("category_percentage").alias("secondary_category_pct"),
                col("category_purchase_count").alias("secondary_category_count")
            )
        
        # Define RFM segments
        
        # Define recency segments
        recency_segments = order_metrics \
            .withColumn("recency_segment", 
                       when(col("days_since_last_order") <= 30, "Active (< 30 days)")
                       .when(col("days_since_last_order") <= 90, "Recent (30-90 days)")
                       .when(col("days_since_last_order") <= 180, "Engaged (90-180 days)")
                       .when(col("days_since_last_order") <= 365, "At Risk (180-365 days)")
                       .otherwise("Inactive (> 365 days)"))
        
        # Define frequency segments
        frequency_segments = recency_segments \
            .withColumn("frequency_segment",
                       when(col("order_count") >= 10, "Very Frequent (10+ orders)")
                       .when(col("order_count") >= 5, "Frequent (5-9 orders)")
                       .when(col("order_count") >= 2, "Repeat (2-4 orders)")
                       .when(col("order_count") == 1, "One-time")
                       .otherwise("No Purchase"))
        
        # Define monetary segments
        monetary_segments = frequency_segments \
            .withColumn("monetary_segment",
                      when(col("total_spent") >= 150000, "High Value (150K+ CFA)")
                      .when(col("total_spent") >= 50000, "Mid Value (50K-150K CFA)")
                      .when(col("total_spent") > 0, "Low Value (< 50K CFA)")
                      .otherwise("No Spend"))
        
        # Create combined RFM segment based on the three dimensions
        rfm_segments = monetary_segments \
            .withColumn("rfm_segment",
                      when((col("recency_segment") == "Active (< 30 days)") & 
                           (col("frequency_segment").isin("Very Frequent (10+ orders)", "Frequent (5-9 orders)")) &
                           (col("monetary_segment").isin("High Value (150K+ CFA)", "Mid Value (50K-150K CFA)")), 
                           "Champions")
                      .when((col("recency_segment").isin("Active (< 30 days)", "Recent (30-90 days)")) &
                            (col("frequency_segment").isin("Very Frequent (10+ orders)", "Frequent (5-9 orders)")), 
                            "Loyal Customers")
                      .when((col("recency_segment") == "Active (< 30 days)") &
                            (col("frequency_segment") == "One-time"), 
                            "New Customers")
                      .when((col("recency_segment") == "At Risk (180-365 days)") &
                            (col("frequency_segment").isin("Very Frequent (10+ orders)", "Frequent (5-9 orders)")), 
                            "At Risk")
                      .when(col("recency_segment") == "Inactive (> 365 days)", 
                            "Lost Customers")
                      .when((col("recency_segment").isin("Active (< 30 days)", "Recent (30-90 days)")) &
                            (col("frequency_segment") == "Repeat (2-4 orders)"), 
                            "Potential Loyalists")
                      .otherwise("Customers Needing Attention"))
        
        # Add customer lifecycle stage
        lifecycle_segments = rfm_segments \
            .withColumn("lifecycle_stage",
                      when(col("order_count") == 0, "Lead")
                      .when((col("order_count") == 1) & (col("days_since_last_order") <= 30), "New Customer")
                      .when((col("recency_segment").isin("Active (< 30 days)", "Recent (30-90 days)")) &
                            (col("order_count") > 1), 
                            "Active Customer")
                      .when((col("recency_segment") == "Engaged (90-180 days)"), "At-Risk Customer")
                      .when((col("recency_segment") == "At Risk (180-365 days)"), "Lapsed Customer")
                      .when((col("recency_segment") == "Inactive (> 365 days)"), "Lost Customer")
                      .otherwise("Unknown"))
        
        # Join order metrics with category preferences
        customer_profiles = lifecycle_segments \
            .join(primary_categories, "customer_id", "left") \
            .join(secondary_categories, "customer_id", "left")
        
        # Finally, join with customer data
        enriched_profiles = customers \
            .join(customer_profiles, "customer_id", "left") \
            .select(
                # Basic customer information
                customers["customer_id"],
                customers["first_name"],
                customers["last_name"],
                customers["email"],
                customers["phone"],
                customers["city"],
                customers["region"],
                customers["registration_date"],
                customers["skin_type"],
                customers["skin_concerns_array"],
                customers["preferred_ingredients_array"],
                customers["allergies_array"],
                customers["age"],
                customers["gender"],
                customers["is_subscribed"],
                customers["email_engagement"],
                customers["acquisition_source"],
                
                # Order metrics
                coalesce(customer_profiles["order_count"], lit(0)).alias("order_count"),
                customer_profiles["total_spent"],
                customer_profiles["avg_order_value"],
                customer_profiles["first_order_date"],
                customer_profiles["last_order_date"],
                customer_profiles["days_since_first_order"],
                customer_profiles["days_since_last_order"],
                customer_profiles["avg_order_frequency_days"],
                customer_profiles["promo_sensitivity"],
                customer_profiles["payment_methods_used"],
                
                # Product preferences
                customer_profiles["primary_category"],
                customer_profiles["primary_category_pct"],
                customer_profiles["primary_category_count"],
                customer_profiles["secondary_category"],
                customer_profiles["secondary_category_pct"],
                customer_profiles["secondary_category_count"],
                
                # Segmentation
                customer_profiles["recency_segment"],
                customer_profiles["frequency_segment"],
                customer_profiles["monetary_segment"],
                customer_profiles["rfm_segment"],
                customer_profiles["lifecycle_stage"]
            ) \
            .withColumn("profile_updated_at", lit(current_date_str))
        
        print(f"Created enriched profiles for {enriched_profiles.count()} customers")
        return enriched_profiles
    
    except Exception as e:
        print(f"Error creating enriched customer profiles: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_customer_profiles(customer_profiles, process_date):
    """Save customer profiles to Silver storage"""
    if customer_profiles is None:
        print("No customer profiles to save")
        return False
    
    try:
        # Add partition columns
        year = process_date.strftime('%Y')
        month = process_date.strftime('%m')
        day = process_date.strftime('%d')
        
        partitioned_profiles = customer_profiles \
            .withColumn("year", lit(year)) \
            .withColumn("month", lit(month)) \
            .withColumn("day", lit(day))
        
        # Write to Silver zone in Parquet format with partitioning
        partitioned_profiles.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .save(MINIO_SILVER_PROFILES)
        
        print(f"Successfully wrote {partitioned_profiles.count()} customer profiles to Silver at {MINIO_SILVER_PROFILES}")
        return True
    except Exception as e:
        print(f"Error writing customer profiles to MinIO Silver: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/customer_profiles"
            print(f"Attempting to write to local path: {local_silver_path}")
            partitioned_profiles.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .save(local_silver_path)
            print(f"Successfully wrote {partitioned_profiles.count()} customer profiles to local Silver")
            return True
        except Exception as e2:
            print(f"Error writing customer profiles to local Silver: {e2}")
            return False

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Load data
    customers = load_customer_data()
    orders = load_order_data()
    order_items = load_order_items_data()
    
    # Create customer profiles
    customer_profiles = create_enriched_customer_profiles(customers, orders, order_items, process_date)
    
    # Save customer profiles
    success = save_customer_profiles(customer_profiles, process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)