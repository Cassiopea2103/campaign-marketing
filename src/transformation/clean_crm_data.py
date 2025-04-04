"""
clean_crm_data.py - Spark script to clean CRM data from Bronze to Silver

This script:
1. Reads customer and order data from the Bronze zone
2. Cleans and standardizes data
3. Enriches with additional calculated fields
4. Writes processed data to the Silver zone of the data lake

Usage:
    spark-submit clean_crm_data.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, lower, trim, 
    to_timestamp, date_format, datediff, current_date,
    explode, split, from_json, struct, to_json,
    concat, expr, year, month, dayofmonth, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType,
    MapType
)
import sys
import os
import json
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Clean CRM Data") \
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

# Define the schema for order items
order_item_schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("item_total", DoubleType(), True)
])

def clean_customer_data():
    """Clean customer data from Bronze"""
    print("Processing customer data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_customers = spark.read.parquet("/data/bronze/customers")
        
        # Log record count
        customer_count = bronze_customers.count()
        print(f"Loaded {customer_count} customer records from Bronze")
        
        if customer_count == 0:
            print("No customer records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze customer data: {e}")
        return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_customers \
        .dropDuplicates(["customer_id"]) \
        .filter(col("customer_id").isNotNull())
    
    # 2. Standardize email addresses
    cleaned_df = cleaned_df \
        .withColumn("email", lower(trim(col("email"))))
    
    # 3. Parse pipe-delimited fields
    cleaned_df = cleaned_df \
        .withColumn("skin_concerns_array", split(col("skin_concerns"), "\\|")) \
        .withColumn("preferred_ingredients_array", split(col("preferred_ingredients"), "\\|")) \
        .withColumn("allergies_array", split(col("allergies"), "\\|"))
    
    # 4. Add derived fields
    cleaned_df = cleaned_df \
        .withColumn("customer_tenure_days", 
                    when(col("registration_date").isNotNull(),
                        datediff(current_date(), col("registration_date")))
                    .otherwise(0))
    
    # 5. Standardize categorical fields
    cleaned_df = cleaned_df \
        .withColumn("skin_type", 
                    when(col("skin_type").isin(["Normal", "Sec", "Mixte", "Gras", "Sensible", "Mature"]), col("skin_type"))
                    .otherwise("Non spécifié"))
    
    # 6. Add customer lifecycle stage
    cleaned_df = cleaned_df \
        .withColumn("lifecycle_stage", 
                    when(col("total_orders").isNull() | (col("total_orders") == 0), "Prospect")
                    .when(col("total_orders") == 1, "Nouveau client")
                    .when(col("total_orders").between(2, 5), "Client régulier")
                    .when(col("total_orders") > 5, "Client fidèle")
                    .otherwise("Prospect"))
    
    # 7. Add potential customer value category based on lifetime value
    cleaned_df = cleaned_df \
        .withColumn("value_segment", 
                    when(col("lifetime_value") > 150000, "Premium")
                    .when(col("lifetime_value").between(50000, 150000), "High")
                    .when(col("lifetime_value").between(10000, 49999), "Medium")
                    .when(col("lifetime_value") > 0, "Low")
                    .otherwise("No purchase"))
    
    # 8. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("year", year(col("registration_date"))) \
        .withColumn("month", month(col("registration_date"))) \
        .withColumn("day", dayofmonth(col("registration_date")))
    
    # 9. Add last updated timestamp 
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_date())
    
    # Select columns for Silver layer
    silver_customers = cleaned_df.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "region",
        "address",
        "registration_date",
        "first_purchase_date",
        "last_purchase_date",
        "total_orders",
        "lifetime_value",
        "favorite_category",
        "skin_type",
        "skin_concerns_array",
        "preferred_ingredients_array",
        "allergies_array",
        "age",
        "gender",
        "is_subscribed",
        "email_engagement",
        "acquisition_source",
        "customer_tenure_days",
        "lifecycle_stage",
        "value_segment",
        "year",
        "month",
        "day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    silver_customers.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save("/data/silver/customers")
    
    print(f"Successfully wrote {silver_customers.count()} customer records to Silver")
    
    return silver_customers

def clean_order_data():
    """Clean order data from Bronze"""
    print("Processing order data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_orders = spark.read.parquet("/data/bronze/orders")
        
        # Log record count
        order_count = bronze_orders.count()
        print(f"Loaded {order_count} order records from Bronze")
        
        if order_count == 0:
            print("No order records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze order data: {e}")
        return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_orders \
        .dropDuplicates(["order_id"]) \
        .filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
    
    # 2. Parse items JSON string to array
    parse_items_udf = udf(lambda x: json.loads(x) if x else [], ArrayType(MapType(StringType(), StringType())))
    
    cleaned_df = cleaned_df \
        .withColumn("items_array", from_json(col("items"), ArrayType(order_item_schema)))
    
    # 3. Add metrics for items
    cleaned_df = cleaned_df \
        .withColumn("item_count", expr("size(items_array)"))
    
    # 4. Add margin calculation assuming 50% margin on average
    cleaned_df = cleaned_df \
        .withColumn("estimated_margin", col("final_total") * 0.5)
    
    # 5. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_day", dayofmonth(col("order_date")))
    
    # 6. Add marketing attribution data
    cleaned_df = cleaned_df \
        .withColumn("has_marketing_attribution", 
                    col("utm_source").isNotNull() | 
                    col("utm_medium").isNotNull() | 
                    col("utm_campaign").isNotNull())
    
    # 7. Add promotion flag
    cleaned_df = cleaned_df \
        .withColumn("has_promotion", col("discount_code").isNotNull() & (col("discount_amount") > 0))
    
    # 8. Add last updated timestamp
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_date())
    
    # Select columns for Silver layer
    silver_orders = cleaned_df.select(
        "order_id",
        "customer_id",
        "order_date",
        "order_total",
        "discount_amount",
        "discount_code",
        "final_total",
        "shipping_cost",
        "payment_method",
        "order_status",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "items_array",
        "item_count",
        "estimated_margin",
        "season",
        "city",
        "has_marketing_attribution",
        "has_promotion",
        "order_year",
        "order_month",
        "order_day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    silver_orders.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .save("/data/silver/orders")
    
    print(f"Successfully wrote {silver_orders.count()} order records to Silver")
    
    # Also create an exploded view of order items
    try:
        order_items_df = cleaned_df.select(
            "order_id",
            "customer_id",
            "order_date",
            "discount_code",
            "order_status",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "season",
            "order_year",
            "order_month",
            "order_day",
            explode(col("items_array")).alias("item")
        ).select(
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "season",
            "order_year",
            "order_month",
            "order_day",
            col("item.product_name").alias("product_name"),
            col("item.category").alias("category"),
            col("item.price").alias("price"),
            col("item.quantity").alias("quantity"),
            col("item.item_total").alias("item_total")
        )
        
        # Write order items to Silver
        order_items_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .save("/data/silver/order_items")
        
        print(f"Successfully wrote {order_items_df.count()} order item records to Silver")
    except Exception as e:
        print(f"Error processing order items: {e}")
    
    return silver_orders

def clean_crm_data(process_date):
    """Clean both customer and order data for the given date"""
    date_str = process_date.strftime('%Y-%m-%d')
    print(f"Processing CRM data for {date_str}")
    
    # Clean customer data
    silver_customers = clean_customer_data()
    
    # Clean order data
    silver_orders = clean_order_data()
    
    # Return success if both processes completed
    return silver_customers is not None and silver_orders is not None

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    clean_crm_data(process_date)