"""
customer_segments.py - Spark script to create customer segmentation for Gold layer

This script:
1. Reads customer and order data from Silver
2. Creates RFM (Recency, Frequency, Monetary) segmentation
3. Creates product affinity segmentation
4. Creates lifecycle stage segmentation
5. Writes segmentation data to Gold zone for BI/reporting

Usage:
    spark-submit customer_segments.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum, count, avg, max, min, datediff, 
    to_date, current_date, explode, split, expr, ntile,
    row_number, rank, dense_rank, first, collect_list, array_contains
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Customer Segmentation") \
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

def load_customer_data():
    """Load customer data from Silver"""
    try:
        customers = spark.read.parquet("/data/silver/customers")
        print(f"Loaded {customers.count()} customer records from Silver")
        return customers
    except Exception as e:
        print(f"Error loading customer data: {e}")
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

def load_order_items_data():
    """Load order items data from Silver"""
    try:
        order_items = spark.read.parquet("/data/silver/order_items")
        print(f"Loaded {order_items.count()} order item records from Silver")
        return order_items
    except Exception as e:
        print(f"Error loading order items data: {e}")
        return None

def create_rfm_segmentation(customers, orders, process_date):
    """Create RFM (Recency, Frequency, Monetary) segmentation"""
    print("Creating RFM segmentation")
    
    if customers is None or orders is None:
        print("Missing required data for RFM segmentation")
        return None
    
    try:
        # Convert process date to string for comparison
        process_date_str = process_date.strftime('%Y-%m-%d')
        
        # Calculate RFM metrics at customer level
        rfm_metrics = orders \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id") \
            .agg(
                datediff(lit(process_date_str), max(col("order_date"))).alias("recency"),
                count("order_id").alias("frequency"),
                sum("final_total").alias("monetary")
            )
        
        # Create quintiles for each metric (1 is best, 5 is worst)
        # Recency - lower is better (more recent)
        recency_window = Window.orderBy(col("recency"))
        # Frequency - higher is better
        frequency_window = Window.orderBy(col("frequency").desc())
        # Monetary - higher is better
        monetary_window = Window.orderBy(col("monetary").desc())
        
        # Add quintile scores (1-5) for each dimension
        rfm_scores = rfm_metrics \
            .withColumn("r_score", ntile(5).over(recency_window)) \
            .withColumn("f_score", ntile(5).over(frequency_window)) \
            .withColumn("m_score", ntile(5).over(monetary_window))
        
        # Calculate composite RFM score
        rfm_scores = rfm_scores \
            .withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))
        
        # Create RFM segments
        rfm_segments = rfm_scores \
            .withColumn("rfm_segment", 
                when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") <= 2), "Champions")
                .when((col("r_score") <= 2) & (col("f_score") <= 3) & (col("m_score") <= 3), "Loyal Customers")
                .when((col("r_score") <= 3) & (col("f_score") <= 1) & (col("m_score") <= 2), "Potential Loyalists")
                .when((col("r_score") <= 2) & (col("f_score") >= 4) & (col("m_score") <= 2), "Recent Customers")
                .when((col("r_score") <= 3) & (col("f_score") >= 4) & (col("m_score") >= 4), "Promising")
                .when((col("r_score") >= 4) & (col("f_score") <= 2) & (col("m_score") <= 2), "Customers Needing Attention")
                .when((col("r_score") >= 4) & (col("f_score") <= 2) & (col("m_score") >= 3), "At Risk")
                .when((col("r_score") >= 4) & (col("f_score") >= 3) & (col("m_score") <= 3), "Can't Lose Them")
                .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "Hibernating")
                .when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "Lost")
                .otherwise("Unknown")
            )
        
        # Join with customer data for final result
        rfm_segment_results = customers \
            .join(rfm_segments, "customer_id") \
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "region",
                "recency",
                "frequency",
                "monetary",
                "r_score",
                "f_score", 
                "m_score",
                "rfm_score",
                "rfm_segment"
            )
        
        print(f"Created RFM segments for {rfm_segment_results.count()} customers")
        return rfm_segment_results
    except Exception as e:
        print(f"Error creating RFM segmentation: {e}")
        return None

def create_product_affinity_segmentation(customers, order_items):
    """Create product affinity segmentation based on purchase history"""
    print("Creating product affinity segmentation")
    
    if customers is None or order_items is None:
        print("Missing required data for product affinity segmentation")
        return None
    
    try:
        # Calculate category purchase frequency by customer
        category_frequency = order_items \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id", "category") \
            .agg(
                count("order_id").alias("purchase_count"),
                sum("quantity").alias("total_quantity"),
                sum("item_total").alias("category_spend")
            )
        
        # Get total spend by customer
        customer_spend = order_items \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id") \
            .agg(sum("item_total").alias("total_spend"))
        
        # Join and calculate category spending percentage
        category_share = category_frequency \
            .join(customer_spend, "customer_id") \
            .withColumn("category_percentage", 
                when(col("total_spend") > 0, col("category_spend") / col("total_spend") * 100)
                .otherwise(0)
            )
        
        # Determine primary and secondary categories for each customer
        customer_window = Window.partitionBy("customer_id").orderBy(col("category_percentage").desc())
        
        # Rank categories by spend percentage
        customer_categories = category_share \
            .withColumn("category_rank", row_number().over(customer_window))
        
        # Get primary category (rank=1) and secondary category (rank=2)
        primary_category = customer_categories \
            .filter(col("category_rank") == 1) \
            .select(
                col("customer_id"),
                col("category").alias("primary_category"),
                col("category_percentage").alias("primary_category_pct")
            )
        
        secondary_category = customer_categories \
            .filter(col("category_rank") == 2) \
            .select(
                col("customer_id"),
                col("category").alias("secondary_category"),
                col("category_percentage").alias("secondary_category_pct")
            )
        
        # Join primary and secondary categories
        customer_product_affinity = primary_category \
            .join(secondary_category, "customer_id", "left")
        
        # Create product affinity segment based on primary category and spending pattern
        customer_product_affinity = customer_product_affinity \
            .withColumn("product_affinity_segment",
                when(col("primary_category") == "soin_visage", "Facial Care Enthusiast")
                .when(col("primary_category") == "soin_corps", "Body Care Focused")
                .when(col("primary_category") == "soin_cheveux", "Hair Care Passionate")
                .when(col("primary_category") == "maquillage", "Makeup Aficionado")
                .when(col("primary_category") == "parfum", "Fragrance Collector")
                .otherwise("Mixed Category Shopper")
            )
        
        # Join with customer data
        customer_product_affinities = customers \
            .join(customer_product_affinity, "customer_id", "left") \
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "region",
                "primary_category",
                "primary_category_pct",
                "secondary_category",
                "secondary_category_pct",
                "product_affinity_segment"
            )
        
        print(f"Created product affinity segments for {customer_product_affinities.count()} customers")
        return customer_product_affinities
    except Exception as e:
        print(f"Error creating product affinity segmentation: {e}")
        return None

def combine_customer_segments(rfm_segments, product_segments, lifecycle_segments):
    """Combine all segmentation models into a single view"""
    print("Combining customer segments")
    
    if rfm_segments is None or product_segments is None or lifecycle_segments is None:
        print("Missing required segmentation data")
        return None
    
    try:
        # Join RFM segments with product affinity segments
        combined_segments = rfm_segments \
            .join(
                product_segments.select(
                    "customer_id", 
                    "primary_category", 
                    "secondary_category", 
                    "product_affinity_segment"
                ), 
                "customer_id", 
                "left"
            )
        
        # Join with lifecycle segments
        combined_segments = combined_segments \
            .join(
                lifecycle_segments.select(
                    "customer_id", 
                    "lifecycle_stage", 
                    "purchase_frequency_segment", 
                    "customer_value_segment",
                    "days_since_first_order",
                    "days_since_last_order",
                    "customer_tenure_days"
                ), 
                "customer_id", 
                "left"
            )
        
        # Add marketing recommendations based on combined segments
        combined_segments = combined_segments \
            .withColumn("marketing_recommendation",
                when(col("lifecycle_stage") == "New Customer", "Welcome Series + Cross-Sell Related Products")
                .when((col("lifecycle_stage") == "Active Customer") & (col("rfm_segment") == "Champions"), 
                      "VIP Program + Early Access to New Products")
                .when((col("lifecycle_stage") == "Active Customer") & (col("rfm_segment") == "Loyal Customers"), 
                      "Loyalty Rewards + Personalized Recommendations")
                .when(col("lifecycle_stage") == "At-Risk Customer", "Re-engagement Campaign + Special Offer")
                .when(col("lifecycle_stage") == "Lapsed Customer", "Win-Back Campaign + Discount")
                .when(col("lifecycle_stage") == "Lost Customer", "Reactivation Campaign + Survey")
                .when(col("rfm_segment") == "Potential Loyalists", "Nurture Campaign + Second Purchase Incentive")
                .when(col("rfm_segment") == "Customers Needing Attention", "Targeted Content + Product Education")
                .when(col("rfm_segment") == "At Risk", "Special Offer + Personalized Communication")
                .when(col("rfm_segment") == "Can't Lose Them", "Deep Discount + Personal Outreach")
                .otherwise("Standard Marketing Communications")
            )
        
        # Add preferred channel recommendation - simplified version
        combined_segments = combined_segments \
            .withColumn("preferred_channel_recommendation",
                when(col("rfm_segment").isin("Champions", "Loyal Customers", "Potential Loyalists"), "Direct Email + SMS")
                .when(col("lifecycle_stage").isin("At-Risk Customer", "Lapsed Customer"), "Retargeting Ads + Email")
                .when(col("customer_value_segment") == "Premium Value", "Personal Outreach + Direct Mail")
                .when(col("customer_value_segment") == "High Value", "Email + Social Media Ads")
                .otherwise("Email + General Advertising")
            )
        
        print(f"Combined segments for {combined_segments.count()} customers")
        return combined_segments
    except Exception as e:
        print(f"Error combining customer segments: {e}")
        return None

def create_customer_segments_gold(process_date):
    """Main function to create customer segmentation for Gold layer"""
    date_str = process_date.strftime('%Y-%m-%d')
    print(f"Creating customer segments for {date_str}")
    
    # Load data from Silver
    customers = load_customer_data()
    orders = load_order_data()
    order_items = load_order_items_data()
    
    if customers is None or orders is None or order_items is None:
        print("Missing required data for segmentation")
        return False
    
    # Create RFM segmentation
    rfm_segments = create_rfm_segmentation(customers, orders, process_date)
    
    # Create product affinity segmentation
    product_segments = create_product_affinity_segmentation(customers, order_items)
    
    # Create lifecycle segmentation
    lifecycle_segments = create_lifecycle_segmentation(customers, orders, process_date)
    
    # Combine all segments
    combined_segments = combine_customer_segments(rfm_segments, product_segments, lifecycle_segments)
    
    # Save to Gold zone
    if combined_segments is not None:
        processing_year = process_date.year
        processing_month = process_date.month
        processing_day = process_date.day
        
        # Add processing date for tracking
        combined_segments = combined_segments \
            .withColumn("processing_date", lit(date_str)) \
            .withColumn("year", lit(processing_year)) \
            .withColumn("month", lit(processing_month)) \
            .withColumn("day", lit(processing_day))
        
        # Write to Gold with appropriate partitioning
        combined_segments.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .save("/data/gold/customer_segments")
        
        print(f"Saved {combined_segments.count()} customer segment records to Gold")
        
        # Also save individual segment views for specialized reporting
        if rfm_segments is not None:
            rfm_segments \
                .withColumn("processing_date", lit(date_str)) \
                .write \
                .format("parquet") \
                .mode("overwrite") \
                .save("/data/gold/rfm_segments")
            
        if lifecycle_segments is not None:
            lifecycle_segments \
                .withColumn("processing_date", lit(date_str)) \
                .write \
                .format("parquet") \
                .mode("overwrite") \
                .save("/data/gold/lifecycle_segments")
        
        return True
    else:
        print("No segment data to save")
        return False

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    create_customer_segments_gold(process_date)

def create_lifecycle_segmentation(customers, orders, process_date):
    """Create customer lifecycle segmentation"""
    print("Creating customer lifecycle segmentation")
    
    if customers is None or orders is None:
        print("Missing required data for lifecycle segmentation")
        return None
    
    try:
        # Convert process date to string for comparison
        process_date_str = process_date.strftime('%Y-%m-%d')
        
        # Calculate key lifecycle metrics
        lifecycle_metrics = orders \
            .filter(col("customer_id").isNotNull()) \
            .groupBy("customer_id") \
            .agg(
                count("order_id").alias("order_count"),
                min(col("order_date")).alias("first_order_date"),
                max(col("order_date")).alias("last_order_date"),
                sum("final_total").alias("total_spend"),
                avg("final_total").alias("avg_order_value")
            ) \
            .withColumn("days_since_first_order", datediff(lit(process_date_str), col("first_order_date"))) \
            .withColumn("days_since_last_order", datediff(lit(process_date_str), col("last_order_date"))) \
            .withColumn("customer_tenure_days", datediff(col("last_order_date"), col("first_order_date")))
        
        # Create lifecycle segments
        lifecycle_segments = lifecycle_metrics \
            .withColumn("lifecycle_stage",
                # New customers (1 order, less than 30 days)
                when((col("order_count") == 1) & (col("days_since_first_order") <= 30), "New Customer")
                # Active customers (ordered in last 90 days, more than 1 order)
                .when((col("days_since_last_order") <= 90) & (col("order_count") > 1), "Active Customer")
                # At-risk customers (91-180 days since last order)
                .when((col("days_since_last_order").between(91, 180)), "At-Risk Customer")
                # Lapsed customers (181-365 days since last order)
                .when((col("days_since_last_order").between(181, 365)), "Lapsed Customer")
                # Lost customers (>365 days since last order)
                .when((col("days_since_last_order") > 365), "Lost Customer")
                # Others
                .otherwise("Unknown")
            ) \
            .withColumn("purchase_frequency_segment",
                when(col("order_count") == 1, "One-Time Buyer")
                .when((col("order_count").between(2, 3)), "Occasional Buyer")
                .when((col("order_count").between(4, 6)), "Regular Buyer")
                .when(col("order_count") > 6, "Frequent Buyer")
                .otherwise("Unknown")
            ) \
            .withColumn("customer_value_segment",
                when(col("total_spend") <= 10000, "Low Value")
                .when(col("total_spend").between(10001, 50000), "Medium Value")
                .when(col("total_spend").between(50001, 150000), "High Value")
                .when(col("total_spend") > 150000, "Premium Value")
                .otherwise("Unknown")
            )
        
        # Join with customer data
        customer_lifecycle = customers \
            .join(lifecycle_segments, "customer_id", "left") \
            .select(
                "customer_id",
                "first_name", 
                "last_name",
                "email",
                "city",
                "region",
                "order_count",
                "first_order_date",
                "last_order_date",
                "total_spend",
                "avg_order_value",
                "days_since_first_order",
                "days_since_last_order",
                "customer_tenure_days",
                "lifecycle_stage",
                "purchase_frequency_segment",
                "customer_value_segment"
            )
        
        print(f"Created lifecycle segments for {customer_lifecycle.count()} customers")
        return customer_lifecycle
    except Exception as e:
        print(f"Error creating lifecycle segmentation: {e}")
        return None