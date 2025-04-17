"""
clean_advertising_data.py - Spark script to clean advertising data from Bronze to Silver

This script:
1. Reads advertising data (Google Ads, Social Media, Influencers) from the Bronze zone
2. Cleans and standardizes data
3. Enriches with additional metrics
4. Writes processed data to the Silver zone of the data lake

Usage:
    spark-submit clean_advertising_data.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, regexp_replace, lower, trim, 
    to_timestamp, date_format, datediff, current_timestamp,
    explode, split, from_json, struct, to_json,
    concat, expr, year, month, dayofmonth, round, avg as sql_avg, sum as sql_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
import sys
import os
from datetime import datetime

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Clean Advertising Data") \
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
MINIO_BRONZE_BUCKET = "s3a://bronze"
MINIO_SILVER_BUCKET = "s3a://silver"

MINIO_BRONZE_GOOGLE_ADS = f"{MINIO_BRONZE_BUCKET}/google_ads"
MINIO_BRONZE_SOCIAL_ADS = f"{MINIO_BRONZE_BUCKET}/social_ads"
MINIO_BRONZE_INFLUENCER = f"{MINIO_BRONZE_BUCKET}/influencer"
MINIO_BRONZE_ALL_PLATFORMS = f"{MINIO_BRONZE_BUCKET}/all_platforms"

MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_SILVER_ALL_PLATFORMS = f"{MINIO_SILVER_BUCKET}/all_platforms"

def clean_google_ads_data():
    """Clean Google Ads data from Bronze"""
    print("Processing Google Ads data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_google_ads = spark.read.parquet(MINIO_BRONZE_GOOGLE_ADS)
        
        # Log record count
        record_count = bronze_google_ads.count()
        print(f"Loaded {record_count} Google Ads records from Bronze")
        
        if record_count == 0:
            print("No Google Ads records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze Google Ads data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_bronze_path = "/data/bronze/google_ads"
            print(f"Attempting to read from local path: {local_bronze_path}")
            bronze_google_ads = spark.read.parquet(local_bronze_path)
            record_count = bronze_google_ads.count()
            print(f"Loaded {record_count} Google Ads records from local Bronze")
            if record_count == 0:
                print("No Google Ads records to process")
                return None
        except Exception as e2:
            print(f"Error reading local Bronze Google Ads data: {e2}")
            return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_google_ads \
        .dropDuplicates(["date", "campaign_id", "ad_type"]) \
        .filter(col("campaign_id").isNotNull() & col("date").isNotNull())
    
    # 2. Standardize and parse date
    cleaned_df = cleaned_df \
        .withColumn("ad_date", to_timestamp(col("date"))) \
        .withColumn("date_str", date_format(col("ad_date"), "yyyy-MM-dd"))
    
    # 3. Add derived metrics
    cleaned_df = cleaned_df \
        .withColumn("roas", when(col("cost") > 0, col("conversion_value") / col("cost")).otherwise(0)) \
        .withColumn("cpa", when(col("conversions") > 0, col("cost") / col("conversions")).otherwise(None)) \
        .withColumn("cpc", when(col("clicks") > 0, col("cost") / col("clicks")).otherwise(col("avg_cpc")))
    
    # 4. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("ad_year", year(col("ad_date"))) \
        .withColumn("ad_month", month(col("ad_date"))) \
        .withColumn("ad_day", dayofmonth(col("ad_date")))
    
    # 5. Add last updated timestamp
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_timestamp())
    
    # 6. Round numeric metrics for consistency
    numeric_cols = ["impressions", "clicks", "cost", "conversions", "conversion_value", "roas", "cpa", "cpc"]
    for col_name in numeric_cols:
        cleaned_df = cleaned_df.withColumn(col_name, round(col(col_name), 2))
    
    # Select columns for Silver layer
    silver_google_ads = cleaned_df.select(
        "date_str",
        "ad_date",
        "platform",
        "campaign_id",
        "campaign_name",
        "ad_type",
        "impressions",
        "clicks",
        "ctr", 
        "cpc",
        "cost",
        "conversions",
        "conversion_rate",
        "conversion_value",
        "roas",
        "cpa",
        "target_audience",
        "category",
        "campaign_day",
        "campaign_duration",
        "keywords",
        "ad_year",
        "ad_month",
        "ad_day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    try:
        silver_google_ads.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("ad_year", "ad_month") \
            .save(MINIO_SILVER_GOOGLE_ADS)
        
        print(f"Successfully wrote {silver_google_ads.count()} Google Ads records to Silver at {MINIO_SILVER_GOOGLE_ADS}")
    except Exception as e:
        print(f"Error writing Google Ads data to MinIO Silver: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/google_ads"
            print(f"Attempting to write to local path: {local_silver_path}")
            silver_google_ads.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ad_year", "ad_month") \
                .save(local_silver_path)
            print(f"Successfully wrote {silver_google_ads.count()} Google Ads records to local Silver")
        except Exception as e2:
            print(f"Error writing Google Ads data to local Silver: {e2}")
            return None
    
    return silver_google_ads

def clean_social_ads_data():
    """Clean Social Media Ads data from Bronze"""
    print("Processing Social Media Ads data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_social_ads = spark.read.parquet(MINIO_BRONZE_SOCIAL_ADS)
        
        # Log record count
        record_count = bronze_social_ads.count()
        print(f"Loaded {record_count} Social Media Ads records from Bronze")
        
        if record_count == 0:
            print("No Social Media Ads records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze Social Media Ads data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_bronze_path = "/data/bronze/social_ads"
            print(f"Attempting to read from local path: {local_bronze_path}")
            bronze_social_ads = spark.read.parquet(local_bronze_path)
            record_count = bronze_social_ads.count()
            print(f"Loaded {record_count} Social Media Ads records from local Bronze")
            if record_count == 0:
                print("No Social Media Ads records to process")
                return None
        except Exception as e2:
            print(f"Error reading local Bronze Social Media Ads data: {e2}")
            return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_social_ads \
        .dropDuplicates(["date", "campaign_id", "ad_type"]) \
        .filter(col("campaign_id").isNotNull() & col("date").isNotNull())
    
    # 2. Standardize and parse date
    cleaned_df = cleaned_df \
        .withColumn("ad_date", to_timestamp(col("date"))) \
        .withColumn("date_str", date_format(col("ad_date"), "yyyy-MM-dd"))
    
    # 3. Add derived metrics
    cleaned_df = cleaned_df \
        .withColumn("roas", when(col("cost") > 0, col("conversion_value") / col("cost")).otherwise(0)) \
        .withColumn("cpa", when(col("conversions") > 0, col("cost") / col("conversions")).otherwise(None)) \
        .withColumn("cpc", when(col("clicks") > 0, col("cost") / col("clicks")).otherwise(col("avg_cpc"))) \
        .withColumn("engagement_rate", when(col("impressions") > 0, col("engagements") / col("impressions")).otherwise(None))
    
    # 4. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("ad_year", year(col("ad_date"))) \
        .withColumn("ad_month", month(col("ad_date"))) \
        .withColumn("ad_day", dayofmonth(col("ad_date")))
    
    # 5. Add last updated timestamp
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_timestamp())
    
    # 6. Round numeric metrics for consistency
    numeric_cols = ["impressions", "clicks", "cost", "conversions", "conversion_value", 
                   "roas", "cpa", "cpc", "engagements", "likes", "comments", "shares"]
    for col_name in numeric_cols:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(col_name, round(col(col_name), 2))
    
    # Select columns for Silver layer
    silver_social_ads = cleaned_df.select(
        "date_str",
        "ad_date",
        "platform",
        "campaign_id",
        "campaign_name",
        "ad_type",
        "content_theme",
        "impressions",
        "clicks",
        "ctr",
        "cpc",
        "cost",
        "engagements",
        "likes",
        "comments",
        "shares",
        "engagement_rate",
        "conversions",
        "conversion_rate",
        "conversion_value",
        "roas",
        "cpa",
        "target_audience",
        "category",
        "campaign_day",
        "campaign_duration",
        "ad_year",
        "ad_month",
        "ad_day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    try:
        silver_social_ads.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("ad_year", "ad_month") \
            .save(MINIO_SILVER_SOCIAL_ADS)
        
        print(f"Successfully wrote {silver_social_ads.count()} Social Media Ads records to Silver at {MINIO_SILVER_SOCIAL_ADS}")
    except Exception as e:
        print(f"Error writing Social Media Ads data to MinIO Silver: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/social_ads"
            print(f"Attempting to write to local path: {local_silver_path}")
            silver_social_ads.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ad_year", "ad_month") \
                .save(local_silver_path)
            print(f"Successfully wrote {silver_social_ads.count()} Social Media Ads records to local Silver")
        except Exception as e2:
            print(f"Error writing Social Media Ads data to local Silver: {e2}")
            return None
    
    return silver_social_ads

def clean_influencer_data():
    """Clean Influencer Marketing data from Bronze"""
    print("Processing Influencer Marketing data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_influencer = spark.read.parquet(MINIO_BRONZE_INFLUENCER)
        
        # Log record count
        record_count = bronze_influencer.count()
        print(f"Loaded {record_count} Influencer Marketing records from Bronze")
        
        if record_count == 0:
            print("No Influencer Marketing records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze Influencer Marketing data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_bronze_path = "/data/bronze/influencer"
            print(f"Attempting to read from local path: {local_bronze_path}")
            bronze_influencer = spark.read.parquet(local_bronze_path)
            record_count = bronze_influencer.count()
            print(f"Loaded {record_count} Influencer Marketing records from local Bronze")
            if record_count == 0:
                print("No Influencer Marketing records to process")
                return None
        except Exception as e2:
            print(f"Error reading local Bronze Influencer Marketing data: {e2}")
            return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_influencer \
        .dropDuplicates(["date", "campaign_id", "influencer_id"]) \
        .filter(col("campaign_id").isNotNull() & col("date").isNotNull())
    
    # 2. Standardize and parse date
    cleaned_df = cleaned_df \
        .withColumn("ad_date", to_timestamp(col("date"))) \
        .withColumn("date_str", date_format(col("ad_date"), "yyyy-MM-dd"))
    
    # 3. Add derived metrics
    cleaned_df = cleaned_df \
        .withColumn("daily_cost", col("fee") / col("duration")) \
        .withColumn("roi", when(col("fee") > 0, (col("conversion_value") - col("fee")) / col("fee")).otherwise(0)) \
        .withColumn("cpa", when(col("conversions") > 0, col("fee") / col("conversions")).otherwise(None)) \
        .withColumn("cpc", when(col("clicks") > 0, col("fee") / col("clicks")).otherwise(None))
    
    # 4. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("ad_year", year(col("ad_date"))) \
        .withColumn("ad_month", month(col("ad_date"))) \
        .withColumn("ad_day", dayofmonth(col("ad_date")))
    
    # 5. Add last updated timestamp
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_timestamp())
    
    # 6. Round numeric metrics for consistency
    numeric_cols = ["impressions", "engagements", "clicks", "conversions", "conversion_value", 
                "fee", "daily_cost", "roi", "cpa", "cpc"]
    for col_name in numeric_cols:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(col_name, round(col(col_name), 2))
    
    # Select columns for Silver layer
    silver_influencer = cleaned_df.select(
        "date_str",
        "ad_date",
        "campaign_id",
        "campaign_name",
        "influencer_id",
        "influencer_name",
        "platform",
        "content_type",
        "impressions",
        "engagements",
        "engagement_rate",
        "clicks",
        "ctr",
        "conversions",
        "conversion_rate",
        "conversion_value",
        "fee",
        "daily_cost",
        "roi",
        "cpa",
        "cpc",
        "duration",
        "promo_code",
        "category",
        "campaign_day",
        "is_post_day",
        "ad_year",
        "ad_month",
        "ad_day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    try:
        silver_influencer.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("ad_year", "ad_month") \
            .save(MINIO_SILVER_INFLUENCER)
        
        print(f"Successfully wrote {silver_influencer.count()} Influencer Marketing records to Silver at {MINIO_SILVER_INFLUENCER}")
    except Exception as e:
        print(f"Error writing Influencer Marketing data to MinIO Silver: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/influencer"
            print(f"Attempting to write to local path: {local_silver_path}")
            silver_influencer.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ad_year", "ad_month") \
                .save(local_silver_path)
            print(f"Successfully wrote {silver_influencer.count()} Influencer Marketing records to local Silver")
        except Exception as e2:
            print(f"Error writing Influencer Marketing data to local Silver: {e2}")
            return None
    
    return silver_influencer

def clean_all_platforms_data():
    """Clean All Platforms combined data from Bronze"""
    print("Processing All Platforms combined data from Bronze")
    
    # Read data from Bronze zone
    try:
        bronze_all_platforms = spark.read.parquet(MINIO_BRONZE_ALL_PLATFORMS)
        
        # Log record count
        record_count = bronze_all_platforms.count()
        print(f"Loaded {record_count} All Platforms records from Bronze")
        
        if record_count == 0:
            print("No All Platforms records to process")
            return None
    except Exception as e:
        print(f"Error reading Bronze All Platforms data from MinIO: {e}")
        # Try local filesystem as fallback
        try:
            local_bronze_path = "/data/bronze/all_platforms"
            print(f"Attempting to read from local path: {local_bronze_path}")
            bronze_all_platforms = spark.read.parquet(local_bronze_path)
            record_count = bronze_all_platforms.count()
            print(f"Loaded {record_count} All Platforms records from local Bronze")
            if record_count == 0:
                print("No All Platforms records to process")
                return None
        except Exception as e2:
            print(f"Error reading local Bronze All Platforms data: {e2}")
            return None
    
    # Data cleaning and standardization
    
    # 1. Basic data cleaning
    cleaned_df = bronze_all_platforms \
        .dropDuplicates(["date", "platform", "campaign_id"]) \
        .filter(col("campaign_id").isNotNull() & col("date").isNotNull())
    
    # 2. Standardize and parse date
    cleaned_df = cleaned_df \
        .withColumn("ad_date", to_timestamp(col("date"))) \
        .withColumn("date_str", date_format(col("ad_date"), "yyyy-MM-dd"))
    
    # 3. Add derived metrics
    cleaned_df = cleaned_df \
        .withColumn("roas", when(col("cost") > 0, col("conversion_value") / col("cost")).otherwise(0)) \
        .withColumn("cpa", when(col("conversions") > 0, col("cost") / col("conversions")).otherwise(None)) \
        .withColumn("cpc", when(col("clicks") > 0, col("cost") / col("clicks")).otherwise(None)) \
        .withColumn("ctr", when(col("impressions") > 0, col("clicks") / col("impressions")).otherwise(None))
    
    # 4. Normalize platform names
    cleaned_df = cleaned_df \
        .withColumn("platform_category", 
                    when(col("platform").isin(["Facebook", "Instagram", "TikTok", "WhatsApp"]), "Social Media")
                    .when(col("platform").isin(["Google"]), "Search")
                    .when(col("platform").startswith("Influencer"), "Influencer")
                    .otherwise("Other"))
    
    # 5. Create date fields for partitioning
    cleaned_df = cleaned_df \
        .withColumn("ad_year", year(col("ad_date"))) \
        .withColumn("ad_month", month(col("ad_date"))) \
        .withColumn("ad_day", dayofmonth(col("ad_date")))
    
    # 6. Add last updated timestamp
    cleaned_df = cleaned_df \
        .withColumn("silver_updated_at", current_timestamp())
    
    # 7. Round numeric metrics for consistency
    numeric_cols = ["impressions", "clicks", "cost", "conversions", "conversion_value", 
                "roas", "cpa", "cpc", "ctr"]
    for col_name in numeric_cols:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(col_name, round(col(col_name), 2))
    
    # Select columns for Silver layer
    silver_all_platforms = cleaned_df.select(
        "date_str",
        "ad_date",
        "platform",
        "platform_category",
        "campaign_id",
        "campaign_name",
        "ad_type",
        "impressions",
        "clicks",
        "ctr",
        "cost",
        "cpc",
        "conversions",
        "cpa",
        "conversion_value",
        "roas",
        "target_audience",
        "category",
        "ad_year",
        "ad_month",
        "ad_day",
        "silver_updated_at"
    )
    
    # Write to Silver zone in Parquet format with partitioning
    try:
        silver_all_platforms.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("ad_year", "ad_month") \
            .save(MINIO_SILVER_ALL_PLATFORMS)
        
        print(f"Successfully wrote {silver_all_platforms.count()} All Platforms records to Silver at {MINIO_SILVER_ALL_PLATFORMS}")
    except Exception as e:
        print(f"Error writing All Platforms data to MinIO Silver: {e}")
        # Try local filesystem as fallback
        try:
            local_silver_path = "/data/silver/all_platforms"
            print(f"Attempting to write to local path: {local_silver_path}")
            silver_all_platforms.write \
                .format("parquet") \
                .mode("overwrite") \
                .partitionBy("ad_year", "ad_month") \
                .save(local_silver_path)
            print(f"Successfully wrote {silver_all_platforms.count()} All Platforms records to local Silver")
        except Exception as e2:
            print(f"Error writing All Platforms data to local Silver: {e2}")
            return None
    
    return silver_all_platforms

def create_cross_platform_metrics():
    """Create cross-platform performance metrics for comparison"""
    # Only create these metrics if all platform data is available
            
    try:
        # Get data from the all_platforms table
        all_platforms_df = spark.read.parquet(MINIO_SILVER_ALL_PLATFORMS)
                
        # Ensure numeric columns are actually numeric
        numeric_cols = ["impressions", "clicks", "conversions", "cost", "conversion_value"]
        for col_name in numeric_cols:
            all_platforms_df = all_platforms_df.withColumn(
                col_name, 
                col(col_name).cast("double")
            )
                
        # Calculate metrics by platform
        platform_metrics = all_platforms_df \
            .groupBy("platform") \
            .agg(
               sql_sum("impressions").alias("total_impressions"),
                sql_sum("clicks").alias("total_clicks"),
                sql_sum("conversions").alias("total_conversions"),
                sql_sum("cost").alias("total_cost"),
                sql_sum("conversion_value").alias("total_revenue"),
                sql_avg("ctr").alias("avg_ctr"),
                sql_avg("cpc").alias("avg_cpc"),
                sql_avg("cpa").alias("avg_cpa")
            ) \
            .withColumn("roi", when(col("total_cost") > 0, (col("total_revenue") - col("total_cost")) / col("total_cost")).otherwise(0)) \
            .withColumn("conversion_rate", when(col("total_clicks") > 0, col("total_conversions") / col("total_clicks")).otherwise(0))
                
        # Write the metrics to a special table
        platform_metrics.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{MINIO_SILVER_BUCKET}/platform_metrics")
                
        print(f"Successfully created cross-platform metrics for {platform_metrics.count()} platforms")
                
        return platform_metrics
    except Exception as e:
        print(f"Error creating cross-platform metrics: {e}")
        # Print more debug information
        print("Error details:")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Clean Google Ads data
    silver_google_ads = clean_google_ads_data()
    
    # Clean Social Media Ads data
    silver_social_ads = clean_social_ads_data()
    
    # Clean Influencer Marketing data
    silver_influencer = clean_influencer_data()
    
    # Clean All Platforms combined data
    silver_all_platforms = clean_all_platforms_data()
    
    # Create cross-platform performance metrics
    if silver_all_platforms is not None:
        cross_platform_metrics = create_cross_platform_metrics()
    
    # Return success if all processes completed successfully
    success = (silver_google_ads is not None and 
            silver_social_ads is not None and 
            silver_influencer is not None and 
            silver_all_platforms is not None)
    
    sys.exit(0 if success else 1)