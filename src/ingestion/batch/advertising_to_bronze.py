"""
advertising_to_bronze.py - Spark script to load advertising data to MinIO Bronze bucket

This script:
1. Loads advertising data (Google Ads, Social Media, Influencers) from CSV files
2. Performs initial validation
3. Writes the data to the Bronze zone in MinIO data lake in Parquet format

Usage:
    spark-submit advertising_to_bronze.py [file_paths_json]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, regexp_replace, when, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType, BooleanType
import json
import sys
import os
import glob
from datetime import datetime

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Advertising Data to Bronze") \
    .master("spark://spark-master:7077") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.speculation", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define storage paths
# First try with S3/MinIO paths
MINIO_BRONZE_BUCKET = "s3a://bronze"
MINIO_GOOGLE_ADS_PATH = f"{MINIO_BRONZE_BUCKET}/google_ads"
MINIO_SOCIAL_ADS_PATH = f"{MINIO_BRONZE_BUCKET}/social_ads"
MINIO_INFLUENCER_PATH = f"{MINIO_BRONZE_BUCKET}/influencer"
MINIO_ALL_PLATFORMS_PATH = f"{MINIO_BRONZE_BUCKET}/all_platforms"

# Fallback to local paths if S3 fails
LOCAL_BRONZE_PATH = "/data/bronze"
LOCAL_GOOGLE_ADS_PATH = f"{LOCAL_BRONZE_PATH}/google_ads"
LOCAL_SOCIAL_ADS_PATH = f"{LOCAL_BRONZE_PATH}/social_ads"
LOCAL_INFLUENCER_PATH = f"{LOCAL_BRONZE_PATH}/influencer"
LOCAL_ALL_PLATFORMS_PATH = f"{LOCAL_BRONZE_PATH}/all_platforms"

def define_google_ads_schema():
    """Define the schema for Google Ads data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("ad_type", StringType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("ctr", DoubleType(), True),
        StructField("avg_cpc", DoubleType(), True),
        StructField("cost", DoubleType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("target_audience", StringType(), True),
        StructField("category", StringType(), True),
        StructField("campaign_day", IntegerType(), True),
        StructField("campaign_duration", IntegerType(), True),
        StructField("keywords", StringType(), True)
    ])

def define_social_ads_schema():
    """Define the schema for Social Media Ads data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("ad_type", StringType(), True),
        StructField("content_theme", StringType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("ctr", DoubleType(), True),
        StructField("avg_cpc", DoubleType(), True),
        StructField("cost", DoubleType(), True),
        StructField("engagements", IntegerType(), True),
        StructField("likes", IntegerType(), True),
        StructField("comments", IntegerType(), True),
        StructField("shares", IntegerType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("target_audience", StringType(), True),
        StructField("category", StringType(), True),
        StructField("campaign_day", IntegerType(), True),
        StructField("campaign_duration", IntegerType(), True)
    ])

def define_influencer_schema():
    """Define the schema for Influencer Marketing data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("influencer_id", StringType(), True),
        StructField("influencer_name", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("engagements", IntegerType(), True),
        StructField("engagement_rate", DoubleType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("ctr", DoubleType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("fee", DoubleType(), True),
        StructField("duration", IntegerType(), True),
        StructField("promo_code", StringType(), True),
        StructField("category", StringType(), True),
        StructField("campaign_day", IntegerType(), True),
        StructField("is_post_day", BooleanType(), True)
    ])

def define_all_platforms_schema():
    """Define the schema for All Platforms combined data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("ad_type", StringType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("cost", DoubleType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("target_audience", StringType(), True),
        StructField("category", StringType(), True)
    ])

def load_ad_data(file_paths, schema, ad_type):
    """Load advertising data from CSV files with improved error handling"""
    # Make sure file_paths is a list
    if isinstance(file_paths, str):
        file_paths = [file_paths]
        
    print(f"Attempting to load {len(file_paths)} {ad_type} files")
        
    # Check if files exist before attempting to load
    existing_files = [f for f in file_paths if os.path.exists(f)]
    print(f"Found {len(existing_files)} existing {ad_type} files")
        
    if not existing_files:
        print(f"No {ad_type} files found!")
        return None
        
    # Process in smaller batches to avoid overwhelming Spark
    batch_size = 10
    all_data_frames = []
        
    for i in range(0, len(existing_files), batch_size):
        batch = existing_files[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(existing_files) + batch_size - 1)//batch_size} with {len(batch)} files")
            
        try:
            # Read CSV files with the schema
            batch_df = spark.read.schema(schema).csv(batch, header=True)
            row_count = batch_df.count()
            print(f"Loaded {row_count} {ad_type} records from batch")
                
            all_data_frames.append(batch_df)
        except Exception as e:
            print(f"Error processing batch: {e}")
            # Continue with next batch
        
    if not all_data_frames:
        print(f"No data could be loaded from any {ad_type} batch")
        return None
        
    # Union all batches
    df = all_data_frames[0]
    for batch_df in all_data_frames[1:]:
        df = df.union(batch_df)
        
    # Basic validation
    row_count = df.count()
    print(f"Loaded {row_count} {ad_type} records in total")
        
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("data_source", lit(ad_type)) \
        .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \
        .withColumn("ingestion_timestamp", current_timestamp())
        
    return df_with_metadata

def process_advertising_data(file_paths):
    """Process advertising data and save to MinIO Bronze bucket"""
    # Parse file paths if provided as string
    if isinstance(file_paths, str):
        if ',' in file_paths:
            file_paths = file_paths.split(',')
        else:
            file_paths = [file_paths]
    
    if not file_paths:
        # Default to latest files in the advertising data directory
        data_dir = "/data/raw/advertising"
        file_paths = glob.glob(f"{data_dir}/google_ads_*.csv")
        file_paths += glob.glob(f"{data_dir}/social_ads_*.csv")
        file_paths += glob.glob(f"{data_dir}/influencer_data_*.csv")
        file_paths += glob.glob(f"{data_dir}/all_platforms_*.csv")
        
        if not file_paths:
            print(f"No advertising files found in {data_dir}")
            sys.exit(0)
        else:
            print(f"Found {len(file_paths)} advertising files to process")
    
    print(f"Processing {len(file_paths)} files")
    
    # Separate files by type
    google_files = [f for f in file_paths if 'google_ads' in f]
    social_files = [f for f in file_paths if 'social_ads' in f]
    influencer_files = [f for f in file_paths if 'influencer_data' in f]
    all_platforms_files = [f for f in file_paths if 'all_platforms' in f]
    
    # Extract date for partitioning
    current_date = datetime.now()
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    
    # Process Google Ads data
    if google_files:
        print(f"Processing {len(google_files)} Google Ads files")
        google_schema = define_google_ads_schema()
        google_df = load_ad_data(google_files, google_schema, "google_ads")
        
        if google_df is not None:
            # Add partition columns
            google_df_with_partitions = google_df \
                .withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day))
            
            # Try to write to MinIO first, if it fails, write to local file system
            try:
                # Try writing to MinIO
                google_df_with_partitions.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("year", "month", "day") \
                    .save(MINIO_GOOGLE_ADS_PATH)
                
                print(f"Successfully wrote {google_df.count()} Google Ads records to MinIO Bronze bucket at {MINIO_GOOGLE_ADS_PATH}")
            except Exception as e:
                print(f"Error writing Google Ads data to MinIO: {e}")
                try:
                    # Fallback to local file system
                    print(f"Attempting to write to local file system at {LOCAL_GOOGLE_ADS_PATH}")
                    google_df_with_partitions.write \
                        .format("parquet") \
                        .mode("overwrite") \
                        .partitionBy("year", "month", "day") \
                        .save(LOCAL_GOOGLE_ADS_PATH)
                    
                    print(f"Successfully wrote {google_df.count()} Google Ads records to local Bronze path at {LOCAL_GOOGLE_ADS_PATH}")
                except Exception as e2:
                    print(f"Error writing Google Ads data to local file system: {e2}")
    
    # Process Social Media Ads data
    if social_files:
        print(f"Processing {len(social_files)} Social Media Ads files")
        social_schema = define_social_ads_schema()
        social_df = load_ad_data(social_files, social_schema, "social_ads")
        
        if social_df is not None:
            # Add partition columns
            social_df_with_partitions = social_df \
                .withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day))
            
            # Try to write to MinIO first, if it fails, write to local file system
            try:
                # Try writing to MinIO
                social_df_with_partitions.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("year", "month", "day") \
                    .save(MINIO_SOCIAL_ADS_PATH)
                
                print(f"Successfully wrote {social_df.count()} Social Media Ads records to MinIO Bronze bucket at {MINIO_SOCIAL_ADS_PATH}")
            except Exception as e:
                print(f"Error writing Social Media Ads data to MinIO: {e}")
                try:
                    # Fallback to local file system
                    print(f"Attempting to write to local file system at {LOCAL_SOCIAL_ADS_PATH}")
                    social_df_with_partitions.write \
                        .format("parquet") \
                        .mode("overwrite") \
                        .partitionBy("year", "month", "day") \
                        .save(LOCAL_SOCIAL_ADS_PATH)
                    
                    print(f"Successfully wrote {social_df.count()} Social Media Ads records to local Bronze path at {LOCAL_SOCIAL_ADS_PATH}")
                except Exception as e2:
                    print(f"Error writing Social Media Ads data to local file system: {e2}")
    
    # Process Influencer Marketing data
    if influencer_files:
        print(f"Processing {len(influencer_files)} Influencer Marketing files")
        influencer_schema = define_influencer_schema()
        influencer_df = load_ad_data(influencer_files, influencer_schema, "influencer")
        
        if influencer_df is not None:
            # Add partition columns
            influencer_df_with_partitions = influencer_df \
                .withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day))
            
            # Try to write to MinIO first, if it fails, write to local file system
            try:
                # Try writing to MinIO
                influencer_df_with_partitions.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("year", "month", "day") \
                    .save(MINIO_INFLUENCER_PATH)
                
                print(f"Successfully wrote {influencer_df.count()} Influencer Marketing records to MinIO Bronze bucket at {MINIO_INFLUENCER_PATH}")
            except Exception as e:
                print(f"Error writing Influencer Marketing data to MinIO: {e}")
                try:
                    # Fallback to local file system
                    print(f"Attempting to write to local file system at {LOCAL_INFLUENCER_PATH}")
                    influencer_df_with_partitions.write \
                        .format("parquet") \
                        .mode("overwrite") \
                        .partitionBy("year", "month", "day") \
                        .save(LOCAL_INFLUENCER_PATH)
                    
                    print(f"Successfully wrote {influencer_df.count()} Influencer Marketing records to local Bronze path at {LOCAL_INFLUENCER_PATH}")
                except Exception as e2:
                    print(f"Error writing Influencer Marketing data to local file system: {e2}")
    
    # Process All Platforms combined data
    if all_platforms_files:
        print(f"Processing {len(all_platforms_files)} All Platforms files")
        all_platforms_schema = define_all_platforms_schema()
        all_platforms_df = load_ad_data(all_platforms_files, all_platforms_schema, "all_platforms")
        
        if all_platforms_df is not None:
            # Add partition columns
            all_platforms_df_with_partitions = all_platforms_df \
                .withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day))
            
            # Try to write to MinIO first, if it fails, write to local file system
            try:
                # Try writing to MinIO
                all_platforms_df_with_partitions.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .partitionBy("year", "month", "day") \
                    .save(MINIO_ALL_PLATFORMS_PATH)
                
                print(f"Successfully wrote {all_platforms_df.count()} All Platforms records to MinIO Bronze bucket at {MINIO_ALL_PLATFORMS_PATH}")
            except Exception as e:
                print(f"Error writing All Platforms data to MinIO: {e}")
                try:
                    # Fallback to local file system
                    print(f"Attempting to write to local file system at {LOCAL_ALL_PLATFORMS_PATH}")
                    all_platforms_df_with_partitions.write \
                        .format("parquet") \
                        .mode("overwrite") \
                        .partitionBy("year", "month", "day") \
                        .save(LOCAL_ALL_PLATFORMS_PATH)
                    
                    print(f"Successfully wrote {all_platforms_df.count()} All Platforms records to local Bronze path at {LOCAL_ALL_PLATFORMS_PATH}")
                except Exception as e2:
                    print(f"Error writing All Platforms data to local file system: {e2}")

if __name__ == "__main__":
    # Get file paths from command line arguments
    file_paths = sys.argv[1] if len(sys.argv) > 1 else None
    
    # If no specific files provided, find latest files
    if not file_paths:
        # Default to latest files in the advertising data directory
        data_dir = "/data/raw/advertising"
        file_paths = glob.glob(f"{data_dir}/google_ads_*.csv")
        file_paths += glob.glob(f"{data_dir}/social_ads_*.csv")
        file_paths += glob.glob(f"{data_dir}/influencer_data_*.csv")
        file_paths += glob.glob(f"{data_dir}/all_platforms_*.csv")
        
        if not file_paths:
            print(f"No advertising files found")
            sys.exit(0)
    
    # Check if MinIO Bronze bucket exists, create if not
    try:
        from pyspark.sql.functions import expr
        
        # Simple test to check connectivity
        dummy_df = spark.createDataFrame([("test",)], ["col1"])
        dummy_df.write.format("parquet").mode("overwrite").save(f"{MINIO_BRONZE_BUCKET}/test")
        print(f"Successfully connected to MinIO at {MINIO_BRONZE_BUCKET}")
    except Exception as e:
        print(f"Warning: Could not verify MinIO bucket access. The bucket may need to be created manually: {e}")
        
    process_advertising_data(file_paths)