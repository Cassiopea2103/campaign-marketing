"""
advertising_to_bronze.py - Spark script to load advertising data to Bronze storage

This script:
1. Loads advertising data from various sources (Google Ads, Social Media, Influencers)
2. Performs initial validation
3. Writes the data to the Bronze zone of the data lake in Parquet format

Usage:
    spark-submit advertising_to_bronze.py [file_paths_json]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
import json
import sys
import os
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Advertising Data to Bronze") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

def define_google_ads_schema():
    """Define schema for Google Ads data"""
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
        StructField("conversions", IntegerType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("conversion_value", DoubleType(), True),
        StructField("target_audience", StringType(), True),
        StructField("category", StringType(), True),
        StructField("campaign_day", IntegerType(), True),
        StructField("campaign_duration", IntegerType(), True),
        StructField("keywords", StringType(), True)  # For Search ads
    ])

def define_social_ads_schema():
    """Define schema for Social Media Ads data"""
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