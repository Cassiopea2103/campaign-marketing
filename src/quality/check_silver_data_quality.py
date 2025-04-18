"""
check_silver_data_quality.py - Spark script to validate Silver data quality

This script:
1. Checks data completeness and integrity in Silver zone
2. Validates relationships between datasets
3. Ensures required fields are populated
4. Identifies missing values, duplicates, and outliers
5. Reports data quality metrics and issues

Usage:
    spark-submit check_silver_data_quality.py [date_string]
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, min, max, avg, 
    stddev, lit, isnan, when, isnull, datediff,
    to_date, current_date, expr, length, year, month, dayofmonth
)
import sys
import os
import json
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Silver Data Quality Check") \
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
MINIO_SILVER_WEB_LOGS = f"{MINIO_SILVER_BUCKET}/web_logs"
MINIO_SILVER_CUSTOMERS = f"{MINIO_SILVER_BUCKET}/customers"
MINIO_SILVER_ORDERS = f"{MINIO_SILVER_BUCKET}/orders"
MINIO_SILVER_ORDER_ITEMS = f"{MINIO_SILVER_BUCKET}/order_items"
MINIO_SILVER_GOOGLE_ADS = f"{MINIO_SILVER_BUCKET}/google_ads"
MINIO_SILVER_SOCIAL_ADS = f"{MINIO_SILVER_BUCKET}/social_ads"
MINIO_SILVER_INFLUENCER = f"{MINIO_SILVER_BUCKET}/influencer"
MINIO_SILVER_ALL_PLATFORMS = f"{MINIO_SILVER_BUCKET}/all_platforms"
MINIO_SILVER_WEB_SESSIONS = f"{MINIO_SILVER_BUCKET}/web_sessions"
MINIO_SILVER_CUSTOMER_SESSIONS = f"{MINIO_SILVER_BUCKET}/customer_sessions"
MINIO_SILVER_ATTRIBUTION = f"{MINIO_SILVER_BUCKET}/marketing_attribution"
MINIO_SILVER_CHANNEL_PERFORMANCE = f"{MINIO_SILVER_BUCKET}/channel_performance"
MINIO_SILVER_CUSTOMER_PROFILES = f"{MINIO_SILVER_BUCKET}/customer_profiles"

# Output paths for quality reports
QUALITY_REPORT_PATH = "/data/quality_reports/silver"
QUALITY_METRICS_PATH = f"{QUALITY_REPORT_PATH}/metrics"
QUALITY_ISSUES_PATH = f"{QUALITY_REPORT_PATH}/issues"

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

def load_dataset(path, date_filter=None):
    """Load a dataset from Silver with optional date filtering"""
    try:
        # Try MinIO path first
        print(f"Attempting to read from MinIO path: {path}")
        df = spark.read.parquet(path)
        
        # Apply date filter if provided
        if date_filter:
            date_col, date_str = date_filter
            if date_col in df.columns:
                df = df.filter(to_date(col(date_col)) == date_str)
        
        print(f"Successfully loaded data from {path}: {df.count()} records")
        return df
    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        
        # Try local filesystem as fallback
        try:
            local_path = f"/data{path[path.find('/', 5):]}"
            print(f"Attempting to read from local path: {local_path}")
            df = spark.read.parquet(local_path)
            
            # Apply date filter if provided
            if date_filter:
                date_col, date_str = date_filter
                if date_col in df.columns:
                    df = df.filter(to_date(col(date_col)) == date_str)
            
            print(f"Successfully loaded data from {local_path}: {df.count()} records")
            return df
        except Exception as e2:
            print(f"Error reading from local filesystem: {e2}")
            print(f"No data could be loaded from {path}")
            return None

def check_dataset_completeness(df, dataset_name):
    """Check dataset for completeness and return metrics"""
    if df is None:
        return {
            "dataset": dataset_name,
            "exists": False,
            "record_count": 0,
            "completeness_score": 0.0,
            "last_updated": None
        }
    
    # Count rows
    row_count = df.count()
    
    # Get column names
    columns = df.columns
    
    # Calculate completeness for each column
    column_metrics = {}
    overall_null_count = 0
    total_cells = row_count * len(columns)
    
    for column in columns:
        # Get column data type
        col_type = df.select(column).dtypes[0][1]
        
        # Apply appropriate null check based on data type
        if col_type in ["double", "float"]:
            # For floating point numbers, check for both NULL and NaN
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        else:
            # For other types, just check for NULL
            null_count = df.filter(col(column).isNull()).count()
        
        non_null_count = row_count - null_count
        completeness = non_null_count / row_count if row_count > 0 else 0.0
        
        column_metrics[column] = {
            "null_count": null_count,
            "non_null_count": non_null_count,
            "completeness": completeness,
            "data_type": col_type
        }
        
        overall_null_count += null_count
    
    # Overall completeness score
    overall_completeness = (total_cells - overall_null_count) / total_cells if total_cells > 0 else 0.0
    
    # Get last updated timestamp if it exists
    last_updated = None
    if "silver_updated_at" in columns:
        max_updated = df.select(max("silver_updated_at")).collect()[0][0]
        last_updated = str(max_updated) if max_updated else None
    
    return {
        "dataset": dataset_name,
        "exists": True,
        "record_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "column_metrics": column_metrics,
        "overall_null_count": overall_null_count,
        "total_cells": total_cells,
        "completeness_score": overall_completeness,
        "last_updated": last_updated
    }

def check_dataset_validity(df, dataset_name, validation_rules):
    """Check dataset for validity based on predefined rules"""
    if df is None:
        return {
            "dataset": dataset_name,
            "validity_checks": [],
            "overall_validity_score": 0.0
        }
    
    validity_checks = []
    valid_record_count = 0
    
    for rule in validation_rules:
        rule_name = rule["name"]
        rule_condition = rule["condition"]
        
        # Apply the condition and count valid records
        valid_records = df.filter(rule_condition).count()
        total_records = df.count()
        validity_ratio = valid_records / total_records if total_records > 0 else 0.0
        
        validity_checks.append({
            "rule": rule_name,
            "valid_count": valid_records,
            "total_count": total_records,
            "validity_ratio": validity_ratio
        })
        
        valid_record_count += valid_records
    
    # Calculate overall validity score
    num_rules = len(validation_rules)
    total_possible_valid = df.count() * num_rules
    overall_validity = valid_record_count / total_possible_valid if total_possible_valid > 0 else 0.0
    
    return {
        "dataset": dataset_name,
        "validity_checks": validity_checks,
        "overall_validity_score": overall_validity
    }

def check_referential_integrity(source_df, target_df, source_key, target_key, source_name, target_name):
    """Check referential integrity between two datasets"""
    if source_df is None or target_df is None:
        return {
            "source_dataset": source_name,
            "target_dataset": target_name,
            "source_key": source_key,
            "target_key": target_key,
            "integrity_score": 0.0,
            "orphaned_records": 0,
            "check_status": "FAILED - Missing datasets"
        }
    
    # Count source keys
    source_key_count = source_df.select(countDistinct(col(source_key))).collect()[0][0]
    
    # Get source keys not in target
    source_keys = source_df.select(source_key).distinct()
    target_keys = target_df.select(target_key).distinct()
    
    orphaned_keys = source_keys.join(
        target_keys,
        source_keys[source_key] == target_keys[target_key],
        "left_anti"
    )
    
    orphaned_count = orphaned_keys.count()
    integrity_score = (source_key_count - orphaned_count) / source_key_count if source_key_count > 0 else 0.0
    
    return {
        "source_dataset": source_name,
        "target_dataset": target_name,
        "source_key": source_key,
        "target_key": target_key,
        "source_key_count": source_key_count,
        "orphaned_records": orphaned_count,
        "integrity_score": integrity_score,
        "check_status": "PASSED" if integrity_score >= 0.98 else "WARNING" if integrity_score >= 0.9 else "FAILED"
    }

def detect_duplicates(df, key_columns, dataset_name):
    """Detect duplicates in a dataset based on key columns"""
    if df is None:
        return {
            "dataset": dataset_name,
            "key_columns": key_columns,
            "has_duplicates": False,
            "duplicate_count": 0,
            "duplicate_ratio": 0.0
        }
    
    # Count total records
    total_count = df.count()
    
    # Count distinct key combinations
    distinct_count = df.select(*key_columns).distinct().count()
    
    # Calculate duplicates
    duplicate_count = total_count - distinct_count
    duplicate_ratio = duplicate_count / total_count if total_count > 0 else 0.0
    
    return {
        "dataset": dataset_name,
        "key_columns": key_columns,
        "has_duplicates": duplicate_count > 0,
        "duplicate_count": duplicate_count,
        "duplicate_ratio": duplicate_ratio,
        "check_status": "PASSED" if duplicate_ratio < 0.01 else "WARNING" if duplicate_ratio < 0.05 else "FAILED"
    }

def detect_outliers(df, column, dataset_name, method="zscore", threshold=3.0):
    """Detect outliers in a numerical column using Z-score or IQR"""
    if df is None or column not in df.columns:
        return {
            "dataset": dataset_name,
            "column": column,
            "outlier_count": 0,
            "outlier_ratio": 0.0,
            "method": method
        }
    
    total_count = df.count()
    
    # Skip if column is not numerical
    if not df.select(column).dtypes[0][1] in ['int', 'bigint', 'double', 'float']:
        return {
            "dataset": dataset_name,
            "column": column,
            "outlier_count": 0,
            "outlier_ratio": 0.0,
            "method": method,
            "status": "SKIPPED - Not numerical"
        }
    
    if method == "zscore":
        # Calculate mean and standard deviation
        stats = df.select(
            avg(col(column)).alias("mean"),
            stddev(col(column)).alias("stddev")
        ).collect()[0]
        
        mean = stats["mean"]
        std_dev = stats["stddev"]
        
        if std_dev is None or std_dev == 0:
            return {
                "dataset": dataset_name,
                "column": column,
                "outlier_count": 0,
                "outlier_ratio": 0.0,
                "method": method,
                "status": "SKIPPED - No variation"
            }
        
        # Count records where value is more than threshold standard deviations from mean
        outlier_count = df.filter(
            (col(column) - mean) / std_dev > threshold
        ).count()
    else:  # IQR method
        # Calculate Q1 and Q3
        quantiles = df.stat.approxQuantile(column, [0.25, 0.75], 0.05)
        q1 = quantiles[0]
        q3 = quantiles[1]
        iqr = q3 - q1
        
        # Count records outside the IQR bounds
        outlier_count = df.filter(
            (col(column) < q1 - 1.5 * iqr) | (col(column) > q3 + 1.5 * iqr)
        ).count()
    
    outlier_ratio = outlier_count / total_count if total_count > 0 else 0.0
    
    return {
        "dataset": dataset_name,
        "column": column,
        "outlier_count": outlier_count,
        "outlier_ratio": outlier_ratio,
        "method": method,
        "threshold": threshold,
        "check_status": "PASSED" if outlier_ratio < 0.01 else "WARNING" if outlier_ratio < 0.05 else "FAILED"
    }

def check_freshness(df, timestamp_column, dataset_name, max_days_old=1):
    """Check data freshness based on timestamp column"""
    if df is None or timestamp_column not in df.columns:
        return {
            "dataset": dataset_name,
            "timestamp_column": timestamp_column,
            "is_fresh": False,
            "max_age_days": None,
            "check_status": "FAILED - Missing data or timestamp"
        }
    
    # Get latest timestamp
    latest_ts = df.select(max(col(timestamp_column))).collect()[0][0]
    
    if latest_ts is None:
        return {
            "dataset": dataset_name,
            "timestamp_column": timestamp_column,
            "is_fresh": False,
            "max_age_days": None,
            "latest_timestamp": None,
            "check_status": "FAILED - No timestamp data"
        }
    
    # Convert the timestamp to a datetime object if it's a string
    if isinstance(latest_ts, str):
        try:
            # Try different formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                try:
                    latest_ts = datetime.strptime(latest_ts, fmt)
                    break
                except ValueError:
                    continue
        except Exception as e:
            return {
                "dataset": dataset_name,
                "timestamp_column": timestamp_column,
                "is_fresh": False,
                "max_age_days": None,
                "latest_timestamp": str(latest_ts),
                "check_status": f"FAILED - Could not parse timestamp: {e}"
            }
    
    # Calculate age in days
    today_date = datetime.now().date()
    
    # Make sure we have datetime objects for comparison
    if hasattr(latest_ts, 'date'):
        latest_date = latest_ts.date()
        age_days = (today_date - latest_date).days
    else:
        # If it's already a date object
        if isinstance(latest_ts, type(today_date)):
            age_days = (today_date - latest_ts).days
        else:
            # For other types, convert to string for display but can't calculate age
            return {
                "dataset": dataset_name,
                "timestamp_column": timestamp_column,
                "is_fresh": False,
                "max_age_days": None,
                "latest_timestamp": str(latest_ts),
                "timestamp_type": str(type(latest_ts)),
                "check_status": "FAILED - Unknown timestamp format"
            }
    
    return {
        "dataset": dataset_name,
        "timestamp_column": timestamp_column,
        "is_fresh": age_days <= max_days_old,
        "max_age_days": age_days,
        "latest_timestamp": str(latest_ts),
        "check_status": "PASSED" if age_days <= max_days_old else "WARNING" if age_days <= 3 else "FAILED"
    }

def check_distribution(df, column, dataset_name):
    """Check value distribution for categorical columns"""
    if df is None or column not in df.columns:
        return {
            "dataset": dataset_name,
            "column": column,
            "distribution": {},
            "distinct_values": 0
        }
    
    total_count = df.count()
    
    # Skip if column has too many distinct values (likely not categorical)
    distinct_count = df.select(column).distinct().count()
    if distinct_count > 100:
        return {
            "dataset": dataset_name,
            "column": column,
            "distinct_values": distinct_count,
            "status": "SKIPPED - Too many distinct values"
        }
    
    # Get value distribution
    distribution = df.groupBy(column) \
        .count() \
        .withColumn("percentage", col("count") / total_count * 100) \
        .orderBy(col("count").desc()) \
        .collect()
    
    # Convert to dictionary
    dist_dict = {str(row[column]): {"count": row["count"], "percentage": row["percentage"]} for row in distribution}
    
    return {
        "dataset": dataset_name,
        "column": column,
        "distribution": dist_dict,
        "distinct_values": distinct_count
    }

def save_quality_report(quality_metrics, process_date):
    """Save quality report to file"""
    # Create directory if it doesn't exist
    os.makedirs(QUALITY_REPORT_PATH, exist_ok=True)
    os.makedirs(QUALITY_METRICS_PATH, exist_ok=True)
    os.makedirs(QUALITY_ISSUES_PATH, exist_ok=True)
    
    # Format date for filenames
    date_str = process_date.strftime('%Y-%m-%d')
    
    # Prepare issues report
    issues = []
    for dataset_name, metrics in quality_metrics.items():
        # Completeness issues
        if "completeness" in metrics:
            comp_score = metrics["completeness"]["completeness_score"]
            if comp_score < 0.95:
                issues.append({
                    "dataset": dataset_name,
                    "issue_type": "LOW_COMPLETENESS",
                    "severity": "HIGH" if comp_score < 0.9 else "MEDIUM",
                    "description": f"Dataset has low completeness score of {comp_score:.2f}",
                    "recommendation": "Investigate missing values and data pipeline issues"
                })
        
        # Validity issues
        if "validity" in metrics:
            val_score = metrics["validity"]["overall_validity_score"]
            if val_score < 0.95:
                issues.append({
                    "dataset": dataset_name,
                    "issue_type": "LOW_VALIDITY",
                    "severity": "HIGH" if val_score < 0.9 else "MEDIUM",
                    "description": f"Dataset has low validity score of {val_score:.2f}",
                    "recommendation": "Review validation rules and data quality"
                })
        
        # Duplicates issues
        if "duplicates" in metrics and metrics["duplicates"]["has_duplicates"]:
            dup_ratio = metrics["duplicates"]["duplicate_ratio"]
            if dup_ratio > 0.01:
                issues.append({
                    "dataset": dataset_name,
                    "issue_type": "DUPLICATES",
                    "severity": "HIGH" if dup_ratio > 0.05 else "MEDIUM",
                    "description": f"Dataset has {metrics['duplicates']['duplicate_count']} duplicates ({dup_ratio:.2f}%)",
                    "recommendation": "Review primary key constraints and deduplication logic"
                })
        
        # Referential integrity issues
        if "integrity" in metrics:
            for check in metrics["integrity"]:
                if check["check_status"] == "FAILED":
                    issues.append({
                        "dataset": dataset_name,
                        "issue_type": "REFERENTIAL_INTEGRITY",
                        "severity": "HIGH",
                        "description": f"Integrity issue between {check['source_dataset']} and {check['target_dataset']}",
                        "details": f"Found {check['orphaned_records']} orphaned records",
                        "recommendation": "Ensure all foreign keys have corresponding records in parent tables"
                    })
        
        # Freshness issues
        if "freshness" in metrics and not metrics["freshness"]["is_fresh"]:
            issues.append({
                "dataset": dataset_name,
                "issue_type": "DATA_STALENESS",
                "severity": "HIGH" if metrics["freshness"]["max_age_days"] > 3 else "MEDIUM",
                "description": f"Data is {metrics['freshness']['max_age_days']} days old",
                "recommendation": "Investigate pipeline delays or scheduling issues"
            })
    
    # Count high and medium severity issues
    high_severity_count = len([issue for issue in issues if issue["severity"] == "HIGH"])
    medium_severity_count = len([issue for issue in issues if issue["severity"] == "MEDIUM"])
    
    # Write metrics to file
    metrics_filename = f"{QUALITY_METRICS_PATH}/silver_quality_metrics_{date_str}.json"
    with open(metrics_filename, 'w') as f:
        json.dump({
            "process_date": date_str,
            "metrics": quality_metrics,
            "summary": {
                "total_datasets": len(quality_metrics),
                "datasets_with_issues": len(set(issue["dataset"] for issue in issues)),
                "total_issues": len(issues),
                "high_severity_issues": high_severity_count,
                "medium_severity_issues": medium_severity_count
            }
        }, f, indent=2)
    
    # Write issues to file
    if issues:
        issues_filename = f"{QUALITY_ISSUES_PATH}/silver_quality_issues_{date_str}.json"
        with open(issues_filename, 'w') as f:
            json.dump({
                "process_date": date_str,
                "issue_count": len(issues),
                "issues": issues
            }, f, indent=2)
    
    print(f"Quality report saved to {metrics_filename}")
    if issues:
        print(f"Issues report saved to {issues_filename}")
        print(f"Found {len(issues)} quality issues: {high_severity_count} high severity, {medium_severity_count} medium severity")
    else:
        print("No quality issues found")
    
    return issues

def check_silver_data_quality(process_date):
    """Main function to check silver data quality"""
    date_str = process_date.strftime('%Y-%m-%d')
    print(f"Checking silver data quality for {date_str}")
    
    # Dictionary to store all quality metrics
    quality_metrics = {}
    
    # Load datasets
    web_logs = load_dataset(MINIO_SILVER_WEB_LOGS)
    customers = load_dataset(MINIO_SILVER_CUSTOMERS)
    orders = load_dataset(MINIO_SILVER_ORDERS)
    order_items = load_dataset(MINIO_SILVER_ORDER_ITEMS)
    google_ads = load_dataset(MINIO_SILVER_GOOGLE_ADS)
    social_ads = load_dataset(MINIO_SILVER_SOCIAL_ADS)
    influencer = load_dataset(MINIO_SILVER_INFLUENCER)
    all_platforms = load_dataset(MINIO_SILVER_ALL_PLATFORMS)
    web_sessions = load_dataset(MINIO_SILVER_WEB_SESSIONS)
    customer_sessions = load_dataset(MINIO_SILVER_CUSTOMER_SESSIONS)
    marketing_attribution = load_dataset(MINIO_SILVER_ATTRIBUTION)
    channel_performance = load_dataset(MINIO_SILVER_CHANNEL_PERFORMANCE)
    customer_profiles = load_dataset(MINIO_SILVER_CUSTOMER_PROFILES)
    
    # Check Web Logs
    print("Checking web logs quality...")
    if web_logs is not None:
        # Completeness
        completeness = check_dataset_completeness(web_logs, "web_logs")
        
        # Validity
        validity_rules = [
            {"name": "valid_event_id", "condition": col("event_id").isNotNull()},
            {"name": "valid_session_id", "condition": col("session_id").isNotNull()},
            {"name": "valid_timestamp", "condition": col("event_timestamp").isNotNull()},
            {"name": "valid_event_type", "condition": col("event_type").isNotNull()}
        ]
        validity = check_dataset_validity(web_logs, "web_logs", validity_rules)
        
        # Duplicates
        duplicates = detect_duplicates(web_logs, ["event_id"], "web_logs")
        
        # Freshness
        freshness = check_freshness(web_logs, "event_timestamp", "web_logs", max_days_old=2)
        
        # Distribution
        event_type_dist = check_distribution(web_logs, "event_type", "web_logs")
        
        # Store metrics
        quality_metrics["web_logs"] = {
            "completeness": completeness,
            "validity": validity,
            "duplicates": duplicates,
            "freshness": freshness,
            "distributions": {"event_type": event_type_dist}
        }
    else:
        quality_metrics["web_logs"] = {"status": "FAILED - Dataset not found"}
    
    # Check Customers
    print("Checking customers quality...")
    if customers is not None:
        # Completeness
        completeness = check_dataset_completeness(customers, "customers")
        
        # Validity
        validity_rules = [
            {"name": "valid_customer_id", "condition": col("customer_id").isNotNull()},
            {"name": "valid_email", "condition": col("email").isNotNull() & (length(col("email")) > 5) & col("email").contains("@")},
            {"name": "valid_registration_date", "condition": col("registration_date").isNotNull()}
        ]
        validity = check_dataset_validity(customers, "customers", validity_rules)
        
        # Duplicates
        duplicates = detect_duplicates(customers, ["customer_id"], "customers")
        
        # Freshness
        freshness = check_freshness(customers, "silver_updated_at", "customers")
        
        # Distributions
        gender_dist = check_distribution(customers, "gender", "customers")
        skin_type_dist = check_distribution(customers, "skin_type", "customers")
        
        # Store metrics
        quality_metrics["customers"] = {
            "completeness": completeness,
            "validity": validity,
            "duplicates": duplicates,
            "freshness": freshness,
            "distributions": {
                "gender": gender_dist,
                "skin_type": skin_type_dist
            }
        }
    else:
        quality_metrics["customers"] = {"status": "FAILED - Dataset not found"}
    
    # Check Orders
    print("Checking orders quality...")
    if orders is not None:
        # Completeness
        completeness = check_dataset_completeness(orders, "orders")
        
        # Validity
        validity_rules = [
            {"name": "valid_order_id", "condition": col("order_id").isNotNull()},
            {"name": "valid_customer_id", "condition": col("customer_id").isNotNull()},
            {"name": "valid_order_date", "condition": col("order_date").isNotNull()},
            {"name": "valid_total", "condition": col("final_total").isNotNull() & (col("final_total") >= 0)}
        ]
        validity = check_dataset_validity(orders, "orders", validity_rules)
        
        # Duplicates
        duplicates = detect_duplicates(orders, ["order_id"], "orders")
        
        # Freshness
        freshness = check_freshness(orders, "silver_updated_at", "orders")
        
        # Outliers
        order_total_outliers = detect_outliers(orders, "final_total", "orders")
        
        # Referential integrity with customers
        integrity = []
        if customers is not None:
            customer_orders_integrity = check_referential_integrity(
                orders, customers, "customer_id", "customer_id", "orders", "customers"
            )
            integrity.append(customer_orders_integrity)
        
        # Store metrics
        quality_metrics["orders"] = {
            "completeness": completeness,
            "validity": validity,
            "duplicates": duplicates,
            "freshness": freshness,
            "outliers": {"final_total": order_total_outliers},
            "integrity": integrity
        }
    else:
        quality_metrics["orders"] = {"status": "FAILED - Dataset not found"}
    
    # Check Order Items
    print("Checking order items quality...")
    if order_items is not None:
        # Completeness
        completeness = check_dataset_completeness(order_items, "order_items")
        
        # Validity
        validity_rules = [
            {"name": "valid_order_id", "condition": col("order_id").isNotNull()},
            {"name": "valid_product_name", "condition": col("product_name").isNotNull()},
            {"name": "valid_price", "condition": col("price").isNotNull() & (col("price") > 0)},
            {"name": "valid_quantity", "condition": col("quantity").isNotNull() & (col("quantity") > 0)}
        ]
        validity = check_dataset_validity(order_items, "order_items", validity_rules)
        
        # Referential integrity
        integrity = []
        if orders is not None:
            order_items_integrity = check_referential_integrity(
                order_items, orders, "order_id", "order_id", "order_items", "orders"
            )
            integrity.append(order_items_integrity)
        
        # Distribution
        category_dist = check_distribution(order_items, "category", "order_items")
        
        # Store metrics
        quality_metrics["order_items"] = {
            "completeness": completeness,
            "validity": validity,
            "integrity": integrity,
            "distributions": {"category": category_dist}
        }
    else:
        quality_metrics["order_items"] = {"status": "FAILED - Dataset not found"}
    
    # Check Advertising Data
    print("Checking advertising data quality...")
    # Google Ads
    if google_ads is not None:
        completeness = check_dataset_completeness(google_ads, "google_ads")
        
        validity_rules = [
            {"name": "valid_campaign_id", "condition": col("campaign_id").isNotNull()},
            {"name": "valid_date", "condition": col("ad_date").isNotNull()},
            {"name": "valid_cost", "condition": col("cost").isNotNull() & (col("cost") >= 0)},
            {"name": "valid_impressions", "condition": col("impressions").isNotNull() & (col("impressions") >= 0)}
        ]
        validity = check_dataset_validity(google_ads, "google_ads", validity_rules)
        
        # Freshness
        freshness = check_freshness(google_ads, "silver_updated_at", "google_ads")
        
        # Outliers
        cost_outliers = detect_outliers(google_ads, "cost", "google_ads")
        
        # Store metrics
        quality_metrics["google_ads"] = {
            "completeness": completeness,
            "validity": validity,
            "freshness": freshness,
            "outliers": {"cost": cost_outliers}
        }
    else:
        quality_metrics["google_ads"] = {"status": "FAILED - Dataset not found"}
    
    # Social Ads
    if social_ads is not None:
        completeness = check_dataset_completeness(social_ads, "social_ads")
        
        validity_rules = [
            {"name": "valid_campaign_id", "condition": col("campaign_id").isNotNull()},
            {"name": "valid_date", "condition": col("ad_date").isNotNull()},
            {"name": "valid_platform", "condition": col("platform").isNotNull()},
            {"name": "valid_cost", "condition": col("cost").isNotNull() & (col("cost") >= 0)}
        ]
        validity = check_dataset_validity(social_ads, "social_ads", validity_rules)
        
        # Freshness
        freshness = check_freshness(social_ads, "silver_updated_at", "social_ads")
        
        # Distribution
        platform_dist = check_distribution(social_ads, "platform", "social_ads")
        
        # Store metrics
        quality_metrics["social_ads"] = {
            "completeness": completeness,
            "validity": validity,
            "freshness": freshness,
            "distributions": {"platform": platform_dist}
        }
    else:
        quality_metrics["social_ads"] = {"status": "FAILED - Dataset not found"}
    
    # Check Web Sessions
    print("Checking web sessions quality...")
    if web_sessions is not None:
        # Completeness
        completeness = check_dataset_completeness(web_sessions, "web_sessions")
        
        # Validity
        validity_rules = [
            {"name": "valid_session_id", "condition": col("session_id").isNotNull()},
            {"name": "valid_start_time", "condition": col("session_start").isNotNull()},
            {"name": "valid_duration", "condition": col("session_duration_seconds").isNotNull() & (col("session_duration_seconds") >= 0)}
        ]
        validity = check_dataset_validity(web_sessions, "web_sessions", validity_rules)
        
        # Duplicates
        duplicates = detect_duplicates(web_sessions, ["session_id"], "web_sessions")
        
        # Freshness
        freshness = check_freshness(web_sessions, "silver_updated_at", "web_sessions")
        
        # Outliers
        duration_outliers = detect_outliers(web_sessions, "session_duration_seconds", "web_sessions")
        
        # Store metrics
        quality_metrics["web_sessions"] = {
            "completeness": completeness,
            "validity": validity,
            "duplicates": duplicates,
            "freshness": freshness,
            "outliers": {"session_duration_seconds": duration_outliers}
        }
    else:
        quality_metrics["web_sessions"] = {"status": "FAILED - Dataset not found"}
    
    # Check Customer Sessions
    print("Checking customer sessions quality...")
    if customer_sessions is not None:
        # Completeness
        completeness = check_dataset_completeness(customer_sessions, "customer_sessions")
        
        # Validity
        validity_rules = [
            {"name": "valid_session_id", "condition": col("session_id").isNotNull()},
            {"name": "valid_user_id", "condition": col("user_id").isNotNull()},
            {"name": "valid_start_time", "condition": col("session_start").isNotNull()}
        ]
        validity = check_dataset_validity(customer_sessions, "customer_sessions", validity_rules)
        
        # Referential integrity
        integrity = []
        if customers is not None:
            customer_session_integrity = check_referential_integrity(
                customer_sessions, customers, "user_id", "customer_id", "customer_sessions", "customers"
            )
            integrity.append(customer_session_integrity)
        
        # Freshness
        freshness = check_freshness(customer_sessions, "silver_updated_at", "customer_sessions")
        
        # Store metrics
        quality_metrics["customer_sessions"] = {
            "completeness": completeness,
            "validity": validity,
            "integrity": integrity,
            "freshness": freshness
        }
    else:
        quality_metrics["customer_sessions"] = {"status": "FAILED - Dataset not found"}
    
    # Check Customer Profiles
    print("Checking customer profiles quality...")
    if customer_profiles is not None:
        # Completeness
        completeness = check_dataset_completeness(customer_profiles, "customer_profiles")
        
        # Validity
        validity_rules = [
            {"name": "valid_customer_id", "condition": col("customer_id").isNotNull()},
            {"name": "valid_email", "condition": col("email").isNotNull() & (length(col("email")) > 5) & col("email").contains("@")},
            {"name": "valid_lifecycle_stage", "condition": col("lifecycle_stage").isNotNull()}
        ]
        validity = check_dataset_validity(customer_profiles, "customer_profiles", validity_rules)
        
        # Referential integrity
        integrity = []
        if customers is not None:
            profile_customer_integrity = check_referential_integrity(
                customer_profiles, customers, "customer_id", "customer_id", "customer_profiles", "customers"
            )
            integrity.append(profile_customer_integrity)
        
        # Freshness
        freshness = check_freshness(customer_profiles, "profile_updated_at", "customer_profiles")
        
        # Distributions
        lifecycle_dist = check_distribution(customer_profiles, "lifecycle_stage", "customer_profiles")
        
        # Store metrics
        quality_metrics["customer_profiles"] = {
            "completeness": completeness,
            "validity": validity,
            "integrity": integrity,
            "freshness": freshness,
            "distributions": {"lifecycle_stage": lifecycle_dist}
        }
    else:
        quality_metrics["customer_profiles"] = {"status": "FAILED - Dataset not found"}
    
    # Check Attribution Data
    print("Checking attribution data quality...")
    if marketing_attribution is not None:
        # Completeness
        completeness = check_dataset_completeness(marketing_attribution, "marketing_attribution")
        
        # Validity
        validity_rules = [
            {"name": "valid_order_id", "condition": col("order_id").isNotNull()},
            {"name": "valid_user_id", "condition": col("user_id").isNotNull()},
            {"name": "valid_attribution_model", "condition": col("attribution_model").isNotNull()},
            {"name": "valid_revenue", "condition": col("attributed_revenue").isNotNull() & (col("attributed_revenue") >= 0)}
        ]
        validity = check_dataset_validity(marketing_attribution, "marketing_attribution", validity_rules)
        
        # Referential integrity
        integrity = []
        if orders is not None:
            attribution_order_integrity = check_referential_integrity(
                marketing_attribution, orders, "order_id", "order_id", "marketing_attribution", "orders"
            )
            integrity.append(attribution_order_integrity)
        
        # Freshness
        freshness = check_freshness(marketing_attribution, "order_date", "marketing_attribution")
        
        # Distributions
        model_dist = check_distribution(marketing_attribution, "attribution_model", "marketing_attribution")
        
        # Store metrics
        quality_metrics["marketing_attribution"] = {
            "completeness": completeness,
            "validity": validity,
            "integrity": integrity,
            "freshness": freshness,
            "distributions": {"attribution_model": model_dist}
        }
    else:
        quality_metrics["marketing_attribution"] = {"status": "FAILED - Dataset not found"}
    
    # Check Channel Performance
    print("Checking channel performance quality...")
    if channel_performance is not None:
        # Completeness
        completeness = check_dataset_completeness(channel_performance, "channel_performance")
        
        # Validity
        validity_rules = [
            {"name": "valid_attribution_model", "condition": col("attribution_model").isNotNull()},
            {"name": "valid_utm_source", "condition": col("utm_source").isNotNull()},
            {"name": "valid_revenue", "condition": col("attributed_revenue").isNotNull() & (col("attributed_revenue") >= 0)}
        ]
        validity = check_dataset_validity(channel_performance, "channel_performance", validity_rules)
        
        # Freshness
        freshness = check_freshness(channel_performance, "attribution_date", "channel_performance")
        
        # Store metrics
        quality_metrics["channel_performance"] = {
            "completeness": completeness,
            "validity": validity,
            "freshness": freshness
        }
    else:
        quality_metrics["channel_performance"] = {"status": "FAILED - Dataset not found"}
    
    # Save quality report
    issues = save_quality_report(quality_metrics, process_date)
    
    # Return success if no high severity issues
    high_severity_issues = sum(1 for issue in issues if issue["severity"] == "HIGH")
    return high_severity_issues == 0

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Run quality checks
    success = check_silver_data_quality(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)