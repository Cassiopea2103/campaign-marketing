"""
load_product_performance.py - Spark script to load product performance data from Gold to warehouse

This script:
1. Reads product performance data from the Gold zone
2. Prepares the data for loading into the data warehouse
3. Applies any final transformations needed for reporting
4. Loads the data into the warehouse tables

Usage:
    spark-submit load_product_performance.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, to_date, current_date, date_format,
    year, month, dayofmonth, coalesce, concat, expr
)
import sys
import os
from datetime import datetime, timedelta

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Load Product Performance to Warehouse") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("spark.jars.packages", 
"net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Define storage paths
MINIO_GOLD_BUCKET = "s3a://gold"
MINIO_GOLD_PRODUCT_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/product_performance"
WAREHOUSE_OUTPUT_DIR = "/data/warehouse/product_performance"

# Define Snowflake connection properties
SNOWFLAKE_OPTIONS = {
    "sfURL": "ZCYQNOS-IA60918.snowflakecomputing.com",
    "sfUser": "CASSIOPEA2103",
    "sfPassword": "Saliou2103wade@", 
    "sfDatabase": "MARKETING_ANALYTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

# JDBC URL for Snowflake
SNOWFLAKE_URL = "net.snowflake.spark.snowflake"

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

def load_data_from_gold(path, description):
    """Load data from Gold zone with error handling"""
    try:
        df = spark.read.parquet(path)
        print(f"Loaded {df.count()} {description} records from {path}")
        return df
    except Exception as e:
        print(f"Error reading {description} from Gold: {e}")
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

def save_to_warehouse(dataframe, table_name):
    """Save dataframe directly to Snowflake"""
    if dataframe is None:
        print(f"No data to save to warehouse table {table_name}")
        return False
        
    # Write directly to Snowflake
    try:
        # Write to Snowflake using the Snowflake connector
        dataframe.write \
          .format("snowflake") \
          .options(**{
              "sfUrl": SNOWFLAKE_OPTIONS["sfURL"],
              "sfUser": SNOWFLAKE_OPTIONS["sfUser"],
              "sfPassword": SNOWFLAKE_OPTIONS["sfPassword"],
              "sfDatabase": SNOWFLAKE_OPTIONS["sfDatabase"] , 
              "sfSchema": SNOWFLAKE_OPTIONS["sfSchema"],    
              "sfWarehouse": SNOWFLAKE_OPTIONS["sfWarehouse"],
              "dbtable": table_name
          }) \
          .mode("overwrite") \
          .save()
            
        print(f"Successfully wrote {dataframe.count()} records to Snowflake table {table_name}")
        return True
    except Exception as e:
        print(f"Error writing to Snowflake: {e}")
        import traceback
        traceback.print_exc()
        return False

def prepare_product_performance_table(product_rankings):
    """Prepare product performance fact table for the warehouse"""
    if product_rankings is None:
        print("No product rankings data available")
        return None
    
    try:
        # Check available columns
        available_columns = product_rankings.columns
        print(f"Available columns in product_rankings: {available_columns}")
        
        # Select relevant columns based on what's available
        base_columns = [
            "product_name", "category", "time_period", 
            "total_revenue", "total_quantity", "order_count", 
            "revenue_rank", "quantity_rank", "overall_revenue_rank", 
            "overall_quantity_rank", "is_top_revenue_product", 
            "is_top_quantity_product", "is_overall_top_product",
            "processing_date", "year", "month", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in base_columns if col in available_columns]
        
        # Select relevant columns and rename for clarity
        product_performance = product_rankings.select(*columns_to_select)
        
        # Add time_period as constant if it doesn't exist
        if "time_period" not in available_columns:
            product_performance = product_performance.withColumn("time_period", lit("monthly"))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            product_performance = product_performance.withColumnRenamed("processing_date", "etl_date")
            
        # Create a product_performance_key
        product_performance = product_performance.withColumn(
            "product_performance_key", 
            concat(
                col("product_name"),
                lit("_"),
                lit("_"),
                col("time_period"),
                lit("_"),
                lit(str(datetime.now().year)),
                lit("_"),
                lit(str(datetime.now().month))
            )
        )
        
        # Add data warehouse metadata
        product_performance = product_performance \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return product_performance
    except Exception as e:
        print(f"Error preparing product performance table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_product_trend_analysis_table(trend_analysis):
    """Prepare product trend analysis facts for the warehouse"""
    if trend_analysis is None:
        print("No product trend analysis data available")
        return None
    
    try:
        # Check available columns
        available_columns = trend_analysis.columns
        print(f"Available columns in trend_analysis: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "product_name", "category", 
            "current_revenue", "current_quantity", "current_orders",
            "previous_revenue", "previous_quantity", "previous_orders",
            "revenue_change", "revenue_growth_pct", 
            "quantity_change", "quantity_growth_pct", 
            "orders_change", "orders_growth_pct", 
            "revenue_trend", "processing_date", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        trend_table = trend_analysis.select(*columns_to_select)
        
        # Add missing columns with default values
        if "year" not in available_columns:
            trend_table = trend_table.withColumn("year", lit(datetime.now().year))
            
        if "month" not in available_columns:
            trend_table = trend_table.withColumn("month", lit(datetime.now().month))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            trend_table = trend_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a product_trend_key
        trend_table = trend_table.withColumn(
            "product_trend_key", 
            concat(
                col("product_name"),
                lit("_"),
                col("category"),
                lit("_"),
                col("year"),
                lit("_"),
                col("month")
            )
        )
        
        # Add data warehouse metadata
        trend_table = trend_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return trend_table
    except Exception as e:
        print(f"Error preparing product trend analysis table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_category_analysis_table(category_analysis):
    """Prepare category analysis facts for the warehouse"""
    if category_analysis is None:
        print("No category analysis data available")
        return None
    
    try:
        # Check available columns
        available_columns = category_analysis.columns
        print(f"Available columns in category_analysis: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "category", "time_period", "start_date", "end_date",
            "category_order_count", "category_quantity", "category_revenue",
            "product_count", "revenue_percentage", "quantity_percentage",
            "revenue_rank", "processing_date", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        category_table = category_analysis.select(*columns_to_select)
        
        # Add missing columns with default values
        if "year" not in available_columns:
            category_table = category_table.withColumn("year", lit(datetime.now().year))
            
        if "month" not in available_columns:
            category_table = category_table.withColumn("month", lit(datetime.now().month))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            category_table = category_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a category_analysis_key
        category_table = category_table.withColumn(
            "category_analysis_key", 
            concat(
                col("category"),
                lit("_monthly_"),  
                col("year"),
                lit("_"),
                col("month")
            )
        )
        
        # Add data warehouse metadata
        category_table = category_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return category_table
    except Exception as e:
        print(f"Error preparing category analysis table: {e}")
        import traceback
        traceback.print_exc()
        return None

def prepare_conversion_analysis_table(conversion_analysis):
    """Prepare product conversion analysis facts for the warehouse"""
    if conversion_analysis is None:
        print("No conversion analysis data available")
        return None
    
    try:
        # Check available columns
        available_columns = conversion_analysis.columns
        print(f"Available columns in conversion_analysis: {available_columns}")
        
        # Define expected columns
        expected_columns = [
            "product_name", "category", "total_views", "total_purchases",
            "order_count", "view_to_purchase_rate", "view_to_order_rate",
            "category_avg_purchase_rate", "category_avg_order_rate",
            "purchase_rate_vs_category", "relative_performance",
            "processing_date", "day"
        ]
        
        # Filter to only include columns that exist
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        # Select relevant columns
        conversion_table = conversion_analysis.select(*columns_to_select)
        
        # Add missing columns with default values
        if "year" not in available_columns:
            conversion_table = conversion_table.withColumn("year", lit(datetime.now().year))
            
        if "month" not in available_columns:
            conversion_table = conversion_table.withColumn("month", lit(datetime.now().month))
        
        # Rename processing_date to etl_date if it exists
        if "processing_date" in available_columns:
            conversion_table = conversion_table.withColumnRenamed("processing_date", "etl_date")
            
        # Create a conversion_analysis_key
        conversion_table = conversion_table.withColumn(
            "conversion_key", 
            concat(
                col("product_name"),
                lit("_unknown_"),
                lit(str(datetime.now().year)),  
                lit("_"),
                lit(str(datetime.now().month)) 
            )
        )
        
        # Add data warehouse metadata
        conversion_table = conversion_table \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return conversion_table
    except Exception as e:
        print(f"Error preparing conversion analysis table: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_product_dimension(product_rankings):
    """Create product dimension for the warehouse"""
    print (f"Creating product dimension from product rankings")
    if product_rankings is None:
        print("No product data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = product_rankings.columns
        print(f"Available columns in product_rankings: {available_columns}")
        if "product_name" not in available_columns :
            print("Required columns product_name or category not found in data")
            # Create a simple default product dimension if data not available
            product_dim = spark.createDataFrame([
                ("face_cleanser", "soin_visage"),
                ("anti_aging_cream", "soin_visage"),
                ("hydrating_serum", "soin_visage"),
                ("body_lotion", "soin_corps"),
                ("shampoo_natural", "soin_cheveux"),
                ("conditioner_repair", "soin_cheveux")
            ], ["product_name"])
        else:
            # Extract distinct product information
            product_dim = product_rankings.select(
                "product_name"
            ).distinct()
        
        # Create product key
        product_dim = product_dim \
            .withColumn("product_key", 
                concat(
                    col("product_name"), 
                    lit("_"), 
                    col("order_count")
                )
            )
        
        # Try to extract product attributes from name
        product_dim = product_dim \
            .withColumn("product_type", 
                when(col("product_name").like("%cleanser%"), "Cleanser")
                .when(col("product_name").like("%cream%"), "Cream")
                .when(col("product_name").like("%serum%"), "Serum")
                .when(col("product_name").like("%lotion%"), "Lotion")
                .when(col("product_name").like("%shampoo%"), "Shampoo")
                .when(col("product_name").like("%conditioner%"), "Conditioner")
                .otherwise("Other")
            ) \
            .withColumn("is_organic", 
                when(
                    col("product_name").like("%bio%") | 
                    col("product_name").like("%natural%") | 
                    col("product_name").like("%organic%"), 
                    lit(True)
                ).otherwise(lit(False))
            )
        
        # Add category display name
        product_dim = product_dim \
            .withColumn("category_display", 
                when(col("category") == "soin_visage", "Facial Care")
                .when(col("category") == "soin_corps", "Body Care")
                .when(col("category") == "soin_cheveux", "Hair Care")
                .when(col("category") == "maquillage", "Make-up")
                .when(col("category") == "parfum", "Fragrance")
                .otherwise(col("category"))
            )
        
        # Add data warehouse metadata
        product_dim = product_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return product_dim
    except Exception as e:
        print(f"Error creating product dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_category_dimension(category_analysis):
    """Create category dimension for the warehouse"""
    if category_analysis is None:
        print("No category data available for dimension")
        return None
    
    try:
        # Check if required columns exist
        available_columns = category_analysis.columns
        if "category" not in available_columns:
            print("Required column category not found in data")
            # Create a simple default category dimension if data not available
            category_dim = spark.createDataFrame([
                ("soin_visage", "Facial Care", "Skin"),
                ("soin_corps", "Body Care", "Skin"),
                ("soin_cheveux", "Hair Care", "Hair"),
                ("maquillage", "Make-up", "Color"),
                ("parfum", "Fragrance", "Scent")
            ], ["category", "category_display", "category_group"])
        else:
            # Extract distinct category information
            category_dim = category_analysis.select("category").distinct()
            
            # Add display name and group
            category_dim = category_dim \
                .withColumn("category_display", 
                    when(col("category") == "soin_visage", "Facial Care")
                    .when(col("category") == "soin_corps", "Body Care")
                    .when(col("category") == "soin_cheveux", "Hair Care")
                    .when(col("category") == "maquillage", "Make-up")
                    .when(col("category") == "parfum", "Fragrance")
                    .otherwise(col("category"))
                ) \
                .withColumn("category_group", 
                    when(col("category").isin("soin_visage", "soin_corps"), "Skin")
                    .when(col("category") == "soin_cheveux", "Hair")
                    .when(col("category") == "maquillage", "Color")
                    .when(col("category") == "parfum", "Scent")
                    .otherwise("Other")
                )
        
        # Create category key (using category directly as key)
        category_dim = category_dim.withColumn("category_key", col("category"))
        
        # Add data warehouse metadata
        category_dim = category_dim \
            .withColumn("dw_load_ts", current_date()) \
            .withColumn("dw_update_ts", current_date())
        
        return category_dim
    except Exception as e:
        print(f"Error creating category dimension: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_product_performance_to_warehouse(process_date):
    """Main function to load product performance data to the warehouse"""
    print(f"Loading product performance data to warehouse for {process_date.strftime('%Y-%m-%d')}")
    
    # Determine year and month to load from gold
    year = process_date.year
    month = process_date.month
    
    # Try to load product rankings data from Gold
    try:
        # Try different path patterns
        product_rankings_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/product_rankings/time_period=monthly/category=*/year={year}/month={month}"
        product_rankings = load_data_from_gold(product_rankings_path, "product rankings")
        
        # If that fails, try without time_period partition
        if product_rankings is None:
            product_rankings_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/product_rankings/category=*/year={year}/month={month}"
            product_rankings = load_data_from_gold(product_rankings_path, "product rankings")
            
        # If that fails too, try with just the base path
        if product_rankings is None:
            product_rankings_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/product_rankings"
            product_rankings = load_data_from_gold(product_rankings_path, "product rankings")
    except Exception as e:
        print(f"Error loading product rankings: {e}")
        product_rankings = None
    
    # Try to load category analysis data from Gold with similar fallback pattern
    try:
        category_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/category_analysis/time_period=monthly/year={year}/month={month}"
        category_analysis = load_data_from_gold(category_analysis_path, "category analysis")
        
        if category_analysis is None:
            category_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/category_analysis/year={year}/month={month}"
            category_analysis = load_data_from_gold(category_analysis_path, "category analysis")
            
        if category_analysis is None:
            category_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/category_analysis"
            category_analysis = load_data_from_gold(category_analysis_path, "category analysis")
    except Exception as e:
        print(f"Error loading category analysis: {e}")
        category_analysis = None
    
    # Try to load trend analysis data from Gold
    try:
        trend_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/trend_analysis/year={year}/month={month}"
        trend_analysis = load_data_from_gold(trend_analysis_path, "trend analysis")
        
        if trend_analysis is None:
            trend_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/trend_analysis"
            trend_analysis = load_data_from_gold(trend_analysis_path, "trend analysis")
    except Exception as e:
        print(f"Error loading trend analysis: {e}")
        trend_analysis = None
    
    # Try to load conversion analysis data from Gold
    try:
        conversion_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/conversion_analysis/category=*/year={year}/month={month}"
        conversion_analysis = load_data_from_gold(conversion_analysis_path, "conversion analysis")
        
        if conversion_analysis is None:
            conversion_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/conversion_analysis/category=*"
            conversion_analysis = load_data_from_gold(conversion_analysis_path, "conversion analysis")
            
        if conversion_analysis is None:
            conversion_analysis_path = f"{MINIO_GOLD_PRODUCT_PERFORMANCE}/conversion_analysis"
            conversion_analysis = load_data_from_gold(conversion_analysis_path, "conversion analysis")
    except Exception as e:
        print(f"Error loading conversion analysis: {e}")
        conversion_analysis = None
    
    # Prepare data for warehouse
    success = True
    
    # 1. Prepare product performance table
    product_performance = prepare_product_performance_table(product_rankings)
    if product_performance is not None:
        success = success and save_to_warehouse(product_performance, "FACT_PRODUCT_PERFORMANCE")
    
    # 2. Prepare product trend analysis table
    trend_table = prepare_product_trend_analysis_table(trend_analysis)
    if trend_table is not None:
        success = success and save_to_warehouse(trend_table, "FACT_PRODUCT_TRENDS")
    
    # 3. Prepare category analysis table
    category_table = prepare_category_analysis_table(category_analysis)
    if category_table is not None:
        success = success and save_to_warehouse(category_table, "FACT_CATEGORY_PERFORMANCE")
    
    # 4. Prepare conversion analysis table
    conversion_table = prepare_conversion_analysis_table(conversion_analysis)
    if conversion_table is not None:
        success = success and save_to_warehouse(conversion_table, "FACT_PRODUCT_CONVERSION")
    
    # 5. Create product dimension
    product_dimension = create_product_dimension(product_rankings)
    if product_dimension is not None:
        success = success and save_to_warehouse(product_dimension, "DIM_PRODUCT")
    
    # 6. Create category dimension
    category_dimension = create_category_dimension(category_analysis)
    if category_dimension is not None:
        success = success and save_to_warehouse(category_dimension, "DIM_CATEGORY")
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    print(f"Starting product performance data load to Snowflake for date: {process_date.strftime('%Y-%m-%d')}")
    
    # Add Snowflake JDBC driver dependency if needed
    try:
        spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/snowflake-jdbc-3.13.30.jar")
        spark.sparkContext.addPyFile("https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.0-spark_3.3/spark-snowflake_2.12-2.11.0-spark_3.3.jar")
        print("Added Snowflake JDBC dependencies to SparkContext")
    except Exception as e:
        print(f"Warning: Could not add Snowflake JDBC dependencies: {e}")
    
    # Load data to warehouse
    success = load_product_performance_to_warehouse(process_date)
    
    if success:
        print("Product performance data successfully loaded to Snowflake warehouse")
    else:
        print("Warning: Some product performance data could not be loaded to Snowflake warehouse")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)