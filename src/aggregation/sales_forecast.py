"""
sales_forecast.py - Spark script to create sales forecasts and trend analysis for Gold layer

This script:
1. Processes historical order data from Silver
2. Creates time-based sales trend analysis
3. Generates sales forecasts by category and product
4. Identifies seasonal patterns and growth rates
5. Writes forecast data to the Gold zone for reporting and dashboards

Usage:
    spark-submit sales_forecast.py [date_string]
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, sum, count, countDistinct, avg, max, min, lead, lag,
    datediff, date_format, to_date, current_date, 
    expr, round, dense_rank, row_number, 
    year, month, dayofmonth, quarter, weekofyear, 
    monotonically_increasing_id
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import sys
import os
from datetime import datetime, timedelta

os.environ['PYSPARK_PYTHON'] = '/opt/bitnami/python/bin/python3.8'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/bitnami/python/bin/python3.8'

# Initialize Spark Session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("Sales Forecast Analytics") \
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
MINIO_GOLD_PRODUCT_PERFORMANCE = f"{MINIO_GOLD_BUCKET}/product_performance"
MINIO_GOLD_SALES_FORECAST = f"{MINIO_GOLD_BUCKET}/sales_forecast"

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
    # Generate the last 12 months of data for forecasting
    forecast_end = process_date
    forecast_start = process_date - timedelta(days=365)
    
    # Forecast period (next 3 months)
    next_month_start = process_date + timedelta(days=1)
    next_month_end = process_date + timedelta(days=30)
    
    next_quarter_start = process_date + timedelta(days=1)
    next_quarter_end = process_date + timedelta(days=90)
    
    return {
        "historical": (forecast_start, forecast_end),
        "next_month": (next_month_start, next_month_end),
        "next_quarter": (next_quarter_start, next_quarter_end)
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

def create_time_series_data(orders, order_items, date_ranges):
    """Create time series data for forecasting"""
    print("Creating time series data")
    
    if orders is None or order_items is None:
        print("Missing orders or order items data for time series creation")
        return None
    
    try:
        # Join order items with orders
        sales_data = order_items.join(
            orders.select(
                "order_id", 
                col("order_date").alias("order_date_from_orders"), 
                col("customer_id").alias("customer_id_from_orders"), 
                col("order_status").alias("order_status_from_orders") ),
            "order_id",
            "inner"
        )

        # Check join results
        join_count = sales_data.count()
        print(f"After joining orders and order items, {join_count} records remain")
        if join_count == 0:
            print("No data after joining orders and order items")
            return None
        
        # Filter for completed orders only
        sales_data = sales_data.filter(col("order_status_from_orders").isin("Livré") )

        # Check filter results
        status_filtered_count = sales_data.count()
        print(f"After filtering for completed orders, {status_filtered_count} records remain")
        if status_filtered_count == 0:
            print("No completed orders found")
            return None
        
        # Filter for date range
        historical_start, historical_end = date_ranges["historical"]
        start_date_str = historical_start.strftime('%Y-%m-%d')
        end_date_str = historical_end.strftime('%Y-%m-%d')
        
        sales_data = sales_data.filter(
            (to_date(col("order_date_from_orders")) >= start_date_str) &
            (to_date(col("order_date_from_orders")) <= end_date_str)
        )

        # Check date filter results
        date_filtered_count = sales_data.count()
        print(f"After filtering for date range {start_date_str} to {end_date_str}, {date_filtered_count} records remain")
        if date_filtered_count == 0:
            print("No orders found in the specified date range")
            return None
        
        # Add date components
        time_series = sales_data \
            .withColumn("order_date_dt", to_date(col("order_date_from_orders"))) \
            .withColumn("year", year(col("order_date_dt"))) \
            .withColumn("month", month(col("order_date_dt"))) \
            .withColumn("day", dayofmonth(col("order_date_dt"))) \
            .withColumn("quarter", quarter(col("order_date_dt"))) \
            .withColumn("week", weekofyear(col("order_date_dt")))
        
        # Create aggregated daily sales
        daily_sales = time_series \
            .groupBy("order_date_dt") \
            .agg(
                sum("item_total").alias("daily_revenue"),
                count("order_id").alias("daily_order_count"),
                sum("quantity").alias("daily_quantity"),
                countDistinct("customer_id_from_orders").alias("daily_unique_customers")
            ) \
            .orderBy("order_date_dt")
        
        # Create daily sales by category
        daily_category_sales = time_series \
            .groupBy("order_date_dt", "category") \
            .agg(
                sum("item_total").alias("daily_category_revenue"),
                count("order_id").alias("daily_category_orders"),
                sum("quantity").alias("daily_category_quantity")
            ) \
            .orderBy("order_date_dt", "category")
        
        # Create monthly sales
        monthly_sales = time_series \
            .groupBy("year", "month") \
            .agg(
                sum("item_total").alias("monthly_revenue"),
                count("order_id").alias("monthly_order_count"),
                sum("quantity").alias("monthly_quantity"),
                countDistinct("customer_id_from_orders").alias("monthly_unique_customers")
            ) \
            .orderBy("year", "month")
        
        # Create monthly sales by category
        monthly_category_sales = time_series \
            .groupBy("year", "month", "category") \
            .agg(
                sum("item_total").alias("monthly_category_revenue"),
                count("order_id").alias("monthly_category_orders"),
                sum("quantity").alias("monthly_category_quantity")
            ) \
            .orderBy("year", "month", "category")
        
        # Create date string for month-year format to use in forecasting
        monthly_sales = monthly_sales \
            .withColumn(
                "month_year",
                expr("concat(cast(year as string), '-', cast(month as string))")
            ) \
            .withColumn(
                "month_idx",
                (col("year") * 12) + col("month")
            )
        
        # Add month index to category sales as well
        monthly_category_sales = monthly_category_sales \
            .withColumn(
                "month_year",
                expr("concat(cast(year as string), '-', cast(month as string))")
            ) \
            .withColumn(
                "month_idx",
                (col("year") * 12) + col("month")
            )
        
        # Calculate moving averages and growth rates for trend analysis
        window_3m = Window.orderBy("month_idx").rowsBetween(-2, 0)
        window_prev_month = Window.orderBy("month_idx").rowsBetween(-1, -1)
        window_prev_year = Window.orderBy("month_idx").rowsBetween(-12, -12)
        
        monthly_sales = monthly_sales \
            .withColumn("revenue_ma_3m", avg("monthly_revenue").over(window_3m)) \
            .withColumn("prev_month_revenue", lag("monthly_revenue", 1).over(Window.orderBy("month_idx"))) \
            .withColumn("prev_year_revenue", lag("monthly_revenue", 12).over(Window.orderBy("month_idx"))) \
            .withColumn(
                "mom_growth",
                when(col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") > 0),
                    (col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100
                ).otherwise(None)
            ) \
            .withColumn(
                "yoy_growth",
                when(col("prev_year_revenue").isNotNull() & (col("prev_year_revenue") > 0),
                    (col("monthly_revenue") - col("prev_year_revenue")) / col("prev_year_revenue") * 100
                ).otherwise(None)
            )
        
        # Create similar trend calculations for category sales
        monthly_category_sales = monthly_category_sales \
            .withColumn(
                "category_revenue_ma_3m",
                avg("monthly_category_revenue").over(Window.partitionBy("category").orderBy("month_idx").rowsBetween(-2, 0))
            ) \
            .withColumn(
                "prev_month_category_revenue",
                lag("monthly_category_revenue", 1).over(Window.partitionBy("category").orderBy("month_idx"))
            ) \
            .withColumn(
                "prev_year_category_revenue",
                lag("monthly_category_revenue", 12).over(Window.partitionBy("category").orderBy("month_idx"))
            ) \
            .withColumn(
                "category_mom_growth",
                when(col("prev_month_category_revenue").isNotNull() & (col("prev_month_category_revenue") > 0),
                    (col("monthly_category_revenue") - col("prev_month_category_revenue")) / col("prev_month_category_revenue") * 100
                ).otherwise(None)
            ) \
            .withColumn(
                "category_yoy_growth",
                when(col("prev_year_category_revenue").isNotNull() & (col("prev_year_category_revenue") > 0),
                    (col("monthly_category_revenue") - col("prev_year_category_revenue")) / col("prev_year_category_revenue") * 100
                ).otherwise(None)
            )
        
        return {
            "daily_sales": daily_sales,
            "daily_category_sales": daily_category_sales,
            "monthly_sales": monthly_sales, 
            "monthly_category_sales": monthly_category_sales
        }
    
    except Exception as e:
        print(f"Error creating time series data: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_growth_trends(time_series_data):
    """Calculate growth trends from time series data"""
    print("Creating growth trends")
    
    if time_series_data is None or "monthly_sales" not in time_series_data:
        print("Missing monthly sales data for growth trends")
        return None
    
    try:
        monthly_sales = time_series_data["monthly_sales"]

        # Check if data exists
        if monthly_sales.count() == 0:
            print("No monthly sales data available for analysis")
            return None
        
        # Calculate overall growth trends
        recent_months = monthly_sales.orderBy(col("month_idx").desc()).limit(6)

        # Verify we have data after filtering
        recent_month_count = recent_months.count()
        print(f"Found {recent_month_count} recent months for growth trend analysis")
        if recent_month_count == 0:
            print("No recent months data available for growth trends")
            return None
        
        # Calculate average growth rates
        avg_mom_growth = recent_months.select(avg("mom_growth")).collect()[0][0]
        avg_yoy_growth = recent_months.filter(col("yoy_growth").isNotNull()).select(avg("yoy_growth")).collect()[0][0]
        
        # Create growth trend summary
        # Handle null values and use explicit typing
        growth_summary = spark.createDataFrame([
            (
                "Overall", 
                float(avg_mom_growth) if avg_mom_growth is not None else 0.0, 
                float(avg_yoy_growth) if avg_yoy_growth is not None else 0.0,
                float(recent_months.select(min("mom_growth")).collect()[0][0]) if recent_months.select(min("mom_growth")).collect()[0][0] is not None else 0.0,
                float(recent_months.select(max("mom_growth")).collect()[0][0]) if recent_months.select(max("mom_growth")).collect()[0][0] is not None else 0.0,
                float(recent_months.select(min("yoy_growth")).collect()[0][0]) if recent_months.select(min("yoy_growth")).collect()[0][0] is not None else 0.0,
                float(recent_months.select(max("yoy_growth")).collect()[0][0]) if recent_months.select(max("yoy_growth")).collect()[0][0] is not None else 0.0
            )
        ], ["segment", "avg_mom_growth", "avg_yoy_growth", "min_mom_growth", "max_mom_growth", "min_yoy_growth", "max_yoy_growth"])
        
        # Add category-level growth trends
        if "monthly_category_sales" in time_series_data:
            monthly_category_sales = time_series_data["monthly_category_sales"]
            
            # Get recent months by category
            max_month_idx = monthly_category_sales.select(max("month_idx")).collect()[0][0]
            recent_category_months = monthly_category_sales \
                .filter(col("month_idx") > (max_month_idx - 6)) \
                .orderBy("category", col("month_idx").desc())
            
            # Calculate average growth by category
            category_growth = recent_category_months \
                .groupBy("category") \
                .agg(
                    avg("category_mom_growth").alias("avg_mom_growth"),
                    avg("category_yoy_growth").alias("avg_yoy_growth"),
                    min("category_mom_growth").alias("min_mom_growth"),
                    max("category_mom_growth").alias("max_mom_growth"),
                    min("category_yoy_growth").alias("min_yoy_growth"),
                    max("category_yoy_growth").alias("max_yoy_growth")
                ) \
                .withColumnRenamed("category", "segment")
            
            # Combine overall and category growth
            growth_summary = growth_summary.union(category_growth)
        
        # Add growth trend classification
        growth_summary = growth_summary \
            .withColumn(
                "growth_trend",
                when(col("avg_yoy_growth") >= 20, "Strong Growth")
                .when(col("avg_yoy_growth") >= 10, "Healthy Growth")
                .when(col("avg_yoy_growth") >= 0, "Stable Growth")
                .when(col("avg_yoy_growth") >= -10, "Slight Decline")
                .otherwise("Significant Decline")
            )
        
        # Add momentum indicator based on MoM vs YoY comparison
        growth_summary = growth_summary \
            .withColumn(
                "momentum",
                when(col("avg_mom_growth") > col("avg_yoy_growth"), "Accelerating")
                .when(col("avg_mom_growth") > 0, "Continuing")
                .when((col("avg_mom_growth") < 0 ) & (col("avg_yoy_growth") > 0), "Slowing")
                .otherwise("Declining")
            )
        
        return growth_summary
    
    except Exception as e:
        print(f"Error creating growth trends: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_seasonality_analysis(time_series_data):
    """Analyze seasonality patterns in sales data"""
    print("Creating seasonality analysis")
    
    if time_series_data is None or "monthly_sales" not in time_series_data:
        print("Missing monthly sales data for seasonality analysis")
        return None
    
    try:
        monthly_sales = time_series_data["monthly_sales"]
        
        # Get average sales by month across all years
        month_seasonality = monthly_sales \
            .groupBy("month") \
            .agg(
                avg("monthly_revenue").alias("avg_monthly_revenue"),
                avg("monthly_order_count").alias("avg_monthly_orders"),
                avg("monthly_quantity").alias("avg_monthly_quantity")
            )
        
        # Verify month seasonality data
        month_count = month_seasonality.count()
        print(f"Found {month_count} months for seasonality analysis")
        if month_count == 0:
            print("No month data available for seasonality analysis")
            return None
        
        # Get overall monthly average
        overall_avg = monthly_sales \
            .select(avg("monthly_revenue").alias("overall_avg_revenue")) \
            .collect()[0][0]
        
        print(f"Overall average revenue: {overall_avg}")
        
        # Calculate seasonal index (ratio of month avg to overall avg)
        seasonality_index = month_seasonality \
            .withColumn(
                "seasonal_index",
                when(col("avg_monthly_revenue").isNotNull() & lit(overall_avg).isNotNull() & (lit(overall_avg) > 0),
                    col("avg_monthly_revenue") / lit(overall_avg)
                ).otherwise(lit(1.0))
            ) \
            .orderBy("month")
        
        # Add month names
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
            7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
        }
        
        mapping_expr = expr("CASE " + " ".join([f"WHEN month = {k} THEN '{v}'" for k, v in month_names.items()]) + " END")
        seasonality_index = seasonality_index \
            .withColumn("month_name", mapping_expr)
        
        # Create category-level seasonality if available
        category_seasonality = None
        if "monthly_category_sales" in time_series_data:
            monthly_category_sales = time_series_data["monthly_category_sales"]
            
            # Calculate average sales by month and category
            category_month_seasonality = monthly_category_sales \
                .groupBy("category", "month") \
                .agg(
                    avg("monthly_category_revenue").alias("avg_category_revenue"),
                    avg("monthly_category_orders").alias("avg_category_orders"),
                    avg("monthly_category_quantity").alias("avg_category_quantity")
                )
            
            # Calculate overall average by category
            category_overall_avg = monthly_category_sales \
                .groupBy("category") \
                .agg(avg("monthly_category_revenue").alias("category_overall_avg"))
            
            # Join to calculate seasonal index
            category_seasonality = category_month_seasonality \
                .join(category_overall_avg, "category") \
                .withColumn(
                    "category_seasonal_index",
                    when(col("category_overall_avg") > 0, 
                        col("avg_category_revenue") / col("category_overall_avg"))
                    .otherwise(1.0)
                ) \
                .withColumn("month_name", mapping_expr) \
                .orderBy("category", "month")
        
        return {
            "overall_seasonality": seasonality_index,
            "category_seasonality": category_seasonality
        }
    
    except Exception as e:
        print(f"Error creating seasonality analysis: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_sales_forecast(time_series_data, date_ranges):
    """Create sales forecasts using simple linear regression"""
    print("Creating sales forecasts")
    
    if time_series_data is None or "monthly_sales" not in time_series_data:
        print("Missing monthly sales data for forecasting")
        return None
    
    try:
        monthly_sales = time_series_data["monthly_sales"]
        
        # Prepare data for overall sales forecast
        forecast_data = monthly_sales.select(
            "month_idx",
            "month_year",
            "year",
            "month",
            "monthly_revenue",
            "monthly_order_count",
            "monthly_quantity"
        ).orderBy("month_idx")
        
        # Use linear regression for time series forecasting
        # First, prepare the feature vector
        assembler = VectorAssembler(inputCols=["month_idx"], outputCol="features")
        forecast_vector = assembler.transform(forecast_data)
        
        # Train models for different metrics
        revenue_model = LinearRegression(featuresCol="features", labelCol="monthly_revenue")
        revenue_model_fit = revenue_model.fit(forecast_vector)
        
        order_model = LinearRegression(featuresCol="features", labelCol="monthly_order_count")
        order_model_fit = order_model.fit(forecast_vector)
        
        quantity_model = LinearRegression(featuresCol="features", labelCol="monthly_quantity")
        quantity_model_fit = quantity_model.fit(forecast_vector)
        
        # Get the next periods to forecast
        max_month_idx = forecast_data.select(max("month_idx")).collect()[0][0]
        max_year = forecast_data.select(max("year")).collect()[0][0]
        max_month = forecast_data.filter(col("year") == max_year).select(max("month")).collect()[0][0]
        
        # Create future periods for forecasting (3 months ahead)
        future_periods = []
        for i in range(1, 4):
            future_month_idx = max_month_idx + i
            future_year = max_year
            future_month = max_month + i
            
            # Handle year rollover
            if future_month > 12:
                future_month = future_month - 12
                future_year = future_year + 1
            
            future_periods.append((future_month_idx, f"{future_year}-{future_month}", future_year, future_month))
        
        future_df = spark.createDataFrame(future_periods, ["month_idx", "month_year", "year", "month"])
        future_vector = assembler.transform(future_df)
        
        # Make predictions
        revenue_forecast = revenue_model_fit.transform(future_vector)
        order_forecast = order_model_fit.transform(future_vector)
        quantity_forecast = quantity_model_fit.transform(future_vector)
        
        # Combine forecasts
        forecast_results = revenue_forecast.select(
            "month_idx", "month_year", "year", "month", 
            col("prediction").alias("forecasted_revenue")
        ).join(
            order_forecast.select(
                "month_idx", col("prediction").alias("forecasted_orders")
            ),
            "month_idx"
        ).join(
            quantity_forecast.select(
                "month_idx", col("prediction").alias("forecasted_quantity")
            ),
            "month_idx"
        )
        
        # Add confidence metrics
        forecast_results = forecast_results \
            .withColumn("forecast_lower_bound", col("forecasted_revenue") * 0.9) \
            .withColumn("forecast_upper_bound", col("forecasted_revenue") * 1.1) \
            .withColumn("confidence_level", lit("Medium"))  # Simplified confidence level
        
        # Create category-level forecasts if available
        category_forecasts = None
        if "monthly_category_sales" in time_series_data:
            monthly_category_sales = time_series_data["monthly_category_sales"]
            
            # Get unique categories
            categories = [row.category for row in monthly_category_sales.select("category").distinct().collect()]
            
            all_category_forecasts = []
            
            for category in categories:
                # Filter data for this category
                category_data = monthly_category_sales \
                    .filter(col("category") == category) \
                    .select(
                        "month_idx",
                        "month_year",
                        "year",
                        "month",
                        "category",
                        "monthly_category_revenue",
                        "monthly_category_orders",
                        "monthly_category_quantity"
                    ) \
                    .orderBy("month_idx")
                
                # Create feature vector
                category_vector = assembler.transform(category_data)
                
                # Train category models
                cat_revenue_model = LinearRegression(featuresCol="features", labelCol="monthly_category_revenue")
                cat_revenue_fit = cat_revenue_model.fit(category_vector)
                
                # Create future data for this category
                future_category = future_df.withColumn("category", lit(category))
                future_cat_vector = assembler.transform(future_category)
                
                # Make category predictions
                category_forecast = cat_revenue_fit.transform(future_cat_vector)
                
                # Add to collection
                category_forecast = category_forecast.select(
                    "month_idx", "month_year", "year", "month", "category",
                    col("prediction").alias("forecasted_category_revenue")
                )
                
                all_category_forecasts.append(category_forecast)
            
            # Combine all category forecasts
            if all_category_forecasts:
                category_forecasts = all_category_forecasts[0]
                for df in all_category_forecasts[1:]:
                    category_forecasts = category_forecasts.union(df)
                
                # Add confidence metrics
                category_forecasts = category_forecasts \
                    .withColumn("forecast_lower_bound", col("forecasted_category_revenue") * 0.9) \
                    .withColumn("forecast_upper_bound", col("forecasted_category_revenue") * 1.1) \
                    .withColumn("confidence_level", lit("Medium"))
        
        return {
            "overall_forecast": forecast_results,
            "category_forecast": category_forecasts
        }
    
    except Exception as e:
        print(f"Error creating sales forecast: {e}")
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

def create_sales_forecast_gold(process_date):
    """Main function to create Gold-level sales forecast analytics"""
    print(f"Creating sales forecast Gold layer for {process_date.strftime('%Y-%m-%d')}")
    
    # Get date ranges for the analysis
    date_ranges = get_date_ranges(process_date)
    
    # Load data from Silver
    orders = load_orders_data()
    order_items = load_order_items_data()

    # Add these verification statements
    if orders is not None:
        print(f"Loaded orders data with {orders.count()} records")
        # Check what order statuses actually exist
        distinct_statuses = orders.select("order_status").distinct().collect()
        print(f"Distinct order statuses: {[row.order_status for row in distinct_statuses]}")

    else:
        print("Failed to load orders data")
        
    if order_items is not None:
        print(f"Loaded order items data with {order_items.count()} records")
    else:
        print("Failed to load order items data")

    # Process data and create gold tables
    
    # 1. Create time series data
    time_series_data = create_time_series_data(orders, order_items, date_ranges)
    
    # 2. Create growth trends
    growth_trends = create_growth_trends(time_series_data)
    
    # 3. Create seasonality analysis
    seasonality_analysis = create_seasonality_analysis(time_series_data)
    
    # 4. Create sales forecast
    sales_forecast = create_sales_forecast(time_series_data, date_ranges)
    
    # Add processing date information to all datasets
    processing_year = process_date.year
    processing_month = process_date.month
    processing_day = process_date.day
    
    # Format for partition paths
    processing_date_str = process_date.strftime('%Y-%m-%d')
    
    success = True
    
    # Save time series data to Gold
    if time_series_data is not None:
        # Save monthly sales
        monthly_sales = time_series_data.get("monthly_sales")
        if monthly_sales is not None:
            monthly_sales = monthly_sales \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                monthly_sales,
                f"{MINIO_GOLD_SALES_FORECAST}/monthly_sales",
                ["year", "processing_year", "processing_month"]
            )
        
        # Save monthly category sales
        monthly_category_sales = time_series_data.get("monthly_category_sales")
        if monthly_category_sales is not None:
            monthly_category_sales = monthly_category_sales \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                monthly_category_sales,
                f"{MINIO_GOLD_SALES_FORECAST}/monthly_category_sales",
                ["category", "year", "processing_year", "processing_month"]
            )
    
    # Save growth trends to Gold
    if growth_trends is not None:
        growth_trends = growth_trends \
            .withColumn("processing_date", lit(processing_date_str)) \
            .withColumn("processing_year", lit(processing_year)) \
            .withColumn("processing_month", lit(processing_month)) \
            .withColumn("processing_day", lit(processing_day))
        
        success = success and save_to_gold(
            growth_trends,
            f"{MINIO_GOLD_SALES_FORECAST}/growth_trends",
            ["segment", "processing_year", "processing_month"]
        )
    
    # Save seasonality analysis to Gold
    if seasonality_analysis is not None:
        # Save overall seasonality
        overall_seasonality = seasonality_analysis.get("overall_seasonality")
        if overall_seasonality is not None:
            overall_seasonality = overall_seasonality \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                overall_seasonality,
                f"{MINIO_GOLD_SALES_FORECAST}/seasonality_index",
                ["month", "processing_year", "processing_month"]
            )
        
        # Save category seasonality
        category_seasonality = seasonality_analysis.get("category_seasonality")
        if category_seasonality is not None:
            category_seasonality = category_seasonality \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                category_seasonality,
                f"{MINIO_GOLD_SALES_FORECAST}/category_seasonality",
                ["category", "month", "processing_year", "processing_month"]
            )
    
    # Save sales forecast to Gold
    if sales_forecast is not None:
        # Save overall forecast
        overall_forecast = sales_forecast.get("overall_forecast")
        if overall_forecast is not None:
            overall_forecast = overall_forecast \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                overall_forecast,
                f"{MINIO_GOLD_SALES_FORECAST}/overall_forecast",
                ["year", "month", "processing_year", "processing_month"]
            )
        
        # Save category forecast
        category_forecast = sales_forecast.get("category_forecast")
        if category_forecast is not None:
            category_forecast = category_forecast \
                .withColumn("processing_date", lit(processing_date_str)) \
                .withColumn("processing_year", lit(processing_year)) \
                .withColumn("processing_month", lit(processing_month)) \
                .withColumn("processing_day", lit(processing_day))
            
            success = success and save_to_gold(
                category_forecast,
                f"{MINIO_GOLD_SALES_FORECAST}/category_forecast",
                ["category", "year", "month", "processing_year", "processing_month"]
            )
    
    return success

if __name__ == "__main__":
    # Get date from command line arguments or use default
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    process_date = get_date_to_process(date_arg)
    
    # Create sales forecast Gold layer
    success = create_sales_forecast_gold(process_date)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)