from pyspark.sql import SparkSession

def main() : 
    spark = SparkSession.builder \
        .appName("Advertising Data to Bronze") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # simple hello world test
    print("Hello World")

    # create a sample dataframe
    data = [("2023-01-01", "campaign_1", 1000, 200, 50, 10, 5, 1, 0.5),
            ("2023-01-02", "campaign_2", 1500, 300, 75, 15, 7, 2, 0.6)]
    
    columns = ["date", "campaign_name", "impressions", "clicks", "conversions", "cost", "revenue", "roi", "cpc"]
    df = spark.createDataFrame(data, columns)
    df.show()
    spark.stop()

if __name__ == "__main__":
    main()