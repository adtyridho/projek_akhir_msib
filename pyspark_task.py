from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, date_format

def process_data():
    # Create Spark session
    spark = SparkSession.builder.appName("SalesProcessing").getOrCreate()

    # Task 1: Read Data
    df = spark.read.csv("/home/ridho/dummy_data/sales.csv", header=True, inferSchema=True)

    # Task 2: Process Data
    result_df = df.groupBy(date_format(col("date"), "yyyy-MM-dd").alias("date"), col("product_name")) \
                  .agg(sum(col("price") * col("quantity")).alias("total_sales"))

    # Stop Spark session
    spark.stop()

    return result_df

if __name__ == "__main__":
    process_data()
