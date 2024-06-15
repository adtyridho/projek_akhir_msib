from pyspark.sql import SparkSession

def read_data_transaction():
    # Membuat sesi Spark
    spark = SparkSession.builder \
        .appName("Read Data Transaction") \
        .getOrCreate()

    # Jalur file CSV lokal
    file_path = "file:///home/ridho/dummy_data/transaction.csv"

    # Membaca file CSV menjadi DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    return df
 