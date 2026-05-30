from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadBronze") \
    .getOrCreate()

df = spark.read.parquet("/opt/spark/data/bronze/orders")

df.show(truncate=False)