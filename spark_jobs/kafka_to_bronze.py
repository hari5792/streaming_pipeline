from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Schema
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("product", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("event_time", DoubleType())
])

# Read Stream From Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert Kafka Binary -> String
value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse JSON
parsed_df = value_df.select(
    from_json(col("value"), order_schema).alias("data")
).select("data.*")

# Add ingestion timestamp
final_df = parsed_df.withColumn(
    "ingestion_time",
    current_timestamp()
)

# Write Stream to Bronze Layer
query = final_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark/data/bronze/orders") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints/orders") \
    .outputMode("append") \
    .start()

query.awaitTermination()