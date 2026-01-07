import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream_events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/"
OUTPUT_PATH = "s3a://lakehouse/processed_data/"

print("--- SPARK ANOMALY DETECTOR STARTED ---", flush=True)

spark = SparkSession.builder \
    .appName("ClickstreamAnomalyDetector") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("click_sequence", IntegerType(), True),
    StructField("page_category", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("page_section", StringType(), True)
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

windowed_counts = json_stream \
    .withWatermark("event_timestamp", "1 minute") \
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("user_id"),
        col("country")
    ) \
    .agg(count("*").alias("actions_count"))

final_output = windowed_counts \
    .withColumn("is_anomaly", when(col("actions_count") > 5, True).otherwise(False)) \
    .withColumn("processed_at", current_timestamp())

query = final_output.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"Scriere date in: {OUTPUT_PATH}", flush=True)
query.awaitTermination()