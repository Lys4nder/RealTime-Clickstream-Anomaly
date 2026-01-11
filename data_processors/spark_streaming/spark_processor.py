import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import session_window, unix_timestamp, max, min, count, col, from_json, window, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream_events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH_ANOMALY = "s3a://lakehouse/checkpoints_anomaly/"
OUTPUT_PATH_ANOMALY = "s3a://lakehouse/processed_data/"

CHECKPOINT_PATH_DEVICES = "s3a://lakehouse/checkpoints_devices/"
OUTPUT_PATH_DEVICES = "s3a://lakehouse/device_stats/"

CHECKPOINT_PATH_TRENDING = "s3a://lakehouse/checkpoints_trending/"
OUTPUT_PATH_TRENDING = "s3a://lakehouse/trending_pages/"

CHECKPOINT_PATH_SESSIONS = "s3a://lakehouse/checkpoints_sessions/"
OUTPUT_PATH_SESSIONS = "s3a://lakehouse/session_stats/"

print("--- SPARK MULTI-STREAM PIPELINE STARTED ---", flush=True)

spark = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
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

#anomaly detection
anomaly_df = json_stream \
    .withWatermark("event_timestamp", "1 minute") \
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("user_id"),
        col("country")
    ) \
    .agg(count("*").alias("actions_count")) \
    .withColumn("is_anomaly", when(col("actions_count") > 5, True).otherwise(False)) \
    .withColumn("processed_at", current_timestamp())

query_anomaly = anomaly_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH_ANOMALY) \
    .option("checkpointLocation", CHECKPOINT_PATH_ANOMALY) \
    .trigger(processingTime="10 seconds") \
    .start()

#device type distribution
device_stats_df = json_stream \
    .withWatermark("event_timestamp", "1 minute") \
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("device_type")
    ) \
    .agg(count("*").alias("device_count")) \
    .withColumn("processed_at", current_timestamp())

query_devices = device_stats_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH_DEVICES) \
    .option("checkpointLocation", CHECKPOINT_PATH_DEVICES) \
    .trigger(processingTime="10 seconds") \
    .start()

#trending pages
trending_pages_df = json_stream \
    .withWatermark("event_timestamp", "5 minutes") \
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("page_section")
    ) \
    .agg(count("*").alias("visit_count"))

query_pages = trending_pages_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH_TRENDING) \
    .option("checkpointLocation", CHECKPOINT_PATH_TRENDING) \
    .trigger(processingTime="30 seconds") \
    .start()

#session duration
session_stats_df = json_stream \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        session_window(col("event_timestamp"), "10 minutes"),
        col("session_id")
    ) \
    .agg(
        (unix_timestamp(max("event_timestamp")) - unix_timestamp(min("event_timestamp"))).alias("duration_seconds"),
        count("*").alias("events_in_session")
    ) \
    .where(col("duration_seconds") > 0)

query_sessions = session_stats_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH_SESSIONS) \
    .option("checkpointLocation", CHECKPOINT_PATH_SESSIONS) \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()