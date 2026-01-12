import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    session_window,
    unix_timestamp,
    max,
    min,
    count,
    col,
    from_json,
    window,
    when,
    current_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from helpers import get_spark_session

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream_events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH_ANOMALY = "s3a://lakehouse/checkpoints_anomaly/"
OUTPUT_PATH_ANOMALY = "s3a://lakehouse/gold/processed_data/"

CHECKPOINT_PATH_DEVICES = "s3a://lakehouse/checkpoints_devices/"
OUTPUT_PATH_DEVICES = "s3a://lakehouse/gold/device_stats/"

CHECKPOINT_PATH_TRENDING = "s3a://lakehouse/checkpoints_trending/"
OUTPUT_PATH_TRENDING = "s3a://lakehouse/gold/trending_pages/"

CHECKPOINT_PATH_SESSIONS = "s3a://lakehouse/checkpoints_sessions/"
OUTPUT_PATH_SESSIONS = "s3a://lakehouse/gold/session_stats/"

# SILVER layer: store raw/enriched clickstreams as single-source-of-truth
CHECKPOINT_PATH_SILVER = "s3a://lakehouse/checkpoints_silver/"
OUTPUT_PATH_SILVER = "s3a://lakehouse/silver/clickstreams/"


schema = StructType(
    [
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
        StructField("page_section", StringType(), True),
    ]
)


def write_to_delta_and_kafka(batch_df, batch_id, delta_path, kafka_topic):
    # 1. WRITE TO DELTA (History / Single Source of Truth)
    batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
        delta_path
    )

    # 2. WRITE TO KAFKA (Real-Time Stream)
    (
        batch_df.select(to_json(struct("*")).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", kafka_topic)
        .save()
    )


def run_streaming(spark: SparkSession):
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    json_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    # 1. SILVER LAYER (High Priority - Fast Write)
    silver_write_df = json_stream.withColumn("ingested_at", current_timestamp())
    query_silver = (
        silver_write_df.writeStream.outputMode("append")
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH_SILVER)
        .option("path", OUTPUT_PATH_SILVER)
        .trigger(processingTime="2 seconds")
        .start()
    )

    # 2. ANOMALY DETECTION (Medium Priority)
    anomaly_df = (
        json_stream.withWatermark("event_timestamp", "1 second")
        .groupBy(
            window(col("event_timestamp"), "5 seconds", "1 second"),
            col("user_id"),
            col("country"),
        )
        .agg(count("*").alias("actions_count"))
        .withColumn("is_anomaly", when(col("actions_count") > 5, True).otherwise(False))
        .withColumn("processed_at", current_timestamp())
        .withColumn("event_timestamp", col("window").getField("start"))
    )

    query_anomaly = (
        anomaly_df.writeStream.outputMode("append")
        .foreachBatch(
            lambda df, epoch: write_to_delta_and_kafka(
                df, epoch, OUTPUT_PATH_ANOMALY, "stats_anomalies"
            )
        )
        .option("checkpointLocation", CHECKPOINT_PATH_ANOMALY)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 3. DEVICE STATS (Low Priority)
    device_stats_df = (
        json_stream.withWatermark("event_timestamp", "1 second")
        .groupBy(
            window(col("event_timestamp"), "5 seconds", "1 second"), col("device_type")
        )
        .agg(count("*").alias("device_count"))
        .withColumn("processed_at", current_timestamp())
        .withColumn("event_timestamp", col("window").getField("start"))
    )

    query_devices = (
        device_stats_df.writeStream.outputMode("append")
        .foreachBatch(
            lambda df, epoch: write_to_delta_and_kafka(
                df, epoch, OUTPUT_PATH_DEVICES, "stats_devices"
            )
        )
        .option("checkpointLocation", CHECKPOINT_PATH_DEVICES)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 4. TRENDING PAGES (Low Priority)
    trending_pages_df = (
        json_stream.withWatermark("event_timestamp", "1 second")
        .groupBy(
            window(col("event_timestamp"), "10 seconds", "5 seconds"),
            col("page_section"),
        )
        .agg(count("*").alias("visit_count"))
        .withColumn("event_timestamp", col("window").getField("start"))
    )

    query_trending = (
        trending_pages_df.writeStream.outputMode("append")
        .foreachBatch(
            lambda df, epoch: write_to_delta_and_kafka(
                df, epoch, OUTPUT_PATH_TRENDING, "stats_trending"
            )
        )
        .option("checkpointLocation", CHECKPOINT_PATH_TRENDING)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 5. SESSION DURATION (Low Priority)
    session_stats_df = (
        json_stream.withWatermark("event_timestamp", "1 second")
        .groupBy(session_window(col("event_timestamp"), "5 seconds"), col("session_id"))
        .agg(
            (
                unix_timestamp(max("event_timestamp"))
                - unix_timestamp(min("event_timestamp"))
            ).alias("duration_seconds"),
            count("*").alias("events_in_session"),
        )
        .withColumn("event_timestamp", col("session_window").getField("start"))
        .where(col("duration_seconds") > 0)
    )

    query_sessions = (
        session_stats_df.writeStream.outputMode("append")
        .foreachBatch(
            lambda df, epoch: write_to_delta_and_kafka(
                df, epoch, OUTPUT_PATH_SESSIONS, "stats_sessions"
            )
        )
        .option("checkpointLocation", CHECKPOINT_PATH_SESSIONS)
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


def main():
    spark = get_spark_session("StreamingPipeline", cores="4")
    spark.sparkContext.setLogLevel("WARN")

    run_streaming(spark)


if __name__ == "__main__":
    main()
