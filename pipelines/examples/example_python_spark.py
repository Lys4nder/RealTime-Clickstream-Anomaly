import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables from a local .env file if present
load_dotenv()

# Define Maven dependencies for Kafka, Delta Lake, and S3
spark_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "io.delta:delta-spark_2.12:3.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]

# Build a minimal builder (no packages) so we can verify master/driver connectivity
builder = (
    SparkSession.builder.appName("InteractiveETL")
    # MinIO/S3 Configuration (safe to include for both minimal and full builders)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

# Apply master/driver overrides before creating the minimal session
spark_master = os.environ.get("SPARK_MASTER")
if spark_master:
    builder = builder.master(spark_master)

driver_host = os.environ.get("SPARK_DRIVER_HOST")
driver_bind = os.environ.get("SPARK_DRIVER_BIND_ADDRESS")
if driver_host:
    builder = builder.config("spark.driver.host", driver_host)
if driver_bind:
    builder = builder.config("spark.driver.bindAddress", driver_bind)

# Create a lightweight session for diagnostics
spark = builder.getOrCreate()


def verify_with_batch_example(spark):
    # Create a simple in-memory DataFrame to verify Spark is working
    data = [
        ("user1", "click", 1),
        ("user2", "view", 1),
        ("user1", "click", 1),
        ("user3", "click", 1),
        ("user2", "view", 1),
    ]
    cols = ["user_id", "event", "count"]
    df_sample = spark.createDataFrame(data, cols)

    print("== Sample DataFrame ==")
    df_sample.show(truncate=False)

    # Simple aggregation: count events per user
    agg = (
        df_sample.groupBy("user_id")
        .sum("count")
        .withColumnRenamed("sum(count)", "total_events")
    )
    print("== Aggregation: total_events per user ==")
    agg.show(truncate=False)


if __name__ == "__main__":
    # Run batch verification first
    verify_with_batch_example(spark)
    spark.stop()
