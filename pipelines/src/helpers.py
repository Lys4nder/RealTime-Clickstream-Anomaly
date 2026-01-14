import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def get_spark_session(app_name="MyApp", cores="2") -> SparkSession:
    # Load repo-local envs first so user environment variables can still override
    load_dotenv()

    spark_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "io.delta:delta-spark_2.12:3.0.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    builder = SparkSession.builder.appName(app_name)

    # S3/MinIO settings: prefer environment values, fall back to sensible defaults
    s3_endpoint = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
    s3_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    s3_path_style = os.environ.get("AWS_S3_PATH_STYLE", "true")
    s3_impl = os.environ.get("AWS_S3_IMPL", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    s3_ssl = os.environ.get("AWS_S3_SSL_ENABLED", "false")

    builder = (
        builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Performance: reduce default shuffle partitions for smaller clusters/local dev
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.access.key", s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style)
        .config("spark.hadoop.fs.s3a.impl", s3_impl)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl)
        .config("spark.jars.packages", ",".join(spark_packages))
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.cores.max", cores)
    )

    # Master and driver settings from env
    spark_master = os.environ.get("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    driver_bind = os.environ.get("SPARK_DRIVER_BIND_ADDRESS")
    driver_port = os.environ.get("SPARK_DRIVER_PORT")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    if driver_bind:
        builder = builder.config("spark.driver.bindAddress", driver_bind)
    if driver_port:
        builder = builder.config("spark.driver.port", driver_port)

    # Create a lightweight session for diagnostics
    spark = builder.getOrCreate()
    return spark
