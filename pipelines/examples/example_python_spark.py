from pyspark.sql import SparkSession

# Define Maven dependencies for Kafka, Delta Lake, and S3
spark_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "io.delta:delta-spark_2.12:3.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

spark = (SparkSession.builder
    .appName("InteractiveETL")
    .config("spark.jars.packages", ",".join(spark_packages))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO/S3 Configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Example Kafka Read Stream:
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "your_topic")
      .load())
