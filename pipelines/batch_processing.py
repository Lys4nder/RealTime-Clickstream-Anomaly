import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, quarter, hour, sum, count, desc, date_format

spark_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "io.delta:delta-spark_2.12:3.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

spark = (SparkSession.builder
         .appName("InteractiveETL_History")
         .config("spark.jars.packages", ",".join(spark_packages))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .getOrCreate()
         )

spark.sparkContext.setLogLevel("WARN")

path_to_parquet = "/data_producers/orders_transaction_history/Orders_Transaction_History.parquet"

try:
    df = spark.read.parquet(path_to_parquet)

    print("=== Data Schema ===")
    df.printSchema()

    print("=== First 5 rows ===")
    df.show(5)

    print("\n=== Total spent per month ===")
    df_monthly_spend = (df
                        .withColumn("month", month(col("timestamp")))
                        .groupBy("month")
                        .agg(sum("total_amount_usd").alias("total_sales"))
                        .orderBy("month")
                        )
    df_monthly_spend.show()

    print("\n=== Top Countires by Q1 orders number ===")
    df_q1_countries = (df
                       .withColumn("quarter", quarter(col("timestamp")))
                       .filter(col("quarter") == 1)
                       .groupBy("shipping_country")
                       .count()
                       .orderBy(col("count").desc())
                       )
    df_q1_countries.show()

    print("\n=== Hours with most orders (peaks) ===")
    df_hourly_activity = (df
                          .withColumn("hour", hour(col("timestamp")))
                          .groupBy("hour")
                          .count()
                          .orderBy(col("count").desc())
                          )
    df_hourly_activity.show(5)

except Exception as e:
    print(f"Error reading the file. {e}")

spark.stop()