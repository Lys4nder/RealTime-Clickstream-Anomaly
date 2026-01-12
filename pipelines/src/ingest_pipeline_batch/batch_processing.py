from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    month,
    quarter,
    hour,
    sum,
)
from helpers import get_spark_session

# --- Configuration Paths ---

# INPUT: Raw data (Bronze Layer)
# We read from the upload bucket where the file lands initially
PATH_RAW = "s3a://parquet-uploads/Orders_Transaction_History.parquet"

# OUTPUT: Your Lakehouse Bucket Structure
# Silver Layer: Cleaned, enriched data (Single Source of Truth)
PATH_SILVER = "s3a://lakehouse/silver/orders"

# Gold Layer: Aggregated tables ready for the Dashboard/API
PATH_GOLD_MONTHLY = "s3a://lakehouse/gold/monthly_sales"
PATH_GOLD_COUNTRIES = "s3a://lakehouse/gold/country_stats"
PATH_GOLD_HOURLY = "s3a://lakehouse/gold/hourly_peaks"


def process_batch(spark: SparkSession):
    try:
        # 1. READ RAW DATA (Bronze)
        print(f"Reading raw data from: {PATH_RAW}")
        df = spark.read.parquet(PATH_RAW)

        print("=== Data Schema ===")
        df.printSchema()

        # 2. PROCESS SILVER LAYER
        print("Transforming data (fixing timestamps)...")
        # Fix the timestamp: Convert Nanoseconds (Long) -> Seconds -> Timestamp
        df_silver = df.withColumn(
            "timestamp", (col("transaction_timestamp") / 1000000000).cast("timestamp")
        )

        # SAVE SILVER TABLE
        print(f"Saving SILVER table to: {PATH_SILVER}")
        (df_silver.write.format("delta").mode("overwrite").save(PATH_SILVER))
        print("Silver table saved successfully.\n")

        # 3. PROCESS GOLD LAYERS

        # --- A. Monthly Spend ---
        print("=== Computing Total spent per month ===")
        df_monthly_spend = (
            df_silver.withColumn("month", month(col("timestamp")))
            .groupBy("month")
            .agg(sum("total_amount_usd").alias("total_sales"))
            .orderBy("month")
        )
        # Save Gold Table
        print(f"Saving GOLD (Monthly) to: {PATH_GOLD_MONTHLY}")
        df_monthly_spend.write.format("delta").mode("overwrite").save(PATH_GOLD_MONTHLY)

        # --- B. Top Countries (Q1) ---
        print("\n=== Computing Top Countries by Q1 orders ===")
        df_q1_countries = (
            df_silver.withColumn("quarter", quarter(col("timestamp")))
            .filter(col("quarter") == 1)
            .groupBy("shipping_country")
            .count()
            .orderBy(col("count").desc())
        )
        # Save Gold Table
        print(f"Saving GOLD (Countries) to: {PATH_GOLD_COUNTRIES}")
        df_q1_countries.write.format("delta").mode("overwrite").save(
            PATH_GOLD_COUNTRIES
        )

        # --- C. Hourly Peaks ---
        print("\n=== Computing Hourly Peaks ===")
        df_hourly_activity = (
            df_silver.withColumn("hour", hour(col("timestamp")))
            .groupBy("hour")
            .count()
            .orderBy(col("count").desc())
        )
        # Save Gold Table
        print(f"Saving GOLD (Hourly) to: {PATH_GOLD_HOURLY}")
        df_hourly_activity.write.format("delta").mode("overwrite").save(
            PATH_GOLD_HOURLY
        )

        print("\nAll batch jobs completed and Lakehouse populated.")

    except Exception as e:
        print(f"Error processing batch: {e}")
        # Re-raise to ensure the container exits with an error status
        raise e


def main():
    spark = None
    try:
        spark = get_spark_session("BatchProcessingApp")
        process_batch(spark)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
