import duckdb
import os
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from duckdb import DuckDBPyConnection
import pandas as pd
from urllib.parse import urlparse

app = FastAPI()

# Enable CORS for Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Gold Table Paths
PATH_GOLD_MONTHLY = "s3://lakehouse/gold/monthly_sales"
PATH_GOLD_COUNTRIES = "s3://lakehouse/gold/country_stats"
PATH_GOLD_HOURLY = "s3://lakehouse/gold/hourly_peaks"


def get_db_connection():
    conn = duckdb.connect()

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL delta; LOAD delta;")

    parsed = urlparse(
        MINIO_ENDPOINT if "//" in MINIO_ENDPOINT else f"//{MINIO_ENDPOINT}", scheme=""
    )
    endpoint = parsed.netloc or parsed.path

    conn.execute(
        f"""
        CREATE OR REPLACE SECRET secret1 (
            TYPE S3,
            KEY_ID '{ACCESS_KEY}',
            SECRET '{SECRET_KEY}',
            REGION 'us-east-1',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """
    )

    try:
        yield conn
    finally:
        conn.close()


# --- ROUTES ---
# Notice how we pass 'conn' as an argument now!


@app.get("/api/monthly-sales")
def get_monthly_sales(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_MONTHLY}') ORDER BY month ASC"
    df = conn.execute(query).fetchdf()
    # Handle NaN values for valid JSON
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


@app.get("/api/country-stats")
def get_country_stats(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_COUNTRIES}') LIMIT 10"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@app.get("/api/hourly-peaks")
def get_hourly_peaks(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_HOURLY}') ORDER BY hour ASC"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")
