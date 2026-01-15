from fastapi import APIRouter, Depends
from duckdb import DuckDBPyConnection

from src.dependencies import get_db_connection

router = APIRouter(prefix="/batch", tags=["batch"])

# Gold Table Paths (redeclare here or import from config if created)
PATH_GOLD_MONTHLY = "s3://lakehouse/gold/monthly_sales"
PATH_GOLD_COUNTRIES = "s3://lakehouse/gold/country_stats"
PATH_GOLD_HOURLY = "s3://lakehouse/gold/hourly_peaks"


@router.get("/monthly-sales")
def get_monthly_sales(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_MONTHLY}') ORDER BY month ASC"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@router.get("/country-stats")
def get_country_stats(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_COUNTRIES}') LIMIT 10"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@router.get("/hourly-peaks")
def get_hourly_peaks(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_HOURLY}') ORDER BY hour ASC"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")
