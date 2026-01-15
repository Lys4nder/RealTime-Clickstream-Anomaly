import duckdb
import os
from urllib.parse import urlparse
from duckdb import DuckDBPyConnection

# Configuration pulled from environment or defaults
MINIO_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


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
