#!/bin/sh

set -e

# Simple helper to upload a local parquet file to a MinIO bucket using mc

PARQUET_PATH=${1:-"data_producers/orders_transaction_history/Orders_Transaction_History.parquet"}
DEST_BUCKET=${2:-"parquet-uploads"}
DEST_KEY=${3:-"Orders_Transaction_History.parquet"}

MINIO_ALIAS=${MINIO_ALIAS:-minio}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

if ! command -v mc >/dev/null 2>&1; then
    echo "mc (MinIO client) is not installed or not in PATH. Install mc and retry."
    exit 1
fi

if [ ! -f "$PARQUET_PATH" ]; then
    echo "Parquet file not found at: $PARQUET_PATH"
    exit 1
fi

echo "Setting up mc alias '$MINIO_ALIAS' -> $MINIO_ENDPOINT"
mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY || true

# First check if destination object already exists; exit early if present
echo "Checking if destination object '$DEST_KEY' already exists in bucket '$DEST_BUCKET'..."
if mc ls "$MINIO_ALIAS/$DEST_BUCKET/$DEST_KEY" >/dev/null 2>&1; then
    echo "Destination object already exists: $DEST_BUCKET/$DEST_KEY - skipping upload."
    exit 0
fi

echo "Creating destination bucket '$DEST_BUCKET' if missing..."
if mc ls $MINIO_ALIAS/$DEST_BUCKET >/dev/null 2>&1; then
    echo "Bucket exists: $DEST_BUCKET"
else
    mc mb $MINIO_ALIAS/$DEST_BUCKET
fi

echo "Uploading $PARQUET_PATH to $MINIO_ALIAS/$DEST_BUCKET/$DEST_KEY"
mc cp "$PARQUET_PATH" "$MINIO_ALIAS/$DEST_BUCKET/$DEST_KEY"

echo "Upload complete."
