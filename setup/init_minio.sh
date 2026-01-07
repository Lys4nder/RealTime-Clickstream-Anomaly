#!/bin/sh

set -e 

MINIO_ALIAS="minio"
BUCKET_NAME="lakehouse"
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

echo "Starting MinIO initialization..."

echo "1. Setting MinIO client alias and waiting for server..."
until mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; do
    echo '   ...waiting for MinIO to be reachable...';
    sleep 5;
done

echo "2. Checking for bucket '$BUCKET_NAME'..."
if mc ls $MINIO_ALIAS/$BUCKET_NAME >/dev/null 2>&1; then
    echo "   Bucket '$BUCKET_NAME' already exists. Skipping creation.";
else
    echo "   Bucket '$BUCKET_NAME' does not exist. Creating now...";
    mc mb $MINIO_ALIAS/$BUCKET_NAME;
    echo "   Bucket '$BUCKET_NAME' created successfully.";
fi

echo "MinIO Initialization complete."