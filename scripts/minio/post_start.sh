#!/bin/sh
set -eu

MINIO_ALIAS="local"
MINIO_URL="http://localhost:9000"

echo "Waiting for MinIO at $MINIO_URL..."

until mc alias set \
    "$MINIO_ALIAS" \
    "$MINIO_URL" \
    "$MINIO_ROOT_USER" \
    "$MINIO_ROOT_PASSWORD" \
    >/dev/null 2>&1
do
    sleep 1
done

echo "MinIO is ready."
echo "Ensuring required buckets exist..."

mc mb --ignore-existing \
    "$MINIO_ALIAS/datalake" \
    "$MINIO_ALIAS/spark-events"

echo "Bucket ready: datalake"
echo "Bucket ready: spark-events"
echo "Post-start initialization completed."