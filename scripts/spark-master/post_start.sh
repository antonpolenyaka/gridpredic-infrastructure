#!/bin/sh
set -eu

READY_FILE="/tmp/post_start_completed"

rm -f "$READY_FILE"

echo "Creating Lakehouse databases..."

spark-sql \
    --master "local[1]" \
    -e "
        CREATE DATABASE IF NOT EXISTS l1_bronze
        LOCATION 's3a://datalake/01_bronze';

        CREATE DATABASE IF NOT EXISTS l2_silver
        LOCATION 's3a://datalake/02_silver';

        CREATE DATABASE IF NOT EXISTS l3_gold
        LOCATION 's3a://datalake/03_gold';
    "

touch "$READY_FILE"

echo "Lakehouse databases are ready."