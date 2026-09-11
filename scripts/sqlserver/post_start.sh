#!/bin/sh
set -eu

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

echo "Waiting for SQL Server..."

until "$SQLCMD" \
    -S localhost \
    -U sa \
    -P "$MSSQL_SA_PASSWORD" \
    -C \
    -b \
    -Q "SELECT 1" \
    >/dev/null 2>&1
do
    sleep 1
done

echo "SQL Server is ready."

echo "Restoring databases..."

"$SQLCMD" \
    -S localhost \
    -U sa \
    -P "$MSSQL_SA_PASSWORD" \
    -C \
    -b \
    -i /scripts/sql/restore_databases.sql

echo "Database restore completed."

echo "Waiting for application databases..."

until "$SQLCMD" \
    -S localhost \
    -U sa \
    -P "$MSSQL_SA_PASSWORD" \
    -C \
    -b \
    -Q "USE Calser_EOSA; USE Calser_Pitarch; USE Calser_ValleSantaAna; USE TedisNet_EOSA;" \
    >/dev/null 2>&1
do
    sleep 1
done

echo "Application databases are ready."

echo "Configuring CDC..."

"$SQLCMD" \
    -S localhost \
    -U sa \
    -P "$MSSQL_SA_PASSWORD" \
    -C \
    -b \
    -i /scripts/sql/enable_cdc.sql

echo "CDC is ready."
echo "Post-start initialization completed."