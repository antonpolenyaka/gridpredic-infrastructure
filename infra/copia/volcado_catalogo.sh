#!/bin/sh
# Runs inside the postgres container (copia_diaria.ps1 copies it to /tmp): dumps the Hive
# catalog (metastore_db) in the custom format of pg_dump to /tmp/metastore_db.dump and prints
# the number of tables of the catalog, to compare with the dump once restored.
set -eu
export PGPASSWORD="$POSTGRES_PASSWORD"
pg_dump -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f /tmp/metastore_db.dump
psql -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SELECT count(*) FROM "TBLS"'
