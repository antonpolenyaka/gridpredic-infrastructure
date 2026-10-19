#!/bin/sh
# Runs inside the minio container (copia_diaria.ps1 copies it to /tmp): mirrors the live
# Landing of the bucket to /tmp/copia_vivo, which stays between days, so every day only the
# new files are copied. The credentials are those of the container and are never printed.
set -eu
mc alias set copia http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc mirror --overwrite --quiet copia/datalake/00_landing/vivo /tmp/copia_vivo >/dev/null
echo "copia_vivo: $(find /tmp/copia_vivo -type f | wc -l) ficheros, $(du -sm /tmp/copia_vivo | cut -f1) MB"
