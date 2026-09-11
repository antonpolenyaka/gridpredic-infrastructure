#!/bin/sh
set -eu

pg_isready \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    >/dev/null 2>&1