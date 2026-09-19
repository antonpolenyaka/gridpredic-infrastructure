#!/bin/sh
set -eu

curl --fail --silent \
    http://localhost:8080/api/v2/monitor/health \
    >/dev/null