#!/bin/sh
set -eu

STATUS="$(curl -fsS \
    http://localhost:8083/connectors/tedisnet-eosa-connector/status)"

echo "$STATUS" | grep -q '"connector":{"state":"RUNNING"'
echo "$STATUS" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'