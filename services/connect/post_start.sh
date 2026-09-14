#!/bin/sh
set -eu

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="tedisnet-eosa-connector"
CONFIG_FILE="/connectors/tedisnet-eosa-connector.json"

echo "Waiting for Kafka Connect..."

until curl -fsS "$CONNECT_URL/" >/dev/null 2>&1; do
  sleep 2
done

echo "Kafka Connect is running."

echo "Creating/updating connector '$CONNECTOR_NAME'..."

curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
  --data @"$CONFIG_FILE"

echo "Connector '$CONNECTOR_NAME' configured successfully."