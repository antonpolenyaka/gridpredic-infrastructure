#!/bin/sh
set -eu

/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --list \
    >/dev/null 2>&1