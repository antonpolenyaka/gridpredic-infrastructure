#!/bin/bash
# Training of the outage model from the Spark master container. The driver
# runs here (client mode) with enough memory for the training sample; the
# executors only score valid and test.
#
#   docker compose exec spark-master bash /app/jobs/04_ml/train.sh
#   docker compose exec spark-master bash /app/jobs/04_ml/train.sh --evaluar-test
#   docker compose exec spark-master bash /app/jobs/04_ml/train.sh --sin-busqueda --modelos tasa_base,naif_historico,xgboost
#
# DRIVER_MEMORY only sizes the JVM of the driver (collecting the sample);
# the matrices and the models live in its Python process, outside that heap.

set -euo pipefail

DRIVER_MEMORY="${DRIVER_MEMORY:-6g}"
JOBS=/app/jobs/04_ml

exec /opt/spark/bin/spark-submit \
    --driver-memory "$DRIVER_MEMORY" \
    --py-files "$JOBS/ml_models.py,$JOBS/ml_metrics.py" \
    "$JOBS/job_ml_train.py" \
    --config /app/config/04_ml/config_ml.json \
    "$@"
