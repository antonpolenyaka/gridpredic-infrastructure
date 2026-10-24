#!/bin/sh
# Healthy when the API answers and its health JSON says the metadatabase and the scheduler (the one that launches
# the tasks) are healthy. A JSON that does not parse is a failure. The dag-processor and the triggerer are not
# required: in standalone mode over SQLite they flap with the disk loaded and do not stop the loads
# (docs/operacion-diaria.md); _runlogs/vigilar_airflow.ps1 watches the dag-processor apart.
# HEALTH_URL lets the tests point it to a fake answer.
set -eu

body=$(curl --fail --silent --max-time 20 "${HEALTH_URL:-http://localhost:8080/api/v2/monitor/health}")

printf '%s' "$body" | python3 -c '
import json, sys
try:
    health = json.load(sys.stdin)
except ValueError:
    sys.exit("health JSON not valid")
bad = [name for name in ("metadatabase", "scheduler")
       if not isinstance(health.get(name), dict) or health[name].get("status") != "healthy"]
if bad:
    sys.exit("not healthy: " + ", ".join(bad))
'
