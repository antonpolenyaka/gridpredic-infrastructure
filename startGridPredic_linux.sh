#!/usr/bin/env bash
# Solo Linux (Docker Engine o Docker Desktop con el plugin compose). Arranca todo lo que necesita GridPredic,
# en orden y esperando a que cada parte este sana:
#   1. Espera a que Docker responda (no lo arranca: en Linux suele ser un servicio del sistema).
#   2. PostgreSQL y MinIO; despues Hive, Trino y Spark master; despues el worker, el History Server, Airflow y el
#      cuadro de mando.
#   3. SQL Server al final y sin esperarlo: tras un reinicio tarda unos minutos en recuperar sus bases y el dia a
#      dia no lo necesita.
# Kafka, Kafka Connect y Kafka UI (perfil cdc) solo arrancan con --cdc. La cola y el vigilante de Airflow son
# scripts de PowerShell del portatil con Windows y aqui no se arrancan.
#
#   ./startGridPredic_linux.sh
#   ./startGridPredic_linux.sh --cdc
#
# Deja un registro en _runlogs/start_<fecha>.log.

set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="$REPO/compose.yaml"
LOGDIR="$REPO/_runlogs"
mkdir -p "$LOGDIR"
OUT="$LOGDIR/start_$(date +%Y%m%d_%H%M).log"
CDC=0
MINUTOS_MAX=30
FALLOS=0

for arg in "$@"; do
    case "$arg" in
        --cdc) CDC=1 ;;
        --minutos-max=*) MINUTOS_MAX="${arg#*=}" ;;
        -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
        *) echo "Opcion desconocida: $arg" >&2; exit 2 ;;
    esac
done

say() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$OUT"; }

fase() {
    local titulo="$1" segundos="$2"; shift 2
    say "--- $titulo ($*)"
    local t0=$SECONDS rc
    if [ "$segundos" -eq 0 ]; then
        docker compose -f "$COMPOSE" up -d "$@" 2>&1 | sed 's/^/    /' | tee -a "$OUT"
        rc=${PIPESTATUS[0]}
    else
        docker compose -f "$COMPOSE" up -d --wait --wait-timeout "$segundos" "$@" 2>&1 | sed 's/^/    /' | tee -a "$OUT"
        rc=${PIPESTATUS[0]}
    fi
    local min=$(( (SECONDS - t0) / 60 ))
    if [ "$rc" -eq 0 ]; then say "    listo en $min min"; else say "    AVISO: no todo esta sano tras $min min (ver arriba)"; FALLOS=$((FALLOS + 1)); fi
}

say "=== Arranque de GridPredic === Log: $OUT"

if ! docker info >/dev/null 2>&1; then
    say "Espero a que Docker responda (hasta $MINUTOS_MAX min). Si no esta arrancado: sudo systemctl start docker"
    limite=$(( SECONDS + MINUTOS_MAX * 60 ))
    until docker info >/dev/null 2>&1; do
        if [ $SECONDS -ge $limite ]; then say "ERROR: Docker no responde."; exit 1; fi
        sleep 10
    done
fi
say "Docker responde"

fase "Almacenamiento" 600 postgres minio
fase "Catalogo, consultas y Spark" 1500 hive trino spark-master
fase "Proceso y operacion" 900 spark-worker spark-history airflow dashboard
fase "SQL Server (recupera sus bases en segundo plano)" 0 sqlserver

if [ "$CDC" -eq 1 ]; then
    say "--- CDC: espero a SQL Server y arranco Kafka, Kafka Connect y Kafka UI"
    docker compose -f "$COMPOSE" --profile cdc up -d --wait --wait-timeout $(( MINUTOS_MAX * 60 )) kafka kafka-connect kafka-ui 2>&1 | sed 's/^/    /' | tee -a "$OUT"
    [ "${PIPESTATUS[0]}" -eq 0 ] || { say "    AVISO: el CDC no ha quedado sano"; FALLOS=$((FALLOS + 1)); }
fi

say "--- Estado"
docker compose -f "$COMPOSE" --profile cdc ps -a --format "table {{.Service}}\t{{.Status}}" 2>&1 | sed 's/^/    /' | tee -a "$OUT"
SPARK_UI=$(grep -E '^SPARK_MASTER_UI_PORT=' "$REPO/.env" 2>/dev/null | cut -d= -f2)
say "Cuadro de mando: http://localhost:8086   Airflow: http://localhost:8084   Spark: http://localhost:${SPARK_UI:-8080}   Trino: http://localhost:8085   MinIO: http://localhost:9001"
if [ "$FALLOS" -eq 0 ]; then say "GridPredic arrancado."; exit 0; fi
say "GridPredic arrancado con $FALLOS avisos: mira las lineas AVISO de arriba."
exit 1
