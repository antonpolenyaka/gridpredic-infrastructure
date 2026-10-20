#!/usr/bin/env bash
# Solo Linux (Docker Engine o Docker Desktop con el plugin compose). Para GridPredic sin cortar nada a medias:
#   1. Comprueba que la cola no tiene un trabajo en marcha (si existe _runlogs/cola/historial.log) y que Spark no
#      tiene aplicaciones; si las hay, no para, salvo con --forzar.
#   2. Avisa si es la franja de la tarde (de 20:15 a 21:00), cuando corre el riesgo de manana.
#   3. Para todos los contenedores, tambien los del perfil cdc, dando a cada uno hasta 2 minutos para cerrar bien.
#
#   ./stopGridPredic_linux.sh
#   ./stopGridPredic_linux.sh --forzar
#
# Deja un registro en _runlogs/stop_<fecha>.log.

set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="$REPO/compose.yaml"
LOGDIR="$REPO/_runlogs"
mkdir -p "$LOGDIR"
OUT="$LOGDIR/stop_$(date +%Y%m%d_%H%M).log"
FORZAR=0

for arg in "$@"; do
    case "$arg" in
        --forzar) FORZAR=1 ;;
        -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
        *) echo "Opcion desconocida: $arg" >&2; exit 2 ;;
    esac
done

say() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$OUT"; }

say "=== Parada de GridPredic === Log: $OUT"
if ! docker info >/dev/null 2>&1; then say "Docker no responde: no hay nada que parar."; exit 0; fi

motivos=()
HIST="$LOGDIR/cola/historial.log"
if [ -f "$HIST" ]; then
    ultima=$(tail -n 50 "$HIST" | grep -E ' (INICIO|FIN) ' | tail -n 1)
    case "$ultima" in *" INICIO "*) motivos+=("la cola esta ejecutando ${ultima##* INICIO }") ;; esac
fi
SPARK_UI=$(grep -E '^SPARK_MASTER_UI_PORT=' "$REPO/.env" 2>/dev/null | cut -d= -f2)
apps=$(curl -s --max-time 10 "http://localhost:${SPARK_UI:-8080}/json/" | python3 -c 'import json,sys
try: print(len(json.load(sys.stdin).get("activeapps", [])))
except Exception: print(0)' 2>/dev/null)
if [ "${apps:-0}" -gt 0 ]; then motivos+=("Spark tiene $apps aplicaciones en marcha"); fi
hm=$(date +%H%M)
if [ "$hm" -ge 2015 ] && [ "$hm" -lt 2100 ]; then motivos+=("es la franja de la tarde (20:15 - 21:00): el riesgo de manana puede estar corriendo"); fi

if [ ${#motivos[@]} -gt 0 ]; then
    for m in "${motivos[@]}"; do say "  AVISO: $m"; done
    if [ "$FORZAR" -eq 0 ]; then
        say "No paro. Espera a que acabe o crea el fichero $LOGDIR/cola/parar para que la cola se detenga al terminar el trabajo actual. Para parar igualmente: --forzar."
        exit 1
    fi
    say "  --forzar: paro igualmente."
fi

say "--- Parando contenedores (hasta 2 minutos cada uno)"
docker compose -f "$COMPOSE" --profile cdc stop -t 120 2>&1 | sed 's/^/    /' | tee -a "$OUT"
docker compose -f "$COMPOSE" --profile cdc ps -a --format "table {{.Service}}\t{{.Status}}" 2>&1 | sed 's/^/    /' | tee -a "$OUT"
say "GridPredic parado. Para volver a arrancarlo: $REPO/startGridPredic_linux.sh"
exit 0
