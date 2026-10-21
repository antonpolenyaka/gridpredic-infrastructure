#!/usr/bin/env bash
# Solo Linux (Docker Engine o Docker Desktop). Arranca Neo4j con el grafo de la red para la defensa (D30), fuera de
# compose.yaml: un contenedor aparte (gridpredic-neo4j) que no toca la plataforma ni la CI.
#   1. Comprueba que Docker responde y que los puertos 7474 y 7687 estan libres la primera vez.
#   2. Crea el contenedor la primera vez (Neo4j 5.26 Community, sin usuario ni clave, solo en 127.0.0.1, 1 GB de
#      heap y 512 MB de cache) o arranca el que ya existe, y espera a que responda.
#   3. Si la base esta vacia (o con --recargar), carga los CSV de _runlogs/neo4j/import con
#      experimentos/topologia/neo4j/carga.cypher. Los CSV los deja preparar_csv.py, solo con seudonimos.
#
#   ./startNeo4j_linux.sh
#   ./startNeo4j_linux.sh --recargar
#
# Despues: http://localhost:7474 en el navegador, "No authentication", y las consultas de
# experimentos/topologia/neo4j/consultas.cypher. Para pararlo: ./stopNeo4j_linux.sh.
# Deja un registro en _runlogs/neo4j_start_<fecha>.log.

set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGDIR="$REPO/_runlogs"
IMPORT="$LOGDIR/neo4j/import"
CARGA="$REPO/experimentos/topologia/neo4j/carga.cypher"
NOMBRE="gridpredic-neo4j"
IMAGEN="neo4j:5.26.31-community"
VOLUMEN="gridpredic_neo4j_data"
MINUTOS_MAX=5
RECARGAR=0

for arg in "$@"; do
    case "$arg" in
        --recargar) RECARGAR=1 ;;
        -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
        *) echo "Opcion desconocida: $arg" >&2; exit 2 ;;
    esac
done

mkdir -p "$LOGDIR"
OUT="$LOGDIR/neo4j_start_$(date +%Y%m%d_%H%M).log"
say() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$OUT"; }
cypher() { docker exec "$NOMBRE" cypher-shell --format plain "$1" 2>&1; }

say "=== Arranque de Neo4j === Log: $OUT"
if ! docker info >/dev/null 2>&1; then say "ERROR: Docker no responde. Arranca antes Docker."; exit 1; fi

if [ -z "$(docker ps -a --filter "name=^/${NOMBRE}$" --format '{{.Names}}')" ]; then
    if [ ! -f "$IMPORT/cts.csv" ]; then say "ERROR: faltan los CSV en $IMPORT (preparar_csv.py)."; exit 1; fi
    for p in 7474 7687; do
        if (ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null) | grep -q ":$p "; then
            say "ERROR: el puerto $p ya esta ocupado. Libera el puerto y vuelve a lanzar."; exit 1
        fi
    done
    say "Creo el contenedor $NOMBRE ($IMAGEN)"
    if ! docker run -d --name "$NOMBRE" \
        -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 \
        -e NEO4J_AUTH=none \
        -e NEO4J_server_directories_import=/import \
        -e NEO4J_server_memory_heap_initial__size=512m \
        -e NEO4J_server_memory_heap_max__size=1G \
        -e NEO4J_server_memory_pagecache_size=512m \
        -v "$VOLUMEN:/data" \
        -v "$IMPORT:/import:ro" \
        "$IMAGEN" 2>&1 | tee -a "$OUT"; then
        say "ERROR: no se ha podido crear el contenedor."; exit 1
    fi
else
    say "Arranco el contenedor $NOMBRE que ya existe"
    docker start "$NOMBRE" 2>&1 | tee -a "$OUT"
fi

say "Espero a que Neo4j responda (hasta $MINUTOS_MAX min)"
limite=$(( $(date +%s) + MINUTOS_MAX * 60 ))
until cypher "RETURN 1;" >/dev/null; do
    if [ "$(date +%s)" -ge "$limite" ]; then say "ERROR: Neo4j no responde. Mira: docker logs $NOMBRE"; exit 1; fi
    sleep 5
done
say "    Neo4j responde"

hay="$(cypher 'MATCH (c:CT) RETURN count(c) AS n;' | tail -1 | tr -dc '0-9')"
hay="${hay:-0}"
if [ "$RECARGAR" -eq 1 ] && [ "$hay" -gt 0 ]; then
    say "Borro el grafo para recargarlo"
    cypher "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;" | tee -a "$OUT"
    hay=0
fi
if [ "$hay" -eq 0 ]; then
    say "Cargo el grafo desde $IMPORT"
    if ! docker exec -i "$NOMBRE" cypher-shell --format plain < "$CARGA" 2>&1 | tee -a "$OUT"; then
        say "ERROR: la carga ha fallado (ver arriba)."; exit 1
    fi
else
    say "El grafo ya esta cargado ($hay CT). Para cargarlo de nuevo: --recargar"
fi

say "Listo: http://localhost:7474 (No authentication). Consultas: $REPO/experimentos/topologia/neo4j/consultas.cypher"
