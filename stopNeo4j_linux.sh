#!/usr/bin/env bash
# Solo Linux. Para el contenedor de Neo4j de la defensa (gridpredic-neo4j). El grafo se queda en el volumen
# gridpredic_neo4j_data y startNeo4j_linux.sh lo vuelve a levantar sin recargar.
# Con --borrar quita tambien el contenedor y el volumen; el siguiente arranque crea todo y recarga los CSV.
#
#   ./stopNeo4j_linux.sh
#   ./stopNeo4j_linux.sh --borrar

set -u

NOMBRE="gridpredic-neo4j"
VOLUMEN="gridpredic_neo4j_data"
BORRAR=0

for arg in "$@"; do
    case "$arg" in
        --borrar) BORRAR=1 ;;
        -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
        *) echo "Opcion desconocida: $arg" >&2; exit 2 ;;
    esac
done

if ! docker info >/dev/null 2>&1; then echo "Docker no responde: no hay nada que parar."; exit 0; fi
if [ -z "$(docker ps -a --filter "name=^/${NOMBRE}$" --format '{{.Names}}')" ]; then
    echo "No existe el contenedor $NOMBRE."; exit 0
fi

echo "Paro $NOMBRE (hasta 60 s para cerrar bien)"
docker stop -t 60 "$NOMBRE" >/dev/null
if [ "$BORRAR" -eq 1 ]; then
    echo "Borro el contenedor $NOMBRE y el volumen $VOLUMEN"
    docker rm "$NOMBRE" >/dev/null
    docker volume rm "$VOLUMEN" >/dev/null
fi
echo "Hecho."
