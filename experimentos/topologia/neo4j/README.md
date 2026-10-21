# El grafo de la red en Neo4j

Para enseñar en la defensa el grafo con el que trabajan los guiones de `experimentos/topologia` (D30). Los análisis de la memoria no dependen de esto: están hechos con networkx y pandas, sin servidor de grafos. Neo4j va en un contenedor aparte, fuera de `compose.yaml`, sin usuario ni clave y escuchando solo en 127.0.0.1.

## Modelo

| Nodo | Clave | Propiedades |
| --- | --- | --- |
| `Distribuidora` | `red` | `nombre` (Distribuidora 1, 2 y 3) |
| `CT` | `(red, codigo)` | `zona`, `kva`, `abonados`, `salidas`, `telemetria`, `vigente_desde`, `vigente_hasta` |
| `Cabecera`, `Linea`, `Posicion` | `(red, codigo)` | - |
| `Municipio` | `nombre` | - |
| `Incidencia` | `(red, codigo)` | `inicio`, `fin`, `fecha`, `causa`, `programada`, `cts` |

Relaciones: `(CT)-[:DE]->(Distribuidora)`, `(CT)-[:EN]->(Municipio)`, `(CT)-[:CUELGA_DE]->(Cabecera)`, `(CT)-[:EN_LINEA]->(Linea)`, `(CT)-[:AGUAS_ABAJO_DE {cortes, primer_corte, ultimo_corte}]->(Posicion)` e `(Incidencia)-[:CORTA {inicio, duracion_s}]->(CT)`. La incidencia es un nodo con una arista por CT, no un par de aristas por cada dos CT que cortan juntos (el e0 de la carpeta compara las dos formas). Los códigos se repiten entre redes, por eso las claves son compuestas con la red. Hay índices sobre `inicio`, `fecha` y `causa` de la incidencia y sobre `inicio` de `CORTA`.

Con la exportación del 13.10.2026: 1.288 CT del estudio, 73 municipios, 256 posiciones, 437 incidencias de Calser desde 2016 (sin el apagón ni el 28.04.2025), 23.033 relaciones aguas arriba y 19.221 cortes de CT.

## Seudónimos

`preparar_csv.py` numera los CT, las cabeceras, las líneas, las posiciones y las incidencias por orden, sin su código real, da a cada municipio el seudónimo de la demo pública (`_runlogs/demo_seudonimos.json`, que no se versiona) y deja fuera las coordenadas, los nombres de los elementos y los textos libres. Al acabar busca en los CSV los códigos y nombres reales y para con un error si encuentra alguno. Los CSV van a `_runlogs/neo4j/import`, fuera de git.

## Uso

    python D:\GitHub\gridpredic-infrastructure\experimentos\topologia\neo4j\preparar_csv.py D:\GitHub\gridpredic-infrastructure\_runlogs\ml_resultados\zonas_20261013_1041 D:\GitHub\gridpredic-infrastructure\_runlogs\demo_seudonimos.json D:\GitHub\gridpredic-infrastructure\_runlogs\neo4j\import
    pwsh -File D:\GitHub\gridpredic-infrastructure\startNeo4j_windows.ps1
    pwsh -File D:\GitHub\gridpredic-infrastructure\stopNeo4j_windows.ps1

En Linux, `./startNeo4j_linux.sh` y `./stopNeo4j_linux.sh`. El arranque crea el contenedor la primera vez (Neo4j 5.26 Community, 1 GB de heap y 512 MB de caché) y carga `carga.cypher` si la base está vacía; `-Recargar` (`--recargar`) la vacía y carga de nuevo. La parada deja el grafo en el volumen `gridpredic_neo4j_data`; `-Borrar` (`--borrar`) quita también el contenedor y el volumen.

Después, `http://localhost:7474` con "No authentication" y las consultas de `consultas.cypher`: los CT que más cortan juntos, las incidencias de un temporal por el índice de fechas, el vecindario de un CT, el camino más corto entre dos CT por las posiciones aguas arriba y el plan de una consulta por fecha.
