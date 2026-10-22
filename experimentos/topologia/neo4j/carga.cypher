// Carga del grafo de la red en Neo4j 5 Community (D30). Lee los CSV de preparar_csv.py desde la carpeta de
// importación del contenedor (file:///). La lanzan startNeo4j_windows.ps1 y startNeo4j_linux.sh cuando la base
// está vacía; a mano: cypher-shell -f carga.cypher.
//
// Modelo (como el e0 de experimentos/topologia):
//   (:CT)-[:DE]->(:Distribuidora)            (:CT)-[:EN]->(:Municipio)
//   (:CT)-[:CUELGA_DE]->(:Cabecera)          (:CT)-[:EN_LINEA]->(:Linea)
//   (:CT)-[:AGUAS_ABAJO_DE {cortes, primer_corte, ultimo_corte}]->(:Posicion)
//   (:Incidencia)-[:CORTA {inicio, duracion_s}]->(:CT)
// La incidencia va como nodo con una arista por CT (reificada), no como aristas CT - CT por cada par.
// Los códigos de CT, cabecera, línea y posición se repiten entre redes: la clave es (red, codigo).

// --- Restricciones (claves compuestas) e índices -------------------------------------------------------
CREATE CONSTRAINT distribuidora_clave IF NOT EXISTS FOR (d:Distribuidora) REQUIRE d.red IS UNIQUE;
CREATE CONSTRAINT municipio_clave IF NOT EXISTS FOR (m:Municipio) REQUIRE m.nombre IS UNIQUE;
CREATE CONSTRAINT ct_clave IF NOT EXISTS FOR (c:CT) REQUIRE (c.red, c.codigo) IS UNIQUE;
CREATE CONSTRAINT cabecera_clave IF NOT EXISTS FOR (c:Cabecera) REQUIRE (c.red, c.codigo) IS UNIQUE;
CREATE CONSTRAINT linea_clave IF NOT EXISTS FOR (l:Linea) REQUIRE (l.red, l.codigo) IS UNIQUE;
CREATE CONSTRAINT posicion_clave IF NOT EXISTS FOR (p:Posicion) REQUIRE (p.red, p.codigo) IS UNIQUE;
CREATE CONSTRAINT incidencia_clave IF NOT EXISTS FOR (i:Incidencia) REQUIRE (i.red, i.codigo) IS UNIQUE;
CREATE INDEX incidencia_inicio IF NOT EXISTS FOR (i:Incidencia) ON (i.inicio);
CREATE INDEX incidencia_fecha IF NOT EXISTS FOR (i:Incidencia) ON (i.fecha);
CREATE INDEX incidencia_causa IF NOT EXISTS FOR (i:Incidencia) ON (i.causa);
CREATE INDEX corta_inicio IF NOT EXISTS FOR ()-[r:CORTA]-() ON (r.inicio);
CALL db.awaitIndexes(300);

// --- Nodos ------------------------------------------------------------------------------------------------
LOAD CSV WITH HEADERS FROM 'file:///distribuidoras.csv' AS f
MERGE (d:Distribuidora {red: toInteger(f.red)}) SET d.nombre = f.nombre;

LOAD CSV WITH HEADERS FROM 'file:///municipios.csv' AS f
MERGE (:Municipio {nombre: f.municipio});

LOAD CSV WITH HEADERS FROM 'file:///posiciones.csv' AS f
MERGE (:Posicion {red: toInteger(f.red), codigo: f.posicion});

LOAD CSV WITH HEADERS FROM 'file:///cts.csv' AS f
MERGE (c:CT {red: toInteger(f.red), codigo: f.codigo})
SET c.zona = f.zona, c.kva = toFloat(f.kva), c.abonados = toInteger(f.abonados), c.salidas = toInteger(f.salidas),
    c.telemetria = f.telemetria = 'True', c.vigente_desde = date(f.vigente_desde),
    c.vigente_hasta = CASE WHEN f.vigente_hasta IS NULL THEN null ELSE date(f.vigente_hasta) END
WITH c, f
MATCH (d:Distribuidora {red: c.red}) MERGE (c)-[:DE]->(d)
WITH c, f
MATCH (m:Municipio {nombre: f.municipio}) MERGE (c)-[:EN]->(m)
WITH c, f WHERE f.cabecera IS NOT NULL
MERGE (k:Cabecera {red: c.red, codigo: f.cabecera})
MERGE (c)-[:CUELGA_DE]->(k);

LOAD CSV WITH HEADERS FROM 'file:///cts.csv' AS f
WITH f WHERE f.linea IS NOT NULL
MATCH (c:CT {red: toInteger(f.red), codigo: f.codigo})
MERGE (l:Linea {red: c.red, codigo: f.linea})
MERGE (c)-[:EN_LINEA]->(l);

LOAD CSV WITH HEADERS FROM 'file:///incidencias.csv' AS f
MERGE (i:Incidencia {red: toInteger(f.red), codigo: f.codigo})
SET i.inicio = localdatetime(f.inicio), i.fin = localdatetime(f.fin), i.fecha = date(f.fecha),
    i.causa = f.causa, i.programada = f.programada = 'True', i.cts = toInteger(f.cts);

// --- Relaciones -------------------------------------------------------------------------------------------
LOAD CSV WITH HEADERS FROM 'file:///ct_posicion.csv' AS f
MATCH (c:CT {red: toInteger(f.red), codigo: f.ct})
MATCH (p:Posicion {red: toInteger(f.red), codigo: f.posicion})
MERGE (c)-[r:AGUAS_ABAJO_DE]->(p)
SET r.cortes = toInteger(f.cortes), r.primer_corte = localdatetime(f.primer_corte),
    r.ultimo_corte = localdatetime(f.ultimo_corte);

LOAD CSV WITH HEADERS FROM 'file:///incidencia_ct.csv' AS f
MATCH (i:Incidencia {red: toInteger(f.red), codigo: f.incidencia})
MATCH (c:CT {red: toInteger(f.red), codigo: f.ct})
MERGE (i)-[r:CORTA]->(c)
SET r.inicio = localdatetime(f.inicio), r.duracion_s = toInteger(f.duracion_s);

// --- Resumen ----------------------------------------------------------------------------------------------
MATCH (n) RETURN labels(n)[0] AS etiqueta, count(*) AS nodos ORDER BY etiqueta;
MATCH ()-[r]->() RETURN type(r) AS relacion, count(*) AS relaciones ORDER BY relacion;
