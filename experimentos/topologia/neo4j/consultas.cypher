// Consultas de ejemplo para la defensa sobre el grafo de carga.cypher. Se pegan en Neo4j Browser
// (http://localhost:7474, "No authentication") de una en una; las que devuelven nodos y relaciones se ven como grafo.

// 1. Qué hay en el grafo
MATCH (n) RETURN labels(n)[0] AS etiqueta, count(*) AS nodos ORDER BY nodos DESC;

// 2. Una cabecera de la Distribuidora 1 con sus CT y su línea (grafo)
MATCH (k:Cabecera {red: 1})<-[:CUELGA_DE]-(c:CT)
WITH k, count(c) AS n ORDER BY n DESC LIMIT 1
MATCH (k)<-[r1:CUELGA_DE]-(c:CT)-[r2:EN_LINEA]->(l:Linea)
RETURN k, r1, c, r2, l;

// 3. Los CT que más veces cortan juntos desde 2023 (co-corte por la incidencia reificada)
MATCH (a:CT)<-[:CORTA]-(i:Incidencia)-[:CORTA]->(b:CT)
WHERE a.red = 1 AND elementId(a) < elementId(b) AND i.inicio >= localdatetime('2023-01-01T00:00:00')
  AND NOT i.programada
RETURN a.codigo AS ct_a, b.codigo AS ct_b, count(i) AS veces_juntos
ORDER BY veces_juntos DESC LIMIT 15;

// 4. Las incidencias de un temporal, por el índice de fechas (27 y 28.01.2026), con los CT que cortaron (grafo)
MATCH (i:Incidencia)-[r:CORTA]->(c:CT)
WHERE i.fecha >= date('2026-01-27') AND i.fecha <= date('2026-01-28')
RETURN i, r, c;

// 5. El vecindario de un CT: cabecera, posiciones aguas arriba y los CT que comparten esas posiciones (grafo)
MATCH (c:CT {red: 1, codigo: 'CT 0001'})-[r:AGUAS_ABAJO_DE]->(p:Posicion)
WITH c, r, p ORDER BY r.cortes DESC LIMIT 3
MATCH (p)<-[r2:AGUAS_ABAJO_DE]-(otro:CT)
RETURN c, r, p, r2, otro LIMIT 60;

// 6. Camino más corto entre dos CT a través de las posiciones que los cortan
MATCH (a:CT {red: 1, codigo: 'CT 0001'}), (b:CT {red: 1, codigo: 'CT 0200'})
MATCH camino = shortestPath((a)-[:AGUAS_ABAJO_DE*..6]-(b))
RETURN camino;

// 7. Los municipios de la Distribuidora 1 con más CT cortados por incidencias imprevistas desde 2023
MATCH (i:Incidencia)-[:CORTA]->(c:CT {red: 1})-[:EN]->(m:Municipio)
WHERE NOT i.programada AND i.inicio >= localdatetime('2023-01-01T00:00:00')
RETURN m.nombre AS municipio, count(DISTINCT i) AS incidencias, count(*) AS cortes_de_ct
ORDER BY cortes_de_ct DESC LIMIT 10;

// 8. Causas por año y red, desde las incidencias
MATCH (i:Incidencia) WHERE NOT i.programada
RETURN i.red AS red, i.inicio.year AS anio, i.causa AS causa, count(*) AS incidencias
ORDER BY red, anio, incidencias DESC;

// 9. El plan de una consulta por fecha: debe usar el índice incidencia_fecha (NodeIndexSeekByRange)
EXPLAIN MATCH (i:Incidencia) WHERE i.fecha >= date('2026-01-01') RETURN count(i);
