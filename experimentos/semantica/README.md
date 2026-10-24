# Semántica: el grafo RDF de GridPredic

Los guiones de las cifras semánticas de la memoria (4.6): `grafo_rdf.py` construye el grafo RDF de la exportación de Gold con la ontología mínima de GridPredic (SOSA, OWL-Time, GeoSPARQL, SKOS y PROV-O), `sameas.py` compara la integración de Calser y TedisNet con owl:sameAs frente al mapa del repositorio, `validar_rq.py` analiza las tres consultas `.rq`, `costo_inferencia.py` mide lo que añade materializar RDFS y OWL 2 RL y `grupos_causa_owl.py` lee los grupos de causas del código y los razona como un T-Box.

Necesitan rdflib 7.6.0 y owlrl 7.6.2, que son dependencias de este experimento y no de `etl/` (`pip install rdflib==7.6.0 owlrl==7.6.2`):

    set EXP=D:\GitHub\gridpredic-infrastructure\_runlogs\ml_resultados\zonas_20261013_1041
    python experimentos\semantica\grafo_rdf.py %EXP% > experimentos\semantica\salida\grafo_rdf_salida.txt
    python experimentos\semantica\sameas.py %EXP% > experimentos\semantica\salida\sameas_salida.txt
    python experimentos\semantica\validar_rq.py > experimentos\semantica\salida\validar_rq_salida.txt
    python experimentos\semantica\costo_inferencia.py %EXP% dashboard\data\riesgo_20260128.json experimentos\semantica\salida\costo_inferencia_salida.txt
    python experimentos\semantica\grupos_causa_owl.py . %EXP% experimentos\semantica\salida\grupos_causa_owl_salida.txt

El riesgo tiene que ser el de la operación (`dashboard/data/riesgo_20260128.json`, el archivo de la repetición del 28.01.2026, con los códigos INE reales, como la exportación de Gold): el de `web/demo/data` lleva los códigos 99NNN de la demo pública y no casa con ningún municipio de Gold, y `grafo_rdf.py` se para con un error si no hay ninguno en común.

Las salidas de texto y la ontología (`ontologia_gp.ttl`) van en `salida/`; los volcados del grafo (`gridpredic_grafo.ttl` y `.nt`, unos 35 MB) se regeneran y no se versionan ni se publican: llevan el código INE de cada municipio y, con él, se podría saber qué distribuidora es cada una.
