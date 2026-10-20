# Causas de los partes: reglas frente a TF-IDF

`comparar_reglas_tfidf.py` compara las reglas de `etl/jobs/02_silver/causas_incidencia.py` con clasificadores clásicos de texto sobre los 563 partes de CT desde 2016, con particiones que agrupan los partes repetidos y con el acuerdo con el código oficial como etiqueta independiente (5.8.1 de la memoria y [docs/causas-incidencias.md](../../docs/causas-incidencias.md) 2). Lee `incidencias_causa.csv` de una ejecución de `job_causas_incidencias.py` (`_runlogs/ml_resultados/cau_20261012T025409_4681c9` en el portátil), solo con scikit-learn y sin Spark:

    python experimentos\causas_texto\comparar_reglas_tfidf.py <incidencias_causa.csv> experimentos\causas_texto\salida [--rapido]

Deja `salida/comparativa.csv`, `comparativa.md` y `desacuerdos.csv` (red, periodo_id, id, regla, causa de la regla, causa del clasificador y código oficial, sin texto). Ni la consola ni la salida llevan textos, términos de los partes, instalaciones o nombres de bases de datos. Los errores de las reglas que salieron de leer los desacuerdos están en `tests/02_silver/test_causas_incidencia.py`.
