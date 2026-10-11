# Historial de cambios

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/). Las fechas son las de la mezcla en `main`.

## [0.8.0] - sin publicar

Análisis de la red con la historia de Calser, TedisNet y la meteorología, riesgo anual por CT y cabecera, curva de aprendizaje y datos públicos. Responde a dónde fallará la red, qué hacer y cuánto vale.

### Añadido

- `etl/jobs/04_ml/job_analisis_red.py` (`analisis_red` en `config_ml.json`): TIEPI y NIEPI por año, clasificación y factor con las reglas de Calser, separando lo que cuenta para el incentivo de la CNMC; límites de calidad zonal e individual del RD 1955/2000 y descuentos del art. 105; causas por tipo de equipo y por el texto libre de la incidencia; cuándo empiezan las incidencias por factor; concentración y persistencia del TIEPI por CT, grupo de red, cabecera y municipio; reposición por maniobra como máximo de lo que evitaría el telemando, con el coste del valor unitario de referencia; probabilidad de incidencia por racha y lluvia; carga de los trafos, intensidad de las cabeceras y kVA por cliente; incentivo de la Circular 8/2025 estimado, valor del minuto de TIEPI, margen y riesgo de un año malo y energía no suministrada al VoLL. El apagón del 28.04.2025 se muestra aparte. Tablas `l3_gold.analisis_red_tiepi`, `analisis_red_carga_trafo` y `analisis_red_prioridades`.
- `etl/jobs/04_ml/job_ml_riesgo_ct.py` (`riesgo_ct`): riesgo de cada CT para el año siguiente (kVA·h imprevistos, XGBoost Tweedie) con historia del CT y de su cabecera, grupo y municipio, estructura, cortes y microcortes del SCADA, datos públicos y clima de la celda; evaluación por origen móvil con la captura del top k % frente al ranking naif y al oráculo, por CT y por cabecera, y ablación por bloques. Ranking del próximo año en `prioridades_ct.csv`, `prioridades_cabecera.csv` y `l3_gold.ml_riesgo_ct_ranking`.
- `job_ml_horas_riesgo.py --curva-aprendizaje` (`horas_riesgo.curva`): curva de aprendizaje con fracciones de los días por distribuidora, proyección sobre el logaritmo de los positivos, datos necesarios para que la diferencia con la regla de la racha sea significativa y transferencia entre distribuidoras.
- Datos públicos por las cuatro capas: `job_landing_datos_publicos.py` (Padrón continuo del INE, superficie y altitud de Wikidata, relieve de Open-Meteo Elevation y exposición de OpenStreetMap por celda con Overpass, reanudable), `job_bronze_datos_publicos.py` (longitudes y superficies cortadas a la celda), `job_silver_contexto_publico.py` (`f_poblacion_municipio`, `d_municipio_publico`, `d_celda_entorno`, en `dag_silver`) y `job_gold_contexto_ct.py` (`l3_gold.contexto_ct_anio`, en `dag_gold`), con su configuración en `config_bronze_publicos.json`.
- Obras públicas y edificación por las cuatro capas, en los mismos jobs: las licitaciones de obra de la Plataforma de Contratación del Sector Público (fichero anual de todo el país filtrado a las provincias del estudio al descargarlo, `l1_bronze.placsp_licitacion_obra` y `placsp_entrada_borrada`, `l2_silver.f_licitacion_obra` con el municipio por el DIR3 del ayuntamiento o por los nombres del título, la ejecución prevista y las obras que abren zanja o calle) y los edificios del Catastro INSPIRE con su año de construcción, viviendas y superficie (`l1_bronze.catastro_edificio`, `catastro_municipio`, `l2_silver.f_edificacion_municipio`). Gold: `l3_gold.obras_municipio_anio` (obras publicadas y en ejecución, edificios y viviendas por municipio y año) y columnas nuevas de `contexto_ct_anio` conocidas antes de cada año (obras licitadas el año anterior, meses de obra previstos, viviendas nuevas y su crecimiento).
- Proyecciones de población 2024-2039 del Instituto de Estadística de Extremadura por agrupación de municipios (AME), la única proyección oficial por debajo de la provincia: `extract_ieex_proyecciones.py` pasa una vez a CSV los Excel 97 en 7z y el mapa en PDF que publica el IEEX (`etl/config/01_bronze/ieex_*.csv`), la fuente `proyecciones` de la Landing los copia y Bronze y Silver los cargan (`l2_silver.f_proyeccion_ame`, `d_ame_municipio`). El análisis de red los cruza con el crecimiento de cada municipio (`crecimiento_ame.csv`).
- Bloque `obras` en `job_ml_riesgo_ct.py` y apartado 11 del análisis de red: averías de terceros, de máquinas y obras y propias por municipio y año frente a las obras públicas en ejecución, con tasas por 100 CT-año, razón de tasas de un modelo de Poisson entre municipios y dentro de cada municipio con intervalo por remuestreo de municipios, el mismo cálculo con las obras licitadas antes del año (aviso) y los kVA que pediría el ritmo de construcción de cada municipio (`obras_terceros_*.csv`, `crecimiento_municipios.csv`).
- `docs/network-analysis.md` con los resultados sobre los datos reales.
- Tests: `tests/01_bronze/test_datos_publicos.py` (parsers y geometría sin Spark), `tests/03_gold/test_contexto_ct.py`, `tests/04_ml/test_analisis_red.py`, `tests/04_ml/test_riesgo_ct.py` y la curva de aprendizaje en `test_ml_train_smoke.py`.

### Cambiado

- Intensidad de las cabeceras: solo años con al menos 6.500 horas y puntas de más de 5 A (`min_horas_anio_cabecera`, `min_amperios_cabecera`), para que una posición que arranca o un año a medias no parezcan crecimiento; el informe separa el crecimiento de tres años o más de los saltos entre dos años, que suelen ser cambios de esquema.
- kVA por cliente: los CT con menos de 0,3 kVA por cliente (`min_kva_por_abonado_plausible`) se marcan para revisar su ficha y salen del ranking.
- Silver de los datos públicos: densidades con `try_divide`, para que una celda sin líneas dé nulo en vez de un error en modo ANSI.

## [0.7.0] - sin publicar

Entrenamiento y evaluación de los modelos sobre el dataset de Gold, el trabajo del hito M7.

### Añadido

- `etl/jobs/04_ml/job_ml_train.py` con `etl/config/04_ml/config_ml.json`: entrena sobre la muestra de train de `l3_gold.dataset_train` XGBoost, Random Forest y una regresión logística, y los compara con dos referencias (tasa base y un modelo naif que ordena los CT por sus interrupciones del último año). Cada modelo prueba una lista corta de configuraciones y se queda la de mejor PR-AUC en una muestra ponderada de valid; XGBoost para por la PR-AUC de esa misma muestra. Las probabilidades se corrigen por el muestreo de negativos. Valid y test (este solo con `--evaluar-test` y hasta `evaluacion.test_hasta`, por la censura de las etiquetas al final de la ventana) se puntúan en los executors fila a fila.
- `ml_metrics.py`: PR-AUC, ROC-AUC, lift y alertas con presupuesto de N avisos por día y distribuidora (precision, recall por filas y por episodio de interrupción, antelación media y mediana), para `y_1_3h` y `y_1_3h_local` y por distribuidora, tipo de zona y telemetría.
- Tablas `l3_gold.ml_runs`, `ml_metricas`, `ml_importancia` (SHAP de XGBoost, importancias de los otros modelos y PR-AUC de cada feature sola con alerta de leakage) y `ml_predicciones`. Los modelos se guardan en MinIO en `datalake/ml/modelos/<run_id>/`.
- Resumen de cada ejecución en tablas markdown en el log y en `resumen.md`, con las métricas y la importancia en CSV (`exportar_dir`), para leer los resultados sin Trino.
- `etl/jobs/04_ml/train.sh` para lanzarlo desde `spark-master`, `make train` y `make test-ml`.
- Meteorología horaria de Open-Meteo (ECMWF IFS, 9 km): Landing reanudable (`job_landing_meteo_openmeteo.py`) y Bronze en hora local (`l1_bronze.meteo_openmeteo_hora`, `meteo_openmeteo_celdas`), con su configuración en `etl/config/01_bronze/config_bronze_meteo.json` y tests en `tests/01_bronze`. Silver con sus reglas de calidad (`f_meteo_hora`, `d_meteo_celda`, en `dag_silver`) y Gold (`meteo_celda_hora` y `map_ct_celda`, en `dag_gold`), como tablas nuevas que no obligan a reconstruir las demás. En el entrenamiento, cada CT se cruza con su celda por el código INE o el nombre del municipio y gana ráfagas, lluvia, nieve, temperatura, humedad y presión de las últimas horas, la previsión de las 3 siguientes (reanálisis como previsión perfecta, documentado como cota superior) y el máximo de la región.
- Grupos de CT por k-means (`clusters`, tabla `l3_gold.ml_clusters_ct`) y un submodelo XGBoost por grupo (`submodelos`, modelo `xgboost_cluster`), comparado con el global en las mismas filas, con SHAP por grupo y métricas por grupo. Variable `estacion`. Siguen la propuesta de la tutoría del 08.10.2026.
- Bloque de estado de la red en el entrenamiento (`contexto_red`, tabla `l3_gold.ml_contexto_red`): fracción de CT de la distribuidora y de la región con cada señal precursora en la última hora y en las últimas 6 horas. La primera ejecución mostró que la señal está en la red y no en el CT, porque la mayoría de las interrupciones son sistémicas; el bloque empeoró los resultados en valid y queda desactivado.
- `etl/jobs/04_ml/job_ml_comparar.py`: compara ejecuciones sobre las predicciones ya guardadas con un bootstrap por días (intervalo del 95 % de la PR-AUC y diferencias emparejadas), en total y por distribuidora.
- `etl/jobs/04_ml/job_ml_horas_riesgo.py`: horas de riesgo por distribuidora (¿empieza una interrupción en algún CT de la distribuidora en las próximas 1 a 3 horas?), con el estado agregado de la red, la meteorología de la distribuidora, referencias de una sola feature (racha máxima), alertas con presupuesto de horas por día y bootstrap. Tablas `l3_gold.ml_dataset_distribuidora_hora` y `ml_predicciones_distribuidora_hora`.
- `etl/jobs/04_ml/diagnostico_senal.py`: PR-AUC de cada feature sola, medias en positivos y negativos y retraso de carga de las interrupciones de Calser.
- Notebook `notebooks/1.0-asp-resultados-modelo.ipynb` para analizar una ejecución desde Trino.
- `docs/ml-training.md` y la model card al día con el procedimiento de entrenamiento.
- `tests/04_ml`: métricas y modelos sin Spark, y el job completo sobre un `dataset_train` sintético en Spark local. Job `ml-smoke` en la CI.

### Cambiado

- La imagen de Spark instala `xgboost-cpu`, `scikit-learn` y `pyarrow` (`infra/spark/requirements.txt`). Hay que reconstruir las imágenes de Spark y de Airflow.

## [0.6.0] - 06.10.2026

Revisión de las tres capas después de la primera construcción completa de Gold: la vigencia de cada CT en el tiempo, que estaba apuntada como pendiente, y varias cosas pequeñas de Bronze y Silver.

### Añadido

- Gold: `dim_ct` dice desde y hasta cuándo existe cada CT en Calser (`vigente_desde`, `vigente_hasta_excl`, `n_periodos`), sacado de los períodos que lo listan porque `CT_FECHA_PES` y `CT_FECHA_BAJA` están vacías en todas las bases reales. `labels_ct_hora` lo lleva a la rejilla como `ct_vigente` y `dataset_train` deja fuera las horas en las que el CT no existía (`dataset.excluir_ct_no_vigente`, activado por defecto). Hasta ahora un CT dado de alta en 2024 tenía tres años de filas antes de existir, todas negativas y sin señal.
- Gold: `job_gold_dq_checks.py` comprueba la vigencia: horas de la rejilla sin CT vigente y positivos de `y_1_3h` en esas horas (cualquiera marca `REVISAR`). `job_gold_dim_ct.py` informa de los CT dados de baja, de los dados de alta dentro de la ventana y de los que no tienen ningún período con fecha. `ct_vigente` entra en la lista de columnas que nunca pueden ser feature.
- `tests/03_gold`: un CT que aparece en Calser a mitad del histórico y otro que desaparece del período abierto a mitad de la ventana, con las comprobaciones de la dimensión, de las etiquetas, del dataset y de las métricas.

### Corregido

- Bronze: en una carga incremental con `partition_column`, los límites de las particiones JDBC se calculaban sobre la tabla entera, de modo que todas las filas nuevas caían en la última partición y el resto de conexiones se quedaban sin trabajo. Ahora se calculan sobre el rango que se va a leer.
- Los tres DAG (`ingest_sqlserver_batch_bronze`, `dag_silver`, `dag_gold`): la configuración de Spark de una tabla o de un job se fusiona con la de por defecto en vez de sustituirla, así que basta con indicar lo que cambia.

### Cambiado

- Silver: las métricas de rechazos de cada job se cuentan leyendo la tabla `<entidad>_rejected` recién escrita en vez de volver a ejecutar todo el linaje desde Bronze, que era la parte del plan que el checkpoint de las filas válidas no cubría. Mismas métricas, una pasada menos sobre las tablas grandes (`f_tag_value_change`, `f_corte_*`).
- Plantilla de PR con el test de Gold y `CONTRIBUTING.md` con `make test` tal como es ahora (Silver y Gold).

## [0.5.0] - 05.10.2026

Capa Gold completa hasta el dataset de entrenamiento versionado, y las correcciones de Bronze y Silver que salieron al revisarlas antes de construirla.

### Añadido

- Capa Gold (`etl/jobs/03_gold/`, `etl/config/03_gold/config_gold.json`, DAG `dag_gold`): `dim_ct` con la potencia imputada y los CT del estudio, `map_tag_ct` y `map_aguas_arriba` para llevar cada señal del SCADA a su CT, `fact_interrupciones_mt` (variantes principal, amplia y todas, con los solapes fusionados), `fact_cortes_scada` (episodios Off -> On, microcortes y maniobras), `labels_ct_hora`, `agg_medida_hora`, `features_ct_hora` con `feature_metadata`, `dataset_train` con split temporal, purga de 6 h en cada frontera y muestreo reproducible, `dataset_versions` y `job_gold_dq_checks.py` (alineación horaria, retraso de llegada de los cambios del SCADA, censura de la etiqueta al final de la ventana, cobertura, positivos, leakage). Cada feature usa los datos en el momento en que se pudieron conocer: la llegada al SCADA y no la hora de campo, la carga del registro en Calser y la topología aguas arriba solo desde el primer corte que la reveló.
- `docs/gold-layer.md` con las decisiones de Gold y lo que queda pendiente.
- Silver: hora de llegada al servidor (`ts_actualizacion`) en `f_tag_quality_event`, `f_evento`, `f_corte_evento` y `f_corte_elemento`, y flag `llegada_tardia` (más de una hora) en `f_tag_value_change` y `f_corte_evento`. En la base real, 13 de los 200 últimos cambios llegaron con meses de retraso.
- `tests/03_gold/test_gold_smoke.py`: Silver y Gold de extremo a extremo en Spark local, etiquetas comprobadas hora a hora y prueba de leakage.
- Bronze: lectura por trozos de `Id` con conexiones JDBC en paralelo y tamaño máximo de fichero (`partition_column`, `num_partitions`, `chunk_size`, `max_records_per_file`), aplicada a `HistoricTagIntervalValuesBig`, que pasa a ser incremental y reanudable. Configuración de Spark por tabla (`conf`).
- Silver: datos eléctricos del trafo en `d_ct_scada` (`is_power_cut`, `potencia_nominal_kva`, tensiones, `observable`), coordenadas del municipio en `d_municipio` y severidad del evento en `f_evento` (`nivel_severidad`).
- `make gold`, `make test-silver`, `make test-gold` y el job de CI `gold-smoke`.

### Corregido

- Silver `f_tag_interval_value` usaba `SourceTimestamp` como instante de la muestra. `CopyTagValue2TagIntervalValue` escribe el instante de rejilla en `UpdateTimestamp` y copia la hora original del valor retenido en `SourceTimestamp`, así que las muestras de un valor estable se deduplicaban en una sola fila y caían en el mes equivocado. Ahora `ts` es el instante de rejilla, `ts_origen` la hora del valor y `antiguedad_s` / `valor_rancio` / `ts_origen_futuro` marcan los valores congelados y los relojes adelantados. Hay que volver a ejecutar el job para toda la ventana.
- Silver `d_tag` rechazaba como huérfanos los tags con `ElementId` NULL (unos 14.700 en EOSA, entre ellos el estado de conexión de 1.134 dispositivos). Ahora se conservan con `sin_elemento` y la distribuidora de su dispositivo; solo se rechaza un elemento o dispositivo que no existe.
- Silver `f_corte_evento` marca `ts_incoherente` los eventos con una hora posterior a su propio procesado (relojes de RTU; en EOSA hay On fechados el 13.09.2026).

### Cambiado

- `compose.yaml`: `spark-master` monta `etl/config` en `/app/config` para lanzar los jobs de Gold a mano. Hay que recrearlo con `docker compose up -d`.
- `make test` ejecuta los tests de Silver y de Gold.
- Los jobs de Gold no aceptan `--rebuild` junto con `--desde` / `--hasta`: borraría la tabla entera y solo reconstruiría esos meses.

## [0.4.0] - 30.09.2026

Entrega del seminario de MLOps (SE4ML) del máster. No cambia el pipeline; documenta y equipa el repositorio.

### Añadido

- `docs/dataset_card.md` y `docs/model_card.md` en formato Hugging Face, con los datos reales de Calser y TedisNet, la definición del target, los sesgos y los criterios de aceptación del modelo.
- `docs/project-structure.md`: correspondencia con Cookiecutter Data Science y cómo reproducir el pipeline.
- `CONTRIBUTING.md`: GitHub Flow tal como lo aplicamos, reglas de commits y versionado de datos.
- `.github/`: plantilla de pull request, plantillas de issues, `CODEOWNERS` y el workflow de integración continua (ruff, validación de `compose.yaml` y test de Silver en Spark local).
- `requirements.txt`, `pyproject.toml`, `Makefile`, `.pre-commit-config.yaml` y `CITATION.cff`.
- `notebooks/README.md` con la convención de nombres.
- Este `CHANGELOG.md`.

### Cambiado

- `README.md`: estructura actualizada, apartado de documentación y apartado de desarrollo y contribución.
- `.gitignore`: cachés de Python, de pytest y de ruff, y salidas CSV de las pruebas manuales.

## [0.3.1] - 30.09.2026

### Cambiado

- Imagen de MinIO: de `quay.io/minio/minio` a `pgsty/minio` con release fijo, porque Quay pasó a pedir autenticación (PR #2).

### Añadido

- `_runlogs/run_silver_test.ps1`: prueba de Silver contra los datos reales desde PowerShell.

## [0.3.0] - 25.09.2026

### Añadido

- Capa Silver completa para Calser y TedisNet (PR #1): `silver_common.py`, dimensiones `d_*`, hechos `f_*`, tablas `_rejected` con motivo, `dq_metrics` y `job_silver_dq_checks.py` (integridad, cobertura de la clave Calser - TedisNet, alineación horaria). DAG `dag_silver` con dependencias leídas de `config_silver.json`.
- `docs/silver-layer.md` con las reglas de cada tabla y su motivo.
- `tests/02_silver/test_silver_smoke.py`: prueba de extremo a extremo en Spark local, sin Docker.

## [0.2.0] - 20.09.2026

### Añadido

- Ingesta del fichero de referencia de municipios (`dag_bronze_reference_municipios`).
- `parametros_configuracion` de Calser en Bronze y dimensión `d_elemento`.
- Documentación de requisitos de la máquina, ubicación del disco de Docker y puertos ocupados.

### Cambiado

- Spark 4.2.0 y Delta Lake 4.4.0, con las dependencias alineadas.
- Límite de memoria de SQL Server a 2 GB; sobreescritura de esquema permitida en el Hive Metastore.
- Healthchecks y `start_period` ajustados al primer arranque; puerto de la UI de Spark configurable por `.env`.

## [0.1.0] - 14.09.2026

### Añadido

- Stack de Docker Compose: SQL Server 2022 con restauración automática de backups y CDC, Kafka y Kafka Connect con Debezium, Spark master, worker e history server, Hive Metastore sobre PostgreSQL, MinIO, Trino, Airflow y Kafka UI.
- Jobs de Landing y Bronze en streaming (CDC) y Bronze en batch (JDBC), con el DAG `ingest_sqlserver_batch_bronze` y la configuración de tablas en JSON.
- README, `.env.example`, `.gitignore` y convención de nombres por capa.
