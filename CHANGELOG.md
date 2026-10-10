# Historial de cambios

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/). Las fechas son las de la mezcla en `main`.

## [0.7.0] - sin publicar

Entrenamiento y evaluación de los modelos sobre el dataset de Gold, el trabajo del hito M7.

### Añadido

- `etl/jobs/04_ml/job_ml_train.py` con `etl/config/04_ml/config_ml.json`: entrena sobre la muestra de train de `l3_gold.dataset_train` XGBoost, Random Forest y una regresión logística, y los compara con dos referencias (tasa base y un modelo naif que ordena los CT por sus interrupciones del último año). Cada modelo prueba una lista corta de configuraciones y se queda la de mejor PR-AUC en una muestra ponderada de valid; XGBoost para por la PR-AUC de esa misma muestra. Las probabilidades se corrigen por el muestreo de negativos. Valid y test (este solo con `--evaluar-test` y hasta `evaluacion.test_hasta`, por la censura de las etiquetas al final de la ventana) se puntúan en los executors fila a fila.
- `ml_metrics.py`: PR-AUC, ROC-AUC, lift y alertas con presupuesto de N avisos por día y distribuidora (precision, recall por filas y por episodio de interrupción, antelación media y mediana), para `y_1_3h` y `y_1_3h_local` y por distribuidora, tipo de zona y telemetría.
- Tablas `l3_gold.ml_runs`, `ml_metricas`, `ml_importancia` (SHAP de XGBoost, importancias de los otros modelos y PR-AUC de cada feature sola con alerta de leakage) y `ml_predicciones`. Los modelos se guardan en MinIO en `datalake/ml/modelos/<run_id>/`.
- Resumen de cada ejecución en tablas markdown en el log y en `resumen.md`, con las métricas y la importancia en CSV (`exportar_dir`), para leer los resultados sin Trino.
- `etl/jobs/04_ml/train.sh` para lanzarlo desde `spark-master`, `make train` y `make test-ml`.
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
