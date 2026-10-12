# Historial de cambios

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/). Las fechas son las de la mezcla en `main`.

## [0.9.0] - sin publicar

Causas de todas las incidencias de Calser de las tres distribuidoras, qué datos podrían anticipar cada una y fuentes públicas nuevas para las que faltaban.

### Añadido

- Tablas del módulo de Extremadura de Calser en Bronze (`incidencias_ex`, `incidencias_metodos_recogida_ex`, `interrupciones_ex` de las tres bases): el código de causa que se declara a la Junta, la instalación donde empezó cada incidencia, la cabecera que la despejó, cómo se detectó y los motivos y trabajos de los cortes programados.
- `job_silver_f_incidencia_ex.py` (`l2_silver.f_incidencia_ex`) y `job_silver_f_incidencia_causa.py` (`l2_silver.f_incidencia_causa`) en `dag_silver`, con las reglas en `causas_incidencia.py`: causa, subcausa, componente y origen de cada incidencia leídos de los textos, el código oficial y el factor, con la regla que decidió para poder revisarla.
- `etl/jobs/04_ml/job_causas_incidencias.py` (`causas_incidencias` en `config_ml.json`): qué guarda Calser de cada incidencia, todas las causas y subcausas con su peso en el TIEPI, origen y componentes, código oficial frente al texto, cuándo pasa cada causa, lugares que se repiten, meteorología por causa (racha, lluvia, helada, calor, primera lluvia tras sequía y CAPE) y datos públicos frente a cada causa. Lista completa clasificada en `incidencias_causa.csv` y tabla `l3_gold.analisis_causas`.
- Meteorología: la historia de Open-Meteo empieza en 2017 (antes en 12.2020) y la Landing vuelve a pedir un año cuyo fichero no lo cubre entero (`_ventanas.json`). Fuente nueva `openmeteo_convectivo`: CAPE e índice de levantamiento de los modelos GFS archivados (Historical Forecast API, desde el 23.03.2021), en `l1_bronze.meteo_openmeteo_convectivo_hora`, en `f_meteo_hora` (`cape_jkg`, `indice_levantamiento`) y en Gold (`met_cape_1h`, `met_cape_max_6h`, `met_cape_max_24h`, `met_li_min_6h`).
- Datos públicos `aves` (GBIF, registros de cigüeña blanca y de todas las aves por celda, año y mes, sin clave) e `incendios` (EFFIS, áreas quemadas de 30 ha o más por su WFS abierto): Landing, Bronze (`gbif_conteo_celda`, `effis_area_quemada`), Silver (`f_aves_celda_anio`, `f_aves_celda_mes`, `f_area_quemada`) y Gold (`celda_ciguena_5a`, `celda_aves_5a`, `celda_ciguena_parte_aves_5a`, `celda_incendios_5a`, `celda_area_quemada_5a_ha` en `contexto_ct_anio`).
- `job_ml_riesgo_ct.py`: bloque `naturaleza` (aves e incendios), `--causas` para evaluar el riesgo de una familia de causas (fauna, meteorología...) y el modelo por familias de causa (`familias_causa`, `bloques_por_causa`): un modelo por familia con su etiqueta y su historia, sumados (`por_causa:<bloque>`), y lo que espera cada familia en el ranking del año siguiente (`esperado_<familia>`).
- `etl/jobs/04_ml/job_ml_meteo_historia.py` (`meteo_historia`): horas de riesgo por distribuidora con meteorología, calendario e historia conocida de Calser desde 2017, con la etiqueta de `fact_interrupciones_mt` y la causa de cada incidencia (`y_principal` y `y_meteo`), variantes por inicio del entrenamiento, bloques y etiqueta, evaluación por años, bootstrap de las diferencias, días de riesgo y predicciones en `l3_gold.ml_predicciones_meteo_historia`.
- `etl/jobs/04_ml/job_ml_combinar_horas.py` (`combinar`): combina hora a hora las predicciones por años del modelo de TedisNet y del de la historia larga (media de probabilidades y de rangos) y las evalúa con bootstrap y por días.
- `etl/jobs/04_ml/calendario.py`: festivos de Extremadura y día laborable para los jobs de ML, los mismos de `gold_common.holidays`.
- Meteorología de Gold: lluvia de 72 h y de los 29 días anteriores a las últimas 24 h (`met_lluvia_72h`, `met_lluvia_previa_30d`) y humedad máxima de 6 h (`met_humedad_max_6h`).
- Fuente `openmeteo_extra`: dirección del viento, viento a 100 m, humedad del suelo a 0 - 7 y 28 - 100 cm, punto de rocío, déficit de presión de vapor, nubes bajas, evapotranspiración, código de tiempo y radiación del mismo reanálisis, en celdas de 0,25 grados para que la historia quepa en el plan gratuito: `l1_bronze.meteo_openmeteo_extra_hora`, columnas nuevas de `f_meteo_hora` y features de Gold (`met_dir_viento_sin`, `met_dir_viento_cos`, `met_viento_100m_1h`, `met_humedad_suelo_0_7`, `met_humedad_suelo_28_100`, `met_rocio_dif_1h`, `met_dpv_max_24h`, `met_nubes_bajas_6h`, `met_et0_7d`, `met_codigo_max_6h`, `met_tormenta_6h`).
- Datos públicos `avisos`: archivo de avisos de fenómenos adversos de AEMET OpenData (mensajes CAP desde el 18.06.2018, CC BY 4.0, con la clave gratuita en `AEMET_API_KEY`): Landing por trozos de días, Bronze (`l1_bronze.aemet_aviso`, con los tar anidados), Silver (`l2_silver.f_aviso_meteo`, una fila por aviso y celda dentro del polígono de la zona, en hora de Madrid) y Gold (`met_aviso_max_nivel`, el nivel de viento, lluvia, tormentas y nieve en vigor ya emitido y `met_aviso_prox6h_max_nivel`). Variante `avisos` y referencia `naif_aviso_aemet` en `meteo_historia`.
- Fuente `openmeteo_previo`: lo que preveía ICON-EU para cada hora uno y dos días antes y su ejecución del mismo día (Previous Runs API de Open-Meteo, desde enero de 2024), por celda de 0,1 grados: `l1_bronze.meteo_openmeteo_previo_hora` (cada previsión se agrega al cambiar la hora como la variable que prevé), columnas `*_prev1_*`, `*_prev2_*` y `*_icon_*` de `f_meteo_hora` y columnas `metp1_*` y `metp2_*` de `meteo_celda_hora`, que los modelos por horas no leen.
- Datos públicos `focos`: focos de incendio activos de NASA FIRMS (MODIS y VIIRS desde 2017, archivo de proceso estándar y casi tiempo real para los últimos meses, con la clave gratuita en `MAP_KEY`, que nunca se escribe en el log): Landing por piezas de 5 días, Bronze (`l1_bronze.firms_foco`), Silver (`l2_silver.f_foco_incendio`, con el nivel de confianza, la hora de Madrid y la celda más cercana) y Gold (`met_focos_24h` y `met_focos_frp_max_24h` por celda, contados desde que FIRMS los habría publicado). Bloque `fuego` en `meteo_historia` y el tiempo del día anterior del riesgo del día siguiente con los focos.
- `etl/jobs/04_ml/job_ml_riesgo_dia.py` (`meteo_dia`): riesgo del día siguiente por distribuidora con lo que se sabe la tarde anterior (calendario, historia de Calser, tiempo del día anterior y avisos de AEMET enviados antes de `aviso_corte_h`), con el tiempo del propio día como previsión perfecta y con la previsión real de ICON-EU en los años que cubre (sin llevar y llevada por cuantiles a la escala del reanálisis), y la combinación `rango` de la logística y XGBoost; PR-AUC de los días, días con avería en el 5, 10 y 20 % de días de más riesgo de cada año, por distribuidora y con bootstrap por fechas.
- `job_ml_meteo_historia.py`: combinación `rango` de cada variante, la media de los percentiles en el año de la regresión logística y de XGBoost.
- Datos públicos `fwi`: índice de peligro de incendio de CEMS del EWDS de Copernicus (Fire Weather Index y sus partes FFMC, DMC, DC, ISI y BUI, reanálisis diario en una rejilla de 0,25 grados, CC BY 4.0, con el token gratuito en `COPERNICUS_EWDS_KEY`, que va en una cabecera y nunca se escribe en el log): Landing por años, del conjunto consolidado mientras cubre el mes y del intermedio después, con varias peticiones a la vez en la cola del EWDS; Bronze (`l1_bronze.cems_fwi_dia`, NetCDF4 leído con h5py, que pasa a `infra/spark/requirements.txt`), Silver (`l2_silver.f_fwi_celda_dia`, el punto más cercano a cada celda) y Gold (`met_fwi_ayer`, `met_ffmc_ayer`, `met_dmc_ayer`, `met_dc_ayer`, `met_isi_ayer`, `met_bui_ayer`, el valor del día anterior en cada hora). Variante `larga_fuego` en `meteo_historia` y `fwi_max` en el tiempo del día y de la víspera del riesgo del día siguiente.
- `job_ml_riesgo_dia.py`: bloque `extremos` (variante `perfecta_extremos`), la racha del día frente al percentil 98 de las rachas de cada distribuidora antes de `climatologia_hasta`, como cociente y como índice de Klawa y Ulbrich, y los días seguidos con la hora más calurosa a `calor_umbral_c` o más; y la etiqueta `y_dia_varias`, los días que empiezan `incidencias_dia_varias` incidencias o más, medida con los mismos modelos en el resumen. `variante_prevision` admite una lista: la previsión real de ICON-EU se mide también con los avisos de AEMET de la víspera (variante `comun_avisos`).
- `docs/causas-incidencias.md`.
- Tests: `tests/04_ml/test_riesgo_dia.py`, la fuente `openmeteo_previo` en `test_meteo_openmeteo.py`, las previsiones en Silver y Gold en `test_meteo_cape.py`, FIRMS en `test_datos_publicos.py` y `tests/03_gold/test_focos_firms.py`; `tests/02_silver/test_causas_incidencia.py`, las tablas nuevas en `test_silver_smoke.py`, `tests/03_gold/test_meteo_cape.py` (CAPE, lluvia de los días anteriores y variables extra), aves e incendios en `test_datos_publicos.py` y `test_contexto_ct.py`, el FWI de CEMS en `test_datos_publicos.py` y `tests/03_gold/test_fwi_cems.py`, el bloque `extremos` en `test_riesgo_dia.py`, la fuente convectiva en `test_meteo_openmeteo.py`, `tests/04_ml/test_causas_incidencias.py`, `tests/04_ml/test_meteo_historia.py`, `tests/04_ml/test_combinar_horas.py`, el modelo por familias en `test_riesgo_ct.py` y los avisos de AEMET en `test_datos_publicos.py` y `tests/03_gold/test_avisos_aemet.py`.

### Cambiado

- Análisis de causas: el día laborable sale del calendario y no del tipo de día de Calser, que marca como laborables muchas incidencias de fin de semana.
- Gold de meteorología: sin umbral de nulos para las features que solo existen en parte de la historia (CAPE desde 2021) o mientras se carga la rejilla extra.
- Evaluación por años de las horas de riesgo (`rolling_evaluation`): purga de 3 horas antes de cada año y antes del corte de la validación interna (`purga_horas`), porque la etiqueta mira hasta 3 horas adelante.
- Silver de meteorología: una celda solo toma los valores de la celda gruesa más cercana si está a un paso de rejilla como mucho; con la descarga a medias queda nula en vez de copiar una celda lejana.
- `meteo_historia`: búsqueda de la regresión logística con más regularización (C de 0,001 a 0,03).
- Avisos de AEMET: peticiones de 2 días como mucho y 4 s entre peticiones, porque el archivo responde con un error a periodos más largos y limita las peticiones por minuto.
- Avisos de AEMET en Bronze: fuera los mensajes verdes (sin aviso), que son casi todo el archivo porque cada zona y fenómeno se envía en verde varias veces al día, y una fila por mensaje y zona aunque el mensaje se repita en varios paquetes de la misma pieza. Con el archivo completo, Bronze pasaba de unos pocos miles de filas a unos 2,7 millones y se quedaba sin memoria.
- FIRMS: 0,3 s entre peticiones en lugar de 1 s.
- `distributor_weather` lee solo las columnas de Gold con los prefijos que se le piden (`met_` y `metprev_` por defecto).

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
