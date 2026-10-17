---
# Metadatos según la especificación de Hugging Face para dataset cards:
# https://github.com/huggingface/hub-docs/blob/main/datasetcard.md?plain=1
pretty_name: GridPredic - Interrupciones y telemetría SCADA de tres distribuidoras (Calser + TedisNet)
language:
  - es
license: other
license_name: propietaria
license_details: Datos de explotación cedidos por SITEL Sistemas Electrónicos y por las distribuidoras para el TFM. No se pueden redistribuir. Los backups no están en el repositorio.
tags:
  - energy
  - electricity
  - power-grid
  - scada
  - time-series
  - tabular
  - delta-lake
task_categories:
  - tabular-classification
  - time-series-forecasting
size_categories:
  - 1B<n<10B
annotations_creators:
  - machine-generated
  - expert-generated
multilinguality: monolingual
source_datasets:
  - original
---

# Dataset Card for GridPredic (Calser + TedisNet)

Conjunto de datos del TFM GridPredic: el histórico de interrupciones de suministro de tres distribuidoras eléctricas (aplicación Calser, que es donde se calculan los índices de calidad TIEPI y NIEPI) unido a la telemetría del SCADA TedisNet de esas mismas redes (medidas eléctricas, estados de interruptores, eventos y cortes deducidos por topología). Calser aporta las etiquetas y TedisNet las variables explicativas. El dataset vive en el lakehouse local del proyecto (MinIO + Delta Lake) en tres zonas, Bronze, Silver y Gold, y este repositorio contiene el código que lo construye desde los backups de SQL Server.

## Dataset Details

### Dataset Description

Las fuentes son cuatro bases de datos de SQL Server, restauradas a partir de backups fechados el 14.08.2026:

| Base de datos | Qué contiene | Tamaño del backup |
| --- | --- | --- |
| Calser de la Distribuidora 1 | Interrupciones, incidencias, CT, salidas, municipios y períodos. 805 CT | 1,3 GB |
| Calser de la Distribuidora 2 | Lo mismo para 598 CT | 1,0 GB |
| Calser de la Distribuidora 3 | Lo mismo para 11 CT | 23 MB |
| TedisNet (las tres redes) | SCADA de las tres distribuidoras: 78.301 tags, 12.307 nodos y 11.449 ramas de topología, 1.386 transformadores con el cálculo de cortes activo (`IsPowerCut = 1`), series de medidas y eventos desde 2021 | 63,5 GB |

Los CT de esta tabla son los distintos de todos los períodos de cada base, de 2010 a 2026, contando los que se dieron de baja; los de la red actual (unos 760, 570 y 10) están en [cifras.md](cifras.md).

Cifras de volumen que condicionan el diseño del pipeline:

- `HistoricTagIntervalValuesBig` tiene 2.152 millones de filas (220 GB): es la serie de medidas muestreada cada 5 o 10 minutos. Solo unos 4.000 de los 78.000 tags tienen serie regular.
- `HistoricTagValueChanges` tiene 2,79 millones de cambios de estado y de valor, con un 47 % telemedido y un 39 % calculado.
- Cortes deducidos por TedisNet: unos 21.800 eventos (Off y On) sobre 1.385 transformadores distintos, desde el 01.01.2021.
- Interrupciones de Calser que cumplen la definición del target, en todos los años: 27.680 en la Distribuidora 1, 19.887 en la Distribuidora 2 y 770 en la Distribuidora 3. Dentro de la ventana en la que hay telemetría, desde 2021, quedan unas 14.000.

- **Curated by:** Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka (TFM del Máster en Data Science and Engineering, UPC School, 2025-2026)
- **Funded by [optional]:** proyecto académico sin financiación externa
- **Shared by [optional]:** SITEL Sistemas Electrónicos SA (propietaria del SCADA TedisNet y de la aplicación Calser) y las tres distribuidoras
- **Language(s) (NLP):** es (catálogos y descripciones en castellano; nombres de tablas y columnas de TedisNet en inglés)
- **License:** propietaria. Uso limitado al TFM y al prototipo GridPredic. Los backups, los volúmenes de Docker y cualquier extracto quedan fuera del control de versiones (`.gitignore`).

### Dataset Sources [optional]

- **Repository:** https://github.com/antonpolenyaka/gridpredic-infrastructure (código de ingesta y transformación; los datos no están en el repositorio)
- **Paper [optional]:** memoria del TFM GridPredic (UPC School, en redacción; se entrega el 26.10.2026)
- **Demo [optional]:** no aplica

## Uses

### Direct Use

- Construir el dataset supervisado de GridPredic: una fila por centro de transformación (CT) y hora, con la etiqueta "empieza una interrupción imprevista en las siguientes 1 a 3 horas" y las variables derivadas de la telemetría previa. Con él se midió el aviso de 1 a 3 horas por CT, que no sirve como alerta ([model_card.md](model_card.md)); el modelo que corre cada tarde, el riesgo del día siguiente por distribuidora, usa las interrupciones y la meteorología de Gold a escala de día ([ml-riesgo-dia.md](ml-riesgo-dia.md)).
- Análisis de calidad de suministro por distribuidora, municipio y CT (frecuencia y duración de interrupciones; TIEPI y NIEPI a nivel de zona solo para validar los cálculos, no para reportar).
- Detección de anomalías en la telemetría: por eso Silver marca los valores dudosos en lugar de borrarlos.

### Out-of-Scope Use

- Cualquier uso fuera del TFM y del prototipo sin autorización de SITEL y de las distribuidoras.
- Reporte regulatorio de calidad de suministro a la CNMC: el dataset limpia y deduplica, y el cálculo oficial se hace en Calser.
- Análisis de clientes o de consumo individual: las tablas de abonados y acometidas no se ingieren, y no debe cruzarse con datos personales.
- Redes de baja tensión: TedisNet no modela la BT de estas distribuidoras, así que el dataset solo sirve para media tensión.
- Reentrenar sobre `BDTest_v640` o cualquier copia de la Distribuidora 2: es la misma base congelada y duplicaría el 43 % de los eventos.

## Dataset Structure

El dataset se organiza en las zonas del lakehouse. La ruta física es `s3a://datalake/<zona>/` en MinIO y las tablas están registradas en el Hive Metastore, así que se consultan por nombre desde Spark o desde Trino (`lakehouse.l1_bronze`, `lakehouse.l2_silver`, `lakehouse.l3_gold`).

| Zona | Esquema | Contenido | Estado |
| --- | --- | --- | --- |
| Landing | `00_landing/` | Eventos CDC de Kafka tal cual llegan (Parquet), ficheros de referencia, las respuestas de las fuentes públicas y lo que baja cada tarde la operación (`00_landing/vivo/`) | operativa |
| Bronze | `l1_bronze` | Copia fiel de SQL Server en Delta: 33 tablas `Historic*` y `Lib*` de TedisNet en batch, 29 tablas `System*` por CDC, 19 tablas de cada base Calser, más `reference_municipios` y las tablas de la meteorología y de las fuentes públicas | operativa |
| Silver | `l2_silver` | Una versión válida de cada registro, con tipos correctos, distribuidora resuelta, rechazos con motivo y anomalías marcadas con flags | operativa |
| Gold | `l3_gold` | Etiquetas, features por CT y hora, meteorología por celda y versiones congeladas del dataset de entrenamiento | operativa, construida con la ventana completa (dataset `ds_20261010T045104_a5a2531cacbb`) |

Tablas de Silver (el detalle de cada regla está en [silver-layer.md](silver-layer.md)):

| Tabla | Grano | Origen |
| --- | --- | --- |
| `d_elemento`, `d_tag`, `d_ct`, `d_ct_scada`, `d_salida`, `d_municipio`, `d_periodo`, `d_tipo_generico` | dimensiones | TedisNet `SystemElements`, `SystemTags`; Calser `cts`, `salidas`, `municipios`, `periodos`, `tipo_generico` |
| `f_interrupcion`, `f_incidencia` | una interrupción o incidencia de Calser, por distribuidora y período | Calser `interrupciones`, `incidencias` |
| `f_incidencia_ex`, `f_incidencia_causa` | una incidencia, con lo que se declara a la Junta de Extremadura y su causa | Calser `incidencias_ex`, `incidencias_metodos_recogida_ex`, `interrupciones_ex` y las tablas de la fila anterior |
| `f_tag_value_change`, `f_tag_quality_event`, `f_evento` | un cambio de valor, un evento de calidad, un evento del SCADA | TedisNet `Historic*` + `System*` |
| `f_tag_interval_value`, `f_tag_interval_value_rechazo_diario` | una muestra por tag e instante de rejilla (`UpdateTimestamp`), con la antigüedad del valor retenido, particionada por mes | `HistoricTagIntervalValuesBig` + `HistoricTagIntervalValues` |
| `f_command_execution`, `f_corte_evento`, `f_corte_elemento` | mandos y cortes deducidos por topología, con los elementos afectados | TedisNet `*CommandExecutions`, `*ElectricPowerCut*` |
| `d_meteo_celda`, `f_meteo_hora` y las tablas de las fuentes públicas (`f_aviso_meteo`, `f_poblacion_municipio`, `f_foco_incendio`...) | celda meteorológica y hora; municipio, año, aviso o foco según la fuente | Open-Meteo, AEMET, INE, Wikidata, OpenStreetMap, PLACSP, Catastro, GBIF, EFFIS, FIRMS, CEMS e IEEX |
| `dq_metrics` y `<tabla>_rejected` | métricas de calidad por ejecución y filas descartadas con su `_motivo` | todos los jobs |

Tablas de Gold (el detalle está en [gold-layer.md](gold-layer.md)):

| Tabla | Grano | Contenido |
| --- | --- | --- |
| `dim_ct`, `map_tag_ct`, `map_aguas_arriba` | CT, tag, trafo y elemento | El CT con su potencia imputada, si entra en el estudio y desde y hasta cuándo existe en Calser; el CT y el grupo de red de cada señal; las posiciones de cabecera que han cortado cada trafo y desde cuándo se sabe |
| `fact_interrupciones_mt`, `fact_cortes_scada` | evento | Interrupciones de Calser en tres variantes con los solapes fusionados; episodios Off -> On del SCADA con microcortes y maniobras |
| `labels_ct_hora` | CT y hora | `y_1_3h` y sus variantes, `en_corte`, `ct_vigente`, horas hasta el próximo evento y era |
| `agg_medida_hora`, `actividad_scada_hora` | clave y hora | Agregados horarios de las series de medida y muestras por distribuidora y hora |
| `features_ct_hora`, `feature_metadata` | CT y hora | Unas 250 features y su descripción |
| `meteo_celda_hora`, `map_ct_celda`, `contexto_ct_anio`, `obras_municipio_anio` | celda y hora; CT; CT y año; municipio y año | La meteorología de cada celda (reanálisis desde 2017, previsiones de ICON-EU desde 2024, avisos de AEMET, focos de FIRMS, FWI de CEMS), la celda de cada CT y los datos públicos de cada CT y municipio por año |
| `dataset_train`, `dataset_versions` | CT y hora | Dataset de entrenamiento sin las horas en corte, sin SCADA activo o en las que el CT no existía, con split temporal (y una purga de cinco horas por CT antes de cada frontera, las que tienen la ventana de etiqueta de 6 h al otro lado) y muestreo reproducible, y el registro de cada versión |

Columnas técnicas comunes: `distribuidora_id` (el `DistributorId` de Calser, que coincide con el `Id` del elemento raíz en TedisNet: 3 Distribuidora 1, 2 Distribuidora 2, 1366 Distribuidora 3), `_origen` (Historic o System), `run_id` de la ejecución del DAG y los flags booleanos de anomalías (`en_solape`, `duracion_cero`, `es_microcorte`, `es_estimado`, `es_copiado`, `es_error_comm`, `es_maniobra`, `potencia_cero`, `sin_telemetria`...).

Clave de cruce entre las dos fuentes: `Calser.cts.CT_ID` (texto de cinco dígitos, por ejemplo "01011") = `TedisNet.SystemElements.ShortName` del elemento de tipo 145 (TRAFO CT) de la misma distribuidora. La cobertura verificada es del 97,8 % de los CT con interrupciones desde 2021 (1.317 de 1.346) y del 99,4 % de los eventos.

Definición del target (se materializa en Gold con los mismos filtros que usa Calser para el TIEPI): interrupción con clasificación `CL_IMPRE` (imprevista), factor distinto de `FA_CLIEN` (causa cliente), duración mayor de 180 segundos y nivel de afectación CT (sin salida, acometida ni abonado). Grano (CT, hora); `y = 1` si una interrupción así empieza en las siguientes 1 a 3 horas. La tasa de positivos es de unas 3 o 4 por cada 10.000 filas CT-hora (0,03 % en train y 0,038 % en valid en el dataset `ds_20261010T045104_a5a2531cacbb`, ver [ml-training.md](ml-training.md)), así que las métricas de referencia son precision, recall y PR-AUC.

Particiones para entrenar: temporal, sin mezclar fechas. Entrenamiento hasta 2024, validación 2025 y prueba 2026 (forward chaining para la selección de hiperparámetros). Cada versión del dataset de entrenamiento se congela en `l3_gold.dataset_train` y queda registrada en `l3_gold.dataset_versions` con sus parámetros, las fechas exactas de la ventana y la versión Delta de cada tabla de entrada.

## Dataset Creation

### Curation Rationale

Las distribuidoras cobran una retribución regulada que depende, entre otras cosas, de sus índices de calidad de suministro. La idea de partida era anticipar una interrupción con 1 a 3 horas de margen para reposicionar a los operarios antes de que ocurriera. Al medirlo con los cortes reales de 2024 y 2025, la brigada tarda de media entre 39 y 66 minutos con una base fija y entre 23 y 39 con dos, y mover las bases según el riesgo gana menos de un minuto de TIEPI al año; como además el modelo por CT y hora no distinguía la hora ni el CT, el producto acabó siendo el riesgo del día siguiente ([model_card.md](model_card.md)). Para entrenar un modelo hacía falta juntar en un solo sitio dos sistemas que en las distribuidoras viven separados: el registro de interrupciones (Calser) y la telemetría (TedisNet). Ninguna de las dos fuentes se creó pensando en machine learning, y de ahí el peso que tienen la limpieza y la documentación de sesgos en esta ficha.

### Source Data

#### Data Collection and Processing

1. **Backups.** SITEL entrega los `.bak` de SQL Server (fechados) y se copian en `infra/sqlserver/backups/`. Al arrancar el stack se restauran automáticamente (`infra/sqlserver/scripts/restore_databases.sql`) y se activa CDC en las tablas `System*` de TedisNet (`enable_cdc.sql`).
2. **Bronze batch.** El DAG `ingest_sqlserver_batch_bronze` de Airflow lanza un job Spark por tabla (`etl/jobs/01_bronze/job_bronze_sqlserver_batch.py`) que lee por JDBC y escribe Delta sin transformar el contenido. Las tablas se listan en `etl/config/01_bronze/config_bronze_sqlserver.json`.
3. **Bronze streaming.** Debezium publica los cambios de las tablas `System*` en Kafka; `job_landing_sqlserver_streaming.py` los guarda tal cual en Landing y `job_bronze_sqlserver_streaming.py` los aplica sobre las tablas Delta de Bronze. Sirve para las tablas que en origen solo guardan la última semana.
4. **Meteorología y fuentes públicas.** `job_landing_meteo_openmeteo.py` y `job_landing_datos_publicos.py` guardan en Landing lo que devuelve cada API y sus jobs de Bronze lo pasan a Delta (configuración en `config_bronze_meteo.json` y `config_bronze_publicos.json`; comandos en el apartado 10 de [causas-incidencias.md](causas-incidencias.md)).
5. **Silver.** El DAG `dag_silver` ejecuta los jobs de `etl/jobs/02_silver/` en el orden de `config_silver.json`. Cada job reconstruye su tabla desde Bronze (idempotente), separa rechazos y marcados, y escribe sus métricas en `dq_metrics`. El último job comprueba integridad referencial, cobertura de la clave Calser - TedisNet y alineación horaria entre las dos fuentes.
6. **Gold.** El DAG `dag_gold` construye la dimensión de CT y los mapas de señales, los hechos de interrupciones y cortes, las etiquetas, los agregados de medidas, las features, la meteorología de cada celda, el dataset de entrenamiento con su versión y los controles finales (alineación horaria entre fuentes, cobertura, positivos, leakage). Las reglas están en `etl/config/03_gold/config_gold.json` y se prueban sin Docker con `python -m pytest tests/03_gold -q`.

Todo el proceso es reproducible con `docker compose up -d --build --wait` y los DAGs, tal como se describe en el [README](../README.md). Las reglas de Silver se prueban sin Docker con `python -m pytest tests/02_silver -q`.

#### Who are the source data producers?

- Las RTU y los equipos de campo de las tres distribuidoras, que envían medidas y estados al SCADA TedisNet (SITEL). Los cortes no se miden: TedisNet los deduce por topología a partir de los tags de posición de los interruptores.
- Los operadores de las distribuidoras, que registran o completan las interrupciones e incidencias en Calser. Desde noviembre de 2025 una parte de las interrupciones se importa automáticamente desde TedisNet.
- El INE, a través del fichero de referencia de municipios (`data/reference_data/municipios.xlsx`).
- Las fuentes públicas que acompañan al dataset en el lakehouse, cada una con su licencia de reutilización: Open-Meteo (reanálisis del ECMWF IFS de 9 km, previsión y escenarios de ICON-EU), AEMET OpenData (avisos desde 2018, predicción por municipios, estaciones), EUMETSAT (rayos del Lightning Imager de MTG), NOAA CPC (índice NAO diario, que hoy no entra en el modelo), INE (padrón), Wikidata, OpenStreetMap, Catastro, PLACSP, IEEX, GBIF, EFFIS, FIRMS, CEMS y GECOS del MITECO (los índices oficiales de calidad de suministro del sector por tipo de zona, 2003 - 2021). La lista con lo que aporta cada una y sus tablas está en [network-analysis.md](network-analysis.md) (apartado 9) y en [causas-incidencias.md](causas-incidencias.md) (apartado 7).

### Annotations [optional]

#### Annotation process

La etiqueta no la pone una persona pensando en el modelo. Sale del registro administrativo de interrupciones de Calser, que tiene tres eras: hasta el 06.02.2018 importaciones de ficheros (con microcortes), de ahí a octubre de 2025 registro manual (sin microcortes, porque el parámetro `MinimalDurationImportInterruptions` está a 180 s en las tres bases) y desde noviembre de 2025 importación desde el SCADA. Entre un 26 y un 28 % de las interrupciones de la Distribuidora 1 y la Distribuidora 2 no tienen incidencia asociada, y la fecha de alta puede ser semanas posterior al evento.

#### Who are the annotators?

Personal de operación de cada distribuidora y el importador automático de Calser.

#### Personal and Sensitive Information

De Calser no ingerimos las tablas de abonados, acometidas ni contratos, y el grano más fino es el centro de transformación, identificado por su código interno y su municipio. Como conservamos ese código para cruzar las fuentes, lo que hacemos es minimizar y seudonimizar: en una red rural con pocos clientes por CT, un corte puede llegar a señalar a personas concretas, así que lo tratamos como si pudiera ser dato personal. En TedisNet, desde el 17.10.2026 la captura de cambios deja fuera `SystemElectricElectricalLVSubscribers` y `SystemElectricElectricalLVLines`, que guardan los datos del abonado (nombre, CUPS, número de contador y dirección): en estas copias no hay baja tensión modelada y ningún trabajo las leía. `enable_cdc.sql` les quita el CDC en una base que ya lo tenía y `tests/01_bronze/test_cdc_tablas.py` falla si vuelven a entrar. Las copias anteriores al cambio que quedan en el entorno de desarrollo (los mensajes de Kafka hasta que caducan, los ficheros de Landing y las dos tablas de Bronze) se borran antes del primer piloto. Los nombres de las distribuidoras y los códigos de las instalaciones son además información de negocio confidencial, y en todo lo visible las redes son la Distribuidora 1, 2 y 3. Las credenciales del entorno van en `.env`, que no se versiona.

## Bias, Risks, and Limitations

- **Cobertura temporal desigual.** Hay etiquetas desde 2010 pero telemetría desde 2021, y la integración automática Calser - TedisNet solo tiene unos meses (noviembre de 2025). El cruce fino entre features y etiquetas es más fiable en el último tramo.
- **Eventos sistémicos.** En la Distribuidora 1 y la Distribuidora 2 el 94 % de las interrupciones afectan a varios CT a la vez (hasta 517). Un modelo puede aprender a predecir "día malo" en lugar de "CT malo"; por eso las métricas se calculan por separado para eventos locales y sistémicos.
- **Duplicados y solapes en origen.** 984 filas duplicadas en la Distribuidora 1 y 1.952 en la Distribuidora 2; 1.605 y 2.388 pares de interrupciones solapadas en el mismo CT. Silver los deduplica y marca; la fusión de solapes se decide en Gold.
- **Cortes deducidos por topología.** Hasta 69 eventos idénticos para un mismo cambio de tag y un 18,7 % de eventos sin elementos son cifras que medimos en una base de pruebas; sobre las copias de explotación, Silver los marca en cada ejecución (`n_eventos_origen`, `sin_elementos`); los transformadores sin nodo nunca aparecen como cortados; si RabbitMQ cae no se copia nada al histórico. Todo esto se corrige o se marca en `f_corte_evento`.
- **Telemetría parcial.** Solo unos 4.000 tags tienen serie regular, una parte de las muestras no tiene calidad Buena (Silver la cuenta por tag, día y motivo), los valores de estado son copiados (detalle de calidad 14) y el muestreo sample-and-hold produce valores congelados (Silver guarda su antigüedad en `antiguedad_s`). La mayoría de las series son de cabecera (`AI.INTENSIDAD`, `AI.TENSION`); muchos CT no tienen medidas propias y solo ven las de la posición que los alimenta.
- **Topología aprendida.** El interruptor que corta un CT suele estar en otra rama de la jerarquía funcional. Gold lo aprende de los cortes históricos (`map_aguas_arriba`), así que un CT que nunca se ha cortado no tiene posición aguas arriba.
- **Metadatos actuales, eventos históricos.** La topología y los atributos de los elementos son los de hoy; los eventos de hace años pueden referirse a elementos que ya cambiaron.
- **Leakage.** `fecha_alta`, `ts`, las columnas `*_OPTIMIZADA` y las tablas `calculos_*` de Calser conocen el futuro y no pueden usarse como feature. El estado del interruptor del propio CT es el corte, no una señal previa.
- **Desbalance extremo** de clases y solo tres distribuidoras (dos de ellas grandes), todas con el mismo SCADA. No hay garantía de que el modelo generalice a otra distribuidora sin recalibrar.
- **Meteorología de reanálisis.** Entró en la 0.7.0 y hoy cubre el reanálisis de Open-Meteo (ECMWF IFS, 9 km, desde 2017) por celda de 0,1 grados, los avisos de AEMET desde 2018 y, desde 2024, lo que preveía ICON-EU la víspera. En Gold no hay rayos: el archivo del Lightning Imager de MTG empieza el 04.07.2024 y solo llega a Landing y al cuadro de mando, así que en el dataset las tormentas se aproximan con las rachas, la lluvia intensa y el CAPE ([ml-training.md](ml-training.md), apartado 5).

### Recommendations

Para entrenar usamos solo las tablas `Historic*`, porque las `System*` retienen una semana, y filtramos por `QualitySourceId = 1` o con los flags de Silver. Evaluamos con división temporal y con métricas separadas por tipo de evento. Los microcortes y los errores de comunicaciones no los borramos antes de Gold: para la etiqueta sobran, pero son señales precursoras. Y cualquier resultado hay que leerlo sabiendo que solo unas 3 o 4 de cada 10.000 filas son positivas.

## Versioning

- La versión cruda es el backup fechado (14.08.2026). Cambiar de backup es cambiar de versión del dataset.
- Bronze, Silver y Gold son tablas Delta: el transaction log guarda cada escritura y permite time travel dentro del período de retención, que es de una semana en las tres zonas. `dag_mantenimiento_delta` hace cada domingo a las 04:10 OPTIMIZE y VACUUM con 168 horas de retención sobre `l1_bronze`, `l2_silver` y `l3_gold`. En la primera pasada, el 16.10.2026, el mantenimiento se saltó por falta de memoria las cinco tablas más grandes (`HistoricTagIntervalValuesBig`, `f_tag_interval_value`, `features_ct_hora`, `dataset_train` y `ml_predicciones`), que de momento no se compactan ni se limpian. `dataset_train` la compactamos a mano por particiones el 17.10.2026 (2.032 ficheros a 70, y 106 después de ordenar 2025 con ZORDER) y desde entonces el domingo solo le pasa VACUUM.
- Las tablas que lee Trino no pueden llevar más característica de tabla que los deletion vectors: el conector de Delta de Trino no admite ninguna otra (documentación de Trino 483, comprobado el 17.10.2026), así que liquid clustering, row tracking o los checkpoints v2 dejarían la tabla fuera del alcance de las consultas. Por eso ordenamos con ZORDER y no con liquid clustering, y `tests/03_gold/test_compatibilidad_trino.py` falla si algún job los activa. Trino calcula los mínimos, máximos y nulos de cada fichero a partir del transaction log: las columnas sin estadísticas (en `dataset_train`, de la 33 en adelante) no sirven para saltar ficheros.
- Cada ejecución del DAG de Silver queda identificada por su `run_id` en `dq_metrics`, y la tabla grande de series se sobrescribe por meses (`replaceWhere`), de modo que se puede reprocesar un mes sin tocar el resto.
- El dataset de entrenamiento se congela en Gold con un identificador de versión (tabla `dataset_versions`, con los parámetros y las versiones Delta de sus entradas) y cada entrenamiento guarda en `l3_gold.ml_runs` la versión que usó; el registro en MLflow sigue pendiente. Evaluamos DVC y decidimos no usarlo: los datos viven en el lakehouse y los backups pesan unos 66 GB (61 GiB); cada versión se reconstruye con el código de su versión, el backup fechado y su fila de `dataset_versions`, y Delta guarda además las versiones de la última semana; en Git solo van el código, la configuración y el fichero de referencia de municipios.

## Citation [optional]

**APA:** Morancho i Poyatos, J. y Shebarshinov Polenyaka, A. (2026). GridPredic: dataset de interrupciones y telemetría SCADA de tres distribuidoras (Calser + TedisNet). TFM, UPC School.

## Glossary [optional]

- **CT**: centro de transformación. Unidad de predicción del modelo.
- **Calser**: aplicación de SITEL para registrar interrupciones y calcular TIEPI y NIEPI.
- **TedisNet**: SCADA de SITEL. Versión 3.4 en las instalaciones del dataset.
- **TIEPI / NIEPI**: tiempo y número de interrupciones equivalentes de la potencia instalada en media tensión (Orden ECO/797/2002).
- **Tag**: señal del SCADA (medida analógica, estado digital o valor calculado).
- **CDC**: change data capture, mecanismo de SQL Server que Debezium usa para publicar cambios en Kafka.

## More Information [optional]

- [silver-layer.md](silver-layer.md): reglas de limpieza, rechazos y flags de cada tabla.
- [gold-layer.md](gold-layer.md): etiquetas, features, dataset de entrenamiento y sus decisiones.
- [project-structure.md](project-structure.md): dónde está cada cosa en el repositorio.
- [model_card.md](model_card.md): modelo que se entrena sobre este dataset.

## Dataset Card Authors [optional]

Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka.

## Dataset Card Contact

A través de los issues del repositorio o de los autores (UPC School, TFM GridPredic 2026).
