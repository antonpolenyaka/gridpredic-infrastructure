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
| `Calser_EOSA` (D1, Eléctrica del Oeste) | Interrupciones, incidencias, CTs, salidas, municipios y períodos. 805 CTs | 1,3 GB |
| `Calser_Pitarch` (D2) | Lo mismo para 598 CTs | 1,0 GB |
| `Calser_ValleSantaAna` (D3) | Lo mismo para 11 CTs | 23 MB |
| `TedisNet_EOSA` | SCADA de las tres distribuidoras: 78.301 tags, 12.307 nodos y 11.449 ramas de topología, 1.386 transformadores observables, series de medidas y eventos desde 2021 | 63,5 GB |

Cifras de volumen que condicionan el diseño del pipeline:

- `HistoricTagIntervalValuesBig` tiene 2.152 millones de filas (220 GB): es la serie de medidas muestreada cada 5 o 10 minutos. Solo unos 4.000 de los 78.000 tags tienen serie regular.
- `HistoricTagValueChanges` tiene 2,79 millones de cambios de estado y de valor, con un 47 % telemedido y un 39 % calculado.
- Cortes deducidos por TedisNet: unos 21.800 eventos (Off y On) sobre 1.385 transformadores distintos, de 01.01.2021 en adelante.
- Interrupciones registradas en Calser: 27.680 en D1, 19.887 en D2 y 770 en D3 (todos los años). Dentro de la ventana en la que hay telemetría, desde 2021, quedan unos 14.000 eventos que cumplen la definición del target.

- **Curated by:** Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka (TFM del Máster en Data Science and Engineering, UPC School, 2025-2026)
- **Funded by [optional]:** proyecto académico sin financiación externa
- **Shared by [optional]:** SITEL Sistemas Electrónicos SA (propietaria del SCADA TedisNet y de la aplicación Calser) y las tres distribuidoras
- **Language(s) (NLP):** es (catálogos y descripciones en castellano; nombres de tablas y columnas de TedisNet en inglés)
- **License:** propietaria. Uso limitado al TFM y al prototipo GridPredic. Los backups, los volúmenes de Docker y cualquier extracto quedan fuera del control de versiones (`.gitignore`).

### Dataset Sources [optional]

- **Repository:** https://github.com/antonpolenyaka/gridpredic-infrastructure (código de ingesta y transformación; los datos no están en el repositorio)
- **Paper [optional]:** memoria del TFM GridPredic (UPC School, hito M7, en redacción)
- **Demo [optional]:** no aplica

## Uses

### Direct Use

- Construir el dataset supervisado de GridPredic: una fila por centro de transformación (CT) y hora, con la etiqueta "empieza una interrupción imprevista en las siguientes 1 a 3 horas" y las variables derivadas de la telemetría previa.
- Análisis de calidad de suministro por distribuidora, municipio y CT (frecuencia y duración de interrupciones; TIEPI y NIEPI a nivel de zona solo para validar los cálculos, no para reportar).
- Detección de anomalías en la telemetría: por eso Silver marca los valores dudosos en lugar de borrarlos.

### Out-of-Scope Use

- Cualquier uso fuera del TFM y del prototipo sin autorización de SITEL y de las distribuidoras.
- Reporte regulatorio de calidad de suministro a la CNMC: el dataset limpia y deduplica, y el cálculo oficial se hace en Calser.
- Análisis de clientes o de consumo individual: las tablas de abonados y acometidas no se ingieren, y no debe cruzarse con datos personales.
- Redes de baja tensión: TedisNet no modela la BT de estas distribuidoras, así que el dataset solo sirve para media tensión.
- Reentrenar sobre `BDTest_v640` o cualquier copia de Pitarch: es la misma base congelada y duplicaría el 43 % de los eventos.

## Dataset Structure

El dataset se organiza en las zonas del lakehouse. La ruta física es `s3a://datalake/<zona>/` en MinIO y las tablas están registradas en el Hive Metastore, así que se consultan por nombre desde Spark o desde Trino (`lakehouse.l1_bronze`, `lakehouse.l2_silver`, `lakehouse.l3_gold`).

| Zona | Esquema | Contenido | Estado |
| --- | --- | --- | --- |
| Landing | `00_landing/` | Eventos CDC de Kafka tal cual llegan (Parquet) y ficheros de referencia | operativa |
| Bronze | `l1_bronze` | Copia fiel de SQL Server en Delta: 33 tablas `Historic*` y `Lib*` de TedisNet en batch, 31 tablas `System*` por CDC, 16 tablas de cada base Calser, más `reference_municipios` | operativa |
| Silver | `l2_silver` | Una versión válida de cada registro, con tipos correctos, distribuidora resuelta, rechazos con motivo y anomalías marcadas con flags | operativa |
| Gold | `l3_gold` | Etiquetas, features por CT y hora y versiones congeladas del dataset de entrenamiento | operativa, pendiente de la primera ejecución con la ventana completa |

Tablas de Silver (el detalle de cada regla está en [silver-layer.md](silver-layer.md)):

| Tabla | Grano | Origen |
| --- | --- | --- |
| `d_elemento`, `d_tag`, `d_ct`, `d_ct_scada`, `d_salida`, `d_municipio`, `d_periodo`, `d_tipo_generico` | dimensiones | TedisNet `SystemElements`, `SystemTags`; Calser `cts`, `salidas`, `municipios`, `periodos`, `tipo_generico` |
| `f_interrupcion`, `f_incidencia` | una interrupción o incidencia de Calser, por distribuidora y período | Calser `interrupciones`, `incidencias` |
| `f_tag_value_change`, `f_tag_quality_event`, `f_evento` | un cambio de valor, un evento de calidad, un evento del SCADA | TedisNet `Historic*` + `System*` |
| `f_tag_interval_value`, `f_tag_interval_value_rechazo_diario` | una muestra por tag e instante de rejilla (`UpdateTimestamp`), con la antigüedad del valor retenido, particionada por mes | `HistoricTagIntervalValuesBig` + `HistoricTagIntervalValues` |
| `f_command_execution`, `f_corte_evento`, `f_corte_elemento` | mandos y cortes deducidos por topología, con los elementos afectados | TedisNet `*CommandExecutions`, `*ElectricPowerCut*` |
| `dq_metrics` y `<tabla>_rejected` | métricas de calidad por ejecución y filas descartadas con su `_motivo` | todos los jobs |

Tablas de Gold (el detalle está en [gold-layer.md](gold-layer.md)):

| Tabla | Grano | Contenido |
| --- | --- | --- |
| `dim_ct`, `map_tag_ct`, `map_aguas_arriba` | CT, tag, trafo y elemento | El CT con su potencia imputada, si entra en el estudio y desde y hasta cuándo existe en Calser; el CT y el grupo de red de cada señal; las posiciones de cabecera que han cortado cada trafo y desde cuándo se sabe |
| `fact_interrupciones_mt`, `fact_cortes_scada` | evento | Interrupciones de Calser en tres variantes con los solapes fusionados; episodios Off -> On del SCADA con microcortes y maniobras |
| `labels_ct_hora` | CT y hora | `y_1_3h` y sus variantes, `en_corte`, `ct_vigente`, horas hasta el próximo evento y era |
| `agg_medida_hora`, `actividad_scada_hora` | clave y hora | Agregados horarios de las series de medida y muestras por distribuidora y hora |
| `features_ct_hora`, `feature_metadata` | CT y hora | Unas 250 features y su descripción |
| `dataset_train`, `dataset_versions` | CT y hora | Dataset de entrenamiento sin las horas en corte, sin SCADA activo o en las que el CT no existía, con split temporal (y 6 h de purga antes de cada frontera) y muestreo reproducible, y el registro de cada versión |

Columnas técnicas comunes: `distribuidora_id` (el `DistributorId` de Calser, que coincide con el `Id` del elemento raíz en TedisNet: 3 EOSA, 2 Pitarch, 1366 Valle de Santa Ana), `_origen` (Historic, System o Stream), `run_id` de la ejecución del DAG y los flags booleanos de anomalías (`en_solape`, `duracion_cero`, `es_microcorte`, `es_estimado`, `es_copiado`, `es_error_comm`, `es_maniobra`, `potencia_cero`, `sin_telemetria`...).

Clave de cruce entre las dos fuentes: `Calser.cts.CT_ID` (texto de cinco dígitos, por ejemplo "01011") = `TedisNet.SystemElements.ShortName` del elemento de tipo 145 (TRAFO CT) de la misma distribuidora. La cobertura verificada es del 97,8 % de los CT con interrupciones desde 2021 (1.317 de 1.346) y del 99,4 % de los eventos.

Definición del target (se materializa en Gold con los mismos filtros que usa Calser para el TIEPI): interrupción con clasificación `CL_IMPRE` (imprevista), factor distinto de `FA_CLIEN` (causa cliente), duración mayor de 180 segundos y nivel de afectación CT (sin salida, acometida ni abonado). Grano (CT, hora); `y = 1` si una interrupción así empieza en las siguientes 1 a 3 horas. La tasa de positivos es del orden de 2 por cada 10.000 filas CT-hora, así que las métricas de referencia son precision, recall y PR-AUC.

Particiones para entrenar: temporal, sin mezclar fechas. Entrenamiento hasta 2024, validación 2025 y prueba 2026 (forward chaining para la selección de hiperparámetros). Cada versión del dataset de entrenamiento se congela en `l3_gold.dataset_train` y queda registrada en `l3_gold.dataset_versions` con sus parámetros, las fechas exactas de la ventana y la versión Delta de cada tabla de entrada.

## Dataset Creation

### Curation Rationale

Las distribuidoras cobran una retribución regulada que depende, entre otras cosas, de sus índices de calidad de suministro. Anticipar una interrupción con 1 a 3 horas de margen permite reposicionar a los operarios antes de que ocurra (hoy tardan 20 a 30 minutos en llegar y la ventana relevante son los primeros 180 segundos). Para entrenar un modelo hacía falta juntar en un solo sitio dos sistemas que en las distribuidoras viven separados: el registro de interrupciones (Calser) y la telemetría (TedisNet). Ninguna de las dos fuentes se creó pensando en machine learning, y de ahí el peso que tienen la limpieza y la documentación de sesgos en esta ficha.

### Source Data

#### Data Collection and Processing

1. **Backups.** SITEL entrega los `.bak` de SQL Server (fechados) y se copian en `infra/sqlserver/backups/`. Al arrancar el stack se restauran automáticamente (`infra/sqlserver/scripts/restore_databases.sql`) y se activa CDC en las tablas `System*` de TedisNet (`enable_cdc.sql`).
2. **Bronze batch.** El DAG `ingest_sqlserver_batch_bronze` de Airflow lanza un job Spark por tabla (`etl/jobs/01_bronze/job_bronze_sqlserver_batch.py`) que lee por JDBC y escribe Delta sin transformar el contenido. Las tablas se listan en `etl/config/01_bronze/config_bronze_sqlserver.json`.
3. **Bronze streaming.** Debezium publica los cambios de las tablas `System*` en Kafka; `job_landing_sqlserver_streaming.py` los guarda tal cual en Landing y `job_bronze_sqlserver_streaming.py` los aplica sobre las tablas Delta de Bronze. Sirve para las tablas que en origen solo guardan la última semana.
4. **Silver.** El DAG `dag_silver` ejecuta los jobs de `etl/jobs/02_silver/` en el orden de `config_silver.json`. Cada job reconstruye su tabla desde Bronze (idempotente), separa rechazos y marcados, y escribe sus métricas en `dq_metrics`. El último job comprueba integridad referencial, cobertura de la clave Calser - TedisNet y alineación horaria entre las dos fuentes.
5. **Gold.** El DAG `dag_gold` construye la dimensión de CT y los mapas de señales, los hechos de interrupciones y cortes, las etiquetas, los agregados de medidas, las features, el dataset de entrenamiento con su versión y los controles finales (alineación horaria entre fuentes, cobertura, positivos, leakage). Las reglas están en `etl/config/03_gold/config_gold.json` y se prueban sin Docker con `python -m pytest tests/03_gold -q`.

Todo el proceso es reproducible con `docker compose up -d --build --wait` y los DAGs, tal como se describe en el [README](../README.md). Las reglas de Silver se prueban sin Docker con `python -m pytest tests/02_silver -q`.

#### Who are the source data producers?

- Las RTU y los equipos de campo de las tres distribuidoras, que envían medidas y estados al SCADA TedisNet (SITEL). Los cortes no se miden: TedisNet los deduce por topología a partir de los tags de posición de los interruptores.
- Los operadores de las distribuidoras, que registran o completan las interrupciones e incidencias en Calser. Desde noviembre de 2025 una parte de las interrupciones se importa automáticamente desde TedisNet.
- El INE, a través del fichero de referencia de municipios (`data/reference_data/municipios.xlsx`).

### Annotations [optional]

#### Annotation process

La etiqueta no la pone una persona pensando en el modelo. Sale del registro administrativo de interrupciones de Calser, que tiene tres eras: hasta 2017 importaciones de ficheros (con microcortes), de 2018 a octubre de 2025 registro manual (sin microcortes, porque el parámetro `MinimalDurationImportInterruptions` está a 180 s en las tres bases) y desde noviembre de 2025 importación desde el SCADA. Entre un 26 y un 28 % de las interrupciones de D1 y D2 no tienen incidencia asociada, y la fecha de alta puede ser semanas posterior al evento.

#### Who are the annotators?

Personal de operación de cada distribuidora y el importador automático de Calser.

#### Personal and Sensitive Information

No se ingieren las tablas de abonados, acometidas ni contratos. El grano más fino es el centro de transformación, identificado por su código interno y su municipio. Los nombres de las distribuidoras y los códigos de las instalaciones son información de negocio confidencial, no datos personales. Las credenciales del entorno van en `.env`, que no se versiona.

## Bias, Risks, and Limitations

- **Cobertura temporal desigual.** Hay etiquetas desde 2010 pero telemetría desde 2021, y la integración automática Calser - TedisNet solo tiene unos meses (noviembre de 2025). El cruce fino entre features y etiquetas es más fiable en el último tramo.
- **Eventos sistémicos.** En D1 y D2 el 94 % de las interrupciones afectan a varios CT a la vez (hasta 517). Un modelo puede aprender a predecir "día malo" en lugar de "CT malo"; por eso las métricas se calculan por separado para eventos locales y sistémicos.
- **Duplicados y solapes en origen.** 984 filas duplicadas en D1 y 1.952 en D2; 1.605 y 2.388 pares de interrupciones solapadas en el mismo CT. Silver los deduplica y marca; la fusión de solapes se decide en Gold.
- **Cortes deducidos, no medidos.** TedisNet genera hasta 69 eventos idénticos para un mismo cambio de tag y un 18,7 % de eventos sin elementos; los transformadores sin nodo nunca aparecen como cortados; si RabbitMQ cae no se copia nada al histórico. Todo esto se corrige o se marca en `f_corte_evento`.
- **Telemetría parcial.** Solo unos 4.000 tags tienen serie regular, el 22 % de las muestras no tienen calidad Buena, los valores de estado son copiados (detalle de calidad 14) y el muestreo sample-and-hold produce valores congelados (Silver guarda su antigüedad en `antiguedad_s`). La mayoría de las series son de cabecera (`AI.INTENSIDAD`, `AI.TENSION`); muchos CT no tienen medidas propias y solo ven las de la posición que los alimenta.
- **Topología aprendida.** El interruptor que corta un CT suele estar en otra rama de la jerarquía funcional. Gold lo aprende de los cortes históricos (`map_aguas_arriba`), así que un CT que nunca se ha cortado no tiene posición aguas arriba.
- **Metadatos actuales, eventos históricos.** La topología y los atributos de los elementos son los de hoy; los eventos de hace años pueden referirse a elementos que ya cambiaron.
- **Leakage.** `fecha_alta`, `ts`, las columnas `*_OPTIMIZADA` y las tablas `calculos_*` de Calser conocen el futuro y no pueden usarse como feature. El estado del interruptor del propio CT es el corte, no una señal previa.
- **Desbalance extremo** de clases y solo tres distribuidoras (dos de ellas grandes), todas con el mismo SCADA. No hay garantía de que el modelo generalice a otra distribuidora sin recalibrar.
- **Sin meteorología todavía.** Es una de las fuentes externas previstas y aún no está ingerida.

### Recommendations

Entrenar solo con `Historic*` (las tablas `System*` retienen una semana), filtrar por `QualitySourceId = 1` o usar los flags de Silver, evaluar con división temporal y con métricas separadas por tipo de evento, y no borrar los microcortes ni los errores de comunicaciones antes de Gold: para la etiqueta sobran, pero son señales precursoras. Cualquier resultado debe leerse sabiendo que unas 2 de cada 10.000 filas son positivas.

## Versioning

- La versión cruda es el backup fechado (14.08.2026). Cambiar de backup es cambiar de versión del dataset.
- Bronze y Silver son tablas Delta: el transaction log guarda cada escritura y permite time travel dentro del período de retención (7 días en Bronze, 30 en Silver y Gold según la política de housekeeping definida en la memoria del TFM).
- Cada ejecución del DAG de Silver queda identificada por su `run_id` en `dq_metrics`, y la tabla grande de series se sobrescribe por meses (`replaceWhere`), de modo que se puede reprocesar un mes sin tocar el resto.
- El dataset de entrenamiento se congela en Gold con un identificador de versión (tabla `dataset_versions`, con los parámetros y las versiones Delta de sus entradas) y se registrará junto al modelo en MLflow. Evaluamos DVC y decidimos no usarlo: los datos viven en el lakehouse, los backups pesan 61 GB y Delta ya aporta el versionado; en Git solo van el código, la configuración y el fichero de referencia de municipios.

## Citation [optional]

**APA:** Morancho, J. y Shebarshinov, A. (2026). GridPredic: dataset de interrupciones y telemetría SCADA de tres distribuidoras (Calser + TedisNet). TFM, UPC School.

## Glossary [optional]

- **CT**: centro de transformación. Unidad de predicción del modelo.
- **Calser**: aplicación de SITEL para registrar interrupciones y calcular TIEPI y NIEPI.
- **TedisNet**: SCADA de SITEL. Versión 3.4 en las instalaciones del dataset.
- **TIEPI / NIEPI**: tiempo y número de interrupciones equivalentes de la potencia instalada en media tensión (Orden ECO/797/2002).
- **Tag**: señal del SCADA (medida analógica, estado digital o valor calculado).
- **CDC**: change data capture, mecanismo de SQL Server que Debezium usa para publicar cambios en Kafka.

## More Information [optional]

- [silver-layer.md](silver-layer.md): reglas de limpieza, rechazos y flags de cada tabla.
- [project-structure.md](project-structure.md): dónde está cada cosa en el repositorio.
- [model_card.md](model_card.md): modelo que se entrena sobre este dataset.

## Dataset Card Authors [optional]

Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka.

## Dataset Card Contact

A través de los issues del repositorio o de los autores (UPC School, TFM GridPredic 2026).
