# Entrenamiento del modelo

Este documento explica cómo entrenamos y evaluamos el modelo de predicción de interrupciones sobre el dataset de Gold, qué decisiones hemos tomado y dónde quedan los resultados. Los requisitos y las métricas de aceptación están en la [model card](model_card.md); aquí está el cómo.

El código está en `etl/jobs/04_ml/` y los parámetros en `etl/config/04_ml/config_ml.json`.

| Fichero | Qué hace |
| --- | --- |
| `job_ml_train.py` | Lee la versión del dataset, entrena, puntúa valid y test, calcula las métricas y escribe las tablas de resultados |
| `ml_models.py` | Los cinco modelos (dos referencias y tres entrenados), la corrección de la probabilidad por el muestreo y las importancias |
| `ml_metrics.py` | PR-AUC, ROC-AUC, alertas con presupuesto, episodios, antelación y PR-AUC de cada feature sola |
| `train.sh` | Lanza el job con `spark-submit` desde el contenedor `spark-master` |

## 1. Qué aprende el modelo

Cada fila de `l3_gold.dataset_train` es un CT en una hora. La etiqueta `y_1_3h` vale 1 si en las siguientes 1 a 3 horas empieza una interrupción imprevista de Calser en ese CT (con los mismos filtros que usa Calser para el TIEPI). Las 246 features describen lo que el SCADA TedisNet sabía antes de esa hora: medidas eléctricas del CT y de la cabecera que lo alimenta, señales de defecto, disparos, alarmas, comunicaciones, cortes vistos por el SCADA, el histórico del CT y el calendario. Gold ya garantiza que ninguna feature usa datos posteriores a su hora (ver [gold-layer.md](gold-layer.md)).

Es un problema muy desbalanceado: en el dataset `ds_20261010T045104_a5a2531cacbb` hay 13.124 positivos entre 43,6 millones de filas de train (un 0,03 %).

## 2. Datos que usa cada paso

| Paso | Filas | Para qué |
| --- | --- | --- |
| Muestra de train | `split = 'train'` y `en_muestra_train`: todos los positivos y el 2 % de los negativos (unas 885.000 filas) | Entrenar los modelos. Cabe en memoria del driver en float32 |
| Muestra de valid | `split = 'valid'`: todos los positivos y un 2 % de los negativos, con peso 1 / 0,02 en los negativos | Parada temprana de XGBoost y elección de la configuración de cada modelo por la PR-AUC ponderada |
| Valid completo | Las 11,2 millones de filas de 2025 | Métricas que reportamos y comparación entre modelos |
| Test completo | 2026 hasta `evaluacion.test_hasta` | Estimación final, una sola vez y con `--evaluar-test` |

Nunca usamos validación cruzada aleatoria: el modelo se usa sobre los mismos CT en el futuro, así que lo evaluamos igual. Los CT no se separan entre splits.

**Corrección del muestreo.** Los modelos se entrenan sobre la muestra, donde los positivos pesan unas 50 veces más que en la realidad. En vez de entrenar con `peso_muestra`, corregimos la probabilidad después con la corrección de prior, `p = p_s * r / (p_s * r + 1 - p_s)` con `r = 0,02`. Es monótona, así que no cambia la ordenación ni ninguna métrica de ranking (PR-AUC, ROC-AUC, alertas con presupuesto), y deja las probabilidades en la escala de la población real. Con `usar_peso_muestra: true` se entrena con los pesos y no se corrige.

**Final de test.** Las etiquetas de los últimos 41 a 62 días de la ventana están censuradas (las interrupciones tardan en cargarse en Calser, lo mide `job_gold_dq_checks`). Contar esas horas como negativas penalizaría al modelo por acertar, así que la evaluación de test se corta en `evaluacion.test_hasta` (01.06.2026 por defecto). Las predicciones de esas horas sí se guardan.

## 3. Estado de la red

El 94 % de las interrupciones de las dos distribuidoras grandes son sistémicas: un temporal o un fallo aguas arriba deja sin suministro a varios CT a la vez. La primera ejecución lo confirmó en los datos. Ninguna feature sola separa bien los positivos (la mejor tiene una PR-AUC de 0,00098, 2,6 veces la tasa base), y el histórico del CT funciona al revés: el 27 % de los positivos de valid son de CT sin ninguna interrupción en el último año, frente al 10 % de los negativos. Lo que sí anticipa los positivos son las señales de su grupo de red y de la cabecera (defectos de fase, faltas de tensión, disparos), de 3 a 5 veces más frecuentes antes de un positivo.

Por eso probamos un bloque de contexto (`contexto_red`) calculado en el momento de entrenar: para cada feature precursora de ventana corta (`ev_`, `evred_`, `evaa_`, `scada_`, `calidad_`, `corte_error_comm_` de 1 y 6 horas), la fracción de CT de la distribuidora en los que no es cero en esa hora (`red_frac_*`) y la misma fracción en las tres distribuidoras (`region_frac_*`). Es lo que ve un operador cuando la red "se mueve" en toda la zona.

- Se calcula sobre `features_ct_hora` (todos los CT y todas las horas), no sobre `dataset_train`, para que no dependa de qué CT estaban ya en corte, que sale de las etiquetas.
- Cada feature de una fila solo usa datos anteriores a su hora; una media de esas features entre los CT de la misma hora tampoco ve el futuro.
- Se escribe en `l3_gold.ml_contexto_red` y se une por `(distribuidora_id, hora)` a la muestra de train, a la de valid y a la puntuación. Para usar el modelo en producción hay que calcular el mismo bloque con las features de la hora.

**Resultado:** no mejoró, empeoró un poco. Con el bloque, XGBoost bajó en valid de una PR-AUC de 0,0008 a 0,00065 y de un ROC-AUC de 0,72 a 0,66. El modelo usaba mucho las fracciones de red y de región (9 de las 15 features con más peso), pero lo aprendido en 2021-2024 no se repetía en 2025: las señales de comunicaciones y de calidad, en particular, dependen de cómo ha ido cambiando el propio SCADA. El bloque queda en el código desactivado (`contexto_red.activo: false`) para poder repetir el experimento.

`etl/jobs/04_ml/diagnostico_senal.py` repite el diagnóstico en unos minutos: positivos por split y por año, PR-AUC de cada feature sola con sus medias en positivos y negativos, y cuándo se conocen las interrupciones de Calser.

## 4. Grupos de CT y submodelos

En la tutoría del 08.10.2026 el profesor de análisis de datos nos recomendó no entrenar un único modelo para los casi 1.300 CT: mezclar CT grandes y pequeños, urbanos y rurales, de zonas con distinta meteorología, diluye la capacidad de predicción. La propuesta es agrupar los CT en conjuntos homogéneos (por ejemplo con k-means) y entrenar un submodelo para cada grupo, empezando por el grupo más homogéneo y con más incidencias como piloto. Las métricas de la línea base ya lo apuntaban: la distribuidora 1366 tiene una PR-AUC 3,5 veces mayor que la tasa base, y las otras dos apenas la superan.

- **Grupos** (`clusters`): k-means sobre una fila por CT con el tipo de zona del municipio (urbana, semiurbana, rural concentrada, rural dispersa), la potencia, los abonados, las salidas, los transformadores, el tamaño del grupo de red, la telemetría, la tensión, las coordenadas (como aproximación de la meteorología), la distribuidora y la tasa de interrupciones del CT en train. Las variables asimétricas van en escala logarítmica, las categóricas en one-hot y todas estandarizadas. k se elige por silueta entre 3 y 8, con un mínimo de 20 CT en el grupo más pequeño. Solo se usan datos estáticos o de train, así que los grupos no miran valid ni test. El reparto queda en `l3_gold.ml_clusters_ct`, y el perfil de cada grupo en el resumen y en `grupos_ct.csv`.
- **Submodelos** (`submodelos`): un XGBoost por grupo con la configuración elegida para el modelo global y parada temprana sobre las filas del grupo en la muestra de valid. Un grupo con menos de 200 positivos en la muestra de train se queda con el modelo global. El conjunto se evalúa como un modelo más, `xgboost_cluster`, con las mismas métricas y sobre las mismas filas que el global. La búsqueda guarda, por grupo, la PR-AUC del submodelo frente a la del global.
- **Por qué pasa en cada grupo**: el resumen lista las 5 features con más peso (SHAP) en el submodelo de cada grupo, que es lo que permite explicar qué anticipa una interrupción en un CT rural disperso frente a uno urbano.
- **Estación del año** (`estacion`): 1 invierno, 2 primavera, 3 verano, 4 otoño, como variable propia, también a propuesta de la tutoría. El split temporal ya garantiza que train cubre todas las estaciones de varios años.
- Las métricas salen también por grupo (segmento `grupo_ct`).

## 5. Meteorología

La meteorología era lo que la model card daba como pendiente ("entrará cuando esté ingerida") y lo que más explica las interrupciones sistémicas. Viene de la [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (CC BY 4.0, plan gratuito para uso no comercial), con el modelo ECMWF IFS, de 9 km y horario desde 2017. Es el único de los reanálisis gratuitos que da a la vez ráfagas de viento y precipitación.

- **Qué se pide:** temperatura, humedad, precipitación, nieve, viento, ráfagas y presión a nivel del mar, de hora en hora, del 01.12.2020 al 31.08.2026. El dataset de Gold va del 01.01.2021 al 14.08.2026 y las features miran hasta 7 días atrás, así que con ese margen no falta ninguna hora.
- **Dónde:** los 73 municipios de `data/reference_data/municipios.xlsx` agrupados en celdas de 0,1 grados (unos 11 km, cerca de la rejilla de 9 km del modelo), que son 43. Así la descarga completa son unas 6.450 llamadas del plan gratuito, por debajo del límite de 10.000 al día.
- **Landing** (`etl/jobs/00_landing/job_landing_meteo_openmeteo.py`): una petición por celda y año, guardada tal cual (JSON) en `datalake/00_landing/meteo/openmeteo/`. Es reanudable y respeta los límites por minuto, hora y día. La configuración está en `etl/config/01_bronze/config_bronze_meteo.json`.
- **Bronze** (`etl/jobs/01_bronze/job_bronze_meteo_openmeteo.py`): `l1_bronze.meteo_openmeteo_hora` en hora local de Madrid, la misma convención que Calser y TedisNet. Las dos horas UTC que caen en la misma hora local al cambiar al horario de invierno se fusionan. `l1_bronze.meteo_openmeteo_celdas` asigna cada municipio a su celda.
- **Cruce con los CT:** el municipio de cada CT en Calser (`dim_ct.municipio_id`, que es el código INE) se cruza con la referencia por código INE y, si falla, por nombre sin tildes. Las coordenadas del municipio entran como `ct_lat_municipio` y `ct_lon_municipio` y en la clusterización, porque las de `dim_ct` estaban vacías.
- **Features** (`l3_gold.ml_meteo_celda_hora`): `met_*` son las últimas horas (ráfaga y viento de la hora; ráfaga máxima de 3, 6 y 24 h; lluvia de 1, 3, 6 y 24 h; nieve de 24 h; temperatura, mínima y máxima de 24 h; humedad; presión y su cambio en 3 y 24 h). `metprev_*` son las 3 horas siguientes: ráfaga máxima, lluvia y nieve. `met_region_*` y `metprev_region_*` son el máximo de todas las celdas, que da el tamaño del temporal. La fila de cada hora describe la hora que termina entonces, así que se conoce a esa hora, igual que el resto de features.
- **Sobre la previsión:** `metprev_*` usa el reanálisis como si fuera una previsión perfecta de las 3 horas siguientes. En operación sería la previsión de AEMET o del ECMWF, que a 1-3 horas es buena pero no perfecta, así que el resultado con estas features es una cota superior. Con `meteo.prevision_h: 0` se quitan para medir cuánto aporta solo la meteorología observada.
- **Limitación:** ninguna fuente gratuita da rayos históricos y la propia Open-Meteo avisa de que con estos datos no se pueden estimar tormentas. Las tormentas eléctricas solo se aproximan con ráfagas y precipitación intensa.

## 6. Modelos

| Modelo | Tipo | Por qué está |
| --- | --- | --- |
| `tasa_base` | Referencia | La misma puntuación para todas las filas. Su PR-AUC es la proporción de positivos: es el suelo |
| `naif_historico` | Referencia | Ordena los CT por sus interrupciones conocidas en 365 días (y 90 y 30 para desempatar). Es lo que haría un operador sin modelo y la referencia que pide la model card |
| `logistica` | Regresión logística L2 | Referencia interpretable. Imputación por mediana con indicador de nulo, escala `sign(x) * log(1 + abs(x))` y estandarización |
| `random_forest` | Random Forest de scikit-learn | Otro modelo de árboles para comparar con el boosting. Acepta nulos sin imputar |
| `xgboost` | Gradient boosting (XGBoost, `hist`) | Candidato principal, el que presentamos en las presentaciones. Parada temprana sobre la PR-AUC de la muestra de valid |

Para cada modelo entrenado `busqueda` tiene una lista corta de configuraciones (3 para la logística, 2 para el bosque, 4 para XGBoost). Se entrenan todas con la muestra de train y se queda la de mejor PR-AUC en la muestra de valid. Todas quedan registradas en `ml_runs.busqueda_json`, también las descartadas, para poder explicar la elección en la memoria. `--sin-busqueda` usa solo la primera de cada lista (para una prueba rápida).

Como la configuración se elige sobre valid, la cifra de valid es un poco optimista; la de test es la honesta. Por eso test solo se mira al final.

## 7. Métricas

Todas salen por modelo, por split, por etiqueta (`y_1_3h` y `y_1_3h_local`) y por segmento (total, distribuidora, tipo de zona del municipio y CT con o sin telemetría). Quedan en formato largo en `l3_gold.ml_metricas`.

| Métrica | Cómo se calcula |
| --- | --- |
| `pr_auc` | Average precision. Métrica principal |
| `roc_auc` | Secundaria, para comparar con la literatura |
| `lift_pr_auc` | `pr_auc / prevalencia`: cuántas veces mejor que puntuar al azar |
| `alertas@N`, `aciertos@N`, `precision@N` | Para cada distribuidora y día, las N filas (CT-hora) con más puntuación son las alertas. N sale de `presupuestos_alertas_dia` (5, 10, 20 y 50) |
| `recall_filas@N` | Positivos alertados / positivos |
| `recall_episodios@N` | Un episodio son las horas positivas seguidas de un mismo CT (una interrupción da unas tres). Es el recall que importa al operador: cuántas interrupciones tuvieron al menos un aviso |
| `antelacion_media_h@N`, `antelacion_mediana_h@N` | Para cada episodio detectado, las horas entre la primera alerta y el inicio de la interrupción (`horas_hasta_proximo_evento`) |

Con `y_1_3h_local` como etiqueta, las alertas que caen en interrupciones sistémicas cuentan como falsas; lo útil ahí es `recall_episodios`, que dice si el modelo encuentra el CT concreto y no solo "el día malo".

## 8. Explicabilidad y control de leakage

`l3_gold.ml_importancia` guarda, por feature:

- `shap_medio_abs` de XGBoost: media del valor SHAP absoluto (TreeSHAP exacto de la propia librería, `pred_contribs`) sobre hasta 50.000 filas de la muestra de valid. Para los gráficos de la memoria, el notebook puede recalcular con el paquete `shap` cargando el modelo guardado.
- `gain` de XGBoost, `importancia_impureza` del bosque y `coef_abs` de la logística (sobre la feature estandarizada).
- `pr_auc_univariante`: la PR-AUC de cada feature usada sola como puntuación. Si una sola feature ordena los positivos casi a la perfección, lo normal es que se haya colado información del futuro. Las que superan `alerta_leakage_pr_auc` (0,3) quedan con `alerta_leakage = true` y salen en el log. Antes de dar por bueno un modelo hay que revisarlas; si alguna es leakage, se añade a `excluir_features` y se vuelve a entrenar.

## 9. Resultados que deja cada ejecución

Cada ejecución tiene un `run_id` (`ml_<fecha>_<hash>`) y escribe en el esquema `l3_gold` (en Trino, `lakehouse.l3_gold`):

| Tabla | Contenido |
| --- | --- |
| `ml_runs` | Una fila por ejecución: versión del dataset, etiqueta, parámetros y resultado de la búsqueda, configuración completa, resumen de PR-AUC y ROC-AUC, splits evaluados y carpeta de los modelos |
| `ml_metricas` | Todas las métricas en formato largo |
| `ml_importancia` | Importancias y PR-AUC univariante, con la alerta de leakage |
| `ml_predicciones` | Puntuación de cada modelo para cada fila de valid y de test, con las etiquetas, la antelación y los segmentos. Particionada por `run_id` y `split` |

Además, cada ejecución escribe en el log un resumen en tablas (comparación de modelos, PR-AUC por distribuidora, eventos locales, las features con más peso, las alertas de leakage y la búsqueda) y deja `resumen.md`, `metricas.csv`, `importancia.csv` y `busqueda.json` en `exportar_dir/<run_id>/` dentro del contenedor (`/tmp/ml_resultados`). Así se pueden leer los resultados sin levantar Trino:

```bash
docker compose cp spark-master:/tmp/ml_resultados/. _runlogs/ml_resultados
```

Los modelos se guardan en MinIO en `s3://datalake/ml/modelos/<run_id>/`: un `.pkl` por modelo (el objeto con su método `score`), `xgboost_booster.json` (formato propio de XGBoost, que no depende de la versión de Python) y `run.json` con las features en orden y los parámetros.

Para el análisis completo de una ejecución (búsqueda, comparación, recall según el presupuesto, curvas precision-recall, calibración, segmentos, eventos locales, SHAP por feature y por bloque, leakage y test) está el notebook `notebooks/1.0-asp-resultados-modelo.ipynb`, que lee de Trino con el `run_id`.

Consultas útiles desde DBeaver:

```sql
-- Comparación de modelos en valid
SELECT modelo, metrica, ROUND(valor, 5) AS valor
FROM lakehouse.l3_gold.ml_metricas
WHERE run_id = '<run_id>' AND split = 'valid' AND etiqueta = 'y_1_3h' AND segmento = 'total'
  AND metrica IN ('pr_auc', 'lift_pr_auc', 'roc_auc', 'precision@10', 'recall_episodios@10', 'antelacion_media_h@10')
ORDER BY metrica, valor DESC;

-- Las 20 features que más pesan en XGBoost
SELECT feature, valor
FROM lakehouse.l3_gold.ml_importancia
WHERE run_id = '<run_id>' AND modelo = 'xgboost' AND metrica = 'shap_medio_abs'
ORDER BY valor DESC
LIMIT 20;

-- Features con sospecha de leakage
SELECT feature, valor
FROM lakehouse.l3_gold.ml_importancia
WHERE run_id = '<run_id>' AND alerta_leakage;
```

## 10. Cómo lanzarlo

Requisitos: el dataset de Gold construido (`dag_gold` terminado) y las imágenes de Spark reconstruidas con las dependencias nuevas (`xgboost-cpu`, `scikit-learn` y `pyarrow` en `infra/spark/requirements.txt`).

```bash
docker compose build spark-master spark-worker spark-history
docker compose up -d spark-master spark-worker spark-history

# Prueba rápida: una configuración por modelo, solo valid
docker compose exec spark-master bash /app/jobs/04_ml/train.sh --sin-busqueda

# Ejecución completa sobre valid
docker compose exec spark-master bash /app/jobs/04_ml/train.sh

# Evaluación final en test, con la configuración ya cerrada
docker compose exec spark-master bash /app/jobs/04_ml/train.sh --evaluar-test
```

Otras opciones: `--modelos xgboost,naif_historico` para entrenar solo algunos, `--dataset-version <id>` para fijar una versión concreta (por defecto, la última de `dataset_versions`, que tiene que ser la que hay en `dataset_train`).

El job no va en Airflow a propósito. El driver hace el entrenamiento y usa todos los cores durante bastante rato; en el contenedor de Airflow eso deja sin CPU al scheduler, que en el portátil ya se cae con la carga de I/O. En `spark-master` el driver tiene la memoria y los cores del contenedor y los executors solo puntúan valid y test.

**Memoria y tiempo en el portátil.** La muestra de train son unos 0,9 GB en float32 y el proceso Python del driver llega a unos 5 o 6 GB al entrenar el bosque. Conviene parar lo que no se usa (`docker compose stop sqlserver kafka kafka-connect kafka-ui trino`). Como referencia, contamos con entre una y dos horas para la ejecución completa con búsqueda, la mayor parte en XGBoost y en puntuar los 11 millones de filas de valid desde el disco externo.

## 11. Pendiente

- MLflow como servicio en `compose.yaml` (con Postgres y MinIO, que ya están) para registrar cada ejecución con sus métricas y artefactos. Mientras tanto `ml_runs` cumple esa función y guarda lo mismo.
- Política de reentreno: el modelo no se reentrena online. Se reentrena en batch cada 1 a 3 meses con los datos nuevos (recomendación de la tutoría), y cada reentreno es una ejecución más de este job con su `run_id` y su versión de dataset.
- Reentrenar con train + valid antes de la evaluación final, si el tiempo lo permite. Ahora el modelo que se evalúa en test es el mismo que se ha elegido en valid, entrenado solo con train.
- DAG `04_ml` de inferencia que escriba `predicciones_ct_hora` sobre `features_ct_hora` con el modelo elegido.
