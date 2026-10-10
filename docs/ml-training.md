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

## 3. Modelos

| Modelo | Tipo | Por qué está |
| --- | --- | --- |
| `tasa_base` | Referencia | La misma puntuación para todas las filas. Su PR-AUC es la proporción de positivos: es el suelo |
| `naif_historico` | Referencia | Ordena los CT por sus interrupciones conocidas en 365 días (y 90 y 30 para desempatar). Es lo que haría un operador sin modelo y la referencia que pide la model card |
| `logistica` | Regresión logística L2 | Referencia interpretable. Imputación por mediana con indicador de nulo, escala `sign(x) * log(1 + abs(x))` y estandarización |
| `random_forest` | Random Forest de scikit-learn | Otro modelo de árboles para comparar con el boosting. Acepta nulos sin imputar |
| `xgboost` | Gradient boosting (XGBoost, `hist`) | Candidato principal, el que presentamos en las presentaciones. Parada temprana sobre la PR-AUC de la muestra de valid |

Para cada modelo entrenado `busqueda` tiene una lista corta de configuraciones (3 para la logística, 2 para el bosque, 4 para XGBoost). Se entrenan todas con la muestra de train y se queda la de mejor PR-AUC en la muestra de valid. Todas quedan registradas en `ml_runs.busqueda_json`, también las descartadas, para poder explicar la elección en la memoria. `--sin-busqueda` usa solo la primera de cada lista (para una prueba rápida).

Como la configuración se elige sobre valid, la cifra de valid es un poco optimista; la de test es la honesta. Por eso test solo se mira al final.

## 4. Métricas

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

## 5. Explicabilidad y control de leakage

`l3_gold.ml_importancia` guarda, por feature:

- `shap_medio_abs` de XGBoost: media del valor SHAP absoluto (TreeSHAP exacto de la propia librería, `pred_contribs`) sobre hasta 50.000 filas de la muestra de valid. Para los gráficos de la memoria, el notebook puede recalcular con el paquete `shap` cargando el modelo guardado.
- `gain` de XGBoost, `importancia_impureza` del bosque y `coef_abs` de la logística (sobre la feature estandarizada).
- `pr_auc_univariante`: la PR-AUC de cada feature usada sola como puntuación. Si una sola feature ordena los positivos casi a la perfección, lo normal es que se haya colado información del futuro. Las que superan `alerta_leakage_pr_auc` (0,3) quedan con `alerta_leakage = true` y salen en el log. Antes de dar por bueno un modelo hay que revisarlas; si alguna es leakage, se añade a `excluir_features` y se vuelve a entrenar.

## 6. Resultados que deja cada ejecución

Cada ejecución tiene un `run_id` (`ml_<fecha>_<hash>`) y escribe en el esquema `l3_gold` (en Trino, `lakehouse.l3_gold`):

| Tabla | Contenido |
| --- | --- |
| `ml_runs` | Una fila por ejecución: versión del dataset, etiqueta, parámetros y resultado de la búsqueda, configuración completa, resumen de PR-AUC y ROC-AUC, splits evaluados y carpeta de los modelos |
| `ml_metricas` | Todas las métricas en formato largo |
| `ml_importancia` | Importancias y PR-AUC univariante, con la alerta de leakage |
| `ml_predicciones` | Puntuación de cada modelo para cada fila de valid y de test, con las etiquetas, la antelación y los segmentos. Particionada por `run_id` y `split` |

Los modelos se guardan en MinIO en `s3://datalake/ml/modelos/<run_id>/`: un `.pkl` por modelo (el objeto con su método `score`), `xgboost_booster.json` (formato propio de XGBoost, que no depende de la versión de Python) y `run.json` con las features en orden y los parámetros.

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

## 7. Cómo lanzarlo

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

## 8. Pendiente

- MLflow como servicio en `compose.yaml` (con Postgres y MinIO, que ya están) para registrar cada ejecución con sus métricas y artefactos. Mientras tanto `ml_runs` cumple esa función y guarda lo mismo.
- Notebook `1.0-asp-resultados-modelo.ipynb` con las curvas precision-recall, la calibración, los gráficos SHAP y el análisis de errores por segmento, leyendo `ml_predicciones` y `ml_importancia`.
- Reentrenar con train + valid antes de la evaluación final, si el tiempo lo permite. Ahora el modelo que se evalúa en test es el mismo que se ha elegido en valid, entrenado solo con train.
- DAG `04_ml` de inferencia que escriba `predicciones_ct_hora` sobre `features_ct_hora` con el modelo elegido.
