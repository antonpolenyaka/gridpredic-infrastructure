---
# Metadatos según la especificación de Hugging Face para model cards:
# https://github.com/huggingface/hub-docs/blob/main/modelcard.md?plain=1
language:
  - es
license: other
license_name: propietaria
license_details: Modelo del TFM GridPredic. Entrenado con datos confidenciales de SITEL y de las distribuidoras; no se publica el modelo ni los datos.
library_name: xgboost
pipeline_tag: tabular-classification
tags:
  - energy
  - power-grid
  - scada
  - outage-prediction
  - time-series
  - xgboost
  - shap
datasets:
  - gridpredic-calser-tedisnet (local, ver docs/dataset_card.md)
metrics:
  - precision
  - recall
  - pr-auc
  - roc-auc
---

# Model Card for GridPredic - Predicción de interrupciones por CT (horizonte 1-3 h)

Clasificador binario que, para cada centro de transformación (CT) de media tensión y cada hora, estima la probabilidad de que en las siguientes 1 a 3 horas empiece una interrupción imprevista de suministro. Se entrena sobre la telemetría histórica del SCADA TedisNet y las interrupciones registradas en Calser (ver la [dataset card](dataset_card.md)). Es una herramienta de apoyo a la decisión del operador de la distribuidora: no actúa sobre la red.

Estado a 10.10.2026: el pipeline hasta Gold está cargado con la ventana completa (dataset `ds_20261010T045104_a5a2531cacbb`, 246 features, 1.287 CT) y el entrenamiento y la evaluación están implementados en `etl/jobs/04_ml` (ver [ml-training.md](ml-training.md)). Los apartados de resultados se rellenan con la primera ejecución completa, dentro del hito M7 del TFM (26.10.2026); los requisitos y la metodología de evaluación ya estaban fijados aquí para que el modelo se construya contra ellos y no al revés.

## Model Details

### Model Description

- **Developed by:** Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka (TFM, UPC School, Máster en Data Science and Engineering 2025-2026)
- **Funded by [optional]:** proyecto académico; SITEL Sistemas Electrónicos SA aporta los datos y el conocimiento del dominio
- **Shared by [optional]:** no se publica
- **Model type:** clasificación binaria supervisada sobre datos tabulares derivados de series temporales. Candidato principal XGBoost (gradient boosting sobre árboles); se compara con Random Forest y con una regresión logística como referencia interpretable. Explicabilidad post hoc con SHAP
- **Language(s) (NLP):** no aplica (datos numéricos y categóricos; documentación en castellano)
- **License:** propietaria
- **Finetuned from model [optional]:** no aplica; se entrena desde cero

### Model Sources [optional]

- **Repository:** https://github.com/antonpolenyaka/gridpredic-infrastructure
- **Paper [optional]:** memoria del TFM GridPredic (UPC School), en redacción
- **Demo [optional]:** no disponible

## Uses

### Direct Use

- Generar, cada hora, una lista de CT ordenada por probabilidad de interrupción en las próximas 1 a 3 horas, para que el centro de control de la distribuidora decida si reposiciona operarios o revisa una instalación.
- Explicar cada alerta con las variables que más pesan (SHAP): por ejemplo, intensidad homopolar creciente, defectos de tierra repetidos o errores de comunicaciones del equipo.
- Evaluar, sobre el histórico, qué fracción de las interrupciones habría tenido aviso previo y con cuánta antelación.

### Downstream Use [optional]

- Integración con el SCADA TedisNet como capa de alertas (fase de serving, fuera del alcance de este hito).
- Priorización de mantenimiento preventivo a partir de la probabilidad acumulada por CT.

### Out-of-Scope Use

- Actuación automática sobre la red (apertura o cierre de interruptores, reenganches). El operador decide siempre.
- Redes de baja tensión: el dataset no las cubre.
- Distribuidoras distintas de las tres del entrenamiento sin recalibrar y sin revisar la cobertura de telemetría.
- Cálculo o reporte oficial de índices de calidad (TIEPI, NIEPI): eso lo hace Calser.
- Cualquier decisión sobre clientes individuales: el grano del modelo es el CT.

## Bias, Risks, and Limitations

- **Desbalance extremo.** Alrededor de 2 filas positivas por cada 10.000 CT-hora. Un modelo que nunca avisa tiene una accuracy del 99,98 %, y por eso la accuracy no se usa como criterio.
- **Eventos sistémicos frente a locales.** En las dos distribuidoras grandes el 94 % de las interrupciones afectan a varios CT a la vez (temporales, fallos aguas arriba). El modelo puede acertar prediciendo "día malo" y fallar en el CT concreto. Las métricas se reportan por separado para eventos locales y sistémicos.
- **Etiquetas con retraso y huecos.** Entre un 26 y un 28 % de las interrupciones no tienen incidencia asociada, y la fecha de alta puede ser semanas posterior al evento. En producción habría una latencia de etiquetado similar, que afecta a la monitorización.
- **Telemetría parcial y cambiante.** Solo unos 4.000 tags tienen serie regular; hay huecos por caídas de RabbitMQ y valores congelados por el muestreo sample-and-hold. Un CT sin telemetría no puede tener alerta, y el modelo no debe interpretarse como "ese CT es seguro".
- **Metadatos actuales sobre eventos históricos.** La topología es la de hoy; el modelo puede aprender relaciones que ya no existen.
- **Sesgo de cobertura.** Tres distribuidoras rurales de Extremadura con el mismo SCADA. No hay evidencia de que generalice a otras redes.
- **Riesgo de uso.** Un exceso de falsas alarmas haría que el operador dejara de mirar las alertas; un exceso de confianza haría que dejara de vigilar los CT sin alerta. Ambos se tratan como requisitos (ver Evaluation).
- **Regulación.** Un sistema de IA aplicado a infraestructura crítica es de alto riesgo según el Reglamento (UE) 2024/1689 (AI Act). Esta ficha, la trazabilidad de versiones de datos y modelo, la explicabilidad y la supervisión humana forman parte de las obligaciones que asumimos desde el diseño.

### Recommendations

Usar el modelo como ordenación de prioridades, no como veredicto. Mantener siempre el umbral de alerta ligado a un presupuesto de falsas alarmas acordado con el centro de control. Registrar cada predicción con la versión del modelo y del dataset. Recalibrar cuando cambie el parque de telemetría o cuando el monitor de deriva detecte un cambio en la distribución de las features.

## How to Get Started with the Model

Cada ejecución de `etl/jobs/04_ml/job_ml_train.py` guarda los modelos en MinIO (`s3://datalake/ml/modelos/<run_id>/`) y deja su registro en `l3_gold.ml_runs` con la versión congelada del dataset de Gold (`l3_gold.dataset_versions`). Cada `.pkl` es un objeto con un método `score` que recibe la matriz de features en float32, en el orden de `run.json`, y devuelve la probabilidad ya corregida a la población real:

```python
import json, pickle, s3fs

# Desde un contenedor del stack (spark-master): MinIO no publica el puerto 9000 al anfitrión
fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "http://minio:9000"})
base = "datalake/ml/modelos/<run_id>"
modelo = pickle.load(fs.open(f"{base}/xgboost.pkl", "rb"))
features = json.load(fs.open(f"{base}/run.json"))["features"]

# filas: DataFrame de pandas leído de l3_gold.features_ct_hora
probabilidad = modelo.score(filas[features].to_numpy(dtype="float32", na_value=float("nan")))
```

Para entrenar, `l3_gold.dataset_train` trae la columna `split` y una muestra reproducible de negativos (`en_muestra_train`, `peso_muestra`). Cómo se lanza y qué tablas deja está en [ml-training.md](ml-training.md).

## Training Details

### Training Data

Tablas de Gold construidas desde Silver (ver [dataset_card.md](dataset_card.md) y [silver-layer.md](silver-layer.md)):

- `labels_ct_hora`: etiqueta `y_1_3h` por (CT, hora). Positivo si en `(hora + 1 h, hora + 3 h]` empieza una interrupción imprevista (`CL_IMPRE`), no atribuible al cliente (`FA_CLIEN`), de más de 180 segundos y a nivel de CT. Son los mismos filtros con los que Calser calcula el TIEPI. Variantes: otros horizontes (`y_0_1h`, `y_0_3h`, `y_0_6h`), solo eventos locales (`y_1_3h_local`), con las interrupciones sin incidencia (`y_1_3h_amplia`) y con los cortes del SCADA (`y_1_3h_scada`), y `en_corte` para excluir las horas en las que el CT ya está sin suministro.
- `features_ct_hora` (unas 250 columnas descritas en `feature_metadata`): medidas eléctricas agregadas por hora y por familia (intensidad, intensidad de neutro, tensión, potencias, factor de potencia, temperatura) del propio CT y de la posición de cabecera que lo alimenta, con desequilibrio entre fases, valores rancios y ventanas de 24 h; cambios de las señales precursoras en el CT, en su grupo de red y en la cabecera (defectos de tierra y de fase, paso de falta, disparos, falta de tensión, reenganches, seccionalizadores, comunicaciones, mando local, presencia de personal); alarmas y avisos del SCADA; lecturas con fallo de comunicaciones; cortes y microcortes del SCADA; histórico sin leakage (interrupciones conocidas en 30, 90 y 365 días, días desde la última, interrupciones del municipio); atributos del CT (potencia imputada, abonados, salidas, tipo de zona, coordenadas) y calendario (hora, día, mes, festivo, víspera). La meteorología entrará cuando esté ingerida. Cada fila usa solo datos que se podían conocer antes de su hora: los del SCADA por su hora de llegada al servidor, los de Calser desde su carga y la cabecera que alimenta a cada CT desde el primer corte que la reveló. El test de Gold lo comprueba recortando las fuentes y reconstruyendo.
- Ventana: 2021 a agosto de 2026, que es donde coinciden telemetría y etiquetas. Fuente: backups del 14.08.2026.

Quedan fuera, por leakage, `fecha_alta`, `ts`, las columnas `*_OPTIMIZADA`, las tablas `calculos_*` de Calser y el estado del interruptor del propio CT en la hora objetivo.

### Training Procedure

#### Preprocessing [optional]

La limpieza genérica (deduplicación, calidad, huérfanos, distribuidora) se hace en Silver y no se repite aquí. En Gold se decide lo que depende del modelo: fusión de solapes de interrupciones, imputación de la potencia del CT (como la hace Calser), winsorización de duraciones, ventanas de agregación y tratamiento de los valores congelados. Las mismas funciones de features se reutilizarán en streaming para que entrenamiento e inferencia calculen lo mismo.

#### Training Hyperparameters

- Algoritmo principal: XGBoost, objetivo `binary:logistic`, `tree_method = hist`, entrenado sobre la muestra de negativos de Gold (todos los positivos y el 2 % de los negativos) y con la probabilidad corregida después por el muestreo (corrección de prior, monótona). Parada temprana sobre la PR-AUC de una muestra ponderada de valid (2025). Búsqueda de hiperparámetros con una lista corta de configuraciones (profundidad 4 a 8, `min_child_weight` 1 y 20, `learning_rate` 0,05, `subsample` 0,8, `colsample_bytree` 0,5 a 0,6) elegidas sobre valid, nunca con validación cruzada aleatoria.
- Comparación: Random Forest (200 árboles, profundidad 14 a 20) y regresión logística L2 (C entre 0,01 y 1) sobre las mismas features y el mismo split, y las referencias de tasa base y naif histórico.
- Los valores de cada configuración probada y la elegida quedan en `l3_gold.ml_runs` (`busqueda_json`, `modelos_json`) y en `etl/config/04_ml/config_ml.json`. Se copiarán aquí con los resultados. [More Information Needed]

#### Speeds, Sizes, Times [optional]

[More Information Needed]. El entrenamiento previsto es local, sobre el dataset agregado de Gold (del orden de 65 millones de filas CT-hora antes de submuestrear), en el mismo portátil que ejecuta el stack.

## Evaluation

### Testing Data, Factors & Metrics

**Datos de prueba.** División temporal: entrenamiento hasta 2024, validación 2025, prueba 2026. Ningún dato posterior a la fecha de corte entra en el entrenamiento ni en la selección de hiperparámetros.

**Factores.** Los resultados se desglosan por distribuidora, por tipo de evento (local o sistémico), por tipo de zona del municipio (urbana, semiurbana, rural concentrada, rural dispersa) y por disponibilidad de telemetría del CT.

**Métricas y criterios de aceptación.** Son los requisitos de rendimiento del modelo, en el sentido de la ingeniería de requisitos para ML: si no se cumplen, el modelo no pasa a la fase de serving.

| Métrica | Por qué | Criterio |
| --- | --- | --- |
| PR-AUC (average precision) | Es la métrica principal con un 0,02 % de positivos; la accuracy y el ROC-AUC engañan | Debe superar claramente a dos referencias: la tasa base de positivos y un modelo naif que ordena los CT por su frecuencia histórica de fallos |
| Recall con presupuesto de alertas | El operador solo puede atender un número limitado de avisos por turno | Recall de los eventos locales medido con el umbral que produce como máximo N alertas por día y distribuidora (N se acuerda con el centro de control) |
| Precision en ese umbral | Evitar la fatiga de alertas | Se reporta junto al recall; no se acepta un umbral cuya precision haga que la mayoría de avisos sean falsos |
| Antelación media del aviso | El valor operativo está en llegar antes de los 180 s posteriores al corte | Se reporta la distribución de antelación (horas entre la primera alerta y el inicio de la interrupción) |
| ROC-AUC | Secundaria, para comparar con la literatura | Se reporta |
| Estabilidad por segmento | Evitar un modelo que solo funciona en una distribuidora | Los resultados se publican por segmento y no se ocultan los débiles |

### Results

[More Information Needed]. Se completará en el hito M7 con la tabla de resultados por modelo y por segmento, sacada de `l3_gold.ml_metricas`. La evaluación de test se corta el 01.06.2026 porque las etiquetas de las últimas semanas de la ventana están censuradas (interrupciones todavía sin cargar en Calser).

## Model Examination [optional]

Explicabilidad post hoc con SHAP: importancia global de las features y explicación local de cada alerta. Se comprobará que las variables con más peso tengan sentido físico (homopolar, THD, defectos de tierra, errores de comunicaciones) y que ninguna variable con leakage se haya colado. Cada ejecución guarda en `l3_gold.ml_importancia` la media del SHAP absoluto de XGBoost, la importancia de los otros dos modelos y la PR-AUC de cada feature sola; las que ordenan los positivos demasiado bien por sí solas quedan marcadas con `alerta_leakage` para revisarlas antes de aceptar el modelo.

## Environmental Impact

Entrenamiento local en un portátil (4 cores, 16 GB) sobre datos agregados; no se usa GPU ni nube. No se ha medido el consumo todavía. [More Information Needed]

## Technical Specifications [optional]

- Entrada: fila de `features_ct_hora` (un CT, una hora).
- Salida: probabilidad en [0, 1] y, aplicado el umbral acordado, alerta sí/no con su explicación SHAP.
- Dependencias: Python, xgboost (`xgboost-cpu` 3.0.5), scikit-learn 1.7.2, pandas y pyarrow, fijadas en `infra/spark/requirements.txt`. Para los gráficos SHAP de los notebooks, el paquete `shap`; MLflow queda pendiente.
- Latencia objetivo en serving: inferencia por debajo de un minuto por ciclo horario para todos los CT de una distribuidora.

## Citation [optional]

Morancho, J. y Shebarshinov, A. (2026). GridPredic: predicción de interrupciones de suministro en redes de distribución de media tensión con 1-3 horas de antelación. TFM, UPC School.

Formatos de documentación seguidos: Mitchell et al. (2019), "Model Cards for Model Reporting"; Gebru et al. (2021), "Datasheets for Datasets".

## Glossary [optional]

- **CT**: centro de transformación (media a baja tensión). Unidad de predicción.
- **Evento local / sistémico**: interrupción que afecta a un solo CT frente a varias a la vez por la misma incidencia.
- **PR-AUC**: área bajo la curva precision-recall.
- **SHAP**: valores de Shapley para atribuir la predicción a cada feature.

## More Information [optional]

- [dataset_card.md](dataset_card.md): datos, sesgos y versionado.
- [silver-layer.md](silver-layer.md): qué limpieza se hace antes de Gold y por qué.

## Model Card Authors [optional]

Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka.

## Model Card Contact

A través de los issues del repositorio o de los autores (UPC School, TFM GridPredic 2026).
