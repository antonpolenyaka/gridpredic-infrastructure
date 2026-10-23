---
# Metadatos según la especificación de Hugging Face para model cards:
# https://github.com/huggingface/hub-docs/blob/main/modelcard.md?plain=1
language:
  - es
license: other
license_name: propietaria
license_details: Modelo del TFM GridPredic. Entrenado con datos confidenciales de SITEL y de las distribuidoras; no se publica el modelo ni los datos.
library_name: sklearn
pipeline_tag: tabular-classification
tags:
  - energy
  - power-grid
  - scada
  - outage-prediction
  - time-series
  - logistic-regression
  - xgboost
  - shap
datasets:
  - gridpredic-calser-tedisnet (local, ver docs/dataset_card.md)
metrics:
  - pr-auc
  - brier
  - roc-auc
  - precision
  - recall
---

# Model Card for GridPredic - Riesgo de interrupción del día siguiente y aviso de 1 a 3 horas

Clasificadores binarios que estiman la probabilidad de que empiece una interrupción imprevista de suministro. Empezamos a 1 - 3 horas vista con dos unidades, cada centro de transformación (CT) de media tensión y hora y cada distribuidora y hora (horas de riesgo), y los resultados nos llevaron a una tercera, la distribuidora y el día siguiente: la señal se ve en el día y se pierde al bajar a la hora y al CT. El riesgo del día siguiente es el modelo que corre cada tarde; los otros dos se quedaron en evaluación. Los modelos por horas se entrenan con la telemetría histórica del SCADA TedisNet y las interrupciones registradas en Calser (ver la [dataset card](dataset_card.md)); el del día, con el calendario, las interrupciones de Calser, la meteorología y los avisos de AEMET, sin telemetría. Es una herramienta de apoyo a la decisión del operador de la distribuidora: no actúa sobre la red.

Estado en la versión 0.16.0 (la etiqueta v0.16.0 cierra el desarrollo antes de la entrega del 26.10.2026): pipeline hasta Gold cargado con la ventana completa (dataset `ds_20261010T045104_a5a2531cacbb`, 246 features, 1.287 CT), meteorología integrada, modelos entrenados y evaluados en `etl/jobs/04_ml` (ver [ml-training.md](ml-training.md)) y test usado una sola vez, con la configuración cerrada. Los requisitos y la metodología de evaluación se fijaron en esta ficha antes de entrenar; los resultados están en Results. Desde el 13.10.2026 Results recoge también el riesgo del día siguiente por distribuidora, con los avisos de AEMET y la previsión real de ICON-EU, y ese modelo corre cada tarde en Airflow (`dag_riesgo_manana`); desde el 15.10.2026 la misma ejecución da también la orientación de pasado mañana (`operativa_48h`).

## Model Details

### Model Description

- **Developed by:** Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka (TFM, UPC School, Máster en Data Science and Engineering 2025-2026)
- **Funded by [optional]:** proyecto académico; SITEL Sistemas Electrónicos SA aporta los datos y el conocimiento del dominio
- **Shared by [optional]:** no se publica
- **Model type:** clasificación binaria supervisada sobre datos tabulares derivados de series temporales. En producción, una regresión logística con regularización L2 de scikit-learn por distribuidora y día, explicada por sus coeficientes. En evaluación, por CT y hora y por distribuidora y hora, XGBoost (gradient boosting sobre árboles), Random Forest y regresión logística, comparados con referencias sin modelo: tasa base, histórico de interrupciones y la racha máxima de viento en la zona, con SHAP para explicar el modelo por horas. En el modelo del día se comparó además la logística L2 de producción con XGBoost, los bosques, LDA, QDA, Naive Bayes, k vecinos, SVM, el perceptrón y variantes de la logística (`experimentos/perfil_dia/familias_dia.py` y `comparar_modelos_dia.py`, cuaderno 3w): ninguno la supera
- **Language(s) (NLP):** no aplica al modelo (datos numéricos y categóricos; documentación en castellano). Sí hay texto detrás de algunas etiquetas: la causa de cada incidencia se lee con reglas del texto libre de Calser, y de ella dependen `y_dia_meteo`, las incidencias de causa local con que la tarde reparte el riesgo por líneas y se mide el caso de negocio, y las familias del riesgo anual; `y_dia`, la etiqueta del modelo de producción, no depende de ella (ver la [dataset card](dataset_card.md), Annotation process)
- **License:** propietaria
- **Finetuned from model [optional]:** no aplica; se entrena desde cero
- **Model date:** 13.10.2026, primera tarde en producción del modelo del día; desde entonces se reentrena cada tarde con todos los días etiquetados
- **Model version:** variante `operativa` de `etl/config/04_ml/config_ml.json` en la etiqueta v0.16.0 del repositorio (la versión que fija `CHANGELOG.md`); cada tarde deja su `run_id` y la configuración de su modelo en `l3_gold.ml_runs`, y desde la 0.16.0 también el modelo ajustado en MinIO (How to Get Started)

En resumen: en producción está el riesgo del día siguiente por distribuidora (regresión logística con la previsión real de ICON-EU de la víspera y los avisos de AEMET), que da cada tarde una probabilidad, un nivel por percentiles de su propia red y una recomendación de guardia. El modelo por CT y hora se evaluó (XGBoost dobla la tasa base) y no pasa a producción.

### Model Sources [optional]

- **Repository:** https://github.com/antonpolenyaka/gridpredic-infrastructure
- **Paper [optional]:** memoria del TFM GridPredic (UPC School), entregada el 26.10.2026 con la versión 0.16.0 de este repositorio; el capítulo 5 describe el modelo y el anexo A, sus variables
- **Demo [optional]:** https://gridpredic.com/demo/, el cuadro de mando con una copia congelada de sus datos (`web/demo/data`, las tres redes como Distribuidora 1, 2 y 3); la copia se renueva con `web/congelar_demo.ps1`

## Uses

### Direct Use

- Cada tarde, el riesgo del día siguiente de cada distribuidora (probabilidad, nivel, motivos y zonas con más racha prevista frente a su clima), para decidir la víspera cuántas brigadas dejar de guardia y dónde. Es el uso que está en marcha.
- La lista horaria de CT ordenada por probabilidad de interrupción en las próximas 1 a 3 horas era el uso previsto al empezar. La medimos y la descartamos como alerta: el modelo por CT y hora encuentra los días malos pero no la hora ni el CT, y la línea alertada se queda en un 1 - 3 % de probabilidad (ver Results). No está en producción.
- Explicar cada día de riesgo con sus motivos, que salen de los coeficientes de la regresión logística: la racha prevista frente a lo normal de cada celda, los avisos de AEMET, el calendario y las averías recientes. SHAP lo usamos para entender el modelo por horas (ver Model Examination).
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

- **Desbalance extremo y pocas incidencias.** Unas 3 filas positivas por cada 10.000 CT-hora (0,030 % en train). Además, la muestra efectiva es el número de incidencias, no de filas: las 4.258 horas positivas de valid por CT son 68 horas de distribuidora y 31 incidencias en todo 2025. Cualquier comparación entre modelos con un solo año tiene intervalos muy anchos. Un modelo que nunca avisa tiene una accuracy del 99,97 %, y por eso la accuracy no se usa como criterio.
- **Eventos sistémicos frente a locales.** En las dos distribuidoras grandes el 94 % de las interrupciones afectan a varios CT a la vez (temporales, fallos aguas arriba). El modelo puede acertar prediciendo "día malo" y fallar en el CT concreto. Las métricas se reportan por separado para eventos locales y sistémicos.
- **Etiquetas con retraso y huecos.** Entre un 26 y un 28 % de las interrupciones no tienen incidencia asociada, y la fecha de alta puede ser semanas posterior al evento. En producción habría una latencia de etiquetado similar, que afecta a la monitorización.
- **Telemetría parcial y cambiante.** Solo unos 4.000 tags tienen serie regular; hay huecos por caídas de RabbitMQ y valores congelados por el muestreo sample-and-hold. Un CT sin telemetría no puede tener alerta, y el modelo no debe interpretarse como "ese CT es seguro".
- **Metadatos actuales sobre eventos históricos.** La topología es la de hoy; el modelo puede aprender relaciones que ya no existen.
- **El riesgo de cada tarde depende de que lleguen los datos.** Sin la previsión de ICON-EU no se calcula; sin los avisos de AEMET se calcula como si no hubiera ninguno y la fila lo marca. La tarde se entrena hasta la última incidencia de la copia de Calser menos 45 días (`riesgo_manana.margen_etiquetas_dias`, 7 hasta el 17.10.2026), porque una interrupción recibe su incidencia cuando se procesa, semanas después, y el control de Gold mide entre 41 y 43 días de etiquetas sin cargar al final de cada copia en la Distribuidora 1 y la 2; el caso de negocio mantiene los 7 días con los que se publicaron sus cifras, así que sus últimas semanas pueden tener alguna avería de menos. Mientras la copia de Calser del lakehouse no se cargue cada día, la historia de incidencias de las últimas semanas no se conoce y la fila lo marca (`historia_al_dia`, con `historia_margen_dias`): el modelo de esa tarde se entrena con la historia y puntúa el día con la mediana de los días de entrenamiento en esas variables (`riesgo_manana.historia_desfasada`, `"mediana"`). Las variantes `operativa_historia_mediana` y `operativa_sin_historia` de `job_ml_riesgo_dia` midieron esa mezcla frente a `operativa` el 17.10.2026 (cuaderno 3v): la mediana no pierde nada y entrenar sin el bloque (`"sin_historia"`) cuesta en torno a 0,01 de PR-AUC. El reparto por zonas no cambia la probabilidad del día y sigue usando la historia de cada zona hasta donde llega la copia: con ella desfasada, la ventana de 30 días queda vacía y las de 365 y 1.095 días cuentan algo menos de lo que pasó. El tiempo del día anterior sale de ICON-EU y no del reanálisis con el que se entrenó.
- **Sesgo de cobertura.** Tres distribuidoras rurales de Extremadura con el mismo SCADA. No hay evidencia de que generalice a otras redes.
- **Riesgo de uso.** Un exceso de falsas alarmas haría que el operador dejara de mirar las alertas; un exceso de confianza haría que dejara de vigilar los CT sin alerta. Ambos se tratan como requisitos (ver Evaluation).
- **Regulación.** El anexo III del Reglamento (UE) 2024/1689 (AI Act) considera de alto riesgo los sistemas de IA que son componentes de seguridad en la gestión y la operación del suministro eléctrico. GridPredic no actúa sobre la red: prepara una decisión humana, la guardia del día siguiente, y en nuestra lectura no es un componente de seguridad ni un sistema de alto riesgo (memoria, 3.10.4). Ese análisis lo documentaremos antes del primer contrato (artículos 6.3 y 6.4), porque la clasificación cambiaría si el sistema actuara solo. Según varias fuentes secundarias concordantes, el Reglamento (UE) 2026/1744 (Digital Omnibus, DOUE del 24.07.2026) retrasa las obligaciones de alto riesgo del anexo III al 02.12.2027; no lo hemos podido leer en EUR-Lex. Esta ficha, la trazabilidad de versiones de datos y modelo, la explicabilidad y la supervisión humana se mantienen sea cual sea la clasificación.

### Recommendations

Recomendamos leer la salida como una ordenación de prioridades y dejar la decisión al jefe de guardia. El umbral de alerta debería ir ligado a un presupuesto de falsas alarmas acordado con el centro de control, y cada predicción queda registrada con su `run_id`. No tenemos un monitor de deriva: el modelo del día se reentrena cada tarde, y lo que nos avisaría de que se degrada es la verificación diaria y el caso de negocio repetido con cada copia de Calser.

## How to Get Started with the Model

El riesgo del día siguiente, que es el modelo de producción, no hay que cargarlo a mano: `dag_riesgo_manana` lo calcula cada tarde a las 20:30 con la previsión de ICON-EU y los avisos de AEMET de ese momento, vuelve a entrenar la regresión logística con todos los días etiquetados y deja una fila por distribuidora y día en `l3_gold.riesgo_dia_siguiente`, una por línea en `l3_gold.riesgo_zona_siguiente`, la orientación de pasado mañana en `l3_gold.riesgo_pasado_manana` (la variante de dos tardes antes, `operativa_48h`, con la misma previsión y los mismos avisos: 5,4 veces la tasa base frente a 6,5 la víspera con las mismas variables en 2024 - 2025; [investigacion-continua.md](investigacion-continua.md), 3e) y el JSON del cuadro de mando en `dashboard/data/` (http://localhost:8086; ver [causas-incidencias.md](causas-incidencias.md), apartados 8 y 10):

```sql
SELECT fecha, distribuidora, nivel, ROUND(probabilidad, 3) AS probabilidad, ROUND(veces_tasa_base, 1) AS veces,
       motivos, zonas, avisos_disponibles, historia_al_dia, params_json, run_id
FROM lakehouse.l3_gold.riesgo_dia_siguiente
ORDER BY fecha DESC, probabilidad DESC;
```

Desde la 0.16.0 la tarde guarda además el modelo que ha ajustado, como los entrenamientos por CT: `logistica.pkl` y `run.json` en `s3://datalake/ml/modelos/<run_id>/`, con el `run_id` de su fila de `riesgo_dia_siguiente`, y la carpeta queda en `artefactos_uri` de `l3_gold.ml_runs`. `run.json` lleva la variante, las variables en el orden en que las espera el modelo, la C elegida, la versión Delta de las ocho tablas que leyó la tarde y el entorno con que se ajustó (Python y las versiones de numpy, scipy, scikit-learn, xgboost y pandas). Lo probamos el 20.10.2026 con una tarde repetida sobre el 28.01.2026 (`prueba_c12_20260128`), que todavía guardaba siete: faltaba `f_incidencia_causa`. Para ver el modelo de una tarde y sus coeficientes:

```python
import json, pickle, sys
import s3fs

sys.path.insert(0, "/app/jobs/04_ml")  # el .pkl es un ml_models.EstimatorScorer
import ml_models

# Desde un contenedor del stack (spark-master): MinIO no publica el puerto 9000 al anfitrión
fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "http://minio:9000"})
base = "datalake/ml/modelos/<run_id>"  # el run_id de la fila de l3_gold.riesgo_dia_siguiente
modelo = pickle.load(fs.open(f"{base}/logistica.pkl", "rb"))
run = json.load(fs.open(f"{base}/run.json"))

print(run["variante"], run["modelos"]["logistica"]["params"], run["versiones_delta"])
coeficientes = ml_models.linear_terms(modelo)  # una fila por variable: coeficiente y razón de odds por desviación típica
```

`modelo.score` recibe la matriz de las variables de `run["features"]` en float32 y devuelve la probabilidad del día. Las filas no se guardan como tabla: las construye la propia tarde con lo que se sabe a las 20:30 (ver la [dataset card](dataset_card.md), el conjunto del riesgo del día siguiente).

El modelo por CT y hora, evaluado y fuera de producción, se carga igual. Cada ejecución de `etl/jobs/04_ml/job_ml_train.py` guarda los modelos en MinIO (`s3://datalake/ml/modelos/<run_id>/`) y deja su registro en `l3_gold.ml_runs` con la versión congelada del dataset de Gold (`l3_gold.dataset_versions`). Cada `.pkl` tiene el mismo método `score`, que recibe la matriz de features en float32, en el orden de `run.json`, y devuelve la probabilidad ya corregida a la población real:

```python
import json, pickle, sys
import s3fs

sys.path.insert(0, "/app/jobs/04_ml")
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
- `features_ct_hora` (unas 250 columnas descritas en `feature_metadata`): medidas eléctricas agregadas por hora y por familia (intensidad, intensidad de neutro, tensión, potencias, factor de potencia, temperatura) del propio CT y de la posición de cabecera que lo alimenta, con desequilibrio entre fases, valores rancios y ventanas de 24 h; cambios de las señales precursoras en el CT, en su grupo de red y en la cabecera (defectos de tierra y de fase, paso de falta, disparos, falta de tensión, reenganches, seccionalizadores, comunicaciones, mando local, presencia de personal); alarmas y avisos del SCADA; lecturas con fallo de comunicaciones; cortes y microcortes del SCADA; histórico sin leakage (interrupciones conocidas en 30, 90 y 365 días, días desde la última, interrupciones del municipio); atributos del CT (potencia imputada, abonados, salidas, tipo de zona, coordenadas) y calendario (hora, día, mes, festivo, víspera). La meteorología horaria de Open-Meteo (ECMWF IFS) entra en el paso de entrenamiento (ver [ml-training.md](ml-training.md)). Cada fila usa solo datos que se podían conocer antes de su hora: los del SCADA por su hora de llegada al servidor, los de Calser desde su carga y la cabecera que alimenta a cada CT desde el primer corte que la reveló. El test de Gold lo comprueba recortando las fuentes y reconstruyendo.
- Ventana: 2021 a agosto de 2026, que es donde coinciden telemetría y etiquetas. Fuente: backups del 14.08.2026.

Quedan fuera, por leakage, `fecha_alta`, `ts`, las columnas `*_OPTIMIZADA`, las tablas `calculos_*` de Calser y el estado del interruptor del propio CT en la hora objetivo.

### Training Procedure

#### Preprocessing [optional]

La limpieza genérica (deduplicación, calidad, huérfanos, distribuidora) se hace en Silver y no se repite aquí. En Gold se decide lo que depende del modelo: fusión de solapes de interrupciones, imputación de la potencia del CT (como la hace Calser), winsorización de duraciones, ventanas de agregación y tratamiento de los valores congelados. Pensábamos reutilizar las mismas funciones de features en streaming, para que entrenamiento e inferencia calcularan lo mismo; como el modelo por CT y hora no pasó a producción, esa parte no la construimos.

#### Training Hyperparameters

- Modelo del día, en producción: regresión logística con regularización L2 de scikit-learn (`lbfgs`) sobre las 35 variables de la variante `operativa`, dentro de una tubería que imputa la mediana con un indicador de hueco, pasa las variables a logaritmo con signo y las estandariza (`ml_models.build_estimator`). Cada tarde elige C entre 0,003, 0,01, 0,03 y 0,1 con la PR-AUC del último 25 % de los días etiquetados y vuelve a ajustar la elegida con todos (`job_ml_riesgo_manana.fit`, con la rejilla de `job_ml_riesgo_dia.py`). En la evaluación por años esa validación interna elige 0,003, la más fuerte de la rejilla, en 2022, 2023, 2024 y 2025, y su PR-AUC baja a medida que se relaja C. Ampliar la rejilla hacia abajo (0,0003 y 0,001) no ayuda: la validación interna pasa a elegir 0,0003 tres años de cuatro y la PR-AUC baja de 0,188 a 0,156 con la previsión real de 2024 - 2025 y de 0,151 a 0,114 sobre el reanálisis de 2022 - 2025 (`experimentos/perfil_dia/salida/familias/`, `familias_busqueda.csv` y `familias.csv`). La C de cada tarde queda en `params_json` de `riesgo_dia_siguiente` y en `busqueda_json` de `ml_runs`.
- Modelo por CT y hora, evaluado y fuera de producción: XGBoost, objetivo `binary:logistic`, `tree_method = hist`, entrenado sobre la muestra de negativos de Gold (todos los positivos y el 2 % de los negativos) y con la probabilidad corregida después por el muestreo (corrección de prior, monótona). Parada temprana sobre la PR-AUC de una muestra ponderada de valid (2025). Búsqueda de hiperparámetros con una lista corta de configuraciones (profundidad 4 a 8, `min_child_weight` 1 y 20, `learning_rate` 0,05, `subsample` 0,8, `colsample_bytree` 0,5 a 0,6) elegidas sobre valid, nunca con validación cruzada aleatoria.
- Comparación en el modelo por CT: Random Forest (200 árboles, profundidad 14 a 20) y regresión logística L2 (C entre 0,01 y 1) sobre las mismas features y el mismo split, y las referencias de tasa base y naif histórico.
- Los valores de cada configuración probada y la elegida quedan en `l3_gold.ml_runs` (`busqueda_json`, `modelos_json`) y en `etl/config/04_ml/config_ml.json`. En las horas de riesgo la configuración se elige en cada año de la evaluación con el último 25 % de las horas anteriores y el modelo se reentrena con todas; XGBoost conserva el número de árboles del early stopping.

#### Speeds, Sizes, Times [optional]

En el portátil del stack (12 CPU lógicas, 32 GB): el modelo del día se reentrena y puntúa cada tarde en unos 5 minutos (apartado Technical Specifications). El modelo por CT entrena sobre una muestra de 884.971 filas (todos los positivos de train y el 2 % de los negativos), y la ejecución completa con búsqueda del 11.10.2026 tardó 43 min 15 s: 15 min 22 s de búsqueda, con nueve configuraciones de la logística, el bosque y XGBoost, y 455 s para puntuar los 11,2 millones de filas de valid. El dataset de horas de riesgo (144.962 filas) se construye en 18 minutos una vez por versión del dataset y la evaluación por años de cuatro modelos con búsqueda tarda 9 minutos. Un modelo XGBoost guardado ocupa unos pocos MB.

## Evaluation

### Testing Data, Factors & Metrics

**Datos de prueba.** División temporal: entrenamiento hasta 2024, validación 2025, prueba 2026 hasta el 01.06.2026 (censura de las etiquetas al final de la ventana). Ningún dato posterior a la fecha de corte entra en el entrenamiento ni en la selección de hiperparámetros. Para las horas de riesgo se evalúan además 2023, 2024 y 2025 por separado, cada año con un modelo que solo ha visto los anteriores, y los intervalos de confianza salen de un bootstrap por días con réplicas emparejadas entre modelos. En el modelo del día, desde la 0.16.0 el bootstrap sortea bloques de siete días seguidos con todas las redes de cada fecha (`meteo_dia.bootstrap_bloque_dias`), porque con la previsión real los días seguidos no son independientes: en el residuo de la víspera el contraste de Ljung y Box da p = 0,008 con la chi cuadrado, 0,019 con la referencia simulada y 0,49 con el residuo de Pearson, así que lo que queda está en los temporales de varios días (`experimentos/evaluacion/salida/autocorrelacion.csv`). Las ejecuciones `dia_*` del 12 al 16.10.2026 que cita Results se midieron por fechas. En las tablas, la parte de las réplicas en la que gana un modelo no es la probabilidad de que sea mejor.

**Factores.** Los resultados se desglosan por distribuidora, por tipo de evento (local o sistémico), por tipo de zona del municipio (urbana, semiurbana, rural concentrada, rural dispersa) y por disponibilidad de telemetría del CT.

**Métricas y criterios de aceptación.** Son los requisitos de rendimiento del modelo, en el sentido de la ingeniería de requisitos para ML: si no se cumplen, el modelo no pasa a la fase de serving.

| Métrica | Por qué | Criterio |
| --- | --- | --- |
| PR-AUC (average precision) | Es la métrica principal con un 0,03 % de positivos; la accuracy y el ROC-AUC engañan | Debe superar claramente a dos referencias: la tasa base de positivos y un modelo naif que ordena los CT por su frecuencia histórica de fallos |
| Recall con presupuesto de alertas | El operador solo puede atender un número limitado de avisos por turno | Recall de los eventos locales medido con el umbral que produce como máximo N alertas por día y distribuidora (N se acuerda con el centro de control) |
| Precision en ese umbral | Evitar la fatiga de alertas | Se reporta junto al recall; no se acepta un umbral cuya precision haga que la mayoría de avisos sean falsos |
| Antelación media del aviso | El valor operativo está en llegar antes de los 180 s posteriores al corte | Se reporta la distribución de antelación (horas entre la primera alerta y el inicio de la interrupción) |
| ROC-AUC | Secundaria, para comparar con la literatura | Se reporta |
| Estabilidad por segmento | Que el modelo no funcione solo en una distribuidora | Publicamos los resultados de cada segmento, también los de los segmentos en los que sale peor |

**Umbrales de decisión del modelo en producción.** El nivel de cada tarde sale de dónde cae la probabilidad entre los días de entrenamiento de la misma red: medio desde el percentil 75, alto desde el 90 y muy alto desde el 95. Alerta es alto o muy alto. La guardia reforzada se recomienda con alerta o con un aviso de AEMET de viento o tormenta para el día. Con la previsión real de 2024 - 2026, la alerta acierta en 13 de cada 100 días, recoge el 41 % de los días con avería y deja en alerta el 11 % de los días tranquilos.

### Results

**Por CT y hora (valid 2025, 500 réplicas por días).** Tasa base 0,00038. XGBoost llega a 0,00077 [0,00016 - 0,00183] en la configuración básica y su ventaja sobre la tasa base es significativa (99,8 % de las réplicas); ni el estado agregado de la red, ni los grupos de CT por k-means, ni la meteorología lo mejoran de forma demostrable, y los submodelos por grupo nunca superan al modelo global. El modelo encuentra "días malos" pero no el CT concreto.

**Horas de riesgo por distribuidora, sin previsión meteorológica.** Evaluación por años 2023 - 2025 (104 incidencias), PR-AUC con intervalo del 95 %:

| Modelo | PR-AUC | Lift | Réplicas en las que gana a la racha |
| --- | --- | --- | --- |
| Racha máxima observada en la zona (referencia) | 0,0094 [0,0057 - 0,0173] | 3,2 | - |
| Regresión logística | 0,0084 [0,0056 - 0,0127] | 2,8 | 28 % |
| XGBoost | 0,0078 [0,0052 - 0,0128] | 2,6 | 22 % |
| Random Forest | 0,0049 [0,0033 - 0,0082] | 1,6 | 0,6 % |
| Tasa base | 0,0028 | 1,0 | - |
| Histórico de interrupciones | 0,0023 | 0,8 | - |

**Test (01.01 - 01.06.2026, 21 incidencias, usado una sola vez, `hr_20261011T100606_7391f5`):**

| Modelo | PR-AUC | Lift | Réplicas en las que gana a la tasa base | Réplicas en las que gana a la racha | recall_episodios con 2 h de alerta al día |
| --- | --- | --- | --- | --- | --- |
| Random Forest | 0,0319 [0,0050 - 0,1386] | 7,2 | 100 % | 89 % | 0,143 |
| XGBoost | 0,0172 [0,0037 - 0,0916] | 3,9 | 100 % | 65 % | 0,238 |
| Regresión logística | 0,0164 [0,0036 - 0,0488] | 3,7 | 100 % | 61 % | 0,333 |
| Racha máxima observada | 0,0140 [0,0044 - 0,0398] | 3,2 | - | - | 0,190 |
| Tasa base | 0,0044 | 1,0 | - | - | 0,286 |

Frente a los criterios de aceptación de esta ficha, el de la PR-AUC se cumple, porque los modelos superan con claridad a la tasa base y al naif histórico, aunque no a la regla de la racha máxima, que no necesita modelo. El de las alertas no se cumple: con un presupuesto fijo de horas de alerta al día ningún modelo avisa de más incidencias que el azar. Con la previsión perfecta de las 3 horas siguientes (el reanálisis, como cota superior), Random Forest llega a 6 veces la prevalencia en 2023 - 2025 y supera a la racha observada, aunque no a la racha prevista (0,0137); eso nos llevó a buscar el valor en una previsión meteorológica real.

**Fuentes, grupos y zonas** (evaluación por años, `hr_20261011T131024_10b6e9`). La meteorología sola es la fuente que más aporta (XGBoost 0,0102, regresión logística 0,0111); los datos eléctricos solos tienen señal pero menos (TedisNet 0,0061, Calser 0,0044). Mezclar las fuentes tal cual no mejora a la meteorología sola, y con interacciones explícitas entre tiempo y estado de la red la mezcla vuelve a su nivel (0,0101) con el comportamiento más estable año a año. Ni un modelo por distribuidora (0,0081) ni un modelo por zona meteorológica (0,0047) mejoran al modelo global. Ninguna variante se distingue con claridad de la regla de la racha máxima.

**Cuántos datos harían falta** (`hr_20261011T180132_a6b4e6`). Con el 25 %, el 50 % y el 100 % de los días por distribuidora de entrenamiento, la regresión logística con la meteorología pasa de 0,0097 a 0,0101 y a 0,0115, y XGBoost con las tres fuentes de 0,0066 a 0,0083 y a 0,0095: las dos curvas siguen subiendo y la del modelo complejo más deprisa, aunque con tres puntos la recta que las prolonga tiene un solo grado de libertad y, con diez veces más datos, su intervalo de predicción incluye la regla de la racha. La logística gana hoy a la racha en el 84 % de las réplicas; si la diferencia se mantuviera, con 3,7 veces más datos quedaría justo en el umbral del 95 %, con una potencia del 50 %, y para detectarla el 80 % de las veces harían falta 8,5 veces más, unos 76 años-distribuidora frente a los 9 actuales. Un modelo entrenado con una distribuidora funciona en las otras (la Distribuidora 1 -> la Distribuidora 2 0,0145, mejor que el propio de la Distribuidora 2, 0,0082), así que juntar distribuidoras ayuda a todas. Ver [network-analysis.md](network-analysis.md).

**Riesgo del año siguiente por CT y cabecera** (`job_ml_riesgo_ct.py`, `rct_20261011T215018_d996ba`, años 2021 - 2025; ejecución anterior, la vigente es `rct_20261020T093058_18e89c` y sus cifras están en docs/cifras.md). XGBoost Tweedie sobre los kVA·h imprevistos de cada CT y año, con lo que se sabía al empezar el año. Por cabecera, el 10 % que señala el modelo con historia y estructura recoge el 70,9 % de los kVA·h del año siguiente, frente al 63,5 % de ordenar por los dos años anteriores y al 80,7 % del máximo posible, y gana en cuatro de los cinco años; con el 20 % y el SCADA llega al 82,9 % (74,3 % la lista naif). Por CT apenas mejora a la lista naif (30,9 % frente a 27,9 % con el clima). Ni los datos públicos ni el bloque de obras y edificación (PLACSP y Catastro) aportan a un año vista. Las obras públicas por municipio y año tampoco anticipan las averías de terceros: comparando cada municipio consigo mismo, la razón de tasas es 0,66 (intervalo del 90 %, de 0,51 a 1,08). Ver [network-analysis.md](network-analysis.md).

**Riesgo del día siguiente por distribuidora** (`job_ml_riesgo_dia.py`, `dia_ext_20261013_0332`, `dia_det_20261013_0353` y `dia_operativa_20261013_1005`). Con lo que se sabe la tarde anterior: calendario, historia de Calser, tiempo del día anterior, avisos de AEMET enviados antes de las 20:00 y la previsión de ICON-EU para el día siguiente con la racha frente al clima de cada celda. Cada año lo puntúa un modelo entrenado con el reanálisis de los años anteriores; 2024 y 2025 se puntúan también con la previsión real (58 días con avería, tasa base 0,027):

| Modelo | PR-AUC con la previsión real | Lift | Días con avería en el 10 % de días de más riesgo | En el 20 % |
| --- | --- | --- | --- | --- |
| Regresión logística con avisos y extremos | 0,191 | 7,0 | 52 % | 72 % |
| La misma con solo la racha máxima y la lluvia de ayer (`operativa`, la que corre cada tarde) | 0,188 | 6,9 | 53 % | 67 % |
| Logística y XGBoost por percentiles, con avisos y extremos | 0,198 | 7,3 | 47 % | 71 % |
| Regresión logística con avisos, sin extremos | 0,170 | 6,2 | 52 % | 69 % |
| Racha máxima prevista (referencia) | 0,105 | 3,9 | 41 % | 57 % |
| Solo el tiempo de ayer, regresión logística | 0,070 | 2,6 | 22 % | 45 % |

Con el reanálisis en 2022 - 2025 (137 días con avería) la logística con avisos y extremos llega a 0,158. Contra los criterios de aceptación: supera con claridad a la tasa base y a la regla de la racha prevista, y al modelo que solo sabe el tiempo de ayer con el intervalo entero por encima de cero (+0,108 con la previsión llevada por cuantiles, de 0,025 a 0,214; es un diagnóstico, no el contraste operativo, que con el reanálisis 2022 - 2025 da +0,087, de 0,032 a 0,161). Pasar del reanálisis a la previsión real apenas cuesta (de 0,204 a 0,191 en esos dos años, sin diferencia que se distinga). Con dos tardes de antelación, la variante con solo la racha y la lluvia previstas baja a 0,147 (5,4 veces la tasa base, frente a 6,5 la víspera con las mismas variables; `dia_48h_20261014_1854`).

**Test 2026 de las horas de riesgo con la historia larga** (01.01 - 01.07.2026, 24 episodios, tasa base 0,0042, usado una sola vez, `mh_test2026_20261013_0442`). La regresión logística con los avisos de AEMET llega a 0,056 (13 veces la tasa base) y la combinación por percentiles a 0,032, frente a 0,030 sin avisos y 0,014 de la regla de la racha; la combinación supera a la regla en el 93 % de las réplicas.

**Test 2026 del riesgo del día siguiente** (01.01 - 01.07.2026, 24 días con avería, usado una sola vez, `dia_test2026_20261013_0428`). Con la previsión real, la logística con avisos y extremos da 0,132 (3,1 veces la tasa base de 0,044) y el 20 % de días de más riesgo recoge el 52 % de los días con avería, pero no se distingue del modelo que solo sabe el tiempo de ayer (0,164), que este año acierta más de lo habitual porque los temporales de enero llegaron seguidos. Con solo 24 días con avería, el test no separa las variantes entre sí. Después de esta prueba, 2026 entró en la evaluación por años como un año más: con los días hasta el 02.08.2026 (84 días con avería), la variante operativa da 0,161 de PR-AUC, 5,3 veces la tasa base (`dia_2026_20261015_1119`). Ver [causas-incidencias.md](causas-incidencias.md), apartado 8.

**Por líneas, municipios y CT, y la hora siguiente** (`riesgo_zonas.py`, 13.10.2026). Dentro del día, la línea que falla (grupo de red de TedisNet, 70 líneas) queda entre las tres de más riesgo de su distribuidora en el 15 - 18 % de las incidencias de 2022 - 2025 (al azar, el 9 %) y el tamaño de la línea lo hace igual de bien que cualquier modelo; el área que falla (la red en cuatro áreas, por k-means sobre las coordenadas de los municipios) está entre las dos de más riesgo en el 62 % de los casos y en el 79 % de los días de viento (42 casos), cuando al azar sería el 50 %, dos áreas de cuatro; es un cálculo de la exploración que no tiene guion en el repositorio. Por línea y hora, con el tiempo observado y la cascada, la PR-AUC es 19 veces la tasa base de 0,041 % y alertar el 1 % de las línea-horas recoge el 23 - 24 % de las incidencias de las 1 - 3 horas siguientes con una precisión del 0,9 %, y por distribuidora y hora, el 3,9 % de las horas recoge el 24 % con una precisión del 2,2 %. Tres de cada cinco incidencias por meteorología empiezan a menos de 3 horas del pico de racha del día; tras una incidencia, otra empieza en 3 horas en el 11 % de los casos (30 % si la primera fue por meteorología). Ver [causas-incidencias.md](causas-incidencias.md), apartado 8.

**En operación, día a día (`job_ml_caso_negocio.py`, `caso_v10_20261014_1543`, 14.10.2026).** La variante `operativa` reentrenada cada tarde solo con los años anteriores, sobre la Distribuidora 1 y la Distribuidora 2 de 2022 al 02.08.2026 (3.348 días de distribuidora, 143 con incidencia de causa local). Con la previsión real de ICON-EU de la víspera (2024 - 2026, 1.844 días): un día muy alto tiene avería el 17,9 % de las veces y un día bajo el 1,9 % (9 veces); la probabilidad está calibrada (muy alto: dice 15,5 % y pasa 17,9 %; bajo: 2,7 % y 1,9 %); AUC 0,72 (la Distribuidora 1 0,76, la Distribuidora 2 0,66) y Brier 0,036 frente a 0,038 de la tasa base (con las tres redes, BSS de 0,052 con intervalo de 0,003 a 0,096 y pendiente de calibración de 1,27: las probabilidades son algo tímidas; la Distribuidora 3 recibe un 2 % de media y en 922 días no tuvo ninguna avería de causa local, y con la etiqueta del modelo, 9 días con interrupción frente a 18,9 esperados; el valor según el coste de reforzar está en el apartado 2c de [caso-de-negocio.md](caso-de-negocio.md)); los días en alerta (alto o muy alto, el 12,5 % de los días) concentran el 72 % de las horas-cliente perdidas por causas locales (46 % sin el temporal del 28.01.2026), detectan el 41 % de los días con incidencia y el 71 % de los días con dos o más con un 11 - 12 % de falsas alarmas, y, a partir de unos 16 días al año, cubren más horas-cliente que los avisos de viento y que los naranjas o rojos con los mismos días, aunque no superan a la regla de viento o tormenta. Dentro de los días de alerta, el 84 % de las incidencias empiezan en la franja horaria prevista y la línea que falla está entre las 3 primeras del ranking el 25 % de las veces (azar 9 %). Las variables probadas después y descartadas (humedad del suelo, lluvia acumulada, factor de racha, horas sobre el percentil 90 local, umbral por estación) están en [investigacion-continua.md](investigacion-continua.md); el detalle, en [caso-de-negocio.md](caso-de-negocio.md).

**Con más antelación y con más fuentes (14 a 16.10.2026).** Con la previsión real de 2024 - 2025 (58 días con avería), la misma variante puntuada con lo que se sabía dos y tres tardes antes da 5,4 y 5,2 veces la tasa base (6,5 la víspera con las mismas variables y 6,9 con todas; 2,6 sabiendo solo el tiempo de ayer); el 20 % de días de más riesgo recoge el 66 % y el 64 % de los días con avería (67 % la víspera) y el 10 %, el 43 % y el 40 % (53 %): entre 48 y 72 h no hay diferencia medible, y lo que se pierde desde la víspera es a partes iguales la previsión a dos días y lo que esa tarde aún no se sabe, sobre todo los avisos de AEMET (cuaderno 3e). Nada de lo probado a partir de la revisión de literatura cumple la regla de entrada (subir la captura con el intervalo de la diferencia por encima de cero). Frente al 0,188 de PR-AUC de la variante de producción en 2024 - 2025: la extensión del viento fuerte, las interacciones de la racha con la lluvia, la hoja y el verano, una pendiente por red y la dirección del viento (3f, 0,177 - 0,187); un modelo de conteos de Poisson y una logística equilibrada (3g, 0,127 y 0,158); la sequía de 90 y 180 días y la precipitación helada (3i, 0,178 y 0,179); un segundo modelo de previsión o la media de ICON-EU, GFS y GEM (3j, 0,172 - 0,178), y el verdor de la red medido con el NDVI de MODIS (3k, 0,172 en crudo, igual por cuantiles). Con 2026 dentro (3m, 84 días con avería) el orden no cambia, y con el UKMO de 10 km tampoco aparece un modelo de previsión mejor que ICON-EU. Las pruebas que vinieron después se midieron con la previsión real de 2024 - 2026, donde la variante de producción da 0,156: la densidad de copa y el tipo de hoja del HRL de Copernicus (3l, 0,151), los árboles en el corredor de las líneas de OpenStreetMap (3n, 0,158, con una diferencia emparejada de -0,001), el régimen del tiempo (3o, 0,143), las frondosas caducas del Mapa Forestal (3p, 0,152) y la NAO diaria de NOAA (3r, 0,153). La NAO pareció mejorar sobre el reanálisis con el valor de D - 2, pero con el retraso real de publicación (D - 3) la mejora desaparece; la tarde la sigue descargando, fuera del modelo. Lo que más se acerca es una curva de fragilidad ajustada con 2017 - 2021 (3o, 0,158, +0,003), sola o con la NAO de D - 3 (+0,013 sobre el reanálisis y +0,007 con la previsión real), siempre sin subir la captura: queda como candidata para 2027. En el modelo anual por CT, el arbolado por celda (WorldCover y altura del dosel de Meta y WRI) solo da un indicio (3h), que desaparece al medir los árboles en el corredor (3n). En el modelo anual de fauna, las líneas que la Junta declaró peligrosas para las aves y las aves grandes del Inventario Español de Especies Terrestres suben la captura por CT, pero no llegan a la historia de fauna de cada CT (3q). Todo queda en el código para volver a medirlo con más años.

**Lectura.** El modelo por horas reconoce los días y las zonas de riesgo con unas horas de antelación, apoyado en la meteorología y en el estado de la red aguas arriba, pero no acierta la hora ni el CT de la interrupción. No recomendamos pasarlo a serving como alerta horaria por CT ni por línea: con lo que se sabe en cada hora, la línea alertada no pasa del 1 - 3 % de probabilidad. La víspera, en cambio, el riesgo del día siguiente con la previsión de ICON-EU y los avisos de AEMET sí sirve para planificar las brigadas, y es el que hemos llevado a serving: `dag_riesgo_manana` lo calcula cada tarde, con los motivos de cada día, y desde la 0.11.0 reparte ese riesgo entre las líneas, los municipios y los CT por su exposición, su historia y el tiempo previsto de sus celdas (sin cambiar la probabilidad del día, que es la validada), da la franja horaria del pico de racha, el reparto de causas de los días como mañana, las incidencias, CT y clientes esperados, las bases que minimizan la llegada de las brigadas y la regla de la cascada, y lo deja en `l3_gold.riesgo_zona_siguiente` y en el cuadro de mando sobre OpenStreetMap, donde `dag_rayos_vivo` añade cada 15 minutos los rayos que ve el satélite MTG sobre la red, la señal más directa de la hora siguiente. A un año vista, la lista de cabeceras es útil para decidir dónde revisar, podar o poner telemando.

## Model Examination [optional]

En producción la explicación son los coeficientes de la regresión logística. `ml_models.linear_terms` los da en la escala que ve el modelo, después de la imputación, el logaritmo y la estandarización, de modo que cada uno se lee como una razón de odds por desviación típica, y la tarde deja los de su modelo en `coeficientes.csv` junto a su informe y dentro del `logistica.pkl` que guarda en MinIO. Los motivos que lee cada tarde el jefe de brigadas suman las contribuciones de cada grupo de variables (viento, lluvia, avisos, historia) en lugar de leer coeficientes sueltos, porque las variables que se mueven juntas se reparten el efecto, y las frases las escribe el parte con plantillas fijas.

En el modelo por horas, que no está en producción, usamos SHAP: importancia global de las features y explicación local de cada alerta. En las horas de riesgo pesan sobre todo la lluvia y las rachas (de la última hora a 24 h), los reenganches aguas arriba en 6 y 24 h, las faltas de tensión y los defectos a tierra en el grupo de red, los microcortes y cortes del SCADA, los cambios de comunicaciones y la hora del día. Ninguna feature supera el umbral de alerta de leakage (PR-AUC sola de 0,3). Cada ejecución guarda en `l3_gold.ml_importancia` la media del SHAP absoluto de XGBoost, la importancia de los otros dos modelos y la PR-AUC de cada feature sola; las que ordenan los positivos demasiado bien por sí solas quedan marcadas con `alerta_leakage` para revisarlas antes de aceptar el modelo.

## Environmental Impact

Entrenamiento local en un portátil (12 CPU lógicas, 32 GB) sobre datos agregados; no se usa GPU ni nube. No hemos medido el consumo eléctrico; cada entrenamiento completo dura de 9 a 43 minutos.

## Technical Specifications [optional]

- Modelo del día (en producción). Entrada: una fila por distribuidora y día con lo que se sabe a las 20:30 de la víspera (calendario, historia de Calser, tiempo de ayer, avisos de AEMET y la previsión de ICON-EU para el día). Salida: probabilidad, nivel, motivos, franja horaria, guardia recomendada y su reparto por líneas, municipios y CT; desde la 0.16.0, también el modelo ajustado (`logistica.pkl` y `run.json` en `datalake/ml/modelos/<run_id>/`). Tiempo: entre 4 min 33 s y 5 min 38 s la puntuación, reentreno incluido, y unos 15 minutos la tarde completa.
- Modelo por CT y hora (evaluado, no en producción). Entrada: fila de `features_ct_hora` (un CT, una hora). Salida: probabilidad en [0, 1] y, aplicado el umbral acordado, alerta sí/no con su explicación SHAP.
- Dependencias: Python, xgboost (`xgboost-cpu` 3.0.5), scikit-learn 1.7.2, pandas y pyarrow, fijadas en `infra/spark/requirements.txt`. Para los gráficos SHAP de los notebooks, el paquete `shap`; MLflow queda pendiente.
- Latencia objetivo del modelo por CT y hora, si se sirviera: inferencia por debajo de un minuto por ciclo horario para todos los CT de una distribuidora.

## Citation [optional]

Morancho, J. y Shebarshinov, A. (2026). GridPredic: predicción en tiempo real de interrupciones eléctricas para la optimización de índices de calidad (TIEPI/NIEPI) en distribuidoras eléctricas. TFM, UPC School.

Formatos de documentación seguidos: Mitchell et al. (2019), "Model Cards for Model Reporting"; Gebru et al. (2021), "Datasheets for Datasets".

## Glossary [optional]

- **CT**: centro de transformación (media a baja tensión). Unidad de predicción.
- **Evento local / sistémico**: interrupción que afecta a un solo CT frente a varias a la vez por la misma incidencia.
- **PR-AUC**: área bajo la curva precision-recall, calculada como la average precision de scikit-learn.
- **Brier**: error cuadrático medio de la probabilidad; el BSS lo compara con el de decir cada día la tasa observada.
- **SHAP**: valores de Shapley para atribuir la predicción a cada feature.

## More Information [optional]

- [dataset_card.md](dataset_card.md): datos, sesgos y versionado.
- [silver-layer.md](silver-layer.md): qué limpieza se hace antes de Gold y por qué.

## Model Card Authors [optional]

Josep Morancho i Poyatos y Anton Shebarshinov Polenyaka.

## Model Card Contact

A través de los issues del repositorio o de los autores (UPC School, TFM GridPredic 2026).
