# Entrenamiento del modelo

Este documento explica cómo entrenamos y evaluamos el modelo de predicción de interrupciones sobre el dataset de Gold, qué decisiones hemos tomado y dónde quedan los resultados. Los requisitos y las métricas de aceptación están en la [model card](model_card.md); aquí está el cómo.

El código está en `etl/jobs/04_ml/` y los parámetros en `etl/config/04_ml/config_ml.json`. Los modelos con la historia meteorológica desde 2017, los avisos de AEMET, el riesgo del día siguiente y las fuentes públicas que los alimentan están en [causas-incidencias.md](causas-incidencias.md), apartado 8, junto con el job que calcula cada tarde el riesgo de mañana (`job_ml_riesgo_manana.py`, lanzado por `dag_riesgo_manana`); las variables de ese modelo del día están en [ml-riesgo-dia.md](ml-riesgo-dia.md).

| Fichero | Qué hace |
| --- | --- |
| `job_ml_train.py` | Lee la versión del dataset, entrena, puntúa valid y test, calcula las métricas y escribe las tablas de resultados |
| `ml_models.py` | Los cinco modelos de este documento (dos referencias y tres entrenados), los submodelos por grupo, la logística equilibrada, el modelo de Poisson y las logísticas de Firth y de Cauchy que prueba el riesgo del día, la corrección de la probabilidad por el muestreo, las importancias y los coeficientes de los modelos lineales (`linear_terms`) |
| `ml_metrics.py` | PR-AUC, ROC-AUC, alertas con presupuesto, episodios, antelación, PR-AUC de cada feature sola, la calibración (Brier, BSS, pendiente e intercepto) y el bootstrap por fechas o por bloques de días con las diferencias emparejadas |
| `train.sh` | Lanza el job con `spark-submit` desde el contenedor `spark-master` |

## 1. Qué aprende el modelo

Cada fila de `l3_gold.dataset_train` es un CT en una hora. La etiqueta `y_1_3h` vale 1 si en las siguientes 1 a 3 horas empieza una interrupción imprevista de Calser en ese CT (con los mismos filtros que usa Calser para el TIEPI). Las 246 features describen lo que el SCADA TedisNet sabía antes de esa hora: medidas eléctricas del CT y de la cabecera que lo alimenta, señales de defecto, disparos, alarmas, comunicaciones, cortes vistos por el SCADA, el histórico del CT y el calendario. Gold ya garantiza que ninguna feature usa datos posteriores a su hora (ver [gold-layer.md](gold-layer.md)).

Es un problema muy desbalanceado: en el dataset `ds_20261010T045104_a5a2531cacbb` hay 13.124 positivos entre 43,6 millones de filas de train (un 0,03 %).

## 2. Datos que usa cada paso

| Paso | Filas | Para qué |
| --- | --- | --- |
| Muestra de train | `split = 'train'` y `en_muestra_train`: todos los positivos y el 2 % de los negativos (unas 885.000 filas) | Entrenar los modelos. Cabe en memoria del driver en float32 |
| Muestra de valid | `split = 'valid'`: todos los positivos y un 2 % de los negativos, con peso 1 / 0,02 en los negativos | Parada temprana de XGBoost y elección de la configuración de cada modelo por la PR-AUC ponderada |
| Valid completo | Los 11,2 millones de filas de 2025 | Métricas que reportamos y comparación entre modelos |
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

En la tutoría del 08.10.2026 el profesor de análisis de datos nos recomendó no entrenar un único modelo para los casi 1.300 CT: mezclar CT grandes y pequeños, urbanos y rurales, de zonas con distinta meteorología, diluye la capacidad de predicción. La propuesta es agrupar los CT en conjuntos homogéneos (por ejemplo con k-means) y entrenar un submodelo para cada grupo, empezando por el grupo más homogéneo y con más incidencias como piloto. Las métricas de la línea base ya lo apuntaban: la Distribuidora 3 (id 1366) tiene una PR-AUC 3,5 veces mayor que la tasa base, y las otras dos apenas la superan.

- **Grupos** (`clusters`): k-means sobre una fila por CT con el tipo de zona del municipio (urbana, semiurbana, rural concentrada, rural dispersa), la potencia, los abonados, las salidas, los transformadores, el tamaño del grupo de red, la telemetría, la tensión, las coordenadas (como aproximación de la meteorología), la distribuidora y la tasa de interrupciones del CT en train. Las variables asimétricas van en escala logarítmica, las categóricas en one-hot y todas estandarizadas. k se elige por silueta entre 3 y 8, con un mínimo de 20 CT en el grupo más pequeño. Solo se usan datos estáticos o de train, así que los grupos no miran valid ni test. El reparto queda en `l3_gold.ml_clusters_ct`, y el perfil de cada grupo en el resumen y en `grupos_ct.csv`.
- **Submodelos** (`submodelos`): un XGBoost por grupo con la configuración elegida para el modelo global y parada temprana sobre las filas del grupo en la muestra de valid. Un grupo con menos de 200 positivos en la muestra de train se queda con el modelo global. El conjunto se evalúa como un modelo más, `xgboost_cluster`, con las mismas métricas y sobre las mismas filas que el global. La búsqueda guarda, por grupo, la PR-AUC del submodelo frente a la del global.
- **Por qué pasa en cada grupo**: el resumen lista las 5 features con más peso (SHAP) en el submodelo de cada grupo, que es lo que permite explicar qué anticipa una interrupción en un CT rural disperso frente a uno urbano.
- **Estación del año** (`estacion`): 1 invierno, 2 primavera, 3 verano, 4 otoño, como variable propia, también a propuesta de la tutoría. El split temporal ya garantiza que train cubre todas las estaciones de varios años.
- Las métricas salen también por grupo (segmento `grupo_ct`).

## 5. Meteorología

La meteorología era lo que la model card daba como pendiente ("entrará cuando esté ingerida") y lo que más explica las interrupciones sistémicas. Viene de la [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (CC BY 4.0, plan gratuito para uso no comercial), con el modelo ECMWF IFS, de 9 km y horario desde 2017. Es el único de los reanálisis gratuitos que da a la vez ráfagas de viento y precipitación.

- **Qué se pide:** temperatura, humedad, precipitación, nieve, viento, ráfagas y presión a nivel del mar, de hora en hora, del 01.01.2017 al 31.08.2026 (la primera descarga empezaba el 01.12.2020; la historia se alargó hasta 2017 en la 0.9.0). El dataset de Gold va del 08.01.2021 al 14.08.2026 y las features miran hasta 7 días atrás, así que con ese margen no falta ninguna hora.
- **Dónde:** los 73 municipios de `data/reference_data/municipios.xlsx` agrupados en celdas de 0,1 grados (unos 11 km, cerca de la rejilla de 9 km del modelo), que son 43. Con el periodo de la primera descarga eran unas 6.450 llamadas del plan gratuito, por debajo del límite de 10.000 al día; desde 2017 son más, y si se acaba el cupo diario la siguiente ejecución sigue donde iba.
- **Landing** (`etl/jobs/00_landing/job_landing_meteo_openmeteo.py`): una petición por celda y año, guardada tal cual (JSON) en `datalake/00_landing/meteo/openmeteo/`. Es reanudable y respeta los límites por minuto, hora y día. La configuración está en `etl/config/01_bronze/config_bronze_meteo.json`.
- **Bronze** (`etl/jobs/01_bronze/job_bronze_meteo_openmeteo.py`): `l1_bronze.meteo_openmeteo_hora` en hora local de Madrid, la misma convención que Calser y TedisNet. Las dos horas UTC que caen en la misma hora local al cambiar al horario de invierno se fusionan. `l1_bronze.meteo_openmeteo_celdas` asigna cada municipio a su celda.
- **Silver** (`job_silver_f_meteo_hora.py`, `job_silver_d_meteo_celda.py`): igual que el resto de la capa. Nombres en castellano con su unidad (`racha_kmh`, `precipitacion_mm`...), una sola fila por celda y hora, valores fuera de rango físico a nulo con el flag `fuera_rango`, flag `racha_menor_viento`, recuento de horas que faltan (el salto del cambio al horario de verano no cuenta como hueco) y métricas en `l2_silver.dq_metrics`. `d_meteo_celda` deja cada municipio con su celda, el código INE a cinco cifras y el nombre normalizado.
- **Gold** (`job_gold_meteo_celda_hora.py`, en `dag_gold`): `l3_gold.map_ct_celda` cruza el municipio de cada CT en Calser (`dim_ct.municipio_id`, que es el código INE) con `d_meteo_celda` por código y, si falla, por nombre sin tildes (columna `cruce`). Las coordenadas del municipio entran como `ct_lat_municipio` y `ct_lon_municipio` y en la clusterización, porque las de `dim_ct` estaban vacías. Son tablas nuevas e independientes: no se reconstruye nada de lo que ya había en Silver ni en Gold, y la meteorología no se mete en `features_ct_hora`; el entrenamiento la une por `(celda_id, hora)`.
- **Features** (`l3_gold.meteo_celda_hora`): `met_*` son las últimas horas (ráfaga y viento de la hora; ráfaga máxima de 3, 6 y 24 h; lluvia de 1, 3, 6 y 24 h; nieve de 24 h; temperatura, mínima y máxima de 24 h; humedad; presión y su cambio en 3 y 24 h). `metprev_*` son las 3 horas siguientes: ráfaga máxima, lluvia y nieve. `met_region_*` y `metprev_region_*` son el máximo de todas las celdas, que da el tamaño del temporal. La fila de cada hora describe la hora que termina entonces, así que se conoce a esa hora, igual que el resto de features.
- **Sobre la previsión:** `metprev_*` usa el reanálisis como si fuera una previsión perfecta de las 3 horas siguientes. En operación sería la previsión de AEMET o del ECMWF, que a 1-3 horas es buena pero no perfecta, así que el resultado con estas features es una cota superior. Con `meteo.prevision: false` en `config_ml.json` el entrenamiento las deja fuera, para medir cuánto aporta solo la meteorología observada.
- **Limitación:** ninguna fuente gratuita da rayos históricos y la propia Open-Meteo avisa de que con estos datos no se pueden estimar tormentas. Las tormentas eléctricas solo se aproximan con ráfagas y precipitación intensa y, desde la 0.9.0, con el CAPE de los modelos GFS archivados (`met_cape_*`).

## 6. Modelos

| Modelo | Tipo | Por qué está |
| --- | --- | --- |
| `tasa_base` | Referencia | La misma puntuación para todas las filas. Su PR-AUC es la proporción de positivos: es el suelo |
| `naif_historico` | Referencia | Ordena los CT por sus interrupciones conocidas en 365 días (y 90 y 30 para desempatar). Es lo que haría un operador sin modelo y la referencia que pide la model card |
| `logistica` | Regresión logística L2 | Referencia interpretable. Imputación por mediana con indicador de nulo, escala `sign(x) * log(1 + abs(x))` y estandarización |
| `random_forest` | Random Forest de scikit-learn | Otro modelo de árboles para comparar con el boosting. Acepta nulos sin imputar |
| `xgboost` | Gradient boosting (XGBoost, `hist`) | Candidato principal, el que llevamos a las presentaciones. Parada temprana sobre la PR-AUC de la muestra de valid |

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

### Comparar ejecuciones con intervalos de confianza

En valid hay unas 4.300 horas positivas en todo 2025 y llegan agrupadas en tormentas, así que una PR-AUC de 0,0008 frente a otra de 0,0019 puede ser ruido. `job_ml_comparar.py` no entrena: lee las predicciones de valid ya guardadas en `ml_predicciones`, junta las ejecuciones sobre las mismas filas (todos los positivos y el 2 % de los negativos, siempre los mismos, con peso 1 / 0,02) y hace un bootstrap por días: cada réplica sortea con reemplazo los 365 días de valid. Con las mismas réplicas para todas las ejecuciones y modelos, las diferencias son emparejadas.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \
    /app/jobs/04_ml/job_ml_comparar.py --config /app/config/04_ml/config_ml.json \
    --runs <run_id_1>,<run_id_2>,...
```

Deja en `exportar_dir/comparacion_<fecha>/` el `resumen.md`, `intervalos.csv` (PR-AUC con su intervalo del 95 % por ejecución, modelo y distribuidora) y `diferencias.csv` (cada XGBoost contra la tasa base de su ejecución, el modelo por grupos contra el global y cada ejecución contra la primera). Si el intervalo de una diferencia cruza el cero, los datos no distinguen los dos modelos. Con 500 réplicas tarda unos 7 minutos en el portátil.

Primera comparación (11.10.2026, 500 réplicas, tasa base 0,00038):

| Ejecución | XGBoost PR-AUC [IC 95 %] | XGBoost - tasa base | P(mejor que la tasa base) | XGBoost - ejecución base | P(mejor que la base) |
| --- | --- | --- | --- | --- | --- |
| base | 0,00077 [0,00016 - 0,00183] | +0,00039 [+0,00006, +0,00096] | 99,8 % | - | - |
| red | 0,00065 [0,00024 - 0,00158] | +0,00033 [+0,00009, +0,00134] | 100 % | -0,00006 [-0,00063, +0,00106] | 42,6 % |
| grupos (k=4) | 0,00192 [0,00012 - 0,00772] | +0,00150 [+0,00003, +0,00686] | 99,4 % | +0,00111 [-0,00022, +0,00585] | 67,4 % |
| grupos (k=3) + meteo | 0,00097 [0,00027 - 0,00592] | +0,00059 [-0,00020, +0,00572] | 81,8 % | +0,00019 [-0,00106, +0,00547] | 62,0 % |

Lo que se puede afirmar con estos datos:

- XGBoost aprende algo real: en tres de las cuatro ejecuciones su PR-AUC es mayor que la de la tasa base con un intervalo que no cruza el cero (unas dos veces la prevalencia).
- Ninguna de las tres ampliaciones (estado de la red, grupos de CT, meteorología) mejora a la ejecución base de forma demostrable. El 0,0019 de grupos tiene un intervalo de 0,0001 a 0,0077: sale casi todo de unos pocos días de la Distribuidora 2.
- Los submodelos por grupo nunca superan al XGBoost global de su ejecución (probabilidad entre el 12 % y el 34 % en total).
- En la Distribuidora 3 (id 1366), XGBoost queda por encima de su tasa base en más del 93 % de las réplicas en las cuatro ejecuciones, y es donde la PR-AUC es más alta (0,003 a 0,006 frente a una prevalencia de 0,0009).

### Horas de riesgo por distribuidora

Con un 0,04 % de positivos por CT y hora el ruido es mayor que las diferencias entre configuraciones, y el 94 % de las interrupciones son sistémicas: lo que las anuncia es el estado de toda la red y el tiempo, no el CT. `job_ml_horas_riesgo.py` hace la misma pregunta al nivel donde vive esa señal: ¿va a empezar una interrupción en algún CT de la distribuidora en las próximas 1 a 3 horas? Es también la decisión que toma el operador, qué horas del día necesitan una brigada preparada.

- **Dataset** (`l3_gold.ml_dataset_distribuidora_hora`), una fila por distribuidora y hora. La etiqueta sale de `dataset_train` (1 si algún CT tiene `y_1_3h`, lo mismo con la local) con el número de CT positivos y la antelación más corta, y el split es el mismo. Las features salen de `features_ct_hora`, todos los CT y todas las horas, para que ninguna dependa de qué filas quitó el dataset (`en_corte` viene de las etiquetas): la media de cada feature del CT (`media_*`) y, para los bloques de eventos, SCADA e histórico (`horas_riesgo.prefijos_frac`), la fracción de CT donde no es cero (`frac_*`). La meteorología de la distribuidora es la media de sus celdas ponderada por los CT de cada una (`metd_media_*`) y la peor celda (`metd_max_*`). Se suman `distribuidora_id` y `estacion`. La tabla se reutiliza mientras no cambien la versión del dataset ni los parámetros de la agregación; `--reconstruir` la vuelve a hacer.
- **Modelos**: los mismos de `modelos` con la misma búsqueda, más referencias de una sola feature (`horas_riesgo.referencias`): la racha máxima de las últimas 3 horas en la peor celda de la distribuidora y la de las 3 siguientes. Son lo que miraría un operador sin modelo, así que un modelo que no las supera no aporta nada. El naif histórico ordena por las interrupciones conocidas de los CT de la distribuidora en 365, 90 y 30 días. Con unas 150.000 filas se entrena con todo train, sin muestra, y valid entero sirve para el early stopping y para elegir la configuración.
- **Evaluación**: PR-AUC, ROC-AUC, lift y alertas con un presupuesto de N horas por distribuidora y día (`presupuestos_horas_dia`: 1, 2, 4 y 6), por distribuidora y por estación, y el bootstrap por días con el intervalo de cada modelo y las diferencias emparejadas contra las referencias. Un episodio aquí son las horas positivas seguidas de una misma distribuidora.
- **Salidas**: `ml_runs`, `ml_metricas` y `ml_importancia` con `run_id` `hr_*`, `l3_gold.ml_predicciones_distribuidora_hora` y, en `exportar_dir/<run_id>/`, `resumen.md`, `metricas.csv`, `importancia.csv`, `intervalos.csv`, `diferencias.csv` y `filas.csv`. `job_ml_comparar.py` deja fuera las ejecuciones `hr_*` si no se le pasan los `run_id`.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \
    /app/jobs/04_ml/job_ml_horas_riesgo.py --config /app/config/04_ml/config_ml.json
```

`--sin-prevision` deja fuera la previsión de las horas siguientes sobre la misma tabla (unos 4 minutos, la tabla tarda unos 18 en construirse la primera vez).

Primeras ejecuciones (11.10.2026, valid 2025, 500 réplicas):

| | Con previsión (`hr_20261011T073905_a68023`) | Sin previsión (`hr_20261011T082531_8f77c8`) |
| --- | --- | --- |
| Horas de valid / positivas | 26.210 / 68 | 26.210 / 68 |
| Tasa base | 0,0026 | 0,0026 |
| Racha máxima sola | 0,0133 | 0,0133 |
| XGBoost PR-AUC [IC 95 %] | 0,0315 [0,0048 - 0,0745] | 0,0574 [0,0058 - 0,1409] |
| XGBoost - tasa base | +0,027 [+0,003, +0,072], 100 % | +0,053 [+0,004, +0,138], 100 % |
| XGBoost - racha máxima | +0,014 [-0,002, +0,040], 94,6 % | +0,040 [-0,017, +0,110], 89,6 % |
| Random Forest PR-AUC | 0,0304 | 0,0173 |
| recall_episodios@2 de XGBoost (racha: 0,226) | 0,194 | 0,194 |

Lo que sale de aquí:

- Las 4.258 horas positivas de valid por CT son solo 68 horas positivas por distribuidora, unas 31 incidencias en todo 2025: cada incidencia toca de media a unos 45 CT a la vez. La muestra efectiva es el número de incidencias, unas 30 al año en valid y un centenar en train, y eso explica la inestabilidad de todas las comparaciones por CT.
- XGBoost queda por encima de la tasa base y del naif histórico en todas las réplicas, con o sin previsión (un lift de 12 a 22).
- La racha máxima de la zona en las últimas 3 horas, sola, ya multiplica por 5 la tasa base. El modelo la supera en PR-AUC en el 90 - 95 % de las réplicas, pero el intervalo de la diferencia cruza el cero, y con 2 horas de alerta por distribuidora y día avisa de las mismas incidencias o de menos.
- La previsión perfecta de las 3 horas siguientes no mejora el resultado: sin ella la PR-AUC sale incluso más alta, porque la búsqueda elige otra configuración (`min_child_weight` 20) y casi todo viene de la Distribuidora 2. Con 31 incidencias la elección de la configuración en valid es en sí misma ruidosa.
- Lo que más pesa en los dos modelos es la meteorología (lluvia, rachas, temperatura mínima, humedad) y el estado de la red aguas arriba: reenganches en 6 y 24 h, defectos a tierra en el grupo de red, microcortes y cortes del SCADA, cambios de comunicaciones.
- La Distribuidora 2 y la Distribuidora 1 (ids 2 y 3) tienen las mismas horas positivas en test (26 y 2 locales), pero solo coinciden en 4 horas: no hay interrupciones duplicadas entre ellas. Sí comparten el mismo instante de inicio en eventos de transporte o del sistema que les afectan a la vez, como el apagón del 28.04.2025.

#### Evaluación por años

Con 31 incidencias en valid, elegir la configuración en valid y medirla en el mismo valid da cifras optimistas. `--por-anios` evalúa 2023, 2024 y 2025 por separado: cada año lo puntúa un modelo que solo ha visto los años anteriores, elegido en el último 25 % de esas horas (`fraccion_validacion_interna`) y reentrenado con todas, con XGBoost limitado a los árboles que encontró el early stopping. Test no se toca. Los tres años juntos son 104 incidencias.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \
    /app/jobs/04_ml/job_ml_horas_riesgo.py --config /app/config/04_ml/config_ml.json --por-anios --sin-prevision
```

Resultados (11.10.2026, 2023 - 2025 juntos, 500 réplicas por días):

| Modelo | Sin previsión (`hr_20261011T092843_b0dd1a`) | Con previsión perfecta (`hr_20261011T093958_c0ffb7`) |
| --- | --- | --- |
| Tasa base | 0,0028 | 0,0028 |
| Racha máxima observada | 0,0094 [0,0057 - 0,0173] | 0,0094 |
| Racha máxima prevista | - | 0,0137 [0,0063 - 0,0331] |
| Logística | 0,0084 [0,0056 - 0,0127] | 0,0097 [0,0064 - 0,0152] |
| XGBoost | 0,0078 [0,0052 - 0,0128] | 0,0104 [0,0061 - 0,0185] |
| Random Forest | 0,0049 [0,0033 - 0,0082] | 0,0177 [0,0080 - 0,0355] |
| P(XGBoost mejor que la racha observada) | 22 % | 67 % |
| recall_episodios@2: XGBoost / racha / tasa base | 0,135 / 0,192 / 0,183 | 0,164 / 0,192 / 0,183 |

Conclusiones:

- Todos los modelos entrenados quedan por encima de la tasa base y del naif histórico en todas las réplicas, unas 3 veces la prevalencia. Hay señal, y es sobre todo meteorológica y del estado de la red aguas arriba.
- Sin previsión, ningún modelo supera a la regla de la racha máxima observada en la zona: XGBoost y la logística quedan empatados con ella y el bosque por debajo. Las cifras de 0,03 a 0,06 de la evaluación sobre valid salían de elegir y medir en el mismo año.
- Con la previsión perfecta de las 3 horas siguientes el bosque llega a 6 veces la prevalencia y supera a la racha observada (99 % de las réplicas), pero no a la racha prevista. El valor está en la previsión del tiempo; un modelo en producción necesitaría una previsión real (AEMET, ECMWF) en lugar del reanálisis, y esta cifra es la cota superior de lo que podría aportar.
- Con un presupuesto fijo de horas por día ningún modelo avisa de más incidencias que elegir las horas al azar. Los modelos saben qué días son peligrosos (temporal, red alterada) pero no a qué hora empezará la interrupción dentro del día, así que la alerta útil es por día o por umbral de riesgo.

#### Evaluación final en test

Con la configuración cerrada (sin previsión, mismo procedimiento que en los años anteriores), test se ha mirado una sola vez con `--por-anios 2026 --sin-prevision --evaluar-test`: el modelo se elige en el último 25 % de las horas hasta 2025 y se reentrena con todas, y puntúa enero a mayo de 2026 (`test_hasta` 01.06.2026, por la censura de las etiquetas). Ejecución `hr_20261011T100606_7391f5`, 10.855 horas, 48 positivas, 21 incidencias:

| Modelo | PR-AUC [IC 95 %] | Lift | P(mejor que la tasa base) | P(mejor que la racha) | recall_episodios@2 |
| --- | --- | --- | --- | --- | --- |
| Random Forest | 0,0319 [0,0050 - 0,1386] | 7,2 | 100 % | 89 % | 0,143 |
| XGBoost | 0,0172 [0,0037 - 0,0916] | 3,9 | 100 % | 65 % | 0,238 |
| Logística | 0,0164 [0,0036 - 0,0488] | 3,7 | 100 % | 61 % | 0,333 |
| Racha máxima observada | 0,0140 [0,0044 - 0,0398] | 3,2 | - | - | 0,190 |
| Tasa base | 0,0044 | 1,0 | - | - | 0,286 |
| Naif histórico | 0,0032 | 0,7 | - | - | 0,286 |

Test confirma lo que dijo la evaluación por años: los modelos son claramente mejores que la tasa base y que el histórico (entre 3,7 y 7 veces la prevalencia, en todas las réplicas), pero ninguno se distingue de la regla de la racha máxima, y con un presupuesto fijo de horas por día no avisan de más incidencias que el azar. Lo que más pesa en XGBoost es la hora del día, los cambios de comunicaciones, la lluvia y las rachas, los microcortes del SCADA y las faltas de tensión en el CT y en su grupo de red.

#### Fuentes, mezclas, grupos y zonas (`--ablacion`)

¿Predice mejor la mezcla de TedisNet, Calser y la meteorología que cada fuente por separado? ¿Conviene separar por grupos o bajar a una unidad más pequeña que la distribuidora? `--ablacion` responde con la misma evaluación por años y compara todas las variantes sobre las mismas horas de distribuidora con un bootstrap emparejado:

- **Por fuente** (`source_of`): `calser` (interrupciones conocidas en Calser, `hist_*` salvo los del SCADA), `tedisnet` (medidas, eventos, cortes, calidad y `hist_scada_*`), `meteo` (`metd_*`) y sus mezclas `calser+tedisnet`, `tedisnet+meteo` y `todo`. Las features de los CT, del calendario y la distribuidora entran siempre.
- **Interacciones** (`todo+interacciones`): productos de una variable del tiempo y otra del estado de la red (`ablacion.interacciones`), por ejemplo la racha de la última hora por la fracción de CT con reenganches aguas arriba en 6 h, o la lluvia de 6 h por la fracción con defectos a tierra en su grupo de red. Los árboles pueden encontrarlas solos, pero con un centenar de incidencias al año en train un producto explícito es más fácil de aprender.
- **Un modelo por distribuidora** (`por_distribuidora`): la misma evaluación con un modelo para cada una.
- **Zona** (`zona`): `l3_gold.ml_dataset_zona_hora`, una fila por zona y hora, donde la zona es la celda meteorológica de 9 km del municipio de los CT (43 en las tres distribuidoras). Cada fila junta la fracción de CT de la zona con cada señal de eventos, SCADA e histórico en 6 y 24 h, la media de las features estáticas y de calendario, el tiempo de la celda (`metz_*`) y el estado de toda la distribuidora (`dist_*`: las mismas fracciones y el peor tiempo): lo local y el conjunto en la misma fila. El modelo de zona se mide por zona y hora y, llevado a la distribuidora con la zona de más riesgo de cada hora, se compara con los demás.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \
    /app/jobs/04_ml/job_ml_horas_riesgo.py --config /app/config/04_ml/config_ml.json --ablacion --sin-prevision
```

Deja `resumen.md`, `resumen.csv` (una fila por variante y modelo con la PR-AUC, su intervalo, el lift, el recall por episodios y la probabilidad de superar a la racha y a `todo`), `por_anio.csv`, `intervalos.csv`, `diferencias.csv` y `metricas.csv` en `exportar_dir/<run_id>/`.

Resultados (11.10.2026, `hr_20261011T131024_10b6e9`, evaluación por años 2023 - 2025, sin previsión, XGBoost salvo que se diga otra cosa):

| Variante | Features | PR-AUC [IC 95 %] | Lift | P(mejor que la racha) | P(mejor que las tres fuentes) |
| --- | --- | --- | --- | --- | --- |
| Solo meteorología | 58 | 0,0102 [0,0056 - 0,0208] | 3,4 | 61 % | 83 % |
| Solo meteorología, regresión logística | 58 | 0,0111 [0,0064 - 0,0219] | 3,8 | 77 % | 88 % |
| TedisNet + meteorología | 398 | 0,0101 [0,0061 - 0,0174] | 3,4 | 61 % | 94 % |
| Las tres fuentes + interacciones tiempo x red | 420 | 0,0101 [0,0061 - 0,0180] | 3,4 | 63 % | 94 % |
| Racha máxima (referencia) | 1 | 0,0094 [0,0057 - 0,0173] | 3,2 | - | - |
| Un modelo por distribuidora | 412 | 0,0081 [0,0052 - 0,0131] | 2,7 | 18 % | 55 % |
| Las tres fuentes | 412 | 0,0078 [0,0052 - 0,0128] | 2,6 | 22 % | - |
| Solo TedisNet | 360 | 0,0061 [0,0041 - 0,0101] | 2,1 | 9 % | 16 % |
| Calser + TedisNet (solo datos eléctricos) | 374 | 0,0059 [0,0042 - 0,0084] | 2,0 | 4 % | 11 % |
| Modelo por zona, llevado a la distribuidora | 215 | 0,0047 [0,0029 - 0,0078] | 1,6 | 0 % | 0,6 % |
| Solo Calser | 34 | 0,0044 [0,0031 - 0,0066] | 1,5 | 0,6 % | 2 % |
| Tasa base | - | 0,0028 | 1,0 | - | - |

Lectura:

- La fuente que más aporta es la meteorología. Los datos eléctricos solos tienen señal (TedisNet duplica la tasa base con un intervalo que no la toca), pero bastante menos.
- Mezclar las fuentes tal cual no suma: TedisNet más meteorología queda igual que la meteorología sola, y al añadir el histórico de Calser baja. Con un centenar de incidencias en train, cada bloque que no aporta es ruido. Las interacciones explícitas devuelven la mezcla completa al nivel de la meteorología y es la variante más estable año a año (0,0093, 0,0122 y 0,0174, por encima de la racha en los tres años), aunque en el conjunto solo supera a la racha en el 63 % de las réplicas.
- Separar no ayuda: un modelo por distribuidora queda igual que el global, y el modelo por zona sale peor (lift de 1,6 medido por zona y hora): cada celda de 9 km tiene muy pocas incidencias propias.
- Esta comparación se hizo después de usar test, así que test no se ha vuelto a usar para elegir entre variantes.

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
- Política de reentreno: el modelo por CT y hora no se reentrena online. Se reentrena en batch cada 1 a 3 meses con los datos nuevos (recomendación de la tutoría), y cada reentreno es una ejecución más de este job con su `run_id` y su versión de dataset. El riesgo del día siguiente es otra cosa: `dag_riesgo_manana` lo vuelve a entrenar cada tarde con todos los días etiquetados.
- Reentrenar con train + valid antes de la evaluación final, si el tiempo lo permite. Ahora el modelo por CT y hora que se evalúa en test es el mismo que se ha elegido en valid, entrenado solo con train (las horas de riesgo ya se evaluaron en test reentrenadas con todas las horas hasta 2025).
- DAG `04_ml` de inferencia que escriba `predicciones_ct_hora` sobre `features_ct_hora` con el modelo elegido. Ha dejado de hacer falta: el aviso de 1 a 3 horas por CT no sirve como alerta ([model_card.md](model_card.md)), y la inferencia que corre cada tarde es la del riesgo del día siguiente.
