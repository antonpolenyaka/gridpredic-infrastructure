# Riesgo del día siguiente: variables y bloques

`etl/jobs/04_ml/job_ml_riesgo_dia.py` estima, para cada distribuidora y día D, si empezará alguna interrupción imprevista en sus CT, con lo que se sabe la tarde anterior. Aquí está el catálogo de variables que antes iba en la cabecera del job. Las pruebas de cada bloque están en [investigacion-continua.md](investigacion-continua.md).

## Unidad y etiquetas

Una fila por distribuidora y día, de `meteo_dia.desde` a `meteo_dia.hasta`, sin los días de `analisis_red.dias_excluidos` (el apagón del 28.04.2025). Las etiquetas salen de la variante `principal` de `fact_interrupciones_mt` con la causa de `l2_silver.f_incidencia_causa`:

| etiqueta | qué es |
| --- | --- |
| `y_dia` | empieza alguna interrupción en un CT de la distribuidora en D (la que aprenden los modelos) |
| `y_dia_meteo` | lo mismo sin las causas de `causas_no_meteo` (aguas arriba, apagón, terceros...) |
| `y_dia_varias` | empiezan `incidencias_dia_varias` o más en D (los días que piden más de una brigada) |
| `n_dia` | cuántas empiezan en D (la etiqueta del modelo de Poisson) |

## Bloques

Todo se conoce antes de que empiece D salvo el tiempo del propio día: el bloque `dia` y los que se calculan con la racha, la lluvia o la temperatura de D (`ext_`, `hie_`, `reg_`, `frag_` y las interacciones con la racha), que al entrenar salen del reanálisis (una previsión perfecta, la cota de cualquier previsión real) y al puntuar con la previsión real se cambian por sus gemelos de previsión.

| prefijo | bloque | qué es | en producción |
| --- | --- | --- | --- |
| `dist_` | distribuidora | una columna por distribuidora (1 en sus filas): el término propio de cada red | sí (entra en todas las variantes) |
| `cal_` | calendario | día de la semana (seno y coseno), fin de semana, festivo nacional o de Extremadura, día del año y estación | sí |
| `hist_` | historia | incidencias conocidas en Calser antes de D en 7, 30 y 365 días, y días desde la última | sí |
| `ayer_` | ayer | el tiempo de D - 1 del reanálisis: racha y viento de la peor celda y media por CT, lluvia, temperaturas, humedad, presión y su caída, nieve, tamaño del temporal, focos de FIRMS y FWI | solo `ayer_racha_max` y `ayer_lluvia_max` |
| `aviso_` | avisos | el nivel más alto de los avisos de AEMET para D enviados antes de las 20:00 de D - 1, en total y por viento, lluvia, tormenta y nieve (0 sin aviso, nulo antes del 18.06.2018) | sí |
| `aviso_det_` | avisos_detalle | lo que anuncian esos avisos (racha, lluvia en 12 h y en 1 h, horas con aviso de viento) y la parte de los CT bajo aviso de viento, de tormenta o de cualquiera de los cuatro fenómenos | no |
| `dia_` | dia | el mismo tiempo diario que `ayer_`, de D | `dia_comun`: lo que también da la previsión |
| `ext_` | extremos | la racha de D frente al p98 de cada celda (peor celda y media por CT), el índice de pérdidas de Klawa y Ulbrich y los días seguidos de calor | sí |
| `prev_` | prevision | el mismo tiempo diario de D según ICON-EU la víspera (`metp1_*` de Gold, desde 01.2024); nunca es variable: sustituye a `dia_` y `ext_` al puntuar con la previsión real | sí, al puntuar |
| `ext_fraccion_` | extension | la parte de los CT bajo racha en el p98 o en 72 km/h en la peor hora y en CT-horas | no (3f) |
| `ext_int_` | interacciones | el exceso de racha por la lluvia previa, la del día, la estación de hoja y el verano | no (3f) |
| `ext_pend_` | pendiente_red | la racha por cada distribuidora | no (3f) |
| `ext_dir_` | direccion | lo lejos que sopla el viento de la dirección habitual de los temporales de la celda; solo reanálisis, con `--direccion` | no (3f) |
| `seq_` | sequia | balance hídrico de 90 y 180 días como z-score del mes | no (3i) |
| `hie_` | hielo | CT-horas con precipitación a 0,5 grados o menos | no (3i) |
| `fen_` | fenologia | NDVI de MODIS de la red, su anomalía y su cambio en un mes, y la racha por el verdor | no (3k) |
| `hoja_` | hoja | parte frondosa de la red según el HRL de Copernicus y la racha por ella en abril - octubre | no (3l) |
| `corr_` | corredor | árbol de 8 m o más en una banda de 15 m de las líneas de OSM, y la racha por esa parte | no (3n) |
| `reg_` | regimen | la racha leída como temporal de invierno, convección o calor | no (3o) |
| `frag_` | fragilidad | la curva de fragilidad (tasa de avería según la racha, ajustada antes de `fragilidad_hasta`) | no (3o) |
| `cad_` | caducas | la copa de frondosas caducas del MFE50 y la racha por ella en abril - octubre | no (3p) |
| `nao_` | nao | el índice NAO diario de NOAA CPC que se conoce la tarde anterior, el de D - 3 (`nao_dia`, con `meteo_dia.nao.retraso_dias`), y su media de 30 días (`nao_30d`) | no (3r); se descarga cada tarde para volver a medirlo |

Los bloques que dependen de la racha o de la lluvia del día tienen su gemelo de previsión (`prev_<variable>`), que es el que se usa al puntuar con la previsión real.

## Horizontes y otros modelos

- **A dos y tres días** (`HORIZONS`): Gold guarda también lo que ICON-EU preveía dos y tres días antes (`metp2_*`, `metp3_*`). Dan `prev2_*` y `prev3_*`, los avisos enviados antes de las 20:00 de D - 2 y D - 3 (`aviso48_*`, `aviso72_*`) y el último día de tiempo conocido esa tarde (`hace2_*`, `hace3_*`). Una variante con `"horizonte": 2` o `3` se puntúa con eso; en lugar de `dia_comun` y `extremos` lleva `dia_comun_48h` o `dia_comun_72h` (el tiempo del día que da esa previsión: racha y lluvia) y `extremos_racha` (los extremos de la racha, sin los días de calor). `operativa_48h` es, desde el 15.10.2026, la orientación de pasado mañana de la ejecución de la tarde (`riesgo_manana.variante_pasado_manana`).
- **Otros modelos de previsión** (`meteo_dia.previsiones_extra`, con `--previsiones-extra`): GFS, GEM o UKMO del archivo de Open-Meteo, leídos de Bronze, e IFS HRES 9 km rehecho desde las pasadas de las 00 UTC del día anterior (`job_landing_meteo_pasadas.py`, desde el 15.03.2024; el archivo Previous Runs no guarda su racha). Nunca son variables; la comparación con la previsión real puntúa también con cada uno y con la media de todos. `--anios-prevision 2025` limita la comparación al año que cubren todos.

## Variantes y métricas

Cada variante (`meteo_dia.variantes`) es una lista de bloques, unas variables sueltas, un primer día de entrenamiento y, si no es la víspera, su horizonte; el bloque `distribuidora` entra siempre. `operativa` es la que ajusta y puntúa cada tarde `job_ml_riesgo_manana` (`riesgo_manana.variante`). Cada año lo puntúa un modelo elegido y ajustado solo con los años anteriores (`rolling_evaluation`), y cada variante lleva también `rango`, la media de los percentiles de su logística y su XGBoost.

Dos variantes miden la tarde con la copia de Calser desfasada, que es lo normal mientras no llegue un backup nuevo: `operativa_sin_historia` quita el bloque `historia`, que es lo que hace la tarde desde el 17.10.2026 (`riesgo_manana.historia_desfasada`), y `operativa_historia_mediana` se ajusta con la historia pero puntúa cada año con ella vacía (`"historia_al_puntuar": "mediana"`), así que el modelo toma la mediana de sus días de entrenamiento, como hacía la tarde hasta entonces. Las dos se comparan con `operativa` en `meteo_dia.comparaciones` y con la previsión real.

Los modelos (`meteo_dia.modelos` o `--modelos`): `logistica` (L2, la de producción), `logistica_firth` (Firth con el intercepto de FLIC) y `logistica_bayes` (prior de Cauchy de Gelman y otros, 2008), los dos sin nada que ajustar; `logistica_equilibrada`, `poisson`, `xgboost` y `random_forest`; y las referencias (`tasa_base` y una variable sola).

Las métricas se calculan sobre los años que puntúan todas las variantes: PR-AUC, ROC-AUC, veces la tasa base, la parte de los días con avería que cae en el 5, 10 y 20 % de días de más riesgo de cada año, PR-AUC por distribuidora y el bootstrap por fechas de las diferencias emparejadas (`meteo_dia.comparaciones`, y con la previsión real `meteo_dia.comparaciones_prevision`; Firth y Cauchy se comparan siempre con la L2 de su variante). Para los modelos que dan una probabilidad (y para `poisson`, la de al menos una incidencia, 1 - exp(-conteo)) también la calibración: Brier, BSS frente a la tasa observada, log loss, pendiente e intercepto de la recalibración logística de Cox y probabilidad media. `meteo_dia.bootstrap_bloque_dias` cambia las réplicas de fechas sueltas a bloques de días seguidos: desde el 17.10.2026 son siete, porque con la previsión real los residuos de días seguidos están correlacionados (contraste de Ljung y Box, apartado 3s del cuaderno). La comparación con la previsión real se hace sobre los días que tienen todas las previsiones que piden las variantes de la misma ejecución: por eso, con la previsión real de 2024 - 2026, la misma `operativa` da 0,161 de PR-AUC en el apartado 3m del cuaderno, que lleva las variantes a 48 y 72 h, y 0,156 en las pruebas de después. Con la previsión real cada año se puntúa tres veces: con el reanálisis, con la previsión tal cual (`__prevision`, la cifra que se publica) y con la previsión llevada por cuantiles a la escala del reanálisis (`__prevision_cuantiles`). Ese mapeo se ajusta con los demás años de la comparación, también los posteriores al que se puntúa, así que `_cuantiles` no es exactamente lo que se sabía la víspera; solo toca la meteorología, no las etiquetas, y sirve para medir cuánto pesa el sesgo de ICON-EU, no para operar.

## Salidas

`l3_gold.ml_runs` y `l3_gold.ml_metricas` con `run_id` `dia_*`, y en `exportar_dir/<run_id>/`: `resumen.md`, `metricas.csv`, `comparacion.csv`, `anios.csv`, `distribuidoras.csv`, `intervalos.csv`, `diferencias.csv`, `filas.csv`, `prevision.csv`, `prevision_diferencias.csv`, `variantes.csv`, `calibracion_anios.csv`, `coeficientes.csv` y `coeficientes_estabilidad.csv`. `calibracion_anios.csv` es la calibración año a año de la logística de `operativa` (tasa observada, probabilidad media, BSS, pendiente e intercepto), donde se vería un cambio en cómo se registran las interrupciones, como el de noviembre de 2025, cuando las horas de Calser pasan a coincidir con las de TedisNet. Los dos últimos guardan los coeficientes de cada modelo lineal en cada año de la evaluación (`ml_models.linear_terms`: la escala que ve el modelo, con la mediana en los huecos, el logaritmo con signo y la estandarización, así que exp(coeficiente) es la razón de odds de una desviación típica) y, por variable, su media, su dispersión y si el signo es el mismo todos los años; el resumen enseña los mayores de la logística de `operativa`.

```
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_ml_horas_riesgo.py \
    /app/jobs/04_ml/job_ml_riesgo_dia.py --config /app/config/04_ml/config_ml.json
```
