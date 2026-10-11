# Análisis de red, riesgo anual y datos públicos

El modelo por horas responde a "cuándo": qué horas del día necesitan una brigada preparada. A una distribuidora le interesa también "dónde" y "qué hacer": qué líneas hay que revisar o podar este año, dónde compensa poner un elemento telemandado, qué transformadores se están quedando cortos y cuánto dinero hay detrás de cada decisión. Este documento recoge el análisis que hicimos con la historia de Calser, TedisNet y la meteorología para contestar esas preguntas, los datos públicos que añadimos y lo que nos falta para hacerlo mejor.

| Fichero | Qué hace |
| --- | --- |
| `etl/jobs/04_ml/job_analisis_red.py` | Análisis de red sobre Gold: TIEPI por factor y causa, límites del RD 1955/2000, concentración y persistencia, reposición por maniobra, meteorología, carga de la red y valor económico |
| `etl/jobs/04_ml/job_ml_riesgo_ct.py` | Riesgo de cada CT y cabecera para el año siguiente, evaluado por captura, y el ranking del próximo año |
| `etl/jobs/04_ml/job_ml_horas_riesgo.py --curva-aprendizaje` | Curva de aprendizaje de las horas de riesgo, potencia frente a la regla de la racha y transferencia entre distribuidoras |
| `etl/jobs/00_landing/job_landing_datos_publicos.py` y siguientes | Padrón del INE, Wikidata, relieve de Open-Meteo y OpenStreetMap por Landing, Bronze, Silver y Gold |

Los parámetros están en `config_ml.json` (`analisis_red`, `riesgo_ct`, `horas_riesgo.curva`), `config_bronze_publicos.json` y `config_gold.json` (`contexto`). Las cifras de este documento son de la ejecución `red_20261011T170848_cbfab1` sobre los datos reales.

## 1. Reglas comunes

- **TIEPI como lo calcula Calser.** Interrupciones de nivel CT de más de 180 s, con incidencia asociada y sin el factor cliente. Potencia de cada CT la de TIEPI (`potencia_tiepi_kva`) y potencia instalada la de los CT vigentes ese año. Lo contrastamos con el TIEPI medio de tres años que usó la CNMC para el incentivo de 2019 (Orden TED/749/2022): para 2015-2017 nos sale 0,61 h en EOSA, 0,88 h en Pitarch y 3,22 h en Valle de Santa Ana, frente a 0,67, 1,04 y 3,28 h. La diferencia está entre el 2 % y el 16 %, siempre por debajo, y la atribuimos a las interrupciones de nivel salida y abonado, que Calser declara pero nosotros no contamos.
- **Qué cuenta para qué.** El incentivo de calidad de la CNMC cuenta las programadas (transporte y distribución) y las imprevistas de transporte y propias; las de terceros, fuerza mayor y generación no cuentan. Los límites de calidad zonal e individual del RD 1955/2000 solo cuentan las imprevistas.
- **El apagón del 28.04.2025.** Dejó sin suministro a 749 CT de EOSA durante el día (10,7 h de TIEPI), 563 de Pitarch (7,2 h) y los 10 de Valle de Santa Ana (14,3 h), todo como terceros. Es más que todo el resto de 2025 junto y no dice nada de la red de cada distribuidora, así que lo mostramos aparte y lo quitamos del resto del análisis (`analisis_red.dias_excluidos`).
- **Interrupciones sin incidencia.** El 82 % de los kVA·h de las interrupciones de CT de Calser no tienen incidencia asociada y no cuentan en el TIEPI. Dos tercios de esos kVA·h son registros de más de 72 horas: CT fuera de servicio por obras o bajas, no averías. Quedan fuera.

## 2. De dónde sale el TIEPI

| Distribuidora | 2021 | 2022 | 2023 | 2024 | 2025 | Media de incentivo 2023-2025 |
| --- | --- | --- | --- | --- | --- | --- |
| EOSA | 1,19 h | 0,43 h | 0,55 h | 0,35 h | 0,41 h | 0,37 h |
| Pitarch | 0,30 h | 0,76 h | 0,25 h | 0,39 h | 0,30 h | 0,28 h |
| Valle de Santa Ana | 0,29 h | 0,68 h | 3,20 h | 0,40 h | 0,27 h | 0,27 h |

**Por factor** (2015-2025): las imprevistas propias son el 46,8 % de los kVA·h que cuentan para el TIEPI, las programadas de distribución el 28,2 %, las de terceros el 13,9 % y las de fuerza mayor el 11,1 %.

**Por equipo**: las líneas aéreas de media tensión se llevan el 62 % de los kVA·h (propias 39,1 %, terceros 12,0 %, fuerza mayor 11,1 %), las subterráneas el 4,4 % y los propios CT el 3,5 %. Casi todo el problema está en la línea aérea; el centro de transformación pesa poco.

**Por causa.** Calser no tiene un campo de causa: la leemos del texto libre de la incidencia con patrones (`analisis_red.causas`), y `causas_palabras.csv` deja las palabras más frecuentes para revisarlos. De las 531 incidencias que cuentan, los trabajos programados son el 23,3 % de los kVA·h, los fallos aguas arriba ("ausencia de interconexión con Endesa", subestación) el 21,8 %, la meteorología el 20,0 %, la fauna el 8,8 % (29 incidencias citan nidos de cigüeña en los apoyos), los incendios el 6,6 %, los equipos el 5,8 % y las causas desconocidas el 3,4 %. La vegetación apenas aparece (0,8 %).

**Cuándo.** Diciembre concentra el 21,6 % de los kVA·h imprevistos (temporales de invierno) y agosto y septiembre otro 21 % (tormentas e incendios). Las incidencias de terceros empiezan en horario laboral (lunes a viernes de 8 a 18 h, el 30 % de las horas de la semana) en un 47 % de los casos, frente al 38 % de las propias: son obras y maquinaria, y explican el 14 % del TIEPI.

## 3. Dónde, y cuánto se repite

El TIEPI está muy concentrado en pocas líneas. Llamamos cabecera al elemento aguas arriba que más veces ha cortado cada trafo según el SCADA (en la práctica, la salida de línea de la subestación). El 10 % de las cabeceras (5 de 48) se lleva el 75 % de los kVA·h imprevistos de 2015-2025.

Y se repite. Si ordenamos las cabeceras por los kVA·h de los dos años anteriores, su 10 % superior recoge el 60 % de los kVA·h del año siguiente (el máximo posible con ese tamaño es el 81 %) y su 20 % superior el 74 %. Por CT, el 10 % superior recoge el 32 % (3,2 veces lo que tocaría por azar). Esta persistencia es la base del modelo de riesgo anual del apartado 7: una lista corta de líneas concentra la mayor parte del problema del año que viene.

## 4. Reposición por maniobra: dónde compensa el telemando

En una incidencia que corta varios CT, los que vuelven antes que los últimos se repusieron con maniobras (aislar el tramo averiado y cerrar el resto), no con la reparación. Si hubiera un elemento telemandado en el punto adecuado, esa reposición bajaría de 3 minutos y dejaría de contar en el TIEPI. Entre 2016 y 2025, el 33 % de los kVA·h imprevistos de EOSA y el 31 % de los de Pitarch fueron tiempo de maniobra, con una mediana de 11 minutos por CT: de media 0,19 h de TIEPI al año en EOSA y 0,09 h en Pitarch. Es un máximo, porque supone que siempre había un punto de maniobra entre el CT y la avería.

Con el precio del minuto de TIEPI del apartado 6 y el coste de un interruptor-seccionador telemandado (valor unitario de referencia TI-187A de la Orden IET/2660/2015: 11.174 EUR y 61 EUR al año de mantenimiento, unos 837 EUR al año a 30 años y 5,58 %), en 8 cabeceras el interruptor se paga si evita un 25 % o menos de ese máximo. La primera, R89-1T1 de EOSA, tuvo 43 incidencias en 2023-2025. La lista completa está en `economia_telemando_cabeceras.csv`.

## 5. Meteorología: cuánto sube la probabilidad de avería

| Racha máxima del día | Días | Días con incidencia | Parte de los kVA·h imprevistos |
| --- | --- | --- | --- |
| Menos de 40 km/h | 44,5 % | 1,7 % | 15,2 % |
| 40 - 60 km/h | 38,0 % | 2,4 % | 27,1 % |
| 60 - 80 km/h | 11,6 % | 5,8 % | 33,2 % |
| 80 - 100 km/h | 3,5 % | 6,6 % | 10,9 % |
| 100 km/h o más | 2,3 % | 19,2 % | 13,6 % |

Con lluvia de 30 mm o más en el día la probabilidad de incidencia también llega al 19 %. Una regla sencilla de aviso con racha de 70 km/h o más marca el 10 % de los días y recoge el 38 % de las incidencias y el 30 % de los kVA·h: la probabilidad de incidencia pasa del 3,0 % en un día cualquiera al 10,1 % en un día marcado. Es la base del aviso de brigadas y coincide con lo que vimos en el modelo por horas: la meteorología es la fuente que más aporta y la previsión es lo que falta.

## 6. Cuánto dinero hay detrás

**Incentivo de calidad.** Con la Circular 8/2025 de la CNMC (periodo 2026-2031), el incentivo solo depende del TIEPI: se compara la media de tres años de la empresa con el TIEPI del sector por tipo de zona (1,0648 h en rural concentrada para 2026-2028) y va del -4 % al +4 % de la retribución. Con nuestro TIEPI de incentivo de 2023-2025 y la retribución de 2023 (BOE-A-2026-19941), sin el término de tendencia:

| Distribuidora | TIEPI de incentivo medio | Incentivo estimado | Valor de un minuto | Margen hasta el tope | Pérdida con un año malo |
| --- | --- | --- | --- | --- | --- |
| EOSA | 0,37 h | +3,51 %, 421.000 EUR/año | 12.600 EUR | 59.000 EUR | 220.000 EUR/año (un año como 2021) |
| Pitarch | 0,28 h | +3,55 %, 385.000 EUR/año | 14.900 EUR | 49.000 EUR | 226.000 EUR/año (un año como 2016) |
| Valle de Santa Ana | 0,27 h | +4 % (tope), 8.400 EUR/año | 0 | 0 | 17.000 EUR/año |

Las dos distribuidoras grandes ya están cerca del tope de bonificación: lo que queda por ganar es poco, pero un año malo les quita más de 200.000 EUR al año durante los tres años que dura en la media. El valor de predecir y prevenir está sobre todo en no perder la bonificación que ya tienen.

**Energía no suministrada.** Con un 25 % de utilización de la potencia instalada y un factor de potencia de 0,9, el TIEPI de 2023-2025 (sin el apagón) supone unos 11.000 kWh al año no suministrados en EOSA y 6.900 en Pitarch. Al valor de la energía no suministrada del estudio de CEPA para ACER (7,88 EUR/kWh doméstico) y de la DGPEM (22,88 EUR/kWh, BOE-A-2025-14438), son entre 87.000 y 252.000 EUR al año de coste para los clientes de EOSA y entre 54.000 y 158.000 EUR en Pitarch.

**Límites del RD 1955/2000.** EOSA y Pitarch están muy por debajo de los límites de calidad zonal (en rural concentrada, entre 0,2 y 1,3 h de TIEPI imprevisto frente a 6 h). Valle de Santa Ana los superó en 2016 y 2017 (11,5 y 16,7 h) y en NIEPI en 2017 y 2020. En calidad individual, sin el apagón, solo 2017 (10 CT, 733 clientes) y 2019 (47 CT, 2.515 clientes, unos 3.700 EUR de descuentos del art. 105) pasan los límites de baja tensión. Para estas empresas lo que pesa es el incentivo; el límite legal queda lejos.

## 7. Riesgo de cada CT y cabecera para el año siguiente

`job_ml_riesgo_ct.py` convierte la persistencia del apartado 3 en un modelo: para cada CT y año predice los kVA·h imprevistos que tendrá, con lo que se sabía al empezar el año. Usa la historia del CT y de su cabecera, grupo y municipio en uno, dos y tres años (los kVA·h de la cabecera divididos por su potencia, para que una línea grande no parezca peor solo por su tamaño), la estructura, los cortes y microcortes del SCADA del año anterior, los datos públicos y el clima de su celda. Es un XGBoost con objetivo Tweedie (casi todos los CT tienen cero un año y unos pocos concentran casi todo), evaluado año a año con origen móvil.

[RESULTADOS DEL MODELO DE RIESGO ANUAL]

## 8. Cuántas distribuidoras harían falta

Con unas 30 incidencias al año entre las tres distribuidoras, la pregunta para el MVP es cuántos datos más harían falta para que el modelo por horas aporte algo claro sobre la regla de la racha. `job_ml_horas_riesgo.py --curva-aprendizaje` lo mide de tres formas, con la misma evaluación por años 2023-2025 del modelo por horas (ejecución `hr_20261011T180132_a6b4e6`, sin previsión).

**Curva de aprendizaje.** Para cada año, los modelos aprenden con una parte al azar de los días por distribuidora anteriores (25 %, 50 % y 100 %, tres repeticiones), que es lo que daría una empresa más pequeña o menos años de historia:

| Variante | 25 % de los datos | 50 % | 100 % (233 horas positivas por año de entrenamiento) |
| --- | --- | --- | --- |
| Meteorología, regresión logística | 0,0097 (3,3 veces la tasa base) | 0,0101 (3,4) | 0,0115 (3,9) |
| Las tres fuentes, XGBoost | 0,0066 (2,2) | 0,0083 (2,8) | 0,0095 (3,2) |
| Regla de la racha | 0,0094 (3,2) | | |

Las dos curvas siguen subiendo con todos los datos y la de XGBoost sube más deprisa: el modelo complejo es el que más necesita datos, y con pocos pierde frente al sencillo. Una recta sobre el logaritmo de las horas positivas da, con el doble de datos, una PR-AUC de 0,0122 para la logística y 0,0110 para XGBoost; con diez veces, 0,0143 y 0,0144, en torno a 4,8 veces la tasa base y un 50 % por encima de la regla de la racha. Es una extrapolación con tres puntos, y la leemos como tendencia.

**Potencia.** La regresión logística con la meteorología supera hoy a la regla de la racha en el 84 % de las réplicas del bootstrap por días, sin llegar al 95 %. Si la diferencia se mantuviera, haría falta 3,7 veces más datos para que fuera significativa: unos 33 años-distribuidora del tamaño medio de estas, frente a los 9 actuales (3 distribuidoras por 3 años). Dicho de otra forma, unas 11 distribuidoras parecidas durante tres años, o estas tres durante once. XGBoost con las tres fuentes empata hoy con la regla, y con estos datos no hay cantidad razonable que lo haga significativo.

**Transferencia.** Un modelo entrenado solo con EOSA ordena las horas de Pitarch mejor que el propio modelo de Pitarch (PR-AUC 0,0145 frente a 0,0082) y uno entrenado con Pitarch funciona en EOSA casi igual que el de EOSA (0,0158 frente a 0,0168). En Valle de Santa Ana, con solo 24 horas positivas de entrenamiento al año, el modelo de EOSA da lo mismo que el suyo. El riesgo meteorológico es común a la zona, así que los datos de una distribuidora sirven a las demás: el MVP puede arrancar en una distribuidora nueva con el modelo de las otras y mejorar todos a la vez con cada una que se suma.

Para la propuesta del MVP sacamos de aquí una cifra de trabajo: entre 10 y 12 distribuidoras del mismo SCADA y de la misma zona climática, con tres años de historia cada una, para que el modelo por horas aporte algo demostrable sobre una regla meteorológica. Con previsión meteorológica real la cifra bajaría: con la previsión perfecta del reanálisis el bosque aleatorio ya casi duplicaba la PR-AUC de la regla observada (0,0177 frente a 0,0094, ver [ml-training.md](ml-training.md)).

## 9. Datos públicos

| Fuente | Qué aporta | Acceso | Tablas |
| --- | --- | --- | --- |
| INE, Padrón continuo (DPOP) | Población de cada municipio desde 1996 | API JSON de INEbase, sin clave | `l1_bronze.ine_padron_municipio` -> `l2_silver.f_poblacion_municipio` |
| Wikidata | Superficie y altitud de cada municipio por su código INE | SPARQL, sin clave | `l1_bronze.wikidata_municipio` -> `l2_silver.d_municipio_publico` |
| Open-Meteo Elevation (Copernicus DEM, 90 m) | Relieve de cada celda meteorológica (25 puntos) y altitud de cada municipio | API, sin clave | `l1_bronze.openmeteo_elevacion_punto` -> `l2_silver.d_celda_entorno` |
| OpenStreetMap (Overpass) | Líneas aéreas y cables, apoyos, trafos, subestaciones, carreteras y pistas, bosque, matorral, obras y edificios de cada celda | Overpass API, licencia ODbL | `l1_bronze.osm_elemento_celda` -> `l2_silver.d_celda_entorno` |

Gold las junta en `l3_gold.contexto_ct_anio`: una fila por CT y año con la población del municipio a 1 de enero del año anterior (la que estaba publicada), su variación en uno y en cinco años, la densidad, la potencia y los clientes del municipio por habitante, la altitud y el entorno de su celda. El riesgo anual las usa como un bloque más.

Lo que aprendimos de ellas:

- La población de los 73 municipios está estancada o baja (Hervás pasa de 3.976 habitantes en 2019 a 3.898 en 2025; Valle de Santa Ana de 1.139 a 1.090). En esta zona la demanda crecerá por la electrificación (bomba de calor, coche eléctrico, autoconsumo) y la población apenas aportará; ese crecimiento solo lo mide la curva de carga.
- Obras públicas (Plataforma de Contratación del Sector Público), incendios (Estadística General de Incendios Forestales) y rayos (AEMET) serían útiles, pero no las incorporamos: las obras explican como mucho el 14 % del TIEPI (terceros) y los ficheros de PLACSP son de cientos de MB al mes, los incendios están consolidados solo hasta 2021 y los rayos históricos de AEMET no son abiertos. Quedan como propuesta.

## 10. Lo que falta para predecir mejor

- **La carga de los transformadores.** Para saber qué CT necesita un segundo trafo hace falta su curva de carga, y el SCADA solo tiene serie de potencia en un CT (los otros tres puntos con potencia son generación). Sí tenemos la intensidad de las cabeceras de las subestaciones (`carga_cabeceras_*.csv`) y los kVA por cliente de cada CT (`cts_kva_por_abonado.csv`), que ordenan dónde mirar primero. La fuente buena son las curvas horarias de los contadores de telegestión, que la distribuidora ya tiene de todos sus clientes: sumadas por CT dan la carga exacta de cada trafo.
- **Una causa estructurada.** Hoy la causa está en texto libre y la mitad de las veces describe la actuación de la protección ("bloqueo del interruptor") y no la causa. Un campo con una lista cerrada (viento, rayo, fauna, vegetación, terceros, equipo, desconocida) en Calser permitiría entrenar un modelo por causa.
- **Telemando en las cabeceras prioritarias**, que además de reducir el TIEPI daría las señales de defecto y de paso de falta que el modelo por horas echa de menos.
- **Más distribuidoras**, en la cantidad que estima el apartado 8.

## 11. Cómo lanzarlo

Desde el contenedor `spark-master` (los lanzadores de `_runlogs` hacen lo mismo y copian los resultados):

```
docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/01_bronze/job_bronze_datos_publicos.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/02_silver/job_silver_contexto_publico.py
docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/03_gold/job_gold_contexto_ct.py --config /app/config/03_gold/config_gold.json

docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_analisis_red.py \
    /app/jobs/04_ml/job_analisis_red.py --config /app/config/04_ml/config_ml.json
```

`job_ml_riesgo_ct.py` y `job_ml_horas_riesgo.py --curva-aprendizaje` se lanzan igual. El análisis de red tarda menos de dos minutos, el riesgo anual unos minutos y la curva de aprendizaje unos 15 minutos. La descarga de OpenStreetMap depende de los servidores de Overpass: una celda que falla se vuelve a pedir en la siguiente ejecución.
