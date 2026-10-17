# Análisis de red, riesgo anual y datos públicos

El riesgo del día siguiente responde a "cuándo": qué días y en qué franja horaria hace falta una brigada preparada ([causas-incidencias.md](causas-incidencias.md)). A una distribuidora le interesa también "dónde" y "qué hacer": qué líneas hay que revisar o podar este año, dónde compensa poner un elemento telemandado, qué transformadores se están quedando cortos y cuánto dinero hay detrás de cada decisión. Este documento recoge el análisis que hicimos con la historia de Calser, TedisNet y la meteorología para contestar esas preguntas, los datos públicos que añadimos y lo que nos falta para hacerlo mejor.

| Fichero | Qué hace |
| --- | --- |
| `etl/jobs/04_ml/job_analisis_red.py` | Análisis de red sobre Gold: TIEPI por factor y causa, límites del RD 1955/2000, concentración y persistencia, reposición por maniobra, meteorología, carga de la red y valor económico |
| `etl/jobs/04_ml/job_ml_riesgo_ct.py` | Riesgo de cada CT y cabecera para el año siguiente, evaluado por captura, y el ranking del próximo año |
| `etl/jobs/04_ml/job_ml_horas_riesgo.py --curva-aprendizaje` | Curva de aprendizaje de las horas de riesgo, potencia frente a la regla de la racha y transferencia entre distribuidoras |
| `etl/jobs/00_landing/job_landing_datos_publicos.py` y siguientes | Padrón del INE, Wikidata, relieve de Open-Meteo, OpenStreetMap, obras públicas de PLACSP, edificios del Catastro y proyecciones de población del IEEX por Landing, Bronze, Silver y Gold |

Los parámetros están en `config_ml.json` (`analisis_red`; el riesgo anual y la curva de aprendizaje usan los valores por defecto de su job, que se cambian con un bloque `riesgo_ct` o `horas_riesgo.curva`), `config_bronze_publicos.json` y `config_gold.json` (`contexto`). Las cifras de este documento son de las ejecuciones `red_20261011T215346_46427f` (análisis de red) y `rct_20261011T215018_d996ba` (riesgo anual) sobre los datos reales.

## 1. Reglas comunes

- **TIEPI como lo calcula Calser.** Interrupciones de nivel CT de más de 180 s, con incidencia asociada y sin el factor cliente. Potencia de cada CT la de TIEPI (`potencia_tiepi_kva`) y potencia instalada la de los CT vigentes ese año. Lo contrastamos con el TIEPI medio de tres años que usó la CNMC para el incentivo de 2019 (Orden TED/749/2022): para 2015-2017 nos sale 0,61 h en la Distribuidora 1, 0,88 h en la Distribuidora 2 y 3,22 h en la Distribuidora 3, frente a 0,67, 1,04 y 3,28 h. La diferencia está entre el 2 % y el 16 %, siempre por debajo, y la atribuimos a las interrupciones de nivel salida y abonado, que Calser declara pero nosotros no contamos.
- **Qué cuenta para qué.** El incentivo de calidad de la CNMC cuenta las programadas (transporte y distribución) y las imprevistas de transporte y propias; las de terceros, fuerza mayor y generación no cuentan. Los límites de calidad zonal e individual del RD 1955/2000 solo cuentan las imprevistas.
- **El apagón del 28.04.2025.** Dejó sin suministro a 749 CT de la Distribuidora 1 durante el día (10,7 h de TIEPI), 563 de la Distribuidora 2 (7,2 h) y los 10 de la Distribuidora 3 (14,3 h), todo como terceros. Es más que todo el resto de 2025 junto y no dice nada de la red de cada distribuidora, así que lo mostramos aparte y lo quitamos del resto del análisis (`analisis_red.dias_excluidos`).
- **Interrupciones sin incidencia.** El 82 % de los kVA·h de las interrupciones de CT de Calser no tienen incidencia asociada y no cuentan en el TIEPI. Dos tercios de esos kVA·h son registros de más de 72 horas: CT fuera de servicio por obras o bajas, no averías. Quedan fuera, como en Calser: una interrupción sin incidencia está pendiente de procesar o la distribuidora no quiere contarla, y cuenta cuando se le asigna una.

## 2. De dónde sale el TIEPI

| Distribuidora | 2021 | 2022 | 2023 | 2024 | 2025 | Media de incentivo 2023-2025 |
| --- | --- | --- | --- | --- | --- | --- |
| Distribuidora 1 | 1,19 h | 0,43 h | 0,55 h | 0,35 h | 0,41 h | 0,37 h |
| Distribuidora 2 | 0,30 h | 0,76 h | 0,25 h | 0,39 h | 0,30 h | 0,28 h |
| Distribuidora 3 | 0,29 h | 0,68 h | 3,20 h | 0,40 h | 0,27 h | 0,27 h |

**Frente al sector.** El Ministerio publica en GECOS el TIEPI oficial de todo el sector por tipo de zona y por causa, para España, cada comunidad y cada provincia (de 2003 a 2021; publica con varios años de retraso). Lo cargamos como `l1_bronze.gecos_indice_zonal` (`extract_gecos_indices.py`, fuente `calidad_sector`) y la sección 1b del análisis compara nuestro TIEPI imprevisto por zona con el oficial (`tiepi_sector.csv`, `sector_series.csv` y `sector.json` para `negocio.html`):

| Zona | Clientes | Distribuidora 1 2023-2025 | Distribuidora 2 2023-2025 | Cáceres 2019-2021 | Extremadura 2019-2021 | España 2019-2021 |
| --- | --- | --- | --- | --- | --- | --- |
| Urbana | Distribuidora 2: 4.310 | - | 0,03 h | 0,16 h | 0,47 h | 0,59 h |
| Semiurbana | Distribuidora 1: 6.146; Distribuidora 2: 7.293 | 0,22 h | 0,07 h | 0,41 h | 0,57 h | 0,89 h |
| Rural concentrada | Distribuidora 1: 30.658; Distribuidora 2: 16.664 | 0,38 h | 0,20 h | 1,06 h | 1,26 h | 1,67 h |
| Rural dispersa | Distribuidora 1: 581; Distribuidora 2: 470 | 0,46 h | 0,24 h | 1,43 h | 1,96 h | 2,55 h |

Con su mezcla de zonas, el TIEPI imprevisto de la Distribuidora 1 (0,34 h al año) es un tercio del que tendría el sector de Cáceres con esa misma red (0,92 h) y menos de un cuarto del de España (1,51 h); el de la Distribuidora 2 (0,12 h) es un quinto del de Cáceres (0,63 h). En los mismos años que el sector (2019-2021) la Distribuidora 1 estaba en la media de Cáceres en rural concentrada (1,12 h frente a 1,06 h) por un solo año, 2019, en el que el 92 % de su TIEPI fue fuerza mayor; la Distribuidora 2, en 0,31 h. Dos salvedades: nuestro TIEPI queda entre un 2 % y un 16 % por debajo del declarado (arriba) y la tendencia del sector es a la baja, así que la distancia real es algo menor que la de la tabla. Sobre el TIEPI de incentivo (programadas, transporte y propias) la referencia de la CNMC para 2026-2028 (1,0648 h en rural concentrada) está en la línea del valor nacional de 2021 (1,11 h); la Distribuidora 1 (0,40 h en rural concentrada en 2023-2025) y la Distribuidora 2 (0,33 h) quedan muy por debajo, que es lo que les da la bonificación del apartado 6. Y la serie nacional dice lo mismo que nuestras redes: el TIEPI por fuerza mayor se multiplica por 20 en los años de grandes temporales (0,88 h en 2009 con Klaus y 1,06 h en 2010 con Xynthia, frente a 0,04 h en 2008) y vuelve a casi cero al siguiente.

**Por factor** (2015-2025): las imprevistas propias son el 46,8 % de los kVA·h que cuentan para el TIEPI, las programadas de distribución el 28,2 %, las de terceros el 13,9 % y las de fuerza mayor el 11,1 %.

**Por equipo**: las líneas aéreas de media tensión se llevan el 62 % de los kVA·h (propias 39,1 %, terceros 12,0 %, fuerza mayor 11,1 %), las subterráneas el 4,4 % y los propios CT el 3,5 %. Casi todo el problema está en la línea aérea; el centro de transformación pesa poco.

**Por causa.** Calser no tiene un campo de causa: la leemos del texto libre de la incidencia con patrones (`analisis_red.causas`), y `causas_palabras.csv` deja las palabras más frecuentes para revisarlos. De las 531 incidencias que cuentan, los trabajos programados son el 23,3 % de los kVA·h, los fallos aguas arriba ("ausencia de interconexión con Endesa", subestación) el 21,8 %, la meteorología el 20,0 %, la fauna el 8,8 % (29 incidencias citan nidos de cigüeña en los apoyos), los incendios el 6,6 %, los equipos el 5,8 % y las causas desconocidas el 3,4 %. La vegetación apenas aparece (0,8 %). El análisis completo de causas, con reglas más finas, el módulo de Extremadura de Calser y la meteorología de cada causa, está en [causas-incidencias.md](causas-incidencias.md): con esas reglas los equipos pesan el 22,7 % del TIEPI de 2016 - 2025, la meteorología el 17,0 %, las desconocidas el 11,9 % y la fauna el 7,8 %.

**Cuándo.** Diciembre concentra el 21,6 % de los kVA·h imprevistos (temporales de invierno) y agosto y septiembre otro 21 % (tormentas e incendios). Las incidencias de terceros empiezan en horario laboral (lunes a viernes de 8 a 18 h, el 30 % de las horas de la semana) en un 47 % de los casos, frente al 38 % de las propias: son obras y maquinaria, y explican el 14 % del TIEPI.

## 3. Dónde, y cuánto se repite

El TIEPI está muy concentrado en pocas líneas. Llamamos cabecera al elemento aguas arriba que más veces ha cortado cada trafo según el SCADA (en la práctica, la salida de línea de la subestación). El 10 % de las cabeceras (5 de 48) se lleva el 75 % de los kVA·h imprevistos de 2015-2025.

Y se repite. Si ordenamos las cabeceras por los kVA·h de los dos años anteriores, su 10 % superior recoge el 60 % de los kVA·h del año siguiente (el máximo posible con ese tamaño es el 81 %) y su 20 % superior el 74 %. Por CT, el 10 % superior recoge el 32 % (3,2 veces lo que tocaría por azar). Esta persistencia es la base del modelo de riesgo anual del apartado 7: una lista corta de líneas concentra la mayor parte del problema del año que viene.

## 4. Reposición por maniobra: dónde compensa el telemando

En una incidencia que corta varios CT, los que vuelven antes que los últimos se repusieron con maniobras (aislar el tramo averiado y cerrar el resto), no con la reparación. Si hubiera un elemento telemandado en el punto adecuado, esa reposición bajaría de 3 minutos y dejaría de contar en el TIEPI. Entre 2016 y 2025, el 33 % de los kVA·h imprevistos de la Distribuidora 1 y el 31 % de los de la Distribuidora 2 fueron tiempo de maniobra, con una mediana de 11 minutos por CT: de media 0,19 h de TIEPI al año en la Distribuidora 1 y 0,09 h en la Distribuidora 2. Es un máximo, porque supone que siempre había un punto de maniobra entre el CT y la avería.

Con el precio del minuto de TIEPI del apartado 6 y el coste de un interruptor-seccionador telemandado (valor unitario de referencia TI-187A de la Orden IET/2660/2015: 11.174 EUR y 61 EUR al año de mantenimiento, unos 837 EUR al año a 30 años y 5,58 %), en 8 cabeceras el interruptor se paga si evita un 25 % o menos de ese máximo. La primera, R89-1T1 de la Distribuidora 1, tuvo 43 incidencias en 2023-2025. La lista completa está en `economia_telemando_cabeceras.csv`.

## 5. Meteorología: cuánto sube la probabilidad de avería

| Racha máxima del día | Días | Días con incidencia | Parte de los kVA·h imprevistos |
| --- | --- | --- | --- |
| Menos de 40 km/h | 44,5 % | 1,7 % | 15,2 % |
| 40 - 60 km/h | 38,0 % | 2,4 % | 27,1 % |
| 60 - 80 km/h | 11,6 % | 5,8 % | 33,2 % |
| 80 - 100 km/h | 3,5 % | 6,6 % | 10,9 % |
| 100 km/h o más | 2,3 % | 19,2 % | 13,6 % |

Con lluvia de 30 mm o más en el día la probabilidad de incidencia también llega al 19 %. Una regla sencilla de aviso con racha de 70 km/h o más marca el 10 % de los días y recoge el 38 % de las incidencias y el 30 % de los kVA·h: la probabilidad de incidencia pasa del 3,0 % en un día cualquiera al 10,1 % en un día marcado. Es la base del aviso de brigadas y coincide con lo que vimos en el modelo por horas: la meteorología es la fuente que más aporta. Por eso el riesgo del día siguiente se calcula con la previsión de ICON-EU de la víspera y los avisos de AEMET ([causas-incidencias.md](causas-incidencias.md), apartado 8).

## 6. Cuánto dinero hay detrás

**Incentivo de calidad.** Con la Circular 8/2025 de la CNMC (periodo 2026-2031), el incentivo solo depende del TIEPI: se compara la media de tres años de la empresa con el TIEPI del sector por tipo de zona (1,0648 h en rural concentrada para 2026-2028) y va del -4 % al +4 % de la retribución. Con nuestro TIEPI de incentivo de 2023-2025 y la retribución de 2023 (BOE-A-2026-19941), sin el término de tendencia:

| Distribuidora | TIEPI de incentivo medio | Incentivo estimado | Valor de un minuto | Margen hasta el tope | Pérdida con un año malo |
| --- | --- | --- | --- | --- | --- |
| Distribuidora 1 | 0,37 h | +3,51 %, 421.000 EUR/año | 12.600 EUR | 59.000 EUR | 220.000 EUR/año (un año como 2021) |
| Distribuidora 2 | 0,28 h | +3,55 %, 385.000 EUR/año | 14.900 EUR | 49.000 EUR | 226.000 EUR/año (un año como 2016) |
| Distribuidora 3 | 0,27 h | +4 % (tope), 8.400 EUR/año | 0 | 0 | 17.000 EUR/año |

Las dos distribuidoras grandes ya están cerca del tope de bonificación: lo que queda por ganar es poco, pero un año malo les quita más de 200.000 EUR al año durante los tres años que dura en la media. El valor de predecir y prevenir está sobre todo en no perder la bonificación que ya tienen.

**Energía no suministrada.** Con un 25 % de utilización de la potencia instalada y un factor de potencia de 0,9, el TIEPI de 2023-2025 (sin el apagón) supone unos 11.000 kWh al año no suministrados en la Distribuidora 1 y 6.900 en la Distribuidora 2. Al valor de la energía no suministrada del estudio de CEPA para ACER (7,88 EUR/kWh doméstico) y de la DGPEM (22,88 EUR/kWh, BOE-A-2025-14438), son entre 87.000 y 252.000 EUR al año de coste para los clientes de la Distribuidora 1 y entre 54.000 y 158.000 EUR en la Distribuidora 2.

**Límites del RD 1955/2000.** La Distribuidora 1 y la Distribuidora 2 están muy por debajo de los límites de calidad zonal (en rural concentrada, entre 0,2 y 1,3 h de TIEPI imprevisto frente a 6 h). La Distribuidora 3 los superó en 2016 y 2017 (11,5 y 16,7 h) y en NIEPI en 2017 y 2020. En calidad individual, sin el apagón, solo 2017 (10 CT, 733 clientes) y 2019 (47 CT, 2.515 clientes, unos 3.700 EUR de descuentos del art. 105) pasan los límites de baja tensión. Para estas empresas lo que pesa es el incentivo; el límite legal queda lejos.

**Límites mensuales de Extremadura.** Extremadura añadió en 2018 sus propios límites mensuales (art. 9.6 de la Ley 2/2002, en la redacción del Decreto-ley 1/2018, DOE 227 de 22.11.2018, BOE-A-2019-1793): un TIEPI mensual de 0,88 h y un NIEPI mensual de 0,74 en zona rural concentrada, y 1,32 h y 1,10 en rural dispersa. Pasarlos dos meses naturales seguidos obliga a la distribuidora a presentar medidas en dos meses y está tipificado como infracción; según la prensa, el acuerdo de 2019 entre la Junta y el Gobierno rebajó esa obligación (solo fuente secundaria). La sección 2 del análisis calcula ahora cada mes por distribuidora, provincia y tipo de zona (`limites_mensuales_extremadura.csv` y la racha de meses seguidos), con el mismo alcance que el art. 106 (solo imprevistas), que es una suposición nuestra: la tabla sale de un texto consolidado y no del DOE. Un mes con un temporal basta para pasar el límite mensual, así que este es el límite que de verdad está cerca de nuestras redes, y la pregunta es si esos meses son los que la víspera había marcado (`experimentos/evaluacion/limites_mensuales.py`).

**Otras normas que hemos revisado (17.10.2026).** La Circular 8/2025 reconoce como inversión de "Tipo 2" (despachos, digitalización y sistemas informáticos) los sistemas técnicos de gestión (código I29003XX) y las aplicaciones informáticas (I2900420), con cinco años de vida útil: es el hueco regulatorio de un software como GridPredic. Los límites del incentivo son del 2 % de la retribución del sector y del 4 % de la de cada empresa, en positivo y en negativo. La disposición transitoria cuarta permitía a las distribuidoras de menos de 10.000 clientes pedir que no se les aplicara el TOTEX en 2029 - 2031, con plazo hasta el 01.10.2026, ya vencido. El RD 641/2026 (BOE-A-2026-16662) fija 762 millones de euros para 2025 - 2026 en ayudas de digitalización de las redes de distribución, dinero de nuestros clientes y no nuestro. Como precedente de fuera, la delibera 31/2018/R/eel de ARERA obliga a las distribuidoras italianas a tener un plan de resiliencia frente a manguitos de hielo y nieve, inundaciones, olas de calor, vendavales y caída de árboles, con un índice de riesgo que multiplica la probabilidad del evento por los usuarios que dejaría sin suministro.

## 7. Riesgo de cada CT y cabecera para el año siguiente

`job_ml_riesgo_ct.py` convierte la persistencia del apartado 3 en un modelo: para cada CT y año predice los kVA·h imprevistos que tendrá, con lo que se sabía al empezar el año. Usa la historia del CT y de su cabecera, grupo y municipio en uno, dos y tres años (los kVA·h de la cabecera divididos por su potencia, para que una línea grande no parezca peor solo por su tamaño), la estructura, los cortes y microcortes del SCADA del año anterior, los datos públicos y el clima de su celda. Es un XGBoost con objetivo Tweedie (casi todos los CT tienen cero un año y unos pocos concentran casi todo), evaluado año a año con origen móvil.

Resultado de la ejecución `rct_20261011T215018_d996ba`, media de los años evaluados 2021-2025: parte de los kVA·h imprevistos del año que cae en el top k % del ranking hecho con los años anteriores.

| Nivel | Top | Naif (dos años anteriores) | Mejor combinación | Modelo con todo | Máximo posible |
| --- | --- | --- | --- | --- | --- |
| Cabecera | 10 % | 63,5 % | 70,9 % (historia y estructura) | 66,4 % | 80,7 % |
| Cabecera | 20 % | 74,3 % | 82,9 % (historia, estructura y SCADA) | 81,1 % | 92,9 % |
| CT | 10 % | 27,9 % | 30,9 % (historia, estructura y clima) | 28,7 % | 68,2 % |
| CT | 20 % | 47,1 % | 52,1 % (historia, estructura y clima) | 47,6 % | 85,7 % |

Por cabecera el modelo cierra casi la mitad de la distancia entre la lista naif y el máximo: las 5 cabeceras que señala (el 10 % de 47) recogen el 71 % de los kVA·h imprevistos del año siguiente, frente al 64 % si se ordenan por los dos años anteriores, y gana a la lista naif en cuatro de los cinco años. Por CT la mejora es pequeña (31 % frente a 28 %): la historia dice qué línea va a fallar, pero no en qué CT de esa línea caerá la avería, y eso es casi azar. La unidad de decisión útil es la cabecera, que es además donde se actúa con la revisión y poda de la línea o con el telemando.

Los bloques nuevos aportan poco a este horizonte. El clima de la celda sube algo la captura por CT y el SCADA la de las cabeceras en el top 20 %; los datos públicos y el bloque de obras y edificación (70,4 % por cabecera y 25,7 % por CT en el top 10 %) no la mejoran, y juntar todos los bloques la empeora un poco. La variable con más ganancia del modelo completo es el relieve de la celda (desviación de la altitud), pero coincide con las líneas de montaña del Jerte y del Ambroz que la historia ya marca. Con cinco años de evaluación y unas 30 incidencias al año, diferencias de uno o dos puntos entre bloques no son significativas.

Para 2026, el modelo con todos los años pone en cabeza R89-1T1 (de la Distribuidora 1, con 491 CT), CC52-3 y CECLAV_2 (de la Distribuidora 2); las cinco primeras cabeceras concentran el 77 % de los kVA·h esperados (`prioridades_cabecera.csv` y `l3_gold.ml_riesgo_ct_ranking`). R89-1T1 es también la primera de la lista del telemando del apartado 4, así que es la candidata clara a una actuación combinada: revisión de la línea y un punto de maniobra telemandado.

## 8. Cuántas distribuidoras harían falta

Con unas 30 incidencias al año entre las tres distribuidoras, la pregunta para el MVP es cuántos datos más harían falta para que el modelo por horas aporte algo claro sobre la regla de la racha. `job_ml_horas_riesgo.py --curva-aprendizaje` lo mide de tres formas, con la misma evaluación por años 2023-2025 del modelo por horas (ejecución `hr_20261011T180132_a6b4e6`, sin previsión).

**Curva de aprendizaje.** Para cada año, los modelos aprenden con una parte al azar de los días por distribuidora anteriores (25 %, 50 % y 100 %, tres repeticiones), que es lo que daría una empresa más pequeña o menos años de historia:

| Variante | 25 % de los datos | 50 % | 100 % (233 horas positivas por año de entrenamiento) |
| --- | --- | --- | --- |
| Meteorología, regresión logística | 0,0097 (3,3 veces la tasa base) | 0,0101 (3,4) | 0,0115 (3,9) |
| Las tres fuentes, XGBoost | 0,0066 (2,2) | 0,0083 (2,8) | 0,0095 (3,2) |
| Regla de la racha | 0,0094 (3,2) | | |

Las dos curvas siguen subiendo con todos los datos y la de XGBoost sube más deprisa: el modelo complejo es el que más necesita datos, y con pocos pierde frente al sencillo. Una recta sobre el logaritmo de las horas positivas da, con el doble de datos, una PR-AUC de 0,0122 para la logística y 0,0110 para XGBoost; con diez veces, 0,0143 y 0,0144, en torno a 4,8 veces la tasa base y un 50 % por encima de la regla de la racha. Es una extrapolación con tres puntos, y la leemos como tendencia.

**Potencia.** La regresión logística con la meteorología supera hoy a la regla de la racha en el 84 % de las réplicas del bootstrap por días, sin llegar al 95 %. Si la diferencia se mantuviera, haría falta 3,7 veces más datos para que fuera significativa: unos 33 años-distribuidora del tamaño medio de estas, frente a los 9 actuales (3 distribuidoras por 3 años). Son unas 11 distribuidoras parecidas durante tres años, o estas tres durante once. XGBoost con las tres fuentes empata hoy con la regla, y con estos datos no hay cantidad razonable que lo haga significativo.

**Transferencia.** Un modelo entrenado solo con la Distribuidora 1 ordena las horas de la Distribuidora 2 mejor que el propio modelo de la Distribuidora 2 (PR-AUC 0,0145 frente a 0,0082) y uno entrenado con la Distribuidora 2 funciona en la Distribuidora 1 casi igual que el de la Distribuidora 1 (0,0158 frente a 0,0168). En la Distribuidora 3, con solo 24 horas positivas de entrenamiento al año, el modelo de la Distribuidora 1 da lo mismo que el suyo. El riesgo meteorológico es común a la zona, así que los datos de una distribuidora sirven a las demás: el MVP puede arrancar en una distribuidora nueva con el modelo de las otras y mejorar todos a la vez con cada una que se suma.

Para la propuesta del MVP sacamos de aquí una cifra de trabajo: entre 10 y 12 distribuidoras del mismo SCADA y de la misma zona climática, con tres años de historia cada una, para que el modelo por horas aporte algo demostrable sobre una regla meteorológica. Con previsión meteorológica real la cifra bajaría: con la previsión perfecta del reanálisis el bosque aleatorio ya casi duplicaba la PR-AUC de la regla observada (0,0177 frente a 0,0094, ver [ml-training.md](ml-training.md)).

## 9. Datos públicos

| Fuente | Qué aporta | Acceso | Tablas |
| --- | --- | --- | --- |
| INE, Padrón continuo (DPOP) | Población de cada municipio desde 1996 | API JSON de INEbase, sin clave | `l1_bronze.ine_padron_municipio` -> `l2_silver.f_poblacion_municipio` |
| Wikidata | Superficie y altitud de cada municipio por su código INE | SPARQL, sin clave | `l1_bronze.wikidata_municipio` -> `l2_silver.d_municipio_publico` |
| Open-Meteo Elevation (Copernicus DEM, 90 m) | Relieve de cada celda meteorológica (25 puntos) y altitud de cada municipio | API, sin clave | `l1_bronze.openmeteo_elevacion_punto` -> `l2_silver.d_celda_entorno` |
| OpenStreetMap (Overpass) | Líneas aéreas y cables, apoyos, trafos, subestaciones, carreteras y pistas, bosque, matorral, obras y edificios de cada celda | Overpass API, licencia ODbL | `l1_bronze.osm_elemento_celda` -> `l2_silver.d_celda_entorno` |
| Plataforma de Contratación del Sector Público | Licitaciones de obra de las dos provincias desde 2018: órgano, objeto, CPV, importe, plazo, adjudicación y lugar | Ficheros anuales abiertos (ATOM CODICE, hasta 2,2 GB por año), filtrados a la zona al descargarlos | `l1_bronze.placsp_licitacion_obra` -> `l2_silver.f_licitacion_obra` |
| Catastro, INSPIRE Buildings | Edificios de cada municipio con año de construcción, uso, viviendas y superficie | Descarga ATOM por municipio, sin clave | `l1_bronze.catastro_edificio` -> `l2_silver.f_edificacion_municipio` |
| Instituto de Estadística de Extremadura | Proyección de población 2024-2039 por agrupación de municipios (AME) y la AME de cada municipio | Excel 97 en 7z y un mapa en PDF, pasados una vez a CSV con `extract_ieex_proyecciones.py` | `l1_bronze.ieex_proyeccion_ame` -> `l2_silver.f_proyeccion_ame`, `d_ame_municipio` |
| GECOS (MITECO), calidad de suministro | TIEPI y NIEPI oficiales del sector por tipo de zona y causa: España, comunidades y provincias, 2003-2021 | Páginas públicas de GECOS, pasadas una vez al año a CSV con `extract_gecos_indices.py` (fuente `calidad_sector`) | `l1_bronze.gecos_indice_zonal` (lo lee `job_analisis_red.py`, sección 1b) |

Gold las junta en `l3_gold.contexto_ct_anio`: una fila por CT y año con la población del municipio a 1 de enero del año anterior (la que estaba publicada), su variación en uno y en cinco años, la densidad, la potencia y los clientes del municipio por habitante, la altitud y el entorno de su celda, las obras licitadas el año anterior y los meses de obra previstos para el año, y las viviendas construidas según el Catastro. El riesgo anual las usa en dos bloques, `publicos` y `obras`. Aparte, `l3_gold.obras_municipio_anio` guarda por municipio y año las obras publicadas y en ejecución y la edificación, para el cruce con las averías del apartado siguiente.

De PLACSP nos quedamos con el fichero anual de todo el país, filtrado al descargarlo a las entradas de obra (contrato de tipo obras o CPV 45) con marca de Extremadura: unas 32.500 versiones de licitaciones entre 2018 y 2025, de más de 3,3 millones de entradas. El municipio sale del código DIR3 cuando licita un ayuntamiento y, si no, de los nombres de municipio del título (con peso 1/n si hay varios, y sin contar como capital la provincia que sigue al nombre del pueblo). Las obras que abren zanja o calle (`mov_tierras`) se marcan por su CPV (movimiento de tierras, obra civil, conducciones, carreteras, agua) o por palabras del título (calle, pavimentación, abastecimiento, saneamiento...).

## 10. Obras, crecimiento y capacidad

**Obras públicas y averías de terceros.** La idea de partida era sencilla: una retroexcavadora o un camión en una obra rompe un cable o se lleva un apoyo, y las obras públicas se licitan meses antes, así que deberían anticipar las averías de terceros, que son el 14 % del TIEPI. La hemos comprobado con un panel de 511 filas (distribuidora, municipio y año, 2019-2025) que junta las incidencias imprevistas que dejaron sin servicio algún CT del municipio con las obras públicas en ejecución ese año que abren zanja o calle. Contamos tres tipos de incidencia: las de terceros (factor de Calser), las que el texto atribuye a máquinas u obras (excavadora, camión, obra, grúa...) y las propias, que hacen de control porque una obra no debería cambiarlas.

| Años del municipio | CT-año | Terceros por 100 CT-año | Máquinas y obras | Propias |
| --- | --- | --- | --- | --- |
| Sin obra que abra calle | 2.197 | 2,9 | 0,50 | 11,3 |
| Hasta 6 meses de obra | 4.315 | 0,9 | 0,30 | 8,6 |
| Más de 6 meses de obra | 2.943 | 0,7 | 0,20 | 5,0 |

El resultado es el contrario del esperado. Los años con obra tienen menos averías de terceros por CT, pero también menos propias: son los pueblos más grandes, con más CT y más red subterránea, los que licitan obras casi todos los años. Si comparamos cada municipio consigo mismo (años con obra frente a años sin obra, con efectos de municipio y de año), la razón de tasas de las averías de terceros queda en 0,66, con un intervalo del 90 % entre 0,51 y 1,08, y la de las propias en 0,95. Con las obras licitadas antes de empezar el año, que es lo que una distribuidora sabría a tiempo, sale 0,76 (entre 0,49 y 1,18). Medido de forma continua, con los meses de obra o con los edificios en construcción, tampoco aparece ninguna relación positiva.

Con estos datos, las obras públicas por municipio y año no anticipan las averías de terceros. La escala es demasiado gruesa: lo que cuenta es si una zanja concreta pasa por encima de una línea enterrada, y eso pide obras con su ubicación (licencias municipales con dirección y, sobre todo, las consultas de planos de redes que las constructoras hacen antes de excavar), no licitaciones. Lo dejamos medido para no recomendar una fuente que no aporta, y por la misma razón el bloque de obras no mejora el riesgo anual del apartado 7.

**Crecimiento y potencia.** Para saber si algún municipio va a necesitar potencia nueva cruzamos tres fuentes: las viviendas construidas según el Catastro, la proyección de población del IEEX y los kVA de los CT. En los últimos cinco años completos del Catastro (2020-2024) ningún municipio ha construido lo bastante para pedir un 5 % más de potencia si siguiera al mismo ritmo y con los kVA por cliente de hoy; el que más, Oliva de Plasencia, un 3 %. La proyección oficial va en la misma dirección: la población baja en casi todas las agrupaciones de municipios donde están los CT y solo sube algo en el Valle del Jerte y en Cáceres.

| Agrupación de municipios (AME) | Municipios | CT | Población 2024-2039 |
| --- | --- | --- | --- |
| Trasierra - Tierras de Granadilla - Valle del Ambroz - Hurdes | 16 | 279 | -13,0 % |
| Sierra de Montánchez | 21 | 315 | -10,8 % |
| Rivera de Fresnedosa | 9 | 142 | -9,5 % |
| Tajo - Salor | 8 | 209 | -7,7 % |
| Sierra Suroeste (la Distribuidora 3) | 1 | 10 | -6,4 % |
| Trujillo | 2 | 28 | -5,9 % |
| Riberos del Tajo | 3 | 55 | -0,5 % |
| Valle del Jerte | 12 | 245 | +1,0 % |
| Cáceres | 1 | 55 | +1,5 % |

En esta zona no va a hacer falta más potencia por población ni por construcción nueva. Si hace falta, será por la electrificación de lo que ya existe (bomba de calor, coche eléctrico, autoconsumo), y para eso los datos señalan dónde mirar primero: los 196 CT con menos de 2 kVA instalados por cliente y las cabeceras cuya punta crece de forma sostenida, como V52-01 de la Distribuidora 1 (de 37 a 73 A entre 2021 y 2025, con 358 CT), J252-5 (de 117 a 145 A entre 2023 y 2025) o A52-1 de la Distribuidora 2 (de 26 a 52 A entre 2021 y 2024). Dos CT tienen en Calser una potencia que no es creíble (10 kVA para 330 clientes en Torremocha, 14 kVA para 94 en Casas de Millán) y conviene revisar su ficha. Para confirmar qué trafo concreto va al límite hace falta su curva de carga.

Y lo que aprendimos del resto de fuentes: la población de los 73 municipios está estancada o baja (Hervás pasa de 3.976 habitantes en 2019 a 3.898 en 2025; el municipio de la Distribuidora 3, de 1.139 a 1.090), y el relieve de la celda pesa en el riesgo anual, pero porque marca las líneas de montaña que la historia ya conoce. Los incendios y los rayos, que aquí quedaban como propuesta, entraron después: las áreas quemadas de EFFIS, los focos de FIRMS y el peligro de incendio de CEMS, y los rayos del satélite MTG de EUMETSAT desde julio de 2024 ([causas-incidencias.md](causas-incidencias.md), apartado 7). La Estadística General de Incendios Forestales (consolidada solo hasta 2021) y AEMET, que no tiene archivo abierto de rayos, quedaron descartadas.

## 11. Lo que falta para predecir mejor

- **La carga de los transformadores.** Para saber qué CT necesita un segundo trafo hace falta su curva de carga, y el SCADA solo tiene serie de potencia en un CT (los otros tres puntos con potencia son generación). Sí tenemos la intensidad de las cabeceras de las subestaciones (`carga_cabeceras_*.csv`) y los kVA por cliente de cada CT (`cts_kva_por_abonado.csv`), que ordenan dónde mirar primero (apartado 10). La fuente buena son las curvas horarias de los contadores de telegestión, que la distribuidora ya tiene de todos sus clientes: sumadas por CT dan la carga exacta de cada trafo.
- **Dónde se va a excavar.** Las licitaciones no anticipan las averías de terceros (apartado 10). Las consultas de planos de redes que hacen las constructoras antes de una obra, o las licencias municipales con su dirección, sí dirían dónde y cuándo va a haber una máquina cerca de un cable.
- **Una causa estructurada.** Hoy la causa está en texto libre y muchas veces describe la actuación de la protección ("bloqueo del interruptor") y no la causa: desde 2016 el texto da la causa en el 79 % de las incidencias de la Distribuidora 1 y en el 65 % de las de la Distribuidora 2, y antes muchos textos solo nombran el interruptor que disparó ([causas-incidencias.md](causas-incidencias.md), apartado 1). Un campo con una lista cerrada (viento, rayo, fauna, vegetación, terceros, equipo, desconocida) en Calser haría innecesarias las reglas que hoy leen la causa del texto (`causas_incidencia.py`) y daría etiquetas más limpias al modelo por familias de causa.
- **Telemando en las cabeceras prioritarias**, que además de reducir el TIEPI daría las señales de defecto y de paso de falta que el modelo por horas echa de menos.
- **Más distribuidoras**, en la cantidad que estima el apartado 8.

## 12. Cómo lanzarlo

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

`job_ml_riesgo_ct.py` y `job_ml_horas_riesgo.py --curva-aprendizaje` se lanzan igual. El análisis de red tarda unos 10 minutos (casi todo en el remuestreo del apartado de obras), el riesgo anual unos minutos y la curva de aprendizaje unos 15 minutos. La descarga de OpenStreetMap depende de los servidores de Overpass: una celda que falla se vuelve a pedir en la siguiente ejecución. La primera descarga de PLACSP baja unos 8,4 GB (2018-2025) y con `--procesos 4` tarda algo menos de una hora; `--anios` elige los años y su orden. `extract_ieex_proyecciones.py` solo hace falta cuando el IEEX publique proyecciones nuevas.
