# Causas de las incidencias y datos para predecirlas

En este documento revisamos todas las incidencias de Calser de las tres distribuidoras para saber por qué se corta la luz, qué datos podrían anunciar cada causa, si los que tenemos bastan y qué fuentes públicas hemos añadido o descartado para predecir mejor. Las cifras salen de la ejecución `cau_20261012T025409_4681c9` del análisis de causas (con la meteorología de 2017 a 2026) y de las de riesgo y horas de riesgo que se citan en el apartado 8.

| Fichero | Qué hace |
| --- | --- |
| `etl/jobs/02_silver/causas_incidencia.py` | Reglas que leen la causa, la subcausa, el componente y el origen de cada incidencia a partir del texto, el factor, el tipo de equipo y el código de Extremadura |
| `etl/jobs/02_silver/job_silver_f_incidencia_ex.py` | Módulo de Extremadura de Calser: código de casuística, instalación de origen y de cabecera, nivel de tensión, tipo de día y métodos de recogida |
| `etl/jobs/02_silver/job_silver_f_incidencia_causa.py` | `l2_silver.f_incidencia_causa`, una fila por incidencia con su causa y el resumen de sus interrupciones |
| `etl/jobs/04_ml/job_causas_incidencias.py` | Análisis por causa: inventario, causas y subcausas con su peso en el TIEPI, origen, código frente a texto, cuándo, lugares que se repiten, meteorología y datos públicos |
| `etl/jobs/04_ml/job_ml_riesgo_ct.py` | Riesgo anual por CT y cabecera, con `--causas` para una familia y el modelo por familias de causa (`por_causa`) |
| `etl/jobs/04_ml/job_ml_meteo_historia.py` | Horas de riesgo con toda la historia meteorológica, desde 2017 |
| `etl/jobs/04_ml/job_ml_combinar_horas.py` | Combinación hora a hora del modelo de TedisNet y el de la historia larga |
| `etl/jobs/04_ml/job_ml_riesgo_dia.py` | Riesgo del día siguiente por distribuidora, con lo que se sabe la tarde anterior, con la previsión perfecta y con la previsión real de ICON-EU |
| `etl/jobs/00_landing/job_landing_vivo.py` | Landing de cada tarde: la previsión de ICON-EU de cada celda y los avisos de AEMET en vigor para el día siguiente |
| `etl/jobs/04_ml/job_ml_riesgo_manana.py` | Riesgo de mañana en operación: entrena la variante `operativa` con todos los días etiquetados y puntúa el día siguiente en `l3_gold.riesgo_dia_siguiente` |
| `etl/dags/04_ml/dag_riesgo_manana.py` | DAG de Airflow que lanza las dos cosas cada tarde a las 20:30 |

## Resumen

- Desde 2016 hay 563 incidencias de CT de más de 180 s. Sin las programadas (la cuarta parte del TIEPI), las causas que más pesan son los equipos (22,7 % del TIEPI), la meteorología (17,0 %), las desconocidas (11,9 %), la fauna (7,8 %) y lo que viene de aguas arriba (6,5 %). Casi todas suben con el viento y la lluvia, también las averías de equipos, así que el tiempo es la base de lo que se puede predecir (apartado 3).
- La fuente pública que más mejora la predicción son los avisos de AEMET enviados la víspera: suben un 65 % el modelo por horas y casi doblan el del día siguiente frente al tiempo de ayer (apartado 8).
- El modelo que proponemos para el MVP es el del día siguiente con la previsión de ICON-EU de la víspera, la racha frente al clima de cada celda y los avisos, con una regresión logística. Con la previsión real de 2024 - 2025 llega a una PR-AUC de 0,191, siete veces la tasa base, y el 10 % de días de más riesgo de cada año recoge el 52 % de los días con avería (el 72 % con el 20 %). Cuatro de cada cinco días con dos incidencias o más caen en ese 10 %. En la prueba de 2026, que no usamos para elegir nada (24 días con avería), da tres veces la tasa base, aunque con tan pocos días no se separa del modelo que solo sabe el tiempo de ayer. Ya funciona cada tarde: `dag_riesgo_manana` lo calcula a las 20:30 y deja en `l3_gold.riesgo_dia_siguiente` el riesgo de mañana de cada distribuidora con sus motivos (apartados 8 y 10).
- Por horas, el mejor modelo es el de los avisos con la logística y XGBoost combinados por percentiles (0,0261, ocho veces la tasa base), y en la prueba de 2026 la logística con avisos llega a trece veces la tasa base. Sabe qué días serán malos, pero dentro del día apenas distingue la hora.
- El día de riesgo, lo que dice dónde mirar primero es el tamaño de cada cabecera; el tiempo de sus celdas no cambia ese orden.
- No han funcionado el CAPE, el polvo sahariano, las observaciones de aves, la lluvia frente al clima de cada celda, contar las averías en lugar de días y los modelos por distribuidora. El límite es el número de casos: unas 45 incidencias imprevistas de CT al año entre las tres distribuidoras.

## 1. Qué guarda Calser de cada incidencia

Calser guarda de cada incidencia un texto libre (descripción, observaciones y resolución), la clasificación (imprevista o programada), el factor (propio, terceros, fuerza mayor, transporte, generación, otra distribuidora o cliente) y el tipo de equipo (línea aérea, subterránea, CT, baja tensión, poste u otros). Las distribuidoras de Extremadura rellenan además un módulo propio que hasta ahora no habíamos traído al lakehouse: el código de casuística de la normativa autonómica (21 fuerza mayor, 23 generación, 25 transporte, 28 otra distribuidora, 29 instalación particular, 30 acción de terceros, 31 huelga, 32 atmosférica, 33 agente externo, 34 interna, 35 desconocida, 36 no subsumible), la instalación donde empezó la avería, la cabecera, el nivel de tensión, el tipo de día y cómo se detectó (telemando, aviso de cliente...). Lo cargamos en Bronze desde las tres bases (`incidencias_ex`, `incidencias_metodos_recogida_ex` e `interrupciones_ex`) y lo limpiamos en `l2_silver.f_incidencia_ex`.

Hay 3.856 incidencias entre 2010 y 2026, pero no son comparables entre sí. Hasta 2015 Calser importaba también los cortes de menos de 180 s y las incidencias de baja tensión: son 3.293, la mitad de menos de 180 s, y en muchas el texto solo dice qué interruptor disparó ("DISPARO LINEA X"). Desde 2016 solo quedan las 563 incidencias con interrupciones de CT de más de 180 s. La mayoría tienen la causa escrita en el texto (el 79 % en EOSA, el 65 % en Pitarch y el 97 % en Valle de Santa Ana), la instalación de origen (80 %, 66 % y 97 %) y un código distinto del 35, que es el valor por defecto (76 %, 82 % y 95 %).

## 2. Cómo leemos la causa

Calser no tiene un campo de causa fiable: el factor dice quién es responsable a efectos regulatorios (una cigüeña es "terceros" en una incidencia y "propios" en otra) y el código de casuística solo se rellena bien desde 2016. Leemos la causa del texto con reglas (`causas_incidencia.py`, 34 pruebas en `tests/02_silver/test_causas_incidencia.py`) en dos ejes:

- **causa**: programada, aguas arriba, apagón, meteorología, fauna, vegetación, incendio, terceros, objeto ajeno, agente externo, equipo, red de baja tensión, cliente de baja tensión, sobrecarga, maniobra, desconocida y sin causa. Cada causa tiene subcausas (nido de cigüeña, puente de línea, transformador de subestación, caída de árbol, corte preventivo por incendio...) y, para los equipos, el componente que falla.
- **origen**: dónde empezó la avería (subestación, línea de alta, línea de media, CT, instalación particular, red de baja, cliente, sistema o aguas arriba).

El orden de las reglas importa. Primero van el apagón del 28.04.2025 y las programadas, después lo que viene de fuera de la red (transporte, interconexión con Endesa o Iberdrola), y luego las causas con un agente claro (fauna, vegetación, terceros, incendio) antes que el equipo o la meteorología. Un equipo solo cuenta como causa cuando el texto habla de un fallo (rotura, avería, quemado), porque muchos textos solo nombran el interruptor que actuó. Cuando el texto no dice nada usamos el código oficial (32 atmosférica, 33 agente externo, 34 interna...) y, si tampoco hay código, el factor. Con esto, de 2016 en adelante ninguna incidencia se queda sin causa.

Comparamos la causa leída con el código oficial: cuadran en el 91,2 % de las incidencias con causa escrita. Donde menos cuadran es en el código 34 (interna, 78,8 %), que además es el que llevan las 113 programadas.

## 3. Todas las causas

Incidencias de CT desde 2016. parte_tiepi es la parte de los kVA·h del TIEPI regulado de 2016 - 2025 (reglas de Calser, sin el apagón).

| Causa | Incidencias | Parte del TIEPI | CT por incidencia (media) | Duración mediana | Lo más frecuente |
| --- | --- | --- | --- | --- | --- |
| Programada | 119 | 26,6 % | 7,9 | 3,0 h | digitalización y telegestión (30), cortes programados (32), reformas (17) |
| Equipo | 110 | 22,7 % | 58,6 | 0,61 h | puentes (26), cables subterráneos y terminales (13), seccionadores con fusible (12), transformadores de subestación (11) |
| Meteorología | 70 | 17,0 % | 103 | 0,34 h | viento y temporal (25), "condiciones adversas" sin detalle (16), lluvia (9), rayo (9) |
| Desconocida | 102 | 11,9 % | 68,9 | 0,15 h | declarada desconocida (94), sobre todo en líneas de media |
| Fauna | 60 | 7,8 % | 63,7 | 0,61 h | nidos de cigüeña (37), palos y restos de nido (12), animales en subestación (7) |
| Aguas arriba | 60 | 6,5 % | 22,1 | 0,19 h | falta de alimentación de Endesa (51), de Iberdrola (8) |
| Vegetación | 10 | 3,1 % | 72,6 | 0,83 h | caída de árbol o rama (8) |
| Incendio | 9 | 1,7 % | 19,3 | 1,88 h | corte preventivo pedido por los bomberos (8) |
| Terceros | 9 | 1,3 % | 46,6 | 0,62 h | maquinaria de obras (3), disparos de caza (2), vandalismo (2), aeronave (1) |
| Apagón | 6 | 0,0 % | 220 | 8,1 h | el cero peninsular del 28.04.2025, aparte |
| Otras | 8 | 1,3 % | - | - | objeto enganchado (2), agente externo (2), maniobra (4) |

Hasta 2015 la mezcla es otra: 923 averías de red de baja tensión (fusibles, acometidas, empalmes), 188 de instalaciones de clientes y 1.405 sin causa (solo el interruptor que actuó), que no usamos para entrenar.

**Programadas.** Son la cuarta parte del TIEPI, pero no hay nada que predecir: la distribuidora las planifica y avisa a los clientes. Las dejamos fuera de la etiqueta. Lo que sí cuenta es su volumen: la digitalización de la red y la telegestión han traído 30 cortes programados desde 2016.

**Equipos.** El primer bloque de averías propias y el que más se puede trabajar. Fallan sobre todo los puentes de las líneas de media (26), los cables subterráneos y sus terminales (13), los seccionadores con fusible (12) y los transformadores de potencia de las subestaciones (11, que se llevan el 6,3 % del TIEPI porque dejan sin luz a 174 CT de media). Aunque parezcan averías "internas", dependen del tiempo: en los días de rachas de 80 km/h o más la probabilidad de una avería de equipo se multiplica por 4,6 y con 30 mm de lluvia o más por 7,2. También se repiten en las mismas instalaciones.

**Meteorología.** Viento y temporal (25), condiciones adversas sin más detalle (16), lluvia (9) y rayos (9). Enero concentra el 34 %, con los temporales atlánticos. Es la causa que mejor responde al tiempo: con 10 mm de lluvia o más en el día la probabilidad de una incidencia meteorológica se multiplica por 13,6 y el 75 % de ellas cae en esos días, que son el 11 % del calendario.

**Desconocida.** 102 incidencias en las que la brigada no encontró la causa, 81 de ellas en líneas de media. Se comportan como una mezcla de meteorología y equipos (con rachas de 80 km/h, por 3,0; con 30 mm de lluvia, por 3,6): en buena parte son descargas o contactos que se despejan solos con el reenganche y no dejan huella.

**Fauna.** Casi todo son cigüeñas: 37 incidencias por nidos y 12 por palos o restos de nido en los apoyos, con el pico en marzo (23 %), mayo y febrero, la época de construcción de nidos y de cría. La mayoría ocurre lloviendo: el 56 % de las incidencias de fauna cae en días con 10 mm de lluvia o más, donde la probabilidad se multiplica por 9,2. El nido mojado conduce y provoca el contorneo del aislador. Esto convierte la fauna en una causa en parte predecible por horas, siempre que sepamos dónde están los nidos.

**Aguas arriba.** 60 incidencias, 52 de Valle de Santa Ana, que depende de una única interconexión con Endesa (la misma instalación aparece 57 veces desde 2012). Tampoco son ajenas al tiempo (con 30 mm de lluvia, por 3,8): un temporal que tumba la red de Endesa también deja sin luz a la distribuidora que cuelga de ella.

**Vegetación, incendios y terceros.** Pesan poco. Las 10 de vegetación son de EOSA (valles del Jerte y del Ambroz) y las 6 que tienen meteorología de 2017 en adelante cayeron todas en días con rachas de 60 km/h o más. Los incendios son cortes preventivos pedidos por los bomberos en agosto, en días de 38 °C o más (por 14,9). Las de terceros son maquinaria, caza (dos disparos a aisladores) y una aeronave.

## 4. Dónde y cuándo

Desde 2016, 234 de las 563 incidencias empiezan en líneas de media, 40 en subestaciones, 38 en instalaciones particulares, 35 en los propios CT y 32 en líneas de alta. Algunas líneas se repiten mucho: LINEA ZONA VALLE de EOSA tiene 17 incidencias con origen en ella (11 desconocidas y 5 meteorológicas), LINEA SANTA MARTA de Pitarch 12, y LINEA TORREMOCHA y LINEA JERTE III 6 meteorológicas cada una. La misma causa en el mismo sitio es donde más rinde una actuación concreta: proteger los apoyos de LINEA ACEHUCHE CECLAVIN (tres incidencias de fauna el mismo día de 2023) o revisar los puntos débiles de las líneas del Jerte frente al viento.

Por horas, las programadas empiezan casi siempre en horario de trabajo (83 % de 8 a 18 h) y en fin de semana: 118 de las 119 desde 2016, para molestar menos a comercios y empresas. La fauna también cae de día (68 % de 8 a 18 h) y la meteorología a cualquier hora (34 % de 8 a 18 h, 17 % de 0 a 6 h). El tipo de día que guarda Calser no sirve para esto: marca como laborables 85 incidencias que empezaron en sábado o domingo, así que el análisis usa el calendario.

## 5. Qué datos podrían predecir cada causa

| Causa | Qué la provoca | Datos que la anuncian | ¿Los tenemos? |
| --- | --- | --- | --- |
| Meteorología | Viento, lluvia intensa, rayos, nieve y hielo | Rachas, lluvia, inestabilidad del aire (CAPE), rayos observados | Rachas, lluvia, nieve y temperatura horarias desde 2017; CAPE desde 2021. Los rayos no tienen archivo abierto |
| Equipos | Envejecimiento, carga, sobretensiones, suciedad en aisladores | Edad y estado de los equipos, carga, historia de fallos, tiempo (viento, lluvia, primera lluvia tras sequía) | Historia de fallos, carga de TedisNet y tiempo; la edad y el mantenimiento no están en ninguna base |
| Desconocida | Contactos y descargas que se despejan solos | Lo mismo que meteorología y equipos, más los reenganches del SCADA | Sí, con TedisNet desde 2021 |
| Fauna | Nidos de cigüeña en los apoyos, sobre todo mojados | Época de cría, lluvia, presencia de cigüeñas, apoyos sin proteger | Calendario y lluvia sí; cigüeñas de GBIF (sesgado); los apoyos protegidos y los nidos no están en datos abiertos estructurados |
| Aguas arriba | Fallos de la red de transporte o de la distribuidora vecina | Estado de la red de transporte, tiempo en la zona | Solo el tiempo regional |
| Vegetación | Ramas y árboles sobre la línea | Arbolado junto a la línea, viento, suelo empapado, fecha de la última poda | Bosque de OpenStreetMap por celda y viento; humedad del suelo añadida; las podas están en las programadas de Calser |
| Incendio | Fuego cerca de la línea o corte preventivo | Calor, sequía, viento, incendios activos, incendios de años anteriores | Calor y viento sí; áreas quemadas de EFFIS sí; incendios activos de FIRMS necesitan clave |
| Terceros | Obras, maquinaria agrícola, caza, vandalismo | Obras licitadas, calendario laboral, temporada de caza | Obras de PLACSP y calendario sí |

## 6. ¿Bastan los datos que tenemos?

Para el riesgo meteorológico por horas, casi: rachas, lluvia, temperatura, humedad y presión cubren las causas que más pesan y responden con fuerza, y desde ahora tenemos nueve años de historia en lugar de cinco. Faltaba una previsión real archivada para medir el modelo como funcionaría de verdad, y ya la tenemos: los avisos de AEMET desde 2018 y lo que preveía ICON-EU desde 2024 (apartado 8). Lo que sigue faltando son los rayos observados antes de 2024 (las tormentas de verano se ven mal en la lluvia y el viento del reanálisis); el satélite MTG los da desde julio de 2024.

Para el riesgo anual por CT, la historia de cada CT y su cabecera ya dice mucho y lo que falta es información de los activos: edad de líneas y transformadores, fechas de revisión y poda, apoyos protegidos frente a la avifauna. Eso no está en datos abiertos y lo tendría que dar la distribuidora.

El límite más duro es el número de casos. Con unas 45 incidencias imprevistas de CT al año entre las tres distribuidoras, cualquier diferencia de un par de puntos entre modelos cae dentro del ruido. Más años de historia y más distribuidoras ayudan más que cualquier variable nueva, y por eso el primer cambio del apartado 8 es aprovechar los años de 2017 a 2020.

## 7. Fuentes públicas revisadas

| Fuente | Qué da | Acceso | Estado |
| --- | --- | --- | --- |
| [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), ECMWF IFS | Tiempo horario a 9 km desde 2017 | Libre, sin clave (uso no comercial) | Integrada: rachas, viento, lluvia, nieve, temperatura, humedad y presión desde 2017 por celda de 0,1 grados (43 celdas) |
| Open-Meteo, variables extra del mismo reanálisis | Dirección del viento, viento a 100 m, humedad del suelo, punto de rocío, déficit de presión de vapor, nubes bajas, evapotranspiración, código de tiempo, radiación | Libre | Integrada en celdas de 0,25 grados (18 celdas): el cupo diario del plan gratuito no da para la historia completa en la rejilla fina |
| [Open-Meteo Historical Forecast API](https://open-meteo.com/en/docs/historical-forecast-api), GFS | CAPE e índice de levantamiento desde el 23.03.2021 | Libre | Integrada, celdas de 0,25 grados |
| [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api), CAMS | PM10 desde 2013, polvo sahariano desde 2018 | Libre | Probada y descartada por ahora: los días de calima no tienen más incidencias (riesgo relativo 0,9 con más de 25 µg/m³ de polvo) y la combinación de polvo acumulado y lluvia ligera solo junta 6 incidencias |
| Índice de rayos de ICON (`lightning_potential`) | Potencial de rayos | Libre | Descartada: solo existe para ICON-D2, que no cubre España |
| [AEMET OpenData](https://opendata.aemet.es/), [avisos de fenómenos adversos](https://datos.gob.es/es/catalogo/e05068001-avisos-meteorologicos) | Avisos amarillos, naranjas y rojos de viento, lluvia, tormentas y nieve por zona, en CAP, desde el 18.06.2018 (CC BY 4.0); rayos solo como mapa del momento | Clave gratuita por correo, válida tres meses | Integrada de Landing a Gold (`avisos` en `config_bronze_publicos.json`, `l2_silver.f_aviso_meteo`, `met_aviso_*`), con la referencia `naif_aviso_aemet` en el modelo por horas y el bloque `avisos` en el del día siguiente. El archivo solo admite 2 días por petición y limita las peticiones por minuto, así que la historia completa son unas 1.500 peticiones. Es una previsión real emitida con antelación y lo que hoy mira un jefe de brigadas. Con el archivo completo es la fuente que más mejora los dos modelos (apartado 8) |
| [Open-Meteo Previous Runs API](https://open-meteo.com/en/docs/previous-runs-api), ICON-EU | Lo que preveía el modelo para cada hora uno y dos días antes, desde enero de 2024 | Libre | Integrada de Landing a Gold (`openmeteo_previo` en `config_bronze_meteo.json`, columnas `*_prev1_*` de `l2_silver.f_meteo_hora`, `metp1_*` y `metp2_*` de Gold). Es la previsión real con la que medimos el modelo del día siguiente. De ECMWF IFS no hay rachas previstas en el archivo y de ICON-EU sí |
| [EUMETSAT Data Store](https://data.eumetsat.int/), Lightning Imager de MTG | Rayos observados desde el satélite geoestacionario: flashes con posición, hora, duración y radiancia cada 10 minutos (producto LI-2-LFL, unos 0,4 MB) | Cuenta gratuita y credenciales del Data Store | Probada. El archivo empieza el 04.07.2024. Entre las 13:50 y las 14:00 UTC del 12.08.2025 hubo 54 flashes en la caja de estudio, en plena ola de incendios del norte de Cáceres; EOSA cortó por incendio a las 03:33 del día siguiente. Para el MVP es el dato de rayos en tiempo real; con año y medio de historia no da para entrenar. Hubo rayos a menos de 10 km en la hora anterior al 17 % de las incidencias de 2024 - 2025, frente al 1,1 % de las ventanas de control (apartado 8) |
| [REE, REData](https://www.ree.es/es/apidatos) | Demanda nacional cada 5 minutos, generación, intercambios y precios; de calidad de servicio, el tiempo de interrupción medio de la red de transporte, por año | API libre, sin clave | Revisada y descartada: la demanda nacional sigue a la temperatura, que ya tenemos, y la calidad de la red de transporte solo viene agregada por año. La demanda por comunidad no la devuelve para Extremadura |
| [GBIF](https://www.gbif.org/developer/occurrence) | Observaciones de cigüeña blanca y de aves por celda | API libre, sin clave | Integrada (`l2_silver.f_aves_celda_anio`). Sale al revés de lo esperado (más observaciones, menos averías por fauna): mide dónde observa la gente, no dónde anidan |
| [EFFIS](https://forest-fire.emergency.copernicus.eu/), áreas quemadas | Incendios de 30 ha o más con fecha y perímetro | WFS libre | Integrada: 507 áreas quemadas alrededor de nuestras celdas. Los CT sin incendios cerca en cinco años no tuvieron ninguna incidencia por incendio |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | Focos de incendio activos de MODIS y VIIRS, con archivo desde 2000 (MODIS) y 2012 (VIIRS) | Clave gratuita (MAP_KEY), peticiones de 5 días como mucho | Integrada de Landing a Gold (`focos` en `config_bronze_publicos.json`, `l1_bronze.firms_foco`, `l2_silver.f_foco_incendio`, `met_focos_24h` y `met_focos_frp_max_24h` por celda, contados cuando FIRMS los habría publicado). En 7 de los 8 episodios de corte por incendio (2017 - 2026) había focos a menos de 10 km de las celdas de la distribuidora en las 48 horas anteriores, y en el que falta (03.10.2022) no hubo ninguno. Anuncia los cortes preventivos con horas de antelación |
| [Copernicus CEMS](https://ewds.climate.copernicus.eu/datasets/cems-fire-historical-v1), índice de peligro de incendio | FWI diario desde 1940 y sus partes (FFMC, DMC, DC, ISI, BUI) en una rejilla de 0,25 grados (CC BY 4.0) | Cuenta gratuita del EWDS, token personal y licencia del conjunto aceptada en su página | Integrada de Landing a Gold (`fwi` en `config_bronze_publicos.json`, `l1_bronze.cems_fwi_dia`, `l2_silver.f_fwi_celda_dia`, `met_fwi_ayer` y el resto de partes del día anterior por celda). Una petición por año, que espera unos minutos en la cola del EWDS. El 12.08.2025 el FWI de las celdas de EOSA llegó a 66 - 68, peligro extremo (más de 50), la víspera del corte por incendio. ERA5 no hace falta: los rayos se añadieron al modelo de ECMWF en el ciclo 45r1 y ERA5 usa el 41r2 |
| [EGIF](https://servicio.mapa.gob.es/incendios/) del Ministerio | Estadística de incendios forestales | Buscador con descarga manual | Descartada para automatizar; EFFIS cubre lo que necesitamos |
| [Zonas de protección de la avifauna](https://doe.juntaex.es/pdfs/doe/2014/1560o/14061643.pdf) (Resolución de 14.07.2014) | Zonas donde las líneas deben estar protegidas | Shapefile en la web de la Junta, sin dirección estable | Pendiente de descarga manual: diría qué líneas están en zona protegida |
| Líneas que no cumplen el RD 1432/2008 en Extremadura (Resolución de 15.11.2021) | Relación de líneas peligrosas para la avifauna | PDF en el DOE | Pendiente: habría que pasarlo a tabla a mano |

Las fuentes del apartado 9 de [network-analysis.md](network-analysis.md) (padrón del INE, Wikidata, relieve, OpenStreetMap, obras de PLACSP, Catastro y proyecciones del IEEX) siguen en el contexto anual de cada CT.

## 8. Qué mejora la predicción

**Riesgo anual: un modelo por familia de causas.** Las causas no se repiten igual. La fauna vuelve a los mismos apoyos año tras año, los equipos fallan en las instalaciones viejas o cargadas y la meteorología castiga las líneas más expuestas, y un único modelo con la historia de todas las causas lo mezcla. Entrenamos el riesgo anual por familias (`--causas`, ejecuciones `rct_*_20261012_0744`) y vimos que cada una se predice de forma distinta:

| Familia | Nivel | Top 10 %: lista naif | Top 10 %: mejor bloque | Bloque |
| --- | --- | --- | --- | --- |
| Meteorología, desconocida, vegetación, incendio y objeto ajeno | CT | 23,5 % | 38,5 % | historia, estructura y clima |
| Fauna | CT | 31,1 % | 22,7 % | historia, estructura y datos públicos |
| Equipos | CT | 20,5 % | 30,2 % | historia y estructura |
| Equipos | Cabecera | 48,6 % | 61,2 % | historia, estructura y clima |

En fauna gana la lista naif (los nidos están donde estaban) y en meteorología y equipos el modelo saca entre 10 y 15 puntos a la lista naif. El mejor bloque está elegido entre ocho mirando los mismos años, así que esas cifras son algo optimistas. Al sumar un modelo por familia, con una familia más para el resto de incidencias, el ranking de CT mejora con todos los bloques. En la ejecución completa de la segunda ronda (`rct_r2_20261012_0907`), el 10 % de CT con más riesgo recoge entre el 33,3 % y el 35,5 % de los kVA·h imprevistos del año siguiente, frente al 23 - 29 % del modelo único y el 27,8 % de la lista naif, y el 20 % de CT recoge entre el 50 % y el 55 % frente al 47 %. Por cabecera no mejora (66 % frente a 71 %): ahí la historia de la línea ya lo dice todo. Lo hemos integrado en `job_ml_riesgo_ct.py` como `por_causa:<bloque>`, y el ranking del año siguiente añade lo que espera cada familia en cada CT y cabecera (`esperado_meteo`, `esperado_fauna`, `esperado_equipo`, `esperado_resto`), que es lo que dice qué actuación toca: proteger apoyos, revisar equipos o podar.

**Horas de riesgo con la historia desde 2017.** El modelo por horas de [ml-training.md](ml-training.md) solo aprende desde 2021 porque TedisNet empieza entonces. Con la meteorología desde 2017 y las interrupciones de Calser, que se registran igual desde 2016, `job_ml_meteo_historia.py` entrena con meteorología, calendario e historia de Calser desde 2017: el año 2023 aprende de 220 episodios en lugar de 70. Resultado de `mh_20261012_0744` en 2022 - 2025 (141 episodios, tasa base 0,0031):

| Variante | Modelo | PR-AUC | Veces la tasa base | ROC-AUC |
| --- | --- | --- | --- | --- |
| Desde 2017, meteorología, calendario e historia | Regresión logística | 0,0185 | 5,9 | 0,742 |
| Desde 2021, lo mismo | Regresión logística | 0,0186 | 5,9 | 0,722 |
| Desde 2017, meteorología, calendario e historia | XGBoost | 0,0171 | 5,5 | 0,750 |
| Desde 2021, lo mismo | XGBoost | 0,0139 | 4,4 | 0,715 |
| Desde 2017, solo meteorología | Regresión logística | 0,0176 | 5,6 | 0,737 |
| Desde 2021, solo meteorología | Regresión logística | 0,0137 | 4,4 | 0,724 |
| Regla de la racha (peor celda, 3 h) | - | 0,0129 | 4,1 | 0,678 |

Los años de más ayudan a los modelos que más datos necesitan: XGBoost sube de 0,0139 a 0,0171 (mejor en el 85 % de las réplicas del bootstrap por días) y la logística solo con meteorología de 0,0137 a 0,0176 (en el 96 %). La logística con calendario e historia ya llegaba a 0,0186 con cuatro años y no gana más. Frente a la regla de la racha, la logística desde 2017 es un 43 % mejor y lo es en el 93 % de las réplicas; el intervalo de la diferencia todavía roza el cero (-0,0012 a 0,0171). Dos ideas que parecían prometedoras no aportan: el CAPE (0,0149 con él) y entrenar solo con las causas que dependen del tiempo (0,0166). Lo segundo tiene sentido después del apartado 3: incluso las averías de equipos y las de aguas arriba suben con el temporal.

Dos avisos al leer estas cifras. Por distribuidora, en Pitarch la regla de la racha sigue siendo mejor (0,0220 frente a 0,0116), mientras que en EOSA y Valle de Santa Ana gana el modelo. Y con un presupuesto de 2 horas por distribuidora y día los modelos no detectan más episodios que la regla (20 %): saben qué días van a ser malos, pero dentro de un día de temporal apenas distinguen qué hora llega primero. Para planificar las brigadas el día es la unidad natural, y por eso añadimos la evaluación por días de riesgo.

**Segunda ronda: la lluvia de los días anteriores y las variables extra.** Con la lluvia de 72 horas y de los 30 días anteriores en Gold (bloque `antecedente`) y las variables extra del reanálisis (bloques `extra` y `suelo`), `mh_r2_20261012_0907` repite las variantes en 2022 - 2025:

| Variante | Regresión logística | XGBoost | Media de las dos |
| --- | --- | --- | --- |
| Meteorología, calendario e historia | 0,0185 | 0,0148 | 0,0182 |
| Más la lluvia de los días anteriores | 0,0186 | 0,0195 | 0,0221 |
| Más las variables extra y la humedad del suelo | 0,0188 | 0,0175 | 0,0224 |
| Todo junto, con el CAPE | 0,0164 | 0,0207 | 0,0202 |
| Con interacciones por distribuidora | 0,0174 | 0,0155 | 0,0182 |

La lluvia de los días anteriores ayuda a XGBoost (mejor en el 88 % de las réplicas) y deja igual a la logística. Las variables extra no añaden nada a la lluvia previa (19 % y 57 % de las réplicas) y las interacciones por distribuidora empeoran un poco. XGBoost con todo es el primer modelo que supera a la regla de la racha con el intervalo entero por encima de cero (+0,0076, de 0,0006 a 0,0172).

**TedisNet y la historia juntos.** El modelo por horas de [ml-training.md](ml-training.md) usa las 448 variables de TedisNet (red, SCADA, cortes y tiempo) y solo puede aprender desde 2021. En 2023 - 2025 (`hr_r2_20261012_0907`) XGBoost llega a una PR-AUC de 0,0105, la regla de la racha a 0,0094 y la logística a 0,0083. Las variables que más pesan son la lluvia, los reenganches aguas arriba, los microcortes del SCADA y las faltas a tierra del grupo. `job_ml_combinar_horas.py` junta esta ejecución con la de la historia larga en las 78.011 horas que puntúan las dos (`cmb_r2_20261012_0907`):

| Modelo | PR-AUC por horas | PR-AUC de los días | Días con avería en el 10 % de días de más riesgo | En el 20 % |
| --- | --- | --- | --- | --- |
| Historia larga, regresión logística | 0,0169 | 0,145 | 37 % | 58 % |
| Historia larga, XGBoost | 0,0159 | 0,125 | 37 % | 59 % |
| TedisNet, XGBoost | 0,0106 | 0,134 | 41 % | 54 % |
| TedisNet, regresión logística | 0,0083 | 0,121 | 44 % | 54 % |
| Logística de TedisNet y XGBoost de la historia, por percentiles | 0,0166 | 0,159 | 39 % | 55 % |
| XGBoost de TedisNet y logística de la historia, por percentiles | 0,0162 | 0,154 | 37 % | 61 % |
| Regla de la racha | 0,0094 | 0,098 | 35 % | 47 % |

Por horas, la historia larga gana a TedisNet (la logística de TedisNet queda por debajo en el 99,6 % de las réplicas) y juntar las dos no mejora a la historia sola: por percentiles queda igual (entre el 44 % y el 58 % de las réplicas) y la media de probabilidades con la logística de TedisNet empeora. Sí mejora a TedisNet solo, con el intervalo entero por encima de cero: +0,0053 (de 0,0013 a 0,0117) frente a su XGBoost. Por días, la combinación es la que mejor ordena (0,159 frente a 0,145 de la historia sola), pero con 103 días con avería no se distingue de la historia. Estos días hay que leerlos con cuidado, porque el modelo de TedisNet usa lo que pasa en la red durante el propio día (los microcortes y reenganches de la mañana): es una alerta del mismo día y no sirve para planificar la víspera. Lo que sí muestra es que las señales de la red llevan información que el tiempo no da, y por eso medimos los precursores del apartado 9.

**Lo que hemos probado fuera de Spark.** Exportamos la tabla horaria del modelo con la historia larga (236.088 horas de 2017 a 2025) y probamos una docena de cambios con la misma evaluación por años. Como cada año lo puntúa un modelo distinto y el nivel de sus probabilidades cambia de un año a otro, además de la PR-AUC de todos los años juntos miramos la de los percentiles de cada año juntos, que solo depende de cómo ordena cada modelo sus horas.

| Cambio sobre la logística de `mh_20261012_0744` | PR-AUC | Con percentiles por año | Mejor que la logística en el bootstrap |
| --- | --- | --- | --- |
| Ninguno | 0,0185 | 0,0178 | - |
| Más regularización (C de 0,001 a 0,03) | 0,0197 | 0,0190 | 84 % |
| Logística y XGBoost juntos por percentiles del año | 0,0213 | 0,0211 | 95 % |
| XGBoost monótono (más viento, lluvia o incidencias nunca bajan el riesgo) | 0,0227 | 0,0181 | igual que XGBoost (44 %) |
| EBM, un modelo aditivo explicable | 0,0143 | - | no |
| Pesos de clase equilibrados | 0,0148 | 0,0148 | no |
| Once variables meteorológicas en lugar de 38 | 0,0162 | 0,0175 | no |
| Una logística por distribuidora | 0,0192 | 0,0180 | no |
| Entrenar con ventanas más anchas (0 - 6 h, 0 - 12 h) | 0,0169 - 0,0195 | 0,0161 - 0,0189 | no |
| Incidencias empezadas en las horas anteriores | 0,0240 | 0,0248 | solo en 2022 |
| Meteorología perfecta de las 3 horas siguientes | 0,0267 | 0,0242 | 99 % |

Nos quedamos con dos cambios: más regularización en la logística y la combinación de logística y XGBoost por percentiles del año (`rango` en `job_ml_meteo_historia.py`). La media de probabilidades que ya teníamos se queda en 0,0180 porque las probabilidades de XGBoost tienen una cola larga y mandan en la media. Con percentiles pesan igual, y la combinación iguala a la regla de la racha en Pitarch (0,0222 frente a 0,0220) sin perder EOSA (0,0290). En el portátil, con los seis años de 2020 a 2025, `rango` vuelve a ser la mejor: 0,0158 frente a 0,0140 de la logística (96,6 % de las réplicas) y a 0,0139 de la media (98,8 %, con el intervalo por encima de cero).

La monotonía de XGBoost parecía la mejor opción con la PR-AUC de todos los años juntos, pero con percentiles se queda como estaba: su ventaja venía del nivel de sus probabilidades en 2025. Las incidencias de las horas anteriores (lo que sabe el centro de control por el SCADA o por las llamadas) suben mucho 2022, un año con temporales de varias averías seguidas, y nada en 2023 - 2025, así que no las añadimos. Lo que mueve el modelo de forma clara es saber qué tiempo va a hacer: con la meteorología de las tres horas siguientes la PR-AUC sube entre un 27 % y un 36 %. Esa es la cota de lo que puede aportar una previsión.

**El día siguiente.** Las brigadas se organizan la tarde anterior, así que `job_ml_riesgo_dia.py` cambia la unidad a la distribuidora y el día. Usa lo que se sabe esa tarde (calendario, historia de Calser y tiempo del día anterior) y, aparte, el tiempo del propio día sacado del reanálisis como previsión perfecta. Resultado de `dia_20261012_1103` en 2022 - 2025 (137 días con interrupción, el 3,1 % de los días):

| Variante | PR-AUC de los días | Días con avería en el 10 % de días de más riesgo | En el 20 % |
| --- | --- | --- | --- |
| Calendario e historia | 0,055 | 21 % | 34 % |
| Más el tiempo de ayer | 0,062 | 22 % | 40 % |
| Más el tiempo del día (previsión perfecta) | 0,125 | 37 % | 54 % |
| Solo la racha máxima del día | 0,100 | 34 % | 45 % |

Saber qué tiempo hará duplica la PR-AUC de los días (+0,061, de 0,026 a 0,114, mejor en todas las réplicas) y lo hace todos los años: entre 0,137 y 0,170 cada año con la previsión perfecta, frente a 0,061 - 0,076 con el tiempo de ayer. Para comprobar cuánto queda con una previsión de verdad descargamos lo que ICON-EU preveía el día anterior desde enero de 2024; el resultado con la descarga completa está más abajo (**Con la previsión real de ICON-EU**). El job entrena con el reanálisis y puntúa los años con previsión dos veces más, con la previsión tal cual y llevada por cuantiles a la escala del reanálisis, porque ICON-EU da rachas más fuertes que ECMWF IFS (45 frente a 39 km/h de media en la racha máxima del día).

**Más variables para el día siguiente.** Con la tabla diaria fuera de Spark probamos cuatro grupos de variables sobre la previsión perfecta en 2022 - 2025: las horas del día por encima de 40, 50, 60 y 70 km/h; la racha del día frente al percentil 98 de las rachas de cada distribuidora en 2017 - 2021, como cociente y como índice de pérdidas por temporal de [Klawa y Ulbrich](https://nhess.copernicus.org/articles/3/725/2003/) (la suma de las horas de (racha / p98 - 1)³), que se usa para estimar los daños de los temporales de viento; la lluvia de los 3 y 30 días anteriores con los días secos previos; y los días seguidos con 35 °C o más. Fuera de Spark las variables del día no son exactamente las del job, por eso la base da 0,118 y no 0,125:

| Grupo añadido a la previsión perfecta | Regresión logística | XGBoost | Rango | Rango mejor que sin el grupo |
| --- | --- | --- | --- | --- |
| Ninguno | 0,118 | 0,099 | 0,115 | - |
| Horas por encima de 40 - 70 km/h | 0,119 | 0,101 | 0,119 | 70 % |
| Racha frente al clima de la distribuidora | 0,137 | 0,110 | 0,121 | 70 % |
| Lluvia de los días anteriores | 0,118 | 0,098 | 0,114 | 56 % |
| Días seguidos de calor | 0,118 | 0,105 | 0,126 | 97 % |
| Todo junto | 0,121 | 0,110 | 0,128 | 91 % |

Contar horas por encima de un umbral, que es como lo hacen en Connecticut, no añade nada a la racha máxima, y la lluvia de los días anteriores tampoco. La racha frente al clima de cada distribuidora es lo que más sube la logística (de 0,118 a 0,137), sobre todo en Pitarch y Valle de Santa Ana, aunque el bootstrap todavía no lo separa del ruido (74 %). Los días de calor ayudan a la combinación por percentiles (97 %) y, con todo junto, XGBoost mejora con el intervalo entero por encima de cero (+0,013, de 0,001 a 0,034). En el modelo por horas las mismas variables no aportan: con todas, la logística baja de 0,0197 a 0,0185 y la combinación por percentiles se queda igual (59 % de las réplicas), otra señal de que por horas el límite está en saber qué hora del día llega la avería. También probamos un modelo por familia de causas, como en el riesgo anual, juntando las probabilidades de las familias: sin las variables nuevas mejora a XGBoost (de 0,099 a 0,120, en el 99 % de las réplicas) y la combinación por percentiles llega a 0,130, pero con ellas ya no añade nada (67 %). Con 137 días con avería, las diferencias de 0,01 - 0,02 entre estas variantes caen dentro del ruido, y como las hemos elegido entre varias pruebas, la mejor cifra es algo optimista. Hemos llevado la racha frente al clima y los días de calor al job del día siguiente como bloque `extremos` (variante `perfecta_extremos`) para medirlos con los datos del Lakehouse, y el FWI del día anterior entra en el tiempo de la víspera y del día.

Al exportar la meteorología por celda salió por qué la racha de la distribuidora engañaba en EOSA: su celda más ventosa (40,30 / -5,70, en la sierra, con 11 CT) tiene un percentil 98 de 111 km/h, mientras que la celda con más CT (146) se queda en 74. La peor celda de EOSA es casi siempre esa y apenas tiene red. Por eso la racha frente al clima se calcula ahora en Gold celda a celda (`met_racha_clima_1h`), y la distribuidora toma la peor celda y la media por CT de esos cocientes. Fuera de Spark, la versión por celda mejora a los tres modelos del día siguiente (logística de 0,118 a 0,142, XGBoost de 0,099 a 0,105 y rango de 0,115 a 0,122, entre el 84 % y el 88 % de las réplicas), y no solo a la logística como la versión por distribuidora.

**Los días con varias averías se ven mejor.** Para las brigadas no pesa igual un día con una avería que uno con cinco. Con la misma tabla diaria y la previsión perfecta, en 2022 - 2025 hubo 137 días con alguna interrupción, 46 con dos o más, 26 con tres o más y 10 con cinco o más (casi todos en EOSA y Pitarch). Cuanto más grave es el día, más se concentra en los días que el modelo marca:

| Días con | Días | En el 10 % de días de más riesgo | En el 20 % | Detectados con un 3 % de falsas alarmas |
| --- | --- | --- | --- | --- |
| Alguna interrupción | 137 | 37 % | 54 % | 19 % |
| Dos o más | 46 | 52 % | 70 % | 35 % |
| Tres o más | 26 | 46 % | 65 % | 31 % |
| Cinco o más | 10 | 60 % | 70 % | 40 % |

Son los modelos de la regresión logística entrenados con cada etiqueta, y con 10 o 26 días las cifras se mueven mucho de una réplica a otra. El trabajo de Valencia y Albacete detecta el 73 % de los días extremos con un 3 % de falsas alarmas, pero define el día extremo por el número de averías de una provincia entera, así que las cifras no se comparan directamente. Contadas las interrupciones en lugar de los días, el 10 % de días de más riesgo de cada año concentra el 51 % de todas las interrupciones. Aprender el número de interrupciones del día (regresión de Poisson, también con XGBoost) en lugar de sí o no no mejora la ordenación (8 % y 68 % de las réplicas). El job mide ya los días con dos incidencias o más (`y_dia_varias`) con los mismos modelos del día siguiente.

**Los avisos de AEMET, la previsión real que faltaba.** Con el archivo completo de avisos (2018 - 2026, unas 1.500 piezas de 2 días) repetimos en el Lakehouse el modelo por horas y el del día siguiente (`mh_avisos_c_20261012_1845` y `dia_c_20261012_1845`). En Gold, cada hora sabe el nivel de los avisos en vigor que ya se habían emitido y el de los que empiezan en las 6 horas siguientes; el modelo del día siguiente solo cuenta los avisos de ese día enviados antes de las 20:00 de la víspera. El modelo por horas, en 2020 - 2025:

| Variante | Regresión logística | XGBoost | Rango |
| --- | --- | --- | --- |
| Meteorología, calendario e historia | 0,0140 | 0,0129 | 0,0158 |
| Más la lluvia de los días anteriores | 0,0137 | 0,0162 | 0,0164 |
| Más el fuego (FWI de CEMS y focos de FIRMS) | 0,0169 | 0,0156 | 0,0179 |
| Más los avisos de AEMET (desde 2018) | 0,0183 | 0,0189 | 0,0261 |
| Regla de la racha | 0,0126 | - | - |
| Solo el nivel del aviso | 0,0064 | - | - |

Con los avisos, y aunque la variante aprende con año y medio menos de historia, la combinación por percentiles sube un 65 % (+0,0095, de 0,0027 a 0,0220, en todas las réplicas) y la logística mejora con el intervalo por encima de cero (+0,0045, 99 %). La mejora crece en los últimos años: 0,046 en 2024 y 0,040 en 2025, el doble que sin avisos. El aviso solo vale poco (0,0064, por debajo de la regla de la racha), pero junto al tiempo dice qué temporales preocupaban a los meteorólogos. El bloque de fuego también ayuda a la logística (+0,0032, 99,8 %) y no a XGBoost. Con los avisos ya dentro, ni el fuego ni la racha frente al clima de cada celda añaden nada por horas (`mh_final_20261013_0259`): la combinación por percentiles se queda en 0,0223 y 0,0251 frente a 0,0261 de los avisos solos (mejor solo en el 14 % y el 35 % de las réplicas) y la logística pasa de 0,0183 a 0,0186 y 0,0195 sin separarse del ruido (70 % y 76 %). Para las horas nos quedamos con la variante de los avisos. Fuera de Spark, juntar por percentiles cada hora con el riesgo de su día que daba la víspera el modelo del día siguiente con la previsión de ICON-EU (más abajo) sube en 2024 - 2025 la logística por horas de 0,035 a 0,056 y XGBoost de 0,030 a 0,054, este con el intervalo por encima de cero, pero a la combinación de los dos, que ya llegaba a 0,057, apenas le añade nada (0,059, 61 % de las réplicas). En la prueba de 2026 hasta el 01.07, que no habíamos usado para nada (24 episodios, tasa base 0,0042, `mh_test2026_20261013_0442`), los avisos vuelven a ganar: la logística con avisos llega a 0,056, trece veces la tasa base, frente a 0,030 sin avisos y 0,014 de la regla de la racha, y la combinación por percentiles a 0,032 (mejor que la regla en el 93 % de las réplicas). Con tan pocos episodios los intervalos son anchos, pero el orden es el mismo que en 2020 - 2025.

Visto día a día (2018 - 2025, con los avisos enviados antes de las 20:00 de la víspera), el 15 % de los días tiene algún aviso amarillo o superior en las celdas de la distribuidora, y en ellos hay avería el 6,5 % de las veces frente al 2,5 % del resto: recogen un tercio de los días con avería. Los avisos de viento son los que más dicen: solo hay en el 2 % de los días, pero en el 19 % de ellos hay avería, siete veces más que el resto. La mayoría de los avisos del archivo son de calor y de lluvia. En el modelo del día siguiente, en 2022 - 2025:

| Variante | Regresión logística | XGBoost | Rango | Días con avería en el 10 % de días de más riesgo |
| --- | --- | --- | --- | --- |
| Calendario e historia | 0,055 | 0,055 | 0,055 | 21 % |
| Más el tiempo de ayer | 0,062 | 0,055 | 0,061 | 20 % |
| Más los avisos enviados la víspera | 0,107 | 0,096 | 0,132 | 31 % |
| Previsión perfecta del día | 0,117 | 0,122 | 0,130 | 36 % |
| Previsión perfecta y avisos | 0,132 | 0,106 | 0,123 | 36 % |
| Previsión perfecta y extremos | 0,139 | 0,092 | 0,125 | 34 % |
| Regla de la racha del día | 0,100 | - | - | 34 % |

Los avisos de la víspera casi doblan la PR-AUC frente al tiempo de ayer (+0,044, de 0,014 a 0,088, en todas las réplicas) y quedan a la altura de la previsión perfecta: la diferencia entre las dos no se distingue (72 % de las réplicas). Es lo más útil del día, porque convierte el modelo del día siguiente en uno que funciona con una previsión real y con archivo desde 2018, sin depender del reanálisis del propio día. Por años, la logística con avisos da 0,136, 0,108, 0,210 y 0,127, frente a 0,062 - 0,081 con el tiempo de ayer. El bloque de extremos, todavía con la racha frente al clima de la distribuidora, sube la logística de la previsión perfecta de 0,117 a 0,139 (96 %), pero empeora a XGBoost (solo es mejor en el 3 % de las réplicas); con la versión por celda ya no pasa (siguiente párrafo). Los días con dos incidencias o más (solo 16 en esos años) caen casi todos en los días marcados: con los avisos de la víspera, el 10 % de días de más riesgo recoge entre el 75 % y el 81 %.

**Con la previsión real de ICON-EU.** Con la racha frente al clima de cada celda en Gold y las previsiones de ICON-EU descargadas desde enero de 2024, repetimos el modelo del día siguiente (`dia_prev_20261012_2232`). Primero, el bloque de extremos por celda: sube la logística de la previsión perfecta de 0,117 a 0,145 (+0,026, de 0,002 a 0,055, en el 98 % de las réplicas), la combinación por percentiles de 0,130 a 0,144 (91 %) y ya no empeora a XGBoost (de 0,122 a 0,123). Con la previsión perfecta es la mejor variante de 2022 - 2025.

Después, la previsión real. El job entrena cada año con el reanálisis y puntúa 2024 y 2025 (58 días con avería) con tres versiones del tiempo del día: la del reanálisis, la que preveía ICON-EU la víspera y esa previsión llevada por cuantiles a la escala del reanálisis. Solo usa las variables del día que también da la previsión (bloque `dia_comun`):

| Variante | Tiempo del día | Regresión logística | XGBoost | Rango | Días con avería en el 10 % de días de más riesgo (logística) |
| --- | --- | --- | --- | --- | --- |
| Tiempo de ayer | - | 0,070 | 0,059 | 0,074 | 22 % |
| Tiempo del día | Reanálisis | 0,136 | 0,204 | 0,207 | 47 % |
| Tiempo del día | Previsión de ICON-EU | 0,141 | 0,153 | 0,156 | 48 % |
| Tiempo del día y avisos | Reanálisis | 0,174 | 0,152 | 0,184 | 43 % |
| Tiempo del día y avisos | Previsión de ICON-EU | 0,170 | 0,138 | 0,162 | 52 % |
| Tiempo del día y avisos | Previsión por cuantiles | 0,165 | 0,132 | 0,163 | 52 % |
| Regla de la racha del día | Previsión de ICON-EU | 0,105 | - | - | 41 % |

Sin los avisos, la previsión real pierde una cuarta parte de lo que daba el reanálisis en XGBoost y en el rango (de 0,207 a 0,156, peor en el 93 % de las réplicas), aunque sigue duplicando al tiempo de ayer. La regla de la racha apenas cambia (0,107 con el reanálisis, 0,105 con la previsión): la racha máxima prevista ordena los días casi igual que la del reanálisis. Con los avisos la pérdida es mucho menor: la logística pasa de 0,174 a 0,170, una diferencia que los datos no distinguen, y queda muy por encima del tiempo de ayer (+0,092, de 0,019 a 0,190, en el 99 % de las réplicas, y lo mismo con XGBoost y con el rango). Con esa variante, el 10 % de días de más riesgo de cada año recoge el 52 % de los días con avería y el 20 %, el 69 %. Llevar la previsión por cuantiles a la escala del reanálisis no aporta de forma clara. Son dos años y 58 días con avería, así que los intervalos son anchos.

**Los extremos y el detalle de los avisos con la previsión.** Para usar el bloque de extremos con la previsión, Gold calcula también la racha que preveía ICON-EU frente al clima de cada celda (`metp1_racha_clima_1h`, sobre el mismo percentil 98 del reanálisis) y el job, los días seguidos de calor con la temperatura prevista para el día encima de los que había hasta la víspera. Al puntuar con la previsión, estos extremos sustituyen a los del reanálisis igual que el tiempo del día. De cada aviso de AEMET tomamos además lo que anuncia (la racha en km/h y la lluvia en 12 horas y en una hora de su parámetro) y cuánto cubre (las horas del día con aviso de viento y la parte de los CT de la distribuidora con aviso de viento, con cualquier aviso de viento, lluvia, tormenta o nieve y con aviso de tormenta), en el bloque `avisos_detalle`. Resultados de `dia_ext_20261013_0332` y `dia_det_20261013_0353`:

| Variante | Reanálisis 2022 - 2025: logística | XGBoost | Rango | Previsión de ICON-EU 2024 - 2025: logística | XGBoost | Rango |
| --- | --- | --- | --- | --- | --- | --- |
| Tiempo del día y avisos | 0,137 | 0,093 | 0,126 | 0,170 | 0,138 | 0,162 |
| Más los extremos | 0,158 | 0,114 | 0,137 | 0,191 | 0,153 | 0,198 |
| Más el detalle de los avisos | 0,155 | 0,103 | 0,142 | 0,185 | 0,145 | 0,195 |
| Más los dos | 0,155 | 0,097 | 0,148 | 0,189 | 0,137 | 0,207 |

Los extremos son lo que más aporta. Con la previsión real suben la combinación por percentiles de 0,162 a 0,198 (+0,033, de 0,002 a 0,067, en el 99 % de las réplicas) y la logística de 0,170 a 0,191 (94 %); con el reanálisis mejoran a XGBoost con el intervalo entero por encima de cero (+0,020) y a la logística en el 97 % de las réplicas, que llega a 0,158, por encima incluso de la previsión perfecta con todas las variables del día y los extremos pero sin avisos (0,145). La logística y XGBoost mejoran en las tres distribuidoras, y la logística en tres de los cuatro años. El detalle de los avisos va en la misma dirección pero no se separa del ruido (entre el 57 % y el 93 % de las réplicas), y junto a los extremos solo sube el rango (0,148 con el reanálisis y 0,207 con la previsión, mejor que sin los dos en el 93 % y el 96 % de las réplicas). Lo que se pierde al pasar del reanálisis a la previsión sigue siendo poco: con avisos y extremos, la logística pasa de 0,204 a 0,191 y el rango de 0,218 a 0,198, sin diferencias que se distingan, y frente al tiempo de ayer la logística con la previsión llevada por cuantiles gana +0,108 (de 0,025 a 0,214). Con esa variante y la previsión, el 10 % de días de más riesgo de cada año recoge el 52 % de los días con avería y el 20 %, el 72 %; de los días con dos incidencias o más, el 81 % y el 94 % con el reanálisis.

Es la configuración que proponemos para el MVP: cada tarde, la previsión de ICON-EU para el día siguiente con la racha frente al clima de cada celda, los avisos de AEMET enviados antes de las 20:00 y una regresión logística. Es la más sencilla de explicar, la mejor con el reanálisis y casi igual que el rango con la previsión.

**Lo que hace falta para usarlo cada tarde.** El bloque `ayer` sale del reanálisis del día anterior, que se publica con días de retraso. Fuera de Spark, con la tabla diaria del job, sin ese bloque la logística con avisos y extremos baja de 0,158 a 0,142 en 2022 - 2025, pero con solo la racha máxima y la lluvia de ayer, que dan esa misma tarde las estaciones de AEMET o el análisis de ICON-EU, se queda casi igual (0,151 con el reanálisis y 0,188 con la previsión; los datos no distinguen la diferencia). Con dos días de antelación (la tarde del viernes para el domingo), con lo que preveía ICON-EU dos días antes y los avisos ya enviados entonces, la logística baja de 0,191 a 0,150 en 2024 - 2025 (peor en el 96 % de las réplicas) y el 10 % de días de más riesgo recoge el 43 % de los días con avería en lugar del 52 %. Sirve para avisar con tiempo, y la tarde anterior se confirma; a dos días solo hay avisos en el 10 % de los días, frente al 15 % la víspera.

**El riesgo de mañana, cada tarde.** Con esa variante (`operativa`: calendario, historia, avisos, el tiempo del día que también da la previsión, los extremos y la racha máxima y la lluvia del día anterior) montamos el job de cada tarde. `dag_riesgo_manana` corre en Airflow a las 20:30, hora de Madrid. Primero deja en la Landing (`s3://datalake/00_landing/vivo/<día>/`) la previsión de ICON-EU de cada celda, pedida a la Forecast API de Open-Meteo con los 31 días anteriores y los 3 siguientes, y los avisos de AEMET en vigor (`avisos_cap/ultimoelaborado` de toda España, que cruzamos con las celdas por el polígono de cada zona, como en Silver). Después `job_ml_riesgo_manana.py` entrena la logística con todos los días etiquetados, elige la regularización con el último cuarto de esos días, como en la evaluación por años, y puntúa el día siguiente con la previsión tal cual llega. Hay tres diferencias con la evaluación: el tiempo del día anterior sale del propio ICON-EU, porque el reanálisis llega con días de retraso; cuentan todos los avisos en vigor a esa hora, aunque se hayan enviado después de las 20:00; y cuando la copia de Calser del lakehouse se queda atrás (más de 7 días sin incidencias nuevas), la historia de incidencias se deja vacía y el modelo toma la mediana de los días de entrenamiento, porque contar cero incidencias en el último mes haría parecer tranquila una red de la que no sabemos nada. Si AEMET no responde, el día se puntúa sin avisos; sin previsión no se puntúa.

Para cada distribuidora, `l3_gold.riesgo_dia_siguiente` guarda la probabilidad de que empiece al menos una interrupción, cuántas veces está por encima de su tasa base, su percentil entre los días de entrenamiento y un nivel: muy alto desde el percentil 95, alto desde el 90 y medio desde el 75. En la evaluación, el 10 % de días de más riesgo de cada año recogía la mitad de los días con avería, y eso es lo que marcan los niveles alto y muy alto. Guarda también los tres motivos que más suben la probabilidad frente a un día medio, con el factor por el que multiplican la razón de probabilidades, y las tres zonas con más racha prevista frente a su clima. Los motivos suman las variables que van juntas (la racha y el viento, la lluvia del día y la de su hora más lluviosa), porque la regresión reparte el peso de dos variables parecidas con signos opuestos y, por separado, saldrían motivos sin sentido como "0 mm de lluvia en una hora". Por debajo de la tasa base no se da ningún motivo. Cada fila dice además si había avisos (`avisos_disponibles`), si la historia estaba al día (`historia_al_dia`), qué parte de los CT tenía previsión y hasta qué día se entrenó el modelo.

**Lo que no ha funcionado en el día siguiente.** La lluvia frente al clima de cada celda, calculada como la racha (30 mm en un día son normales en la sierra y raros en el llano), empeora la logística de avisos y extremos de 0,158 a 0,128 (peor en el 97 % de las réplicas). En el modelo tampoco queda margen: una búsqueda más amplia de la regularización, interacciones de la racha y la lluvia con el verano y con el aviso de viento, XGBoost con cinco semillas o monótono no mejoran a la logística.

**La prueba de 2026.** Con la configuración ya elegida en 2022 - 2025 puntuamos una sola vez los días de 2026 hasta el 01.07 (las etiquetas de Calser se conocen hasta agosto), que no habíamos usado para nada; cada variante aprende de los años anteriores (`dia_test2026_20261013_0428`). Hubo 24 días con avería y la tasa base es de 0,044. Con la previsión real, la logística con avisos y extremos llega a 0,132, tres veces la tasa base, y el 20 % de días de más riesgo recoge el 52 % de los días con avería. Pero no se distingue del modelo que solo sabe el tiempo de ayer (0,164 en los mismos días; con el reanálisis, la diferencia va de -0,15 a +0,12). En 2026 los temporales de enero llegaron seguidos (del 23 al 29 hubo avería cinco de los siete días) y el tiempo de ayer acierta más que otros años: el 17 % de los días con avería venía de un día con avería antes de las 20:00, frente a entre el 0 % y el 8 % de 2018 - 2025, salvo 2024 (19 %). Además, 15 de los 24 días no tenían ningún aviso y en 8 la racha máxima del día no llegó a 50 km/h: son averías de equipos o sin causa conocida que el tiempo no anuncia. Añadir las interrupciones del día anterior que ya ve el SCADA a las 20:00 sube la logística en 2026 (de 0,126 a 0,146, 92 % de las réplicas) pero la baja en 2022 - 2025 (de 0,158 a 0,148), así que no la añadimos. La prueba muestra que el modelo funciona fuera de los años con los que lo elegimos, pero con 24 días no basta para separar unas variantes de otras.

**El índice de peligro de incendio y los focos.** Con el FWI de CEMS de la víspera (el máximo de las celdas de la distribuidora), los 8 días con corte por incendio de 2017 - 2026 estaban en peligro muy alto o extremo (38 o más), pero así está uno de cada cuatro días del periodo (26 %). Con 50 o más quedan 4 de los 8 en el 11 % de los días. Para el resto de causas el FWI no cambia el riesgo: los equipos y las averías de causa desconocida pasan igual con peligro bajo que extremo. Los focos de FIRMS a menos de 15 km el día anterior solo estaban en 2 de los 8 días, porque el incendio suele empezar el mismo día del corte. Por eso los focos sirven para el aviso de las horas siguientes (el bloque de fuego mejora la logística por horas) y no para la víspera, donde el FWI marca la temporada de peligro pero no el día.

**Los rayos del satélite.** Descargamos los flashes del Lightning Imager de MTG (EUMETSAT, producto LI-2-LFL, cada 10 minutos) en las 3 - 4 horas anteriores a las incidencias de agosto de 2024 a noviembre de 2025 y en ventanas de control sin incidencias: 2.593 productos y 19.318 flashes en la caja de estudio. Antes del 17 % de las incidencias hubo rayos a menos de 10 km de las celdas de la distribuidora en la hora anterior, frente al 1,1 % de las ventanas de control, y a menos de 20 km en las 3 horas anteriores, el 24 % frente al 2,8 %. Entre las incidencias con rayos a menos de 20 km hay 2 de las 17 averías de equipos de la muestra y 2 de las 8 de causa desconocida, así que parte de esas causas pueden ser rayos que no se anotaron como tales. Con 46 incidencias en la muestra no da para entrenar, pero en el MVP es la señal más directa para el aviso de la hora siguiente.

**Precursores en el SCADA.** Siguiendo lo que publican Texas A&M, SEL y los fabricantes de relés (apartado 9), medimos en TedisNet (2021 - 2026) cuántas señales de cada tipo hubo antes de 1.135 inicios de interrupción en el CT, en su grupo de red y en las posiciones que lo alimentan (aguas arriba), frente a 3.384 horas de control de los mismos CT sin incidencias a menos de 7 días:

| Señal en los 7 días anteriores | Antes de una incidencia | En las horas de control | Veces más |
| --- | --- | --- | --- |
| Paso de falta en las posiciones aguas arriba | 2,7 % | 0,6 % | 4,4 |
| Paso de falta en el grupo de red | 6,6 % | 1,8 % | 3,6 |
| Falta de tensión en el CT el día anterior | 2,8 % | 1,1 % | 2,6 |
| Falta entre fases aguas arriba | 38,7 % | 25,6 % | 1,5 |
| Falta a tierra aguas arriba | 42,1 % | 29,3 % | 1,4 |
| Disparo aguas arriba | 39,5 % | 30,3 % | 1,3 |

Antes de las averías de equipos la señal es más clara: faltas entre fases aguas arriba en la semana anterior en el 47 % de los casos y faltas a tierra en el 52 %, frente al 26 % y el 29 % de las horas de control. Es lo que describen los fabricantes: faltas intermitentes que acaban en una avería permanente. En las averías por meteorología también suben (faltas a tierra en el grupo, 20 % frente a 10 %), pero ahí pesa que los temporales vienen seguidos. La señal está en la última semana: con 28 o 56 días se diluye (entre 1,0 y 1,5 veces), así que no hace falta añadir a Gold ventanas más largas que las de 7 días que ya usa el modelo de TedisNet. Coincide con lo que más pesa en ese modelo (faltas a tierra del grupo, reenganches aguas arriba).

**Dónde, el día de riesgo.** Para mandar las brigadas no basta con saber qué día será malo; hay que saber dónde. Con la meteorología de cada celda montamos una tabla de cabecera y día (47 cabeceras, 2017 - 2025) con el tamaño de la cabecera, su historia y el tiempo de sus celdas, y miramos, en los días con avería de 2022 - 2025 en EOSA y Pitarch (Valle de Santa Ana tiene una sola cabecera), en qué puesto de riesgo quedaba la cabecera que falló (unas 340). Ordenadas solo por su número de CT, la cabecera más grande es la que falla en el 17 % de los casos y una de las tres más grandes en el 32 %, cuando al azar serían el 4 - 5 % y el 12 - 14 %. Añadir la historia y el tiempo de las celdas de cada cabecera apenas cambia ese orden dentro del día (entre el 32 % y el 35 % entre las tres primeras), aunque sí ayuda a saber qué días son malos. Tampoco lo cambia el riesgo anual: por CT, el 10 % de CT de más riesgo solo recoge el 10 % de los CT que fallan cada día, porque una avería en una línea apaga todos los CT de la cabecera a la vez y el ranking anual, medido en kVA·h, sube sobre todo a los CT grandes. Con el reanálisis a 10 km, el tiempo no distingue una cabecera de la de al lado: el día de riesgo, lo que dice dónde mirar primero es el tamaño de la red de cada cabecera.

## 9. Lo que hacen otras distribuidoras y los fabricantes

Revisamos lo publicado por centros de investigación, distribuidoras y fabricantes de aparamenta de otros países para ver qué variables pesan y qué secuencias de eventos anuncian una avería.

**Modelos de predicción en otros países.** El modelo de la Universidad de Connecticut para Eversource trabaja en celdas de 2 km, prevé hasta tres días antes y mide el viento como horas por encima de un umbral, junto con la vegetación cerca de la línea y la humedad del suelo; su error es mayor en las tormentas convectivas de verano ([Wanik et al.](https://hartman.byu.edu/docs/files/WanikAnagnostouHartmanFredianiAstitha_StormDamage.pdf)). El modelo de huracanes de Guikema y Quiring añade la duración del viento fuerte y el suelo saturado, porque con el suelo empapado caen más árboles y apoyos. En Finlandia, el Instituto Meteorológico predice los cortes por tormentas convectivas con objetos de radar y bosques aleatorios (AUC de 0,86 - 0,88 con 33.858 cortes) y, en las borrascas, el viento medio y el arbolado pesan más que la racha ([Tervo et al.](https://nhess.copernicus.org/articles/21/607/2021/nhess-21-607-2021.pdf)). En Escocia se predicen las averías diarias por región con uno o dos días de antelación, y en España un trabajo de 2025 con datos de Valencia y Albacete detecta el 38 % y el 73 % de los días extremos con un 1 % y un 3 % de falsas alarmas. En China, State Grid modeliza tifones, hielo y rayos sobre las líneas de 10 kV, y en Rusia Rosseti da escenarios mensuales porque la autocorrelación de las desconexiones es débil. Lo que se repite: el viento como duración por encima de un umbral, mucho peso del territorio, modelos sencillos y evaluación por tormenta, por día o por año, con la partición aleatoria desaconsejada porque da cifras optimistas.

**Precursores según los fabricantes.** Las antelaciones medidas vienen casi todas del programa de Texas A&M y EPRI, que analiza las formas de onda de la cabecera de unas 1.000 líneas ([Wischkaemper](https://cigre-usnc.org/wp-content/uploads/2023/10/3A-4_Wischkaemper.pdf)). Un arco en un puente acabó en avería 18 días después de las primeras ráfagas. Un árbol provocó 44 faltas repetidas en dos años antes de quemar el conductor, con muchas interrupciones momentáneas y ningún corte largo. Un terminal de cable dio 79 pulsos en seis meses y medio, muchos de menos de medio ciclo. SEL cuenta las sobreintensidades que se apagan solas en medio ciclo como aviso de un cable que falla, y ABB, ZIV y Siemens describen faltas a tierra intermitentes de pocos milisegundos en redes de neutro aislado o compensado, ligadas a humedad en empalmes y terminales, que acaban en falta permanente. Para los CT y los transformadores (temperatura de las conexiones, descargas parciales, gases en el aceite) los fabricantes no publican antelaciones; EA Technology sí da una cifra: en dos años fallaron menos del 2 % de los cables de 33 kV clasificados en verde por descargas parciales, el 21 % de los ámbar y el 41 % de los rojos. Puentes y terminales de cable son justo las primeras averías de equipos de nuestras distribuidoras (apartado 3).

**Qué podemos aplicar con nuestros datos**, de más a menos útil:

1. Recurrencia por CT y por posición en TedisNet: faltas a tierra, reenganches, disparos y microcortes. Medido (apartado 8): la señal está en los 7 días anteriores y se diluye con 30 o 60 días.
2. Precursores cruzados con la previsión: faltas a tierra recientes con lluvia prevista (terminales y nidos mojados) y faltas entre fases con viento previsto (conductores que se tocan, ramas).
3. Viento como horas por encima de un umbral y como exceso sobre el clima de cada zona. Lo primero no aporta y lo segundo, calculado celda a celda, mejora la logística del día siguiente (bloque `extremos`, apartado 8).
4. Rayos medidos por el satélite (EUMETSAT) para ver qué incidencias de causa desconocida tuvieron rayos cerca.
5. Evaluación por día y zona con detección y falsas alarmas, comparable con la de Valencia y Albacete: es lo que da el modelo del día siguiente.
6. Para cables y terminales, la duración de las olas de calor, la humedad del suelo profundo y las oscilaciones de carga, y pedir a las distribuidoras que activen en los relés que lo permitan los contadores de faltas intermitentes.
7. Para las cigüeñas, la época de cría y la lluvia después de días secos; para los incendios, los focos de FIRMS y el índice de peligro de Copernicus, que ya están integrados.

Datos públicos de otros países que podrían servir de comparación: [EAGLE-I](https://pmc.ncbi.nlm.nih.gov/articles/PMC10915145) (clientes sin suministro por condado cada 15 minutos en EE. UU., 2014 - 2022, sin causa), las interrupciones activas de [E-REDES](https://opennetzero.org/dataset/active-energy-interruptions) en Portugal (el Alentejo linda con Extremadura) y los índices TIEPI y NIEPI por provincia del [MITECO](https://www.miteco.gob.es/es/energia/energia-electrica/electricidad/calidad-servicio/indices.html).

## 10. Cómo se ejecuta

En el portátil, desde `D:\GitHub\gridpredic-infrastructure`:

```powershell
# Bronze del módulo de Extremadura (las tres bases de Calser) y Silver de causas
docker compose exec -T airflow spark-submit /app/jobs/01_bronze/job_bronze_sqlserver_batch.py --database Calser_EOSA --table incidencias_ex --extract-mode FULL --write-mode OVERWRITE
docker compose exec -T spark-master /opt/spark/bin/spark-submit /app/jobs/02_silver/job_silver_f_incidencia_ex.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit /app/jobs/02_silver/job_silver_f_incidencia_causa.py

# Fuentes públicas nuevas: aves de GBIF e incendios de EFFIS
docker compose exec -T spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes "aves,incendios"

# Meteorología desde 2017, CAPE, variables extra y previsiones de ICON-EU del día anterior (cada fuente sigue donde iba si se acaba el cupo diario)
docker compose exec -T spark-master python3 /app/jobs/00_landing/job_landing_meteo_openmeteo.py --fuente openmeteo
docker compose exec -T spark-master python3 /app/jobs/00_landing/job_landing_meteo_openmeteo.py --fuente openmeteo_convectivo
docker compose exec -T spark-master python3 /app/jobs/00_landing/job_landing_meteo_openmeteo.py --fuente openmeteo_extra
docker compose exec -T spark-master python3 /app/jobs/00_landing/job_landing_meteo_openmeteo.py --fuente openmeteo_previo
docker compose exec -T spark-master /opt/spark/bin/spark-submit /app/jobs/01_bronze/job_bronze_meteo_openmeteo.py --fuente openmeteo_previo

# Avisos de AEMET, focos de FIRMS e índice de peligro de incendio de CEMS con las claves de .env (lo que falta se pide en la siguiente ejecución)
docker compose exec -T -e AEMET_API_KEY=<clave> spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes avisos
docker compose exec -T -e MAP_KEY=<clave> spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes focos
docker compose exec -T -e COPERNICUS_EWDS_KEY=<clave> spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes fwi

# Análisis de causas, riesgo anual con el modelo por familias y horas de riesgo con la historia larga
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_analisis_red.py,/app/jobs/04_ml/job_ml_horas_riesgo.py /app/jobs/04_ml/job_causas_incidencias.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_riesgo_ct.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_meteo_historia.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_combinar_horas.py --config /app/config/04_ml/config_ml.json --hr-run <hr_...> --mh-run <mh_...>
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 4g --py-files <los mismos> /app/jobs/04_ml/job_ml_riesgo_dia.py --config /app/config/04_ml/config_ml.json
```

Los parámetros están en `config_ml.json` (`causas_incidencias`, `meteo_historia`, `meteo_dia`, `combinar`), en `config_bronze_meteo.json` (`openmeteo`, `openmeteo_convectivo`, `openmeteo_extra`, `openmeteo_previo`) y en `config_bronze_publicos.json` (`aves`, `incendios`, `avisos`, `focos`, `fwi`). Los resultados quedan en `exportar_dir/<run_id>/` y en las tablas `l3_gold.analisis_causas`, `ml_metricas`, `ml_runs` y `ml_predicciones_meteo_historia`.

El riesgo de mañana no hace falta lanzarlo: `dag_riesgo_manana` corre solo cada tarde a las 20:30 (la interfaz de Airflow está en http://localhost:8084). A mano, para el día siguiente al de hoy o para otro día (PowerShell 7):

```powershell
docker compose exec -T airflow airflow dags trigger dag_riesgo_manana
docker compose exec -T airflow airflow dags trigger dag_riesgo_manana --conf '{"fecha": "2026-10-20"}'
```

Sus parámetros están en `riesgo_manana` de `config_ml.json` (variante, modelo, hora, niveles, carpeta de la Landing y peticiones a Open-Meteo y a AEMET), y la clave de AEMET llega al contenedor de Airflow desde `.env`. Cada tarde el resultado queda en `l3_gold.riesgo_dia_siguiente`, en `ml_runs` y en `/tmp/ml_resultados/<run_id>/resumen.md` del contenedor de Airflow.
