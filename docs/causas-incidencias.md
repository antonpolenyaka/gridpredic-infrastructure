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

Para el riesgo meteorológico por horas, casi: rachas, lluvia, temperatura, humedad y presión cubren las causas que más pesan y responden con fuerza, y desde ahora tenemos nueve años de historia en lugar de cinco. Lo que más echamos en falta son los rayos observados (las tormentas de verano se ven mal en la lluvia y el viento del reanálisis) y una previsión real archivada para medir el modelo como funcionaría de verdad.

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
| [AEMET OpenData](https://opendata.aemet.es/), [avisos de fenómenos adversos](https://datos.gob.es/es/catalogo/e05068001-avisos-meteorologicos) | Avisos amarillos, naranjas y rojos de viento, lluvia, tormentas y nieve por zona, en CAP, desde el 18.06.2018 (CC BY 4.0); rayos solo como mapa del momento | Clave gratuita por correo, válida tres meses | Integrada de Landing a Gold (`avisos` en `config_bronze_publicos.json`, `l2_silver.f_aviso_meteo`, `met_aviso_*`), con la referencia `naif_aviso_aemet` en el modelo por horas y el bloque `avisos` en el del día siguiente. El archivo solo admite 2 días por petición y limita las peticiones por minuto, así que la historia completa son unas 1.500 peticiones. Es una previsión real emitida con antelación y lo que hoy mira un jefe de brigadas |
| [Open-Meteo Previous Runs API](https://open-meteo.com/en/docs/previous-runs-api), ICON-EU | Lo que preveía el modelo para cada hora uno y dos días antes, desde enero de 2024 | Libre | Integrada de Landing a Gold (`openmeteo_previo` en `config_bronze_meteo.json`, columnas `*_prev1_*` de `l2_silver.f_meteo_hora`, `metp1_*` y `metp2_*` de Gold). Es la previsión real con la que medimos el modelo del día siguiente. De ECMWF IFS no hay rachas previstas en el archivo y de ICON-EU sí |
| [EUMETSAT Data Store](https://data.eumetsat.int/), Lightning Imager de MTG | Rayos observados desde el satélite geoestacionario: flashes con posición, hora, duración y radiancia cada 10 minutos (producto LI-2-LFL, unos 0,4 MB) | Cuenta gratuita y credenciales del Data Store | Probada. El archivo empieza el 04.07.2024. Entre las 13:50 y las 14:00 UTC del 12.08.2025 hubo 54 flashes en la caja de estudio, en plena ola de incendios del norte de Cáceres; EOSA cortó por incendio a las 03:33 del día siguiente. Para el MVP es el dato de rayos en tiempo real; con año y medio de historia no da para entrenar |
| [REE, REData](https://www.ree.es/es/apidatos) | Demanda nacional cada 5 minutos, generación, intercambios y precios; de calidad de servicio, el tiempo de interrupción medio de la red de transporte, por año | API libre, sin clave | Revisada y descartada: la demanda nacional sigue a la temperatura, que ya tenemos, y la calidad de la red de transporte solo viene agregada por año. La demanda por comunidad no la devuelve para Extremadura |
| [GBIF](https://www.gbif.org/developer/occurrence) | Observaciones de cigüeña blanca y de aves por celda | API libre, sin clave | Integrada (`l2_silver.f_aves_celda_anio`). Sale al revés de lo esperado (más observaciones, menos averías por fauna): mide dónde observa la gente, no dónde anidan |
| [EFFIS](https://forest-fire.emergency.copernicus.eu/), áreas quemadas | Incendios de 30 ha o más con fecha y perímetro | WFS libre | Integrada: 507 áreas quemadas alrededor de nuestras celdas. Los CT sin incendios cerca en cinco años no tuvieron ninguna incidencia por incendio |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | Focos de incendio activos de MODIS y VIIRS, con archivo desde 2000 (MODIS) y 2012 (VIIRS) | Clave gratuita (MAP_KEY), peticiones de 5 días como mucho | Integrada de Landing a Gold (`focos` en `config_bronze_publicos.json`, `l1_bronze.firms_foco`, `l2_silver.f_foco_incendio`, `met_focos_24h` y `met_focos_frp_max_24h` por celda, contados cuando FIRMS los habría publicado). En 7 de los 8 episodios de corte por incendio (2017 - 2026) había focos a menos de 10 km de las celdas de la distribuidora en las 48 horas anteriores, y en el que falta (03.10.2022) no hubo ninguno. Anuncia los cortes preventivos con horas de antelación |
| [Copernicus CEMS](https://ewds.climate.copernicus.eu/datasets/cems-fire-historical-v1), índice de peligro de incendio | FWI diario desde 1940 (CC BY) | Cuenta gratuita | Propuesta para los cortes preventivos, junto con FIRMS |
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

En fauna gana la lista naif (los nidos están donde estaban) y en meteorología y equipos el modelo saca entre 10 y 15 puntos a la lista naif. El mejor bloque está elegido entre ocho mirando los mismos años, así que esas cifras son algo optimistas. Al sumar un modelo por familia, con una familia más para el resto de incidencias, el ranking de CT mejora con todos los bloques: el 10 % de CT con más riesgo recoge entre el 29 % y el 34 % de los kVA·h del año siguiente, frente al 25 - 28 % del modelo único y el 28 % de la lista naif. Por cabecera no mejora (66 % frente a 71 %): ahí la historia de la línea ya lo dice todo. Lo hemos integrado en `job_ml_riesgo_ct.py` como `por_causa:<bloque>`, y el ranking del año siguiente añade lo que espera cada familia en cada CT y cabecera (`esperado_meteo`, `esperado_fauna`, `esperado_equipo`, `esperado_resto`), que es lo que dice qué actuación toca: proteger apoyos, revisar equipos o podar.

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

Saber qué tiempo hará duplica la PR-AUC de los días (+0,061, de 0,026 a 0,114, mejor en todas las réplicas) y lo hace todos los años: entre 0,137 y 0,170 cada año con la previsión perfecta, frente a 0,061 - 0,076 con el tiempo de ayer. Para comprobar cuánto queda con una previsión de verdad descargamos lo que ICON-EU preveía el día anterior. A falta de completar la descarga (el cupo diario del plan gratuito se acabó a mitad), con las celdas de Pitarch y Valle de Santa Ana en 2024 - 2025 (27 días con avería) la previsión real da una PR-AUC de 0,080 - 0,131 frente a 0,075 - 0,079 con el reanálisis del propio día y 0,058 sin el tiempo del día. Son pocos días para separar las dos primeras cifras, pero la previsión del día anterior conserva lo que aporta el tiempo del día. El job entrena con el reanálisis y puntúa los años con previsión dos veces más, con la previsión tal cual y llevada por cuantiles a la escala del reanálisis, porque ICON-EU da rachas más fuertes que ECMWF IFS (45 frente a 39 km/h de media en la racha máxima del día).

## 9. Lo que hacen otras distribuidoras y los fabricantes

Revisamos lo publicado por centros de investigación, distribuidoras y fabricantes de aparamenta de otros países para ver qué variables pesan y qué secuencias de eventos anuncian una avería.

**Modelos de predicción en otros países.** El modelo de la Universidad de Connecticut para Eversource trabaja en celdas de 2 km, prevé hasta tres días antes y mide el viento como horas por encima de un umbral, junto con la vegetación cerca de la línea y la humedad del suelo; su error es mayor en las tormentas convectivas de verano ([Wanik et al.](https://hartman.byu.edu/docs/files/WanikAnagnostouHartmanFredianiAstitha_StormDamage.pdf)). El modelo de huracanes de Guikema y Quiring añade la duración del viento fuerte y el suelo saturado, porque con el suelo empapado caen más árboles y apoyos. En Finlandia, el Instituto Meteorológico predice los cortes por tormentas convectivas con objetos de radar y bosques aleatorios (AUC de 0,86 - 0,88 con 33.858 cortes) y, en las borrascas, el viento medio y el arbolado pesan más que la racha ([Tervo et al.](https://nhess.copernicus.org/articles/21/607/2021/nhess-21-607-2021.pdf)). En Escocia se predicen las averías diarias por región con uno o dos días de antelación, y en España un trabajo de 2025 con datos de Valencia y Albacete detecta el 38 % y el 73 % de los días extremos con un 1 % y un 3 % de falsas alarmas. En China, State Grid modeliza tifones, hielo y rayos sobre las líneas de 10 kV, y en Rusia Rosseti da escenarios mensuales porque la autocorrelación de las desconexiones es débil. Lo que se repite: el viento como duración por encima de un umbral, mucho peso del territorio, modelos sencillos y evaluación por tormenta, por día o por año, con la partición aleatoria desaconsejada porque da cifras optimistas.

**Precursores según los fabricantes.** Las antelaciones medidas vienen casi todas del programa de Texas A&M y EPRI, que analiza las formas de onda de la cabecera de unas 1.000 líneas ([Wischkaemper](https://cigre-usnc.org/wp-content/uploads/2023/10/3A-4_Wischkaemper.pdf)). Un arco en un puente acabó en avería 18 días después de las primeras ráfagas. Un árbol provocó 44 faltas repetidas en dos años antes de quemar el conductor, con muchas interrupciones momentáneas y ningún corte largo. Un terminal de cable dio 79 pulsos en seis meses y medio, muchos de menos de medio ciclo. SEL cuenta las sobreintensidades que se apagan solas en medio ciclo como aviso de un cable que falla, y ABB, ZIV y Siemens describen faltas a tierra intermitentes de pocos milisegundos en redes de neutro aislado o compensado, ligadas a humedad en empalmes y terminales, que acaban en falta permanente. Para los CT y los transformadores (temperatura de las conexiones, descargas parciales, gases en el aceite) los fabricantes no publican antelaciones; EA Technology sí da una cifra: en dos años fallaron menos del 2 % de los cables de 33 kV clasificados en verde por descargas parciales, el 21 % de los ámbar y el 41 % de los rojos. Puentes y terminales de cable son justo las primeras averías de equipos de nuestras distribuidoras (apartado 3).

**Qué podemos aplicar con nuestros datos**, de más a menos útil:

1. Recurrencia por CT y por posición en TedisNet: faltas a tierra, reenganches, disparos y microcortes en los últimos 7, 30 y 60 días, el tiempo desde el último y una marca de falta que se repite en el mismo sitio. Antes conviene medir en Calser y TedisNet con cuánta antelación aparece el primer precursor en la posición de cada avería.
2. Precursores cruzados con la previsión: faltas a tierra recientes con lluvia prevista (terminales y nidos mojados) y faltas entre fases con viento previsto (conductores que se tocan, ramas).
3. Viento como horas por encima de 50, 60 y 80 km/h y viento medio además de la racha, con la humedad del suelo que ya tenemos.
4. Rayos medidos por el satélite (EUMETSAT) para ver qué incidencias de causa desconocida tuvieron rayos cerca.
5. Evaluación por día y zona con detección y falsas alarmas, comparable con la de Valencia y Albacete: es lo que da el modelo del día siguiente.
6. Para cables y terminales, la duración de las olas de calor, la humedad del suelo profundo y las oscilaciones de carga, y pedir a las distribuidoras que activen en los relés que lo permitan los contadores de faltas intermitentes.
7. Para las cigüeñas, la época de cría y la lluvia después de días secos; para los incendios, los focos de FIRMS y el índice de peligro de Copernicus.

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

# Avisos de AEMET y focos de FIRMS con las claves de .env (lo que falta se pide en la siguiente ejecución)
docker compose exec -T -e AEMET_API_KEY=<clave> spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes avisos
docker compose exec -T -e MAP_KEY=<clave> spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes focos

# Análisis de causas, riesgo anual con el modelo por familias y horas de riesgo con la historia larga
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_analisis_red.py,/app/jobs/04_ml/job_ml_horas_riesgo.py /app/jobs/04_ml/job_causas_incidencias.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_riesgo_ct.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_meteo_historia.py --config /app/config/04_ml/config_ml.json
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 6g --py-files <los mismos> /app/jobs/04_ml/job_ml_combinar_horas.py --config /app/config/04_ml/config_ml.json --hr-run <hr_...> --mh-run <mh_...>
docker compose exec -T spark-master /opt/spark/bin/spark-submit --driver-memory 4g --py-files <los mismos> /app/jobs/04_ml/job_ml_riesgo_dia.py --config /app/config/04_ml/config_ml.json
```

Los parámetros están en `config_ml.json` (`causas_incidencias`, `meteo_historia`, `meteo_dia`, `combinar`), en `config_bronze_meteo.json` (`openmeteo`, `openmeteo_convectivo`, `openmeteo_extra`, `openmeteo_previo`) y en `config_bronze_publicos.json` (`aves`, `incendios`, `avisos`, `focos`). Los resultados quedan en `exportar_dir/<run_id>/` y en las tablas `l3_gold.analisis_causas`, `ml_metricas`, `ml_runs` y `ml_predicciones_meteo_historia`.
