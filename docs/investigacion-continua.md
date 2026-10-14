# Investigación continua: lo que probamos, lo que entró y lo que descartamos

Este documento es el cuaderno de laboratorio del proyecto desde que el sistema está en marcha. Recoge cada hipótesis que probamos con los datos en la mano, cómo la probamos, qué salió y qué decidimos, incluidas las que no funcionaron. La regla es la misma que en clase: mirar los datos, hacer el gráfico o la tabla, formular la hipótesis, medirla con una prueba que no se pueda engañar (años rodantes, el mismo conjunto de días para todas las variantes) y decidir. Una fuente que no aporta se descarta explicando por qué, y queda escrito para no volver a probarla sin motivo.

Los documentos de resultados son [caso-de-negocio.md](caso-de-negocio.md) (el valor), [causas-incidencias.md](causas-incidencias.md) (el porqué de cada pieza) y [network-analysis.md](network-analysis.md) (la red y el dinero). Aquí solo está el camino.

## 1. Cómo medimos si algo aporta

Todas las pruebas sobre el modelo del día usan el mismo banco: los días de 2022 a 2026 de la Distribuidora 1 y la Distribuidora 2 que puntúa el replay de `job_ml_caso_negocio.py` (3.348 días, 143 con alguna incidencia de causa local, un 4,3 %), con la probabilidad que el sistema habría dado la víspera. Una variable candidata se añade a esa probabilidad (en escala logit) en una regresión logística y miramos tres cosas:

- el coeficiente y su p-valor con todos los días (si no es distinto de cero, no hay nada que rascar);
- el AUC y el Brier por años rodantes: cada año se puntúa con el modelo ajustado a los años anteriores, igual que en la operación real; es la cifra que cuenta;
- una tabla de contraste con los días partidos por la variable, para ver el efecto a ojo y con un test exacto.

Si el AUC rodante no sube y el Brier no baja, la variable no entra, aunque la literatura la respalde: con 143 días positivos el modelo no tiene sitio para variables que solo añaden ruido.

## 2. Lo que dice la literatura y lo que ya teníamos

- **Modelos de interrupciones por tormenta en EE. UU.** El modelo de la Universidad de Connecticut para Eversource (Cerrai, Wanik, Bhuiyan, Zhang, Yang, Frediani y Anagnostou, "Predicting Storm Outages Through New Representations of Weather and Vegetation", IEEE Access 7, 2019, 29639-29654, https://doi.org/10.1109/ACCESS.2019.2902558) predice el número de cortes por tormenta a partir de la previsión numérica del tiempo (máximos y duraciones de racha, lluvia), los activos de la red, la cubierta del suelo junto a las líneas y el estado de las hojas; con un clasificador de tormentas y la combinación de varios modelos bajan el error mediano de las predicciones por tormenta del 130 % al 59 %. Es el mismo planteamiento que el nuestro a escala de una gran compañía (120 tormentas, miles de cortes): el tiempo manda, la vegetación y los activos matizan. Lo que no podemos copiar es el tamaño de la muestra.
- **Caída de árboles con viento.** Lorenz, Becker, Gardiner, Ulbrich, Hanewinkel y Schmitz, "Tree fall along railway lines: modelling the impact of wind and other meteorological factors", Natural Hazards and Earth System Sciences 25, 2025, 2179-2196, https://doi.org/10.5194/nhess-25-2179-2025: con los árboles caídos sobre las vías de Alemania, el predictor más fuerte es la anomalía de la racha frente a lo habitual del lugar, después la duración del viento por encima del percentil 90 local y su interacción con la racha; el factor de racha (racha entre viento medio) baja el riesgo; la anomalía de humedad del suelo y la lluvia relativa al percentil 90 suben el riesgo pero pesan poco. Brier skill score de 0,069. Nuestro modelo del día ya tenía la racha frente al percentil 98 local y el índice de Klawa y Ulbrich (NHESS 3, 2003, 725-732), que pondera por la duración, y habíamos probado sin éxito las horas por encima de umbrales fijos de 40 a 70 km/h y la lluvia de 3 y 30 días (apartado 8 de [causas-incidencias.md](causas-incidencias.md)); la humedad del suelo está en Gold como variable horaria para el modelo por horas (fuente `openmeteo_extra`), pero nunca la habíamos puesto en el modelo del día. Lo que faltaba por probar era el suelo, la duración relativa al clima del punto y el factor de racha. Es la prueba del apartado 3.
- **Lo que hacen las grandes distribuidoras españolas.** Endesa lideró PASTORA (Preventive Analysis of Smart Grids with Real Time Operation and Renewable Assets Integration, anunciado el 03.01.2019; 31 meses y 2,8 millones de euros con AICIA de la Universidad de Sevilla, la Universidad de Granada, Ormazabal, Ayesa e Ingelectus, con apoyo del CDTI y fondos FEDER) para anticipar incidencias en líneas de media y baja tensión con aprendizaje automático sobre el estado en tiempo real de la red digitalizada y los centros de transformación con sensores de su Smartcity Málaga; no hay cifras públicas de acierto. La diferencia con nuestro caso es la escala y la fuente: ellos parten de la red sensorizada, nosotros de la meteorología, porque en una red rural con pocos sensores el tiempo explica más que el estado eléctrico (lo medimos en el modelo por horas).
- **El mismo problema en España.** Prieto-Godino, Peláez-Rodríguez, Pérez-Aracil, Pastor-Soriano y Salcedo-Sanz (Universidad de Alcalá), "Predicting weather-related power outages in large scale distribution grids with deep learning ensembles", International Journal of Electrical Power and Energy Systems 170, 2025, 110811: incidencias diarias de las redes de distribución de Valencia y Albacete a partir del reanálisis, con conjuntos de redes neuronales; los mejores modelos detectan el 38 % y el 73 % de los días extremos con un 1 % y un 3 % de falsas alarmas. Es la referencia más cercana a la nuestra: mismo país, mismo grano (el día), misma fuente meteorológica. Nuestras cifras del caso de negocio (el 12,5 % de los días en alerta recoge el 53 % de las incidencias y el 72 % de las horas-cliente con la previsión real) están en el mismo orden con una red cien veces más pequeña y un modelo que cabe en una hoja.

## 3. Prueba: humedad del suelo, duración del viento y factor de racha (14.10.2026)

**Hipótesis.** Si los árboles caen más con el suelo empapado y con el viento que dura horas (Lorenz et al.), los días con esas condiciones deberían tener más incidencias que lo que dice el viento solo, y una variable de suelo o de duración debería mejorar la probabilidad de la víspera.

**Datos.** Serie horaria de ERA5-Land a través del archivo de Open-Meteo (sin clave) para cinco puntos de las dos redes (Plasencia, Hervás y Aldea del Cano en la Distribuidora 1; Casar de Cáceres y Alcántara en la Distribuidora 2), del 01.10.2021 al 05.08.2026: racha y viento a 10 m, lluvia, humedad del suelo a 0-7 y 7-28 cm y temperatura. Por día y red: pico de racha, horas con racha por encima del percentil 90 del punto, factor de racha en la hora del pico, lluvia, humedad media del suelo y su anomalía frente a la media de esa fecha en todos los años, lluvia acumulada de 7 y 30 días. El código está en `experimentos/suelo_humedad/` (`bajar_suelo.py` y `analizar.py`) y se reproduce con el `dias.csv` del replay.

**Resultado** (replay `caso_v9_20261014_1351`, modo reanálisis, 3.348 días, 143 con incidencia):

| variable añadida a la probabilidad de la víspera | coeficiente (por desviación típica) | p-valor | AUC rodante | Brier rodante |
| --- | --- | --- | --- | --- |
| ninguna (la probabilidad sola) | - | - | 0,685 | 0,0388 |
| humedad del suelo 0-7 cm | -0,045 | 0,63 | 0,684 | 0,0388 |
| humedad del suelo 7-28 cm | -0,062 | 0,48 | 0,683 | 0,0388 |
| anomalía de humedad del suelo | +0,037 | 0,68 | 0,685 | 0,0388 |
| horas con racha sobre el percentil 90 | +0,145 | 0,09 | 0,669 | 0,0390 |
| factor de racha en el pico | +0,032 | 0,65 | 0,683 | 0,0388 |
| lluvia de 7 días | -0,033 | 0,69 | 0,683 | 0,0388 |
| lluvia de 30 días | -0,033 | 0,71 | 0,683 | 0,0388 |
| horas sobre el percentil 90 x anomalía de suelo | +0,067 | 0,30 | 0,659 | 0,0389 |

Las tablas de contraste van en la misma dirección que el artículo, pero sin fuerza: en el 5 % de días con más viento, el 18,0 % de los que tenían el suelo más húmedo de lo normal tuvieron incidencia (23 de 128) frente al 12,2 % de los que lo tenían más seco (5 de 41); test exacto de Fisher p = 0,48. Las horas de viento fuerte sí ordenan los días (2,9 % de días con incidencia sin ninguna hora, 3,9 % con 1-3, 5,2 % con 4-8 y 8,8 % con más de 8), pero esa información ya la lleva el índice de Klawa y Ulbrich: añadida al modelo, la variable empeora el AUC rodante (0,669 frente a 0,685).

**Decisión.** Descartamos la humedad del suelo, la lluvia acumulada y el factor de racha como variables del modelo del día, y no añadimos las horas de viento fuerte por separado. Lo que Lorenz et al. encuentran con decenas de miles de árboles caídos no se ve con 143 días de incidencias: no es que el suelo no importe, es que con nuestra muestra no se puede medir y una variable que no se puede medir solo añade ruido. Se vuelve a probar cuando haya más años etiquetados o si entra una tercera red grande.

## 3b. Prueba: un umbral de alerta por estación (14.10.2026)

**Hipótesis.** El umbral de alerta es el mismo todo el año (el percentil 90 de la probabilidad de los días de entrenamiento), y como la probabilidad media es menor en verano, en verano hay menos alertas (el 7,7 % de los días de abril a septiembre frente al 17,9 % del resto, con la previsión real de 2024-2026) aunque las tormentas y los incendios traen averías (junio es el mes con más días con incidencia). Repartir las alertas por estación o por mes, con la misma parte de días en alerta en total, podría cubrir más.

**Resultado** (`experimentos/umbral_estacion/analizar.py` sobre el `dias.csv` del replay `caso_v9_20261014_1351`):

| modo | regla | parte de días en alerta | horas-cliente cubiertas | incidencias cubiertas | días con incidencia cubiertos |
| --- | --- | --- | --- | --- | --- |
| previsión real | alerta actual (umbral fijo) | 12,5 % | 72,3 % | 53,1 % | 41,1 % |
| previsión real | mismos días, por año y estación | 12,6 % | 73,5 % | 53,1 % | 41,1 % |
| previsión real | mismos días, por año y mes | 13,1 % | 72,9 % | 53,1 % | 41,1 % |
| reanálisis | alerta actual (umbral fijo) | 13,5 % | 59,0 % | 42,5 % | 32,2 % |
| reanálisis | mismos días, por año y estación | 13,7 % | 60,3 % | 44,2 % | 32,9 % |
| reanálisis | mismos días, por año y mes | 13,1 % | 59,7 % | 45,3 % | 35,0 % |

**Decisión.** No cambiamos el umbral: el reparto por estación gana un punto de horas-cliente y ninguna incidencia, dentro del ruido de 73 días con incidencia. El modelo ya está calibrado por estación (en verano da de media un 3,7 % y pasa el 3,4 %; en invierno, 4,6 % y 4,6 %), así que las pocas alertas de verano no son un fallo del umbral sino el riesgo real: las averías de verano (tormentas, incendios, equipos con calor) llegan con menos aviso meteorológico que las de invierno. Lo que sí deja esta prueba es el argumento para la guardia de verano: ahí mandan los rayos en vivo y la franja de la tarde, no la alerta de la víspera.

## 3c. Prueba: la regla de los avisos de viento o tormenta frente al sistema (14.10.2026)

**Hipótesis.** La regla que un jefe de brigadas aplicaría de cabeza, "reforzar la guardia cuando AEMET tiene aviso de viento o de tormenta para mañana", debería cubrir menos que el sistema con el mismo número de días, porque el sistema usa además la previsión numérica, el clima de cada celda y la historia.

**Resultado** (replay `caso_v10_20261014_1543`, previsión real 2024 - 2026, la Distribuidora 1 y la Distribuidora 2; `policy_table` y `detection_table` de `job_ml_caso_negocio.py`, con los cortes de 2024 - 2025 y sin el 28.01.2026 calculados sobre `dias.csv`):

| política | días al año | horas-cliente cubiertas 2024 - 2026 | 2024 - 2025 | sin el 28.01.2026 |
| --- | --- | --- | --- | --- |
| aviso de AEMET de viento o tormenta, amarillo o más | 23 | 69 % | 49 % | 40 % |
| GridPredic con esos mismos 23 días (los de más probabilidad de cada año) | 23 | 66 % | 42 % | 34 % |
| GridPredic en alerta (alto o muy alto) | 46 | 72 % | 57 % | 46 % |
| GridPredic en alerta o aviso de viento o tormenta | 50 | 74 % | 61 % | 50 % |
| GridPredic en alerta o aviso naranja o rojo | 63 | 74 % | 58 % | 49 % |

La hipótesis era falsa a igual número de días: la regla sencilla cubre de 3 a 7 puntos más que el sistema con los mismos 23 días. El motivo está en los avisos de tormenta: los días con aviso de tormenta y sin aviso de viento (12 al año) tienen incidencia el 14 % de las veces (los días en alerta, el 13 %; un día cualquiera, el 4 %), y el sistema los pone en alto o muy alto en 48 de 63 casos de 2022 - 2026, pero con probabilidades del 10 % que no llegan a los 23 primeros puestos del año, ocupados por los temporales de invierno. Juntar las dos cosas es mejor que cada una: 50 días para el 74 % (61 % y 50 % en los otros cortes), y es la unión que gasta menos días por hora-cliente cubierta de todas las que pasan del 70 %. Lo que añade el aviso lo añade en verano: de abril a septiembre la alerta sola cubre el 42 % de las horas-cliente con el 7,7 % de los días y la unión el 48 % con el 9,3 %; de octubre a marzo las dos cubren el 84 % (con el 17,9 % y el 18,9 % de los días).

**Decisión.** El riesgo de mañana lleva desde hoy la recomendación de guardia con esa unión (`guardia` en `riesgo.json`: reforzar con nivel alto o muy alto o con aviso de viento o tormenta de AEMET para el día), el mensaje de la tarde la cuenta y el parte de guardia la escribe. No tocamos el modelo para "subir" los días de tormenta: con 7 días de dos incidencias o más en dos años y medio, cualquier ajuste a mano sería sobreajuste; la regla queda como política explícita y medible, no como un parche en la probabilidad. En la memoria se dice tal cual.

## 3d. Con cuánta antelación llegan los avisos de AEMET (14.10.2026)

**Pregunta.** El sistema corre a las 20:30 de la víspera con los avisos que ya están emitidos. ¿Qué parte de los avisos de un día ya existe a esa hora, y cuáles llegan después?

**Datos.** El archivo de avisos de AEMET en Silver (`l2_silver.f_aviso_meteo`, del 20.06.2018 al 31.08.2026) cruzado con las celdas de cada red: 2.158 (distribuidora, día, fenómeno) con algún aviso amarillo o más. Para cada uno, las horas entre el primer mensaje y las 00:00 del día (`experimentos/avisos_antelacion/analizar.py`; calculado sobre la exportación `avisos_c_20261012_1845` y repetido sobre el lakehouse el 14.10.2026 con las mismas cifras).

| fenómeno | días con aviso | primer mensaje antes de las 20:00 de la víspera | antes de las 12:00 de la víspera | dos días antes o más | mediana de antelación |
| --- | --- | --- | --- | --- | --- |
| todos | 2.158 | 79 % | 76 % | 57 % | 25 h |
| viento (VI) | 243 | 78 % | 74 % | 55 % | 25 h |
| tormentas (TO) | 346 | 57 % | 52 % | 21 % | 13 h |
| lluvia (PR) | 503 | 71 % | 66 % | 43 % | 13 h |
| nieve (NE) | 119 | 86 % | 84 % | 60 % | 36 h |
| temperaturas máximas (AT) | 755 | 93 % | 91 % | 86 % | 48 h |

El 21 % de los avisos de un día no existían a las 20:00 de la víspera (en las tormentas, el 43 %), y en un 3 % el nivel subió después. El aviso de viento se conoce con un día de margen en tres de cada cuatro casos; el de tormenta, que es el que más añade al sistema (apartado 3c), muchas veces llega la misma mañana.

**Decisión.** La ejecución de la tarde no basta para los avisos de tormenta: la operación vuelve a leer los avisos de AEMET durante el día (tarea `avisos_hoy` de `dag_observacion_vivo`, cada hora) y, cuando aparece un aviso de viento o tormenta para hoy que no estaba anoche, sube la guardia a reforzada, lo escribe en el cuadro de mando y lo manda al webhook una vez por distribuidora y día.

## 3e. El riesgo por zona con dos y tres días de antelación (en marcha, 14.10.2026)

**Pregunta.** Al principio del proyecto propusimos pasar del CT concreto al riesgo por zona (línea o municipio) a 24 - 72 horas, más un ranking de CT, y dejamos dicho que había que comprobarlo con datos antes de comprometerlo. Lo de 24 horas está hecho y medido (el día y la franja funcionan; la línea, poco; el CT, nada: apartado 8 de [causas-incidencias.md](causas-incidencias.md) y apartado 2 de [caso-de-negocio.md](caso-de-negocio.md)). Queda lo de 48 y 72 horas: ¿cuánto se pierde cuando la guardia se decide dos o tres tardes antes?

**Qué ya sabemos antes de medir.** Dos cosas ponen el listón: los avisos de AEMET existen dos días antes solo en el 55 % de los días de viento y el 21 % de los de tormenta (apartado 3d), y la previsión de racha de ICON-EU pierde acierto con cada día de antelación. Y la parte del sistema que sí funciona dentro del día (la franja, el reparto entre líneas por tamaño e historia) no depende del horizonte: el reparto entre líneas lo manda el tamaño de la línea, igual a 24 que a 72 horas.

**Diseño.** Gold guarda desde enero de 2024 lo que ICON-EU previó para cada hora un día antes (`metp1_*`, seis variables), dos días antes (`metp2_*`, racha y lluvia) y, desde esta noche, tres días antes (`metp3_*`, racha y lluvia: `wind_gusts_10m_previous_day3` de la API Previous Runs). `job_ml_riesgo_dia.py` tiene ahora variantes con horizonte: se entrenan con el reanálisis como siempre y puntúan 2024 - 2025 con la previsión de ese horizonte, los avisos de AEMET que ya estaban emitidos a las 20:00 de D - 2 (o D - 3) y el tiempo del último día conocido esa tarde. Tres variantes con las mismas variables (calendario, historia, avisos, racha y lluvia del día con sus extremos sobre el clima de cada celda, la racha y la lluvia del último día conocido): `operativa_racha` a 24 h (la referencia justa, sin viento ni temperatura, que a 48 h no hay), `operativa_48h` y `operativa_72h`. La historia de incidencias es la conocida al empezar el día (uno o dos días después de aquella tarde; con la semana de retraso con la que Calser conoce las incidencias, la diferencia es pequeña).

**Resultados a 48 h** (`dia_48h_20261014_1854`, previsión real de 2024 - 2025, 58 días con avería de distribuidora, regresión logística; cada año puntuado por un modelo que solo vio los años anteriores):

| lo que se sabe | PR-AUC | veces la tasa base | ROC-AUC | días con avería en el 10 % de días de más riesgo | en el 20 % |
| --- | --- | --- | --- | --- | --- |
| la tarde anterior, todo (`operativa`, la de producción) | 0,188 | 6,9 | 0,82 | 53 % | 67 % |
| la tarde anterior, solo racha y lluvia (`operativa_racha`) | 0,177 | 6,5 | 0,81 | 53 % | 67 % |
| dos tardes antes (`operativa_48h`) | 0,147 | 5,4 | 0,80 | 43 % | 66 % |
| la racha prevista sola, dos tardes antes | 0,104 | 3,8 | 0,70 | 33 % | 52 % |
| solo el tiempo del día anterior (`ayer`) | 0,071 | 2,6 | 0,73 | 24 % | 45 % |

Con dos tardes de antelación el sistema conserva la mayor parte de la señal: 5,4 veces la tasa base frente a 6,5 con un día y 2,6 sabiendo solo el tiempo de ayer. Con el 20 % de días de más riesgo se recogen los mismos días con avería (66 % frente a 67 %); con el 10 %, se pierden diez puntos (43 % frente a 53 %). Los días de dos o más incidencias (16 en 2024 - 2025) caen en el 20 % de más riesgo en el 94 % de los casos a los dos horizontes, y en el 10 % en el 62 % (75 % a un día). La diferencia emparejada de PR-AUC entre 48 y 24 h con las mismas variables es de -0,026 (bootstrap por fechas, intervalo del 95 % de -0,069 a +0,026; el 48 h gana en el 13 % de las réplicas): con 58 días positivos los datos no la separan del cero, pero el signo es el esperado. De esa pérdida, la mitad viene de la previsión de ICON-EU (con el reanálisis como previsión perfecta, la variante a 48 h da 0,163 y con la previsión real, 0,147) y la otra mitad de lo que aún no se sabe dos tardes antes (los avisos de AEMET que faltan y el último día conocido: 0,178 frente a 0,163 con la misma previsión perfecta). La racha prevista sola pierde poco de 24 a 48 h (0,106 frente a 0,104): lo que más se degrada no es el viento previsto, sino los avisos.

**Lectura.** El riesgo por zona a dos días es viable para planificar (reforzar con un presupuesto amplio de días, avisar a las contratas, mover vehículos entre bases), con la pérdida medida: la mitad del acierto de más que da el sistema sobre el tiempo de ayer se mantiene a 48 h cuando se mira el 10 % de días peores, y casi todo cuando se mira el 20 %. El reparto entre líneas y municipios dentro del día no cambia con el horizonte (lo manda el tamaño y la historia de cada línea). Lo de 72 h se mide esta noche con `metp3_*`.

## 4. Datos públicos de España que revisamos

| Fuente | Qué es | Qué hicimos | Decisión |
| --- | --- | --- | --- |
| GECOS (MITECO), índices zonales de calidad de suministro | TIEPI y NIEPI oficiales por tipo de zona (urbana, semiurbana, rural concentrada, rural dispersa) y por causa (programadas; imprevistas por generación, transporte, terceros, fuerza mayor y propias) para España, cada comunidad y cada provincia, de 2003 a 2021 (el Ministerio publica con varios años de retraso) | `extract_gecos_indices.py` los pide a las páginas públicas y deja un CSV (13.400 filas) que entra en el lakehouse como `l1_bronze.gecos_indice_zonal`; `job_analisis_red.py` añade la sección "Frente al sector" y deja `sector.json` para `negocio.html` | **Entra, como referencia, no como variable.** No sirve para predecir (es anual y de todo el sector), pero es la referencia con la que la CNMC compara a cada distribuidora para el incentivo y el contexto que un comprador mira primero. Con la mezcla de zonas de la Distribuidora 1, su TIEPI imprevisto de 2023-2025 (0,34 h) es un tercio del de Cáceres de 2019-2021 (0,92 h) y menos de un cuarto del de España (1,51 h); el de la Distribuidora 2 (0,12 h) es un quinto del de Cáceres. En 2019-2021, los mismos años, la Distribuidora 1 estaba en la media de Cáceres (1,12 h frente a 1,06 h en zona rural concentrada) por un solo año, 2019, en el que el 92 % de su TIEPI fue fuerza mayor (ese año el 43 % del TIEPI imprevisto de la zona rural concentrada de Cáceres también lo fue). La serie nacional confirma lo que vemos en nuestras redes: el TIEPI por fuerza mayor se multiplica por 20 en los años de grandes temporales (0,88 h en 2009 con Klaus, 1,06 h en 2010 con Xynthia, frente a 0,04 h en 2008) |
| datos.gob.es, "Calidad eléctrica zonal" | El mismo conjunto, catalogado en el portal de datos abiertos | Comprobado que apunta a GECOS | Cubierto con lo anterior |
| CNMC, retribución de la distribución y Circular 8/2025 | Retribución de cada distribuidora y la fórmula del incentivo de calidad | Ya estaba en el caso de negocio (artículos 15, 24 y 25) | Sin cambios |
| AEMET OpenData, avisos y predicción por municipios | Avisos desde 2018 y la predicción horaria del propio modelo de AEMET | Ya en el sistema: los avisos como variable y como política de comparación, la predicción municipal como segunda opinión cada tarde | Sin cambios |

## 5. Datos de los dispositivos de los clientes

Preguntamos qué datos de los equipos instalados en los clientes podrían cruzarse con las incidencias. Lo que hay y lo que no:

- **Contadores inteligentes (telegestión).** Las distribuidoras tienen la telegestión desplegada (la digitalización de la red y la telegestión aparecen en Calser como 30 cortes programados desde 2016), y un contador registra los cortes que sufre, lo que daría el mapa de los cortes de baja tensión que no llegan a Calser. Esos datos no están en ninguna de las copias que tenemos (Calser y TedisNet) y pedirlos exige un acuerdo con cada empresa y anonimizarlos cliente a cliente. No se puede probar en este trabajo; queda como la primera fuente a pedir si el sistema se implanta.
- **El SCADA ya ve lo que importa para la media tensión.** TedisNet registra las señales de defecto, los disparos y los cortes deducidos por topología de cada CT y cabecera. Las probamos a fondo en el modelo por horas (246 variables, ver [ml-training.md](ml-training.md)) y la conclusión fue que la meteorología aporta más que las señales eléctricas para anticipar una avería con una a tres horas de margen; sirven para saber que el corte ya ha ocurrido, no para verlo venir.

## 6. Lo que queda en la lista

- Volver a probar el suelo y la duración del viento cuando haya dos temporadas más etiquetadas (2027), con el mismo banco de pruebas.
- Pedir los cortes de los contadores de telegestión a una de las distribuidoras para medir cuántos cortes de baja tensión no vemos.
- Hecho el 14.10.2026: el replay da ya la detección y las falsas alarmas por día de cada política (apartado 2b de [caso-de-negocio.md](caso-de-negocio.md)): el sistema en alerta detecta el 71 % de los días con dos o más incidencias marcando el 12,5 % de los días, frente al 73 % con el 3 % de falsas alarmas del trabajo de Alcalá, que cuenta averías de una provincia entera. Queda pendiente definir el día extremo como ellos (por número de averías) cuando entre una red mayor.
- Con los avisos de AEMET, una política mixta por fenómeno (viento y tormenta sí, calor no) frente a la unión que ya medimos.
- El riesgo por zona a 48 y 72 horas (apartado 3e): medido a 48 h (se conserva la mayor parte de la señal); 72 h en marcha. Con eso, decidir si la ejecución de la tarde da también el nivel de pasado mañana (previsión de D+2 y avisos de D+2, que la misma petición ya trae) como orientación para planificar, sin parte de guardia.
