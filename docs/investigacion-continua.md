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

## 3e. El riesgo por zona con dos y tres días de antelación (14.10 y 15.10.2026)

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

Con dos tardes de antelación el sistema conserva la mayor parte de la señal: 5,4 veces la tasa base frente a 6,5 con un día y 2,6 sabiendo solo el tiempo de ayer. Con el 20 % de días de más riesgo se recogen los mismos días con avería (66 % frente a 67 %); con el 10 %, se pierden diez puntos (43 % frente a 53 %). Los días de dos o más incidencias (16 en 2022 - 2025, sobre el reanálisis) caen en el 20 % de más riesgo en el 94 % de los casos a los dos horizontes, y en el 10 % en el 62 % (75 % a un día). La diferencia emparejada de PR-AUC entre 48 y 24 h con las mismas variables es de -0,026 (bootstrap por fechas, intervalo del 95 % de -0,069 a +0,026; el 48 h gana en el 13 % de las réplicas): con 58 días positivos los datos no la separan del cero, pero el signo es el esperado. De esa pérdida, la mitad viene de la previsión de ICON-EU (con el reanálisis como previsión perfecta, la variante a 48 h da 0,163 y con la previsión real, 0,147) y la otra mitad de lo que aún no se sabe dos tardes antes (los avisos de AEMET que faltan y el último día conocido: 0,178 frente a 0,163 con la misma previsión perfecta). La racha prevista sola pierde poco de 24 a 48 h (0,106 frente a 0,104): lo que más se degrada no es el viento previsto, sino los avisos.

**Resultados a 72 h** (`dia_72h_20261014_1854`, la madrugada del 15.10, con `metp3_*` ya en Gold; los mismos 58 días con avería de 2024 - 2025, regresión logística; las cifras de 24 y 48 h son las de esta misma corrida, iguales a las de arriba):

| lo que se sabe | PR-AUC | veces la tasa base | ROC-AUC | días con avería en el 10 % de días de más riesgo | en el 20 % |
| --- | --- | --- | --- | --- | --- |
| la tarde anterior, todo (`operativa`) | 0,188 | 6,9 | 0,82 | 53 % | 67 % |
| la tarde anterior, solo racha y lluvia (`operativa_racha`) | 0,177 | 6,5 | 0,81 | 53 % | 67 % |
| dos tardes antes (`operativa_48h`) | 0,147 | 5,4 | 0,80 | 43 % | 66 % |
| tres tardes antes (`operativa_72h`) | 0,143 | 5,2 | 0,79 | 40 % | 64 % |
| solo el tiempo del día anterior (`ayer`) | 0,071 | 2,6 | 0,73 | 24 % | 45 % |

De 48 a 72 horas casi no se pierde más: 5,2 veces la tasa base frente a 5,4, tres puntos menos de días con avería en el 10 % de más riesgo (40 % frente a 43 %) y dos en el 20 % (64 % frente a 66 %); la diferencia emparejada de PR-AUC entre 72 y 48 h es -0,005 (intervalo del 95 % de -0,043 a +0,034; el 72 h gana en el 40 % de las réplicas), es decir, nada que los datos distingan. La pérdida grande está entre 24 y 48 horas (de 53 % a 43 % en el 10 %), y es la de los avisos de AEMET que aún no existen. La racha y la lluvia que ICON-EU prevé a tres días valen casi lo mismo que a dos: con el reanálisis como previsión perfecta, la variante a 72 h da 0,161 y la de 48 h 0,163; con la previsión real, 0,143 y 0,147. Por redes, a 72 h la Distribuidora 1 baja a 0,17 de PR-AUC (0,24 a un día) y la Distribuidora 2 se queda en 0,17 (0,12 a un día: con pocos días positivos, la cifra de una red sola baila). Frente a saber solo el tiempo de ayer, el sistema a 72 h sigue ganando con claridad (+0,071 de PR-AUC, intervalo de -0,008 a +0,177, mejor en el 96 % de las réplicas). Los días de dos o más incidencias en 2024 - 2025 son solo cinco: a 72 h caen cuatro en el 10 % de más riesgo y los cinco en el 20 % (a 24 h, cuatro y cuatro), demasiado pocos para decir más.

**Lectura.** El riesgo por zona a dos y tres días es viable para planificar (reforzar con un presupuesto amplio de días, avisar a las contratas, mover vehículos entre bases), con la pérdida medida: la mitad del acierto de más que da el sistema sobre el tiempo de ayer se mantiene a 48 y 72 h cuando se mira el 10 % de días peores, y casi todo cuando se mira el 20 %; entre dos y tres días no hay diferencia que se pueda medir con dos años. El reparto entre líneas y municipios dentro del día no cambia con el horizonte (lo manda el tamaño y la historia de cada línea). Para la operación, eso dice dos cosas: la orientación de pasado mañana que la tarde ya calcula (`two_day_outlook`, apagada en producción) tiene un acierto conocido y suficiente para planificar con el 20 % de días, y se puede dar también la de dentro de tres días sin perder casi nada más, pero ninguna de las dos sirve para la decisión fina de la guardia del 10 %, que sigue siendo la de la víspera con los avisos.

## 3f. Prueba: la extensión del viento fuerte, las interacciones con la racha, la pendiente por red y la dirección (14.10.2026)

**De dónde sale.** De la revisión de literatura del 14.10 ([revision-literatura.md](revision-literatura.md)): Láng-Ritter y otros (NHESS 2025) encuentran en 92 temporales de Finlandia que el área con rachas de más de 21 m/s se correlaciona mejor con los puntos sin suministro (0,61) que la racha máxima (0,46); Manning, Wilkinson, Fowler y Kendon (Communications Earth and Environment 2025) encuentran en la red británica que la lluvia antecedente, la dirección anómala del viento y el verano multiplican por 2, por 5 y por 2 la probabilidad de muchas faltas cuando la racha pasa de 25 m/s, pero no sirven solas; Lorenz y otros (NHESS 2025) ven lo mismo con la duración: pesa como interacción con la racha. Eso explica nuestros resultados negativos del apartado 3 (la lluvia acumulada y la duración, probadas como efectos propios, no aportaron) y dice cómo volver a probarlos. Watson y otros (Journal of Engineering 2020) apoyan juntar varias redes en un modelo con un término por red.

**Diseño.** Cuatro bloques nuevos en `job_ml_riesgo_dia.py`, todos calculados también desde la previsión (`prev_ext_*`) para medirlos con la previsión real de 2024 - 2025, salvo la dirección, que ICON-EU no nos da en el archivo de previsiones y queda como cota superior sobre el reanálisis:

- `extension`: la parte de los CT de la red cuya celda tiene una racha en su p98 o más, y en 72 km/h o más (20 m/s), en la peor hora del día, y la suma de esa parte sobre las horas del día (CT-horas de viento fuerte).
- `interacciones`: el exceso de la racha sobre 0,9 veces el p98 de su celda (max(0, cociente - 0,9)) por la lluvia del día anterior (la conocida esa tarde), por la lluvia del día, por la estación de hoja (abril - octubre) y por el verano (junio - septiembre).
- `pendiente_red`: el cociente de la racha por cada distribuidora, una pendiente propia por red (el interceptor ya era propio con `dist_*`).
- `direccion` (con `--direccion`): el ángulo entre el viento de las horas con racha en el p98 o más y la dirección de la que suelen venir las rachas fuertes de la celda (media circular del reanálisis), en grados, promediado en el día, y ese ángulo por el exceso de la racha.

Variantes `operativa_extension`, `operativa_interacciones`, `operativa_pendiente_red`, `operativa_direccion` y `operativa_mas` (las tres primeras juntas) frente a `operativa`, con el mismo banco del apartado 1: PR-AUC, veces la tasa base, parte de los días con avería en el 10 y el 20 % de días de más riesgo, y la diferencia emparejada con bootstrap por fechas, sobre el reanálisis y sobre la previsión real. Criterio: entra en producción lo que suba la captura al 10 - 20 % de forma consistente en 2024 y 2025 con la previsión real, no solo sobre el reanálisis.

**Resultados** (`dia_mas_20261014_2301`, 7 minutos; regresión logística, cada año puntuado por un modelo que solo vio los anteriores). Con la previsión real de ICON-EU de 2024 - 2025 (58 días con avería de distribuidora):

| variante | PR-AUC | ROC-AUC | días con avería en el 10 % de más riesgo | en el 20 % | diferencia emparejada de PR-AUC frente a `operativa` [IC 95 %] | prob. de ser mejor |
| --- | --- | --- | --- | --- | --- | --- |
| `operativa` (producción) | 0,188 | 0,82 | 53 % | 67 % | | |
| `operativa_interacciones` | 0,187 | 0,82 | 53 % | 67 % | -0,002 [-0,016, +0,017] | 39 % |
| `operativa_direccion` (reanálisis, cota superior) | 0,186 | 0,82 | 53 % | 67 % | -0,001 [-0,006, +0,001] | 17 % |
| `operativa_pendiente_red` | 0,179 | 0,82 | 53 % | 67 % | -0,005 [-0,026, +0,002] | 16 % |
| `operativa_extension` | 0,178 | 0,82 | 52 % | 67 % | -0,007 [-0,031, +0,012] | 24 % |
| `operativa_mas` (las tres juntas) | 0,177 | 0,82 | 53 % | 67 % | -0,008 [-0,038, +0,011] | 21 % |

Sobre el reanálisis de 2022 - 2025 (cuatro años, la comparación con más días) pasa lo mismo: `operativa` 0,151 de PR-AUC; la extensión 0,149 (-0,002 [-0,015, +0,011], 33 %), las interacciones 0,150 (-0,001, 40 %), la pendiente por red 0,148 (-0,004 [-0,010, +0,000], 5 %), la dirección 0,150 (-0,001, 20 %) y las tres juntas 0,148 (-0,004, 22 %). Año a año ninguna variante gana en los cuatro: la extensión gana en 2023 y 2024 (0,163 y 0,241 frente a 0,152 y 0,235) y pierde en 2022 y 2025 (0,122 y 0,187 frente a 0,139 y 0,189); las interacciones empatan en 2024 y pierden en 2025. XGBoost con todo (`operativa_mas`) queda por debajo con la previsión real (-0,035 [-0,127, +0,032]); el rango de los dos modelos, también (-0,031 [-0,069, +0,000]).

**Lectura.** Con nuestros datos, ninguno de los cuatro bloques añade nada medible al modelo del día que ya usa la racha frente al p98 de cada celda, el índice de Klawa y Ulbrich y la lluvia: las diferencias son de milésimas, dentro del ruido del bootstrap, y las variantes con más variables pierden un poco con la previsión real, que es donde se decide. La extensión del viento fuerte no aporta porque nuestras redes caben en unas pocas celdas de ICON-EU (10 km) y en un temporal casi todas las celdas pasan el p98 a la vez: la fracción se mueve con la racha máxima. Las interacciones de Manning tampoco: en la red británica salen con 12.500 km2 y miles de faltas por temporal; aquí hay 58 días positivos en dos años y una logística con regularización no puede separar el producto racha x lluvia del efecto de cada una. La dirección, incluso con el reanálisis (que no tendríamos en producción), no mueve nada, así que no vale la pena pedir `wind_direction_10m_previous_day1` al archivo de previsiones. La pendiente por red empeora: tres redes, dos de ellas parecidas, no dan para pendientes propias. Decisión: no entra nada en producción; los bloques quedan en el job para volver a medirlos con más años (y, si alguna red grande entra, con más celdas), y la lista de la revisión sigue con el modelo de conteos, el arbolado y las aves.

## 3g. Prueba: el modelo de conteos y la logística equilibrada (14.10.2026)

**De dónde sale.** La tercera recomendación de la revisión ([revision-literatura.md](revision-literatura.md)): Liu y otros (2005), Guikema y Quiring (2012), Kabir y otros (2019) y Cerrai y otros (2019) modelan el número de cortes (Poisson, binomial negativa, hurdle) y no un sí o no, porque un día con seis incidencias enseña más que uno con una; Sharma y otros (2023) recuerdan que en las distribuidoras rurales pequeñas el desbalance es el problema central y usan una logística ponderada. Las dos cosas se pueden probar sin tocar los datos.

**Diseño.** En `ml_models.py`, dos modelos nuevos en la misma tubería (imputación, logaritmo, estandarización): `poisson`, una regresión de Poisson regularizada que aprende de `n_dia` (el número de incidencias que empiezan ese día) y cuya puntuación es el número esperado de incidencias, y `logistica_equilibrada`, la misma logística con `class_weight` equilibrado. Se comparan con la logística de producción en la variante `operativa`, con las mismas métricas de ordenación (PR-AUC sobre el día con avería, captura al 10 y al 20 %), sobre el reanálisis de 2022 - 2025 y con la previsión real de 2024 - 2025, con la diferencia emparejada y su bootstrap. Lo que el conteo puede aportar, además de la ordenación, es la lectura "incidencias esperadas" ya calibrada por día; eso se mira comparando la suma de lo esperado con lo observado por año.

**Resultados** (`dia_conteos_20261014_2343`, variante `operativa`, cada año puntuado por un modelo que solo vio los anteriores). Con la previsión real de ICON-EU de 2024 - 2025:

| modelo | PR-AUC | ROC-AUC | días con avería en el 10 % de más riesgo | en el 20 % | diferencia emparejada frente a la logística [IC 95 %] | prob. de ser mejor |
| --- | --- | --- | --- | --- | --- | --- |
| logística (producción) | 0,188 | 0,82 | 53 % | 67 % | | |
| logística equilibrada | 0,158 | 0,81 | 53 % | 66 % | -0,028 [-0,052, -0,005] | 0,2 % |
| Poisson sobre `n_dia` | 0,127 | 0,78 | 50 % | 69 % | -0,056 [-0,103, -0,014] | 0,2 % |

Sobre el reanálisis de 2022 - 2025: logística 0,151, equilibrada 0,135 (-0,014 [-0,037, +0,008], 15 %) y Poisson 0,083 (-0,064 [-0,132, -0,017], 0,2 %); el Poisson gana solo en 2022 (0,160 frente a 0,139) y pierde en los otros tres años.

**Lectura.** Ninguno de los dos sustituye a la logística. El modelo de conteos ordena peor la cabeza de la lista (su PR-AUC cae un tercio) aunque recoja los mismos días con avería en el 20 % de más riesgo: aprende de los días con muchas incidencias (el temporal de enero de 2026 o las tormentas de verano concentran decenas) y eso tira de la escala hacia los días extremos, que son pocos y distintos entre años, mientras que el día "con alguna avería" es lo que la guardia necesita anticipar. La logística equilibrada sube las probabilidades pero no cambia la ordenación y pierde algo de PR-AUC por la regularización con pesos. Lo que sí vale del conteo es la lectura "incidencias esperadas", que ya damos en el cuadro de mando por otra vía (las incidencias esperadas de cada línea salen del riesgo repartido). Decisión: la logística se queda; los dos modelos quedan en `ml_models.py` y en las comparaciones para volver a mirarlos con más años, y el modelo de conteos con efecto de red y las bandas conformales pasan de la lista de pruebas a la de "medido, no aporta hoy".

## 3h. Prueba: el arbolado de cada celda desde mapas públicos (15.10.2026)

**De dónde sale.** La cuarta recomendación de la revisión: Wanik y otros (2017) ponen la altura del arbolado junto a la línea entre las tres variables que más pesan en el modelo de Connecticut, Tervo y otros (2021) encuentran en las distribuidoras rurales finlandesas que la altura y el diámetro del rodal suman a la racha, y Kabir y otros (2018) modelan la caída de árboles con sus atributos. Nosotros solo teníamos los polígonos de bosque y matorral de OpenStreetMap por celda (`celda_pct_bosque`), que en la dehesa casi no existen. Dos mapas públicos sin registro lo arreglan: ESA WorldCover 2021 (cubierta del suelo a 10 m, CC BY 4.0) y la altura del dosel de Meta y WRI (Tolan y otros, 2024; 1 m, CC BY 4.0, en un bucket abierto de AWS). Las capas de Copernicus (HRL) y el LiDAR del PNOA, que necesitan cuenta, quedan para después.

**Diseño.** `experimentos/arbolado/bajar_arbolado.py` baja los tres mosaicos de WorldCover y los siete de altura del dosel que tocan las 43 celdas de 0,1 grados de las tres redes (unos 5 GB) y deja por celda la parte que es arbolado, matorral, pastizal, cultivo y construido, la parte con dosel (1 m o más), la altura media y el percentil 90 de los píxeles con árbol y la parte de la celda con dosel de 5, 8 y 12 m o más (los árboles que llegan a un conductor de media tensión), en `etl/config/01_bronze/arbolado_celdas.csv`. `job_ml_riesgo_ct.py` lo lee como bloque `arbolado` (`celda_pct_arbol`, `celda_dosel_media_m`, `celda_pct_dosel_8m`...), estático, y la variante `historia+estructura+arbolado` se compara con `historia+estructura` y con los demás bloques en la captura de kVA·h del año siguiente por cabecera y por CT (años 2021 - 2025, origen móvil). El grano es la celda meteorológica (unos 10 km), porque los CT solo tienen la coordenada de su municipio; es la misma limitación que el relieve y OpenStreetMap.

**Los datos.** Las 43 celdas separan bien el norte de Cáceres del resto: en el Jerte y el Ambroz (celdas 40.10 - 40.30 N) el arbolado cubre el 50 - 67 % de la celda y el 10 - 26 % de la celda tiene dosel de 8 m o más (altura media de los árboles de 6 a 8 m, percentil 90 de 12 a 15 m); en la penillanura de Cáceres y en el oeste de la Distribuidora 2 el arbolado es el 1 - 20 % y casi nada pasa de 8 m (dehesa de 3 m de media); la celda de la Distribuidora 3 tiene un 77 % de arbolado y un 23 % con dosel de 8 m o más. Es justo lo que OpenStreetMap no distinguía (`celda_pct_bosque` deja la dehesa fuera).

**Resultados** (`rct_arbolado_20261015_0032`, XGBoost Tweedie, captura de los kVA·h imprevistos del año siguiente, media de 2021 - 2025 con origen móvil):

| bloque | cabecera, top 10 % | cabecera, top 20 % | CT, top 10 % | CT, top 20 % |
| --- | --- | --- | --- | --- |
| historia | 65,1 % | 80,5 % | 25,4 % | 47,8 % |
| historia + estructura | 68,4 % | 82,0 % | 27,5 % | 49,3 % |
| historia + estructura + arbolado | 68,8 % | 81,8 % | 30,3 % | 50,7 % |
| historia + estructura + clima | 70,9 % | 82,9 % | 26,0 % | 50,6 % |
| historia + estructura + públicos (relieve, OSM, INE) | 71,0 % | 82,2 % | 27,2 % | 49,4 % |
| historia + estructura + naturaleza (cigüeñas, incendios) | 70,7 % | 82,4 % | 26,2 % | 48,1 % |
| lista naif (los dos años anteriores) | 63,5 % | 74,3 % | 27,8 % | 47,1 % |
| máximo posible | 80,7 % | 92,9 % | 68,2 % | 85,7 % |

Por cabecera, que es la unidad con la que se decide (revisar, podar, telemandar), el arbolado no cambia nada: 68,8 % frente a 68,4 %. Por CT sube la captura del 10 % de más riesgo de 27,5 % a 30,3 %, la mejor de los bloques sueltos, pero la diferencia la hace un solo año, 2025 (34 % frente a 19 %; en 2024 +2 puntos, en 2021 -4), el del temporal de enero que cayó sobre el norte, donde están las celdas con árboles altos. En el modelo con todos los bloques las variables del arbolado tienen una décima de la ganancia del relieve de la celda y de las áreas quemadas, al nivel de la historia de la cabecera.

**Lectura.** Va en la dirección de la literatura (los CT entre árboles altos salen mejor ordenados el año del temporal) pero con cinco años y unas 30 incidencias al año tres puntos por CT son ruido, y por cabecera no hay nada. La causa más probable es el grano: el arbolado está medido en la celda de 10 km del municipio del CT, no en el corredor de la línea, que es donde Wanik lo mide (15 m a cada lado, con LiDAR). Sin las coordenadas de cada CT y el trazado de las líneas (TedisNet las tiene como topología, no como geometría; Calser solo tiene el municipio) no se puede hacer mejor. Decisión: el bloque y el CSV se quedan en el job (entran en el ranking de 2026 como parte de "todo", como el resto de bloques públicos), el resultado se apunta como indicio, y lo que haría falta para una prueba de verdad es la geometría de las líneas de la distribuidora piloto, que es lo primero que pediremos en el piloto. Las capas de Copernicus HRL y el LiDAR del PNOA no cambiarían esta conclusión mientras el grano sea el municipio.

## 3i. Prueba: la sequía de los meses anteriores y la precipitación helada (15.10.2026)

**De dónde sale.** Dos filas más de la tabla de fuentes de la revisión que se pueden medir sin bajar nada nuevo. La sequía: Arora y Ceferino (NHESS 2023) meten el SPI de seis meses en sus modelos de cortes y la literatura de incendios usa el SPEI como sequedad del combustible y mortalidad del arbolado; nosotros probamos la lluvia de 7 y 30 días (apartado 3) y no aportó, pero una sequía de meses es otra cosa, y Gold ya tiene la lluvia y la evapotranspiración de referencia de cada celda. La precipitación helada: los sistemas de aviso de nieve húmeda (Bonelli y otros, NHESS 2011) y la climatología de lluvia engelante de Kämäräinen y otros (2017) la señalan como la causa de las caídas de conductores en el norte; en Extremadura es rara, pero el norte de Cáceres la tiene.

**Diseño.** En `job_ml_riesgo_dia.py`: `seq_3m` y `seq_6m` (bloque `sequia`), el balance hídrico (lluvia menos evapotranspiración de referencia, del reanálisis) de los 90 y 180 días que acaban en D - 1, como z-score de su mes del calendario con la media y la desviación de los años anteriores a 2022, por distribuidora; y `hie_horas` (bloque `hielo`), las CT-horas de D con precipitación a 0,5 grados o menos, del reanálisis, con su gemelo desde la previsión del día anterior (`prev_hie_horas`, ICON-EU da temperatura y lluvia) para medirlo con la previsión real. Variantes `operativa_sequia` y `operativa_hielo` frente a `operativa`, mismo banco que 3f.

**Resultados** (`dia_sequia_20261015_0109`, regresión logística, cada año puntuado por un modelo que solo vio los anteriores). Con la previsión real de 2024 - 2025: `operativa` 0,188 de PR-AUC (53 % de los días con avería en el 10 % de más riesgo, 67 % en el 20 %); con la sequía 0,178 (53 %, 66 %; diferencia emparejada -0,009 [-0,030, +0,005], probabilidad de ser mejor 11 %); con la precipitación helada 0,179 (53 %, 67 %; -0,006 [-0,027, +0,001], 23 %). Sobre el reanálisis de 2022 - 2025: 0,151 frente a 0,144 con la sequía (-0,006, 10 %) y 0,152 con el hielo (+0,000, 52 %); por años, la sequía solo gana en 2025 (0,197 frente a 0,189) y el hielo en 2023.

**Lectura.** Ninguna de las dos entra. La sequía de meses no añade nada a lo que ya dicen el calendario, el FWI del día y la historia reciente (y los días con avería de verano los marcan la tormenta y el calor, no el balance hídrico acumulado); la precipitación helada es demasiado rara en estas redes para que una logística aprenda de ella (unas pocas CT-horas al año, casi todas en el norte de Cáceres) y el aviso de nieve de AEMET ya está dentro. Las dos quedan en el job, y con ellas se cierran las filas de sequía y nieve de la lista de la revisión.

## 3j. Prueba: un segundo modelo de previsión y la media de los modelos (15.10.2026)

**De dónde sale.** De lo que más pesa en nuestras propias cifras y en la literatura: la mitad de lo que se pierde de la previsión perfecta a la real está en la racha prevista (apartado 3e), Yang y otros (2021) encuentran que el error del modelo de cortes a uno y tres días lo mandan la racha y la lluvia previstas, y Pardowitz y otros (2016) y Quiring y otros (2014) ganan habilidad con el conjunto de miembros. No tenemos el archivo del conjunto de ICON-EU para 2024 - 2025, pero la API Previous Runs de Open-Meteo guarda lo que otros modelos deterministas previeron el día antes para cada hora. Es la forma más barata de "conjunto": la media de varios modelos suele acertar más que el mejor de ellos cuando sus errores no van juntos. Es también una fuente pública que no habíamos probado.

**Qué modelos hay.** Sondeo del 15.10 sobre una celda de Cáceres (racha, viento, lluvia, temperatura y humedad previstas el día antes, en enero y junio de 2024 y marzo de 2025): ICON-EU (7 km, el nuestro) y los globales GFS (0,25 grados), ICON global (13 km) y GEM (15 km) tienen todo desde 2024; el UKMO de 10 km solo desde 2025; los de ECMWF (IFS 0,25), Météo-France (ARPEGE, AROME), KNMI y DMI no guardan la racha prevista (y los Harmonie no cubren Extremadura); JMA tampoco. Ninguno tiene más resolución que ICON-EU, así que lo que se mide es si la media con modelos más bastos pero independientes (GFS de NCEP, GEM de Environment Canada) mejora la previsión de racha y lluvia que entra al modelo del día.

**Diseño.** `job_landing_meteo_openmeteo.py --fuente openmeteo_previo_gfs` (y `_gem`) baja `*_previous_day1` de los dos modelos para las 43 celdas y 2024 - 2025 (cinco variables, 2.250 llamadas del cupo diario cada uno) y la Bronze los deja en `l1_bronze.meteo_openmeteo_previo_gfs_hora` y `_gem_hora` sin pasar por Silver ni Gold. `job_ml_riesgo_dia.py --previsiones-extra` los une al marco de Gold por celda y hora (`metp1gfs_*`), les calcula los mismos extremos sobre el p98 de cada celda y el mismo tiempo diario (`prevgfs_*`), y en la comparación con la previsión real puntúa la variante `operativa` (entrenada con el reanálisis, como siempre) con cinco tiempos del día: ICON-EU solo (`prevision`, lo de producción), GFS solo, GEM solo, la media de los tres modelos variable a variable (`prevision_media`) y cada uno llevado por cuantiles a la escala del reanálisis (`_cuantiles`, donde no cuenta el sesgo de cada modelo). Nunca son variables del modelo: no hay historia con la que entrenarlas. Criterio: la media entra en producción (la tarde pediría dos modelos más a Open-Meteo, que es una llamada más) si sube la PR-AUC y la captura al 10 - 20 % frente a ICON-EU solo con la diferencia emparejada a favor en la mayoría de las réplicas, y en los dos años.

**Resultados** (`dia_modelos_20261015_0144`; GFS y GEM cubren el 100 % de los días con previsión de ICON-EU; variante `operativa`, regresión logística, cada año puntuado por un modelo que solo vio los anteriores; 58 días con avería en 2024 - 2025):

| el tiempo del día que entra al modelo | PR-AUC | veces la tasa base | ROC-AUC | días con avería en el 10 % de más riesgo | en el 20 % | diferencia emparejada frente a ICON-EU solo [IC 95 %] | prob. de ser mejor |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ICON-EU solo (producción) | 0,188 | 6,9 | 0,82 | 53 % | 67 % | | |
| GEM solo | 0,177 | 6,5 | 0,81 | 47 % | 67 % | -0,011 [-0,036, +0,010] | 16 % |
| media de ICON-EU, GFS y GEM | 0,178 | 6,5 | 0,81 | 52 % | 67 % | -0,010 [-0,025, +0,003] | 5 % |
| GFS solo | 0,172 | 6,3 | 0,81 | 52 % | 66 % | -0,015 [-0,042, +0,004] | 7 % |
| el reanálisis del día (previsión perfecta) | 0,184 | 6,8 | 0,81 | 47 % | 69 % | | |

Llevados por cuantiles a la escala del reanálisis, donde el sesgo de cada modelo no cuenta, pasa lo mismo: ICON-EU 0,179, GEM 0,176 (-0,003, 38 %), la media 0,177 (-0,002 [-0,014, +0,011], 31 %) y GFS 0,175 (-0,005, 21 %). Con el rango de los dos modelos, la media queda en -0,003 (40 %); XGBoost solo, que no es el de producción, prefiere GFS (+0,024 con un intervalo de -0,053 a +0,103), ruido de un modelo que sobre el reanálisis va muy por debajo de la logística. Por redes, GEM sube la Distribuidora 3 de 0,24 a 0,39 y baja las otras dos: con diez CT y un puñado de días positivos, esa cifra baila.

**Lectura.** La media de modelos no mejora a ICON-EU, y ninguno de los dos globales lo mejora solo: ICON-EU a 7 km es el mejor de los modelos que guardan la racha prevista y la media con otros más bastos (GFS a 25 km, GEM a 15 km) se queda entre medias, como pasa cuando los errores no son independientes y uno de los miembros es claramente mejor. Lo que sí deja la prueba es el precio del respaldo: la Landing de la tarde ya cae a GFS cuando ICON-EU no responde, y ahora sabemos lo que cuesta esa tarde (0,172 frente a 0,188 de PR-AUC, un punto menos de captura en el 10 % y en el 20 %), un recorte aceptable para un día de respaldo. Decisión: la tarde sigue pidiendo solo ICON-EU; los dos modelos quedan en Bronze y en el job (`--previsiones-extra`) para volver a medirlos si entra una red fuera del dominio de ICON-EU o si algún día el archivo guarda un modelo de más resolución (AROME, UKMO a 10 km desde 2025).

## 3k. Prueba: el verdor de la red medido por satélite (fenología, 15.10.2026)

**De dónde sale.** La fila de fenología de la tabla de fuentes de la revisión: Cerrai y otros (2019) postprocesan la hoja, Cerrai y otros (2020) ponen el LAI entre lo que más pesa en los temporales de nieve, y Manning y otros (2025) encuentran el verano como multiplicador de la racha. En 3f probamos la hoja como calendario (abril - octubre) sin resultado; lo que faltaba era medirla. La fila decía "necesita cuenta (WEkEO o Earthdata)", pero el Planetary Computer de Microsoft sirve los compuestos de 16 días de NDVI de MODIS (MOD13A1 de Terra y MYD13A1 de Aqua, colección 6.1, 500 m) como COG con catálogo STAC y firma de enlaces anónimos: sin registro, y en lecturas por ventana de unos KB por celda. Lo que se mide no es solo la hoja: el NDVI de la dehesa y el pasto sigue la lluvia (verde en invierno y primavera, seco en verano), así que es a la vez estado de la hoja y sequedad del combustible, la señal lenta que la sequía de 3i buscaba por otro camino.

**Diseño.** `experimentos/fenologia/bajar_ndvi.py` deja en `etl/config/01_bronze/ndvi_celdas.csv`, para cada compuesto desde 2017 (uno cada 8 días contando los dos satélites) y cada una de las 43 celdas, la media del NDVI de los píxeles fiables (`pixel_reliability` 0 o 1) y cuántos eran. `job_ml_riesgo_dia.py` (bloque `fenologia`, `meteo_dia.fenologia`) lo pasa a red y día ponderando las celdas por sus CT: `fen_ndvi`, el último compuesto cuyo periodo había acabado siete días antes de D (la latencia con la que se publican; lo que se sabría esa tarde), `fen_ndvi_anom`, su anomalía como z-score del mismo periodo de 16 días del año sobre los compuestos anteriores a 2022, `fen_ndvi_cambio_1m`, el cambio frente al compuesto del mismo satélite un mes antes (la hoja llegando o cayendo), y `fen_int_racha_ndvi`, el exceso de la racha sobre 0,9 veces el p98 de la celda por el verdor (la interacción de 3f con la hoja medida en vez del calendario), con su gemelo desde la previsión. Variante `operativa_fenologia` frente a `operativa`, mismo banco que 3f.

**Los datos.** 848 compuestos de 2016-12 a 2026-09 para las 43 celdas (19.080 filas; el 97,4 % de los días del conjunto tienen valor con la latencia de una semana). Las celdas se comportan como se espera de la dehesa y de la montaña: en la penillanura de Cáceres el NDVI va de 0,61 en invierno a 0,26 en agosto (el pasto se seca), y en el Jerte y el Ambroz se queda entre 0,58 y 0,72 todo el año con el máximo en mayo (la hoja de los castaños y los robles); 2024 y 2025 salen como años muy verdes (anomalía media +1,2 y +1,0) y 2017 y 2019 como secos (-0,6).

**Resultados** (`dia_fenologia_20261015_0423`, regresión logística, cada año puntuado por un modelo que solo vio los anteriores). Con la previsión real de 2024 - 2025: `operativa` 0,188 de PR-AUC (53 % de los días con avería en el 10 % de más riesgo, 67 % en el 20 %) y `operativa_fenologia` 0,172 (53 %, 67 %; diferencia emparejada -0,012 [-0,032, -0,000], probabilidad de ser mejor 2 %); llevadas por cuantiles, las dos dan 0,179 (-0,000 [-0,007, +0,007], 47 %). Sobre el reanálisis de 2022 - 2025: 0,151 frente a 0,150 (-0,001 [-0,007, +0,004], 34 %), con la captura igual (37 - 38 % en el 10 %, 54 - 55 % en el 20 %); por años gana solo en 2023 (0,160 frente a 0,152) y pierde en 2022 y 2025.

**Lectura.** El verdor medido no añade nada a lo que ya dicen el calendario (que lleva la estación), la racha frente al clima de cada celda y la lluvia; la interacción racha x verdor, que era la apuesta (la hoja medida en lugar de abril - octubre), tampoco, y con la previsión en crudo incluso resta, porque hereda el sesgo al alza de la racha de ICON-EU. Cierra la fila de fenología de la revisión con una medida en vez de un "necesita cuenta", y deja dos cosas útiles: el camino sin registro al NDVI de MODIS (que sirve igual para VIIRS cuando Terra y Aqua se apaguen) y la serie de verdor por celda, que vale como contexto de sequía de combustible para el apartado de incendios de la memoria. El bloque queda en el job, apagado en producción (no está en `operativa`).

## 3m. Las pruebas otra vez con 2026 dentro (15.10.2026)

**Por qué.** Todas las comparaciones de 3e a 3k se hicieron con la previsión real de 2024 y 2025, 58 días con avería. Calser está cargado hasta el 02.08.2026, así que repetimos las doce variantes con la ventana hasta el 03.08.2026 y la previsión real de 2024, 2025 y 2026 (`dia_2026_20261015_1119`, regresión logística, cada año puntuado por un modelo que solo vio los anteriores): 84 días con avería en lugar de 58, con el temporal de enero de 2026 dentro.

**Resultados** con la previsión real de 2024 - 2026:

| variante | PR-AUC | veces la tasa base | días con avería en el 10 % de más riesgo | en el 20 % | diferencia frente a `operativa` [IC 95 %] |
| --- | --- | --- | --- | --- | --- |
| `operativa` (producción) | 0,161 | 5,3 | 46 % | 64 % | |
| solo racha y lluvia (`operativa_racha`) | 0,160 | 5,3 | 46 % | 64 % | -0,004 [-0,018, +0,011] |
| sequía | 0,158 | 5,2 | 46 % | 58 % | -0,006 [-0,022, +0,009] |
| pendiente por red | 0,157 | 5,2 | 46 % | 64 % | -0,002 [-0,012, +0,002] |
| interacciones | 0,156 | 5,1 | 46 % | 63 % | -0,004 [-0,015, +0,004] |
| verdor por satélite | 0,153 | 5,0 | 46 % | 64 % | -0,006 [-0,019, +0,001] |
| precipitación helada | 0,152 | 5,0 | 46 % | 63 % | -0,006 [-0,023, +0,001] |
| extensión del viento fuerte | 0,152 | 5,0 | 45 % | 66 % | -0,008 [-0,028, +0,006] |
| las tres de 3f juntas | 0,150 | 4,9 | 46 % | 64 % | -0,009 [-0,032, +0,004] |
| dos tardes antes (`operativa_48h`) | 0,146 | 4,8 | 39 % | 63 % | |
| tres tardes antes (`operativa_72h`) | 0,135 | 4,4 | 38 % | 62 % | |
| solo el tiempo de ayer | 0,082 | 2,7 | 26 % | 46 % | |

**Lectura.** Con un 45 % más de días con avería el orden no cambia: ninguna variante supera a la de producción y los intervalos se estrechan alrededor de cero o por debajo (el verdor por satélite queda por debajo en el 96 % de las réplicas). Las cifras bajan respecto a 2024 - 2025 porque 2026 es un año más difícil para el modelo (0,12 de PR-AUC sobre el reanálisis frente a 0,19 - 0,24 en 2024 y 2025, con 27 días con avería hasta agosto). A dos y tres días el sistema conserva casi todo lo que da con el 20 % de días (63 % y 62 % frente a 64 %) y pierde siete u ocho puntos con el 10 %, como con dos años. Estas son las cifras con tres años de previsión real que usamos desde ahora en la memoria.

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
- De la revisión de literatura ([revision-literatura.md](revision-literatura.md)), lo que no cabe antes del 18.10 (el modelo de conteos está medido en 3g, el arbolado por celda en 3h, la sequía y el hielo en 3i, el segundo modelo de previsión en 3j y la fenología en 3k): la exposición al arbolado en el corredor de la línea (hace falta la geometría de las líneas) junto a las líneas (densidad de copa y tipo de hoja del HRL de Copernicus, altura del dosel del LiDAR del PNOA o de los modelos globales de 10 m) como interacción con la racha, sobre todo para el modelo anual por cabecera; el índice de presión de nidos de cigüeña por cabecera (SEO/BirdLife, GBIF, vertederos y agua) para el modelo horario y el anual; CERRA como climatología del p98; y la dirección del viento prevista en el archivo de previsiones (`wind_direction_10m_previous_day1`) si la prueba 3f con el reanálisis la justifica.
- Pedir los cortes de los contadores de telegestión a una de las distribuidoras para medir cuántos cortes de baja tensión no vemos.
- Hecho el 14.10.2026: el replay da ya la detección y las falsas alarmas por día de cada política (apartado 2b de [caso-de-negocio.md](caso-de-negocio.md)): el sistema en alerta detecta el 71 % de los días con dos o más incidencias marcando el 12,5 % de los días, frente al 73 % con el 3 % de falsas alarmas del trabajo de Alcalá, que cuenta averías de una provincia entera. Queda pendiente definir el día extremo como ellos (por número de averías) cuando entre una red mayor.
- Con los avisos de AEMET, una política mixta por fenómeno (viento y tormenta sí, calor no) frente a la unión que ya medimos.
- El riesgo por zona a 48 y 72 horas (apartado 3e): medido a 48 h y a 72 h (se conserva la mayor parte de la señal y entre los dos horizontes no hay diferencia medible). Decidido el 15.10.2026: la ejecución de la tarde da también el nivel de pasado mañana (`variante_pasado_manana` = `operativa_48h`, con la previsión de D+2 y los avisos de D+2 que la misma petición ya trae) como orientación para planificar, sin parte de guardia.
