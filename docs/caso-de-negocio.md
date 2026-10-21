# Caso de negocio con los datos reales: qué habría pasado en 2022 - 2026

Hasta ahora medíamos el modelo con métricas de clasificación (PR-AUC, parte de días con avería en el 10 % de más riesgo). Para una distribuidora o un inversor la pregunta es otra: si el sistema hubiera estado en marcha desde 2022, qué habría dicho cada tarde y qué habría pasado al día siguiente. Este documento responde con los datos de la Distribuidora 1 y la Distribuidora 2 de 2022 al 02.08.2026, el último día con etiqueta conocida de la copia de Calser (la Distribuidora 3, con 10 CT, tuvo una sola incidencia de causa local en esos años, en 2023, y con una incidencia sus días no dicen nada del nivel). La página `historial.html` del cuadro de mando lo enseña día a día: el color es el nivel que habría dado el sistema la tarde anterior y los puntos, las averías que hubo.

## 1. Cómo lo hemos medido

- Cada día se puntúa con el mismo modelo que corre cada tarde (`job_ml_riesgo_manana`, variante `operativa`, regresión logística), entrenado solo con los años anteriores al que se puntúa: 2022 con 2018 - 2021, 2023 con 2018 - 2022 y así hasta 2026 (siete meses, hasta el 02.08.2026). El nivel sale como en el job: muy alto desde el percentil 95 de los días de entrenamiento de la distribuidora, alto desde el 90, medio desde el 75.
- El tiempo del día: desde 2024, la previsión real de ICON-EU de la víspera, la misma que usa el job; en 2022 y 2023, el reanálisis, que hace de previsión perfecta y da una cota superior. Los avisos de AEMET para el día emitidos antes de las 20:00 de la víspera entran en el modelo y sirven además de política de comparación (apartado 2b).
- Las averías: las incidencias de causa local de Calser (equipos, sobrecargas, meteorología, vegetación e incendios, fauna, terceros, objetos ajenos y agentes externos, y causa desconocida o no indicada; fuera las de aguas arriba, los apagones, las programadas, las maniobras y las que la clasificación de causas atribuye a la red de baja tensión o a la instalación de un cliente, `riesgo_zonas.LOCAL_EXCLUDED`; las que empiezan en el mismo segundo en dos distribuidoras se quedan con la causa que escribió el operador y solo llevan la marca `simultanea_otra_red`, después de leerlas el 17.10.2026 como cortes de un proveedor común y retirar esa lectura al día siguiente, así que las cifras de este documento no cambian) en los CT en estudio, con los clientes de cada CT y la duración real de cada corte. Las horas-cliente son la suma, CT a CT, de clientes por horas sin suministro. Como un solo corte (el incendio del 13.08.2025, con tres CT de un mismo municipio sin luz 32 horas) pesa tanto como un año entero, las tablas recortan los cortes a 110 minutos (el percentil 99 de 2022 - 2025) y dan al lado la cifra sin recortar.
- Las brigadas: las bases que minimizan la llegada (p-mediana sobre los municipios de los CT) con los minutos por carretera de OSRM sobre OpenStreetMap, 1 y 2 brigadas, y fuera del horario laboral (lunes a viernes de 8 a 17) una movilización de 30 minutos cuando la brigada está de retén.

El cálculo está en el job `job_ml_caso_negocio.py` y deja `historico.json` para la página del cuadro de mando, las tablas de este documento (ejecución `caso_v10_20261014_1543` del lakehouse) y los Excel `2026-10-14 Caso_de_negocio_2022-2026.xlsx` y `2026-10-14 Caso_de_negocio_2022-2025.xlsx` de la carpeta de exportación del proyecto, con las tablas como fórmulas sobre los días y los cortes.

## 2. Lo que vería un director de distribución

Con la previsión real de la víspera, del 20.01.2024 al 02.08.2026 (la Distribuidora 1 y la Distribuidora 2 juntas, 1.844 días de distribuidora):

| nivel previsto la víspera | días | con avería | incidencias | horas-cliente (recortadas) | parte | parte sin recortar |
| --- | --- | --- | --- | --- | --- | --- |
| bajo | 1.317 (71 %) | 1,9 % | 27 | 10.626 | 16 % | 22 % |
| medio | 296 (16 %) | 6,1 % | 18 | 7.400 | 11 % | 8 % |
| alto | 108 (6 %) | 7,4 % | 8 | 3.917 | 6 % | 22 % |
| muy alto | 123 (7 %) | 17,9 % | 43 | 43.065 | 66 % | 47 % |

Solo con 2024 y 2025 (1.422 días), que es lo que había antes de incorporar 2026: bajo 1.047 días y 1,7 % con avería; medio 198 y 4,5 %; alto 86 y 9,3 %; muy alto 91 y 17,6 %; los días en alerta eran el 12 % y llevaban el 57 % de las horas-cliente (el 73 % sin recortar).

- Un día muy alto tiene avería 9 veces más a menudo que un día bajo (17,9 % frente a 1,9 %: 22 de 123 días y 25 de 1.317, con intervalos de Wilson del 95 % de 12,1 a 25,6 % y de 1,3 a 2,8 %; con 2024 - 2025 solo, 10 veces: 17,6 % frente a 1,7 %). Por separado se mantiene: en la Distribuidora 1, 22,2 % frente a 1,5 % en 2024 - 2025 (45 días muy altos, 10 con avería); en la Distribuidora 2, 13,0 % frente a 1,9 % (46 días, 6 con avería).
- Los días en alerta (alto o muy alto) son el 12,5 % de los días y concentran el 72 % de las horas-cliente perdidas por causas locales (el 70 % sin recortar). El temporal del 28.01.2026 pesa mucho en esa cifra: las dos distribuidoras estaban en muy alto desde la tarde anterior (la Distribuidora 2 con un 21 % de probabilidad y la 1 con un 18 %, 6 y 4 veces su tasa base de 2022 - 2025), tuvieron 20 incidencias, 73.485 clientes afectados y 31.795 horas-cliente, el 49 % de todas las de 2024 - 2026. Sin ese día, los días en alerta llevan el 46 % de las horas-cliente (53 % sin recortar).
- De los 3 peores días de cada distribuidora y año de 2024 - 2026 (18 días que suman el 77 % de las horas-cliente), 9 estaban en alerta la víspera; de los 5 peores (30 días, el 87 %), 15. Con el reanálisis, en 2022 - 2026: 13 de 30 y 19 de 50. Mirando los 10 peores días de 2024 - 2026 sin separar por distribuidora ni año (el 68 % de las horas-cliente), 7 estaban en alerta; en 2024 - 2025, 8 de 10.
- Dentro del día, en las 51 incidencias de los días de alerta de 2024 - 2026: el 84 % empezaron dentro de la franja horaria que daba el sistema (43 de 51; de 72 a 92 % con el intervalo de Wilson del 95 %), y la línea que falló estaba entre las 3 primeras del ranking de su distribuidora el 25 % de las veces (13 de 51, de 16 a 39 %; azar 9 %, con unas 35 líneas por distribuidora) y entre las 5 primeras el 41 % (21 de 51, de 29 a 55 %; azar 14 %). El sistema acierta el día y la franja mucho mejor que la línea.
- El 71 % de los días son bajos y en ellos cae una de cada seis horas-cliente (una de cada cuatro en 2024 - 2025): son las averías de equipos y de causa desconocida que el tiempo no anuncia.
- La probabilidad que ve el usuario es de fiar: en los días muy altos el modelo da de media un 15,5 % y pasa el 17,9 %; en los altos, 7,3 % y pasa el 7,4 %; en los medios, 4,6 % y pasa el 6,1 %; en los bajos, 2,7 % y pasa el 1,9 %. El día más alto fue el de la Distribuidora 1 del 05.02.2026, con un 48 %; las tardes repetidas que guarda el cuadro de mando, que entrenan con todos los días hasta la víspera, llegan al 49,9 % (la Distribuidora 1 el 05.11.2025, `dashboard/data/riesgo_20251105.json`). Como separador de días, AUC 0,72 (la Distribuidora 1 0,76, la Distribuidora 2 0,66) con una tasa base del 4,0 %; el Brier es 0,036 frente a 0,038 de la tasa base, una mejora modesta porque la mayor parte de las averías no la explica ningún dato meteorológico. En 2024 - 2025 el 10 % de días de más riesgo concentraba el 41 % de los días con avería y el 53 % de las horas-cliente.

Por distribuidora y año:

| distribuidora | año | tiempo que ve el modelo | días en alerta | incidencias | en días de alerta | horas-cliente | en días de alerta | sin recortar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distribuidora 1 | 2022 | reanálisis | 41 (11 %) | 30 | 13 (43 %) | 13.134 | 4.380 (33 %) | 34 % |
| Distribuidora 1 | 2023 | reanálisis | 36 (10 %) | 21 | 5 (24 %) | 16.693 | 10.871 (65 %) | 65 % |
| Distribuidora 1 | 2024 | previsión real | 38 (11 %) | 17 | 8 (47 %) | 7.543 | 3.867 (51 %) | 51 % |
| Distribuidora 1 | 2025 | previsión real | 54 (15 %) | 18 | 10 (56 %) | 9.670 | 5.388 (56 %) | 82 % |
| Distribuidora 1 | 2026 (hasta el 02.08) | previsión real | 27 (13 %) | 14 | 5 (36 %) | 17.726 | 13.032 (74 %) | 73 % |
| Distribuidora 2 | 2022 | reanálisis | 50 (14 %) | 13 | 3 (23 %) | 8.101 | 244 (3 %) | 3 % |
| Distribuidora 2 | 2023 | reanálisis | 35 (10 %) | 19 | 5 (26 %) | 3.916 | 1.298 (33 %) | 33 % |
| Distribuidora 2 | 2024 | previsión real | 44 (13 %) | 11 | 5 (45 %) | 4.555 | 3.288 (72 %) | 72 % |
| Distribuidora 2 | 2025 | previsión real | 41 (11 %) | 9 | 4 (44 %) | 3.283 | 1.752 (53 %) | 50 % |
| Distribuidora 2 | 2026 (hasta el 02.08) | previsión real | 27 (13 %) | 27 | 19 (70 %) | 22.231 | 19.653 (88 %) | 63 % |

La Distribuidora 2 en 2022 es el año malo: sus dos peores días (6.300 horas-cliente, averías de equipos en días de calma) salieron bajos. Ya conocíamos ese límite: el sistema mira el tiempo y no sabe en qué estado están los equipos. 2026 es el año del temporal del 28.01.2026: en la Distribuidora 2, 16 incidencias en un día, 35.782 clientes y 18.812 horas-cliente; el sistema lo tenía en muy alto desde la tarde anterior, y AEMET en aviso naranja.

Los diez peores días de 2024 - 2026 y lo que decía el sistema la tarde anterior:

| día | distribuidora | nivel previsto | probabilidad | incidencias | clientes | horas-cliente (recortadas) | sin recortar | corte más largo (min) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 28.01.2026 | Distribuidora 2 | muy alto | 21,2 % | 16 | 35.782 | 18.812 | 18.875 | 116 |
| 28.01.2026 | Distribuidora 1 | muy alto | 18,2 % | 4 | 37.703 | 12.983 | 12.983 | 35 |
| 10.06.2024 | Distribuidora 1 | muy alto | 11,9 % | 1 | 24.381 | 2.039 | 2.039 | 5 |
| 05.11.2025 | Distribuidora 1 | muy alto | 35,7 % | 2 | 1.971 | 1.895 | 2.465 | 169 |
| 24.01.2026 | Distribuidora 1 | bajo | 2,5 % | 1 | 9.949 | 1.861 | 1.861 | 9 |
| 01.08.2026 | Distribuidora 2 | bajo | 2,9 % | 2 | 794 | 1.455 | 10.531 | 800 |
| 12.10.2024 | Distribuidora 2 | muy alto | 11,5 % | 1 | 313 | 1.431 | 1.431 | 75 |
| 04.04.2025 | Distribuidora 2 | muy alto | 15,2 % | 1 | 365 | 1.262 | 1.262 | 58 |
| 18.03.2025 | Distribuidora 1 | alto | 10,2 % | 1 | 1.813 | 1.249 | 1.249 | 41 |
| 28.05.2025 | Distribuidora 1 | bajo | 2,6 % | 1 | 11.194 | 1.113 | 1.113 | 6 |

La noche del incendio del 13.08.2025 (el corte más largo de estos años, 17.349 horas-cliente sin recortar en tres CT) la Distribuidora 1 estaba en alto desde la tarde anterior; con el recorte queda fuera de los diez peores.

## 2b. Frente a los avisos de AEMET, que es lo que hoy mira un jefe de brigadas

Los avisos de AEMET son una previsión real con archivo desde 2018 y lo que una distribuidora consulta cada tarde. Con los mismos días (previsión real, 2024 - 2026, la Distribuidora 1 y la Distribuidora 2; avisos para el día siguiente emitidos antes de las 20:00 de la víspera, en las celdas de cada red):

| política de guardia | días al año por distribuidora | parte de los días | incidencias cubiertas | horas-cliente cubiertas (recortadas) | sin recortar |
| --- | --- | --- | --- | --- | --- |
| aviso de AEMET amarillo o más, de cualquier fenómeno | 85 | 23 % | 56 % | 80 % | 85 % |
| aviso de AEMET de viento, amarillo o más | 10 | 3 % | 34 % | 58 % | 41 % |
| aviso de AEMET de viento o tormenta, amarillo o más | 23 | 6 % | 45 % | 69 % | 68 % |
| aviso de AEMET naranja o rojo | 20 | 6 % | 32 % | 57 % | 59 % |
| GridPredic en alerta (alto o muy alto) | 46 | 12 % | 53 % | 72 % | 70 % |
| GridPredic en alerta o aviso de viento o tormenta | 50 | 14 % | 55 % | 74 % | 71 % |
| GridPredic en alerta o aviso naranja o rojo | 63 | 17 % | 55 % | 74 % | 71 % |
| GridPredic con tantos días como avisos amarillos | 85 | 23 % | 69 % | 84 % | 78 % |

- El temporal del 28.01.2026 lo vio todo el mundo (aviso naranja y muy alto), y por eso en 2024 - 2026 los avisos de viento y los naranjas cubren más de la mitad de las horas-cliente con muy pocos días. Sin ese día (2024 - 2025) los avisos de viento cubrían una de cada cinco horas-cliente con 8 días al año y los naranjas el 18 %: la mayor parte de las averías de causa local llegan sin aviso de viento.
- Cualquier aviso amarillo (viento, lluvia, tormentas, calor) cubre el 80 % de las horas-cliente, pero reforzando la guardia 85 días al año.
- La regla que un jefe de brigadas aplicaría de cabeza, "aviso de viento o de tormenta", es la mejor de las reglas sencillas: con 23 días al año cubre el 69 % de las horas-cliente (49 % en 2024 - 2025; 40 % sin el temporal del 28.01.2026), y el sistema con esos mismos 23 días cubre menos (66 %, 42 % y 34 %): los avisos de tormenta señalan días de verano que el modelo pone en alto o muy alto (48 de los 63 días con aviso de tormenta sin viento de 2022 - 2026) pero no entre los 23 primeros del año. Donde el sistema gana es con más días, y sobre todo juntando las dos cosas: "GridPredic en alerta o aviso de viento o tormenta" son 50 días al año para el 74 % de las horas-cliente (61 % en 2024 - 2025; 50 % sin el temporal), frente al 72 % (57 %, 46 %) de la alerta sola con 46 días y al 74 % (58 %) de la alerta o aviso naranja con 63. Es la política que recomendamos para la guardia. De las dos formas de juntar el sistema con los avisos es la que menos días gasta (con los naranjas o rojos hacen falta 63 para el mismo 74 %); la alerta sola gasta todavía menos, 46 días para el 72 %, pero se le escapan los días de tormenta de verano. Con los mismos 85 días que los avisos amarillos, elegidos por el sistema y repartidos entre los años como los avisos, se cubre el 84 %; repartidos por igual cada año, como en la curva de `negocio.html`, casi lo mismo que los avisos. En 2024 - 2025: avisos amarillos 60 % con 80 días, sistema 77 % con los mismos 80 días y 57 % con 44 días de alerta. Con poco más de la mitad de los días de los avisos amarillos, la alerta sola cubre el 72 %, ocho puntos menos que ellos.
- Con el reanálisis en 2022 - 2026 el orden es el mismo (avisos amarillos 64 % de las horas-cliente con el 22 % de los días; GridPredic 59 % con el 13,5 % y 66 % con los mismos días que los avisos), con la cota optimista que ya hemos dicho.
- Los calendarios fijos, con los mismos días (`experimentos/perfil_dia/salida/serie/calendarios.csv`, de `serie_dia.py`): reforzar los fines de semana de noviembre a marzo, 44 días al año, recoge el 4 % de las horas-cliente, y esos días tienen además menos averías (incidencia en 13 de 532 días de fin de semana frente a 60 de 1.312 laborables; Fisher, p = 0,035). Un calendario climatológico, que elige tantos días como la alerta por la tasa de más o menos 15 días de los años anteriores, recoge el 15,6 % de las incidencias y el 10,9 % de las horas-cliente (21,3 % sin el temporal del 28.01.2026, cuando la alerta se queda en el 46 %), frente al 53 % y el 72 % de la alerta: las averías de estas redes siguen al tiempo de cada año, y el calendario no lo ve.
- `negocio.html` dibuja la misma comparación a cualquier presupuesto: la cobertura de las horas-cliente frente a los días reforzados al año, con el sistema a cada umbral (los k días de más riesgo de cada año) como curva y los calendarios y los avisos de AEMET como puntos. Con menos de 15 días al año los avisos de viento cubren más que el sistema (el 28.01.2026, que los dos vieron, es la mitad de las horas-cliente); a partir de unos 16 días el sistema cubre más que los avisos de viento y que los naranjas o rojos, pero no supera a la regla de viento o tormenta, que con sus 23 días sigue unos tres puntos por delante; los calendarios fijos quedan sobre la línea del azar.
- En las métricas de los trabajos que predicen los días con averías (detección y falsas alarmas por día; tabla "Detección y falsas alarmas" de `resumen.md`), con la previsión real de 2024 - 2026: el sistema en alerta marca el 12,5 % de los días y detecta el 41 % de los días con alguna incidencia (73), el 71 % de los días con dos o más (5 de 7, de 36 a 92 % con el intervalo de Wilson del 95 %) y el 41 % de los 10 peores días de cada año por horas-cliente (59), con un 11 - 12 % de falsas alarmas; los avisos amarillos marcan el 23 % de los días y detectan el 44 %, el 86 % y el 39 %; el sistema con esos mismos días, el 62 %, el 71 % y el 63 %. El trabajo de la Universidad de Alcalá sobre Valencia y Albacete detecta el 38 % y el 73 % de los días extremos con un 1 % y un 3 % de falsas alarmas, pero define el día extremo por el número de averías de una provincia entera, con decenas de averías al día; con 7 días de dos o más incidencias en dos años y medio, nuestra cifra equivalente (71 %) se mueve mucho de un año a otro y la contamos como orden de magnitud.

## 2c. Calibración y valor según lo que cueste reforzar

La PR-AUC y la captura dicen qué días van primero. Para un director cuentan otras dos cosas: si la probabilidad se puede leer tal cual (un 10 % es un día de cada diez) y cuánto ahorra decidir con ella según lo que le cueste reforzar la guardia. Lo medimos el 17.10.2026 con la misma tabla de días y la previsión real de las tres redes (apartado 3s de [investigacion-continua.md](investigacion-continua.md); salidas en `experimentos/evaluacion/salida/`).

- **Se puede leer tal cual en las dos redes que averían.** En la Distribuidora 1 la probabilidad media es 0,047 y la tasa de días con avería 0,046; en la Distribuidora 2, 0,035 y 0,034. Por niveles, muy alto dice 12,8 % y pasa 11,7 %, y bajo dice 2,3 % y pasa 1,3 %. La Distribuidora 3 recibe un 2 % de media y en 922 días no tuvo ninguna avería de causa local; con la etiqueta del modelo, que cuenta cualquier interrupción computable, tuvo 9 días frente a los 18,9 que esperaba el modelo, así que allí la probabilidad no se puede leer tal cual. El Brier mejora un 5 % el de decir cada día la tasa observada (BSS de 0,052, intervalo por semanas de 0,003 a 0,096), una mejora modesta, como corresponde a un suceso que pasa un día de cada 40.
- **Compensa cuando reforzar cuesta poco frente a lo que se pierde.** Si un día de guardia reforzada cuesta el 5 % de lo que se pierde en un día con avería sin ella, reforzar cuando la probabilidad pasa del 5 % ahorra el 29 % de lo que ahorraría una previsión perfecta, contando días con avería (intervalo de 0,14 a 0,40), y el 51 % contando horas-cliente. La alerta de hoy (alto o muy alto) da el 19 % y el 48 %. Con un coste del 10 %, la regla probabilística sigue en positivo (13 % por días) y la alerta ya no (-6 %).
- **El umbral debería salir del coste de cada distribuidora.** La alerta por percentiles equivale a reforzar desde un 7 % de probabilidad en la Distribuidora 1, un 5 % en la 2 y un 2,5 % en la 3. Por días con avería, la alerta solo compensa si reforzar cuesta menos del 8 % de lo que se pierde; leyendo la probabilidad, hasta el 44 %. En la Distribuidora 3, que casi nunca avería, la alerta es casi siempre una falsa alarma: allí conviene leer la probabilidad y no el nivel.
- **Sin el temporal del 28.01.2026** la regla probabilística conserva casi todo su valor por días (0,27 en el coste del 5 %) y pierde parte del de horas-cliente (de 0,51 a 0,36), que depende mucho de ese día.

Lo que falta para dar una cifra en euros es el coste de un retén o de una guardia reforzada en cada distribuidora, que no hemos podido verificar en los convenios. Con ese dato, la curva de valor da el umbral de cada una.

## 3. Las brigadas: lo que dicen los datos

| distribuidora | brigadas | año | cortes de CT | más largos que la llegada de la brigada | horas-cliente de esos cortes | llegada media con bases fijas (min) | en alerta, con las bases del día (min) | minutos de TIEPI ganados |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Distribuidora 1 | 1 | 2024 | 1.267 | 53 (4 %) | 1.103 (14 %) | 45 | 36 | 0,00 |
| Distribuidora 1 | 2 | 2024 | 1.267 | 67 (5 %) | 2.080 (27 %) | 25 | 25 | 0,00 |
| Distribuidora 1 | 1 | 2025 | 755 | 17 (2 %) | 19.752 (73 %) | 66 | 60 | 0,08 |
| Distribuidora 1 | 2 | 2025 | 755 | 74 (10 %) | 21.697 (80 %) | 23 | 25 | 0,08 |
| Distribuidora 2 | 1 | 2024 | 634 | 15 (2 %) | 50 (1 %) | 47 | 45 | 0,54 |
| Distribuidora 2 | 2 | 2024 | 634 | 15 (2 %) | 50 (1 %) | 32 | 27 | 0,54 |
| Distribuidora 2 | 1 | 2025 | 319 | 16 (5 %) | 642 (18 %) | 39 | 37 | 0,21 |
| Distribuidora 2 | 2 | 2025 | 319 | 16 (5 %) | 642 (18 %) | 39 | 30 | 0,31 |

Las horas-cliente de esta tabla van sin recortar: en la Distribuidora 1 en 2025 el incendio del 13.08.2025 (17.349 horas-cliente en tres CT) es casi todo lo que esperó a la brigada.

- Casi tres de cada cuatro cortes de CT (el 72 %) duran menos de 15 minutos, la mediana son 8: los repone el reenganche o el telemando antes de que ninguna brigada pueda llegar. Solo entre el 2 % y el 10 % de los cortes duran más que la llegada de la brigada, pero son los largos: en la Distribuidora 1, el 14 - 27 % de las horas-cliente de 2024 y, con el incendio, el 73 - 80 % de las de 2025. Son esos los que la brigada puede acortar.
- Cambiar las bases según el riesgo del día apenas mueve la llegada media (entre 0 y 9 minutos, y con dos brigadas en la Distribuidora 1 no la mejora) y gana menos de un minuto de TIEPI al año (0,54 como máximo, en la Distribuidora 2 en 2024): con dos brigadas, la base fija óptima ya cubre bien las dos mitades de cada red. El valor de la víspera está en tener gente y material preparados los días que toca y en la franja horaria en la que toca, más que en mover furgonetas (el 84 % de las incidencias de los días de alerta de 2024 - 2026 empiezan dentro de la franja del pico de racha, y el 64 % de las incidencias empiezan fuera del horario laboral).
- Lo que sí acorta las horas-cliente en estas redes es el telemando: los cortes que esperan a la brigada son pocos pero largos. El ranking anual de CT y cabeceras ([network-analysis.md](network-analysis.md)) dice dónde ponerlo.

## 4. Cuánto vale

Las distribuidoras no facturan al cliente: cobran la retribución regulada que fija la CNMC. La de 2023, publicada en el BOE (BOE-A-2026-19941), fue de 11.994.208 euros para la Distribuidora 1, 10.833.762 para la Distribuidora 2 y 208.986 para la Distribuidora 3. La Circular 8/2025 de la CNMC, que fija la metodología de 2026 a 2031, liga parte de esa retribución a la calidad: su artículo 24 establece un incentivo a la mejora de la calidad de suministro calculado sobre el TIEPI de los años n-4 a n-2 frente a una referencia sectorial, que va del -4 % al +4 % de la retribución, y el artículo 25 da la fórmula; el artículo 15 permite reconocer, con cargo a los costes de distribución y siempre con la aprobación previa de la CNMC, inversiones en proyectos piloto con beneficio cuantificable, incluidos los de adaptación de la red al cambio climático. Con la retribución de la Distribuidora 1, cada décima de punto son unos 12.000 euros al año; la licencia del sistema se paga por el tamaño de la red ([experimentos/plan_financiero](../experimentos/plan_financiero/README.md)): 3.000 euros fijos, 150 euros por CT hasta 100 CT y, a partir de ahí, descuento por volumen (15 euros por CT del 101 al 300 y 10 desde el 301), porque el coste de servir a una red apenas crece con sus CT, más 0,10 euros por señal, con un mínimo de 6.000 euros para las redes muy pequeñas y sin máximo. Con sus CT y sus señales de TedisNet (20.10.2026), la Distribuidora 1 pagaría 29.425 euros al año, lo que valen 2,3 minutos de TIEPI de su incentivo; la Distribuidora 2, 26.531 euros (1,8 minutos), y la Distribuidora 3, el mínimo. La referencia sectorial existe y es pública: el Ministerio publica en GECOS el TIEPI oficial por tipo de zona de todo el sector, y con ella la Distribuidora 1 y la Distribuidora 2 están hoy muy por debajo del sector (con su misma mezcla de zonas, 0,34 h de TIEPI imprevisto al año en 2023 - 2025 en la Distribuidora 1 frente a 0,92 h en Cáceres y 1,51 h en España en 2019 - 2021, los últimos años publicados, y 0,12 h en la Distribuidora 2 frente a 0,63 h; apartado 2 de [network-analysis.md](network-analysis.md) y la sección "Dónde está cada red frente al sector" de `negocio.html`). Del castigo una distribuidora así está lejos; lo que se juega es perder la bonificación el año del temporal.

Lo que el sistema aporta a esa cuenta, con los números de arriba:

- Dónde está el TIEPI de causas locales: en la Distribuidora 1, entre 16 y 35 minutos al año en los CT en estudio (2022 - 2025); en la Distribuidora 2, entre 5 y 19. Los días en alerta concentran más de la mitad. Es el TIEPI sobre el que actúan las brigadas y el telemando; el de aguas arriba y el apagón del 28.04.2025 (12.228 horas de CT, más que todas las causas locales de cuatro años juntas) no dependen de la distribuidora.
- La guardia: en lugar de reforzar la guardia todos los fines de semana de invierno, reforzarla los 40 - 55 días de alerta al año (o los 50 de "alerta o aviso de viento o tormenta", la política que recomendamos: de las dos formas de juntar el sistema con los avisos, es la que menos días gasta), durante la franja de 12 - 14 horas que marca el sistema, con las bases y los municipios que cubre cada una. Son unas 500 - 750 horas de franja al año por distribuidora.
- Las compensaciones a los clientes del Real Decreto 1955/2000 (descuentos cuando se superan los límites individuales de calidad) y el coste reputacional de un corte largo caen sobre los mismos días: los 20 peores días de 2024 - 2025 suman el 73 % de las horas-cliente y 12 estaban en alerta la víspera.

La cifra en euros del incentivo la estimamos en la memoria (3.8.3) con la fórmula de la Circular 8/2025 y la retribución de 2023: un minuto de TIEPI vale unos 12.600 euros de incentivo al año en la Distribuidora 1 y 14.900 en la Distribuidora 2. Lo que todavía no podemos afirmar con estos datos es cuántos minutos evita el sistema, que tiene que medirlo un piloto, ni el ahorro en horas extra, que depende de cómo organiza hoy cada empresa su retén.

## 5. Cómo enseñarlo

- `negocio.html` en el cuadro de mando (http://localhost:8086/negocio.html): la página para una distribuidora o un inversor, calculada en el navegador a partir de `historico.json`: las cuatro cifras (veces más averías un día muy alto, parte de las horas-cliente en los días de alerta, peores días avisados, días de alerta al año), la tasa de avería por nivel y dónde cayeron las horas-cliente, la guardia a igual presupuesto (fines de semana de invierno, todos los fines de semana, el sistema con los mismos días, solo los días en alerta, con el coste por día que ponga quien la mira), la tabla por distribuidora y año, los diez peores días, la retribución y la licencia, y lo que el sistema no hace. Se imprime o se guarda en PDF desde el enlace de la cabecera.
- `historial.html` en el cuadro de mando (http://localhost:8086/historial.html): de 2022 a 2026 en una pantalla por distribuidora, con el selector de previsión real o reanálisis y el recorte del incendio. Al pasar el ratón por un día salen el nivel, la probabilidad, las incidencias, los clientes y las bases propuestas.
- El cuadro de mando de mañana (`index.html`): el mismo producto en vivo, con los rayos del satélite cada 15 minutos.
- Para una reunión: la tabla por niveles (9 veces más averías un día muy alto), la guardia a igual presupuesto frente a los calendarios fijos y a los avisos de AEMET con la curva de cobertura a cualquier presupuesto (apartado 2b y `negocio.html`), dónde está la red frente al sector (GECOS), los 10 peores días con lo que decía el sistema, la tarde del 27.01.2026 repetida en el cuadro de mando (`index.html?dia=20260128`) y la noche del incendio del 13.08.2025. El guion completo de la defensa está en [guion-presentacion.md](guion-presentacion.md) y las cifras con su origen en [cifras.md](cifras.md).

## 6. Límites

- Son pocas incidencias, unas 35 al año entre las dos distribuidoras, y la parte de las horas-cliente que cae en días de alerta salta de un año a otro: suele quedar entre el 30 % y el 70 %, pero en la tabla del apartado 2 va del 3 % al 88 %. Hay que mirar todos los años juntos.
- Calser carga las averías con retraso, y con la copia del 14.08.2026 los últimos 41 a 62 días de la ventana tienen etiquetas incompletas: en junio y julio de 2026 puede faltar algún día con avería.
- 2022 y 2023 usan el reanálisis como previsión, que no tiene el error de la previsión de la víspera: por ese lado la cota es optimista. Desde 2024 el modelo ve la previsión real y los resultados son los que daría el job.
- La duración de cada corte es la de Calser, que incluye el tiempo de localización y reposición; no sabemos qué parte de cada corte esperó a la brigada, así que tomamos como cortes que dependen de la brigada los que duraron más que su llegada.
- Las bases fijas de referencia son las óptimas por clientes, no las reales de cada empresa; con las reales, la ganancia de mover las bases sería mayor o menor, pero el orden de magnitud (menos de un minuto de TIEPI al año) no cambia.
