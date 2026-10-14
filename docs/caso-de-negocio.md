# Caso de negocio con los datos reales: qué habría pasado en 2022 - 2025

Hasta ahora medíamos el modelo con métricas de clasificación (PR-AUC, parte de días con avería en el 10 % de más riesgo). Para una distribuidora o un inversor la pregunta es otra: si el sistema hubiera estado en marcha estos cuatro años, qué habría dicho cada tarde y qué habría pasado al día siguiente. Este documento responde con los datos de EOSA y Pitarch de 2022 a 2025 (Valle de Santa Ana, con 10 CT, no tuvo incidencias de causa local en esos años). La página `historial.html` del cuadro de mando lo enseña día a día: el color es el nivel que habría dado el sistema la tarde anterior y los puntos, las averías que hubo.

## 1. Cómo lo hemos medido

- Cada día se puntúa con el mismo modelo que corre cada tarde (`job_ml_riesgo_manana`, variante `operativa`, regresión logística), entrenado solo con los años anteriores al que se puntúa: 2022 con 2018 - 2021, 2023 con 2018 - 2022 y así hasta 2025. El nivel sale como en el job: muy alto desde el percentil 95 de los días de entrenamiento de la distribuidora, alto desde el 90, medio desde el 75.
- El tiempo del día: desde 2024, la previsión real de ICON-EU de la víspera, la misma que usa el job; en 2022 y 2023, el reanálisis, que hace de previsión perfecta y da una cota superior.
- Las averías: las incidencias de causa local de Calser (equipos, meteorología, vegetación, fauna, terceros y causa desconocida; fuera las de aguas arriba, los apagones, las programadas y las maniobras) en los CT en estudio, con los clientes de cada CT y la duración real de cada corte. Las horas-cliente son la suma, CT a CT, de clientes por horas sin suministro. Como un solo corte (el incendio de Jarilla del 13.08.2025, tres CT de Cabezabellosa sin luz 32 horas) pesa tanto como un año entero, las tablas recortan los cortes a 110 minutos (el percentil 99 de 2022 - 2025) y dan al lado la cifra sin recortar.
- Las brigadas: las bases que minimizan la llegada (p-mediana sobre los municipios de los CT) con los minutos por carretera de OSRM sobre OpenStreetMap, 1 y 2 brigadas, y fuera del horario laboral (lunes a viernes de 8 a 17) una movilización de 30 minutos cuando la brigada está de retén.

El cálculo está en el job `job_ml_caso_negocio.py` y deja `historico.json` para la página del cuadro de mando y las tablas de este documento.

## 2. Lo que vería un director de distribución

Con la previsión real de la víspera, en 2024 y 2025 (EOSA y Pitarch juntas, 1.422 días de distribuidora):

| nivel previsto la víspera | días | con avería | incidencias | horas-cliente (recortadas) | parte | parte sin recortar |
| --- | --- | --- | --- | --- | --- | --- |
| bajo | 1.047 (74 %) | 1,7 % | 19 | 6.316 | 25 % | 17 % |
| medio | 198 (14 %) | 4,5 % | 9 | 4.412 | 18 % | 10 % |
| alto | 86 (6 %) | 9,3 % | 8 | 3.885 | 16 % | 47 % |
| muy alto | 91 (6 %) | 17,6 % | 19 | 10.341 | 41 % | 26 % |

- Un día muy alto tiene avería 10 veces más a menudo que un día bajo (17,6 % frente a 1,7 %).
- Los días en alerta (alto o muy alto) son el 12 % de los días y concentran el 57 % de las horas-cliente perdidas por causas locales (el 73 % sin recortar el incendio).
- De los 3 peores días de cada distribuidora y año de 2024 - 2025 (12 días que suman el 54 % de las horas-cliente), 7 estaban en alerta la víspera; de los 5 peores (20 días, el 72 %), 12. Con el reanálisis, en 2022 - 2025: 11 de 24 y 16 de 40.
- El 74 % de los días son bajos y en ellos cae una de cada cuatro horas-cliente: son las averías de equipos y de causa desconocida que el tiempo no anuncia.

Por distribuidora y año:

| distribuidora | año | tiempo que ve el modelo | días en alerta | incidencias | en días de alerta | horas-cliente | en días de alerta | sin recortar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| EOSA | 2022 | reanálisis | 41 (11 %) | 30 | 13 (43 %) | 13.126 | 4.371 (33 %) | 34 % |
| EOSA | 2023 | reanálisis | 36 (10 %) | 21 | 5 (24 %) | 16.692 | 10.871 (65 %) | 65 % |
| EOSA | 2024 | previsión real | 38 (11 %) | 17 | 8 (47 %) | 7.519 | 3.864 (51 %) | 51 % |
| EOSA | 2025 | previsión real | 54 (15 %) | 18 | 10 (56 %) | 9.595 | 5.323 (55 %) | 82 % |
| Pitarch | 2022 | reanálisis | 50 (14 %) | 13 | 3 (23 %) | 8.100 | 244 (3 %) | 3 % |
| Pitarch | 2023 | reanálisis | 35 (10 %) | 19 | 5 (26 %) | 3.916 | 1.298 (33 %) | 33 % |
| Pitarch | 2024 | previsión real | 44 (13 %) | 11 | 5 (45 %) | 4.555 | 3.288 (72 %) | 72 % |
| Pitarch | 2025 | previsión real | 41 (11 %) | 9 | 4 (44 %) | 3.285 | 1.752 (53 %) | 50 % |

Pitarch en 2022 es el año malo: sus dos peores días (6.300 horas-cliente, averías de equipos en días de calma) salieron bajos. Es el límite que ya conocíamos: el sistema ve el tiempo, no el estado de los equipos.

Los diez peores días de 2024 - 2025 y lo que decía el sistema la tarde anterior:

| día | distribuidora | nivel previsto | probabilidad | incidencias | clientes | horas-cliente (recortadas) | corte más largo (min) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10.06.2024 | EOSA | muy alto | 11,9 % | 1 | 24.335 | 2.035 | 5 |
| 05.11.2025 | EOSA | muy alto | 35,7 % | 2 | 1.971 | 1.861 | 169 |
| 12.10.2024 | Pitarch | muy alto | 11,5 % | 1 | 313 | 1.431 | 75 |
| 04.04.2025 | Pitarch | muy alto | 15,2 % | 1 | 365 | 1.262 | 58 |
| 18.03.2025 | EOSA | alto | 10,2 % | 1 | 1.813 | 1.249 | 41 |
| 28.05.2025 | EOSA | bajo | 2,6 % | 1 | 11.194 | 1.113 | 6 |
| 17.06.2025 | EOSA | medio | 5,5 % | 1 | 1.269 | 1.108 | 77 |
| 13.08.2025 | EOSA | alto | 7,9 % | 1 | 535 | 949 | 1.946 |
| 19.09.2024 | Pitarch | muy alto | 6,9 % | 1 | 6.852 | 867 | 55 |
| 27.03.2024 | EOSA | muy alto | 23,3 % | 1 | 12.308 | 831 | 4 |

La noche del incendio de Jarilla (13.08.2025, el corte más largo de los cuatro años) EOSA estaba en alto desde la tarde anterior.

## 3. Las brigadas: lo que dicen los datos

| distribuidora | brigadas | año | cortes de CT | más largos que la llegada de la brigada | horas-cliente de esos cortes | llegada media con bases fijas (min) | en alerta, con las bases del día (min) | minutos de TIEPI ganados |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| EOSA | 1 | 2024 | 1.263 | 52 (4 %) | 517 (7 %) | 45 | 36 | 0,00 |
| EOSA | 2 | 2024 | 1.263 | 67 (5 %) | 1.950 (25 %) | 25 | 25 | 0,00 |
| EOSA | 1 | 2025 | 755 | 10 (1 %) | 962 (4 %) | 67 | 60 | 0,08 |
| EOSA | 2 | 2025 | 755 | 177 (23 %) | 4.948 (18 %) | 21 | 24 | 0,08 |
| Pitarch | 1 | 2024 | 633 | 15 (2 %) | 50 (1 %) | 47 | 43 | 0,53 |
| Pitarch | 2 | 2024 | 633 | 15 (2 %) | 50 (1 %) | 32 | 27 | 0,53 |
| Pitarch | 1 | 2025 | 319 | 16 (5 %) | 438 (13 %) | 39 | 37 | 0,20 |
| Pitarch | 2 | 2025 | 319 | 16 (5 %) | 438 (13 %) | 39 | 32 | 0,20 |

- Tres de cada cuatro cortes de CT duran menos de 15 minutos: los repone el reenganche o el telemando antes de que ninguna brigada pueda llegar. Solo entre el 1 % y el 5 % de los cortes (el 7 - 25 % de las horas-cliente) duran más que la llegada de la brigada, y son esos los que la brigada puede acortar.
- Cambiar las bases según el riesgo del día apenas mueve la llegada media (de 2 a 7 minutos) y gana menos de un minuto de TIEPI al año: con dos brigadas, la base fija óptima ya cubre bien las dos mitades de cada red. El valor de la víspera no está en mover furgonetas, está en tener gente y material preparados los días que toca, y en la franja horaria en la que toca (el 72 % de las incidencias de los días de alerta empiezan dentro de la franja del pico de racha, y dos de cada tres incidencias empiezan fuera del horario laboral).
- Lo que sí acorta las horas-cliente en estas redes es el telemando: los cortes que esperan a la brigada son pocos pero largos. El ranking anual de CT y cabeceras ([network-analysis.md](network-analysis.md)) dice dónde ponerlo.

## 4. Cuánto vale

Las distribuidoras no facturan al cliente: cobran la retribución regulada que fija la CNMC. La de 2022, publicada en la resolución de la CNMC de 6 de noviembre de 2025, fue de 11.821.045 euros para Eléctrica del Oeste Distribución (R1-035), 10.541.790 para Eléctricas Pitarch Distribución (R1-049) y 206.434 para Distribución de Electricidad Valle de Santa Ana (R1-164). La Circular 8/2025 de la CNMC, que fija la metodología de 2026 a 2031, liga parte de esa retribución a la calidad: su artículo 24 establece un incentivo a la mejora de la calidad de suministro calculado sobre el TIEPI de los años n-4 a n-2 frente a una referencia sectorial, con límites que la CNMC fija al principio de cada periodo, y el artículo 25 da la fórmula; el artículo 15 permite reconocer, con cargo a los costes de distribución, inversiones en proyectos piloto con beneficio cuantificable, incluidos los de adaptación de la red al cambio climático. Con la retribución de EOSA, cada décima de punto son 11.800 euros al año; la licencia del sistema (18.000 euros al año según el plan de negocio) equivale al 0,15 % de su retribución y al 0,17 % de la de Pitarch.

Lo que el sistema aporta a esa cuenta, con los números de arriba:

- Dónde está el TIEPI de causas locales: en EOSA, entre 16 y 35 minutos al año en los CT en estudio (2022 - 2025); en Pitarch, entre 5 y 19. Los días en alerta concentran más de la mitad. Es el TIEPI sobre el que actúan las brigadas y el telemando; el de aguas arriba y el apagón del 28.04.2025 (12.228 horas de CT, más que todas las causas locales de cuatro años juntas) no dependen de la distribuidora.
- La guardia: en lugar de reforzar la guardia todos los fines de semana de invierno, reforzarla los 40 - 55 días de alerta al año, durante la franja de 13 - 14 horas que marca el sistema, con las bases y los municipios que cubre cada una. Son unas 600 - 750 horas de franja al año por distribuidora.
- Las compensaciones a los clientes del Real Decreto 1955/2000 (descuentos cuando se superan los límites individuales de calidad) y el coste reputacional de un corte largo caen sobre los mismos días: los 20 peores días de 2024 - 2025 suman el 72 % de las horas-cliente y 12 estaban en alerta la víspera.

Lo que no podemos afirmar con estos datos: una cifra en euros del incentivo de calidad para cada distribuidora (depende de la referencia sectorial y de los límites que publique la CNMC) ni el ahorro en horas extra, que depende de cómo organiza hoy cada empresa su retén.

## 5. Cómo enseñarlo

- `historial.html` en el cuadro de mando (http://localhost:8086/historial.html): cuatro años en una pantalla por distribuidora, con el selector de previsión real o reanálisis y el recorte del incendio. Al pasar el ratón por un día salen el nivel, la probabilidad, las incidencias, los clientes y las bases propuestas.
- El cuadro de mando de mañana (`index.html`): el mismo producto en vivo, con los rayos del satélite cada 15 minutos.
- Para una reunión: la tabla por niveles (10 veces más averías un día muy alto), los 10 peores días con lo que decía el sistema y la noche del incendio de Jarilla.

## 6. Límites

- Son pocas incidencias (35 al año entre las dos distribuidoras): las partes por año se mueven entre el 30 % y el 70 % y hay que mirar los cuatro años juntos.
- 2022 y 2023 usan el reanálisis como previsión, que no tiene el error de la previsión de la víspera: por ese lado la cota es optimista. Desde 2024 el modelo ve la previsión real y los resultados son los que daría el job.
- La duración de cada corte es la de Calser, que incluye el tiempo de localización y reposición; no sabemos qué parte de cada corte esperó a la brigada, así que tomamos como cortes que dependen de la brigada los que duraron más que su llegada.
- Las bases fijas de referencia son las óptimas por clientes, no las reales de cada empresa; con las reales, la ganancia de mover las bases sería mayor o menor, pero el orden de magnitud (menos de un minuto de TIEPI al año) no cambia.
