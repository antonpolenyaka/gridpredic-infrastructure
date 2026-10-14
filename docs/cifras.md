# GridPredict en cifras: las que van a la memoria y a la presentación

Una página con los números que sostienen el trabajo, de dónde sale cada uno (documento, ejecución) y la frase con la que lo contamos. Las figuras de la memoria salen del cuadro de mando con `experimentos/figuras/capturar.py` (un PNG a doble resolución por bloque de las tres páginas, con los datos de `dashboard/data`). Todas las cifras son de los datos reales de las tres distribuidoras salvo que se diga lo contrario. Fecha de corte de los datos: 02.08.2026 (último día con etiqueta conocida de la copia de Calser). Cuando una cifra cambie con una ejecución nueva, se cambia aquí y en el documento de origen.

## 1. Los datos

| Cifra | Valor | De dónde |
| --- | --- | --- |
| Distribuidoras | 3: EOSA (unos 37.400 clientes y 760 CT), Pitarch (28.700 y 570 CT) y Valle de Santa Ana (730 y 10 CT), en Extremadura | [network-analysis.md](network-analysis.md) 1 y 2 |
| Copias de origen | Calser (interrupciones y cálculo oficial de TIEPI y NIEPI) y TedisNet (SCADA): 61 GB de backups de SQL Server | [dataset_card.md](dataset_card.md) |
| Lakehouse | Spark + Delta Lake + MinIO + Airflow en Docker; capas Landing, Bronze, Silver y Gold | [project-structure.md](project-structure.md), [gold-layer.md](gold-layer.md) |
| Incidencias de CT (más de 180 s) desde 2016 | 563; unas 45 imprevistas al año entre las tres | [causas-incidencias.md](causas-incidencias.md) resumen |
| Qué pesa en el TIEPI (2016 - 2025, sin programadas) | Equipos 22,7 %, meteorología 17,0 %, desconocidas 11,9 %, fauna 7,8 %, aguas arriba 6,5 % | [causas-incidencias.md](causas-incidencias.md) 3 |
| Fuentes públicas integradas | Open-Meteo (reanálisis ERA5 e ICON-EU, previsión y escenarios), AEMET (avisos desde 2018, predicción por municipios, estaciones), EUMETSAT (rayos), INE, Wikidata, OSM, Catastro, PLACSP, IEEX, GBIF, EFFIS, FIRMS, CEMS, GECOS | [network-analysis.md](network-analysis.md) 9, [causas-incidencias.md](causas-incidencias.md) 7 |
| Nuestro TIEPI frente al oficial de la CNMC (2015 - 2017) | Entre un 2 % y un 16 % por debajo, siempre por debajo (interrupciones de salida y abonado que no contamos) | [network-analysis.md](network-analysis.md) 1 |

## 2. Qué predice el sistema y con qué fiabilidad

| Cifra | Valor | De dónde |
| --- | --- | --- |
| Un día muy alto frente a un día bajo (previsión real, 2024 - 2026) | 17,9 % de los días con avería frente a 1,9 %: 9 veces (10 con 2024 - 2025) | [caso-de-negocio.md](caso-de-negocio.md) 2, ejecución `caso_v9_20261014_1351` |
| Días en alerta (alto o muy alto) | 12,5 % de los días; concentran el 72 % de las horas-cliente perdidas por causas locales (70 % sin recortar; 46 % sin el temporal del 28.01.2026) | [caso-de-negocio.md](caso-de-negocio.md) 2 |
| Calibración de la probabilidad | Muy alto: dice 15,5 %, pasa 17,9 %; alto 7,3 % y 7,4 %; bajo 2,7 % y 1,9 % | [caso-de-negocio.md](caso-de-negocio.md) 2 |
| AUC y Brier del día (previsión real) | AUC 0,72 (EOSA 0,76, Pitarch 0,66); Brier 0,036 frente a 0,038 de la tasa base (4,0 %) | [caso-de-negocio.md](caso-de-negocio.md) 2 |
| Los peores días | De los 10 peores de 2024 - 2026 (68 % de las horas-cliente), 7 estaban en alerta la víspera; el 28.01.2026, muy alto en las dos redes | [caso-de-negocio.md](caso-de-negocio.md) 2 |
| Dentro del día | El 84 % de las incidencias de los días de alerta empiezan en la franja horaria prevista; la línea que falla está entre las 3 primeras del ranking el 25 % de las veces (azar 9 %) y entre las 5 primeras el 41 % (azar 14 %) | [caso-de-negocio.md](caso-de-negocio.md) 2 |
| Detección y falsas alarmas por día (previsión real) | En alerta: marca el 12,5 % de los días, detecta el 41 % de los días con incidencia, el 71 % de los de dos o más y el 41 % de los 10 peores de cada año, con un 11 - 12 % de falsas alarmas (Alcalá, Valencia y Albacete: 73 % con 3 %, sobre provincias enteras) | [caso-de-negocio.md](caso-de-negocio.md) 2b |
| Frente a los avisos de AEMET (los mismos días) | Avisos amarillos: 85 días al año, 80 % de las horas-cliente; sistema con los mismos 85 días: 84 %; sistema en alerta: 46 días, 72 %; avisos de viento: 10 días, 58 % (41 % sin recortar); naranjas o rojos: 20 días, 57 % | [caso-de-negocio.md](caso-de-negocio.md) 2b |
| Modelo por horas (1 - 3 horas) | PR-AUC hasta 8 veces la tasa base con avisos; sabe qué días serán malos pero no distingue la hora; la línea alertada queda en un 1 - 3 % de probabilidad | [causas-incidencias.md](causas-incidencias.md) resumen, [ml-training.md](ml-training.md) |
| Lo que más aporta | Los avisos de AEMET de la víspera (suben un 65 % el modelo por horas y casi doblan el del día); la racha frente al clima de cada celda y el índice de Klawa y Ulbrich | [causas-incidencias.md](causas-incidencias.md) 8 |
| Reglas medidas que usa la página en vivo | 62 % de las incidencias por meteorología a menos de 3 h del pico de racha; rayos a menos de 10 km en la hora anterior en el 17 % de las incidencias frente al 1,1 % del control (15 veces) | [causas-incidencias.md](causas-incidencias.md) 8 y 10 |
| Verificación en producción | `verificacion.json`: cada día, pico previsto frente a medido por las estaciones de AEMET, franja, banda de los escenarios y la predicción de AEMET; se acumula desde octubre de 2026 | [operacion-diaria.md](operacion-diaria.md) 4 |

## 3. El valor para una distribuidora

| Cifra | Valor | De dónde |
| --- | --- | --- |
| Retribución regulada (2022) | EOSA 11.821.045 EUR, Pitarch 10.541.790, Valle de Santa Ana 206.434 | [caso-de-negocio.md](caso-de-negocio.md) 4 |
| Incentivo de calidad (Circular 8/2025, 2026 - 2031) | Del -4 % al +4 % de la retribución según el TIEPI de n-4 a n-2 frente a la referencia del sector (1,0648 h en rural concentrada en 2026 - 2028); cada décima de hora son 11.800 EUR al año en EOSA | [caso-de-negocio.md](caso-de-negocio.md) 4, [network-analysis.md](network-analysis.md) 6 |
| Frente al sector (GECOS) | TIEPI imprevisto con su mezcla de zonas: EOSA 0,34 h (2023 - 2025) frente a 0,92 h en Cáceres y 1,51 h en España (2019 - 2021); Pitarch 0,12 h frente a 0,63 h | [network-analysis.md](network-analysis.md) 2 |
| Guardia a igual presupuesto | Reforzar los 46 días en alerta cubre el 72 % de las horas-cliente; los fines de semana de invierno (44 días al año, el 12 % de los días) cubren el 4 %; con los mismos días que los avisos amarillos, el 84 % frente al 80 % de los avisos | [caso-de-negocio.md](caso-de-negocio.md) 2b y `negocio.html` |
| Mover las brigadas según el riesgo | Menos de un minuto de TIEPI al año (0,54 como máximo): el valor está en tener gente y material preparados el día y en la franja que toca, no en mover furgonetas | [caso-de-negocio.md](caso-de-negocio.md) 3 |
| Telemando | El 33 % de los kVA·h imprevistos de EOSA y el 31 % de Pitarch fueron tiempo de maniobra (mediana 11 min por CT); en 8 cabeceras un interruptor telemandado se paga evitando el 25 % o menos | [network-analysis.md](network-analysis.md) 4 |
| Concentración | El 10 % de las cabeceras se lleva el 75 % de los kVA·h imprevistos; ordenadas por los dos años anteriores, su 10 % superior recoge el 60 % del año siguiente | [network-analysis.md](network-analysis.md) 3 |
| Licencia frente a retribución | 18.000 EUR al año: el 0,15 % de la retribución de EOSA y el 0,17 % de la de Pitarch; implantación 20.000 EUR | [caso-de-negocio.md](caso-de-negocio.md) 4 |
| El tiempo manda también en la estadística oficial | TIEPI nacional por fuerza mayor: 0,04 h en 2008, 0,88 h en 2009 (Klaus), 1,06 h en 2010 (Xynthia) | [network-analysis.md](network-analysis.md) 2 |

## 4. La operación

| Cifra | Valor | De dónde |
| --- | --- | --- |
| Cada tarde (20:30) | Previsión de ICON-EU, 40 escenarios de ICON-EU EPS, predicción de AEMET por municipios y avisos; reentrena y puntúa por distribuidora, línea, municipio y CT; franja, brigadas y bases; cuadro de mando, parte de guardia en Excel y aviso al webhook | [operacion-diaria.md](operacion-diaria.md) 1 |
| Durante el día | Rayos del satélite cada 15 min (aviso de tormenta sobre la red una vez por hora), estaciones de AEMET cada hora, columnas "Ahora" y "Próx. 3 h" por línea | [operacion-diaria.md](operacion-diaria.md) 1 y 2 |
| Lo que no hace | No dice qué CT se quedará sin luz; no ve el estado de los equipos (una de cada cuatro horas-cliente cae en días bajos); con unas 25 incidencias al año por distribuidora la cobertura de un año se mueve entre el 30 % y el 70 % | `negocio.html`, [caso-de-negocio.md](caso-de-negocio.md) 5 |

## 5. Lo que probamos y descartamos

| Qué | Resultado | De dónde |
| --- | --- | --- |
| CAPE, polvo sahariano, aves, lluvia frente al clima, contar averías en vez de días, modelos por distribuidora | No mejoran; el límite es el número de casos | [causas-incidencias.md](causas-incidencias.md) resumen |
| Horas por encima de umbrales fijos de racha | No aportan sobre la racha frente al clima y el índice de Klawa y Ulbrich | [causas-incidencias.md](causas-incidencias.md) 8 |
| Humedad del suelo, lluvia acumulada, factor de racha, horas sobre el percentil 90 local | AUC rodante 0,685 sin ellas, de 0,659 a 0,685 con ellas; suelo húmedo frente a seco en los días de más viento 18,0 % frente a 12,2 % (Fisher p = 0,48). Descartadas, con el código en `experimentos/` | [investigacion-continua.md](investigacion-continua.md) 3 |
| Umbral de alerta por estación o por mes | Gana un punto de horas-cliente y ninguna incidencia; el modelo ya está calibrado por estación (verano 3,7 % previsto y 3,4 % real) | [investigacion-continua.md](investigacion-continua.md) 3b |
| Obras públicas como aviso de averías de terceros | Panel de 511 filas (municipio y año): los años con obra no tienen más averías de terceros (razón de tasas 0,76 con las obras licitadas antes del año, intervalo del 90 % de 0,49 a 1,18); la escala de la licitación es demasiado gruesa | [network-analysis.md](network-analysis.md) 10 |
| Datos de los contadores de los clientes | No están en las copias; primera fuente a pedir si el sistema se implanta | [investigacion-continua.md](investigacion-continua.md) 5 |
