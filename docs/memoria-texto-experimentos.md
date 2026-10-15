# Texto para la memoria: lo que probamos para mejorar el modelo del día

Borrador del apartado de resultados sobre las pruebas de mejora del modelo del día siguiente (octubre de 2026). Las cifras salen de los cuadernos 3 a 3k de [investigacion-continua.md](investigacion-continua.md) y de la tabla 5 de [revision-literatura.md](revision-literatura.md); cada una tiene su ejecución (`run_id`) en el cuaderno. Escrito para pegarlo en la memoria y retocarlo allí.

---

## Cómo decidimos si una idea entra

Durante las dos últimas semanas de desarrollo dedicamos buena parte del tiempo a intentar mejorar el modelo que cada tarde estima el riesgo del día siguiente. Antes de probar nada fijamos una regla, porque con tan pocos días de avería es muy fácil engañarse: entre 2024 y 2025 hay 58 días en los que alguna de las dos redes grandes tuvo una interrupción de causa local, y con esa muestra una diferencia de un par de centésimas de PR-AUC aparece y desaparece según qué días caigan en cada lado.

La regla fue la misma para todas las pruebas. Cada año lo puntúa un modelo entrenado solo con los años anteriores; la comparación se hace con la previsión real que ICON-EU daba la víspera (la que tendría el jefe de brigadas a las 20:30), no con el reanálisis; y la diferencia entre la variante nueva y la de producción se mide sobre los mismos días, con un bootstrap por fechas de 500 réplicas. Una idea entra si sube la captura de días con avería en el 10 y el 20 % de días de más riesgo en los dos años y el intervalo de la diferencia queda por encima de cero. Hasta ahora ninguna lo ha conseguido.

## De dónde salieron las ideas

Partimos de una revisión de unos 45 trabajos publicados entre 2012 y 2026 sobre predicción de cortes por meteorología: la escuela de Guikema y Quiring en Estados Unidos, el modelo de la Universidad de Connecticut para Eversource, las distribuidoras rurales finlandesas, las curvas de fragilidad británicas y algunos trabajos alemanes de daños por viento. Comprobamos el DOI de cada uno. De ahí salieron cinco pruebas por orden de ganancia esperada frente a esfuerzo y una lista de fuentes públicas que todavía no usábamos. Al repasar la lista vimos que varias ya estaban en el lakehouse desde septiembre (los registros de cigüeña de GBIF, las áreas quemadas de EFFIS, los focos de FIRMS, el índice de incendios FWI, el relieve del DEM de Copernicus y la exposición de OpenStreetMap), así que nos centramos en las que faltaban.

## Lo que medimos

La variante de producción da 0,188 de PR-AUC con la previsión real, 6,9 veces la tasa base, y recoge el 53 % de los días con avería en el 10 % de días que marca como más arriesgados y el 67 % en el 20 %. Frente a ella:

| prueba | qué añade | PR-AUC con la previsión real |
| --- | --- | --- |
| extensión del viento fuerte | la parte de la red bajo racha extrema (Láng-Ritter y otros, 2025) | 0,178 |
| interacciones | la racha multiplicada por la lluvia previa, la del día, la hoja y el verano (Manning y otros, 2025) | 0,187 |
| pendiente por red | una sensibilidad a la racha propia de cada distribuidora | 0,179 |
| dirección del viento | lo lejos que sopla de la dirección habitual de los temporales (con el reanálisis, como cota) | 0,186 |
| modelo de conteos | una regresión de Poisson sobre el número de incidencias del día | 0,127 |
| logística equilibrada | más peso a los días con avería | 0,158 |
| sequía | el balance de lluvia y evapotranspiración de 90 y 180 días | 0,178 |
| precipitación helada | horas con lluvia a 0,5 grados o menos | 0,179 |
| segundo modelo de previsión | GFS o GEM en lugar de ICON-EU, o la media de los tres | 0,172 - 0,178 |
| verdor por satélite | el NDVI de MODIS de la red y su anomalía | 0,172 (0,179 por cuantiles, igual que producción) |

Ninguna diferencia queda fuera del ruido a favor de la variante nueva; el modelo de conteos y la logística equilibrada quedan claramente por debajo, con el intervalo entero bajo cero.

En el modelo anual por CT probamos además el arbolado de cada celda, con dos mapas públicos que no piden registro: la cubierta del suelo de ESA WorldCover y la altura del dosel de Meta y WRI a un metro. Separan muy bien el norte de Cáceres (en el Jerte y el Ambroz entre el 10 y el 26 % de la celda tiene árboles de 8 m o más) de la penillanura (casi nada). Por CT la captura del 10 % de más riesgo sube del 27,5 % al 30,3 %, pero esa subida la hace un solo año, 2025, el del temporal de enero en el norte; por cabecera no cambia nada. Lo tratamos como un indicio.

## Qué sacamos en limpio

La lectura que hacemos es sencilla. Con dos redes rurales y unas 25 averías al año en cada una, el modelo pequeño (una regresión logística con la racha frente al clima de cada celda, la lluvia, el calendario, la historia reciente y los avisos de AEMET) ya recoge lo que los datos permiten distinguir. Los trabajos de los que salieron las ideas tienen miles de faltas por temporal o redes de decenas de miles de kilómetros; allí una interacción entre racha y lluvia se puede estimar, aquí se confunde con el efecto de cada variable por separado.

Hay dos casos que nos parecen útiles para quien siga el trabajo. El primero es el de los modelos de previsión: ICON-EU, a 7 km, es el mejor de los que el archivo de Open-Meteo guarda con racha prevista, y promediarlo con dos globales más bastos empeora un poco. De paso quedó medido lo que cuesta el respaldo que ya tiene la ejecución de la tarde cuando ICON-EU no responde: con GFS se pierde un punto de captura. El segundo es el arbolado. El grano con el que lo medimos, la celda meteorológica de unos 10 km donde cae el municipio del CT, es demasiado grueso; para probarlo de verdad hace falta el trazado de las líneas, y es lo primero que pediríamos a una distribuidora en un piloto.

Cuando cargamos los primeros siete meses de 2026 repetimos todas las pruebas con la previsión real de 2024 a 2026, 84 días con avería en lugar de 58 y el temporal de enero dentro. El orden no se movió: la variante de producción se queda en 0,161 de PR-AUC (5,3 veces la tasa base, el 46 % de los días con avería en el 10 % de días de más riesgo y el 64 % en el 20 %), las demás entre 0,150 y 0,158, y los intervalos de las diferencias se estrecharon alrededor de cero o por debajo. 2026 está siendo un año más difícil para el modelo, y eso baja todas las cifras por igual.

Todo lo probado sigue en el código, con sus tests, para volver a medirlo cuando haya dos temporadas más de datos o una red mayor.

## Con más antelación

La misma variante, puntuada con lo que se sabía dos y tres tardes antes (la previsión de ICON-EU de esos días, los avisos de AEMET ya emitidos y el último día de tiempo conocido), da 5,4 y 5,2 veces la tasa base. Con el 20 % de días marcados recoge el 66 % y el 64 % de los días con avería, casi lo mismo que la víspera (67 %). Con el 10 % la caída es mayor: del 53 % al 43 % y al 40 %. Con 2026 dentro las cifras son parecidas (4,8 y 4,4 veces la tasa base; 63 % y 62 % de los días con avería en el 20 % de días, frente al 64 % de la víspera). Entre dos y tres días no hay diferencia que se pueda medir con dos años de datos. Casi toda la pérdida desde la víspera viene de los avisos de AEMET que todavía no se han emitido, porque la racha que ICON-EU prevé a tres días vale casi lo mismo que la de dos. Para una distribuidora esto quiere decir que el riesgo a dos o tres días sirve para planificar (avisar a una contrata, mover un vehículo entre bases), mientras que la decisión fina de reforzar la guardia sigue tomándose la víspera.
