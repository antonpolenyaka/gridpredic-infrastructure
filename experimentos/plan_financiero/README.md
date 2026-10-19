# E01. Plan financiero

El modelo mensual que da las tablas de 3.9 de la memoria (cuenta de explotación, balances de los momentos 0 a 3, tesorería del año 1 mes a mes y de los tres años, estructura de financiación y sensibilidad), con las tres decisiones del 19.10.2026 sobre el precio, ENISA y el capital.

```
python -I experimentos/plan_financiero/plan_financiero.py              # escribe salida/
python -I experimentos/plan_financiero/plan_financiero.py --sin-salida # solo enseña las cifras
python -m pytest tests/negocio -q
```

Solo usa la biblioteca estándar (openpyxl, si está, para el Excel). Tarda menos de un segundo.

## Qué comprueba cada vez que se ejecuta

- El balance cuadra en los 37 cierres (momento 0 y meses 1 a 36): activo = patrimonio neto + pasivo, calculado desde las cuentas y sin partida de ajuste. En la extensión a 60 meses, en los 61.
- La tesorería final de cada año es la inicial del siguiente y el resultado de cada año pasa a resultados de ejercicios anteriores.
- ENISA no presta más que los fondos propios del balance con el que se pide (el de las cuentas de 2026, el momento 0).
- En la v11, la caja y el patrimonio neto con el préstamo participativo quedan por encima de cero y de la mitad del capital (art. 363.1.e LSC) todos los meses.
- La v11 queda igual o mejor que la v10 en ingresos y resultado de los años 1 a 5, en tesorería, en el VAN y en el punto muerto en licencias.
- Con los supuestos de la v10 reproduce al euro las cifras de la memoria v10 (-75.330, -19.173 y 39.147 €; caja 288.313, 286.906 y 374.377 €; mínimo de 30.625 € en el mes 5).

Si algo no cuadra, el guion se para con un `AssertionError` y no escribe nada; las pruebas de `tests/negocio` comprueban además que esa parada salta cuando se descuadra la caja.

## Las tres decisiones

**Precio (decisión 9).** La licencia se cobra como un SCADA, por el tamaño de la red: 3.000 € fijos, 50 € por CT y 1 € por señal, con un mínimo de 6.000 € y sin máximo. La implantación sigue en 20.000 €.

| Red | CT | Señales | Licencia al año |
| --- | --- | --- | --- |
| Distribuidora 1 | 760 | unos 44.400 (provisional) | 85.400 € |
| Distribuidora 2 | 570 | unos 33.300 (provisional) | 64.800 € |
| Distribuidora 3 | 10 | unos 600 (provisional) | 6.000 € (el mínimo) |
| Red tipo del plan | 150 | 9.000 | 19.500 € |

Calibración: la parte por CT y por señal está puesta para que las tres redes del TFM den cifras del mismo orden que la regla rápida de NE04-M3 (0,75 % de la retribución, suelo de 6.000 €), que es la que usamos para el resto del mercado mientras no tengamos los CT y las señales de cada distribuidora; las dos coinciden en unos 18.000 € para una red de unos 140 CT. El plan cuenta cada cliente como una red tipo de 150 CT y 9.000 señales, más pequeña que la mitad de las 52 mayores del mercado, y con eso queda algo mejor que el de la v10. Si la red media fuera de 100 CT, la licencia sería de 14.000 € y el año 3 cerraría en -673 € (fila de la sensibilidad).

Las señales son provisionales: son los 78.301 tags de TedisNet repartidos por CT (unos 58 por CT). El recuento por distribuidora sale de `senales_por_distribuidora.sql`; cuando lo tengamos, se cambian en `REDES_CALIBRACION` y se vuelve a ejecutar.

**ENISA (decisión 10).** El préstamo participativo de 38.000 € entra en el mes 6 (junio de 2027), no en el momento 0. ENISA pide la sociedad constituida, las cuentas del último ejercicio depositadas en el Registro Mercantil y fondos propios al menos iguales al préstamo (enisa.es, líneas "Startups y pymes" e "Innovación x Clima", consultadas el 19.10.2026). Constituimos en diciembre de 2026, así que el último ejercicio es el de 2026: lo formulamos, lo aprobamos en junta universal y lo depositamos en enero (arts. 253.1, 178, 164.1 y 279.1 de la Ley de Sociedades de Capital), pedimos el préstamo en febrero (mes 2), ENISA da 30 días para completar el expediente y, según las asesorías que tramitan estos préstamos, resuelve de uno a tres meses después de tenerlo completo; con la póliza y el desembolso, el dinero llega en el mes 6. ENISA no publica un plazo de resolución, y eso es lo que más puede mover el mes (la sensibilidad lo prueba con el mes 8).

**Capital (decisión 11).** Sin Neotec. El capital social se queda en 3.000 €: con menos se aplicaría el régimen de las sociedades de menos de 3.000 € (art. 4.2 y 4.3 LSC, redacción de la Ley 18/2022), y con más subiría el umbral de disolución. Lo que hace falta es más aportación: sin ENISA hasta el mes 6, las pérdidas de los primeros meses dejarían el patrimonio neto por debajo de la mitad del capital (1.500 €) desde el mes 3 y la caja en negativo en los meses 4 y 5. La aportación mínima que lo evita es de 56.287 € (16.287 € más que los 40.000 de la v10, por el patrimonio del mes 4); ponemos 60.000 €, 30.000 cada fundador, como 3.000 de capital y 57.000 de prima de asunción. Con eso el peor mes es el 4, con 5.213 € de patrimonio frente a un umbral de 1.500, y la caja más baja, 13.385 € en el mes 5.

## Supuestos de los años 4 y 5

Solo para comparar los dos planes más allá del año 3 (no van a las tablas de 3.9): ocho clientes nuevos al año al ritmo del año 3, dos bajas al año, la misma plantilla y los gastos del año 3. El VAN es el de los flujos libres (explotación más inversiones) de los años 1 a 5 al 10 %, sin valor residual; es una cifra para comparar los dos planes, no una valoración.

## Salidas (salida/)

| Fichero | Qué tiene |
| --- | --- |
| `tablas_v11.txt` | Las tablas de 3.9 con los supuestos de la v11, en el formato de la memoria (T título, H cabecera, W anchos, R filas) |
| `tablas_v10.txt` | Las mismas con los supuestos de la v10, para comparar |
| `comparacion.txt` | Antes y después de las cifras clave y el cálculo de la tarifa |
| `cifras_v11.json` | Las cifras sueltas que cita el texto |
| `mensual_v11.csv` | Ingresos, gastos, cobros, pagos, tesorería y patrimonio neto de cada mes, con el umbral del art. 363 |
| `detalle_v11.xlsx` | El detalle mes a mes, los balances y los supuestos |

## Qué falta

- El recuento de señales de cada distribuidora (`senales_por_distribuidora.sql` contra la copia de TedisNet de las tres redes, en el SQL Server del portátil).
- Confirmar con ENISA (solicitudes@enisa.es) con qué balance mide los fondos propios de una sociedad con un solo ejercicio cerrado: el plan lo mide con el de las cuentas de 2026 (58.500 € frente a 38.000). Si lo midiera con el avance del mes de la resolución, el préstamo tendría que bajar; la sensibilidad prueba el mínimo de la línea, 25.000 €.
