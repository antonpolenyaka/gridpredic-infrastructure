# E01. Plan financiero

El modelo mensual que da las tablas de 3.9 de la memoria: supuestos, presupuesto del primer año, cuenta de explotación, balances de los momentos 0 a 3, tesorería del año 1 mes a mes y de los tres años, estructura de financiación, uso de la ronda y sensibilidad. Lleva las decisiones del 19 y del 20.10.2026 sobre el precio, ENISA y el capital.

```
python -I experimentos/plan_financiero/plan_financiero.py              # escribe salida/
python -I experimentos/plan_financiero/plan_financiero.py --sin-salida # solo enseña las cifras
python -m pytest tests/negocio -q
```

Solo usa la biblioteca estándar (openpyxl, si está, para el Excel). Tarda unos segundos.

## Qué comprueba cada vez que se ejecuta

- El balance cuadra en los 37 cierres (momento 0 y meses 1 a 36): activo = patrimonio neto + pasivo, calculado desde las cuentas y sin partida de ajuste. En la extensión a 60 meses, en los 61.
- La tesorería final de cada año es la inicial del siguiente y el resultado de cada año pasa a resultados de ejercicios anteriores.
- En la v11, todos los meses de los 36 y de los 60: la caja no baja de cero y el patrimonio neto con el préstamo participativo no baja de la mitad del capital (art. 363.1.e de la Ley de Sociedades de Capital).
- ENISA no presta más que los fondos propios de las cuentas con las que se pide (las de 2026, el momento 0), y en la v11 no entra dinero de ENISA antes del mes de cobro.
- La v11 queda igual o mejor que la v10 en ingresos y resultado de los años 1 a 5, en tesorería, en el VAN y en el punto muerto en licencias.
- El medio sueldo de los fundadores basta: la rebaja mínima que cumple es del 39,8 % en los meses 1 a 5.
- Con los supuestos de la v10 reproduce al euro las cifras de la memoria v10 (-75.330, -19.173 y 39.147 €; caja de 288.313, 286.906 y 374.377 €; mínimo de 30.625 € en el mes 5).

Si algo no cuadra, el guion se para con un `AssertionError` y no escribe nada. Las pruebas de `tests/negocio` comprueban además que esas paradas saltan: cuando se descuadra la caja, cuando la v11 queda peor que la v10 y cuando, sin el medio sueldo, algún mes incumple.

## Las decisiones

**Precio (decisión 9).** La licencia se paga por el tamaño de la red, como un SCADA: 3.000 € fijos, 150 € por CT del 1 al 100, 15 € del 101 al 300 y 10 € desde el 301, y 0,10 € por señal, con un mínimo de 6.000 € y sin máximo. Cada CT paga el precio de su tramo, como en el IRPF, así que la licencia no da saltos al pasar de un tramo a otro. A partir de 100 CT el precio por CT baja mucho porque el coste de servir a una red apenas crece con sus CT: el modelo, la previsión y el cuadro de mando son los mismos. El mínimo es para las redes muy pequeñas, y no hay máximo. La implantación sigue en 20.000 €.

| Red | CT | Señales | Licencia al año |
| --- | --- | --- | --- |
| Distribuidora 1 | 760 | unas 44.400 (provisional) | 30.040 € |
| Distribuidora 2 | 570 | unas 33.300 (provisional) | 27.030 € |
| Distribuidora 3 | 10 | unas 600 (provisional) | 6.000 € (el mínimo) |
| Red tipo del plan | 150 | 9.000 | 19.650 € |
| Red de 50 CT | 50 | 2.900 | 10.790 € |
| Red de 100 CT | 100 | 6.000 | 18.600 € |

En la Distribuidora 1 la licencia es lo que valen 2,4 minutos de TIEPI de su incentivo de calidad (12.600 € el minuto) y en la Distribuidora 2, 1,8 minutos (14.900 €). El plan cuenta cada cliente como una red tipo de 150 CT y 9.000 señales, más pequeña que la mitad de las 52 mayores del mercado. Si los clientes fueran redes de 100 CT, la licencia bajaría a 18.600 € y el año 3 seguiría en beneficio (45.134 €). La primera guía para el precio fue una regla rápida, el 0,75 % de la retribución con un mínimo de 6.000 €; con los tramos, las redes grandes pagan bastante menos que con esa regla.

Las señales de las tres distribuidoras son provisionales: son los 78.301 tags de TedisNet repartidos por CT, unos 58 por CT. Están en un solo sitio, `REDES_CALIBRACION` en `plan_financiero.py`. Cuando el recuento de `senales_por_distribuidora.sql` esté hecho, hay que:

1. cambiar las señales en `REDES_CALIBRACION` y poner `SENALES_PROVISIONALES = False`;
2. volver a ejecutar el guion;
3. copiar las licencias nuevas de `salida/comparacion.txt` a este README, a `docs/cifras.md` y a `docs/caso-de-negocio.md`.

Como las señales pesan poco en la tarifa (el 15 % de la licencia de la Distribuidora 1), una desviación del 20 % en su recuento mueve la licencia en unos 900 €.

**ENISA (decisión 10).** El préstamo participativo es de 35.000 € y entra en el mes 6 (junio de 2027), no en el momento 0. ENISA pide la sociedad constituida, las cuentas del último ejercicio depositadas en el Registro Mercantil y fondos propios al menos iguales al préstamo (enisa.es, líneas "Startups y pymes" e "Innovación x Clima", consultadas el 19.10.2026). El calendario es este:

1. Constituimos en diciembre de 2026, así que el último ejercicio es el de 2026.
2. En enero lo formulamos, lo aprobamos en junta universal y lo depositamos (arts. 253.1, 178, 164.1 y 279.1 de la Ley de Sociedades de Capital).
3. En febrero (mes 2) pedimos el préstamo. ENISA da 30 días para completar el expediente.
4. Según las asesorías que tramitan estos préstamos, ENISA resuelve de uno a tres meses después de tener el expediente completo.
5. Con la póliza y el desembolso, el dinero llega en el mes 6.

ENISA no publica un plazo de resolución, y eso es lo que más puede mover el mes; la sensibilidad prueba el mes 8. Pedimos 35.000 € y no 38.000 para tener margen en la condición de fondos propios: con las cuentas de 2026 son 38.500 €, 3.500 más que el préstamo. Con el modelo, el importe redondo más alto que cumple es 38.500 €.

**Capital y sueldos (decisión 11).** Sin Neotec. El capital social se queda en 3.000 €: con menos se aplicaría el régimen de las sociedades de menos de 3.000 € (art. 4.2 y 4.3 LSC, redacción de la Ley 18/2022), y con más subiría el umbral de disolución. Los fundadores ponemos 40.000 €, 20.000 cada uno: 3.000 de capital y 37.000 de prima de asunción.

Hasta que llega ENISA cobramos medio sueldo, meses 1 a 5, y renunciamos así a 18.750 € brutos el año 1. Sin esa rebaja:

- el patrimonio quedaría en -1.465 € en el mes 3 y en -14.787 € en el mes 4, por debajo de la mitad del capital (1.500 €);
- la caja quedaría en -6.615 € en el mes 5.

La rebaja tiene que ser de verdad: un sueldo que solo se aplaza se devenga igual y no mejora el patrimonio. Con el medio sueldo, el peor mes es el 4, con una caja de 12.301 € y un patrimonio de 5.688 € frente a un umbral de 1.500. Desde el mes 6, el préstamo participativo cuenta como patrimonio para el art. 363 (art. 20.1.d del Real Decreto-ley 7/1996).

## Supuestos de los años 4 y 5

Solo sirven para comparar los dos planes más allá del año 3 y no van a las tablas de 3.9:

- ocho clientes nuevos al año, al ritmo del año 3;
- dos bajas al año;
- la misma plantilla y los gastos del año 3.

El VAN es el de los flujos libres (explotación más inversiones) de los años 1 a 5 al 10 %, sin valor residual. Es una cifra para comparar los dos planes, no una valoración.

## Salidas (salida/)

| Fichero | Qué tiene |
| --- | --- |
| `tablas_v11.txt` | Las tablas de 3.9 con los supuestos de la v11, en el formato de la memoria (T título, H cabecera, W anchos, R filas), y la de la tarifa |
| `tablas_v10.txt` | Las mismas con los supuestos de la v10, para comparar |
| `comparacion.txt` | Antes y después de las cifras clave, la rebaja mínima, el importe máximo de ENISA y la licencia de cada red |
| `cifras_v11.json` | Las cifras sueltas que cita el texto, con las de cada escenario |
| `mensual_v11.csv` | Ingresos, gastos, cobros, pagos, tesorería y patrimonio neto de cada mes, con el umbral del art. 363 |
| `detalle_v11.xlsx` | El detalle mes a mes, los balances y los supuestos |

## Qué falta

- El recuento de señales de cada distribuidora: `senales_por_distribuidora.sql` contra la copia de TedisNet de las tres redes, en el SQL Server del portátil.
- Confirmar con ENISA (solicitudes@enisa.es) con qué balance mide los fondos propios de una sociedad con un solo ejercicio cerrado. El plan los mide con las cuentas de 2026. Con el avance del ejercicio en curso serían 22.094 € en febrero y 19.123 € en mayo, y la sensibilidad prueba entonces el mínimo de la línea, 25.000 €.
- Aunque no cobren o cobren medio sueldo, los fundadores con control de la sociedad pagan su cuota de autónomos (art. 305.2.b de la Ley General de la Seguridad Social). Con medio sueldo cabe dentro del 30 % de cargas del modelo.
