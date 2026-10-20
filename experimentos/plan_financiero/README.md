# E01. Plan financiero

El modelo mensual que da las tablas de 3.9 de la memoria: supuestos, presupuesto del primer año, cuenta de explotación, balances de los momentos 0 a 3, tesorería del año 1 mes a mes y de los tres años, estructura de financiación, uso de la ronda y sensibilidad. Lleva las decisiones del 19 y del 20.10.2026 sobre el precio, ENISA y el capital.

```
python -I experimentos/plan_financiero/plan_financiero.py              # escribe salida/
python -I experimentos/plan_financiero/plan_financiero.py --sin-salida # solo enseña las cifras
python -I experimentos/plan_financiero/mercado.py                     # necesita privado/mercado.json
python -m pytest tests/negocio -q
```

Solo usa la biblioteca estándar (openpyxl, si está, para el Excel). Tarda unos segundos.

## Qué comprueba cada vez que se ejecuta

- El balance cuadra en los 37 cierres (momento 0 y meses 1 a 36): activo = patrimonio neto + pasivo, calculado desde las cuentas, sin partida de ajuste y sumando otra vez las partidas una a una. En la extensión a 60 meses, en los 61.
- La tesorería final de cada año es la inicial del siguiente y el resultado de cada año pasa a resultados de ejercicios anteriores.
- En la v11, todos los meses de los 36 y de los 60: la caja no baja de cero y el patrimonio neto con el préstamo participativo no baja de la mitad del capital (art. 363.1.e de la Ley de Sociedades de Capital).
- ENISA no presta más que los fondos propios de las cuentas con las que se pide (las de 2026, el momento 0), y en la v11 no entra dinero de ENISA antes del mes de cobro.
- La v11 queda igual o mejor que la v10 en ingresos y resultado de los años 1 a 5, en la caja al final de los años 3 y 5, en el VAN y en el punto muerto en licencias. La caja del momento 0, la caja más baja y el margen sobre el umbral del art. 363 bajan, porque ENISA pasa del momento 0 al mes 6.
- El medio sueldo de los fundadores basta: la rebaja mínima que cumple es del 39,8 % en los meses 1 a 5.
- Con los supuestos de la v10 reproduce al euro las cifras de la memoria v10 (-75.330, -19.173 y 39.147 €; caja de 288.313, 286.906 y 374.377 €; mínimo de 30.625 € en el mes 5).

Si algo no cuadra, el guion se para con un `AssertionError` y no escribe nada. Las pruebas de `tests/negocio` comprueban además que esas paradas saltan: cuando se descuadra la caja, cuando el activo deja de ser igual al patrimonio neto más el pasivo, cuando la v11 queda peor que la v10 y cuando, sin el medio sueldo, algún mes incumple. Y comprueban que `salida/tablas_v11.txt` y `salida/comparacion.txt` están al día con el código (el Excel, el JSON y el CSV no, porque cambian en el cuarto decimal entre versiones de Python), y que `salida/mercado.json` solo tiene agregados y está al día cuando el fichero privado está.

## Las decisiones

**Precio (decisión 9).** La licencia se paga por el tamaño de la red, como un SCADA: 3.000 € fijos, 150 € por CT del 1 al 100, 15 € del 101 al 300 y 10 € desde el 301, y 0,10 € por señal, con un mínimo de 6.000 € y sin máximo. Cada CT paga el precio de su tramo, como en el IRPF, así que la licencia no da saltos al pasar de un tramo a otro. A partir de 100 CT el precio por CT baja mucho porque el coste de servir a una red apenas crece con sus CT: el modelo, la previsión y el cuadro de mando son los mismos. El mínimo es para las redes muy pequeñas, y no hay máximo. La implantación sigue en 20.000 €.

| Red | CT | Señales | Licencia al año |
| --- | --- | --- | --- |
| Distribuidora 1 | 796 | 34.647 | 29.425 € |
| Distribuidora 2 | 578 | 27.513 | 26.531 € |
| Distribuidora 3 | 10 | 606 | 6.000 € (el mínimo) |
| Red tipo del plan | 150 | 9.000 | 19.650 € |
| Red de 50 CT | 50 | 2.900 | 10.790 € |
| Red de 100 CT | 100 | 6.000 | 18.600 € |

En la Distribuidora 1 la licencia es lo que valen 2,3 minutos de TIEPI de su incentivo de calidad (12.600 € el minuto) y en la Distribuidora 2, 1,8 minutos (14.900 €). El plan cuenta cada cliente como una red tipo de 150 CT y 9.000 señales, cerca del centro de las 89 distribuidoras más grandes: estimando sus CT por su retribución, 39 tienen 150 CT o más y 62 tienen 100 CT o más. Si los clientes fueran redes de 100 CT, la licencia bajaría a 18.600 € y el año 3 seguiría en beneficio (45.134 €). La primera guía para el precio fue una regla rápida, el 0,75 % de la retribución con un mínimo de 6.000 €; con los tramos, las redes grandes pagan bastante menos que con esa regla.

Los CT y las señales de las tres distribuidoras salen de `senales_por_distribuidora.sql` sobre la copia de TedisNet de las tres redes (20.10.2026). Los CT son los elementos de tipo TRAFO CT de cada una, que pueden diferir algo de los CT de Calser (unos 760, 570 y 10), y las señales, los tags cuyo elemento cuelga de la distribuidora; los 14.705 tags sin elemento no se reparten. Están en un solo sitio, `REDES_CALIBRACION` en `plan_financiero.py`: si cambian, se vuelve a ejecutar el guion y se copian las licencias nuevas de `salida/comparacion.txt` a este README, a `docs/cifras.md` y a `docs/caso-de-negocio.md`.

**Ingresos de la implantación.** La implantación se reconoce entera como ingreso en la puesta en marcha, aunque la mitad se facture a la firma; esa mitad queda mientras tanto como anticipo. La tratamos como una obligación distinta de la licencia (instalación, integración con su SCADA, calibración con su histórico e informe) que se cumple al ponerla en marcha, según la Resolución del ICAC de 10.02.2021 sobre el reconocimiento de ingresos (BOE del 13.02.2021; su artículo 8 fija cuándo se cumple cada obligación). El criterio pesa en el art. 363: si la implantación se repartiera a lo largo de tres años con la licencia, el patrimonio del mes 5 bajaría de 19.123 € a -322 €, por debajo del umbral. Lo confirmaremos con la gestoría.

**ENISA (decisión 10).** El préstamo participativo es de 35.000 € y entra en el mes 6 (junio de 2027), no en el momento 0. ENISA pide la sociedad constituida, las cuentas del último ejercicio depositadas en el Registro Mercantil y fondos propios al menos iguales al préstamo (enisa.es, líneas "Startups y pymes" e "Innovación x Clima", consultadas el 19.10.2026). El calendario es este:

1. Constituimos en diciembre de 2026, así que el último ejercicio es el de 2026.
2. En enero lo formulamos, lo aprobamos en junta universal y lo depositamos (arts. 253.1, 178, 164.1 y 279.1 de la Ley de Sociedades de Capital).
3. En febrero (mes 2) pedimos el préstamo. ENISA da 30 días para completar el expediente.
4. Según las asesorías que tramitan estos préstamos, ENISA resuelve de uno a tres meses después de tener el expediente completo.
5. Con la póliza y el desembolso, el dinero llega en el mes 6.

ENISA no publica un plazo de resolución, y eso es lo que más puede mover el mes; la sensibilidad prueba el mes 8. Pedimos 35.000 € y no 38.000 para tener margen en la condición de fondos propios: con las cuentas de 2026 son 38.500 €, 3.500 más que el préstamo. Con el modelo, el importe redondo más alto que cumple es 38.500 €. Si ENISA denegara el préstamo, el plan seguiría cumpliendo: la caja más baja sigue en 12.301 € (mes 4) y el patrimonio más bajo queda en 2.118 € en el mes 11, 618 € por encima del umbral.

**Capital y sueldos (decisión 11).** Sin Neotec. El capital social se queda en 3.000 €: con menos se aplicaría el régimen de las sociedades de menos de 3.000 € (art. 4.1 LSC, redacción de la Ley 18/2022), y con más subiría el umbral de disolución. Los fundadores ponemos 40.000 €, 20.000 cada uno: 3.000 de capital y 37.000 de prima de asunción.

De enero a mayo de 2027 (meses 1 a 5), hasta que llega ENISA en junio, cobramos medio sueldo, y renunciamos así a 18.750 € brutos el año 1. Sin esa rebaja:

- el patrimonio quedaría en -1.465 € en el mes 3 y en -14.787 € en el mes 4, por debajo de la mitad del capital (1.500 €);
- la caja quedaría en -6.615 € en el mes 5.

La rebaja tiene que ser de verdad: un sueldo que solo se aplaza se devenga igual y no mejora el patrimonio. Con el medio sueldo, el peor mes es el 4, con una caja de 12.301 € y un patrimonio de 5.688 € frente a un umbral de 1.500. Desde el mes 6, el préstamo participativo cuenta como patrimonio para el art. 363 (art. 20.1.d del Real Decreto-ley 7/1996).

## Escenarios y contrataciones

En la v11 las dos contrataciones esperan a las firmas: el ingeniero entra con la tercera puesta en marcha y el comercial con la novena, nunca antes de los meses 13 y 25 (`contrataciones_ligadas`). En el plan no cambia nada. En los escenarios cambia lo siguiente:

- Seis meses de retraso: el ingeniero entra en el mes 15 y el comercial en el 30.
- Un año de retraso: entran en los meses 21 y 36.
- Pesimista (dos clientes nuevos menos en los años 2 y 3): el comercial entra en el mes 27, porque la novena puesta en marcha llega entonces.
- Optimista (dos clientes nuevos más en los años 2 y 3, en los meses que quita el pesimista): mismo ingeniero, como en la decisión 12.

La memoria dice también que el ingeniero entra "con la ronda en la cuenta". Como la ronda entra en el mes 12, eso no cambia nada en el modelo, y el escenario sin ronda no lo aplica.

## Lo que cita la memoria además de las tablas de 3.9

Todo sale de `salida/tablas_v11.txt` y `salida/cifras_v11.json`:

- la cuenta en cascada (coste de las ventas = nube, comisión del socio y sueldo y cargas del ingeniero, y margen bruto);
- el punto muerto con la fórmula del curso (costes fijos, margen por licencia, ventas de equilibrio con la mezcla del año, margen de seguridad y peso de los costes fijos);
- el fondo de maniobra y las necesidades operativas de fondos, con el peor mes antes de la ronda;
- el valor actual para el cliente al 6,58 % de la CNMC, con los minutos de TIEPI que lo compensan;
- la cuenta del inversor y lo que sumaría una subida de la licencia del 3 % en cada aniversario.

Las cifras de mercado (el 4 % de la retribución mediana de las 92 de más de un millón, y cuántas de las 89 mayores tienen 150 y 100 CT) salen de `mercado.py`. Ese guion lee la resolución del BOE y un fichero privado que no se versiona, `privado/mercado.json`, con los códigos de la Distribuidora 1 y la 2. Su formato está en la cabecera del guion. En `salida/mercado.txt` y `salida/mercado.json` deja solo sumas redondeadas, recuentos y una mediana redondeada.

## Supuestos de los años 4 y 5

Solo sirven para comparar los dos planes más allá del año 3 y no van a las tablas de 3.9:

- ocho clientes nuevos al año, al ritmo del año 3;
- dos bajas al año;
- la misma plantilla y los gastos del año 3.

El VAN es el de los flujos libres (explotación más inversiones) de los años 1 a 5 al 10 %, sin valor residual. Es una cifra para comparar los dos planes, no una valoración.

## Simplificaciones conocidas

Vienen de la v10, no cambian el modelo y las dejamos escritas con su efecto:

- El IVA soportado del inmovilizado del momento 0 y de los gastos de constitución no está en la tesorería. Pagarlo y recuperarlo después, al compensarlo con el IVA de las primeras facturas, quitaría unos 2.900 € de caja en los meses 1 a 6: la caja más baja bajaría de 12.301 € a unos 9.400 €. El patrimonio no cambia.
- Las cargas sociales son el 30 % del bruto para todos. Para los dos empleados (el ingeniero y el responsable comercial), el tipo empresarial del Régimen General de 2026 es unos 1,6 puntos mayor: unos 1.100 € más de gasto el año 3.

## Salidas (salida/)

| Fichero | Qué tiene |
| --- | --- |
| `tablas_v11.txt` | Las tablas de 3.9 con los supuestos de la v11, en el formato de la memoria (T título, H cabecera, W anchos, R filas), y la de la tarifa |
| `tablas_v10.txt` | Las mismas con los supuestos de la v10, para comparar |
| `comparacion.txt` | Antes y después de las cifras clave, la rebaja mínima, el importe máximo de ENISA y la licencia de cada red |
| `cifras_v11.json` | Las cifras sueltas que cita el texto, con las de cada escenario |
| `mensual_v11.csv` | Ingresos, gastos, cobros, pagos, tesorería y patrimonio neto de cada mes, con el umbral del art. 363 |
| `detalle_v11.xlsx` | El detalle mes a mes, los balances y los supuestos |
| `mercado.txt`, `mercado.json` | Agregados del mercado con la tarifa (los escribe `mercado.py`) |

## Qué falta

- El recuento de señales de cada distribuidora: `senales_por_distribuidora.sql` contra la copia de TedisNet de las tres redes, en el SQL Server del portátil.
- Con qué balance mide ENISA los fondos propios. ENISA no lo publica. Suponemos, como dicen las guías de las asesorías que tramitan estos préstamos (IDR Consulting e Intelectium, consultadas el 20.10.2026), que los comprueba con las últimas cuentas depositadas más las ampliaciones de capital escrituradas después del cierre, y que si no llegan puede aprobar el préstamo con la condición de ampliar capital en uno o dos meses. Es un supuesto, no una regla publicada. Con las cuentas de 2026 son 38.500 €. Si mirase el avance del ejercicio en curso (22.094 € en febrero y 19.123 € en mayo), bajar el préstamo no bastaría, porque el mínimo de la línea es de 25.000 €: habría que ampliar capital entre 13.000 y 16.000 € antes de firmar. Lo confirmaremos con ENISA (solicitudes@enisa.es).
- Aunque no cobren o cobren medio sueldo, los fundadores con control de la sociedad pagan su cuota de autónomos (art. 305.2.b de la Ley General de la Seguridad Social). Con medio sueldo cabe dentro del 30 % de cargas del modelo.
