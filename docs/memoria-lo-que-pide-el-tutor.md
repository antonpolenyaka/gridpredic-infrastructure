# La memoria del TFM: lo que pide el tutor de negocio y cómo lo cubrimos

Revisión de las dos tutorías con Joaquim Deulofeu (06.07.2026 y 14.10.2026) frente a lo que ya tenemos hecho, con los textos que faltan listos para llevar a la memoria (entrega del 26.10.2026) y a la defensa del 05.11.2026. Lo que está entre corchetes son cifras o decisiones que tenemos que cerrar nosotros.

## 1. Lo que dijo el tutor, en una tabla

| Petición | Tutoría | Lo que ya tenemos | Lo que falta | Dónde va |
| --- | --- | --- | --- | --- |
| Explicar el producto final, no los vaivenes técnicos; el tribunal mira la tecnología, pero el documento de negocio tiene que leerse como lo leería un inversor, que piensa como cliente | 14.10 | El sistema en producción (riesgo de mañana por distribuidora, línea, municipio y CT; franja; brigadas; guardia; pasado mañana, en producción desde el 15.10.2026; verificación diaria) y las cifras ([cifras.md](cifras.md)) | Reescribir la introducción y la propuesta de valor "como si lo viejo no hubiera existido"; dejar el cambio de horizonte como una decisión de diseño, no como un fracaso | Capítulos 2 y 3 |
| La idea de negocio no cambia: mismo cliente, misma necesidad; el título del proyecto sigue valiendo | 14.10 | Título y cliente objetivo (distribuidoras pequeñas y medianas) | Nada | Portada, 2 |
| Cuenta de explotación a 3 años: faltan los años 2 y 3 | 06.07 y 14.10 | Año 1 (87.000 ingresos, 141.000 gastos) y las cifras del Pitch v3 para los años 2 y 3 | Las dos columnas que faltan, con sus supuestos, y la coherencia con el plan de tesorería | 3.x Plan financiero |
| Balance inicial y balances a 3 años (momento 0, 1, 2 y 3) | 06.07 y 14.10 | Balance "Momento 0" (36.000 euros, 19.06.2026) | Esbozarlo en el documento (hoy solo hay información suelta) y los balances de cierre de cada año | 3.x Plan financiero |
| Plan de tesorería a 3 años, y en especial los primeros 6 meses (ciclo de venta de 6 a 18 meses) | 06.07 | Nada formal | El plan mes a mes del primer año y anual después, con el saldo final de un año como inicial del siguiente | 3.x Plan financiero |
| Estructura de financiación: 20/80 (propio/ICO) es demasiado riesgo; ir hacia 40/60 o ronda | 06.07 | Ronda de 250.000 euros por el 25 % en el Pitch v3 | Decidir el mix y explicarlo | 3.x Plan financiero |
| Marca y posicionamiento: decir algo de la marca (que cumple lo que debe cumplir una marca) y una declaración de posicionamiento | 06.07 y 14.10 | El nombre GridPredic y el dominio GridPredic.com | La sección entera | 3.x Mercado, competencia y posicionamiento |
| Marketing mix (las 4 P): producto, precio, promoción, distribución, sacados del Canvas | 06.07 y 14.10 | Precio (20.000 de implantación, 18.000 al año), canal (SITEL, 52 distribuidoras), promoción (ferias y asociaciones) repartidos por el documento | La sección entera, en un solo sitio | 3.x Marketing mix |
| DAFO actualizado al horizonte nuevo (uno a tres días) y a las fuentes nuevas, con la priorización por fases (primero defensiva, después agresiva) | 06.07 y 14.10 | DAFO del M2 | Revisarlo y añadir la priorización | 3.x DAFO |
| Propuesta de valor y riesgos actualizados | 14.10 | Propuesta de valor del M2 (1 a 3 horas por CT) | Reescribirla con lo que hace hoy el sistema | 3.x Propuesta de valor |
| Cuadro de gestión de riesgos (alto, medio, bajo) | 06.07 y 14.10 | Nada | La tabla | 3.x Gestión de riesgos |
| Visión a 2 o 3 años con el posicionamiento deseado (referente, ámbito geográfico), propósito separado de la misión, valores como comportamientos, la cultura como palanca | 06.07 | Misión del M2 | La visión, los valores y la separación propósito y misión | 3.1 |
| Plan de validación con históricos de las distribuidoras: horas de interrupción en días y zonas de riesgo alto, brigadas movilizadas sin necesidad, ahorro | 14.10 | Medido: cobertura de horas-cliente, falsas alarmas por día, llegada de brigadas, verificación diaria ([caso-de-negocio.md](caso-de-negocio.md)) | Presentarlo como plan de validación con KPIs y como argumento de venta | 3.x Validación y 5 |
| Demo gratuita de un mes y entrada por el partner que ya está instalado en las distribuidoras | 14.10 | La propuesta de SITEL como socio | Escribirlo como estrategia comercial con cifras (328 distribuidoras, 52 ya clientes del partner, el 25 % del mercado) | 3.x Go-to-market |
| Argumento económico: lo que cuesta un minuto de corte y lo que se gana situando la brigada en el punto medio (35 minutos frente a 90) | 14.10 | La llegada media de las brigadas por día en el cuadro de mando; el valor de un minuto de TIEPI en el incentivo (12.600 euros en la Distribuidora 1, 14.900 en la Distribuidora 2) y para los clientes (10.600 euros al valor de carga perdida oficial) | Escribirlo con las dos cifras separadas (apartado 3.7) | 3.x Propuesta de valor |
| Pitch orientado a inversor para el 05.11 | 14.10 | [guion-presentacion.md](guion-presentacion.md) (14 diapositivas) | Añadir la diapositiva de negocio (mercado, modelo, cuenta de explotación, ronda) | Defensa |
| Clientes conservadores: ir con seguridad, con la prueba del ahorro y con paciencia; convertir cada "no" en una pregunta | 14.10 | Nada escrito | Un argumentario corto en el documento (apartado 3.9) | 3.x Go-to-market |
| Patente o protección de la idea | 06.07 | Marca en OEPM/EUIPO en el M2 | Una línea sobre qué se protege (marca, código y modelo; una patente de software en España es difícil) | 3.x Marco legal |

## 2. Lo que la tecnología ya le da al documento de negocio

Todo esto está hecho, medido y en producción, y es lo que el tutor quiere ver como "producto final":

- Cada tarde a las 20:30, el riesgo del día siguiente por distribuidora con nivel (bajo, medio, alto, muy alto), probabilidad calibrada, franja horaria, líneas, municipios, CT, bases de las brigadas y recomendación de guardia; durante el día, rayos, estaciones y avisos de AEMET releídos cada hora; por la mañana, el parte. Con dos tardes de antelación, la orientación de pasado mañana, en producción desde el 15.10.2026 (conserva la mayor parte de la señal; [investigacion-continua.md](investigacion-continua.md), 3e).
- Lo que vale, medido sobre 2022 - 2026 como si el sistema hubiera estado en marcha: un día muy alto tiene avería 9 veces más que uno bajo; el 72 % de las horas-cliente perdidas caen en el 12,5 % de días en alerta; 7 de los 10 peores días estaban avisados la víspera; la política recomendada (alerta o aviso de viento o tormenta) cubre el 74 % con 50 días al año ([caso-de-negocio.md](caso-de-negocio.md)).
- Falsas alarmas: en alerta se marca el 12,5 % de los días con un 11 - 12 % de falsas alarmas por día; es la cifra que pide el tutor para "brigadas movilizadas sin necesidad".
- El sector como referencia: el TIEPI imprevisto de la Distribuidora 1 (0,34 h) es un tercio del de Cáceres y menos de un cuarto del de España; un minuto de TIEPI (un minuto de corte de toda la red) son 12.600 euros de incentivo en la Distribuidora 1 y 14.900 en la Distribuidora 2, y cada décima de punto porcentual de la retribución son 11.800 euros al año ([network-analysis.md](network-analysis.md)).
- Lo que no hace y lo que probamos y descartamos, dicho con claridad: no acierta el CT concreto; no ve el estado de los equipos; la humedad del suelo, el umbral por estación y el resto de las pruebas de octubre (cuadernos 3 a 3r) no mejoran el modelo ([investigacion-continua.md](investigacion-continua.md)).

## 3. Textos propuestos para la memoria

### 3.1 Propósito, misión, visión y valores

**Propósito.** Que ningún cliente de una distribuidora pequeña esté sin luz más tiempo del necesario por falta de anticipación.

**Misión.** Dar a las distribuidoras pequeñas y medianas, cada tarde, el riesgo de interrupción del día siguiente por zona y franja horaria, con las brigadas donde más falta van a hacer, a partir de sus propios datos y de la previsión meteorológica.

**Visión a tres años.** Ser el referente nacional en análisis de riesgo de calidad de suministro para distribuidoras pequeñas y medianas: la herramienta con la que se decide la guardia en una de cada cuatro distribuidoras de España, con presencia en las asociaciones del sector y una primera referencia fuera de España. En lo económico: el año 1 son proyectos pivote con tres clientes, el año 2 se sale de pérdidas y el año 3 se factura con recurrencia.

**Valores, como comportamientos.** Ética: decimos lo que el sistema acierta y lo que no, también al cliente (la tabla de verificación diaria está en el propio cuadro de mando). Humildad: la regla sencilla de AEMET cubre casi lo mismo que nuestro modelo en los mismos días, y así lo escribimos; el valor está en sumar y en el resto. Innovación: cada fuente nueva (rayos del satélite, avisos, incendios, escenarios) entra con una prueba medida y se queda solo si aporta. La cultura es lo que hace que esto se cumpla sin que nadie lo vigile: (cabeza + extremidades) x innovación, elevado a cultura.

**Fases.** Fase 1 (año 1), defensiva: tres pilotos con el partner, sin inversión comercial fuera de su cartera, datos y referencias. Fase 2 (años 2 y 3), agresiva: ferias y asociaciones, 6 a 8 clientes, y la entrada en el resto de las 89 distribuidoras cualificadas.

### 3.2 Propuesta de valor, actualizada

GridPredic dice cada tarde, para el día siguiente, en qué distribuidora, en qué municipios y líneas y en qué franja horaria es más probable una interrupción de media tensión, cuántos clientes se esperan sin luz y dónde conviene dejar las brigadas para llegar antes. Con dos tardes de antelación da una orientación para planificar. Durante el día, sigue los rayos, las estaciones y los avisos de AEMET y avisa si la guardia tiene que subir. Por la mañana, cuenta qué pasó frente a lo que dijo.

Lo que gana la distribuidora es cobrar más de la retribución regulada por hacer lo mismo mejor. La retribución de la distribución depende de la calidad de suministro (TIEPI y NIEPI) a través del incentivo de la Circular 8/2025 de la CNMC; cada minuto que una brigada llega antes es un minuto menos de interrupción para todos los clientes afectados. En la Distribuidora 1 y la Distribuidora 2, medido con los cortes reales de 2024 y 2025 y los minutos por carretera de OpenStreetMap, la brigada tarda de media entre 39 y 66 minutos con una base fija y entre 23 y 39 con dos; situarla cada día de alerta en el punto medio de los municipios de riesgo recorta entre 0 y 9 minutos más. Lo que de verdad cambia las horas-cliente no es mover la furgoneta sino tener gente y material preparados los días que toca y en la franja que toca (el 84 % de las averías de los días de alerta empiezan dentro de la franja prevista y el 64 % fuera del horario laboral), y saber qué cortes largos esperan a la brigada: son pocos (del 2 % al 10 % de los cortes), pero en la Distribuidora 1 concentran del 14 % al 80 % de las horas-cliente del año (apartado 3 de [caso-de-negocio.md](caso-de-negocio.md)). En una distribuidora con una sola base lejos de sus líneas la ganancia de minutos sería mayor; se calcula con sus datos en la demo.

Lo que no hace, y lo decimos: no dice qué centro de transformación fallará; no ve el estado de los equipos; con unas 20 incidencias al año por distribuidora, la cobertura de un año se mueve entre el 30 % y el 70 %.

### 3.3 Marca y posicionamiento

**La marca.** GridPredic cumple lo que debe cumplir una marca: es corta (una sílaba más dos), pronunciable igual en español, en catalán y en inglés, evocadora (grid es la red eléctrica y predic lo que hace), distinta de cualquier marca registrada del sector [comprobado en OEPM y EUIPO el dd.mm.yyyy], registrable como marca y como dominio (GridPredic.com) y escalable: no lleva "Extremadura" ni "TFM" ni ninguna palabra que limite el ámbito. Se escribe siempre GridPredic, sin t final.

**Declaración de posicionamiento.** Para las distribuidoras eléctricas pequeñas y medianas que deciden la guardia con el calendario o con los avisos de AEMET, GridPredic es el sistema de análisis de riesgo de calidad de suministro que les dice cada tarde dónde y cuándo es más probable una interrupción al día siguiente y dónde dejar las brigadas, porque aprende de sus propias interrupciones y de la previsión meteorológica y se verifica cada mañana frente a lo que pasó. A diferencia de los sistemas de las grandes eléctricas, se instala junto al SCADA que ya tienen, sin sensores nuevos y con un coste que es el 0,15 % de su retribución.

**Cómo queremos que nos recuerden.** "Los que te dicen la tarde antes dónde va a fallar la red mañana."

### 3.4 Marketing mix (las 4 P)

**Producto.** Un servicio de suscripción con cuatro entregas: el riesgo de mañana (cuadro de mando, parte de guardia en Excel, mensaje de la tarde), el seguimiento del día (rayos, estaciones, avisos releídos cada hora), el parte de la mañana con la verificación, y las cifras anuales para la dirección (lo que habría pasado con el sistema en marcha, la posición frente al sector). Se instala en un servidor del cliente o del partner, en contenedores, y se alimenta del SCADA (TedisNet) y del cálculo oficial de calidad (Calser) que ya tienen. Incluye la puesta en marcha con sus datos históricos, la verificación mensual y el soporte.

**Precio.** 20.000 euros de implantación (la distribuidora puede presentarla como proyecto piloto: el artículo 15 de la Circular 8/2025 permite reconocer esas inversiones con cargo a los costes de distribución, con aprobación previa de la CNMC) y 18.000 euros al año de licencia, que son el 0,15 % de la retribución de una distribuidora mediana y lo que vale minuto y medio de TIEPI en el incentivo (12.600 euros el minuto en la Distribuidora 1). Una demo gratuita de un mes, con sus datos, antes de firmar. [Valorar un precio por tamaño: por tramo de clientes o de CT.]

**Promoción.** Nada de medios masivos ni redes sociales: el comprador está en las ferias y los congresos del sector eléctrico y en las asociaciones de distribuidoras pequeñas [CIDE, ASEME u otras: confirmar cuáles reúnen a las distribuidoras pequeñas de Extremadura y del resto de España], en la revista técnica del sector y en los casos de éxito. El primer argumento es siempre el mismo y se calcula con los datos del propio cliente: cuántas horas-cliente de los últimos años habrían caído en días y zonas de riesgo alto y cuánto tiempo de llegada habrían ahorrado sus brigadas. A las asociaciones se llega con el producto ya validado, no antes.

**Distribución (place).** El canal de entrada es el partner (SITEL), que ya tiene su software instalado en 52 de las 328 distribuidoras de España, el 25 % del mercado: abre la puerta, instala y da el primer soporte. Las primeras ventas son directas a esos clientes, con la demo de un mes. A partir del año 2, venta directa al resto de las 89 distribuidoras cualificadas con SCADA compatible.

### 3.5 DAFO actualizado y priorizado

**Fortalezas.** Datos reales de tres distribuidoras y diez años; el sistema en producción y verificado cada día; la alianza con el partner y su cartera; el conocimiento del incentivo de calidad de la CNMC; la honestidad medida (lo que no funciona está documentado).

**Debilidades.** Pocos casos (unas 45 incidencias imprevistas al año entre las tres redes); no acierta el CT ni la hora; equipo pequeño; sin referencias comerciales todavía.

**Oportunidades.** La Circular 8/2025 (incentivo de calidad y proyectos piloto reconocidos con cargo a los costes de distribución); 328 distribuidoras, de las que 89 están cualificadas y 52 son clientes del partner; los temporales de los últimos años (el TIEPI nacional por fuerza mayor se multiplica por 20 en los años de Klaus y Xynthia, y casi por seis en el de Ana); fuentes públicas nuevas (rayos del satélite, avisos, incendios).

**Amenazas.** Clientes conservadores y ciclo de venta largo; dependencia del partner; que una gran eléctrica o un fabricante de SCADA ofrezca lo mismo; el AI Act (sistema de alto riesgo para infraestructura crítica) y sus obligaciones; cambios en la fórmula del incentivo.

**Priorización por fases.** Año 1, estrategia defensiva (fortalezas frente a amenazas): entrar solo por el partner, con pilotos y la demo gratuita, sin gasto comercial; conseguir dos referencias y los datos de más distribuidoras. Años 2 y 3, estrategia agresiva (oportunidades): ferias y asociaciones, venta directa, y el horizonte de dos y tres días como producto propio.

### 3.6 Cuadro de gestión de riesgos

| Riesgo | Nivel | Probabilidad | Impacto | Cómo lo tratamos |
| --- | --- | --- | --- | --- |
| Pocos datos para aprender (unas 300 incidencias útiles) | Alto | Alta | Alto | Diseño por días y zonas en lugar de por CT y hora; más distribuidoras (objetivo: 11 más); cada fuente nueva se mide antes de entrar |
| Ciclo de venta largo (6 a 18 meses) y clientes conservadores | Alto | Alta | Alto | Entrar por el partner; demo gratuita de un mes; el ahorro calculado con los datos del cliente; plan de tesorería de los primeros 6 meses |
| Dependencia del partner comercial | Medio | Media | Alto | Acuerdo escrito (exclusividad limitada, condiciones de salida); venta directa desde el año 2 |
| Que una previsión meteorológica mala dé un día malo sin avisar | Medio | Media | Medio | Tres fuentes (ICON-EU, 40 escenarios, AEMET) y la confianza en la previsión en el cuadro de mando; los avisos releídos cada hora |
| Regulación: AI Act (alto riesgo), RGPD, NIS2 | Medio | Media | Alto | Soporte a la decisión, nunca acción automática; ficha del modelo y del dataset; supervisión humana; registro cuando toque |
| Cambio de la fórmula del incentivo de calidad | Bajo | Baja | Alto | El valor operativo (llegar antes) no depende de la fórmula; seguimiento de la CNMC |
| Un competidor grande ofrece lo mismo | Medio | Baja | Alto | Velocidad y cercanía: las grandes no atienden a distribuidoras de 700 o 30.000 clientes; datos propios del cliente como foso |
| Financiación insuficiente antes de los primeros cobros | Alto | Media | Alto | Mix 40/60 en lugar de 20/80; ronda de 250.000 euros; gastos variables ligados a clientes |
| Equipo pequeño (dependencia de dos personas) | Medio | Media | Alto | Documentación completa (operación diaria, model card, dataset card); el partner cubre instalación y soporte |

### 3.7 Validación y argumento económico

**Cómo medimos el valor con los históricos del cliente** (es lo que hacemos ya con las tres redes): con sus interrupciones de los últimos años y la previsión meteorológica que había cada víspera, calculamos qué nivel habría dado el sistema cada tarde; contamos las horas-cliente de interrupción que caen en los días y zonas marcados como riesgo alto (hoy, el 72 % en el 12,5 % de los días), cuántos días se habría reforzado la guardia sin avería (las falsas alarmas: el 11 - 12 % de los días) y cuánto habrían tardado las brigadas desde las bases que propone el sistema frente a las habituales. De ahí sale, en euros de retribución, lo que el cliente habría recuperado.

**La cifra del minuto.** En la tutoría dijimos que un minuto de interrupción de toda la red le cuesta a una distribuidora mediana entre 12.000 y 15.000 euros, y la cifra es nuestra y está medida: con la fórmula del incentivo de calidad de la Circular 8/2025 (del -4 % al +4 % de la retribución según el TIEPI medio de tres años frente a la referencia del sector) y la retribución de 2023 (BOE-A-2026-19941), un minuto de TIEPI, es decir, un minuto de corte de toda la potencia instalada, vale 12.600 euros de incentivo en la Distribuidora 1 (124.000 kVA, 37.400 clientes) y 14.900 en la Distribuidora 2 (apartado 6 de [network-analysis.md](network-analysis.md), `job_analisis_red.py`, columna `eur_por_minuto_tiepi`). Es lo que la distribuidora deja de cobrar mientras su TIEPI está dentro del tramo lineal del incentivo; las dos grandes están cerca del tope de bonificación, así que lo que se juegan es sobre todo no perderla el año del temporal (más de 200.000 euros al año durante tres años).

A esa cifra se le puede sumar, por separado, lo que el corte cuesta a los clientes: el valor de carga perdida (VoLL) que España ha fijado para el Reglamento (UE) 2019/943 es de 22.879 euros/MWh (Resolución del 07.07.2025 de la Dirección General de Política Energética y Minas, BOE-A-2025-14438; su memoria justificativa lo calcula con la subasta del servicio de respuesta activa de la demanda de 2022 y lo llama límite inferior, con 24.635 euros/MWh para los hogares según el panel de la CNMC). Con el 25 % de uso de la potencia instalada y factor de potencia 0,9, un minuto de corte de toda la red son unos 460 kWh no servidos en la Distribuidora 1 (10.600 euros al VoLL oficial, 3.700 con los 7,88 euros/kWh domésticos del estudio de CEPA para ACER de 2018) y 410 kWh en la Distribuidora 2 (9.400 euros). La frase para la memoria: "un minuto de corte de toda la red le cuesta a la distribuidora entre 12.600 y 14.900 euros de incentivo de calidad, y a sus clientes otros 9.400 a 10.600 euros de energía no suministrada al valor oficial". Lo que no hay que hacer es mezclar las dos en una sola cifra sin decirlo.

**El ahorro por llegar antes, con nuestras cifras.** En la tutoría dijimos "de 90 a 35 minutos"; con los datos de nuestras redes hay que decirlo con más cuidado. La llegada media de la brigada a los cortes que la esperan es de 39 a 66 minutos con una base fija y de 23 a 39 con dos (Distribuidora 1 y 2, 2024 y 2025, minutos por carretera de OSRM sobre OpenStreetMap), y salir cada día de alerta desde el punto medio de los municipios de riesgo recorta entre 0 y 9 minutos: menos de un minuto de TIEPI al año (0,54 como máximo, en la Distribuidora 2 en 2024), porque con dos brigadas la base fija óptima ya cubre bien las dos mitades de cada red. Tres de cada cuatro cortes de CT duran menos de 15 minutos (los repone el reenganche o el telemando antes de que llegue nadie) y solo del 2 % al 10 % duran más que la llegada, pero en la Distribuidora 1 esos pocos son del 14 % al 80 % de las horas-cliente del año. Por eso el argumento económico que sí sostienen los datos es otro: la guardia reforzada los días que el sistema señala (50 días al año para el 74 % de las horas-cliente, la política que más cubre por día gastado), la franja horaria (el 84 % de las averías de los días de alerta empiezan dentro de ella) y, para acortar los cortes largos, el telemando en las cabeceras que el ranking anual pone en cabeza (en 8 de 47 cabeceras un interruptor telemandado se paga evitando el 25 % o menos del tiempo de maniobra). La cifra del minuto (12.600 euros de incentivo por minuto de TIEPI en la Distribuidora 1) es la que convierte esos minutos en dinero. Para una distribuidora con una sola base lejos de sus líneas, la ganancia de llegada sería mayor y se calcula con sus datos en la demo de un mes.

**KPIs del piloto de un mes** (se miden cada mañana en el propio sistema): parte de las averías que cayeron en días de alerta y en la franja prevista; falsas alarmas por semana; tiempo medio de llegada de la brigada en los días de alerta frente al histórico; pico de racha previsto frente al medido; y, al final del mes, el TIEPI del mes frente al mismo mes de los tres años anteriores.

### 3.8 Plan financiero: lo que hay que completar

- Cuenta de explotación a tres años con las cifras del Pitch v3 como punto de partida: año 1, 87.000 de ingresos y 141.000 de gastos (3 clientes); año 2, 220.000 y 180.000 (6 a 8 clientes, salida de pérdidas); año 3, 360.000 a 480.000 de recurrencia. Hay que escribir los supuestos (clientes nuevos por año, bajas, precio, plantilla, ferias) y que cuadren con el marketing mix.
- Balances al cierre de los años 1, 2 y 3 a partir del balance de momento 0 (activo y pasivo de 36.000 euros): el resultado de cada año pasa a fondos propios, las amortizaciones bajan el activo, y la tesorería final de cada año es la inicial del siguiente.
- Plan de tesorería: mes a mes el primer año (el cobro de la implantación al firmar y la licencia por adelantado reducen el hueco), anual después. El flujo de caja se calcula como beneficio más amortización.
- Financiación: pasar del 20/80 propio/ajeno al 40/60, con la ronda de 250.000 euros por el 25 % como vía, y las pérdidas de los primeros años como crédito fiscal.

### 3.9 Argumentario para el cliente conservador

Para el documento y para la defensa, cinco respuestas cortas:

1. "No quiero instalar nada de terceros": se instala junto al software que ya tienes del partner, en un contenedor aparte, sin tocar el SCADA; solo lee.
2. "Mis brigadas ya saben dónde ir": con tus propios datos te enseñamos cuántas horas-cliente de los últimos tres años cayeron en días que el sistema habría marcado la tarde anterior, y cuánto habrían tardado menos tus brigadas.
3. "Eso falla": cada mañana el sistema escribe lo que dijo y lo que pasó; lo ves tú, no nosotros. Y el cuadro de mando dice qué no sabe hacer.
4. "Es caro": 18.000 euros al año son el 0,15 % de tu retribución, lo que vale minuto y medio de TIEPI en el incentivo; la implantación puede presentarse como proyecto piloto del artículo 15.
5. "Ahora no": un mes de demo gratuita con tus datos y sin compromiso; si no te sirve, se desinstala.

Cada "no" es una pregunta: respondemos al motivo, con la cifra, sin ceder y sin pesar.

## 4. Para la defensa del 05.11

Al guion de 14 diapositivas ([guion-presentacion.md](guion-presentacion.md)) le falta una diapositiva de negocio entre la 11 y la 12: mercado (328, 89, 52), modelo (20.000 + 18.000 al año, demo de un mes, partner), cuenta de explotación a tres años en una línea (año 1 pivote, año 2 sin pérdidas, año 3 recurrencia), la ronda (250.000 por el 25 %) y el riesgo principal con su mitigación. El tutor se pondrá en el papel de inversor: el inversor quiere ver que se vende, y se vende si al cliente le ahorra dinero que puede medir.

## 5. Pendientes que salen de la tutoría y que podemos resolver con el sistema

- Incendios: el tutor anotó integrar el feed oficial de incendios (publicación diaria a las 08:00). Hoy tenemos los focos del satélite (FIRMS) y el índice de peligro de incendio (FWI de Copernicus); falta el parte diario de la administración [INFOEX en Extremadura, o el parte nacional] como fuente para anticipar los cortes que piden los bomberos. Se puede probar con el mismo banco de pruebas de [investigacion-continua.md](investigacion-continua.md).
- Más distribuidoras: cada distribuidora nueva son unas 15 incidencias al año más para aprender; el objetivo de 11 más viene de ahí.
- Horizonte de tres días: medido el 15.10.2026 con la previsión de tres días antes (apartado 3e del cuaderno: 5,2 veces la tasa base, casi lo mismo que a dos días). La orientación de pasado mañana ya sale cada tarde.
