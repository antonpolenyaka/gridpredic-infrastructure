# Capa Gold (Exploitation Zone)

Este documento explica qué hace la capa Gold de GridPredic, qué decide cada job y por qué. Parte del diseño de la Exploitation Zone que hicimos el 07.09.2026 y de lo que aprendimos al contrastarlo con los datos reales de `TedisNet_EOSA` y de las tres bases Calser. Lo que se ha quedado fuera de esta primera versión está al final, en el apartado 9.

Silver deja una sola versión válida de cada registro y marca lo dudoso. Gold es donde se toman las decisiones de negocio y de modelado: qué interrupción cuenta para la etiqueta, cómo se fusionan dos registros solapados, cómo se imputa la potencia de un CT, qué ventanas usan las features y cómo se parte el dataset. Todas esas decisiones son parámetros de `etl/config/03_gold/config_gold.json`, de modo que se pueden cambiar y reconstruir Gold sin tocar Silver.

## 1. Convenciones

**Grano.** Todo se refiere a `(distribuidora_id, ct_id, hora)`. El `ct_id` es el `CT_ID` de Calser (cinco dígitos, por ejemplo `06011`), que coincide con el `ShortName` del elemento TRAFO CT (tipo 145) de TedisNet. Es la clave que validamos en agosto: cubre el 97,8 % de los CT con interrupciones desde 2021.

**`hora` es el instante de la predicción.** Una fila de la hora `h` solo usa datos con marca de tiempo anterior a `h`, y sus etiquetas solo miran hacia delante. Los agregados horarios se guardan con la hora del final de su intervalo: las muestras de `[09:00, 10:00)` aparecen en la fila de las 10:00, que es el primer momento en que se conocen todas. Un ejemplo con la etiqueta principal:

| Interrupción que empieza a las | Filas con `y_1_3h = 1` | Por qué |
| --- | --- | --- |
| 10:30 | 08:00 y 09:00 | A las 08:00 faltan 2,5 h y a las 09:00 faltan 1,5 h; a las 07:00 faltarían 3,5 h y a las 10:00 solo 0,5 h |
| 10:00 | 07:00 y 08:00 | El intervalo es `(h + 1 h, h + 3 h]`: a las 07:00 faltan justo 3 h y entra; a las 09:00 falta 1 h y no entra |

**Cuándo se conoce un dato.** "Anterior a `h`" se refiere al momento en que el dato se pudo conocer, no al momento en que pasó la cosa. Las etiquetas usan la hora de campo del evento; las features usan:

- Para TedisNet, la hora de llegada al servidor: la mayor entre la hora de campo (`SourceTimestamp`) y la hora en que el servidor guardó el cambio (`UpdateTimestamp`). Suelen diferir en segundos (mediana de 1 s y p90 de 21 s en una muestra de los 200 últimos cambios de la base real), pero un cambio que la RTU retuvo durante una caída de comunicaciones tiene una hora de campo antigua y solo existe desde que llega. En esa misma muestra, 13 de los 200 llegaron con meses de retraso (posiciones ICCP, estados que se vuelven a leer al reconectar): los cambios que llegan más de `tiempo.retraso_max_llegada_h` horas tarde (24 por defecto) no cuentan como señal. Los eventos de calidad y los de corte sí cuentan siempre, en su hora de llegada.
- Para una muestra de una serie, su instante de rejilla, o la hora de campo de su valor si es posterior (la copia se hizo tarde o el reloj de la RTU va adelantado).
- Para Calser, el día siguiente a la carga del registro (`conocido_ts`).
- Para la topología aprendida de los cortes, el primer corte que la reveló (apartado 3).

**Reloj.** Calser y TedisNet guardan hora local sin zona horaria. Las sesiones de Spark de Gold fijan `spark.sql.session.timeZone = UTC` para que las operaciones por horas no tengan saltos de cambio de hora. Si el control de Silver demostrara que TedisNet guarda UTC, se corrige en un solo sitio (`tiempo.tedisnet_en_utc` y `tiempo.desfase_tedisnet_min`) y todos los jobs de Gold lo aplican al leer TedisNet. Por los datos que tenemos pensamos que las dos fuentes están en hora local: el último cambio de `HistoricTagValueChanges` es de las 01:41 del 14.08.2026 y el backup se lanzó a las 02:00 de ese día, lo que con UTC sería imposible. Aun así es una hipótesis, y `job_gold_dq_checks` la mide en cada ejecución.

**Escritura.** Las tablas pequeñas se reescriben enteras. Las grandes (`labels_ct_hora`, `agg_medida_hora`, `features_ct_hora`) se particionan por `fecha_mes` y cada job escribe solo los meses de su lote con `replaceWhere`. Todos los jobs aceptan `--desde` y `--hasta` (`YYYY-MM`) para rehacer unos meses, y `--rebuild` para borrar la tabla y empezar de cero (por ejemplo después de quitar una feature).

## 2. Tablas

| Tabla | Grano | Contenido | Job |
| --- | --- | --- | --- |
| `dim_ct` | distribuidora, CT | El CT con los atributos de su último período en Calser, su municipio y coordenadas, su trafo en TedisNet, la potencia imputada y si entra en el estudio | `job_gold_dim_ct.py` |
| `map_tag_ct` | tag | El CT (`anchor_id`) y el grupo de red (`grupo_red_id`) al que pertenece cada señal, y su familia de medida o de evento | `job_gold_dim_ct.py` |
| `map_aguas_arriba` | trafo, elemento | Los interruptores que alguna vez han cortado cada trafo, la posición (celda) en la que están y desde cuándo se sabe (`primer_conocido_ts`) | `job_gold_dim_ct.py` |
| `fact_interrupciones_mt` | evento | Las interrupciones de Calser a nivel de CT en tres variantes, con los solapes fusionados | `job_gold_fact_interrupciones_mt.py` |
| `fact_cortes_scada` | episodio | Los episodios Off -> On de cada trafo según el SCADA, con microcortes y maniobras marcados | `job_gold_fact_cortes_scada.py` |
| `labels_ct_hora` | distribuidora, CT, hora | Las etiquetas. Es también la rejilla del dataset: tiene todas las horas, también las que no tienen ningún evento | `job_gold_labels_ct_hora.py` |
| `agg_medida_hora` | ámbito, clave, familia, hora | Sumas horarias de las series de medida por CT y por posición aguas arriba | `job_gold_agg_medida_hora.py` |
| `actividad_scada_hora` | distribuidora, hora | Muestras válidas por hora: si no hay ninguna, el histórico del SCADA tiene un hueco | `job_gold_agg_medida_hora.py` |
| `features_ct_hora` | distribuidora, CT, hora | Las features | `job_gold_features_ct_hora.py` |
| `feature_metadata` | feature | Bloque, ámbito, familia, ventana, fuente, descripción y porcentaje de nulos en train de cada feature | `job_gold_features_ct_hora.py` |
| `dataset_train` | distribuidora, CT, hora | Etiquetas y features unidas, filtradas y partidas en train, valid y test | `job_gold_dataset_train.py` |
| `dataset_versions` | versión | Una fila por construcción del dataset con los parámetros y las versiones Delta de todas las tablas de entrada | `job_gold_dataset_train.py` |
| `dq_metrics` | ejecución, métrica | Las métricas de calidad de Gold, con el mismo esquema que las de Silver | todos |

## 3. Del SCADA al CT

**El ancla de cada señal.** En TedisNet el trafo de un CT (tipo 145, `06011`) cuelga del elemento CT (tipo 144, `0601`), y de ese mismo elemento cuelgan las celdas y los interruptores del centro. Por eso el ancla de un elemento es su antecesor más cercano de tipo 144, o de tipo 145 si no está debajo de ningún CT, y todas las señales que quedan debajo de un ancla describen a todos los trafos de ese CT. Un tag sin elemento (el estado de conexión de una RTU, por ejemplo) toma el ancla que tienen la mayoría de los tags de su dispositivo; Silver ya no lo rechaza.

**El grupo de red.** El elemento del que cuelga el CT es su grupo: en `/EODSLU/TORRE DE SANTA MARÍA/MONTANCHEZ/0601/06011:TRA` es la línea `MONTANCHEZ`. Las señales que cuelgan directamente de esa línea (un detector de paso de falta en un apoyo, por ejemplo) pertenecen al grupo y no a un CT concreto.

**Aguas arriba.** Al revisar los cortes reales vimos que casi siempre los provoca un interruptor de cabecera de una subestación (`CC/Estados/EPDSLU/S.T.R. CORIA/CAÑAVERAL/I.A.A.T./TORREJONCILLO 3`), y que ese interruptor no está por encima del CT en la jerarquía funcional: el CT del ejemplo está en `S.T.R. GARROVILLAS/CAÑAVERAL`. El SCADA conoce el camino eléctrico porque recorre el grafo de nodos y ramas, pero eso todavía no lo hacemos nosotros. Mientras tanto, `map_aguas_arriba` guarda, para cada trafo, los elementos cuyo cambio de estado le ha producido un Off en el histórico, y la posición (el elemento padre, la celda) donde viven las protecciones de ese interruptor. Es conocimiento de topología: dice qué cabecera alimenta a qué CT.

Ese conocimiento sale de los cortes y por eso tiene fecha. Antes del primer corte de un par nadie sabía que esa cabecera alimentaba al CT, y un CT que solo se corta una vez, en el período de test, llevaría ese corte futuro en todas sus horas anteriores (sus medidas aguas arriba y el número de posiciones dirían "este CT se va a cortar"). Por eso cada par guarda `primer_conocido_ts`, la llegada al SCADA de su primer corte, y las features solo usan el par a partir de ese momento. `n_posiciones_aguas_arriba` de `dim_ct` cuenta las posiciones de todo el histórico y solo describe la dimensión; la feature es `aa_n_posiciones`, las posiciones conocidas en cada hora.

**Potencia del CT.** En Calser `CT_POTENCIA_INSTAL` es 0 en el 37 % de los CT de EOSA y en el 48 % de los de Pitarch. Gold usa la potencia instalada si es mayor que 0, si no la administrativa (como hace Calser) y, si tampoco hay, la potencia nominal del trafo en TedisNet (`RatedPower`). `potencia_fuente` dice de dónde sale y `potencia_imputada` lo marca.

**Qué CT entran en el estudio.** Los que tienen trafo en TedisNet y además son observables: `IsPowerCut = 1` y nodo en `SystemNodes`. Un trafo que no cumple las dos cosas nunca puede aparecer como cortado en el SCADA, así que sus horas sin corte no significarían nada. El parámetro `ct.solo_observables` permite relajarlo.

## 4. Etiquetas

### `fact_interrupciones_mt`

Una fila por evento de interrupción de un CT, en tres variantes:

| Variante | Qué entra | Para qué |
| --- | --- | --- |
| `principal` | Incidencia `CL_IMPRE`, factor distinto de `FA_CLIEN`, `INT_DURACION > 180` y nivel CT. Son los filtros con los que Calser calcula el TIEPI (`ZonaCalculateIndexValues.cs`). Un factor NULL no pasa, igual que en el SQL de Calser | Es el target del TFM |
| `amplia` | La principal más las interrupciones de CT de más de 180 s sin incidencia válida (26 - 28 % en EOSA y Pitarch) | Medir cuánto cambia el resultado por ese hueco de etiquetado |
| `todas` | Toda interrupción de CT de más de 180 s, también las programadas | Saber cuándo un CT ya está sin suministro. Nunca es etiqueta |

Cada variante fusiona sus propios solapes. Silver agrupa los solapes sobre todos los registros, pero si fusionáramos con ese grupo dos interrupciones del target podrían acabar unidas a través de una programada que no forma parte de la variante. La regla es `target.fusion_solapes`: `envolvente` (del primer inicio al último fin, la opción por defecto) o `mas_larga`. La duración se winsoriza al percentil 99 por distribuidora en `duracion_winsor_s`, con el percentil calculado solo con los eventos anteriores a `dataset.train_hasta` para que el tope no dependa de valid ni de test, y la original se conserva. La misma interrupción grabada en dos períodos se queda una vez.

`INT_FECHA_ALTA` es leakage como feature, pero dice cuándo existió el registro en Calser. Gold la usa para una sola cosa: saber cuándo se conocía cada registro, el día siguiente a su carga (o el inicio más `target.retraso_alta_defecto_dias` si no hay fecha). Un evento fusionado tiene dos momentos:

- `conocido_ts`: se conoce su primer registro. Desde ahí cuenta en las features de histórico.
- `conocido_fin_ts`: se conocen todos sus registros y el evento ha terminado. Solo desde ahí se usa su duración, porque otro registro cargado más tarde puede alargarla.

Es lo que vería el modelo en producción.

### `fact_cortes_scada`

TedisNet escribe un estado por evento y elemento (Off, On, error de comunicaciones) pero no una duración. El job ordena los estados de cada trafo, se queda con las transiciones (un Off detrás de otro Off es el mismo corte visto desde otro interruptor) y empareja cada Off con el siguiente On. Deja fuera los errores de comunicaciones, que van a otra feature, y los eventos que Silver marca como `ts_incoherente`. Solo usa el trafo que `dim_ct` eligió para cada CT: si fuera por el `ShortName`, cuando dos elementos se llaman igual (`mapeo_ambiguo`) contaría sus Offs dos veces.

Cada extremo del episodio tiene dos horas: `inicio_ts` y `fin_ts` son las de campo (las usan las etiquetas) e `inicio_conocido_ts` y `fin_conocido_ts` las de llegada del Off y del On al SCADA (las usan las features). `ProcessedTimestamp` no sirve para esto: lo marca el consumidor que lee el evento (Calser, por la API de exportación), no el SCADA.

De aquí salen dos cosas: la etiqueta alternativa `y_1_3h_scada` (episodios de más de 180 s que no son maniobra), que en la era 3 debería coincidir con la de Calser, y los microcortes y reenganches, que Calser no graba porque las tres bases filtran a 180 s y que son el mejor precursor que tenemos dentro del propio SCADA. Un On que llega más de `scada.emparejamiento_max_h` horas después se marca como `emparejamiento_dudoso`: suele ser un On perdido en un hueco del histórico.

### `labels_ct_hora`

| Columna | Definición |
| --- | --- |
| `y_1_3h` | Empieza una interrupción principal en `(hora + 1 h, hora + 3 h]`. Target del TFM |
| `y_0_1h`, `y_0_3h`, `y_0_6h` | Lo mismo para otros horizontes (`horizontes` en la configuración) |
| `y_1_3h_local` | Solo interrupciones cuya incidencia afecta a un único CT. En EOSA y Pitarch el 94 % son sistémicas, y esta etiqueta dice si un modelo encuentra el CT y no solo el día malo |
| `y_1_3h_amplia` | Con la variante amplia |
| `y_1_3h_scada` | Con los cortes del SCADA |
| `en_corte`, `en_corte_calser`, `en_corte_scada` | El CT ya está sin suministro en `hora`. Esas filas no se usan para entrenar |
| `horas_hasta_proximo_evento` | Horas hasta el siguiente inicio de la variante principal, nulo si no hay ninguno en un año |
| `era` | 1 hasta el 06.02.2018 (importación de ficheros), 2 hasta octubre de 2025, 3 desde noviembre de 2025 (Calser importa del SCADA) |

Cada evento se convierte en el conjunto de horas exactas que etiqueta: para un horizonte `(a, b]` y un inicio `e`, son las horas `h` con `e - b <= h < e - a`. Como los eventos son pocos (unos 14.000 desde 2021), las etiquetas se construyen explotando los eventos y uniéndolos una sola vez a la rejilla.

## 5. Features

Unas 250 columnas en la configuración actual, en estos bloques:

| Prefijo | Qué mide | Fuente | Ventanas |
| --- | --- | --- | --- |
| `med_` | Series de medida del CT: media, máximo, mínimo, desviación, número de muestras y fracción de valores rancios de la última hora, desequilibrio entre tags (fases), media y máximo de 24 h, diferencia con la hora anterior y con la misma hora del día anterior | `agg_medida_hora` (ámbito LOCAL) | 1 h y 24 h |
| `medaa_` | Lo mismo, más reducido, para las posiciones aguas arriba conocidas en esa hora. En EOSA es donde están las intensidades de cabecera (`AI.INTENSIDAD`, 800 tags con serie), así que es la única medida que puede tener un CT sin RTU | `agg_medida_hora` (ámbito AGUAS_ARRIBA) | 1 h y 24 h |
| `aa_n_posiciones` | Posiciones cuyos interruptores habían cortado el CT antes de esa hora | `map_aguas_arriba` | histórico |
| `ev_` | Cambios de valor de cada familia de señales en el CT | `f_tag_value_change` | 1, 6, 24 y 168 h |
| `evred_` | Lo mismo en el grupo de red del CT | `f_tag_value_change` | 6, 24 y 168 h |
| `evaa_` | Lo mismo en las posiciones que alimentan el CT, desde que se conocen | `f_tag_value_change` | 6, 24 y 168 h |
| `ev_alarmas_`, `ev_avisos_` | Eventos que el SCADA levantó como alarma o aviso en el CT | `f_evento` | 1, 6, 24 y 168 h |
| `calidad_`, `corte_error_comm_` | Lecturas marcadas como fallo de comunicaciones o no reales, y errores de comunicaciones del cálculo de cortes sobre el trafo | `f_tag_quality_event`, `f_corte_elemento` | 24 y 168 h |
| `scada_cortes_`, `scada_microcortes_` | Offs y microcortes del trafo, y Offs de los trafos del mismo grupo | `fact_cortes_scada` | 24 y 168 h; grupo 6, 24 y 168 h |
| `hist_` | Interrupciones del CT conocidas en Calser, días desde la última, duración media, cortes y microcortes del SCADA, interrupciones del municipio | `fact_interrupciones_mt`, `fact_cortes_scada` | 30, 90 y 365 días |
| `ct_` | Potencia, abonados, salidas, tipo de zona, trafos del CT, CT del grupo, cobertura de medidas, tensión primaria, coordenadas | `dim_ct` | estáticas |
| calendario | Hora del día, día de la semana, mes, fin de semana, festivo y víspera | calculado | |
| `scada_activo`, `ct_con_datos_1h` | Hay muestras del SCADA de la distribuidora en la última hora; el CT tuvo alguna muestra o cambio | `actividad_scada_hora` | 1 h |

**Familias de señales.** Una familia es una expresión regular sobre el nombre de la clase del tag en `LibTagClasses` (`familias_medida` y `familias_evento` en la configuración; gana la primera que encaja, por eso `DI.DISPARO TEMPORIZADO NEUTRO` es un defecto a tierra antes que un disparo genérico). Las comprobamos contra las 1.122 clases reales de EOSA, que no se parecen al vocabulario del banco de pruebas de Girona: aquí casi no hay THD ni homopolar, y las series que más abundan son `AI.INTENSIDAD` (800 tags), `AI.TENSION` (331) y `AI.INT. FASE L1/L2/L3` (227 cada una). Las familias de evento cubren los defectos a tierra y de fase (unos 3.200 tags entre disparos, arranques y detectores), el paso de falta de los ekorRCI y Nortroll, los reenganches, los seccionalizadores, las faltas de tensión, las posiciones, las comunicaciones (incluido `SYS.Estado Dispositivo`), el mando local, la presencia de personal en el CT y las alarmas de equipo.

**Medidas en la misma escala.** Los valores en V, W, VAr o VA se pasan a kV, kW, kVAr y kVA, porque en un mismo CT conviven `AI.TENSION` en kV y `AI.Tension Fase 1` en V.

**Valores rancios.** Silver ya guarda en cada muestra la antigüedad del valor retenido (`antiguedad_s`) y lo marca como rancio a partir de una hora. Gold lo convierte en `med_*_frac_rancio`, que es la forma de ver un valor congelado sin una ventana sobre 2.000 millones de filas.

**Cambios de valor, eventos y cortes en su hora de llegada.** Todos los conteos de TedisNet van a la hora en que el dato llegó al SCADA, no a la de campo (apartado 1). Un defecto a tierra de las 08:20 que la RTU retuvo hasta las 09:40 cuenta en la fila de las 10:00, no en la de las 09:00. Los cambios que llegan más de `tiempo.retraso_max_llegada_h` horas tarde no cuentan.

**Cortes del SCADA en el histórico.** Un Off cuenta como corte (y no como microcorte) solo cuando ha llegado y han pasado 180 s sin On. Si usáramos la duración final del episodio, un corte que empezó un minuto antes de `hora` estaría mirando al futuro. Un microcorte cuenta cuando llega su On.

**Fuentes que faltan.** Si no existe una tabla opcional (`f_evento`, `f_tag_quality_event`, `f_corte_elemento`), sus conteos quedan nulos y no a 0: "no sabemos" no es "no pasó nada", y el control de features siempre nulas lo detecta.

**`scada_activo`.** Cuando RabbitMQ cae, TedisNet no copia nada a `Historic*` y el histórico tiene un hueco. Una hora sin eventos en un hueco no es una hora tranquila: no hay datos. Estas filas se quedan fuera del dataset.

## 6. Dataset de entrenamiento

`job_gold_dataset_train.py` une etiquetas y features, quita las filas con `en_corte = 1` y con `scada_activo = 0`, y añade:

- `split`: train antes de `dataset.train_hasta` (01.01.2025), valid antes de `dataset.valid_hasta` (01.01.2026) y test después. Es temporal y los CT no se separan entre conjuntos: el modelo se evalúa sobre los mismos CT en el futuro, que es el uso real. Las últimas `dataset.purga_horas` horas antes de cada frontera (6, el horizonte más largo de las etiquetas) van a `split = 'purga'`: su etiqueta mira al período siguiente, y la misma interrupción sería un positivo a los dos lados. Se guardan para poder contarlas, pero no se entrena ni se evalúa con ellas.
- `en_muestra_train` y `peso_muestra`: todos los positivos de `y_1_3h` en train y una muestra determinista de los negativos (`dataset.tasa_negativos_train`, por hash de la clave y la semilla), con peso `1 / tasa` para corregir las probabilidades. Valid y test no se submuestrean. Usar la muestra es una decisión del entrenamiento; la columna solo la hace reproducible.
- `dataset_version`: el identificador de la construcción.

Cada construcción añade una fila a `dataset_versions` con los parámetros completos, su hash, la lista de features, las filas y positivos por split y la versión Delta de cada tabla de entrada, de Silver y de Gold. Con eso se puede reconstruir exactamente un dataset con time travel, y es el identificador que irá a MLflow junto al modelo. El dataset anterior queda en el historial Delta de `dataset_train` hasta el siguiente `VACUUM`.

## 7. Control de calidad

Cada job escribe sus métricas en `l3_gold.dq_metrics`, con el mismo esquema que `l2_silver.dq_metrics`. El último job, `job_gold_dq_checks.py`, comprueba:

1. **Alineación horaria después de la corrección.** Empareja cada interrupción principal de la era 3 con el Off más cercano del mismo CT en el SCADA. La mediana del desfase tiene que estar por debajo de 30 minutos; si saliera en 60 o 120, la ventana de 1 a 3 horas estaría desplazada y las features verían el corte que intentan anticipar. También informa de qué parte de los eventos de Calser vio el SCADA, que es la concordancia entre las dos etiquetas.
2. **Llegada de los cambios del SCADA**: mediana del retraso entre la hora de campo y la del servidor por distribuidora (si sale cerca de 3.600 o 7.200 s, un equipo o el servidor están en otra zona horaria), cambios que llegan más de una hora tarde y cambios que se quedan fuera por pasar de `tiempo.retraso_max_llegada_h`.
3. **Censura de la etiqueta al final de la ventana**: Calser carga las interrupciones días o semanas después de que pasen, así que en los últimos días antes del backup faltan positivos. Con el p95 del retraso de carga de la era 3 calcula hasta qué fecha debería llegar `ventana.hasta` y marca `REVISAR` si llega más lejos.
4. **Cobertura**: CT del estudio con medidas y con posiciones aguas arriba, y horas con el SCADA activo.
5. **Positivos por split**: cero o más del 1 % significa que la etiqueta está rota.
6. **Leakage por construcción**: ninguna columna de una lista prohibida (`fecha_alta`, `inicio_ts`, `en_corte`, `n_posiciones_aguas_arriba`, las etiquetas...) puede estar en `feature_metadata`.
7. **Features siempre nulas en train.**

Con `"fail_on_review": true` en `config_gold.json` el DAG falla si queda alguna métrica en `REVISAR`.

La prueba local está en `tests/03_gold/test_gold_smoke.py`. Monta un Bronze pequeño con dos distribuidoras, un CT con dos trafos, una celda de cabecera que los corta, series de medida, señales precursoras e interrupciones de todos los tipos, ejecuta Silver y Gold en un Spark local y comprueba cada decisión: qué interrupciones son target, en qué horas exactas vale 1 cada etiqueta, qué datos ve cada feature, el split y la versión. El último test es el de leakage: borra de Silver todo lo que se conocía a partir de una hora (por hora de llegada), reconstruye el mapa aguas arriba y las features y comprueba que las filas hasta esa hora no cambian ni en un decimal. El fixture incluye un defecto a tierra retenido por la RTU, un estado que llega con meses de retraso, un Off que llega 50 minutos tarde y un CT cuya cabecera solo se conoce por un corte del período de valid.

## 8. Ejecución

Requisitos: Silver completo, incluida `f_tag_interval_value` para toda la ventana, y la tabla grande cargada en Bronze por trozos (apartado 10.1 del README).

Con el DAG `dag_gold` en Airflow, que respeta este orden:

```text
dim_ct -> fact_interrupciones_mt -> labels_ct_hora
dim_ct -> fact_cortes_scada      -> labels_ct_hora
dim_ct -> agg_medida_hora
labels_ct_hora, agg_medida_hora -> features_ct_hora -> dataset_train -> dq_checks
```

O un job suelto desde el contenedor de Spark (lee la configuración de `/app/config`):

```bash
docker compose exec spark-master \
  spark-submit --py-files /app/jobs/03_gold/gold_common.py \
  /app/jobs/03_gold/job_gold_features_ct_hora.py --desde 2026-01 --hasta 2026-03
```

Los jobs pesados son `agg_medida_hora` (lee la serie de Silver una vez, mes a mes) y `features_ct_hora` (unos 1,3 millones de filas por mes, más la semana anterior que necesitan las ventanas). Todavía no los hemos medido sobre la ventana completa en el portátil; lo haremos en la primera ejecución con los datos reales.

## 9. Lo que queda fuera de esta versión

- **Validación con los datos reales**: la alineación horaria, el retraso de llegada de los cambios, el retraso de carga de Calser al final de la ventana, la cobertura de `map_tag_ct` (cuántos CT tienen medidas propias) y la concordancia entre las etiquetas de Calser y del SCADA son resultados que todavía no tenemos.
- **Atributos del CT en el tiempo.** `dim_ct` toma el último período de Calser y lo aplica a todas las horas desde 2021. Un CT dado de alta en 2024 tendría filas antes de existir; las que no tienen datos del SCADA ya se quedan fuera por `scada_activo`, pero falta usar las fechas de alta y baja.
- **Cambio de hora de octubre.** En la hora que se repite, ordenar por hora local puede poner un On de las 02:10 (CET) delante de un Off de las 02:50 (CEST) y emparejar mal un episodio. Pasa una hora al año y solo si el Off y el On caen en ella: el Off se empareja con un On posterior y el episodio sale mucho más largo de lo real; si pasa de `scada.emparejamiento_max_h` horas queda marcado como `emparejamiento_dudoso`.
- **El camino eléctrico real.** `map_aguas_arriba` aprende la topología de los cortes. Recorrer el grafo de `SystemNodes` y `SystemBranches` daría también los CT que nunca se han cortado y las cabeceras alternativas.
- **Meteorología**, cuando esté la ingesta. `dim_ct` ya trae las coordenadas del municipio para cruzarla.
- **Festivos locales**: el calendario solo tiene los nacionales y los de Extremadura (8 de septiembre y Jueves Santo), porque `LibElectricCalendarDates` tiene 9 filas en EOSA.
- **Operaciones de anomalía del SCADA** (`LibOperationTypes` 9, 10 y 11): en EOSA solo hay 99 configuradas y no se ingiere `SystemOperations` en Silver.
- **TIEPI y NIEPI por municipio** para validar el pipeline contra `calculos_anual_resumen` de Calser.
- **Serving**: features en streaming con las mismas funciones, `predicciones_ct_hora` y `alertas` (pasos 21 a 23 del diseño), que llegan con el modelo.
