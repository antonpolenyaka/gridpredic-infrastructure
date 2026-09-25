# Capa Silver (Trusted Zone)

Este documento explica qué hace la capa Silver de GridPredic, qué reglas aplica a cada tabla y por qué. Las reglas salen de tres sitios: el diseño de la Trusted Zone que hicimos el 07.09.2026, el análisis de las bases de datos reales de Calser y TedisNet, y lo que vimos en la asignatura de Big Data Architecture del máster (sesión DataOps/MLOps y el lab P2 de Trusted y Exploitation zone).

## 1. Qué es y qué no es Silver

Bronze es una copia fiel de SQL Server. Silver deja una sola versión válida de cada registro, con tipos correctos, nombres de columna limpios y la distribuidora resuelta. Gold es la que integra entidades, etiqueta y calcula features.

La regla que seguimos es la del lab P2 de la UPC: en esta zona solo se hacen correcciones genéricas, que valen para cualquier análisis posterior. El ejemplo que ponían en clase es el de los outliers: si los quitamos aquí para que el modelo prediga mejor, luego no hay forma de hacer detección de anomalías con los mismos datos, porque hemos borrado justo lo que queríamos detectar. En nuestro caso pasa exactamente eso con los microcortes, los reenganches y los errores de comunicaciones: para la etiqueta sobran, pero como señales precursoras son de lo más valioso que tenemos.

Por eso hay dos acciones y no se mezclan:

- **Rechazar**: solo lo que no sirve para nadie. Calidad mala, valor vacío, sin marca de tiempo, claves rotas, duplicados exactos. Va a una tabla `<entidad>_rejected` con un código de motivo, así que se puede auditar y volver atrás si una regla resulta demasiado agresiva.
- **Marcar**: todo lo dudoso. Duración cero, solapes, microcortes, valores estimados, maniobras, errores de comunicaciones, potencia a cero. La fila se queda con una columna booleana y Gold decide.

Otras reglas de la capa:

- Cada job reconstruye su tabla desde Bronze, así que ejecutarlo dos veces da el mismo resultado. La tabla grande de series (`f_tag_interval_value`) solo sobrescribe los meses que procesa.
- Primero se filtra y después se deduplica. Filtrar es barato y reduce el volumen antes de la ventana, que es lo caro.
- No se imputa nada y no se winsoriza nada. Eso son decisiones de modelado.
- Todas las métricas de calidad van a `l2_silver.dq_metrics`.

## 2. Tablas

| Tabla | Origen | Grano |
| --- | --- | --- |
| `d_elemento` | TedisNet `SystemElements` | elemento, con su distribuidora (ya existía) |
| `d_periodo` | Calser `periodos` | distribuidora, período |
| `d_municipio` | Calser `municipios` | distribuidora, período, municipio |
| `d_tipo_generico` | Calser `tipo_generico` | distribuidora, código |
| `d_ct` | Calser `cts` | distribuidora, período, CT |
| `d_ct_scada` | `d_ct` + `d_elemento` + `SystemNodes` | distribuidora, CT: el puente Calser - TedisNet |
| `d_salida` | Calser `salidas` | distribuidora, período, salida |
| `f_incidencia` | Calser `incidencias` | distribuidora, período, incidencia |
| `f_interrupcion` | Calser `interrupciones` | distribuidora, período, interrupción |
| `d_tag` | TedisNet `SystemTags` + `LibTagClasses` | tag |
| `f_tag_value_change` | `HistoricTagValueChanges` + `SystemTagValueChanges` | cambio de valor |
| `f_tag_quality_event` | los cambios con detalle de calidad no real | tag, instante, detalle |
| `f_evento` | `HistoricEvents` + `SystemEvents` | evento |
| `f_tag_interval_value` | `*TagIntervalValuesBig` + `*TagIntervalValues` | tag, instante (particionada por mes) |
| `f_tag_interval_value_rechazo_diario` | rechazos de la anterior | tag, día, motivo |
| `f_command_execution` | `*CommandExecutions` | ejecución de mando (estado final) |
| `f_corte_evento` | `*ElectricPowerCutEvents` | evento de corte canónico |
| `f_corte_elemento` | `*ElectricPowerCutElementEvents` | evento, elemento afectado |
| `dq_metrics` | todos los jobs | ejecución, entidad, métrica |

Cada entidad tiene además su `<entidad>_rejected` con las filas descartadas y la columna `_motivo`.

Las tablas `d_` son dimensiones y las `f_` hechos, igual que en `d_elemento`. El nombre del job coincide con el de la tabla: `job_silver_d_ct.py` escribe `l2_silver.d_ct`.

## 3. La distribuidora de cada fila

Es lo primero que hay que resolver, porque los códigos de CT se repiten entre distribuidoras y sin ella cualquier cruce es ambiguo.

- **Calser**: cada base de datos declara su distribuidora en `parametros_configuracion`, código `DistributorId`. Ese valor es el `Id` del elemento raíz de la distribuidora en TedisNet (3 EOSA, 2 Pitarch, 1366 Valle de Santa Ana). No es un ordinal: EOSA es la base 1 de los backups pero su id es 3. Si una base Calser tiene filas pero no tiene `DistributorId`, el job se para en vez de seguir con filas sin atribuir.
- **TedisNet**: `d_elemento` sube por `ParentElementId` hasta un elemento de tipo 115. Los elementos de Iberdrola (raíz 6005) quedan con estado `DISTRIBUIDORA_SIN_CALSER`: son el punto frontera y pueden ser una feature, así que no se borran. Tags, cortes y valores heredan la distribuidora de su elemento.

## 4. Calser (etiquetas)

Todas las tablas de Calser están versionadas por período: la clave es siempre `(distribuidora, período, id)`. Un mismo CT en dos períodos son dos versiones legítimas, nunca se colapsan. Para contar CTs reales hay que usar `COUNT(DISTINCT id)`: las tablas tienen unas 200 filas por CT.

Las columnas de fecha y hora que Calser guarda por separado se juntan en un único timestamp (`inicio_ts`, `fin_ts`). El `time` de SQL Server puede llegar a Spark como timestamp de 1970, como tipo TIME o como texto, y la función lo resuelve en los tres casos.

### `f_interrupcion`

| Regla | Acción | Motivo |
| --- | --- | --- |
| Sin id, período o elemento | rechazo `NULL_KEY` | |
| Sin fecha de inicio | rechazo `NO_TIMESTAMP` | |
| Fin anterior al inicio o duración negativa | rechazo `INVALID_INTERVAL` | Calser solo valida esto al guardar; si aparece es un error |
| CT que no existe en `d_ct` en ese período | rechazo `ORPHAN_FK` | |
| PK repetida | rechazo `DUPLICATE_ID` | |
| Misma fila grabada dos veces | rechazo `DUPLICATE_NATURAL_KEY` | 984 filas en EOSA y 1.952 en Pitarch |
| Solape con otra interrupción del mismo CT | flag `en_solape` + `grupo_solape_id` | 1.605 pares en EOSA y 2.388 en Pitarch |
| Duración 0 | flag `duracion_cero` | 815 y 1.255 filas |
| Duración entre 1 y 180 s | flag `es_microcorte` | mismo umbral que `MinimalDurationImportInterruptions` |
| Duración mayor de 7 días | flag `duracion_extrema` | no se winsoriza aquí |
| `INT_DURACION` no cuadra con fin - inicio (más de 60 s) | flag `duracion_incoherente` | |
| Sin fecha de fin | flag `sin_fin` | |
| Sin incidencia | flag `sin_incidencia` | 26 - 28 % en EOSA y Pitarch, asociación manual en Calser |
| Incidencia que no existe | flag `incidencia_inexistente` | |
| Mismo evento en dos períodos | flag `duplicado_otro_periodo` | |
| Período que no cuadra con el mes del inicio | flag `periodo_incoherente` | |
| Importada del SCADA / incompleta | flags `es_origen_scada`, `es_incompleta` | texto de `INT_DESC` |

La clave natural incluye salida, acometida y abonado. Dos salidas del mismo CT cortadas a la misma hora y con la misma duración son dos interrupciones, no una repetida. Cuando hay copias, se queda la que tiene incidencia.

Los solapes no se fusionan aquí. Se agrupan con el algoritmo clásico de intervalos (ordenar por inicio y abrir grupo nuevo cuando el inicio supera el mayor fin visto hasta ese momento; dos intervalos que se tocan no solapan). La regla de fusión, sea el intervalo envolvente o quedarse con el más largo, se aplica en Gold al etiquetar, porque queremos poder cambiarla sin regenerar Silver.

Se eliminan `INT_FECHA_FIN_OPTIMIZADA`, `INT_HORA_FIN_OPTIMIZADA` e `INT_DURACION_OPTIMIZADA`: son una hipótesis manual ("si la reparación hubiera sido más rápida"), no un dato. `fecha_alta` y `ts` se conservan para auditoría, pero son **leakage**: la fecha de alta puede ser semanas posterior al evento y no puede ser una feature.

Se añade `nivel_afectacion` (ABONADO > ACOMETIDA > SALIDA > CT), la misma prioridad que calcula Calser en la aplicación, y `tipo_evento_familia`, porque `INT_TIPO_EVENTO_ID` mezcla valores `CL_*` y `TI_*`.

### Resto de Calser

- `f_incidencia`: flags `es_imprevista` (CL_IMPRE), `es_programada` (CL_PROGR), `es_factor_cliente` (FA_CLIEN), `intervalo_invalido`. `fecha_alta` es leakage.
- `d_ct`: rechaza los CT cuyo municipio no existe en el período. Flags `potencia_cero` (37 % en EOSA, 48 % en Pitarch), `potencia_admin_cero` y `sin_telemetria`. La imputación de la potencia la hace Gold, como la hace Calser.
- `d_ct_scada`: una fila por (distribuidora, CT) con su trafo en TedisNet. La clave que verificamos es `CT_ID` = `SystemElements.ShortName` de un elemento de tipo 145 (TRAFO CT) de la misma distribuidora, comparando como texto y respetando mayúsculas. Añade `tiene_nodo`: un trafo sin nodo en `SystemNodes` nunca entra en el flood fill y no puede aparecer como cortado. Si hay dos trafos con el mismo nombre se marca `mapeo_ambiguo`.
- `d_salida`: el ejemplo de Josep completado con distribuidora, textos limpios, deduplicación y comprobación de que el CT existe en el período.
- `d_periodo`: añade `periodo_norm` (YYYYMM sacado de la fecha de inicio), `fecha_fin_excl`, `es_default` y el formato del id. Para ordenar y particionar se usa siempre la fecha del evento, nunca el id de período.
- `d_tipo_generico`: comprueba que siguen existiendo `CL_IMPRE`, `CL_PROGR`, `FA_CLIEN`, `TI_DETEC` y `TI_MANDO` en cada distribuidora. Si falta alguno la etiqueta del modelo cambiaría sin avisar, así que se marca para revisión.

## 5. TedisNet (features y cortes)

Las tablas `System*` solo guardan la última semana en origen y llegan por CDC; las `Historic*` guardan todo y llegan en batch. La misma fila puede venir por los dos lados, así que cada job une las dos y deduplica dando preferencia a `Historic`, que es la copia definitiva. La columna `_origen` dice de dónde vino cada fila.

### Calidad del dato

Catálogos de TedisNet 3.4: `LibQualities` 1 Buena, 2 Mala, 3 Desconocida, 4 Retenida; `LibQualitySources` 1 Telemedido, 2 Calculado, 3 Manual, 4 Estimado.

| Regla | Acción |
| --- | --- |
| Sin tag | rechazo `NULL_KEY` |
| Sin `SourceTimestamp` ni `UpdateTimestamp` | rechazo `NO_TIMESTAMP` |
| Las cinco columnas de valor a NULL | rechazo `EMPTY_VALUE` |
| Detalle de calidad que indica que el valor no es real (1 - 10, 15 - 20) | rechazo `QUALITY_DETAIL_NOT_REAL` y copia en `f_tag_quality_event` |
| Calidad distinta de Buena o NULL | rechazo `QUALITY_BAD` |
| Fuente Manual | rechazo `SOURCE_MANUAL` |
| Valor NaN | rechazo `NOT_A_NUMBER` |
| Tag que no está en `d_tag` | rechazo `ORPHAN_FK` |
| Fuente Estimado / Calculado | flags `es_estimado`, `es_calculado` |
| Detalle 14 Copiado | flag `es_copiado`, se conserva: es el valor de estado que usa el SCADA para detectar cortes |
| Detalle 11 Excedido / 12 Insuficiente | flag `fuera_rango_egu`: lectura real fuera de rango, puede ser precursora |

Los valores con detalle no real no se pierden: van a `f_tag_quality_event`, porque el número de fallos de comunicaciones por dispositivo es una feature precursora.

### `f_tag_value_change`

Deduplicación por `Id` (Historic antes que System) y después por clave natural `(tag, instante, valor, calidad)`, por si el mismo cambio se reinsertó con otro `Id`. Solo colapsan filas idénticas.

### `f_tag_interval_value`

Es la tabla grande: más de 2.150 millones de filas y 220 GB en `HistoricTagIntervalValuesBig`. El job no la trata nunca entera:

- Procesa un mes cada vez (`--desde 2021-01 --hasta 2026-08`). Bronze no está particionada, pero Delta guarda mínimos y máximos de `SourceTimestamp` por fichero y las filas se escribieron en orden de `Id`, que es orden temporal, así que el filtro del mes se salta casi todos los ficheros.
- La salida está particionada por `fecha_mes` y cada ejecución sobrescribe solo los meses procesados (`replaceWhere`). Se puede reprocesar un mes sin tocar el resto.
- La clave de deduplicación es `(tag, instante)`: la serie es un muestreo sample and hold anclado a la hora y solo puede haber una muestra por tag e instante. `Id` no sirve como clave porque la tabla Big y la pequeña tienen espacios de `Id` distintos.
- Los rechazos no se copian fila a fila (serían unos 470 millones): `f_tag_interval_value_rechazo_diario` guarda el recuento por tag, día y motivo. En la BD real alrededor del 22 % de las filas no tiene calidad Buena, y el umbral de revisión está en el 30 %.

### `d_tag`

Rechaza los tags sin elemento o con un elemento o dispositivo que no existe: no se pueden asociar a ningún CT (la vista `SystemTagDetails` del SCADA los descarta igual). Añade la distribuidora, el nombre de la clase (`AI.INTENS L1`, `DI.DEFECTO DE TIERRA.2`, `ES.POSICIÓN`...) y `tiene_serie`: solo unos 4.000 de los 78.000 tags tienen `StoreInterval` y serie regular.

### `f_corte_evento` y `f_corte_elemento`

TedisNet no mide el corte, lo deduce por topología, y eso deja rastros que hay que limpiar:

- Un cambio de tag no produce un único evento: en la BD real hay hasta 69 eventos idénticos para el mismo `TagValueChangeId`, `Timestamp`, `CutStateId` y elemento raíz. Se colapsan en un evento canónico (el `Id` menor) y los elementos de todas las copias se pasan a ese evento, así no se pierde ningún trafo afectado. `n_eventos_origen` dice cuántos había.
- Eventos sin estado y sin cambio de tag, o sin timestamp: rechazo.
- Eventos que se quedan sin elementos (18,7 % en la BD de pruebas): se conservan con `sin_elementos`.
- `CutStateId = 3` es un error de comunicaciones, no un corte: flag `es_error_comm`.
- `IsCommand` siempre llega a `false`. Un corte se marca como maniobra (`es_maniobra`) cuando su cambio de tag es el de una ejecución de mando (`f_command_execution`, que guarda solo el estado final de cada mando).
- `ElectricalElementType` no está validado en origen (aparecen valores que en realidad son `ElementTypeId`). Fuera de 1 - 4 se pone a NULL y se marca `tipo_electrico_invalido`.
- Cada fila de `f_corte_elemento` lleva el instante y el estado del evento, `ct_id` cuando el elemento es un TRAFO CT y `tiene_nodo`. Gold empareja Off y On por elemento sin tener que volver a cruzar con los eventos.

### `f_evento`

Un evento solo es un puntero a un cambio de tag. Si el cambio no sobrevivió a `f_tag_value_change`, el evento se rechaza. Se le añade el nivel (`LibTagClass_EnumValues_EventLevels`, por clase del tag y valor).

## 6. Métricas de calidad y control final

Cada job escribe en `l2_silver.dq_metrics` filas de entrada, filas de salida, rechazos por motivo y filas marcadas por flag, con el `run_id` de la ejecución del DAG. Cuando el porcentaje de rechazo supera el umbral de la entidad (5 % por defecto, 30 % en las series de valores), la métrica queda con estado `REVISAR`.

El último job, `job_silver_dq_checks.py`, hace tres comprobaciones más:

1. **Integridad referencial** entre las tablas ya limpias: interrupción -> CT -> municipio, salida -> CT, corte_elemento -> corte_evento y -> elemento, cambio de valor -> tag -> elemento, evento -> cambio de valor.
2. **Cobertura de la clave Calser - TedisNet** sobre los CT con interrupciones desde 2021. En el análisis verificado era del 97,8 %; si cae por debajo del 95 % se marca para revisión.
3. **Alineación temporal**. Empareja cada interrupción que Calser importó del SCADA (desde noviembre de 2025) con el Off más cercano del mismo trafo en TedisNet y calcula el desfase. Si la mediana sale en 60 o 120 minutos es que una fuente está en hora local y la otra en UTC, y toda la ventana de predicción de 1 a 3 horas estaría desplazada. Silver no corrige nada por su cuenta: lo mide y lo marca.

Con `"fail_on_review": true` en `config_silver.json`, el DAG falla si queda alguna métrica en `REVISAR` y Gold no se carga. Mientras ajustamos umbrales lo dejamos en `false`.

Consulta útil desde Trino:

```sql
SELECT job, entidad, ambito, metrica, valor, total, pct, umbral_pct, detalle
FROM lakehouse.l2_silver.dq_metrics
WHERE estado = 'REVISAR'
ORDER BY ts DESC;
```

## 7. Ejecución

Con el DAG `dag_silver` en Airflow, que respeta las dependencias entre jobs definidas en `etl/config/02_silver/config_silver.json`. También se puede lanzar un job suelto:

```bash
docker compose exec spark-master \
  spark-submit /app/jobs/02_silver/job_silver_f_interrupcion.py
```

Orden de dependencias:

```text
d_elemento  -> d_tag -> f_tag_value_change -> f_evento
               d_tag -> f_tag_interval_value
d_elemento  -> d_ct -> d_salida
d_municipio -> d_ct -> f_interrupcion
f_incidencia, d_periodo -> f_interrupcion
d_elemento, f_command_execution -> f_corte
todos -> dq_checks
```

Requisito: las tablas `System*` (`SystemElements`, `SystemTags`, `SystemNodes`, `SystemDevices`) llegan por el streaming CDC, así que los jobs de Landing y Bronze streaming tienen que haber corrido al menos una vez.

La prueba local está en `tests/02_silver/test_silver_smoke.py`. Monta tablas Bronze pequeñas con los problemas reales sembrados a propósito (duplicados, solapes, calidad mala, huérfanos, eventos de corte repetidos...), ejecuta todos los jobs en un Spark local y comprueba el resultado. No necesita Docker:

```bash
pip install pyspark==4.2.0 delta-spark==4.4.0 pytest
python -m pytest tests/02_silver -q
```

## 8. Lo que queda fuera de Silver a propósito

- Fusión de solapes, imputación de potencia, winsorización de duraciones y filtro del target (CL_IMPRE, sin FA_CLIEN, más de 180 s, nivel CT): Gold.
- Valores congelados de las series (sample and hold): es una feature y cuesta una ventana sobre 2.000 millones de filas, así que va en Gold y sobre los agregados.
- Rangos físicos de las medidas: dependen de la clase y de la escala de cada tag. Por ahora nos apoyamos en los detalles de calidad 11 y 12 y en las operaciones de anomalía que ya calcula el SCADA (máximo, mínimo, valor congelado).
- Meteorología: se tratará cuando esté la ingesta.
- `SystemTagValues`, `SystemElectricPowerCutElementStates` y `SystemDeviceStates` son snapshots del estado actual. Sirven para el tiempo real, no para entrenar, y se tratarán con el serving.
