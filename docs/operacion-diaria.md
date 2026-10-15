# Operación diaria: qué corre, cuándo, qué deja y qué mirar si falla

Este documento es el manual de la operación en marcha: los tres DAG de Airflow que mantienen el cuadro de mando al día, los ficheros que dejan, los avisos que mandan y qué hacer cuando algo no sale. El porqué de cada pieza está en [causas-incidencias.md](causas-incidencias.md) (apartado 10) y los resultados en [caso-de-negocio.md](caso-de-negocio.md).

## 1. El ciclo de un día

| Hora (Madrid) | DAG y tarea | Qué hace | Qué deja |
| --- | --- | --- | --- |
| 20:30 | `dag_riesgo_manana` / `dia` | Decide el día que se predice (mañana) | - |
| 20:30 | `prevision` | Previsión de ICON-EU de la víspera para las celdas de las tres redes (Open-Meteo, sin clave) | `00_landing/vivo/<día>/prevision/<celda>.json` |
| 20:30 | `avisos` | Avisos de AEMET en vigor para el día (clave de AEMET) | `00_landing/vivo/<día>/avisos/` |
| 20:30 | `ensemble` | Los 40 escenarios de ICON-EU EPS para las celdas de cada red (Open-Meteo, sin clave); 44 celdas, menos de un minuto | `00_landing/vivo/<día>/ensemble/<celda>.json` y `dashboard/data/ensemble.json` (+ `ensemble_<día>.json`) |
| 20:30 | `aemet_municipios` | Predicción horaria de AEMET por municipios, su propio modelo (73 municipios, unos 6 minutos por el límite de peticiones de AEMET) | `00_landing/vivo/<día>/aemet/<ine>.json` y `dashboard/data/aemet.json` (+ `aemet_<día>.json`) |
| 20:33 - 20:40 | `puntuar` | `job_ml_riesgo_manana` en Spark: reentrena con todos los días etiquetados, puntúa el día por distribuidora, línea, municipio y CT, calcula la franja, las brigadas y las bases; adjunta los escenarios y la predicción de AEMET; y, con `riesgo_manana.variante_pasado_manana` puesta (`operativa_48h`, encendida el 15.10.2026 tras el backtest a 48 y 72 h; `null` la apaga), con la misma previsión y los mismos avisos puntúa pasado mañana con el modelo de dos tardes antes (orientación, sin zonas ni guardia) | `l3_gold.riesgo_dia_siguiente`, `l3_gold.riesgo_pasado_manana`, `dashboard/data/riesgo.json` (+ `riesgo_<día>.json`, con el bloque `pasado_manana`), `parte_guardia.xlsx`, `indice.json`, carpeta de exportación |
| al acabar | `avisar` | El mensaje de la tarde al webhook si alguna distribuidora está en alto o muy alto o con la guardia reforzada por un aviso de viento o tormenta | mensaje en Slack, Teams o el endpoint de `AVISO_WEBHOOK_URL` |
| cada 15 min | `dag_rayos_vivo` / `rayos` | Rayos del Lightning Imager de MTG de la última hora y media (EUMETSAT, claves) | `00_landing/vivo/rayos/<día>/` y `dashboard/data/rayos.json` |
| cada 15 min | `avisar_rayos` | Rayos a menos de 10 km de una red al webhook, una vez por distribuidora y hora | `dashboard/data/rayos_aviso.json` (cuándo se avisó) |
| cada hora, a y 20 | `dag_observacion_vivo` / `observar` | Las últimas 24 horas de las estaciones de AEMET de la caja de estudio (clave de AEMET); verifica la previsión de ayer y de hoy frente a lo medido | `00_landing/vivo/observacion/<día>/obs_<hhmm>.json`, `dashboard/data/observado.json`, `dashboard/data/verificacion.json` |
| cada hora, a y 20 | `avisos_hoy` | Vuelve a pedir a AEMET los avisos en vigor, busca los que cubren hoy los municipios de cada red (uno de cada cinco avisos de un día llega después de las 20:30; dos de cada cinco de los de tormenta), recalcula la guardia y, cuando un aviso nuevo de viento o tormenta la sube a reforzada, lo manda al webhook una vez por distribuidora y día | `00_landing/vivo/<día>/avisos_hoy/<área>_<hhmm>.tar`, `dashboard/data/avisos_hoy.json`, `avisos_hoy_aviso.json` (a quién se avisó) |
| 07:20 (la primera ejecución desde `aviso.parte_manana_hora`) | `parte_manana` | El parte de la mañana al webhook: lo que dijimos para ayer y lo que midieron las estaciones (pico previsto y medido, franja, banda, AEMET, aviso) y lo que dice el sistema para hoy (nivel, franja, pico), una vez al día | mensaje en el webhook; `dashboard/data/parte_manana.json` (el último día enviado) |

Las tareas `ensemble`, `aemet_municipios` y `avisos` nunca bloquean la puntuación: sin respuesta se saltan y el día se puntúa sin ellas (la fila lo dice). Sin la previsión de ICON-EU no hay puntuación, y la tarea reintenta dos veces con diez minutos entre medias.

## 2. Lo que ve el usuario

- `http://localhost:8086` (`index.html`): el riesgo de mañana (o de hoy, durante el día) por distribuidora, con la etiqueta "guardia reforzada" cuando toca reforzar la guardia (nivel alto o muy alto, o aviso de viento o de tormenta de AEMET para el día: la política que más cubre por día, apartado 2b de [caso-de-negocio.md](caso-de-negocio.md)), con el perfil de racha por horas (barras), la banda de los escenarios (morado), la predicción de AEMET (verde) y lo medido por las estaciones (rojo), y una línea de confianza en la previsión del viento (alta, media o baja según coincidan ICON-EU y AEMET en el pico y según lo repartidos que estén los escenarios); los rayos ahora; las líneas con su nivel, lo que pasa ahora cerca de cada una y el riesgo de las próximas tres horas; las brigadas y sus bases; y al final, la tabla de lo que dijimos cada tarde y lo que pasó con el viento. Si la ejecución de anoche no dejó riesgo nuevo, lo dice en naranja arriba. Debajo de la guardia, la línea "Pasado mañana": el nivel de cada red a dos días, orientativo (el modelo de dos tardes antes conserva la mayor parte de la señal, 5,4 frente a 6,9 veces la tasa base, y pierde sobre todo los avisos que AEMET aún no ha emitido; apartado 3e de [investigacion-continua.md](investigacion-continua.md)); en cada bloque, la misma cifra con la racha prevista. El mensaje de la tarde la añade en una línea cuando alguna red queda en alto o muy alto a dos días.
- Todas las páginas llevan arriba el mismo menú (Riesgo de mañana, Días archivados, Día a día 2022 - 2026, Negocio y cifras, Airflow): no hace falta saber cómo se llama ningún fichero. Comparten un mismo sistema de diseño (`dashboard/gridpredic.css`: colores, tipografía IBM Plex Sans servida desde `dashboard/vendor/fonts`, tarjetas, tablas, insignias de riesgo; el movimiento discreto va en `dashboard/motion.js` con GSAP y ScrollTrigger en local y respeta "reducir movimiento"), con tema claro por defecto y oscuro si el sistema lo pide, y funcionan de 360 px (móvil) a 1920 px. Cada red se lee de arriba abajo: el nivel y las cifras clave, el mapa y el perfil de racha, las líneas, y al final las causas y las brigadas.
- `dias.html` (Días archivados): la lista de todos los días con riesgo guardado, de las ejecuciones de las 20:30 y de los días repetidos desde Gold, con el nivel, la probabilidad, la guardia, el aviso de AEMET y la franja de cada red, filtros por año y nivel, y para cada día el enlace al cuadro de mando de ese día (`index.html?dia=AAAAMMDD`), a su parte de guardia y al JSON. Abajo explica qué hay en cada página y cómo repetir un día que no está. Lee `data/indice.json`, que escribe el propio job del riesgo cada vez que guarda un día (vivo o repetido), con lo que hay en la carpeta.
- `historial.html`: lo que habría dicho el sistema cada tarde desde 2022, día a día, y lo que pasó.
- Las redes se llaman Distribuidora 1, 2 y 3 en todas las páginas, los partes y los mensajes: el nombre sale de `riesgo_manana.nombres` en `config_ml.json` (1 = id 3, 2 = id 2, 3 = id 1366) y las páginas las ordenan por ese nombre. Los ficheros escritos antes del cambio se pasan a los nombres nuevos con `python _runlogs/anonimizar_dashboard.py dashboard/data` (solo cambia el nombre de la distribuidora; municipios y líneas se quedan como están).
- `negocio.html`: las cifras para una distribuidora o un inversor, la guardia a igual presupuesto frente a los calendarios fijos y a los avisos de AEMET, y lo que vale la previsión medido en producción. Se imprime a PDF.
- `data/parte_guardia.xlsx`: el parte de guardia de mañana, con los escenarios y la predicción de AEMET en la hoja Resumen.
- El webhook (opcional, `AVISO_WEBHOOK_URL` en `.env`): el mensaje de la tarde cuando hay alerta o guardia reforzada, el parte de la mañana a las 07:20 (ayer frente a lo medido, y hoy), los avisos nuevos de AEMET que suben la guardia durante el día, los rayos sobre la red y las tareas que fallan del todo.

## 3. Qué mirar si algo no sale

| Síntoma | Dónde mirar | Qué hacer |
| --- | --- | --- |
| La cabecera dice en naranja que el riesgo es de otro día | Airflow (http://localhost:8080), `dag_riesgo_manana`, tarea `prevision` o `puntuar` | Si falló la previsión, Open-Meteo no respondió: relanzar el DAG con el día en `fecha`. Si falló `puntuar`, el log de Spark dice la causa (memoria, tabla que falta); relanzar solo esa tarea |
| No hay banda morada ni línea verde | `ensemble.json` o `aemet.json` de otro día, o las tareas saltadas | Lanzar a mano `job_landing_vivo.py --fuente ensemble` o `--fuente aemet_municipios` con `--fecha`; la página los coge sola al recargar |
| No hay línea roja de las estaciones | `dag_observacion_vivo` saltado (falta `AEMET_API_KEY`) o AEMET sin datos | Comprobar la clave en `.env` y `docker compose up -d airflow`; lanzar `job_landing_observacion.py` a mano |
| Rayos ahora dice que el satélite no responde | `dag_rayos_vivo`, claves de EUMETSAT, red del portátil | Suele ser la red; el siguiente run reconstruye el JSON con lo que hay en la Landing |
| El mensaje de la tarde no llega | `avisar` saltada ("sin url" o "sin alerta") | Sin URL no se manda; con URL, el log dice el estado HTTP del webhook |
| La guardia no cambia aunque AEMET haya sacado un aviso | `avisos_hoy` de `dag_observacion_vivo` (saltada sin clave o sin respuesta de AEMET) | `avisos_hoy.json` lleva `generado`; la tarea corre a y 20 de cada hora y solo sube la guardia con avisos de viento o tormenta que cubran los municipios de la red |
| El parte de la mañana no llega | `parte_manana` de `dag_observacion_vivo` ("sin url", "sin datos", "ya enviado") | Necesita `verificacion.json` con el bloque de ayer o `riesgo.json` de hoy; se manda en la primera ejecución desde las 7 (hora local) y una vez al día |
| Un cambio en un DAG no aparece en Airflow, o `monitor/health` dice `dag_processor: unhealthy` | El dag-processor del contenedor (Airflow en modo standalone sobre SQLite) se muere con "database is locked" (pasó el 14.10 a las 20:42); el scheduler sigue lanzando los DAG ya serializados, pero no se vuelve a leer ningún fichero | Desde el 14.10 SQLite espera 60 s por el bloqueo en vez de 5 (`etl/config/airflow_sqlite.py`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS` en `compose.yaml`), que es lo que lo provocaba. Si vuelve a pasar: `docker compose restart airflow` (un minuto, no toca Spark); el vigilante `_runlogs\vigilar_airflow.ps1` lo hace solo cuando lleva más de 20 minutos muerto y Spark está vacío |
| La tabla de los últimos días no crece | `verificacion.json` necesita `riesgo_<día>.json` de ayer y el archivo de observaciones de ayer | Comprobar que `dag_riesgo_manana` corrió anoche y que `dag_observacion_vivo` está activo |

Los comandos de cada job, con sus opciones, están en el apartado 10 de [causas-incidencias.md](causas-incidencias.md); las claves y variables de entorno, en `.env.example`.

## 3b. Repetir un día pasado (la demo de un temporal)

Para enseñar lo que el sistema habría dicho la tarde anterior a un día conocido, sin tocar lo vivo:

```
docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \
    --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_ml_horas_riesgo.py,/app/jobs/04_ml/job_ml_meteo_historia.py,/app/jobs/04_ml/job_ml_riesgo_dia.py,/app/jobs/04_ml/riesgo_zonas.py,/app/jobs/04_ml/calendario.py \
    /app/jobs/04_ml/job_ml_riesgo_manana.py --config /app/config/04_ml/config_ml.json \
    --fecha 2026-01-28 --desde-gold --solo-archivo --run-id demo_20260128
```

`--desde-gold` toma la previsión que ICON-EU hizo la víspera de Gold (`metp1_*` de `meteo_celda_hora`, desde enero de 2024) y los avisos de AEMET enviados antes de las 20:00 de la víspera del archivo de Silver, entrena solo con los días anteriores y deja lo mismo que una tarde cualquiera: la fila en `l3_gold.riesgo_dia_siguiente`, las zonas, `resumen.md` y, con `--solo-archivo`, solo las copias con fecha en el cuadro de mando (`riesgo_20260128.json`, `parte_guardia_20260128.xlsx`), sin pisar `riesgo.json`. `http://localhost:8086/index.html?dia=20260128` lo enseña con la cabecera "día repetido"; la misma dirección sirve para cualquier día archivado por la operación (`riesgo_<día>.json`, con sus `ensemble_<día>.json` y `aemet_<día>.json` si los hay).

## 4. Lo que se acumula y para qué sirve

- `riesgo_<día>.json`, `ensemble_<día>.json` y `aemet_<día>.json` guardan lo que se dijo cada tarde; `verificacion.json` compara cada día las tres previsiones con las estaciones (pico previsto y medido, hora, franja, banda, umbrales) y resume por distribuidora el sesgo y el error de cada previsión y qué parte de los días el pico cayó en la banda. Con tres semanas en marcha se puede decir con datos cuál de las previsiones se acerca más al viento medido y si los escenarios están bien calibrados.
- Las incidencias reales de cada día llegan con la siguiente copia de Calser; con ella, `job_ml_caso_negocio.py` vuelve a puntuar esos días y el historial dice si las alertas acertaron.
- Cada ejecución de la tarde queda en `l3_gold.riesgo_dia_siguiente` y `l3_gold.ml_runs`, con su `run_id`, para repetirla o compararla.
- `sector.json` (la sección "Dónde está cada red frente al sector" de `negocio.html`) no cambia cada día: lo deja `job_analisis_red.py --dashboard-dir /app/dashboard/data` con la copia anual de Calser, y los índices oficiales se renuevan con `extract_gecos_indices.py` cuando el Ministerio publica un año nuevo (fuente `calidad_sector` de los datos públicos).

## 5. Apagar y volver a encender el portátil

Todo el sistema vive en Docker sobre el disco D: y sobrevive a un apagado normal de Windows; lo único que no se levanta solo son las dos ventanas de PowerShell.

Antes de apagar:

1. Mirar `_runlogs\cola\historial.log`: si un trabajo de la cola sigue en marcha (línea "INICIO" sin su "FIN"), esperar a que acabe o crear el fichero `_runlogs\cola\parar` (la cola termina el trabajo en curso y se para). Cerrar la ventana a medias no rompe nada: cada tabla Delta se escribe entera o no se escribe, y el trabajo se vuelve a encolar.
2. Cerrar las ventanas de `cola.ps1` y `vigilar_airflow.ps1`.
3. Docker no se toca: todos los contenedores llevan `restart: unless-stopped`. Apagar Windows (mejor apagar que hibernar); nunca "Quit" y "Reset to factory defaults" en Docker Desktop.

Al encender:

1. Con el disco D: conectado antes de iniciar sesión; Docker Desktop no debe arrancar sin D:. Si no arranca solo, abrirlo cuando D: esté en el Explorador.
2. `docker compose -f D:\GitHub\gridpredic-infrastructure\compose.yaml ps`: todo "Up" y "healthy". Si falta alguno, `docker compose -f D:\GitHub\gridpredic-infrastructure\compose.yaml up -d`. Para una revisión completa, `& "D:\GitHub\gridpredic-infrastructure\_runlogs\comprobar_disco_d.ps1"`.
3. Abrir de nuevo `& "D:\GitHub\gridpredic-infrastructure\_runlogs\cola.ps1"` y `& "D:\GitHub\gridpredic-infrastructure\_runlogs\vigilar_airflow.ps1"`.
4. La ejecución de las 20:30 corre sola si Airflow está arriba antes de esa hora. Si el portátil se enciende más tarde, Airflow no recupera la hora pasada (`catchup=False`): lanzar `dag_riesgo_manana` a mano en http://localhost:8080 (Trigger) o con `docker compose -f ... exec airflow airflow dags trigger dag_riesgo_manana`. Las observaciones y los rayos se reanudan solos en su siguiente hora.
