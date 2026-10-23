# Conciliación de SQL Server con Bronze (20.10.2026)

Compara las filas de cada tabla en el origen con las de su tabla en `l1_bronze`, para las 90 cargas por lotes y las 29 tablas System* del CDC. Es de solo lectura y no lanza Spark.

| Fichero | Qué es |
|---|---|
| `conciliar_bronze.ps1` | El script. Recuento por metadatos (`sys.dm_db_partition_stats`) en SQL Server y `COUNT_BIG(*)` exacto en las tablas de hasta 2 millones de filas; en Bronze, `count(*)` con Trino. |
| `salida_conciliar_bronze_20.10.2026.log` | Registro de la ejecución, con el resumen y las diferencias esperadas por tipo de carga. |
| `conciliacion_20.10.2026.csv` | Tabla completa, una fila por carga. |
| `historic_events_trino.ps1` y `historic_events_airflow.ps1` | Los dos pasos con los que miramos por qué `HistoricEvents` no está en Bronze: tablas de eventos en Bronze y `f_evento` en Silver, y el registro de Airflow. |
| `salida_historic_events_*_20.10.2026.log` | Sus registros. |

## Resultado

- Lotes: 90 cargas, 84 iguales, 5 con el origen vacío (Bronze no crea la tabla, es lo esperado) y 1 que falta en Bronze (`HistoricEvents`, 2.757.447 filas en el origen). Ninguna con diferencia. Filas comparadas: 2.159.477.900 en el origen y en Bronze.
- CDC: 29 cargas, 21 iguales y 8 con el origen vacío. Ninguna con diferencia. 6.378.220 filas en el origen y en Bronze.
- `HistoricTagIntervalValuesBig`: 2.152.292.109 filas en los dos lados.

Las copias de SQL Server están quietas desde el 14.08.2026, por eso se espera diferencia cero en las cargas completas.

## Cómo repetirla

Desde la raíz del repositorio, con la pila en marcha (`startGridPredic_windows.ps1`):

    pwsh -File experimentos\conciliacion\conciliar_bronze.ps1

Las rutas del script son las del portátil donde se ejecutó (`D:\GitHub\gridpredic-infrastructure`).
