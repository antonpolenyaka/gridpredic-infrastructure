"""
Silver fact of the hourly weather of every cell (Open-Meteo, ECMWF IFS).

Bronze keeps what the API gave. Silver leaves one clean version per (cell,
hour), with the same rules as the rest of the layer:

- Names in Spanish and with their unit: temperatura_c, humedad_pct,
  precipitacion_mm, nieve_cm, viento_kmh, racha_kmh, presion_hpa.
- Rows without cell or hour are rejected (NULL_KEY); a repeated (cell,
  hour) keeps the last load (DUPLICATE_KEY).
- A value out of its physical range (RANGES) is set to null and the row is
  flagged fuera_rango, not rejected: the other variables of the hour are
  still good. A gust lower than the mean wind of the same hour is flagged
  racha_menor_viento (it happens with the interpolation of the model).
- Hours missing in a cell are counted (huecos). When the clock goes forward
  in March the local hour 02:00 does not exist, so that jump is not a gap.

The table is small (a few million rows) and is rebuilt whole in every run.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    get_spark,
    keep_first,
    logger,
    parse_args,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "f_meteo_hora"
TARGET_TABLE = silver_table(ENTITY)
SOURCE_TABLE = "l1_bronze.meteo_openmeteo_hora"

# Bronze name -> Silver name, physical range (min, max).
VARIABLES = {
    "temperature_2m": ("temperatura_c", -30.0, 55.0),
    "relative_humidity_2m": ("humedad_pct", 0.0, 100.0),
    "precipitation": ("precipitacion_mm", 0.0, 200.0),
    "snowfall": ("nieve_cm", 0.0, 100.0),
    "wind_speed_10m": ("viento_kmh", 0.0, 250.0),
    "wind_gusts_10m": ("racha_kmh", 0.0, 300.0),
    "pressure_msl": ("presion_hpa", 900.0, 1080.0),
}


def transform(df_in: DataFrame):
    present = {k: v for k, v in VARIABLES.items() if k in df_in.columns}

    df = df_in.select(
        F.trim(F.col("celda_id")).alias("celda_id"),
        F.col("hora").cast("timestamp").alias("hora"),
        *[F.col(k).cast("double").alias(name) for k, (name, _, _) in present.items()],
        (F.col("modelo") if "modelo" in df_in.columns else F.lit(None).cast("string")).alias("modelo"),
        (F.col("audit_loaded_at") if "audit_loaded_at" in df_in.columns
         else F.lit(None).cast("timestamp")).alias("_cargado"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("celda_id").isNull() | F.col("hora").isNull()),
    ])

    kept, duplicated = keep_first(kept, ["celda_id", "hora"], [F.col("_cargado").desc_nulls_last()], "DUPLICATE_KEY")

    out_of_range = F.lit(False)

    for name, low, high in present.values():
        bad = (F.col(name) < F.lit(low)) | (F.col(name) > F.lit(high))
        out_of_range = out_of_range | F.coalesce(bad, F.lit(False))
        kept = kept.withColumn(name, F.when(F.coalesce(bad, F.lit(False)), F.lit(None)).otherwise(F.col(name)))

    kept = kept.withColumn("fuera_rango", out_of_range)

    if "racha_kmh" in kept.columns and "viento_kmh" in kept.columns:
        kept = kept.withColumn(
            "racha_menor_viento", F.coalesce(F.col("racha_kmh") < F.col("viento_kmh"), F.lit(False))
        )

    out = (
        kept.drop("_cargado")
        .withColumn("anio", F.year("hora"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected.drop("_cargado"), duplicated.drop("_cargado")])


def count_gaps(out: DataFrame) -> int:
    """
    Missing hours inside the range of every cell. The jump of the change to
    summer time (last Sunday of March, 01:00 -> 03:00 local) is not a gap.
    """
    ordered = Window.partitionBy("celda_id").orderBy("hora")
    step = out.select(
        "celda_id", "hora",
        (F.unix_timestamp("hora") - F.unix_timestamp(F.lag("hora").over(ordered))).alias("_salto"),
    )
    summer_time = (
        (F.col("_salto") == 7200) & (F.month("hora") == 3) & (F.dayofweek("hora") == 1)
        & (F.dayofmonth("hora") >= 25) & (F.hour("hora") == 3)
    )
    gaps = step.where((F.col("_salto") > 3600) & ~summer_time)
    return int(gaps.agg(F.coalesce(F.sum((F.col("_salto") / 3600 - 1).cast("long")), F.lit(0))).first()[0])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_meteo_hora")
    dq = DQCollector(spark, "job_silver_f_meteo_hora", args.run_id)

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning("%s not found: run the weather Landing and Bronze first. Nothing to do", SOURCE_TABLE)
        return

    df_in = spark.table(SOURCE_TABLE)
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE, partition_by=["anio"])
    write_rejected(rejected, ENTITY, args.run_id)

    total_out = out.count()
    dq.add(ENTITY, "filas_entrada", total_in)
    dq.add(ENTITY, "filas_salida", total_out, total_in)
    dq.add(ENTITY, "celdas", out.select("celda_id").distinct().count())

    for row in rejected.groupBy("_motivo").count().collect():
        dq.add(ENTITY, f"rechazo:{row['_motivo']}", row["count"], total_in, umbral_pct=1.0)

    sums = out.agg(
        F.sum(F.col("fuera_rango").cast("long")).alias("fuera_rango"),
        *([F.sum(F.col("racha_menor_viento").cast("long")).alias("racha_menor_viento")]
          if "racha_menor_viento" in out.columns else []),
        *[F.sum(F.col(name).isNull().cast("long")).alias(f"nulos:{name}")
          for name, _, _ in VARIABLES.values() if name in out.columns],
    ).first().asDict()

    dq.add(ENTITY, "flag:fuera_rango", sums.pop("fuera_rango") or 0, total_out, umbral_pct=0.5)
    if "racha_menor_viento" in sums:
        dq.add(ENTITY, "flag:racha_menor_viento", sums.pop("racha_menor_viento") or 0, total_out, umbral_pct=5.0)
    for metric, value in sums.items():
        dq.add(ENTITY, metric, value or 0, total_out, umbral_pct=1.0)

    dq.add(ENTITY, "huecos_horas", count_gaps(out), total_out, umbral_pct=0.5,
           detalle="missing hours inside the range of each cell, summer time jump excluded")

    rng = out.agg(F.min("hora").alias("desde"), F.max("hora").alias("hasta")).first()
    dq.add(ENTITY, "rango_horas", total_out, detalle=f"{rng['desde']} - {rng['hasta']}")
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
