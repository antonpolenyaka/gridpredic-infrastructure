"""
Gold weather: the features of every cell and hour and the cell of every CT.

- map_ct_celda: (distribuidora_id, ct_id) -> celda_id and the coordinates of
  the municipality. The municipality of the CT in Calser (dim_ct.municipio_id,
  the INE code) is matched with l2_silver.d_meteo_celda by code and, when
  the code fails, by the normalised name. cruce says which one matched.
- meteo_celda_hora: one row per (celda_id, hora). A row of Silver at hora
  describes the hour that ends then, so it is known at hora, like the rest
  of the features of Gold.
  - met_*: the last hours. Gust and wind of the hour; maximum gust of 3, 6
    and 24 h; rain of 1, 3, 6 and 24 h; snow of 24 h; temperature and its
    minimum and maximum of 24 h; humidity; pressure and its change in 3 and
    24 h (the passing fronts).
  - metprev_*: the next meteo.prevision_h hours (maximum gust, rain, snow).
    The reanalysis stands for a perfect forecast of the next hours: in
    operation it would be the forecast of AEMET or ECMWF, good at 1 - 3 h
    but not perfect, so it is an upper bound. The training step can leave
    these columns out (meteo.prevision false in config_ml.json).
  - met_region_* / metprev_region_*: the maximum over all the cells, the
    size of the storm.

Both tables are small and are rebuilt whole. They are not joined into
features_ct_hora: the training step joins them by (celda_id, hora), so the
weather can be added or changed without rebuilding the big tables of Gold.
"""

import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    get_spark,
    gold_table,
    load_params,
    logger,
    parse_args,
    silver_table,
    write_table,
)


WEATHER_TABLE = gold_table("meteo_celda_hora")
MAP_TABLE = gold_table("map_ct_celda")


def normalized_name(column):
    plain = F.translate(F.lower(F.trim(column)), "áàäâéèëêíìïîóòöôúùüûñç", "aaaaeeeeiiiioooouuuunc")
    return F.regexp_replace(plain, "[^a-z]", "")


def ct_cells(dim, cells):
    """map_ct_celda: every CT of dim_ct with its cell, by INE code or by name."""
    ref = cells.select(
        "codigo_ine", "nombre_normalizado", "celda_id",
        F.col("latitud").alias("ct_lat_municipio"), F.col("longitud").alias("ct_lon_municipio"),
    )
    by_ine = ref.dropDuplicates(["codigo_ine"]).drop("nombre_normalizado")
    by_name = ref.dropDuplicates(["nombre_normalizado"]).select(
        "nombre_normalizado",
        F.col("celda_id").alias("_celda_n"),
        F.col("ct_lat_municipio").alias("_lat_n"),
        F.col("ct_lon_municipio").alias("_lon_n"),
    )

    cts = dim.select(
        "distribuidora_id", "ct_id",
        F.lpad(F.trim(F.col("municipio_id").cast("string")), 5, "0").alias("codigo_ine"),
        normalized_name(F.col("municipio_nombre")).alias("nombre_normalizado"),
        F.col("municipio_nombre"),
    ).dropDuplicates(["distribuidora_id", "ct_id"])

    return (
        cts.join(F.broadcast(by_ine), "codigo_ine", "left")
        .join(F.broadcast(by_name), "nombre_normalizado", "left")
        .select(
            "distribuidora_id", "ct_id", "codigo_ine", "municipio_nombre",
            F.coalesce("celda_id", "_celda_n").alias("celda_id"),
            F.coalesce("ct_lat_municipio", "_lat_n").alias("ct_lat_municipio"),
            F.coalesce("ct_lon_municipio", "_lon_n").alias("ct_lon_municipio"),
            F.when(F.col("celda_id").isNotNull(), "ine").when(F.col("_celda_n").isNotNull(), "nombre")
            .otherwise("sin_celda").alias("cruce"),
        )
    )


def weather_features(hourly, lead: int, region: bool):
    """(features DataFrame, feature names) from l2_silver.f_meteo_hora."""
    w = hourly.select(
        "celda_id", "hora",
        F.col("racha_kmh").alias("racha"), F.col("viento_kmh").alias("viento"),
        F.col("precipitacion_mm").alias("lluvia"), F.col("nieve_cm").alias("nieve"),
        F.col("temperatura_c").alias("temp"), F.col("humedad_pct").alias("humedad"),
        F.col("presion_hpa").alias("presion"),
    ).withColumn("_t", F.unix_timestamp("hora"))

    ordered = Window.partitionBy("celda_id").orderBy("_t")

    def past(hours):
        return ordered.rangeBetween(-(hours * 3600 - 1), 0)

    def ahead(hours):
        return ordered.rangeBetween(1, hours * 3600)

    def at(hours):
        return ordered.rangeBetween(-hours * 3600, -hours * 3600)

    columns = [
        F.col("racha").alias("met_racha_1h"),
        *[F.max("racha").over(past(h)).alias(f"met_racha_max_{h}h") for h in (3, 6, 24)],
        F.col("viento").alias("met_viento_1h"),
        F.col("lluvia").alias("met_lluvia_1h"),
        *[F.sum("lluvia").over(past(h)).alias(f"met_lluvia_{h}h") for h in (3, 6, 24)],
        F.sum("nieve").over(past(24)).alias("met_nieve_24h"),
        F.col("temp").alias("met_temp_1h"),
        F.min("temp").over(past(24)).alias("met_temp_min_24h"),
        F.max("temp").over(past(24)).alias("met_temp_max_24h"),
        F.col("humedad").alias("met_humedad_1h"),
        F.col("presion").alias("met_presion_1h"),
        (F.col("presion") - F.first("presion").over(at(3))).alias("met_presion_delta_3h"),
        (F.col("presion") - F.first("presion").over(at(24))).alias("met_presion_delta_24h"),
    ]

    if lead > 0:
        columns += [
            F.max("racha").over(ahead(lead)).alias(f"metprev_racha_max_{lead}h"),
            F.sum("lluvia").over(ahead(lead)).alias(f"metprev_lluvia_{lead}h"),
            F.sum("nieve").over(ahead(lead)).alias(f"metprev_nieve_{lead}h"),
        ]

    features = w.select("celda_id", "hora", *columns)

    if region:
        sources = ["met_racha_max_3h", "met_lluvia_3h"] + ([f"metprev_racha_max_{lead}h"] if lead > 0 else [])

        def region_name(column):
            if column.startswith("metprev_"):
                return column.replace("metprev_", "metprev_region_", 1)
            return column.replace("met_", "met_region_", 1)

        region_df = features.groupBy("hora").agg(*[F.max(c).alias(region_name(c)) for c in sources])
        features = features.join(region_df, "hora", "left")

    names = [c for c in features.columns if c.startswith(("met_", "metprev_"))]
    return features.select("celda_id", "hora", *names), names


def main():
    args = parse_args()
    params = load_params(args)
    cfg = params.get("meteo", {})
    spark = get_spark("job-gold-meteo_celda_hora")
    dq = DQCollector(spark, "job_gold_meteo_celda_hora", args.run_id)

    hourly_table, cells_table = silver_table("f_meteo_hora"), silver_table("d_meteo_celda")

    if not (spark.catalog.tableExists(hourly_table) and spark.catalog.tableExists(cells_table)):
        logger.warning("%s or %s not found: no weather in Gold. Nothing to do", hourly_table, cells_table)
        return

    mapping = ct_cells(spark.table(gold_table("dim_ct")), spark.table(cells_table))
    write_table(mapping.withColumn("audit_loaded_at", F.current_timestamp()), MAP_TABLE)

    located = {r["cruce"]: r["count"] for r in spark.table(MAP_TABLE).groupBy("cruce").count().collect()}
    total_cts = sum(located.values())
    for cruce in ("ine", "nombre", "sin_celda"):
        dq.add("map_ct_celda", f"cts:{cruce}", located.get(cruce, 0), total_cts,
               umbral_pct=5.0 if cruce == "sin_celda" else None)

    start = time.time()
    features, names = weather_features(spark.table(hourly_table), int(cfg.get("prevision_h", 3)),
                                       bool(cfg.get("region", True)))
    write_table(
        features.withColumn("anio", F.year("hora")).withColumn("audit_loaded_at", F.current_timestamp()),
        WEATHER_TABLE, partition_by=["anio"],
    )

    written = spark.table(WEATHER_TABLE)
    rows = written.count()
    dq.add("meteo_celda_hora", "filas", rows, detalle=f"{len(names)} features, {time.time() - start:.0f} s")
    nulls = written.agg(*[F.sum(F.col(n).isNull().cast("long")).alias(n) for n in names]).first().asDict()
    for name, value in nulls.items():
        # The first and last hours of the window have incomplete windows.
        dq.add("meteo_celda_hora", f"nulos:{name}", value or 0, rows, umbral_pct=2.0)
    dq.flush()

    logger.info("Gold completed: %s and %s", MAP_TABLE, WEATHER_TABLE)


if __name__ == "__main__":
    main()
