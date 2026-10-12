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
    and 24 h; rain of 1, 3, 6, 24 and 72 h and of the 29 days before the
    last 24 h (first rain after a dry spell); snow of 24 h; temperature and
    its minimum and maximum of 24 h; humidity and its maximum of 6 h;
    pressure and its change in 3 and 24 h (the passing fronts).
  - metprev_*: the next meteo.prevision_h hours (maximum gust, rain, snow).
    The reanalysis stands for a perfect forecast of the next hours: in
    operation it would be the forecast of AEMET or ECMWF, good at 1 - 3 h
    but not perfect, so it is an upper bound. The training step can leave
    these columns out (meteo.prevision false in config_ml.json).
  - met_region_* / metprev_region_*: the maximum over all the cells, the
    size of the storm.
  - met_cape_* and met_li_min_6h, when Silver has CAPE (from 23.03.2021):
    the instability of the air of the hour and its maximum of 6 and 24 h,
    and the lowest lifted index of 6 h, the conditions of a thunderstorm.
  - When Silver has the extra variables of the reanalysis: wind direction
    (sine and cosine) and wind at 100 m of the hour; soil moisture at 0 - 7
    and 28 - 100 cm (soaked ground lets the trees fall with the wind);
    temperature minus dew point (near zero, dew or fog wet the dirty
    insulators); highest vapour pressure deficit of 24 h and
    evapotranspiration of 7 days (dryness, fires); low clouds of 6 h; and
    the highest weather code of 6 h with a flag for thunderstorm codes.
  - metp1_* and metp2_*, when Silver has the forecasts of ICON-EU (from
    January 2024): what was forecast for the hour one day before (gust,
    wind, rain, snow, temperature and humidity of the hour) and two days
    before (gust and rain). They are passed as they come, with their own
    prefix, so the models of the next hours never take them by mistake: the
    model of the next day uses them in place of the weather of the day.
  - met_aviso_*, when Silver has the warnings of AEMET (f_aviso_meteo, from
    18.06.2018): the highest level (1 yellow, 2 orange, 3 red) of the
    warnings in force at hora that had been issued by then, in total and for
    wind, rain, thunderstorms and snow, and the highest level of the
    warnings that start in the next aviso_prevision_h hours. 0 without
    warning, null before the archive starts.

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

# Extra variables of the reanalysis in Silver -> short name in the window functions.
EXTRA_COLUMNS = {
    "direccion_viento_grados": "dir", "viento_100m_kmh": "v100", "humedad_suelo_0_7_m3m3": "hs07",
    "humedad_suelo_28_100_m3m3": "hs28", "punto_rocio_c": "rocio", "deficit_presion_vapor_kpa": "dpv",
    "nubes_bajas_pct": "nubes", "evapotranspiracion_mm": "et0", "codigo_tiempo": "codigo",
}
# Features that only exist for part of the history (CAPE from 2021, warnings from 2018) or while the
# extra grid loads.
PARTIAL_FEATURES = ("met_cape_", "met_li_", "met_dir_viento_", "met_viento_100m_", "met_humedad_suelo_",
                    "met_rocio_", "met_dpv_", "met_nubes_bajas_", "met_et0_", "met_codigo_", "met_tormenta_",
                    "met_aviso_", "metp1_", "metp2_")

# Forecasts of ICON-EU in Silver -> hourly column of Gold (one and two days before).
FORECAST_COLUMNS = {
    "racha_prev1_kmh": "metp1_racha_1h", "viento_prev1_kmh": "metp1_viento_1h",
    "precipitacion_prev1_mm": "metp1_lluvia_1h", "nieve_prev1_cm": "metp1_nieve_1h",
    "temperatura_prev1_c": "metp1_temp_1h", "humedad_prev1_pct": "metp1_humedad_1h",
    "racha_prev2_kmh": "metp2_racha_1h", "precipitacion_prev2_mm": "metp2_lluvia_1h",
}
WARNINGS_TABLE = "l2_silver.f_aviso_meteo"
# Groups of phenomena of AEMET for the warning features.
WARNING_GROUPS = {"viento": ["VI"], "lluvia": ["PR"], "tormenta": ["TO"], "nieve": ["NE"]}


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
    convective = "cape_jkg" in hourly.columns
    w = hourly.select(
        "celda_id", "hora",
        F.col("racha_kmh").alias("racha"), F.col("viento_kmh").alias("viento"),
        F.col("precipitacion_mm").alias("lluvia"), F.col("nieve_cm").alias("nieve"),
        F.col("temperatura_c").alias("temp"), F.col("humedad_pct").alias("humedad"),
        F.col("presion_hpa").alias("presion"),
        *([F.col("cape_jkg").alias("cape"),
           (F.col("indice_levantamiento") if "indice_levantamiento" in hourly.columns
            else F.lit(None).cast("double")).alias("li")] if convective else []),
        *[F.col(source).alias(alias) for source, alias in EXTRA_COLUMNS.items() if source in hourly.columns],
        *[F.col(source).alias(alias) for source, alias in FORECAST_COLUMNS.items() if source in hourly.columns],
    ).withColumn("_t", F.unix_timestamp("hora"))

    ordered = Window.partitionBy("celda_id").orderBy("_t")

    def past(hours):
        return ordered.rangeBetween(-(hours * 3600 - 1), 0)

    def ahead(hours):
        return ordered.rangeBetween(1, hours * 3600)

    def at(hours):
        return ordered.rangeBetween(-hours * 3600, -hours * 3600)

    def before(start, end):
        """From start hours ago (excluded) to end hours ago (included)."""
        return ordered.rangeBetween(-(start * 3600 - 1), -end * 3600)

    columns = [
        F.col("racha").alias("met_racha_1h"),
        *[F.max("racha").over(past(h)).alias(f"met_racha_max_{h}h") for h in (3, 6, 24)],
        F.col("viento").alias("met_viento_1h"),
        F.col("lluvia").alias("met_lluvia_1h"),
        *[F.sum("lluvia").over(past(h)).alias(f"met_lluvia_{h}h") for h in (3, 6, 24, 72)],
        # Rain of the 29 days before the last 24 h: the first rain after a dry
        # spell washes the dirt of the insulators onto them, and soaked ground
        # lets the wind bring the trees down. Null when the window has less
        # than 90 % of its hours (the start of the series, gaps), not "dry".
        F.when(F.count("lluvia").over(before(30 * 24, 24)) >= int(0.9 * 29 * 24),
               F.sum("lluvia").over(before(30 * 24, 24))).alias("met_lluvia_previa_30d"),
        F.sum("nieve").over(past(24)).alias("met_nieve_24h"),
        F.col("temp").alias("met_temp_1h"),
        F.min("temp").over(past(24)).alias("met_temp_min_24h"),
        F.max("temp").over(past(24)).alias("met_temp_max_24h"),
        F.col("humedad").alias("met_humedad_1h"),
        F.max("humedad").over(past(6)).alias("met_humedad_max_6h"),
        F.col("presion").alias("met_presion_1h"),
        (F.col("presion") - F.first("presion").over(at(3))).alias("met_presion_delta_3h"),
        (F.col("presion") - F.first("presion").over(at(24))).alias("met_presion_delta_24h"),
    ]

    if convective:
        columns += [
            F.col("cape").alias("met_cape_1h"),
            *[F.max("cape").over(past(h)).alias(f"met_cape_max_{h}h") for h in (6, 24)],
            F.min("li").over(past(6)).alias("met_li_min_6h"),
        ]

    present = set(w.columns)
    if "dir" in present:
        columns += [F.sin(F.radians("dir")).alias("met_dir_viento_sin"),
                    F.cos(F.radians("dir")).alias("met_dir_viento_cos")]
    if "v100" in present:
        columns.append(F.col("v100").alias("met_viento_100m_1h"))
    if "hs07" in present:
        columns.append(F.col("hs07").alias("met_humedad_suelo_0_7"))
    if "hs28" in present:
        columns.append(F.col("hs28").alias("met_humedad_suelo_28_100"))
    if "rocio" in present:
        columns.append((F.col("temp") - F.col("rocio")).alias("met_rocio_dif_1h"))
    if "dpv" in present:
        columns.append(F.max("dpv").over(past(24)).alias("met_dpv_max_24h"))
    if "nubes" in present:
        columns.append(F.avg("nubes").over(past(6)).alias("met_nubes_bajas_6h"))
    if "et0" in present:
        columns.append(F.sum("et0").over(past(7 * 24)).alias("met_et0_7d"))
    if "codigo" in present:
        code = F.max("codigo").over(past(6))
        columns += [code.alias("met_codigo_max_6h"), (code >= 95).cast("double").alias("met_tormenta_6h")]

    columns += [F.col(alias) for alias in FORECAST_COLUMNS.values() if alias in present]

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

    names = [c for c in features.columns if c.startswith(("met_", "metprev_", "metp1_", "metp2_"))]
    return features.select("celda_id", "hora", *names), names


def ceil_hour(column):
    """The first whole hour at or after a timestamp."""
    floor = F.date_trunc("hour", column)
    return F.when(floor == column, floor).otherwise(floor + F.expr("INTERVAL 1 HOUR"))


def warning_features(warnings, lead: int = 6):
    """
    (features by (celda_id, hora), names, first hour of the archive) from
    l2_silver.f_aviso_meteo. A warning counts at hora when hora falls in
    (onset, expiry] (the hour that ends at hora overlaps it) and it had been
    sent by then; the upcoming ones, when it starts in the next lead hours.
    Cancellations and green levels do not count.
    """
    w = warnings.where((F.col("nivel_num") >= 1) & (F.coalesce(F.col("tipo_mensaje"), F.lit("Alert")) != "Cancel"))
    w = w.select("celda_id", "fenomeno_codigo", "nivel_num", "enviado_ts", "inicio_ts", "fin_ts")

    def hours(first, last):
        return F.explode(F.sequence(first, last, F.expr("INTERVAL 1 HOUR")))

    in_force = (
        w.withColumn("_desde", ceil_hour(F.greatest(F.col("inicio_ts") + F.expr("INTERVAL 1 SECOND"),
                                                    F.col("enviado_ts"))))
        .withColumn("_hasta", ceil_hour(F.col("fin_ts")))
        .where(F.col("_desde") <= F.col("_hasta"))
        .select("celda_id", "fenomeno_codigo", "nivel_num", hours(F.col("_desde"), F.col("_hasta")).alias("hora"))
    )
    upcoming = (
        w.withColumn("_desde", ceil_hour(F.greatest(F.col("enviado_ts"),
                                                    F.col("inicio_ts") - F.expr(f"INTERVAL {int(lead)} HOURS"))))
        .withColumn("_hasta", F.date_trunc("hour", F.col("inicio_ts") - F.expr("INTERVAL 1 SECOND")))
        .where(F.col("_desde") <= F.col("_hasta"))
        .select("celda_id", "nivel_num", hours(F.col("_desde"), F.col("_hasta")).alias("hora"))
    )

    level = F.col("nivel_num").cast("double")
    aggs = [F.max(level).alias("met_aviso_max_nivel")]
    aggs += [F.max(F.when(F.col("fenomeno_codigo").isin(codes), level)).alias(f"met_aviso_{name}_nivel")
             for name, codes in WARNING_GROUPS.items()]
    current = in_force.groupBy("celda_id", "hora").agg(*aggs)
    ahead = upcoming.groupBy("celda_id", "hora").agg(F.max(level).alias(f"met_aviso_prox{int(lead)}h_max_nivel"))
    features = current.join(ahead, ["celda_id", "hora"], "full")
    names = [c for c in features.columns if c.startswith("met_aviso_")]
    start = warnings.agg(F.min("enviado_ts")).first()[0]
    return features, names, start


def add_warnings(features, names: list, warnings, lead: int):
    """The warning features joined to the weather: 0 without warning, null before the archive starts."""
    table, warning_names, start = warning_features(warnings, lead)
    if start is None:
        return features, names
    out = features.join(table, ["celda_id", "hora"], "left")
    known = F.col("hora") >= F.lit(start)
    for name in warning_names:
        out = out.withColumn(name, F.when(known, F.coalesce(F.col(name), F.lit(0.0))))
    return out, names + warning_names


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
    if spark.catalog.tableExists(WARNINGS_TABLE):
        features, names = add_warnings(features, names, spark.table(WARNINGS_TABLE),
                                       int(cfg.get("aviso_prevision_h", 6)))
    else:
        logger.info("%s not found: no warnings of AEMET in Gold", WARNINGS_TABLE)
    write_table(
        features.withColumn("anio", F.year("hora")).withColumn("audit_loaded_at", F.current_timestamp()),
        WEATHER_TABLE, partition_by=["anio"],
    )

    written = spark.table(WEATHER_TABLE)
    rows = written.count()
    dq.add("meteo_celda_hora", "filas", rows, detalle=f"{len(names)} features, {time.time() - start:.0f} s")
    nulls = written.agg(*[F.sum(F.col(n).isNull().cast("long")).alias(n) for n in names]).first().asDict()
    for name, value in nulls.items():
        # The first and last hours of the window have incomplete windows. The
        # convective features only exist from 23.03.2021 and the extra grid
        # may still be loading: no threshold.
        partial = name.startswith(PARTIAL_FEATURES)
        dq.add("meteo_celda_hora", f"nulos:{name}", value or 0, rows, umbral_pct=None if partial else 2.0)
    dq.flush()

    logger.info("Gold completed: %s and %s", MAP_TABLE, WEATHER_TABLE)


if __name__ == "__main__":
    main()
