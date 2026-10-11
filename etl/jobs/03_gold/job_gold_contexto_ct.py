"""
Gold context of every CT from the public data, one row per CT and year:
l3_gold.contexto_ct_anio.

The year of a row is the year a model predicts or describes, and the row only
uses what was published before that year started:

- Population of the municipality of the CT at 1 January of the year before
  (the INE publishes the Padron of 1 January of a year at the end of that
  year), its change in one year and its yearly change in five, and the
  density (inhabitants per km2 of the municipality).
- kVA installed and customers of all the CTs of the municipality per
  inhabitant (topology of today, as dim_ct), a measure of how loaded the
  network of a growing town is.
- Surface and altitude of the municipality (Wikidata and the elevation model)
  and the surroundings of the weather cell of the CT (l2_silver.d_celda_entorno):
  relief and the OpenStreetMap exposure (overhead lines, poles, roads and
  tracks, forest and scrub, buildings, construction sites), prefixed celda_.

The municipality of a CT is matched by INE code and, when the code fails, by
the normalised name (cruce says which one). Columns of a source that has not
been loaded stay null.

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        /app/jobs/03_gold/job_gold_contexto_ct.py --config /app/config/03_gold/config_gold.json
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    get_spark,
    gold_table,
    load_params,
    logger,
    optional_table,
    parse_args,
    require_table,
    silver_table,
    write_table,
)


TARGET_TABLE = gold_table("contexto_ct_anio")

CELL_COLUMNS = [
    "elev_media_m", "elev_std_m", "desnivel_m", "osm_disponible", "dens_linea_aerea_km_km2", "dens_apoyos_km2",
    "dens_carreteras_km_km2", "dens_pistas_km_km2", "pct_bosque", "pct_matorral", "dens_edificios_km2",
    "pct_cable", "km2_obras", "km_linea_aerea", "n_trafos_osm",
]


def normalized_name(column):
    plain = F.translate(F.lower(F.trim(column)), "áàäâéèëêíìïîóòöôúùüûñç", "aaaaeeeeiiiioooouuuunc")
    return F.regexp_replace(plain, "[^a-z]", "")


def ine_code(column):
    digits = F.regexp_replace(F.trim(column.cast("string")), "[^0-9]", "")
    return F.when(F.length(digits) >= 6, F.substring(digits, 1, 5)).otherwise(F.lpad(digits, 5, "0"))


def base_cts(dim, cells):
    kva = F.coalesce(
        F.when(F.col("potencia_tiepi_kva") > 0, F.col("potencia_tiepi_kva")),
        F.col("potencia_kva"),
        F.lit(0.0),
    )
    cts = dim.select(
        "distribuidora_id", "ct_id", ine_code(F.col("municipio_id")).alias("codigo_ine"),
        normalized_name(F.col("municipio_nombre")).alias("nombre_normalizado"),
        kva.cast("double").alias("kva"), F.coalesce(F.col("num_abonados"), F.lit(0)).cast("double").alias("abonados"),
    ).dropDuplicates(["distribuidora_id", "ct_id"])

    if cells is not None:
        cts = cts.join(cells.select("distribuidora_id", "ct_id", "celda_id"), ["distribuidora_id", "ct_id"], "left")
    else:
        cts = cts.withColumn("celda_id", F.lit(None).cast("string"))

    return cts


def population_by_code(cts, population):
    """codigo_ine_padron of every CT: its own code when the Padron has it, otherwise the code of the same name."""
    codes = population.select("codigo_ine").distinct().withColumn("_por_codigo", F.lit(True))
    names = (
        population.select("codigo_ine", normalized_name(F.col("municipio")).alias("nombre_normalizado"))
        .dropDuplicates(["nombre_normalizado"])
        .withColumnRenamed("codigo_ine", "_codigo_por_nombre")
    )
    return (
        cts.join(codes, "codigo_ine", "left")
        .join(names, "nombre_normalizado", "left")
        .withColumn("codigo_ine_padron", F.when(F.col("_por_codigo"), F.col("codigo_ine"))
                    .otherwise(F.col("_codigo_por_nombre")))
        .withColumn("cruce", F.when(F.col("_por_codigo"), F.lit("ine"))
                    .when(F.col("_codigo_por_nombre").isNotNull(), F.lit("nombre")).otherwise(F.lit("sin_padron")))
        .drop("_por_codigo", "_codigo_por_nombre")
    )


def build(spark, params: dict):
    cfg = params.get("contexto", {})
    years = list(range(int(cfg.get("anio_desde", 2016)), int(cfg.get("anio_hasta", 2027)) + 1))

    dim = spark.table(require_table(spark, gold_table("dim_ct")))
    cells = optional_table(spark, gold_table("map_ct_celda"))
    population = optional_table(spark, silver_table("f_poblacion_municipio"))
    municipalities = optional_table(spark, silver_table("d_municipio_publico"))
    surroundings = optional_table(spark, silver_table("d_celda_entorno"))

    cts = base_cts(dim, cells)

    if population is not None:
        cts = population_by_code(cts, population)
    else:
        cts = cts.withColumn("codigo_ine_padron", F.col("codigo_ine")).withColumn("cruce", F.lit("sin_padron"))

    grid = cts.crossJoin(spark.createDataFrame([(y,) for y in years], "anio int"))

    if population is not None:
        pop = population.select(F.col("codigo_ine").alias("codigo_ine_padron"), "anio",
                                F.col("poblacion").cast("double").alias("poblacion"))
        for lag, name in ((1, "pob_municipio"), (2, "_pob_2"), (6, "_pob_6")):
            grid = grid.join(
                pop.withColumn("anio", F.col("anio") + F.lit(lag)).withColumnRenamed("poblacion", name),
                ["codigo_ine_padron", "anio"], "left",
            )
        grid = (
            grid
            .withColumn("pob_crec_1a", F.try_divide(F.col("pob_municipio"), F.col("_pob_2")) - F.lit(1.0))
            .withColumn("pob_crec_5a_anual",
                        F.pow(F.try_divide(F.col("pob_municipio"), F.col("_pob_6")), F.lit(0.2)) - F.lit(1.0))
            .drop("_pob_2", "_pob_6")
        )
    else:
        for name in ("pob_municipio", "pob_crec_1a", "pob_crec_5a_anual"):
            grid = grid.withColumn(name, F.lit(None).cast("double"))

    # Network of the municipality (every distribuidora) against its population.
    town = Window.partitionBy("codigo_ine_padron", "anio")
    grid = (
        grid
        .withColumn("kva_municipio_por_hab", F.try_divide(F.sum("kva").over(town), F.col("pob_municipio")))
        .withColumn("abonados_municipio_por_hab",
                    F.try_divide(F.sum("abonados").over(town), F.col("pob_municipio")))
    )

    if municipalities is not None:
        grid = grid.join(
            municipalities.select(F.col("codigo_ine").alias("codigo_ine_padron"), "area_km2",
                                  F.coalesce("elevacion_centro_m", "elevacion_m").alias("elevacion_municipio_m")),
            "codigo_ine_padron", "left",
        ).withColumn("densidad_hab_km2", F.try_divide(F.col("pob_municipio"), F.col("area_km2")))
    else:
        for name in ("area_km2", "elevacion_municipio_m", "densidad_hab_km2"):
            grid = grid.withColumn(name, F.lit(None).cast("double"))

    if surroundings is not None:
        present = [c for c in CELL_COLUMNS if c in surroundings.columns]
        grid = grid.join(
            surroundings.select("celda_id", *[F.col(c).alias(f"celda_{c}") for c in present]), "celda_id", "left")
        missing = [c for c in CELL_COLUMNS if c not in present]
    else:
        missing = CELL_COLUMNS
    for name in missing:
        kind = "boolean" if name == "osm_disponible" else "double"
        grid = grid.withColumn(f"celda_{name}", F.lit(None).cast(kind))

    return grid.drop("nombre_normalizado", "kva", "abonados").withColumn("audit_loaded_at", F.current_timestamp())


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-contexto_ct")
    dq = DQCollector(spark, "job_gold_contexto_ct", args.run_id)

    write_table(build(spark, params), TARGET_TABLE)

    written = spark.table(TARGET_TABLE)
    rows = written.count()
    dq.add("contexto_ct_anio", "filas", rows)
    for row in written.groupBy("cruce").agg(F.countDistinct("distribuidora_id", "ct_id").alias("n")).collect():
        dq.add("contexto_ct_anio", f"cts:{row['cruce']}", row["n"],
               umbral_pct=None if row["cruce"] != "sin_padron" else 5.0)
    dq.add("contexto_ct_anio", "sin_poblacion", written.where(F.col("pob_municipio").isNull()).count(), rows)
    dq.add("contexto_ct_anio", "sin_osm", written.where(~F.coalesce(F.col("celda_osm_disponible"), F.lit(False)))
           .count(), rows)
    dq.flush()

    logger.info("Gold completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
