"""
Silver of the public data (INE, Wikidata, Open-Meteo Elevation and
OpenStreetMap), cleaned and taken to the units Gold uses.

- f_poblacion_municipio: population of every municipality and year (Padron
  continuo at 1 January, both sexes). The rows of the province totals have no
  municipality and are left out; a repeated (municipality, year) keeps one row
  (DUPLICATE_ID) and a negative or missing value is rejected (INVALID_VALUE).
  Secret values (secreto) are kept with their flag.
- d_municipio_publico: surface (km2) and altitude of every municipality from
  Wikidata, and the altitude of its centre from the elevation model. A surface
  of 0 or less is taken as unknown.
- d_celda_entorno: the surroundings of every weather cell. Relief from the grid
  of points (mean, spread, minimum, maximum and their difference) and, when
  the OpenStreetMap download of the cell exists, the exposure of the network:
  km of overhead power lines (line and minor_line) and of cables, poles and
  towers, transformers, substations, km of roads and of tracks, km2 of forest
  (forest and wood), scrub and construction sites, and the buildings, with
  the densities per km2 of cell. osm_disponible says whether the cell was
  downloaded: a cell without download has its OSM columns null, not 0.

    docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/02_silver/job_silver_contexto_publico.py
"""

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


POPULATION = "f_poblacion_municipio"
MUNICIPALITY = "d_municipio_publico"
CELL = "d_celda_entorno"

PADRON_TABLE = "l1_bronze.ine_padron_municipio"
WIKIDATA_TABLE = "l1_bronze.wikidata_municipio"
ELEVATION_TABLE = "l1_bronze.openmeteo_elevacion_punto"
OSM_TABLE = "l1_bronze.osm_elemento_celda"
OSM_CELLS_TABLE = "l1_bronze.osm_consulta_celda"

# (output column, class, values, measure): sum of the length, the surface or
# the count of the OSM elements of the cell.
OSM_MEASURES = [
    ("km_linea_aerea", "power", ["line", "minor_line"], "longitud_km"),
    ("km_linea_minor", "power", ["minor_line"], "longitud_km"),
    ("km_cable_subterraneo", "power", ["cable"], "longitud_km"),
    ("n_apoyos", "power", ["pole", "tower"], "n"),
    ("n_trafos_osm", "power", ["transformer"], "n"),
    ("n_subestaciones", "power", ["substation"], "n"),
    ("km_carreteras", "highway", ["motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
                                  "residential"], "longitud_km"),
    ("km_pistas", "highway", ["track"], "longitud_km"),
    ("km2_bosque", None, ["forest", "wood"], "area_km2"),
    ("km2_matorral", "natural", ["scrub"], "area_km2"),
    ("km2_obras", "landuse", ["construction"], "area_km2"),
]


def normalized_code(column):
    return F.lpad(F.regexp_replace(F.trim(column.cast("string")), "[^0-9]", ""), 5, "0")


def population(padron):
    df = padron.select(
        normalized_code(F.col("codigo_ine")).alias("codigo_ine"),
        F.trim(F.col("municipio")).alias("municipio"),
        F.col("provincia_codigo").cast("string").alias("provincia_codigo"),
        F.col("anio").cast("int").alias("anio"),
        F.col("poblacion").cast("double").alias("poblacion"),
        F.coalesce(F.col("secreto").cast("boolean"), F.lit(False)).alias("secreto"),
        F.col("sexo"),
        F.col("tabla_id").cast("string").alias("tabla_id"),
    ).where(F.col("codigo_ine").isNotNull() & (F.col("codigo_ine") != "00000"))

    df = df.where(F.lower(F.col("sexo")).isin("total", "ambos sexos")).drop("sexo")

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("anio").isNull()),
        ("INVALID_VALUE", F.col("poblacion").isNull() | (F.col("poblacion") < 0)),
    ])
    kept, duplicated = keep_first(kept, ["codigo_ine", "anio"], [F.col("tabla_id")], "DUPLICATE_ID")
    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def municipalities(spark, wikidata, elevation):
    df = wikidata.select(
        normalized_code(F.col("codigo_ine")).alias("codigo_ine"),
        F.trim(F.col("nombre")).alias("nombre"),
        F.when(F.col("area_km2").cast("double") > 0, F.col("area_km2").cast("double")).alias("area_km2"),
        F.col("elevacion_m").cast("double").alias("elevacion_m"),
        F.col("wikidata_id"),
    )
    kept, rejected = split_rejects(df, [("NULL_KEY", F.col("codigo_ine").isNull() | (F.col("codigo_ine") == "00000"))])
    kept, duplicated = keep_first(kept, ["codigo_ine"], [F.col("area_km2").isNull(), F.col("wikidata_id")],
                                  "DUPLICATE_ID")

    if elevation is not None:
        centres = elevation.where(F.col("tipo") == "municipio").groupBy(
            normalized_code(F.col("clave")).alias("codigo_ine")
        ).agg(F.avg(F.col("elevacion_m").cast("double")).alias("elevacion_centro_m"))
        kept = kept.join(centres, "codigo_ine", "full")
    else:
        kept = kept.withColumn("elevacion_centro_m", F.lit(None).cast("double"))

    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def cells(spark, elevation, osm, osm_cells):
    relief = None
    if elevation is not None:
        relief = elevation.where(F.col("tipo") == "celda").groupBy(F.col("clave").alias("celda_id")).agg(
            F.avg(F.col("elevacion_m").cast("double")).alias("elev_media_m"),
            F.stddev_pop(F.col("elevacion_m").cast("double")).alias("elev_std_m"),
            F.min(F.col("elevacion_m").cast("double")).alias("elev_min_m"),
            F.max(F.col("elevacion_m").cast("double")).alias("elev_max_m"),
            F.count(F.lit(1)).alias("n_puntos_relieve"),
        ).withColumn("desnivel_m", F.col("elev_max_m") - F.col("elev_min_m"))

    exposure = None
    if osm is not None and osm_cells is not None:
        measures = []
        for name, klass, values, measure in OSM_MEASURES:
            condition = F.col("valor").isin(values)
            if klass is not None:
                condition = condition & (F.col("clase") == klass)
            value = F.lit(1.0) if measure == "n" else F.col(measure).cast("double")
            measures.append(F.sum(F.when(condition, value).otherwise(F.lit(0.0))).alias(name))
        buildings = F.sum(F.when(F.col("clase") == "building", F.col("n").cast("double")).otherwise(F.lit(0.0)))
        measures.append(buildings.alias("n_edificios"))

        per_cell = osm.groupBy("celda_id").agg(*measures)
        exposure = (
            osm_cells.select("celda_id", F.col("area_celda_km2").cast("double").alias("area_celda_km2"),
                             F.col("osm_base"), F.col("endpoint").alias("osm_endpoint"))
            .join(per_cell, "celda_id", "left")
            .withColumn("osm_disponible", F.lit(True))
        )
        for name, *_ in OSM_MEASURES + [("n_edificios",)]:
            exposure = exposure.withColumn(name, F.coalesce(F.col(name), F.lit(0.0)))
        area = F.col("area_celda_km2")

        def per(column, base=area):
            # try_divide: a cell without any line gives null, not an error (ANSI mode).
            return F.try_divide(F.col(column), base)

        exposure = (
            exposure
            .withColumn("dens_linea_aerea_km_km2", per("km_linea_aerea"))
            .withColumn("dens_apoyos_km2", per("n_apoyos"))
            .withColumn("dens_carreteras_km_km2", per("km_carreteras"))
            .withColumn("dens_pistas_km_km2", per("km_pistas"))
            .withColumn("pct_bosque", F.least(per("km2_bosque"), F.lit(1.0)))
            .withColumn("pct_matorral", F.least(per("km2_matorral"), F.lit(1.0)))
            .withColumn("dens_edificios_km2", per("n_edificios"))
            .withColumn("pct_cable", per("km_cable_subterraneo",
                                         F.col("km_cable_subterraneo") + F.col("km_linea_aerea")))
        )

    if relief is None and exposure is None:
        return None
    if relief is None:
        out = exposure
    elif exposure is None:
        out = relief.withColumn("osm_disponible", F.lit(False))
    else:
        out = relief.join(exposure, "celda_id", "full").withColumn(
            "osm_disponible", F.coalesce(F.col("osm_disponible"), F.lit(False)))

    return out.withColumn("audit_loaded_at", F.current_timestamp())


def optional(spark, table):
    if spark.catalog.tableExists(table):
        return spark.table(table)
    logger.warning("%s not found: run the Landing and Bronze of the public data first", table)
    return None


def main():
    args = parse_args()
    spark = get_spark("job-silver-contexto_publico")
    dq = DQCollector(spark, "job_silver_contexto_publico", args.run_id)

    padron = optional(spark, PADRON_TABLE)
    if padron is not None:
        total = padron.count()
        out, rejected = population(padron)
        write_table(out, silver_table(POPULATION))
        write_rejected(rejected, POPULATION, args.run_id)
        written = spark.table(silver_table(POPULATION))
        dq.add(POPULATION, "filas_entrada", total)
        dq.add(POPULATION, "filas", written.count())
        dq.add(POPULATION, "municipios", written.select("codigo_ine").distinct().count())
        span = written.agg(F.min("anio"), F.max("anio")).first()
        dq.add(POPULATION, "anio_min", span[0])
        dq.add(POPULATION, "anio_max", span[1])
        if rejected is not None:
            for row in rejected.groupBy("_motivo").count().collect():
                dq.add(POPULATION, f"rechazo:{row['_motivo']}", row["count"], total, umbral_pct=5.0)

    wikidata = optional(spark, WIKIDATA_TABLE)
    elevation = optional(spark, ELEVATION_TABLE)
    if wikidata is not None:
        out, rejected = municipalities(spark, wikidata, elevation)
        write_table(out, silver_table(MUNICIPALITY))
        write_rejected(rejected, MUNICIPALITY, args.run_id)
        written = spark.table(silver_table(MUNICIPALITY))
        n = written.count()
        dq.add(MUNICIPALITY, "municipios", n)
        dq.add(MUNICIPALITY, "sin_superficie", written.where(F.col("area_km2").isNull()).count(), n, umbral_pct=5.0)

    out = cells(spark, elevation, optional(spark, OSM_TABLE), optional(spark, OSM_CELLS_TABLE))
    if out is not None:
        write_table(out, silver_table(CELL))
        written = spark.table(silver_table(CELL))
        n = written.count()
        dq.add(CELL, "celdas", n)
        dq.add(CELL, "celdas_sin_osm", written.where(~F.col("osm_disponible")).count(), n, umbral_pct=10.0)

    dq.flush()
    logger.info("Silver completed: public data")


if __name__ == "__main__":
    main()
