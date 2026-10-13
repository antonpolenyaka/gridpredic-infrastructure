"""
Gold context of every CT from the public data, one row per CT and year:
l3_gold.contexto_ct_anio. And the works and building activity of every
municipality and year: l3_gold.obras_municipio_anio.

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

- Public works of the municipality (PLACSP, l2_silver.f_licitacion_obra):
  works tendered in the year before (number, budget and the ones that dig
  the street, mov_tierras) and the months of execution planned for the year
  by the tenders published before it started (obras_previstas_meses).
- Building (Catastro, l2_silver.f_edificacion_municipio): dwellings and m2
  built in the three years that end two years before (the Catastro takes
  about a year to register a new building), the dwellings at that date and
  their yearly change in five years.
- Birds (GBIF, l2_silver.f_aves_celda_anio): records of the white stork and
  of all the birds in the cell of the CT in the five years before, and the
  share of storks among them (celda_ciguena_5a, celda_aves_5a,
  celda_ciguena_parte_aves_5a).
- Fires (EFFIS, l2_silver.f_area_quemada): burnt areas of 30 ha or more
  whose centre falls in the cell of the CT or in the eight around it (about
  30 x 33 km) in the five years before: number and hectares
  (celda_incendios_5a, celda_area_quemada_5a_ha).

obras_municipio_anio is descriptive, not a feature table: for every
municipality of the CTs and year, the works published, the works in
execution (number and months inside the year), the buildings, dwellings and
m2 built that year and the dwellings standing at its end. The columns of the
works are null outside the years of PLACSP loaded (the execution needs the
year before too); the ones of the Catastro, after the current year.

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
WORKS_TABLE = gold_table("obras_municipio_anio")

WORKS_COLUMNS = ["obras_publicadas", "obras_publicadas_importe", "obras_publicadas_mov_tierras", "obras_en_ejecucion",
                 "obras_meses_ejecucion", "obras_mov_tierras_meses", "obras_previstas_meses",
                 "obras_previstas_mov_tierras_meses"]
BUILDING_COLUMNS = ["edificios_nuevos", "viviendas_nuevas", "m2_nuevos", "edificios_parque", "viviendas_parque"]
CONTEXT_WORKS = ["obras_publicadas_1a", "obras_importe_1a", "obras_mov_tierras_1a", "obras_previstas_meses",
                 "obras_previstas_mov_tierras_meses"]
CONTEXT_BUILDINGS = ["viviendas_nuevas_3a", "m2_nuevos_3a", "viviendas_parque_2a", "crec_viviendas_5a"]

CONTEXT_BIRDS = ["celda_ciguena_5a", "celda_aves_5a", "celda_ciguena_parte_aves_5a"]
CONTEXT_FIRES = ["celda_incendios_5a", "celda_area_quemada_5a_ha"]
HISTORY_YEARS = 5

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


def year_bounds():
    return F.make_date(F.col("anio"), F.lit(1), F.lit(1)), F.make_date(F.col("anio") + F.lit(1), F.lit(1), F.lit(1))


def works_by_year(tenders, towns_years):
    """Works of every municipality and year: published, in execution and planned before the year started."""
    live = tenders.where(~F.coalesce(F.col("anulada"), F.lit(False)) & F.col("codigo_ine").isNotNull())
    weight = F.col("peso").cast("double")
    digs = F.coalesce(F.col("mov_tierras"), F.lit(False))
    published = live.groupBy("codigo_ine", F.col("anio_publicacion").alias("anio")).agg(
        F.sum(weight).alias("obras_publicadas"),
        F.sum(weight * F.coalesce(F.col("importe_licitacion"), F.lit(0.0))).alias("obras_publicadas_importe"),
        F.sum(F.when(digs, weight).otherwise(F.lit(0.0))).alias("obras_publicadas_mov_tierras"),
    )

    running = live.where(F.col("inicio_ejecucion").isNotNull() & (F.col("fin_ejecucion") >= F.col("inicio_ejecucion")))
    running = running.withColumn("anio", F.explode(F.sequence(F.year("inicio_ejecucion"), F.year("fin_ejecucion"))))
    first, after = year_bounds()
    inside = F.greatest(F.lit(0), F.datediff(F.least(F.col("fin_ejecucion"), after),
                                             F.greatest(F.col("inicio_ejecucion"), first))) / F.lit(30.4)
    running = running.withColumn("_meses", inside * weight).withColumn("_antes", F.col("fecha_publicacion") < first)
    executing = running.groupBy("codigo_ine", "anio").agg(
        F.sum(F.when(F.col("_meses") > 0, weight).otherwise(F.lit(0.0))).alias("obras_en_ejecucion"),
        F.sum("_meses").alias("obras_meses_ejecucion"),
        F.sum(F.when(digs, F.col("_meses")).otherwise(F.lit(0.0))).alias("obras_mov_tierras_meses"),
        F.sum(F.when(F.col("_antes"), F.col("_meses")).otherwise(F.lit(0.0))).alias("obras_previstas_meses"),
        F.sum(F.when(F.col("_antes") & digs, F.col("_meses")).otherwise(F.lit(0.0)))
        .alias("obras_previstas_mov_tierras_meses"),
    )

    # The years of PLACSP loaded (a tender published before the first one may come in its updates).
    span = tenders.agg(F.min("anio_fichero"), F.max("anio_fichero")).first()
    out = towns_years.join(published, ["codigo_ine", "anio"], "left").join(executing, ["codigo_ine", "anio"], "left")
    if span[0] is None:
        for name in WORKS_COLUMNS:
            out = out.withColumn(name, F.lit(None).cast("double"))
        return out
    lo, hi = int(span[0]), int(span[1])
    # Published: the years loaded. In execution: the year before must be loaded too. Planned: known up to hi + 1.
    windows = {"obras_publicadas": (lo, hi), "obras_publicadas_importe": (lo, hi),
               "obras_publicadas_mov_tierras": (lo, hi), "obras_en_ejecucion": (lo + 1, hi),
               "obras_meses_ejecucion": (lo + 1, hi), "obras_mov_tierras_meses": (lo + 1, hi),
               "obras_previstas_meses": (lo + 1, hi + 1), "obras_previstas_mov_tierras_meses": (lo + 1, hi + 1)}
    for name, (a, b) in windows.items():
        valid = (F.col("anio") >= a) & (F.col("anio") <= b)
        out = out.withColumn(name, F.when(valid, F.coalesce(F.col(name), F.lit(0.0))))
    return out


def buildings_by_year(buildings, towns_years, this_year: int):
    """Buildings, dwellings and m2 built in the year and standing at its end, by municipality."""
    built = buildings.select("codigo_ine", "anio", F.col("edificios").cast("double").alias("edificios_nuevos"),
                             F.col("viviendas").cast("double").alias("viviendas_nuevas"),
                             F.col("superficie_m2").cast("double").alias("m2_nuevos"))
    stock = towns_years.alias("g").join(
        buildings.alias("b"), (F.col("g.codigo_ine") == F.col("b.codigo_ine")) & (F.col("b.anio") <= F.col("g.anio"))
    ).groupBy(F.col("g.codigo_ine").alias("codigo_ine"), F.col("g.anio").alias("anio")).agg(
        F.sum(F.col("b.edificios").cast("double")).alias("edificios_parque"),
        F.sum(F.col("b.viviendas").cast("double")).alias("viviendas_parque"),
    )
    known = buildings.select("codigo_ine").distinct().withColumn("_con_catastro", F.lit(True))
    out = towns_years.join(built, ["codigo_ine", "anio"], "left").join(stock, ["codigo_ine", "anio"], "left") \
        .join(known, "codigo_ine", "left")
    for name in BUILDING_COLUMNS:
        out = out.withColumn(name, F.when(F.col("_con_catastro") & (F.col("anio") <= this_year),
                                          F.coalesce(F.col(name), F.lit(0.0))))
    return out.drop("_con_catastro")


def activity(spark, cts, years: list):
    """obras_municipio_anio for the municipalities of the CTs, or None without works nor buildings."""
    tenders = optional_table(spark, silver_table("f_licitacion_obra"))
    buildings = optional_table(spark, silver_table("f_edificacion_municipio"))
    if tenders is None and buildings is None:
        return None
    towns = cts.where(F.col("codigo_ine_padron").isNotNull()).select(
        F.col("codigo_ine_padron").alias("codigo_ine")).distinct()
    towns_years = towns.crossJoin(spark.createDataFrame([(y,) for y in years], "anio int"))

    out = towns_years
    if tenders is not None:
        out = out.join(works_by_year(tenders, towns_years), ["codigo_ine", "anio"], "left")
    else:
        for name in WORKS_COLUMNS:
            out = out.withColumn(name, F.lit(None).cast("double"))
    if buildings is not None:
        this_year = spark.range(1).select(F.year(F.current_date())).first()[0]
        out = out.join(buildings_by_year(buildings, towns_years, int(this_year)), ["codigo_ine", "anio"], "left")
    else:
        for name in BUILDING_COLUMNS:
            out = out.withColumn(name, F.lit(None).cast("double"))
    return out.withColumn("audit_loaded_at", F.current_timestamp())


def add_activity(grid, works):
    """Columns of contexto_ct_anio from obras_municipio_anio, all known before the year starts."""
    if works is None:
        for name in CONTEXT_WORKS + CONTEXT_BUILDINGS:
            grid = grid.withColumn(name, F.lit(None).cast("double"))
        return grid

    w = works.withColumnRenamed("codigo_ine", "codigo_ine_padron")

    def shifted(columns: dict, lag: int):
        return w.select("codigo_ine_padron", (F.col("anio") + F.lit(lag)).alias("anio"),
                        *[F.col(source).alias(target) for source, target in columns.items()])

    grid = grid.join(shifted({"obras_publicadas": "obras_publicadas_1a", "obras_publicadas_importe": "obras_importe_1a",
                              "obras_publicadas_mov_tierras": "obras_mov_tierras_1a"}, 1),
                     ["codigo_ine_padron", "anio"], "left")
    grid = grid.join(w.select("codigo_ine_padron", "anio", "obras_previstas_meses",
                              "obras_previstas_mov_tierras_meses"), ["codigo_ine_padron", "anio"], "left")

    new = None
    for lag in (2, 3, 4):
        part = shifted({"viviendas_nuevas": "viviendas_nuevas_3a", "m2_nuevos": "m2_nuevos_3a"}, lag)
        new = part if new is None else new.unionByName(part)
    new = new.groupBy("codigo_ine_padron", "anio").agg(
        # Null when any of the three years is unknown.
        F.when(F.count("viviendas_nuevas_3a") == 3, F.sum("viviendas_nuevas_3a")).alias("viviendas_nuevas_3a"),
        F.when(F.count("m2_nuevos_3a") == 3, F.sum("m2_nuevos_3a")).alias("m2_nuevos_3a"),
    )
    grid = grid.join(new, ["codigo_ine_padron", "anio"], "left")
    grid = grid.join(shifted({"viviendas_parque": "viviendas_parque_2a"}, 2), ["codigo_ine_padron", "anio"], "left")
    grid = grid.join(shifted({"viviendas_parque": "_parque_7a"}, 7), ["codigo_ine_padron", "anio"], "left")
    return grid.withColumn(
        "crec_viviendas_5a", F.pow(F.try_divide(F.col("viviendas_parque_2a"), F.col("_parque_7a")), F.lit(0.2))
        - F.lit(1.0)).drop("_parque_7a")


def cell_center(column):
    parts = F.split(column, "_")
    return parts.getItem(0).try_cast("double"), parts.getItem(1).try_cast("double")


def years_before(spark, df, years: list, value_columns: list, keys: list):
    """Sum of value_columns over the HISTORY_YEARS years before every year of years, by keys."""
    targets = spark.createDataFrame([(y,) for y in years], "anio_obj int")
    window = (F.col("anio") >= F.col("anio_obj") - F.lit(HISTORY_YEARS)) & (F.col("anio") <= F.col("anio_obj") - 1)
    return (
        df.crossJoin(F.broadcast(targets)).where(window)
        .groupBy(*keys, "anio_obj").agg(*[F.sum(c).alias(c) for c in value_columns])
        .withColumnRenamed("anio_obj", "anio")
    )


def bird_history(spark, birds, years: list):
    """celda_ciguena_5a, celda_aves_5a and the share of storks, by (celda_id, anio)."""
    if birds is None:
        return None
    per_year = birds.groupBy("celda_id", "anio").agg(
        F.sum(F.when(F.col("taxon") == "ciguena_blanca", F.col("registros")).otherwise(0)).cast("double")
        .alias("celda_ciguena_5a"),
        F.sum(F.when(F.col("taxon") == "aves", F.col("registros")).otherwise(0)).cast("double").alias("celda_aves_5a"),
    )
    out = years_before(spark, per_year, years, ["celda_ciguena_5a", "celda_aves_5a"], ["celda_id"])
    return out.withColumn("celda_ciguena_parte_aves_5a",
                          F.try_divide(F.col("celda_ciguena_5a"), F.col("celda_aves_5a")))


def fire_history(spark, fires, cells, years: list):
    """celda_incendios_5a and celda_area_quemada_5a_ha of the cell and its neighbours, by (celda_id, anio)."""
    if fires is None or cells is None:
        return None
    lat, lon = cell_center(F.col("celda_id"))
    targets = cells.select("celda_id").distinct().select("celda_id", lat.alias("_lat"), lon.alias("_lon"))
    burnt = fires.select("anio", F.col("latitud").alias("_flat"), F.col("longitud").alias("_flon"),
                         F.lit(1.0).alias("celda_incendios_5a"),
                         F.coalesce(F.col("area_ha"), F.lit(0.0)).alias("celda_area_quemada_5a_ha"))
    near = (F.abs(F.col("_lat") - F.col("_flat")) <= 0.15) & (F.abs(F.col("_lon") - F.col("_flon")) <= 0.15)
    per_year = targets.join(burnt, near).groupBy("celda_id", "anio").agg(
        F.sum("celda_incendios_5a").alias("celda_incendios_5a"),
        F.sum("celda_area_quemada_5a_ha").alias("celda_area_quemada_5a_ha"))
    out = years_before(spark, per_year, years, CONTEXT_FIRES, ["celda_id"])
    # A cell with no fire in the window has 0, not null, once EFFIS is loaded.
    return targets.select("celda_id").crossJoin(spark.createDataFrame([(y,) for y in years], "anio int")).join(
        out, ["celda_id", "anio"], "left").fillna(0.0, subset=CONTEXT_FIRES)


def add_cell_history(grid, history, columns: list):
    if history is None:
        for name in columns:
            grid = grid.withColumn(name, F.lit(None).cast("double"))
        return grid
    return grid.join(history.select("celda_id", "anio", *columns), ["celda_id", "anio"], "left")


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

    # The activity of the municipalities needs the years before the first one of the context.
    works = activity(spark, cts, list(range(years[0] - 8, years[-1] + 1)))
    if works is not None:
        works = works.cache()

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

    grid = add_activity(grid, works)
    grid = add_cell_history(grid, bird_history(spark, optional_table(spark, silver_table("f_aves_celda_anio")), years),
                            CONTEXT_BIRDS)
    grid = add_cell_history(grid, fire_history(spark, optional_table(spark, silver_table("f_area_quemada")), cells,
                                               years), CONTEXT_FIRES)
    context = grid.drop("nombre_normalizado", "kva", "abonados").withColumn("audit_loaded_at", F.current_timestamp())
    return context, works


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-contexto_ct")
    dq = DQCollector(spark, "job_gold_contexto_ct", args.run_id)

    context, works = build(spark, params)
    if works is not None:
        write_table(works.drop("audit_loaded_at").withColumn("audit_loaded_at", F.current_timestamp()), WORKS_TABLE)
        done = spark.table(WORKS_TABLE)
        dq.add("obras_municipio_anio", "filas", done.count())
        dq.add("obras_municipio_anio", "municipios", done.select("codigo_ine").distinct().count())
        dq.add("obras_municipio_anio", "con_obras", done.where(F.col("obras_publicadas") > 0).count())
        dq.add("obras_municipio_anio", "con_catastro", done.where(F.col("viviendas_parque").isNotNull()).count())
    write_table(context, TARGET_TABLE)

    written = spark.table(TARGET_TABLE)
    rows = written.count()
    dq.add("contexto_ct_anio", "filas", rows)
    for row in written.groupBy("cruce").agg(F.countDistinct("distribuidora_id", "ct_id").alias("n")).collect():
        dq.add("contexto_ct_anio", f"cts:{row['cruce']}", row["n"],
               umbral_pct=None if row["cruce"] != "sin_padron" else 5.0)
    dq.add("contexto_ct_anio", "sin_poblacion", written.where(F.col("pob_municipio").isNull()).count(), rows)
    dq.add("contexto_ct_anio", "sin_osm", written.where(~F.coalesce(F.col("celda_osm_disponible"), F.lit(False)))
           .count(), rows)
    dq.add("contexto_ct_anio", "sin_aves", written.where(F.col("celda_aves_5a").isNull()).count(), rows)
    dq.add("contexto_ct_anio", "con_incendios", written.where(F.col("celda_incendios_5a") > 0).count(), rows)
    dq.flush()

    logger.info("Gold completed: %s%s", TARGET_TABLE, f" and {WORKS_TABLE}" if works is not None else "")


if __name__ == "__main__":
    main()
