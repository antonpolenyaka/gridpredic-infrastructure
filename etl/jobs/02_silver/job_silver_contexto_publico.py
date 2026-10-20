"""
Silver of the public data (INE, Wikidata, Open-Meteo Elevation, OpenStreetMap,
PLACSP, Catastro, IEEX, GBIF, EFFIS, AEMET, FIRMS and CEMS), cleaned and
taken to the units Gold uses.

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
- f_licitacion_obra: one row per works tender of PLACSP and municipality,
  from its last version (state, award, amounts) and its first publication.
  The municipality is the one of the contracting body when it is a town
  council (DIR3 L01 + INE code); for the other bodies (Diputacion, Junta,
  State) the names of the municipalities of the provinces found in the
  title, every one with weight 1/n; without any, a row without municipality
  with the province of the NUTS. Tenders of other regions (they came because
  the contractor is from here) are rejected (OUT_OF_TERRITORY), and so are
  the ones without works type or CPV 45 (NOT_WORKS). Planned execution:
  start (planned, contract, award plus one month, deadline of the offers
  plus two months or publication plus three) and months (duration in
  months, days or years; WORKS["meses_por_defecto"], 6, when there is none).
  mov_tierras marks the works that dig or open the street: CPV of earth
  works, civil engineering, pipelines, roads and water, or words of the
  title (calle, pavimentacion, abastecimiento, saneamiento, zanja...).
- f_edificacion_municipio: buildings, dwellings and built surface (total and
  by use) of every municipality by year of construction, from the Catastro.
  Ruins and years outside 1800 - this year are left out.
- f_proyeccion_ame and d_ame_municipio: population projected by the IEEX for
  every group of municipalities (AME) and year, and the AME of every
  municipality (one per INE code).
- f_aves_celda_anio and f_aves_celda_mes: records of GBIF of the white stork
  and of all the birds in every cell, by year and by month. The records
  depend on how many people look, so the share of storks among the birds is
  a better measure of their presence than the count alone (Gold uses both).
- f_area_quemada: one row per burnt area of EFFIS (30 ha or more), with the
  fire date, the area, the province and municipality the EFFIS gives, the
  centre of the polygon and its weather cell (the grid of 0.1 degrees of
  the weather). A repeated identifier keeps the last update; rows without
  date (NULL_KEY) or without position (NO_GEOMETRY) are rejected.
- f_aviso_meteo: one row per warning message of AEMET and weather cell
  whose centre falls inside the polygon of the warning zone: times in the
  wall clock of Madrid (sent, onset, expiry), phenomenon (VI wind, PR rain,
  TO thunderstorms, NE snow, TA and TI temperatures...), level and its
  number (verde 0, amarillo 1, naranja 2, rojo 3), probability, zone and
  the cell. Rows without identifier or times (NULL_KEY) or without polygon
  (NO_GEOMETRY) are rejected; the zones that hold no cell of the study are
  left out.
- f_foco_incendio: one row per active fire of FIRMS: time of the pass in UTC
  and in the wall clock of Madrid, position, sensor and satellite, the
  confidence as it comes and as a level (1 low, 2 nominal, 3 high; MODIS
  below 30, from 30 to 80 and above), fire radiative power, day or night,
  and the nearest cell of the study within one step and a half of the grid
  with its distance (null cell for the fires farther away, which stay for
  the alerts around the network). Rows without position or time are
  rejected (NULL_KEY) and a repeated pass keeps one row (DUPLICATE_KEY).
- f_fwi_celda_dia: the fire danger indices of CEMS of every cell of the
  study and day, from the nearest point of their grid of 0.25 degrees (at
  most max_paso_fwi away): Fire Weather Index and its parts (FFMC, DMC, DC,
  ISI, BUI), the distance to the point and the dataset (consolidated or
  intermediate). A day and point that come twice keep the consolidated
  dataset and then the newest piece (DUPLICATE_KEY); rows without date or
  position are rejected (NULL_KEY).

    docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/02_silver/job_silver_contexto_publico.py
"""

from datetime import date

from pyspark.sql import Window
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
TENDERS = "f_licitacion_obra"
BUILDINGS = "f_edificacion_municipio"
PROJECTION = "f_proyeccion_ame"
AME = "d_ame_municipio"

PADRON_TABLE = "l1_bronze.ine_padron_municipio"
WIKIDATA_TABLE = "l1_bronze.wikidata_municipio"
ELEVATION_TABLE = "l1_bronze.openmeteo_elevacion_punto"
OSM_TABLE = "l1_bronze.osm_elemento_celda"
OSM_CELLS_TABLE = "l1_bronze.osm_consulta_celda"
PLACSP_TABLE = "l1_bronze.placsp_licitacion_obra"
PLACSP_DELETED_TABLE = "l1_bronze.placsp_entrada_borrada"
CATASTRO_TABLE = "l1_bronze.catastro_edificio"
IEEX_TABLE = "l1_bronze.ieex_proyeccion_ame"
IEEX_AME_TABLE = "l1_bronze.ieex_ame_municipio"
GBIF_TABLE = "l1_bronze.gbif_conteo_celda"
EFFIS_TABLE = "l1_bronze.effis_area_quemada"
AEMET_TABLE = "l1_bronze.aemet_aviso"
FIRMS_TABLE = "l1_bronze.firms_foco"
HOTSPOTS = "f_foco_incendio"
FWI_TABLE = "l1_bronze.cems_fwi_dia"
FIRE_DANGER = "f_fwi_celda_dia"
FWI_COLUMNS = ["fwi", "ffmc", "dmc", "dc", "isi", "bui"]
WEATHER_CELLS_TABLE = "l2_silver.d_meteo_celda"
WARNINGS = "f_aviso_meteo"
WARNING_LEVELS = {"verde": 0, "amarillo": 1, "naranja": 2, "rojo": 3}
BIRDS_YEAR = "f_aves_celda_anio"
BIRDS_MONTH = "f_aves_celda_mes"
FIRES = "f_area_quemada"
CELL_DEGREES = 0.1

WORKS = {
    "nuts": ["ES43"],
    "provincias": ["06", "10"],
    "nuts_provincia": {"ES431": "06", "ES432": "10"},
    # The capitals share the name of their province: "Albalá (Cáceres)" or "Diputación de Badajoz" name the province.
    "nombres_provincia": ["badajoz", "caceres"],
    "meses_por_defecto": 6.0,
    "cpv_mov_tierras": ["4511", "4523", "4524", "45262"],
    "palabras_mov_tierras": "calle|pavimenta|urbaniza|abastecimiento|saneamiento|alcantarillado|colector|red de agua|"
                            "redes de agua|zanja|canaliza|asfalt|acerado|aceras|camino|carretera|soterra|"
                            "conduccion|tuberia|emisario|depurador|puente|glorieta|rotonda|travesia",
}

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


# ---------------------------------------------------------------------------
# Works tenders (PLACSP) and buildings (Catastro)
# ---------------------------------------------------------------------------

def plain_text(column, provinces: list = None):
    """
    Lower case without accents, every run of other characters as one space, a space at both ends. With the names
    of the provinces, the mentions of a province (between brackets, "provincia de", "Diputación de") go out first,
    so they do not count as the capital of the same name.
    """
    low = F.translate(F.lower(F.coalesce(column, F.lit(""))), "áàäâéèëêíìïîóòöôúùüûñç", "aaaaeeeeiiiioooouuuunc")
    if provinces:
        names = "|".join(provinces)
        low = F.regexp_replace(low, rf"\(\s*(provincia de\s+)?({names})\s*\)", " ")
        low = F.regexp_replace(
            low, rf"(provincia|diputacion(\s+provincial)?|excma\.?\s+diputacion(\s+provincial)?)\s+de\s+(la\s+)?({names})",
            " ")
    return F.concat(F.lit(" "), F.trim(F.regexp_replace(low, "[^a-z]+", " ")), F.lit(" "))


def day(column):
    """Date of the first ten characters (CODICE dates may carry a time zone); null when it is not a date."""
    return F.to_date(F.try_to_timestamp(F.substring(F.col(column), 1, 10), F.lit("yyyy-MM-dd")))


def latest_known(column, order):
    """Value of the latest version that has it."""
    return F.max_by(F.col(column), F.when(F.col(column).isNotNull(), order))


def town_names(population):
    """INE code, province and plain name of every municipality of the Padron ("Torno, El" as "el torno")."""
    names = population.where(F.col("municipio").isNotNull()).select("codigo_ine", "municipio").dropDuplicates(
        ["codigo_ine"])
    moved = F.regexp_replace(F.trim(F.col("municipio")), r"^(.*?)\s*[,(]\s*(El|La|Los|Las)\s*\)?$", "$2 $1")
    return names.select("codigo_ine", F.substring("codigo_ine", 1, 2).alias("provincia_municipio"),
                        plain_text(moved).alias("nombre_plano")).where(F.length("nombre_plano") > 3)


def any_prefix(array, prefixes):
    def test(code):
        hit = F.lit(False)
        for prefix in prefixes:
            hit = hit | code.startswith(prefix)
        return hit
    return F.exists(array, test)


def tenders(placsp, deleted, towns, cfg: dict = None):
    """(rows of f_licitacion_obra, rejected) from the versions of the works tenders."""
    cfg = {**WORKS, **(cfg or {})}
    versions = placsp.withColumn("_ts", F.try_to_timestamp(F.col("actualizado")))
    order = F.coalesce(F.col("_ts"), F.lit("1900-01-01").cast("timestamp"))
    months = (
        F.when(F.upper(F.col("duracion_unidad")) == "DAY", F.col("duracion").cast("double") / 30.4)
        .when(F.upper(F.col("duracion_unidad")) == "ANN", F.col("duracion").cast("double") * 12.0)
        .otherwise(F.col("duracion").cast("double"))
    )
    versions = versions.withColumn("_meses", F.when(months > 0, F.least(months, F.lit(60.0))))
    for name in ("fecha_primera_publicacion", "fecha_adjudicacion", "fecha_contrato", "inicio_previsto",
                 "fin_previsto", "fecha_limite_ofertas"):
        versions = versions.withColumn(f"_{name}", day(name))

    last = Window.partitionBy("licitacion_id").orderBy(order.desc(), F.col("fichero_anio").desc())
    latest = versions.withColumn("_n", F.row_number().over(last)).where(F.col("_n") == 1).select(
        "licitacion_id", "expediente", "titulo", "objeto", "organo", "organo_tipo", "dir3", "organo_cp", "estado",
        "tipo_contrato", "cpv", "nuts", "importe_sin_iva", "importe_estimado", "importe_adjudicado", "enlace")
    known = versions.groupBy("licitacion_id").agg(
        F.min("_fecha_primera_publicacion").alias("_pub"),
        F.to_date(F.min("_ts")).alias("_visto"),
        F.min("_fecha_adjudicacion").alias("fecha_adjudicacion"),
        F.min("_fecha_contrato").alias("_contrato"),
        latest_known("_inicio_previsto", order).alias("_inicio"),
        latest_known("_fin_previsto", order).alias("_fin"),
        latest_known("_fecha_limite_ofertas", order).alias("_limite"),
        latest_known("_meses", order).alias("_meses"),
        F.count(F.lit(1)).alias("versiones"),
        F.min(F.col("fichero_anio").cast("int")).alias("anio_fichero"),
    )
    df = latest.join(known, "licitacion_id", "left")

    if deleted is not None:
        cancelled = deleted.where(F.upper(F.col("motivo")) == "ANULADA").select("licitacion_id").distinct() \
            .withColumn("_borrada", F.lit(True))
        df = df.join(cancelled, "licitacion_id", "left")
    else:
        df = df.withColumn("_borrada", F.lit(None).cast("boolean"))

    cpv = F.split(F.coalesce(F.col("cpv"), F.lit("")), ",")
    provinces = list(cfg["provincias"])
    nuts_ok = F.lit(False)
    for prefix in cfg["nuts"]:
        nuts_ok = nuts_ok | F.coalesce(F.col("nuts"), F.lit("")).startswith(prefix)
    dir3_town = F.when(F.col("dir3").rlike(r"^L01\d{6}"), F.substring("dir3", 4, 5))
    no_place = F.col("nuts").isNull() | (F.col("nuts") == "ES")
    local = F.substring(F.coalesce(F.col("organo_cp"), F.lit("")), 1, 2).isin(provinces) \
        | F.substring(F.coalesce(dir3_town, F.lit("")), 1, 2).isin(provinces)
    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("licitacion_id").isNull()),
        ("NOT_WORKS", ~((F.col("tipo_contrato") == "3") | any_prefix(cpv, ["45"]))),
        ("OUT_OF_TERRITORY", ~(nuts_ok | (no_place & local))),
    ])

    text = plain_text(F.concat_ws(" ", F.col("titulo"), F.col("objeto")), cfg.get("nombres_provincia"))
    nuts_province = F.lit(None).cast("string")
    for code, province in cfg["nuts_provincia"].items():
        nuts_province = F.when(F.col("nuts") == code, F.lit(province)).otherwise(nuts_province)
    start = F.coalesce(F.col("_inicio"), F.col("_contrato"), F.date_add(F.col("fecha_adjudicacion"), 30),
                       F.date_add(F.col("_limite"), 60), F.date_add(F.coalesce(F.col("_pub"), F.col("_visto")), 90))
    length = F.coalesce(F.col("_meses"), F.lit(float(cfg["meses_por_defecto"])))
    kept = (
        kept
        .withColumn("_texto", text)
        .withColumn("_codigo_dir3", dir3_town)
        .withColumn("_provincia_nuts", nuts_province)
        .withColumn("fecha_publicacion", F.coalesce(F.col("_pub"), F.col("_visto")))
        .withColumn("anio_publicacion", F.year("fecha_publicacion"))
        .withColumn("inicio_ejecucion", start)
        .withColumn("meses_ejecucion", length)
        .withColumn("fin_ejecucion", F.coalesce(
            F.when(F.col("_fin") >= start, F.col("_fin")),
            F.date_add(start, F.round(length * F.lit(30.4)).cast("int"))))
        .withColumn("anulada", (F.coalesce(F.upper(F.col("estado")), F.lit("")) == "ANUL")
                    | F.coalesce(F.col("_borrada"), F.lit(False)))
        .withColumn("mov_tierras", any_prefix(cpv, cfg["cpv_mov_tierras"])
                    | F.col("_texto").rlike(cfg["palabras_mov_tierras"]))
        .withColumn("importe_licitacion", F.coalesce(F.col("importe_sin_iva").cast("double"),
                                                     F.col("importe_estimado").cast("double")))
        .withColumn("importe_adjudicado", F.col("importe_adjudicado").cast("double"))
    )

    # Municipality: the town council itself, or the names in the title for the other bodies.
    by_body = kept.where(F.col("_codigo_dir3").isNotNull()).withColumn("codigo_ine", F.col("_codigo_dir3")) \
        .withColumn("cruce", F.lit("dir3")).withColumn("peso", F.lit(1.0))
    others = kept.where(F.col("_codigo_dir3").isNull())
    found = others.select("licitacion_id", "_texto", "_provincia_nuts").join(
        F.broadcast(towns),
        (F.instr(F.col("_texto"), F.col("nombre_plano")) > 0)
        & (F.col("_provincia_nuts").isNull() | (F.col("provincia_municipio") == F.col("_provincia_nuts"))),
    ).select("licitacion_id", "codigo_ine", "nombre_plano")
    inside = found.alias("a").join(
        found.alias("b"),
        (F.col("a.licitacion_id") == F.col("b.licitacion_id"))
        & (F.length(F.col("b.nombre_plano")) > F.length(F.col("a.nombre_plano")))
        & (F.instr(F.col("b.nombre_plano"), F.col("a.nombre_plano")) > 0),
        "left_semi",
    ).select("licitacion_id", "codigo_ine")
    found = found.join(inside, ["licitacion_id", "codigo_ine"], "left_anti").dropDuplicates(
        ["licitacion_id", "codigo_ine"])
    found = found.withColumn("peso", F.lit(1.0) / F.count(F.lit(1)).over(Window.partitionBy("licitacion_id")))
    by_title = others.join(found.select("licitacion_id", "codigo_ine", "peso"), "licitacion_id") \
        .withColumn("cruce", F.lit("titulo"))
    unplaced = others.join(found.select("licitacion_id").distinct(), "licitacion_id", "left_anti") \
        .withColumn("codigo_ine", F.lit(None).cast("string")).withColumn("cruce", F.lit("sin_municipio")) \
        .withColumn("peso", F.lit(1.0))

    columns = ["licitacion_id", "expediente", "titulo", "organo", "organo_tipo", "dir3", "estado", "anulada",
               "tipo_contrato", "cpv", "mov_tierras", "importe_licitacion", "importe_adjudicado", "fecha_publicacion",
               "anio_publicacion", "fecha_adjudicacion", "inicio_ejecucion", "fin_ejecucion", "meses_ejecucion", "nuts",
               "codigo_ine", "cruce", "peso", "versiones", "anio_fichero", "enlace", "_provincia_nuts"]
    out = by_body.select(*columns).unionByName(by_title.select(*columns)).unionByName(unplaced.select(*columns))
    out = out.withColumn("provincia", F.coalesce(F.substring("codigo_ine", 1, 2), F.col("_provincia_nuts"))) \
        .drop("_provincia_nuts")
    return out.withColumn("audit_loaded_at", F.current_timestamp()), rejected


def buildings(catastro):
    """(rows of f_edificacion_municipio, rejected): buildings of the Catastro by municipality and year built."""
    this_year = date.today().year
    df = catastro.select(
        normalized_code(F.col("codigo_ine")).alias("codigo_ine"),
        F.col("referencia"),
        F.col("anio_construccion").cast("int").alias("anio"),
        F.lower(F.col("estado")).alias("estado"),
        F.coalesce(F.col("uso"), F.lit("")).alias("uso"),
        F.coalesce(F.col("n_viviendas").cast("double"), F.lit(0.0)).alias("viviendas"),
        F.coalesce(F.col("superficie_m2").cast("double"), F.lit(0.0)).alias("m2"),
    )
    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("codigo_ine").isNull() | F.col("referencia").isNull()),
        ("INVALID_VALUE", F.col("anio").isNull() | (F.col("anio") < 1800) | (F.col("anio") > this_year)),
        ("RUIN", F.coalesce(F.col("estado"), F.lit("")) == "ruin"),
    ])
    kept, duplicated = keep_first(kept, ["referencia"], [F.col("anio").desc()], "DUPLICATE_ID")

    use = F.col("uso")
    out = kept.groupBy("codigo_ine", "anio").agg(
        F.count(F.lit(1)).alias("edificios"),
        F.sum("viviendas").alias("viviendas"),
        F.sum("m2").alias("superficie_m2"),
        F.sum(F.when(use.startswith("1_"), 1).otherwise(0)).alias("edificios_residenciales"),
        F.sum(F.when(use.startswith("1_"), F.col("m2")).otherwise(0.0)).alias("superficie_residencial_m2"),
        F.sum(F.when(use.startswith("2_"), F.col("m2")).otherwise(0.0)).alias("superficie_agraria_m2"),
        F.sum(F.when(use.startswith("3_"), F.col("m2")).otherwise(0.0)).alias("superficie_industrial_m2"),
        F.sum(F.when(use.startswith("4_"), F.col("m2")).otherwise(0.0)).alias("superficie_servicios_m2"),
    )
    return out.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def projections(ieex):
    """(rows of f_proyeccion_ame, rejected)."""
    df = ieex.select(
        F.lpad(F.trim(F.col("ame_codigo")), 4, "0").alias("ame_codigo"), F.trim(F.col("ame_nombre")).alias("ame_nombre"),
        F.col("anio").cast("int").alias("anio"), F.col("poblacion").cast("double").alias("poblacion"))
    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("ame_codigo").isNull() | F.col("anio").isNull()),
        ("INVALID_VALUE", F.col("poblacion").isNull() | (F.col("poblacion") < 0)),
    ])
    kept, duplicated = keep_first(kept, ["ame_codigo", "anio"], [F.col("poblacion").desc()], "DUPLICATE_ID")
    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def ame_towns(members):
    """(rows of d_ame_municipio, rejected): one AME per INE code."""
    df = members.select(normalized_code(F.col("codigo_ine")).alias("codigo_ine"), F.trim(F.col("municipio")).alias(
        "municipio"), F.lpad(F.trim(F.col("ame_codigo")), 4, "0").alias("ame_codigo"),
        F.trim(F.col("ame_nombre")).alias("ame_nombre"))
    kept, rejected = split_rejects(df, [("NULL_KEY", F.col("codigo_ine").isNull() | F.col("ame_codigo").isNull())])
    kept, duplicated = keep_first(kept, ["codigo_ine"], [F.col("ame_codigo")], "DUPLICATE_ID")
    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def birds(gbif, kind: str, key: str):
    """(rows of f_aves_celda_anio or f_aves_celda_mes, rejected) from the GBIF counts."""
    df = gbif.where(F.col("tipo") == F.lit(kind)).select(
        F.trim(F.col("celda_id")).alias("celda_id"), F.trim(F.col("taxon")).alias("taxon"),
        F.col("clave").cast("int").alias(key), F.col("registros").cast("long").alias("registros"))
    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("celda_id").isNull() | F.col("taxon").isNull() | F.col(key).isNull()),
        ("INVALID_VALUE", F.col("registros").isNull() | (F.col("registros") < 0)),
    ])
    kept, duplicated = keep_first(kept, ["celda_id", "taxon", key], [F.col("registros").desc()], "DUPLICATE_ID")
    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def cell_id(lat, lon, size: float = CELL_DEGREES):
    """Weather cell of a position, the same id as job_landing_meteo_openmeteo.cell_of."""
    return F.format_string("%.2f_%.2f", F.round(F.round(lat / F.lit(size)) * F.lit(size), 4),
                           F.round(F.round(lon / F.lit(size)) * F.lit(size), 4))


def burnt_areas(effis):
    """(rows of f_area_quemada, rejected)."""
    date_text = F.substring(F.trim(F.col("fecha_incendio")), 1, 10)
    df = effis.select(
        F.trim(F.col("id").cast("string")).alias("id"),
        F.coalesce(F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd")),
                   F.try_to_timestamp(date_text, F.lit("yyyy/MM/dd")),
                   F.try_to_timestamp(date_text, F.lit("dd/MM/yyyy"))).cast("date").alias("fecha"),
        F.try_to_timestamp(F.substring(F.trim(F.col("ultima_actualizacion")), 1, 19),
                           F.lit("yyyy-MM-dd HH:mm:ss")).alias("ultima_actualizacion"),
        F.col("area_ha").cast("double").alias("area_ha"),
        F.trim(F.col("pais")).alias("pais"), F.trim(F.col("provincia")).alias("provincia"),
        F.trim(F.col("municipio")).alias("municipio"),
        F.col("latitud").cast("double").alias("latitud"), F.col("longitud").cast("double").alias("longitud"),
    )
    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("fecha").isNull()),
        ("NO_GEOMETRY", F.col("latitud").isNull() | F.col("longitud").isNull()),
    ])
    kept, duplicated = keep_first(kept, ["id"], [F.col("ultima_actualizacion").desc_nulls_last()], "DUPLICATE_ID")
    out = (
        kept.withColumn("anio", F.year("fecha"))
        .withColumn("celda_id", cell_id(F.col("latitud"), F.col("longitud")))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )
    return out, union_rejects([rejected, duplicated])


def inside(lat: float, lon: float, points: list) -> bool:
    """Point in polygon (ray casting) for (lat, lon) points."""
    hit = False
    j = len(points) - 1
    for i in range(len(points)):
        lat_i, lon_i = points[i]
        lat_j, lon_j = points[j]
        if (lon_i > lon) != (lon_j > lon):
            crossing = lat_i + (lon - lon_i) / (lon_j - lon_i) * (lat_j - lat_i)
            if lat < crossing:
                hit = not hit
        j = i
    return hit


def local_wall_clock(values):
    """CAP times with their offset -> naive wall clock of Madrid, as the rest of the lakehouse."""
    import pandas as pd

    stamps = pd.to_datetime(values, utc=True, errors="coerce", format="ISO8601")
    return stamps.dt.tz_convert("Europe/Madrid").dt.tz_localize(None)


def weather_warnings(spark, aemet, cells: list):
    """
    (rows of f_aviso_meteo, rejected rows as a list of dicts) from the
    warnings of Bronze and the cells (celda_id, lat, lon) of the study.
    """
    import pandas as pd

    pdf = aemet.select("identificador", "tipo_mensaje", "enviado", "inicio", "fin", "fenomeno", "nivel",
                       "probabilidad", "parametro", "zona", "zona_nombre", "poligono").toPandas()
    rows, rejected = [], []
    for record in pdf.to_dict("records"):
        if not record["identificador"] or not record["inicio"] or not record["fin"]:
            rejected.append({**record, "_motivo": "NULL_KEY"})
            continue
        points = []
        for pair in str(record["poligono"] or "").split():
            parts = pair.split(",")
            if len(parts) == 2:
                points.append((float(parts[0]), float(parts[1])))
        if len(points) < 3:
            rejected.append({**record, "_motivo": "NO_GEOMETRY"})
            continue
        for cell, lat, lon in cells:
            if inside(lat, lon, points):
                rows.append({**{k: v for k, v in record.items() if k != "poligono"}, "celda_id": cell})

    out = pd.DataFrame(rows, columns=[c for c in pdf.columns if c != "poligono"] + ["celda_id"])
    for column, source in (("enviado_ts", "enviado"), ("inicio_ts", "inicio"), ("fin_ts", "fin")):
        out[column] = local_wall_clock(out[source]) if len(out) else pd.Series(dtype="datetime64[ns]")
    code = out["fenomeno"].fillna("").astype(str)
    out["fenomeno_codigo"] = code.str.split(";").str[0].str.strip().str.upper().replace("", None)
    out["fenomeno_nombre"] = code.str.split(";").str[1].str.strip() if len(out) else None
    out["nivel"] = out["nivel"].fillna("").astype(str).str.strip().str.lower().replace("", None)
    out["nivel_num"] = out["nivel"].map(WARNING_LEVELS)
    out["aviso_id"] = out["identificador"].astype(str) + "|" + out["zona"].fillna("").astype(str)
    out = out.drop(columns=["enviado", "inicio", "fin", "fenomeno"]).drop_duplicates(["aviso_id", "celda_id"])
    out = out[out["inicio_ts"].notna() & out["fin_ts"].notna()]
    for column in out.columns:
        if out[column].dtype == object:
            out[column] = out[column].astype("string")
    df = (
        spark.createDataFrame(out) if len(out) else None
    )
    if df is not None:
        df = df.withColumn("anio", F.year("inicio_ts")).withColumn("audit_loaded_at", F.current_timestamp())
    return df, rejected


def confidence_level(values):
    """FIRMS confidence level (l, n, h for VIIRS, 0 - 100 for MODIS) as 1 low, 2 nominal, 3 high."""
    import numpy as np
    import pandas as pd

    text = pd.Series(values).fillna("").astype(str).str.strip().str.lower()
    named = text.map({"l": 1.0, "low": 1.0, "n": 2.0, "nominal": 2.0, "h": 3.0, "high": 3.0})
    number = pd.to_numeric(text, errors="coerce")
    by_number = pd.Series(np.where(number < 30, 1.0, np.where(number < 80, 2.0, 3.0)), index=text.index)
    return named.fillna(by_number.where(number.notna()))


def hotspots(spark, firms, cells: list, max_step: float = 1.5 * CELL_DEGREES):
    """(rows of f_foco_incendio, rejected rows as a list of dicts) from the fires of Bronze and the cells."""
    import numpy as np
    import pandas as pd

    pdf = firms.toPandas()
    for column in ("latitud", "longitud", "fecha", "hora_utc", "satelite", "instrumento", "confianza", "frp",
                   "dia_noche", "fuente"):
        if column not in pdf.columns:
            pdf[column] = None
    pdf["latitud"] = pd.to_numeric(pdf["latitud"], errors="coerce")
    pdf["longitud"] = pd.to_numeric(pdf["longitud"], errors="coerce")
    clock = pdf["hora_utc"].fillna("").astype(str).str.strip().str.zfill(4)
    pdf["ts_utc"] = pd.to_datetime(pdf["fecha"].astype(str) + " " + clock, format="%Y-%m-%d %H%M", errors="coerce")
    bad = pdf["latitud"].isna() | pdf["longitud"].isna() | pdf["ts_utc"].isna()
    rejected = [{**r, "_motivo": "NULL_KEY"} for r in pdf[bad].astype(str).to_dict("records")]
    pdf = pdf[~bad].copy()
    duplicated = pdf.duplicated(["satelite", "ts_utc", "latitud", "longitud"])
    rejected += [{**r, "_motivo": "DUPLICATE_KEY"} for r in pdf[duplicated].astype(str).to_dict("records")]
    pdf = pdf[~duplicated].copy()

    pdf["ts"] = pdf["ts_utc"].dt.tz_localize("UTC").dt.tz_convert("Europe/Madrid").dt.tz_localize(None)
    pdf["sensor"] = np.where(pdf["instrumento"].fillna("").str.upper().str.contains("VIIRS"), "VIIRS", "MODIS")
    pdf["confianza_nivel"] = confidence_level(pdf["confianza"]).to_numpy()
    pdf["frp"] = pd.to_numeric(pdf["frp"], errors="coerce")
    pdf["celda_id"], pdf["distancia_celda_km"] = None, np.nan
    if len(pdf) and cells:
        ids = np.array([c[0] for c in cells])
        clat = np.array([c[1] for c in cells])
        clon = np.array([c[2] for c in cells])
        dlat = np.abs(pdf["latitud"].to_numpy()[:, None] - clat[None, :])
        dlon = np.abs(pdf["longitud"].to_numpy()[:, None] - clon[None, :])
        # The nearest cell in km: a degree of longitude is cos(latitude) of a degree of latitude.
        nearest = np.argmin(dlat ** 2 + (dlon * np.cos(np.radians(pdf["latitud"].to_numpy()))[:, None]) ** 2, axis=1)
        rows = np.arange(len(pdf))
        close = (dlat[rows, nearest] <= max_step + 1e-9) & (dlon[rows, nearest] <= max_step + 1e-9)
        km = 111.2 * np.sqrt(dlat[rows, nearest] ** 2
                             + (dlon[rows, nearest] * np.cos(np.radians(pdf["latitud"].to_numpy()))) ** 2)
        pdf["celda_id"] = np.where(close, ids[nearest], None)
        pdf["distancia_celda_km"] = np.where(close, km, np.nan)
    pdf["foco_id"] = (pdf["satelite"].fillna("").astype(str) + "|" + pdf["ts_utc"].dt.strftime("%Y%m%d%H%M") + "|"
                      + pdf["latitud"].round(5).astype(str) + "|" + pdf["longitud"].round(5).astype(str))
    out = pdf[["foco_id", "fuente", "sensor", "satelite", "ts_utc", "ts", "latitud", "longitud", "confianza",
               "confianza_nivel", "frp", "dia_noche", "celda_id", "distancia_celda_km"]].copy()
    for column in ("foco_id", "fuente", "sensor", "satelite", "confianza", "dia_noche", "celda_id"):
        out[column] = out[column].astype("string")
    if not len(out):
        return None, rejected
    df = spark.createDataFrame(out).withColumn("anio", F.year("ts")).withColumn("audit_loaded_at",
                                                                                F.current_timestamp())
    return df, rejected


def fire_danger(spark, bronze, cells: list, max_step: float = 0.2):
    """(rows of f_fwi_celda_dia, rejected rows as a list of dicts) from the indices of Bronze and the cells."""
    import numpy as np
    import pandas as pd

    pdf = bronze.toPandas()
    for column in ["fecha", "latitud", "longitud", "pieza"] + FWI_COLUMNS:
        if column not in pdf.columns:
            pdf[column] = None
    for column in ["latitud", "longitud"] + FWI_COLUMNS:
        pdf[column] = pd.to_numeric(pdf[column], errors="coerce")
    pdf["fecha"] = pd.to_datetime(pdf["fecha"], errors="coerce")
    bad = pdf["fecha"].isna() | pdf["latitud"].isna() | pdf["longitud"].isna()
    rejected = [{**r, "_motivo": "NULL_KEY"} for r in pdf[bad].astype(str).to_dict("records")]
    pdf = pdf[~bad].copy()
    pdf["conjunto"] = np.where(pdf["pieza"].fillna("").str.contains("intermediate"), "intermediate", "consolidated")
    # The consolidated dataset first, then the newest piece (the one with more months of the year).
    pdf = pdf.sort_values(["fecha", "latitud", "longitud", "conjunto", "pieza"], ascending=[True, True, True, True,
                                                                                            False])
    duplicated = pdf.duplicated(["fecha", "latitud", "longitud"])
    rejected += [{**r, "_motivo": "DUPLICATE_KEY"} for r in pdf[duplicated].astype(str).to_dict("records")]
    pdf = pdf[~duplicated]
    if not len(pdf) or not cells:
        return None, rejected

    points = pdf[["latitud", "longitud"]].drop_duplicates().to_numpy()
    nearest = []
    for cell_id, lat, lon in cells:
        dlat = points[:, 0] - lat
        dlon = (points[:, 1] - lon) * np.cos(np.radians(lat))
        best = int(np.argmin(dlat ** 2 + dlon ** 2))
        if abs(points[best, 0] - lat) <= max_step + 1e-9 and abs(points[best, 1] - lon) <= max_step + 1e-9:
            nearest.append((cell_id, points[best, 0], points[best, 1],
                            float(111.2 * np.sqrt(dlat[best] ** 2 + dlon[best] ** 2))))
        else:
            logger.warning("Cell %s without a point of the fire danger grid within %s degrees", cell_id, max_step)
    if not nearest:
        return None, rejected
    match = pd.DataFrame(nearest, columns=["celda_id", "latitud", "longitud", "distancia_punto_km"])
    out = match.merge(pdf, on=["latitud", "longitud"], how="inner")
    out = out.rename(columns={"latitud": "latitud_punto", "longitud": "longitud_punto"})
    out["fecha"] = out["fecha"].dt.date
    out = out[["celda_id", "fecha"] + FWI_COLUMNS + ["latitud_punto", "longitud_punto", "distancia_punto_km",
                                                     "conjunto"]]
    out["celda_id"] = out["celda_id"].astype("string")
    out["conjunto"] = out["conjunto"].astype("string")
    df = spark.createDataFrame(out)
    for column in FWI_COLUMNS:
        df = df.withColumn(column, F.when(F.isnan(column), F.lit(None).cast("double")).otherwise(F.col(column)))
    df = df.withColumn("anio", F.year("fecha")).withColumn("audit_loaded_at", F.current_timestamp())
    return df, rejected


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

    placsp = optional(spark, PLACSP_TABLE)
    if placsp is not None and spark.catalog.tableExists(silver_table(POPULATION)):
        towns = town_names(spark.table(silver_table(POPULATION)))
        out, rejected = tenders(placsp, optional(spark, PLACSP_DELETED_TABLE), towns)
        write_table(out, silver_table(TENDERS))
        write_rejected(rejected, TENDERS, args.run_id)
        written = spark.table(silver_table(TENDERS))
        total = written.select("licitacion_id").distinct().count()
        dq.add(TENDERS, "licitaciones", total)
        dq.add(TENDERS, "filas", written.count())
        for row in written.groupBy("cruce").agg(F.countDistinct("licitacion_id").alias("n")).collect():
            dq.add(TENDERS, f"licitaciones:{row['cruce']}", row["n"], total)
        dq.add(TENDERS, "anuladas", written.where(F.col("anulada")).select("licitacion_id").distinct().count(), total)
        if rejected is not None:
            for row in rejected.groupBy("_motivo").count().collect():
                dq.add(TENDERS, f"rechazo:{row['_motivo']}", row["count"])
    elif placsp is not None:
        logger.warning("%s needs %s for the names of the municipalities", TENDERS, silver_table(POPULATION))

    catastro = optional(spark, CATASTRO_TABLE)
    if catastro is not None:
        total = catastro.count()
        out, rejected = buildings(catastro)
        write_table(out, silver_table(BUILDINGS))
        write_rejected(rejected, BUILDINGS, args.run_id)
        written = spark.table(silver_table(BUILDINGS))
        dq.add(BUILDINGS, "edificios_entrada", total)
        dq.add(BUILDINGS, "edificios", written.agg(F.sum("edificios")).first()[0])
        dq.add(BUILDINGS, "municipios", written.select("codigo_ine").distinct().count())
        if rejected is not None:
            for row in rejected.groupBy("_motivo").count().collect():
                dq.add(BUILDINGS, f"rechazo:{row['_motivo']}", row["count"], total, umbral_pct=10.0)

    for source, entity, build in ((IEEX_TABLE, PROJECTION, projections), (IEEX_AME_TABLE, AME, ame_towns)):
        bronze = optional(spark, source)
        if bronze is None:
            continue
        out, rejected = build(bronze)
        write_table(out, silver_table(entity))
        write_rejected(rejected, entity, args.run_id)
        dq.add(entity, "filas", spark.table(silver_table(entity)).count())

    gbif = optional(spark, GBIF_TABLE)
    if gbif is not None:
        for entity, kind, key in ((BIRDS_YEAR, "anio", "anio"), (BIRDS_MONTH, "mes", "mes")):
            out, rejected = birds(gbif, kind, key)
            write_table(out, silver_table(entity))
            write_rejected(rejected, entity, args.run_id)
            written = spark.table(silver_table(entity))
            dq.add(entity, "filas", written.count())
            dq.add(entity, "celdas", written.select("celda_id").distinct().count())

    effis = optional(spark, EFFIS_TABLE)
    if effis is not None:
        total = effis.count()
        out, rejected = burnt_areas(effis)
        write_table(out, silver_table(FIRES))
        write_rejected(rejected, FIRES, args.run_id)
        written = spark.table(silver_table(FIRES))
        dq.add(FIRES, "areas_entrada", total)
        dq.add(FIRES, "areas", written.count(), total)
        span = written.agg(F.min("fecha"), F.max("fecha")).first()
        dq.add(FIRES, "rango_fechas", written.count(), detalle=f"{span[0]} - {span[1]}")
        if rejected is not None:
            for row in rejected.groupBy("_motivo").count().collect():
                dq.add(FIRES, f"rechazo:{row['_motivo']}", row["count"], total, umbral_pct=10.0)

    aemet = optional(spark, AEMET_TABLE)
    if aemet is not None and spark.catalog.tableExists(WEATHER_CELLS_TABLE):
        study = [
            (r["celda_id"], float(r["celda_id"].split("_")[0]), float(r["celda_id"].split("_")[1]))
            for r in spark.table(WEATHER_CELLS_TABLE).select("celda_id").distinct().collect() if r["celda_id"]
        ]
        total = aemet.count()
        out, rejected = weather_warnings(spark, aemet, study)
        if out is not None:
            write_table(out, silver_table(WARNINGS))
            written = spark.table(silver_table(WARNINGS))
            dq.add(WARNINGS, "mensajes_zona_entrada", total)
            dq.add(WARNINGS, "filas", written.count())
            dq.add(WARNINGS, "avisos", written.select("aviso_id").distinct().count())
            dq.add(WARNINGS, "celdas", written.select("celda_id").distinct().count(), len(study))
            span = written.agg(F.min("inicio_ts"), F.max("inicio_ts")).first()
            dq.add(WARNINGS, "rango_fechas", written.count(), detalle=f"{span[0]} - {span[1]}")
        for reason in sorted({r["_motivo"] for r in rejected}):
            dq.add(WARNINGS, f"rechazo:{reason}", sum(r["_motivo"] == reason for r in rejected), total,
                   umbral_pct=5.0)
    elif aemet is not None:
        logger.warning("%s needs %s for the cells of the study", WARNINGS, WEATHER_CELLS_TABLE)

    firms = optional(spark, FIRMS_TABLE)
    if firms is not None and spark.catalog.tableExists(WEATHER_CELLS_TABLE):
        study = [
            (r["celda_id"], float(r["celda_id"].split("_")[0]), float(r["celda_id"].split("_")[1]))
            for r in spark.table(WEATHER_CELLS_TABLE).select("celda_id").distinct().collect() if r["celda_id"]
        ]
        total = firms.count()
        out, rejected = hotspots(spark, firms, study)
        if out is not None:
            write_table(out, silver_table(HOTSPOTS), partition_by=["anio"])
            written = spark.table(silver_table(HOTSPOTS))
            rows = written.count()
            dq.add(HOTSPOTS, "focos_entrada", total)
            dq.add(HOTSPOTS, "filas", rows, total)
            dq.add(HOTSPOTS, "con_celda", written.where(F.col("celda_id").isNotNull()).count(), rows)
            span = written.agg(F.min("ts"), F.max("ts")).first()
            dq.add(HOTSPOTS, "rango_fechas", rows, detalle=f"{span[0]} - {span[1]}")
        for reason in sorted({r["_motivo"] for r in rejected}):
            dq.add(HOTSPOTS, f"rechazo:{reason}", sum(r["_motivo"] == reason for r in rejected), total,
                   umbral_pct=5.0)

    fwi = optional(spark, FWI_TABLE)
    if fwi is not None and spark.catalog.tableExists(WEATHER_CELLS_TABLE):
        study = [
            (r["celda_id"], float(r["celda_id"].split("_")[0]), float(r["celda_id"].split("_")[1]))
            for r in spark.table(WEATHER_CELLS_TABLE).select("celda_id").distinct().collect() if r["celda_id"]
        ]
        total = fwi.count()
        out, rejected = fire_danger(spark, fwi, study)
        if out is not None:
            write_table(out, silver_table(FIRE_DANGER), partition_by=["anio"])
            written = spark.table(silver_table(FIRE_DANGER))
            rows = written.count()
            dq.add(FIRE_DANGER, "puntos_dia_entrada", total)
            dq.add(FIRE_DANGER, "filas", rows)
            dq.add(FIRE_DANGER, "celdas", written.select("celda_id").distinct().count(), len(study))
            dq.add(FIRE_DANGER, "sin_fwi", written.where(F.col("fwi").isNull()).count(), rows, umbral_pct=2.0)
            span = written.agg(F.min("fecha"), F.max("fecha")).first()
            dq.add(FIRE_DANGER, "rango_fechas", rows, detalle=f"{span[0]} - {span[1]}")
        # A day and point repeated by two pieces is expected (consolidated and intermediate): no threshold.
        for reason in sorted({r["_motivo"] for r in rejected}):
            dq.add(FIRE_DANGER, f"rechazo:{reason}", sum(r["_motivo"] == reason for r in rejected), total,
                   umbral_pct=5.0 if reason == "NULL_KEY" else None)

    dq.flush()
    logger.info("Silver completed: public data")


if __name__ == "__main__":
    main()
