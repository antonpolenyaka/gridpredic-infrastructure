"""
Gold dimension of the CTs and the maps that attach every TedisNet signal to
the CTs it describes.

Three tables:

- dim_ct: one row per (distribuidora_id, ct_id), the unit of prediction. It
  takes the Calser CT in its latest period (the topology Calser has today),
  its municipality, its transformer in TedisNet and the decisions of this
  layer: the imputed power and whether the CT enters the study (en_estudio).
- map_tag_ct: one row per tag with the CT it belongs to (anchor_id) and the
  network group the CT hangs from (grupo_red_id).
- map_aguas_arriba: one row per (transformer, upstream switching element),
  learnt from the cuts of the SCADA, with the moment it became known.

How a tag reaches a CT. In TedisNet a transformer (type 145 TRAFO CT,
ShortName = Calser CT_ID, for instance 06011) hangs from its CT (type 144,
ShortName 0601), and the cells and switches of the CT hang from the same CT
element. The anchor of an element is therefore its nearest ancestor of type
144 (or of type 145 if there is none), and every tag below an anchor
describes all the transformers of that CT. The element the anchor hangs from
(the line or zone, "MONTANCHEZ" in /EODSLU/TORRE DE SANTA MARIA/MONTANCHEZ/
0601/06011:TRA) is its network group. A tag without element (the connection
state of a device, for instance) takes the anchor most tags of its device
have.

Why the cuts are also used. In EOSA the switch that cuts a CT is usually a
feeder breaker of a substation (CC/Estados/EPDSLU/S.T.R. CORIA/CAÑAVERAL/
I.A.A.T./TORREJONCILLO 3), and that breaker does not sit above the CT in the
functional hierarchy (the CT is under S.T.R. GARROVILLAS/CAÑAVERAL). The SCADA
knows the electrical path because it floods the network graph. Until Gold
walks the graph itself, map_aguas_arriba records, for every transformer, the
elements whose state change produced an Off event on it.

That knowledge is learnt from the cuts, so it has a date: before the first
cut of a pair nobody knew that the breaker fed the CT, and a CT that is only
cut once, in the test period, would carry its future cut in every earlier
hour. primer_conocido_ts is the arrival of the first cut of each pair, on the
Calser clock, and the features use a pair only from that moment on.
n_posiciones_aguas_arriba of dim_ct counts the pairs of the whole history:
it describes the dimension and is never a feature.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    family_expr,
    get_spark,
    gold_table,
    known_time,
    load_params,
    logger,
    optional_table,
    parse_args,
    require_table,
    scale_factor,
    silver_table,
    tedisnet_time,
    write_table,
)


DIM_TABLE = gold_table("dim_ct")
MAP_TAG_TABLE = gold_table("map_tag_ct")
MAP_UPSTREAM_TABLE = gold_table("map_aguas_arriba")

MAX_HIERARCHY_DEPTH = 40

ZONE_CODES = {"TZ_URBAN": 1, "TZ_SUBUR": 2, "TZ_RUCON": 3, "TZ_RUDIS": 4}


def column_or_null(df: DataFrame, name: str, data_type: str = "string"):
    """
    A column that an older Silver run may not have yet comes back as null.
    """
    if name in df.columns:
        return F.col(name)

    return F.lit(None).cast(data_type)


# ---------------------------------------------------------------------------
# Calser side
# ---------------------------------------------------------------------------

def latest_cts(cts: DataFrame, periods: DataFrame) -> DataFrame:
    """
    The Calser topology is versioned by period. The CT keeps the attributes
    of its latest period (DEFAULT, the open one, when it is the newest).
    """
    ranked = Window.partitionBy("distribuidora_id", "id").orderBy(
        F.col("fecha_inicio").desc_nulls_last(),
        F.col("periodo_id").desc(),
    )

    return (
        cts.join(
            periods.select("distribuidora_id", "periodo_id", "fecha_inicio"),
            ["distribuidora_id", "periodo_id"],
            "left",
        )
        .withColumn("_rn", F.row_number().over(ranked))
        .where(F.col("_rn") == 1)
        .drop("_rn", "fecha_inicio")
    )


def municipality(municipios: DataFrame) -> DataFrame:
    return municipios.select(
        "distribuidora_id",
        "periodo_id",
        F.col("id").alias("municipio_id"),
        F.col("nombre").alias("municipio_nombre"),
        F.col("provincia_id"),
        F.col("tipo_zona_estat"),
        column_or_null(municipios, "latitud", "double").alias("latitud"),
        column_or_null(municipios, "longitud", "double").alias("longitud"),
    )


def outputs_per_ct(salidas: DataFrame) -> DataFrame:
    return salidas.groupBy("distribuidora_id", "periodo_id", "ct_id").agg(
        F.countDistinct("id").alias("n_salidas")
    )


# ---------------------------------------------------------------------------
# TedisNet hierarchy
# ---------------------------------------------------------------------------

def hierarchy_closure(elements: DataFrame) -> DataFrame:
    """
    One row per (element, ancestor) including the element itself at level 0.
    The climb stops at the roots or after MAX_HIERARCHY_DEPTH levels, because
    ParentElementId is not guaranteed to be acyclic.
    """
    parents = elements.select(
        F.col("id").alias("_next"),
        F.col("parent_id").alias("_next_parent"),
    )

    current = elements.select(
        F.col("id").alias("elemento_id"),
        F.col("id").alias("ancestro_id"),
        F.lit(0).alias("nivel"),
        F.col("parent_id").alias("_next"),
    ).localCheckpoint(eager=True)

    parts = [current.drop("_next")]

    for depth in range(1, MAX_HIERARCHY_DEPTH + 1):
        step = (
            current.where(F.col("_next").isNotNull())
            .join(parents, "_next")
            .select(
                "elemento_id",
                F.col("_next").alias("ancestro_id"),
                F.lit(depth).alias("nivel"),
                F.col("_next_parent").alias("_next"),
            )
            .localCheckpoint(eager=True)
        )

        if step.isEmpty():
            break

        parts.append(step.drop("_next"))
        current = step

    closure = parts[0]

    for part in parts[1:]:
        closure = closure.unionByName(part)

    return closure.localCheckpoint(eager=True)


def element_scope(elements: DataFrame, closure: DataFrame, params: dict) -> DataFrame:
    """
    For every element: its anchor (nearest ancestor-or-self of type CT, or
    of type TRAFO CT if it is not under any CT) and the parent of the anchor.
    """
    tipo_ct = params["ct"]["tipo_ct"]
    tipo_trafo = params["ct"]["tipo_trafo_ct"]

    typed = closure.join(
        elements.select(
            F.col("id").alias("ancestro_id"),
            F.col("tipo_elemento_id").alias("ancestro_tipo"),
        ),
        "ancestro_id",
    )

    first_ct = (
        typed.where(F.col("ancestro_tipo") == F.lit(tipo_ct))
        .groupBy("elemento_id")
        .agg(F.min_by("ancestro_id", "nivel").alias("_anchor_ct"))
    )

    first_trafo = (
        typed.where(F.col("ancestro_tipo") == F.lit(tipo_trafo))
        .groupBy("elemento_id")
        .agg(F.min_by("ancestro_id", "nivel").alias("_anchor_trafo"))
    )

    parent_of = elements.select(
        F.col("id").alias("anchor_id"),
        F.col("parent_id").alias("anchor_parent_id"),
    )

    return (
        elements.select(
            F.col("id").alias("elemento_id"),
            F.col("parent_id").alias("elemento_parent_id"),
            F.col("tipo_elemento_id"),
        )
        .join(first_ct, "elemento_id", "left")
        .join(first_trafo, "elemento_id", "left")
        .withColumn("anchor_id", F.coalesce("_anchor_ct", "_anchor_trafo"))
        .drop("_anchor_ct", "_anchor_trafo")
        .join(parent_of, "anchor_id", "left")
    )


def add_groups(scope: DataFrame, closure: DataFrame, groups: DataFrame) -> DataFrame:
    """
    grupo_red_id: the parent of the anchor; for an element that is not under
    any CT (a switch on the line, the line itself), the nearest
    ancestor-or-self that is the group of some CT of the study.
    """
    nearest = (
        closure.join(groups.select(F.col("grupo_red_id").alias("ancestro_id")), "ancestro_id")
        .groupBy("elemento_id")
        .agg(F.min_by("ancestro_id", "nivel").alias("_grupo_cercano"))
    )

    return (
        scope.join(nearest, "elemento_id", "left")
        .withColumn(
            "grupo_red_id",
            F.when(F.col("anchor_id").isNotNull(), F.col("anchor_parent_id"))
            .otherwise(F.col("_grupo_cercano")),
        )
        .drop("_grupo_cercano")
    )


# ---------------------------------------------------------------------------
# Maps
# ---------------------------------------------------------------------------

def build_tag_map(tags: DataFrame, scope: DataFrame, params: dict) -> DataFrame:
    feats = params["features"]

    base = tags.select(
        F.col("id").alias("tag_id"),
        "elemento_id",
        "dispositivo_id",
        "distribuidora_id",
        "clase_id",
        "clase_nombre",
        "clase_prefijo",
        "unidades",
        "tiene_serie",
        "store_interval_s",
    ).join(
        scope.select("elemento_id", "anchor_id", "grupo_red_id",
                     F.col("elemento_parent_id").alias("posicion_id")),
        "elemento_id",
        "left",
    )

    # Tags without element take the anchor most tags of their device have.
    ranked = Window.partitionBy("dispositivo_id").orderBy(F.col("_n").desc(), F.col("anchor_id"))

    device_anchor = (
        base.where(F.col("anchor_id").isNotNull())
        .groupBy("dispositivo_id", "anchor_id", "grupo_red_id")
        .agg(F.count(F.lit(1)).alias("_n"))
        .withColumn("_rn", F.row_number().over(ranked))
        .where(F.col("_rn") == 1)
        .select(
            "dispositivo_id",
            F.col("anchor_id").alias("_anchor_disp"),
            F.col("grupo_red_id").alias("_grupo_disp"),
        )
    )

    from_device = F.col("elemento_id").isNull() & F.col("_anchor_disp").isNotNull()

    return (
        base.join(device_anchor, "dispositivo_id", "left")
        .withColumn(
            "origen_mapeo",
            F.when(F.col("anchor_id").isNotNull(), F.lit("ELEMENTO"))
            .when(from_device, F.lit("DISPOSITIVO"))
            .when(F.col("grupo_red_id").isNotNull(), F.lit("GRUPO")),
        )
        .withColumn("anchor_id", F.when(from_device, F.col("_anchor_disp")).otherwise(F.col("anchor_id")))
        .withColumn("grupo_red_id", F.when(from_device, F.col("_grupo_disp")).otherwise(F.col("grupo_red_id")))
        .drop("_anchor_disp", "_grupo_disp")
        .withColumn("familia_medida", family_expr("clase_nombre", feats["familias_medida"]))
        .withColumn("familia_evento", family_expr("clase_nombre", feats["familias_evento"]))
        .withColumn("factor_escala", scale_factor("unidades"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def build_upstream_map(spark, elements: DataFrame, params: dict) -> DataFrame:
    """
    For every transformer, the elements whose state change produced an Off
    event on it, the position (parent element, the bay where the protection
    signals of that breaker live) of each one and when the pair became known
    (primer_conocido_ts). Times are on the Calser clock.

    The cut tables are required: without them the job would leave an old
    map in place for the jobs that read it.
    """
    events = spark.table(require_table(spark, silver_table("f_corte_evento")))
    affected = spark.table(require_table(spark, silver_table("f_corte_elemento")))
    changes = spark.table(require_table(spark, silver_table("f_tag_value_change")))
    tags = spark.table(require_table(spark, silver_table("d_tag")))

    incoherent = column_or_null(events, "ts_incoherente", "boolean")

    triggers = (
        events.where(F.col("estado") == F.lit("OFF"))
        .where(~F.coalesce(incoherent, F.lit(False)))
        .where(F.col("tag_value_change_id").isNotNull())
        .select("evento_id", "tag_value_change_id")
        .join(changes.select(F.col("id").alias("tag_value_change_id"), "tag_id"), "tag_value_change_id")
        .join(tags.select(F.col("id").alias("tag_id"), F.col("elemento_id").alias("elemento_corte_id")), "tag_id")
        .where(F.col("elemento_corte_id").isNotNull())
        .select("evento_id", "elemento_corte_id")
        .distinct()
    )

    cut = (
        affected.where(F.col("es_trafo_ct"))
        .where(F.col("estado") == F.lit("OFF"))
        .select(
            "evento_id",
            F.col("elemento_id").alias("trafo_elemento_id"),
            tedisnet_time("ts", params).alias("ts"),
            known_time(affected, params).alias("ts_conocido"),
        )
    )

    return (
        triggers.join(cut, "evento_id")
        .groupBy("trafo_elemento_id", "elemento_corte_id")
        .agg(
            F.countDistinct("evento_id").alias("n_cortes"),
            F.min("ts").alias("primer_corte_ts"),
            F.max("ts").alias("ultimo_corte_ts"),
            F.min("ts_conocido").alias("primer_conocido_ts"),
        )
        .join(
            elements.select(
                F.col("id").alias("elemento_corte_id"),
                F.col("parent_id").alias("posicion_id"),
                F.col("nombre").alias("elemento_corte_nombre"),
            ),
            "elemento_corte_id",
            "left",
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Dimension
# ---------------------------------------------------------------------------

def build_dim(spark, params: dict):
    cts = spark.table(require_table(spark, silver_table("d_ct")))
    periods = spark.table(require_table(spark, silver_table("d_periodo")))
    municipios = spark.table(require_table(spark, silver_table("d_municipio")))
    ct_map = spark.table(require_table(spark, silver_table("d_ct_scada")))
    salidas = optional_table(spark, silver_table("d_salida"))

    ct = latest_cts(cts, periods)

    dim = (
        ct.select(
            "distribuidora_id",
            "source_database",
            "periodo_id",
            F.col("id").alias("ct_id"),
            "municipio_id",
            "potencia_instal_kva",
            "potencia_instal_admin_kva",
            "potencia_contra_mt_kw",
            "num_abonados",
        )
        .join(municipality(municipios), ["distribuidora_id", "periodo_id", "municipio_id"], "left")
    )

    if salidas is not None:
        dim = dim.join(
            outputs_per_ct(salidas),
            ["distribuidora_id", "periodo_id", "ct_id"],
            "left",
        )
    else:
        dim = dim.withColumn("n_salidas", F.lit(None).cast("long"))

    scada = ct_map.select(
        "distribuidora_id",
        "ct_id",
        F.col("elemento_id").alias("trafo_elemento_id"),
        F.col("elemento_ruta").alias("trafo_ruta"),
        "tiene_telemetria",
        "tiene_nodo",
        "mapeo_ambiguo",
        column_or_null(ct_map, "is_power_cut", "boolean").alias("is_power_cut"),
        column_or_null(ct_map, "observable", "boolean").alias("observable"),
        column_or_null(ct_map, "potencia_nominal_kva", "double").alias("potencia_nominal_kva"),
        column_or_null(ct_map, "tension_primaria_kv", "double").alias("tension_primaria_kv"),
        column_or_null(ct_map, "tension_secundaria_kv", "double").alias("tension_secundaria_kv"),
    )

    dim = dim.join(scada, ["distribuidora_id", "ct_id"], "left")

    instal = F.col("potencia_instal_kva")
    admin = F.col("potencia_instal_admin_kva")
    nominal = F.col("potencia_nominal_kva")

    zone = None

    for code, value in ZONE_CODES.items():
        condition = F.col("tipo_zona_estat") == F.lit(code)
        zone = F.when(condition, F.lit(value)) if zone is None else zone.when(condition, F.lit(value))

    return (
        dim
        .withColumnRenamed("periodo_id", "periodo_id_vigente")
        # Power of the CT, imputed as Calser does when the installed power is
        # 0 (37 % in EOSA, 48 % in Pitarch): administrative power first, then
        # the rated power of the transformer in the SCADA.
        .withColumn(
            "potencia_kva",
            F.when(instal > 0, instal).when(admin > 0, admin).when(nominal > 0, nominal),
        )
        .withColumn(
            "potencia_fuente",
            F.when(instal > 0, F.lit("CALSER_INSTAL"))
            .when(admin > 0, F.lit("CALSER_ADMIN"))
            .when(nominal > 0, F.lit("SCADA_NOMINAL")),
        )
        .withColumn(
            "potencia_imputada",
            F.coalesce(F.col("potencia_fuente") != F.lit("CALSER_INSTAL"), F.lit(True)),
        )
        # Denominator Calser uses for TIEPI: contracted MT power if the CT has
        # MT customers, administrative power otherwise. Only for validation.
        .withColumn(
            "potencia_tiepi_kva",
            F.when(F.col("potencia_contra_mt_kw") > 0, F.col("potencia_contra_mt_kw")).otherwise(admin),
        )
        .withColumn("tipo_zona_codigo", zone)
    )


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-dim_ct")
    dq = DQCollector(spark, "job_gold_dim_ct", args.run_id)

    elements = spark.table(require_table(spark, silver_table("d_elemento"))).select(
        "id", "parent_id", "tipo_elemento_id", "nombre", "distribuidora_id",
    ).localCheckpoint(eager=True)

    dim = build_dim(spark, params).localCheckpoint(eager=True)

    closure = hierarchy_closure(elements)
    scope = element_scope(elements, closure, params)

    # Anchor and group of every transformer of the study.
    trafo_scope = scope.select(
        F.col("elemento_id").alias("trafo_elemento_id"),
        "anchor_id",
        F.col("anchor_parent_id").alias("grupo_red_id"),
    )

    dim = dim.join(trafo_scope, "trafo_elemento_id", "left")

    groups = dim.where(F.col("grupo_red_id").isNotNull()).select("grupo_red_id").distinct()

    scope = add_groups(scope, closure, groups).localCheckpoint(eager=True)

    tags = spark.table(require_table(spark, silver_table("d_tag")))
    tag_map = build_tag_map(tags, scope, params).localCheckpoint(eager=True)

    upstream = build_upstream_map(spark, elements, params).join(
        dim.select("trafo_elemento_id", "distribuidora_id", "ct_id"),
        "trafo_elemento_id",
        "inner",
    ).localCheckpoint(eager=True)

    # Transformers per CT element and CTs of the study per network group.
    trafos_per_anchor = (
        scope.where(F.col("tipo_elemento_id") == F.lit(params["ct"]["tipo_trafo_ct"]))
        .groupBy("anchor_id")
        .agg(F.count(F.lit(1)).alias("n_trafos_ct"))
    )

    cts_per_group = dim.groupBy("grupo_red_id").agg(F.count(F.lit(1)).alias("n_ct_grupo"))

    coverage = tag_map.where(F.col("anchor_id").isNotNull()).groupBy("anchor_id").agg(
        F.count(F.lit(1)).alias("n_tags_local"),
        F.sum(F.col("tiene_serie").cast("long")).alias("n_tags_serie_local"),
        F.sum((F.col("familia_medida").isNotNull() & F.col("tiene_serie")).cast("long")).alias("n_tags_medida_local"),
        F.concat_ws(
            ",",
            F.array_sort(F.collect_set(F.when(F.col("tiene_serie"), F.col("familia_medida")))),
        ).alias("familias_medida_local"),
        F.sum(F.col("familia_evento").isNotNull().cast("long")).alias("n_tags_evento_local"),
    )

    dim = (
        dim.join(trafos_per_anchor, "anchor_id", "left")
        .join(cts_per_group, "grupo_red_id", "left")
        .join(coverage, "anchor_id", "left")
    )

    # Whole history: descriptive only. The features count the bays known at
    # each hour (aa_n_posiciones).
    dim = dim.join(
        upstream.groupBy("trafo_elemento_id").agg(
            F.countDistinct("posicion_id").alias("n_posiciones_aguas_arriba")
        ),
        "trafo_elemento_id",
        "left",
    )

    only_observable = params["ct"].get("solo_observables", True)

    exclusion = (
        F.when(~F.coalesce(F.col("tiene_telemetria"), F.lit(False)), F.lit("SIN_TRAFO_SCADA"))
        .when(F.lit(only_observable) & ~F.coalesce(F.col("observable"), F.lit(False)), F.lit("NO_OBSERVABLE"))
    )

    dim = (
        dim
        .withColumn("n_tags_local", F.coalesce("n_tags_local", F.lit(0)))
        .withColumn("n_tags_serie_local", F.coalesce("n_tags_serie_local", F.lit(0)))
        .withColumn("n_tags_medida_local", F.coalesce("n_tags_medida_local", F.lit(0)))
        .withColumn("n_tags_evento_local", F.coalesce("n_tags_evento_local", F.lit(0)))
        .withColumn("n_posiciones_aguas_arriba", F.coalesce("n_posiciones_aguas_arriba", F.lit(0)))
        .withColumn("tiene_medidas", F.col("n_tags_medida_local") > F.lit(0))
        .withColumn("motivo_exclusion", exclusion)
        .withColumn("en_estudio", F.col("motivo_exclusion").isNull())
        .withColumn("audit_loaded_at", F.current_timestamp())
        .localCheckpoint(eager=True)
    )

    write_table(dim, DIM_TABLE)
    write_table(tag_map, MAP_TAG_TABLE)
    write_table(upstream, MAP_UPSTREAM_TABLE)

    for row in (
        dim.groupBy("source_database")
        .agg(
            F.count(F.lit(1)).alias("cts"),
            F.sum(F.col("en_estudio").cast("long")).alias("en_estudio"),
            F.sum(F.col("tiene_medidas").cast("long")).alias("con_medidas"),
            F.sum((F.col("n_posiciones_aguas_arriba") > 0).cast("long")).alias("con_aguas_arriba"),
            F.sum(F.col("potencia_imputada").cast("long")).alias("potencia_imputada"),
        )
        .collect()
    ):
        ambito = row["source_database"]
        dq.add("dim_ct", "cts", row["cts"], ambito=ambito)
        dq.add("dim_ct", "cts_en_estudio", row["en_estudio"], row["cts"], minimo_pct=90.0, ambito=ambito)
        dq.add("dim_ct", "cts_con_medidas", row["con_medidas"], row["en_estudio"], ambito=ambito,
               detalle="CT with at least one measurement series in its CT element")
        dq.add("dim_ct", "cts_con_aguas_arriba", row["con_aguas_arriba"], row["en_estudio"], ambito=ambito)
        dq.add("dim_ct", "potencia_imputada", row["potencia_imputada"], row["cts"], ambito=ambito)

    for row in tag_map.groupBy("origen_mapeo").count().collect():
        dq.add("map_tag_ct", f"tags:{row['origen_mapeo']}", row["count"], tag_map.count())

    dq.flush()

    logger.info("Gold completed: %s, %s and %s", DIM_TABLE, MAP_TAG_TABLE, MAP_UPSTREAM_TABLE)


if __name__ == "__main__":
    main()
