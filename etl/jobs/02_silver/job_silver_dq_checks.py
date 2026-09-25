"""
Final checks of the Silver layer. Runs after every entity job and decides if
the batch can move on to Gold.

1. Referential integrity between the clean tables. Each job already rejects
   its own orphans; this check catches what a later job could have left
   hanging (for example a CT rejected after its interruptions were built).
2. Coverage of the Calser to TedisNet key on the CTs that carry the label
   (unplanned interruptions since 2021; 97,8 % in the verified analysis).
3. Time alignment between Calser and TedisNet. Calser records local time
   and the SCADA may store UTC. If the offset between an interruption that
   Calser imported from the SCADA and the Off event of the same transformer
   is a constant hour or two, the whole 1 - 3 h prediction window would be
   shifted. Silver does not correct it on its own: it measures it and asks
   for review.
4. The run decision: every metric of this run with estado REVISAR is listed
   and, with --fail-on-review, the job fails so the DAG stops before Gold.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    DQ_METRICS_TABLE,
    get_spark,
    logger,
    parse_args,
    silver_table,
)


JOB = "job_silver_dq_checks"

# Only the interruptions Calser imported from the SCADA can be matched with
# a TedisNet event (integration live since November 2025).
ALIGNMENT_FROM = "2025-11-01"
ALIGNMENT_WINDOW_MINUTES = 180
ALIGNED_TOLERANCE_MINUTES = 5

TARGET_FROM = "2021-01-01"

INTEGRITY_CHECKS = [
    # (child, child keys, parent, parent keys, description)
    ("f_interrupcion", ["distribuidora_id", "periodo_id", "elemento_id"],
     "d_ct", ["distribuidora_id", "periodo_id", "id"], "interrupcion -> ct"),
    ("d_ct", ["distribuidora_id", "periodo_id", "municipio_id"],
     "d_municipio", ["distribuidora_id", "periodo_id", "id"], "ct -> municipio"),
    ("d_salida", ["distribuidora_id", "periodo_id", "ct_id"],
     "d_ct", ["distribuidora_id", "periodo_id", "id"], "salida -> ct"),
    ("f_corte_elemento", ["evento_id"],
     "f_corte_evento", ["evento_id"], "corte_elemento -> corte_evento"),
    ("f_corte_elemento", ["elemento_id"],
     "d_elemento", ["id"], "corte_elemento -> elemento"),
    ("f_tag_value_change", ["tag_id"],
     "d_tag", ["id"], "tag_value_change -> tag"),
    ("d_tag", ["elemento_id"],
     "d_elemento", ["id"], "tag -> elemento"),
    ("f_evento", ["tag_value_change_id"],
     "f_tag_value_change", ["id"], "evento -> tag_value_change"),
]


def add_args(parser):
    parser.add_argument(
        "--fail-on-review",
        action="store_true",
        help="Fail the job if any metric of the run is marked REVISAR",
    )


def exists(spark, entity: str) -> bool:
    return spark.catalog.tableExists(silver_table(entity))


def check_integrity(spark, dq: DQCollector) -> None:
    for child, child_keys, parent, parent_keys, label in INTEGRITY_CHECKS:
        if not (exists(spark, child) and exists(spark, parent)):
            dq.add("integridad", f"no_evaluado:{label}", None, detalle="table missing")
            continue

        child_df = spark.table(silver_table(child)).select(
            *[F.col(key).alias(f"k{i}") for i, key in enumerate(child_keys)]
        )

        # Nullable foreign keys (for example a tag without element) are not
        # orphans: they are handled by the flags of each entity.
        for i in range(len(child_keys)):
            child_df = child_df.where(F.col(f"k{i}").isNotNull())

        parent_df = spark.table(silver_table(parent)).select(
            *[F.col(key).alias(f"k{i}") for i, key in enumerate(parent_keys)]
        ).distinct()

        total = child_df.count()
        orphans = child_df.join(parent_df, [f"k{i}" for i in range(len(child_keys))], "left_anti").count()

        dq.add("integridad", f"huerfanos:{label}", orphans, total, umbral_pct=0.0)


def check_target_coverage(spark, dq: DQCollector) -> None:
    if not (exists(spark, "f_interrupcion") and exists(spark, "d_ct_scada")):
        return

    target_cts = (
        spark.table(silver_table("f_interrupcion"))
        .where(F.col("inicio_ts") >= F.lit(TARGET_FROM).cast("timestamp"))
        .where(F.col("nivel_afectacion") == F.lit("CT"))
        .select("distribuidora_id", F.col("elemento_id").alias("ct_id"))
        .distinct()
    )

    joined = target_cts.join(
        spark.table(silver_table("d_ct_scada")).select(
            "distribuidora_id", "ct_id", "tiene_telemetria", "source_database"
        ),
        ["distribuidora_id", "ct_id"],
        "left",
    )

    for row in (
        joined.groupBy("distribuidora_id", "source_database")
        .agg(
            F.count(F.lit(1)).alias("cts"),
            F.sum(F.coalesce(F.col("tiene_telemetria"), F.lit(False)).cast("long")).alias("con_trafo"),
        )
        .collect()
    ):
        dq.add(
            "cobertura",
            "cts_con_interrupcion_sin_trafo_scada",
            row["cts"] - (row["con_trafo"] or 0),
            row["cts"],
            umbral_pct=5.0,
            ambito=row["source_database"] or str(row["distribuidora_id"]),
            detalle=f"CTs with interruptions since {TARGET_FROM}",
        )


def time_alignment(interrupciones: DataFrame, ct_map: DataFrame, cortes: DataFrame) -> DataFrame:
    """
    For every SCADA imported interruption of a CT, the nearest Off event of
    the same transformer within the window. Returns the offset in minutes
    (TedisNet minus Calser).
    """
    calser = (
        interrupciones
        .where(F.col("es_origen_scada"))
        .where(F.col("nivel_afectacion") == F.lit("CT"))
        .where(F.col("inicio_ts") >= F.lit(ALIGNMENT_FROM).cast("timestamp"))
        .select("distribuidora_id", F.col("elemento_id").alias("ct_id"), "id", "periodo_id", "inicio_ts")
        .join(
            ct_map.where(F.col("tiene_telemetria")).select("distribuidora_id", "ct_id", "elemento_id"),
            ["distribuidora_id", "ct_id"],
        )
    )

    offs = cortes.where(F.col("estado") == F.lit("OFF")).select(
        "elemento_id", F.col("ts").alias("ts_scada")
    )

    window_seconds = ALIGNMENT_WINDOW_MINUTES * 60

    pairs = (
        calser.join(offs, "elemento_id")
        .withColumn(
            "offset_s",
            F.unix_timestamp("ts_scada") - F.unix_timestamp("inicio_ts"),
        )
        .where(F.abs(F.col("offset_s")) <= F.lit(window_seconds))
    )

    return (
        pairs
        .groupBy("distribuidora_id", "periodo_id", "id")
        .agg(F.min_by("offset_s", F.abs(F.col("offset_s"))).alias("offset_s"))
        .withColumn("offset_min", F.col("offset_s") / F.lit(60.0))
    )


def check_time_alignment(spark, dq: DQCollector) -> None:
    needed = ["f_interrupcion", "d_ct_scada", "f_corte_elemento"]

    if not all(exists(spark, entity) for entity in needed):
        return

    aligned = time_alignment(
        spark.table(silver_table("f_interrupcion")),
        spark.table(silver_table("d_ct_scada")),
        spark.table(silver_table("f_corte_elemento")),
    ).localCheckpoint(eager=True)

    total = aligned.count()

    if total == 0:
        dq.add("alineacion_temporal", "pares_calser_scada", 0,
               estado="REVISAR", detalle="no SCADA interruption could be matched")
        return

    stats = aligned.agg(
        F.percentile_approx("offset_min", 0.5).alias("mediana"),
        F.sum((F.abs(F.col("offset_min")) <= F.lit(ALIGNED_TOLERANCE_MINUTES)).cast("long")).alias("alineados"),
    ).first()

    mode_row = (
        aligned.withColumn("hora", F.round(F.col("offset_min") / F.lit(60.0)).cast("int"))
        .groupBy("hora").count()
        .orderBy(F.col("count").desc())
        .first()
    )

    dq.add("alineacion_temporal", "pares_calser_scada", total)
    dq.add(
        "alineacion_temporal",
        "pares_alineados",
        stats["alineados"],
        total,
        detalle=f"|offset| <= {ALIGNED_TOLERANCE_MINUTES} min",
    )
    dq.add(
        "alineacion_temporal",
        "desfase_mediana_min",
        int(round(stats["mediana"])),
        estado="REVISAR" if abs(stats["mediana"]) >= 30 else "OK",
        detalle="TedisNet minus Calser; a constant 60 or 120 means local time vs UTC",
    )
    dq.add(
        "alineacion_temporal",
        "desfase_moda_horas",
        mode_row["hora"],
        estado="REVISAR" if mode_row["hora"] != 0 else "OK",
    )


def review_decision(spark, run_id: str) -> list:
    if not spark.catalog.tableExists(DQ_METRICS_TABLE):
        return []

    return (
        spark.table(DQ_METRICS_TABLE)
        .where((F.col("run_id") == F.lit(run_id)) & (F.col("estado") == F.lit("REVISAR")))
        .select("job", "entidad", "ambito", "metrica", "valor", "total", "pct", "umbral_pct", "detalle")
        .collect()
    )


def main():
    args = parse_args(add_args)
    spark = get_spark("job-silver-dq_checks")
    dq = DQCollector(spark, JOB, args.run_id)

    check_integrity(spark, dq)
    check_target_coverage(spark, dq)
    check_time_alignment(spark, dq)

    dq.flush()

    review = review_decision(spark, args.run_id)

    for row in review:
        logger.warning(
            "REVISAR %s %s %s %s: %s of %s (%s %% > %s %%) %s",
            row["job"], row["entidad"], row["ambito"] or "", row["metrica"],
            row["valor"], row["total"], row["pct"], row["umbral_pct"],
            row["detalle"] or "",
        )

    if review and args.fail_on_review:
        raise RuntimeError(
            f"{len(review)} Silver metrics need review in run {args.run_id}, "
            f"see {DQ_METRICS_TABLE}. Gold is not loaded."
        )

    logger.info(
        "Silver checks completed: %s metrics to review",
        len(review),
    )


if __name__ == "__main__":
    main()
