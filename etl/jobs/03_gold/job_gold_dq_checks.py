"""
Final checks of the Gold layer, run after the dataset is built.

1. Time alignment after the correction of the configuration. Pairs every
   principal Calser interruption of era 3 (Calser imports from the SCADA)
   with the nearest Off of the same CT in fact_cortes_scada. The median
   offset has to be close to zero: a constant hour or two would shift the
   whole 1 - 3 h window and the features would see the cut they are supposed
   to anticipate. It also says which share of the Calser events the SCADA
   saw (concordance of the two labels).
2. Arrival of the SCADA changes: delay between the field time and the time
   the server stored them. A median of about an hour or two means a device
   or the server in another time zone; the share above
   tiempo.retraso_max_llegada_h is what the event features leave out.
3. Censoring of the label at the end of the window: Calser loads the
   interruptions days or weeks after they happen, so the last days before
   the backup lack positives. The p95 of the delay of era 3 says how far
   before the last load ventana.hasta can be.
4. Coverage: CTs of the study with measurements and with upstream bays, and
   rows with an active SCADA history.
5. Validity of the CTs: hours of the grid in which the CT was not listed in
   Calser (ct_vigente = 0) and, above all, positives of the main label in
   those hours. An interruption of a CT in an hour where the CT did not
   exist means that the validity read from the periods is wrong for it.
6. Labels: rate of positives per split (zero or above 1 % means the label is
   broken, not that the problem got easy).
7. Leakage by construction: no column of a forbidden list can be a feature,
   and no label can be in feature_metadata.
8. Features that are always null in train.
9. The decision: every metric of this run with estado REVISAR is listed and,
   with --fail-on-review, the job fails.
"""

import json
from datetime import timedelta

from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    DQ_METRICS_TABLE,
    get_spark,
    gold_table,
    load_params,
    logger,
    optional_table,
    parse_args,
    silver_table,
    to_datetime,
    ts_lit,
)


JOB = "job_gold_dq_checks"

ALIGNMENT_WINDOW_MINUTES = 180
ALIGNED_TOLERANCE_MINUTES = 30

FORBIDDEN = {"fecha_alta", "ts", "conocido_ts", "conocido_fin_ts", "inicio_ts", "fin_ts",
             "inicio_conocido_ts", "fin_conocido_ts", "primer_conocido_ts", "en_corte",
             "en_corte_calser", "en_corte_scada", "ct_vigente", "horas_hasta_proximo_evento",
             "n_posiciones_aguas_arriba"}

# A median delay of arrival this large is a clock or time zone difference.
ARRIVAL_MEDIAN_REVIEW_S = 1800

DEFAULT_MAX_DELAY_H = 24


def add_args(parser):
    parser.add_argument(
        "--fail-on-review",
        action="store_true",
        help="Fail the job if any metric of the run is marked REVISAR",
    )


def era3_start(params: dict) -> str:
    previous = [era["hasta"] for era in params["eras"] if era["hasta"] is not None]
    return max(previous) if previous else "1900-01-01"


def check_alignment(spark, dq: DQCollector, params: dict) -> None:
    fact = optional_table(spark, gold_table("fact_interrupciones_mt"))
    cuts = optional_table(spark, gold_table("fact_cortes_scada"))

    if fact is None or cuts is None:
        return

    calser = (
        fact.where(F.col("variante") == F.lit("principal"))
        .where(F.col("en_estudio"))
        .where(F.col("inicio_ts") >= ts_lit(era3_start(params)))
        .select("distribuidora_id", "ct_id", "evento_id", "inicio_ts")
    )

    total = calser.count()

    if total == 0:
        dq.add("alineacion", "eventos_era3", 0, detalle="no principal event in era 3 to compare")
        return

    window = ALIGNMENT_WINDOW_MINUTES * 60

    pairs = (
        calser.join(
            cuts.select("distribuidora_id", "ct_id", F.col("inicio_ts").alias("ts_scada")),
            ["distribuidora_id", "ct_id"],
        )
        .withColumn("offset_s", F.unix_timestamp("ts_scada") - F.unix_timestamp("inicio_ts"))
        .where(F.abs(F.col("offset_s")) <= F.lit(window))
        .groupBy("evento_id")
        .agg(F.min_by("offset_s", F.abs(F.col("offset_s"))).alias("offset_s"))
        .withColumn("offset_min", F.col("offset_s") / F.lit(60.0))
    )

    matched = pairs.count()

    dq.add("alineacion", "eventos_era3_con_corte_scada", matched, total, minimo_pct=80.0,
           detalle=f"Off of the same CT within {ALIGNMENT_WINDOW_MINUTES} min")

    if matched == 0:
        return

    stats = pairs.agg(
        F.percentile_approx("offset_min", 0.5).alias("mediana"),
        F.sum((F.abs(F.col("offset_min")) <= F.lit(ALIGNED_TOLERANCE_MINUTES)).cast("long")).alias("alineados"),
    ).first()

    dq.add("alineacion", "eventos_alineados", stats["alineados"], matched,
           detalle=f"|offset| <= {ALIGNED_TOLERANCE_MINUTES} min")
    dq.add(
        "alineacion",
        "desfase_mediana_min",
        int(round(stats["mediana"])),
        estado="REVISAR" if abs(stats["mediana"]) >= ALIGNED_TOLERANCE_MINUTES else "OK",
        detalle="SCADA minus Calser after tiempo.* of config_gold.json; 60 or 120 means local time vs UTC",
    )


def check_arrival(spark, dq: DQCollector, params: dict) -> None:
    changes = optional_table(spark, silver_table("f_tag_value_change"))

    if changes is None or "ts_actualizacion" not in changes.columns:
        return

    max_delay_h = params["tiempo"].get("retraso_max_llegada_h", DEFAULT_MAX_DELAY_H)
    delay = F.unix_timestamp("ts_actualizacion") - F.unix_timestamp("ts")

    rows = (
        changes.where(F.col("ts_actualizacion").isNotNull())
        .withColumn("_delay", delay)
        .groupBy("distribuidora_id")
        .agg(
            F.count(F.lit(1)).alias("cambios"),
            F.percentile_approx("_delay", 0.5).alias("mediana"),
            F.percentile_approx("_delay", 0.99).alias("p99"),
            F.sum((F.col("_delay") > F.lit(3600)).cast("long")).alias("tardios_1h"),
            F.sum((F.col("_delay") > F.lit(int(max_delay_h * 3600))).cast("long")).alias("descartados"),
            F.sum((F.col("_delay") < F.lit(-60)).cast("long")).alias("adelantados"),
        )
        .collect()
    )

    for row in rows:
        ambito = str(row["distribuidora_id"])
        dq.add("llegada", "retraso_mediana_s", row["mediana"], ambito=ambito,
               estado="REVISAR" if abs(row["mediana"] or 0) >= ARRIVAL_MEDIAN_REVIEW_S else "OK",
               detalle=f"UpdateTimestamp - SourceTimestamp of {row['cambios']} changes, p99 {row['p99']} s; "
                       "about 3600 or 7200 means a device or the server in another time zone")
        dq.add("llegada", "cambios_llegada_tardia_1h", row["tardios_1h"], row["cambios"], ambito=ambito)
        dq.add("llegada", "cambios_descartados_por_retraso", row["descartados"], row["cambios"],
               umbral_pct=5.0, ambito=ambito,
               detalle=f"arrived more than {max_delay_h} h late: left out of the event features")
        dq.add("llegada", "cambios_llegada_antes_de_origen", row["adelantados"], row["cambios"], ambito=ambito,
               detalle="stored more than 60 s before their field time: device clock ahead")


def check_label_censoring(spark, dq: DQCollector, params: dict) -> None:
    fact = optional_table(spark, gold_table("fact_interrupciones_mt"))

    if fact is None:
        return

    hasta = to_datetime(params["ventana"]["hasta"]).date()

    rows = (
        fact.where(F.col("variante") == F.lit("principal"))
        .where(F.col("inicio_ts") >= ts_lit(era3_start(params)))
        .where(F.col("fecha_alta").isNotNull())
        .withColumn("_alta", F.to_date("fecha_alta"))
        .withColumn("_dias", F.datediff(F.col("_alta"), F.to_date("inicio_ts")))
        .groupBy("distribuidora_id")
        .agg(
            F.count(F.lit(1)).alias("eventos"),
            F.percentile_approx("_dias", 0.95).alias("p95"),
            F.max("_alta").alias("ultima_alta"),
        )
        .collect()
    )

    for row in rows:
        ambito = str(row["distribuidora_id"])
        p95 = max(int(row["p95"] or 0), 0)
        limit = row["ultima_alta"] - timedelta(days=p95)
        uncovered = max((hasta - limit).days, 0)

        dq.add("censura_etiqueta", "retraso_carga_p95_dias", p95, ambito=ambito,
               detalle=f"{row['eventos']} principal events of era 3, last load {row['ultima_alta']}")
        dq.add("censura_etiqueta", "dias_ventana_sin_cargar", uncovered, ambito=ambito,
               estado="REVISAR" if uncovered else "OK",
               detalle=f"ventana.hasta {hasta}; with the last load minus the p95 the window should end "
                       f"by {limit}")


def check_coverage(spark, dq: DQCollector) -> None:
    dim = optional_table(spark, gold_table("dim_ct"))

    if dim is not None:
        row = dim.where(F.col("en_estudio")).agg(
            F.count(F.lit(1)).alias("cts"),
            F.sum(F.col("tiene_medidas").cast("long")).alias("medidas"),
            F.sum((F.col("n_posiciones_aguas_arriba") > 0).cast("long")).alias("aguas_arriba"),
        ).first()

        dq.add("cobertura", "cts_en_estudio_con_medidas", row["medidas"], row["cts"])
        dq.add("cobertura", "cts_en_estudio_con_aguas_arriba", row["aguas_arriba"], row["cts"], minimo_pct=50.0)

    features = optional_table(spark, gold_table("features_ct_hora"))

    if features is not None:
        row = features.agg(
            F.count(F.lit(1)).alias("filas"),
            F.sum("scada_activo").alias("activas"),
        ).first()

        dq.add("cobertura", "horas_scada_activo", row["activas"], row["filas"], minimo_pct=95.0)


def check_validity(spark, dq: DQCollector) -> None:
    labels = optional_table(spark, gold_table("labels_ct_hora"))

    if labels is None or "ct_vigente" not in labels.columns:
        return

    main_label = "y_1_3h" if "y_1_3h" in labels.columns else None

    row = labels.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum(F.lit(1) - F.col("ct_vigente")).alias("no_vigentes"),
        F.countDistinct(F.when(F.col("ct_vigente") == 0, F.concat_ws("-", "distribuidora_id", "ct_id"))).alias("cts"),
        (F.sum(main_label) if main_label else F.lit(None)).alias("positivos"),
        (F.sum(F.when(F.col("ct_vigente") == 0, F.col(main_label))) if main_label else F.lit(None)).alias(
            "positivos_no_vigentes"
        ),
    ).first()

    dq.add("vigencia", "horas_ct_no_vigente", row["no_vigentes"], row["filas"],
           detalle=f"{row['cts']} CTs with hours before they were listed in Calser or after they were removed")

    if main_label:
        dq.add("vigencia", f"positivos_en_horas_no_vigentes:{main_label}", row["positivos_no_vigentes"] or 0,
               row["positivos"], umbral_pct=0.0,
               detalle="an interruption of a CT in an hour where Calser did not list it: check the periods of that CT")


def check_dataset(spark, dq: DQCollector) -> None:
    versions = optional_table(spark, gold_table("dataset_versions"))

    if versions is None:
        return

    last = versions.orderBy(F.col("creado_ts").desc()).first()
    sizes = json.loads(last["filas_json"])

    for split, size in sizes.items():
        if split == "purga":
            dq.add("dataset", "filas_purga", size["filas"], ambito=split, detalle=last["dataset_version"])
            continue

        dq.add("dataset", f"tasa_positivos:{last['etiqueta_principal']}", size["positivos"], size["filas"],
               umbral_pct=1.0, ambito=split, detalle=last["dataset_version"])

        if not size["positivos"]:
            dq.add("dataset", "sin_positivos", 0, ambito=split, estado="REVISAR", detalle=last["dataset_version"])


def check_features(spark, dq: DQCollector) -> None:
    metadata = optional_table(spark, gold_table("feature_metadata"))

    if metadata is None:
        return

    names = {row["feature"] for row in metadata.select("feature").collect()}

    leaked = sorted(name for name in names if name in FORBIDDEN or name.startswith("y_"))

    dq.add("leakage", "columnas_prohibidas_como_feature", len(leaked),
           estado="REVISAR" if leaked else "OK", detalle=", ".join(leaked) or None)

    if "pct_nulos_train" in metadata.columns:
        empty = [row["feature"] for row in metadata.where(F.col("pct_nulos_train") >= 100.0).collect()]
        dq.add("features", "siempre_nulas_en_train", len(empty), len(names),
               detalle=", ".join(sorted(empty))[:900] or None)


def check_silver_alignment(spark, dq: DQCollector, params: dict) -> None:
    """
    What Silver measured before any correction, next to what the
    configuration corrects, so the report shows both.
    """
    metrics = optional_table(spark, silver_table("dq_metrics"))

    if metrics is None:
        return

    row = (
        metrics.where(F.col("metrica") == F.lit("desfase_mediana_min"))
        .orderBy(F.col("ts").desc())
        .first()
    )

    if row is None:
        return

    tiempo = params["tiempo"]
    dq.add("alineacion", "desfase_silver_sin_corregir_min", row["valor"],
           detalle=f"Silver run {row['run_id']}; config tedisnet_en_utc={tiempo.get('tedisnet_en_utc')}, "
                   f"desfase_tedisnet_min={tiempo.get('desfase_tedisnet_min')}")


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
    params = load_params(args)
    spark = get_spark("job-gold-dq_checks")
    dq = DQCollector(spark, JOB, args.run_id)

    check_silver_alignment(spark, dq, params)
    check_alignment(spark, dq, params)
    check_arrival(spark, dq, params)
    check_label_censoring(spark, dq, params)
    check_coverage(spark, dq)
    check_validity(spark, dq)
    check_dataset(spark, dq)
    check_features(spark, dq)

    dq.flush()

    review = review_decision(spark, args.run_id)

    for row in review:
        logger.warning(
            "REVISAR %s %s %s %s: %s of %s (%s %%, threshold %s %%) %s",
            row["job"], row["entidad"], row["ambito"] or "", row["metrica"],
            row["valor"], row["total"], row["pct"], row["umbral_pct"], row["detalle"] or "",
        )

    if review and args.fail_on_review:
        raise RuntimeError(
            f"{len(review)} Gold metrics need review in run {args.run_id}, see {DQ_METRICS_TABLE}"
        )

    logger.info("Gold checks completed: %s metrics to review", len(review))


if __name__ == "__main__":
    main()
