"""
Gold fact of the MT interruptions, the source of the label of the model.

One row per interruption event of a CT, in three variants (column variante):

- principal: the target of the TFM. Same filters Calser applies to compute
  TIEPI (ZonaCalculateIndexValues.cs, validated on the real databases):
  incident classified CL_IMPRE (unplanned), factor other than FA_CLIEN (cause
  on the customer side), INT_DURACION > 180 s, and CT level (no salida,
  acometida or abonado).
- amplia: principal plus the CT interruptions longer than 180 s without a
  valid incident. The association with an incident is manual in Calser and
  26 - 28 % of the interruptions of EOSA and Pitarch have none, so their cause
  is unknown. It measures how much the result depends on that gap.
- todas: every CT level interruption longer than 180 s, planned works
  included. It is never a label: it tells when a CT was already without
  supply (en_corte), so those hours can be left out of training.

Each variant merges its own overlapping records of the same CT. Silver only
grouped them (grupo_solape_id) over all the records; merging after filtering
avoids joining two target interruptions through a record that is not part of
the variant. The rule (envolvente: from the first start to the last end, or
mas_larga: keep the longest record) is target.fusion_solapes.

The same interruption recorded in two periods (duplicado_otro_periodo in
Silver) is kept once. INT_FECHA_ALTA is leakage as a feature, but it says
when the record existed in Calser, which is the only use Gold makes of it, so
that the history features only count what would have been known at each
hour. A record is known the day after its load, or at its start plus
target.retraso_alta_defecto_dias when it has no date. For a merged event:

- conocido_ts: its first record is known. From then on the event counts.
- conocido_fin_ts: every record is known and the event has ended. Only then
  is its final duration known (another record loaded later can extend it).

The duration is winsorized at the percentile target.percentil_winsor of the
events that start before dataset.train_hasta, so the cap does not depend on
the validation and test periods.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    era_expr,
    get_spark,
    gold_table,
    load_params,
    logger,
    parse_args,
    require_table,
    silver_table,
    ts_lit,
    window_bounds,
    write_table,
)


TARGET_TABLE = gold_table("fact_interrupciones_mt")

VARIANTS = ["principal", "amplia", "todas"]


def base_interruptions(interrupciones: DataFrame, incidencias: DataFrame, params: dict) -> DataFrame:
    target = params["target"]

    per_incident = (
        interrupciones.where(F.col("incidencia_id").isNotNull())
        .groupBy("distribuidora_id", "periodo_id", "incidencia_id")
        .agg(F.countDistinct("elemento_id").alias("n_cts_incidencia"))
    )

    incidents = incidencias.select(
        "distribuidora_id",
        "periodo_id",
        F.col("id").alias("incidencia_id"),
        "clasificacion_id",
        "factor_id",
        F.lit(True).alias("_incidencia_ok"),
    )

    df = (
        interrupciones
        .where(F.col("nivel_afectacion") == F.lit("CT"))
        .where(F.col("inicio_ts").isNotNull())
        .where(F.col("duracion_s") > F.lit(target["duracion_min_s"]))
        .where(~F.col("duracion_cero"))
        .where(~F.coalesce(F.col("es_incompleta"), F.lit(False)))
        .join(incidents, ["distribuidora_id", "periodo_id", "incidencia_id"], "left")
        .join(per_incident, ["distribuidora_id", "periodo_id", "incidencia_id"], "left")
        # Calser's own end when it exists, start plus duration otherwise.
        .withColumn(
            "fin_ts",
            F.coalesce(
                F.col("fin_ts"),
                F.timestamp_seconds(F.unix_timestamp("inicio_ts") + F.col("duracion_s")),
            ),
        )
    )

    # Same interruption in two periods: keep the copy with incident.
    ranked = Window.partitionBy(
        "distribuidora_id", "elemento_id", "inicio_ts", "duracion_s"
    ).orderBy(
        F.col("_incidencia_ok").isNull().asc(),
        F.col("periodo_id").desc(),
        F.col("id"),
    )

    df = df.withColumn("_rn", F.row_number().over(ranked)).where(F.col("_rn") == 1).drop("_rn")

    principal = (
        F.coalesce(F.col("_incidencia_ok"), F.lit(False))
        & (F.col("clasificacion_id") == F.lit(target["clasificacion"]))
        # NULL factor fails the filter, as in the SQL of Calser.
        & F.coalesce(F.col("factor_id") != F.lit(target["factor_excluido"]), F.lit(False))
    )

    without_incident = F.col("_incidencia_ok").isNull()

    delay = int(target["retraso_alta_defecto_dias"])

    record_known = F.greatest(
        F.col("inicio_ts"),
        F.coalesce(
            F.col("fecha_alta").cast("timestamp") + F.expr("INTERVAL 1 DAY"),
            F.col("inicio_ts") + F.expr(f"INTERVAL {delay} DAYS"),
        ),
    )

    return (
        df
        .withColumn("_conocido_registro", record_known)
        .withColumn("_principal", principal)
        .withColumn("_amplia", principal | without_incident)
        .withColumn("_todas", F.lit(True))
    )


def merge_overlaps(df: DataFrame, rule: str) -> DataFrame:
    """
    Groups the overlapping records of a CT (touching intervals do not
    overlap) and returns one row per group.
    """
    ordered = Window.partitionBy("distribuidora_id", "elemento_id").orderBy(
        "inicio_ts", "fin_ts", "periodo_id", "id"
    )

    grouped = (
        df
        .withColumn(
            "_max_fin_prev",
            F.max("fin_ts").over(ordered.rowsBetween(Window.unboundedPreceding, -1)),
        )
        .withColumn(
            "_nuevo",
            F.when(
                F.col("_max_fin_prev").isNull() | (F.col("inicio_ts") >= F.col("_max_fin_prev")),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "_grupo",
            F.sum("_nuevo").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "_dur",
            F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts"),
        )
    )

    if rule == "mas_larga":
        start = F.max_by("inicio_ts", "_dur")
        end = F.max_by("fin_ts", "_dur")
    elif rule == "envolvente":
        start = F.min("inicio_ts")
        end = F.max("fin_ts")
    else:
        raise ValueError(f"Unknown target.fusion_solapes rule: {rule}")

    return grouped.groupBy("distribuidora_id", "source_database", "elemento_id", "_grupo").agg(
        start.alias("inicio_ts"),
        end.alias("fin_ts"),
        F.count(F.lit(1)).alias("n_registros"),
        F.max("duracion_s").alias("duracion_registrada_max_s"),
        F.sort_array(F.collect_list("id")).alias("interrupcion_ids"),
        F.min_by("periodo_id", "inicio_ts").alias("periodo_id"),
        F.min_by("incidencia_id", "inicio_ts").alias("incidencia_id"),
        F.min_by("clasificacion_id", "inicio_ts").alias("clasificacion_id"),
        F.min_by("factor_id", "inicio_ts").alias("factor_id"),
        F.min_by("tipo_evento_id", "inicio_ts").alias("tipo_evento_id"),
        F.min_by("tipo_evento_familia", "inicio_ts").alias("tipo_evento_familia"),
        F.max("n_cts_incidencia").alias("n_cts_incidencia"),
        F.max(F.col("es_origen_scada").cast("int")).cast("boolean").alias("es_origen_scada"),
        F.min("fecha_alta").alias("fecha_alta"),
        F.min("_conocido_registro").alias("_conocido_min"),
        F.max("_conocido_registro").alias("_conocido_max"),
    )


def build(spark, params: dict) -> DataFrame:
    interrupciones = spark.table(require_table(spark, silver_table("f_interrupcion")))
    incidencias = spark.table(require_table(spark, silver_table("f_incidencia")))
    dim = spark.table(require_table(spark, gold_table("dim_ct")))

    target = params["target"]
    base = base_interruptions(interrupciones, incidencias, params).localCheckpoint(eager=True)

    window_start, window_end = window_bounds(params)

    parts = []

    for variant in VARIANTS:
        merged = merge_overlaps(base.where(F.col(f"_{variant}")), target["fusion_solapes"])
        parts.append(merged.withColumn("variante", F.lit(variant)))

    events = parts[0]

    for part in parts[1:]:
        events = events.unionByName(part)

    # Cap of the winsorization from the training period only. A distribuidora
    # without events before train_hasta is not winsorized.
    p99 = (
        events.where(F.col("inicio_ts") < ts_lit(params["dataset"]["train_hasta"]))
        .groupBy("variante", "distribuidora_id")
        .agg(
            F.percentile_approx(
                F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts"),
                float(target["percentil_winsor"]),
            ).alias("_p_winsor")
        )
    )

    return (
        events
        .join(p99, ["variante", "distribuidora_id"], "left")
        .withColumnRenamed("elemento_id", "ct_id")
        .withColumn("duracion_s", F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts"))
        .withColumn("duracion_winsor_s", F.least(F.col("duracion_s"), F.col("_p_winsor")))
        .drop("_p_winsor", "_grupo")
        .withColumn(
            "evento_id",
            F.concat_ws(
                "-",
                F.col("variante"),
                F.col("distribuidora_id").cast("string"),
                F.col("ct_id"),
                F.date_format("inicio_ts", "yyyyMMddHHmmss"),
            ),
        )
        .withColumn("es_sistemico", F.col("n_cts_incidencia") > F.lit(1))
        .withColumn("era", era_expr("inicio_ts", params))
        # When the event would have been known in Calser: its first record
        # (it counts from then on) and all its records once it has ended
        # (its duration is final from then on).
        .withColumn("conocido_ts", F.greatest(F.col("inicio_ts"), F.col("_conocido_min")))
        .withColumn("conocido_fin_ts", F.greatest(F.col("fin_ts"), F.col("_conocido_max")))
        .drop("_conocido_min", "_conocido_max")
        .withColumn(
            "en_ventana",
            (F.col("inicio_ts") >= ts_lit(window_start)) & (F.col("inicio_ts") < ts_lit(window_end)),
        )
        .join(
            dim.select("distribuidora_id", "ct_id", "en_estudio"),
            ["distribuidora_id", "ct_id"],
            "left",
        )
        .withColumn("en_estudio", F.coalesce(F.col("en_estudio"), F.lit(False)))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-fact_interrupciones_mt")
    dq = DQCollector(spark, "job_gold_fact_interrupciones_mt", args.run_id)

    out = build(spark, params).localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)

    for row in (
        out.groupBy("variante", "source_database")
        .agg(
            F.count(F.lit(1)).alias("eventos"),
            F.sum("n_registros").alias("registros"),
            F.sum((F.col("n_registros") > 1).cast("long")).alias("fusionados"),
            F.sum(F.col("en_ventana").cast("long")).alias("en_ventana"),
            F.sum((F.col("en_ventana") & F.col("en_estudio")).cast("long")).alias("en_ventana_estudio"),
            F.sum(F.coalesce(F.col("es_sistemico"), F.lit(False)).cast("long")).alias("sistemicos"),
            F.max("duracion_s").alias("duracion_max"),
        )
        .collect()
    ):
        ambito = f"{row['variante']}:{row['source_database']}"
        dq.add("fact_interrupciones_mt", "eventos", row["eventos"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "registros_origen", row["registros"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "eventos_fusionados", row["fusionados"], row["eventos"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "eventos_en_ventana", row["en_ventana"], row["eventos"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "eventos_en_ventana_ct_estudio", row["en_ventana_estudio"],
               row["en_ventana"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "eventos_sistemicos", row["sistemicos"], row["eventos"], ambito=ambito)
        dq.add("fact_interrupciones_mt", "duracion_max_s", row["duracion_max"], ambito=ambito,
               detalle="before winsorizing (duracion_winsor_s)")

    dq.flush()

    logger.info("Gold completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
