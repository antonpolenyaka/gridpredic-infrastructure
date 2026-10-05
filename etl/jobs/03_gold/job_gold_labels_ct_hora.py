"""
Gold labels per (distribuidora_id, ct_id, hora). It is also the grid of the
dataset: one row for every CT of the study and every hour of the window, so
the hours without any event exist with label 0.

hora is the prediction instant. A label looks only forward from it:

- y_1_3h: an interruption of the principal variant starts in
  (hora + 1 h, hora + 3 h]. The target of the TFM.
- y_0_1h, y_0_3h, y_0_6h: other horizons, to compare.
- y_1_3h_local: y_1_3h counting only local interruptions (the incident
  affects a single CT). In EOSA and Pitarch 94 % of the interruptions are
  systemic; this label tells whether a model finds the "bad CT" and not only
  the "bad day".
- y_1_3h_amplia: y_1_3h with the amplia variant (interruptions without
  incident included).
- y_1_3h_scada: y_1_3h with the cuts of the SCADA (fact_cortes_scada), the
  label that does not depend on Calser.
- en_corte (en_corte_calser, en_corte_scada): the CT is already without
  supply at hora. Those rows are left out of training: there is nothing to
  predict when the cut is in progress.
- horas_hasta_proximo_evento: hours until the next start of the principal
  variant, null if there is none within a year (regression or survival
  variants).

An event becomes the set of exact hours it labels: for a horizon (a, b] the
event at time e labels every hour h with e - b <= h < e - a. Events are few,
so the positives are built by exploding the events and the grid only joins
them once.
"""

from datetime import timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    batches,
    drop_table,
    era_expr,
    get_spark,
    gold_table,
    hour_grid,
    hours_in,
    load_params,
    logger,
    month_range_condition,
    parse_args,
    require_table,
    ts_lit,
    write_table,
)


TARGET_TABLE = gold_table("labels_ct_hora")

LOOKAHEAD_DAYS = 365


def label_names(params: dict) -> list:
    return list(params["horizontes"]) + [
        "y_1_3h_local", "y_1_3h_amplia", "y_1_3h_scada", "en_corte_calser", "en_corte_scada",
    ]


def positives(events: DataFrame, a: int, b: int, name: str) -> DataFrame:
    """
    Hours labelled by the start of each event for the horizon (a, b] hours.
    """
    lo = F.col("inicio_ts") - F.expr(f"INTERVAL {b} HOURS")
    hi = F.col("inicio_ts") - F.expr(f"INTERVAL {a} HOURS")

    return events.select(
        "distribuidora_id",
        "ct_id",
        F.explode(hours_in(lo, hi)).alias("hora"),
        F.lit(name).alias("etiqueta"),
    )


def outage_hours(intervals: DataFrame, name: str, cap_hours: int) -> DataFrame:
    """
    Hours h with start <= h < end: the CT is without supply at the
    prediction instant. Very long records are capped (there are interruptions
    of up to 126 days in other Calser databases).
    """
    end = F.least(F.col("fin_ts"), F.col("inicio_ts") + F.expr(f"INTERVAL {cap_hours} HOURS"))

    return intervals.where(F.col("fin_ts").isNotNull()).select(
        "distribuidora_id",
        "ct_id",
        F.explode(hours_in(F.col("inicio_ts"), end)).alias("hora"),
        F.lit(name).alias("etiqueta"),
    )


def all_positive_rows(fact: DataFrame, scada: DataFrame, params: dict) -> DataFrame:
    principal = fact.where(F.col("variante") == F.lit("principal"))
    amplia = fact.where(F.col("variante") == F.lit("amplia"))
    todas = fact.where(F.col("variante") == F.lit("todas"))

    one_three = params["horizontes"]["y_1_3h"]

    parts = [
        positives(principal, a, b, name) for name, (a, b) in params["horizontes"].items()
    ]

    parts += [
        positives(principal.where(~F.coalesce(F.col("es_sistemico"), F.lit(True))),
                  one_three[0], one_three[1], "y_1_3h_local"),
        positives(amplia, one_three[0], one_three[1], "y_1_3h_amplia"),
        positives(scada.where(F.col("es_corte_etiqueta")), one_three[0], one_three[1], "y_1_3h_scada"),
        outage_hours(todas, "en_corte_calser", params["target"]["duracion_max_corte_h"]),
        outage_hours(
            scada.where(~F.col("emparejamiento_dudoso")),
            "en_corte_scada",
            params["target"]["duracion_max_corte_h"],
        ),
    ]

    out = parts[0]

    for part in parts[1:]:
        out = out.unionByName(part)

    return out.distinct()


def next_event_hours(grid: DataFrame, starts: DataFrame) -> DataFrame:
    """
    Hours from each grid hour to the next start strictly after it. Sorting
    in descending time, the last start seen before a grid row is the closest
    one in the future; at the same instant the grid row goes first, so an
    event exactly at hora does not count as "next".
    """
    rows = grid.select(
        "distribuidora_id", "ct_id",
        F.col("hora").alias("_t"),
        F.lit(0).alias("_es_evento"),
        F.lit(None).cast("timestamp").alias("_inicio"),
    ).unionByName(
        starts.select(
            "distribuidora_id", "ct_id",
            F.col("inicio_ts").alias("_t"),
            F.lit(1).alias("_es_evento"),
            F.col("inicio_ts").alias("_inicio"),
        )
    )

    ordered = (
        Window.partitionBy("distribuidora_id", "ct_id")
        .orderBy(F.col("_t").desc(), F.col("_es_evento").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        rows
        .withColumn("_siguiente", F.last("_inicio", ignorenulls=True).over(ordered))
        .where(F.col("_es_evento") == F.lit(0))
        .select(
            "distribuidora_id",
            "ct_id",
            F.col("_t").alias("hora"),
            (
                (F.unix_timestamp("_siguiente") - F.unix_timestamp("_t")) / F.lit(3600.0)
            ).alias("horas_hasta_proximo_evento"),
        )
    )


def build_batch(spark, cts: DataFrame, positive_rows: DataFrame, starts: DataFrame,
                lo, hi, params: dict) -> DataFrame:
    names = label_names(params)

    grid = hour_grid(spark, cts, lo, hi)

    in_batch = positive_rows.where((F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi)))

    wide = (
        in_batch.groupBy("distribuidora_id", "ct_id", "hora")
        .pivot("etiqueta", names)
        .agg(F.count(F.lit(1)))
    )

    out = grid.join(wide, ["distribuidora_id", "ct_id", "hora"], "left")

    for name in names:
        out = out.withColumn(name, (F.coalesce(F.col(name), F.lit(0)) > 0).cast("int"))

    upcoming = starts.where(
        (F.col("inicio_ts") > ts_lit(lo))
        & (F.col("inicio_ts") <= ts_lit(hi + timedelta(days=LOOKAHEAD_DAYS)))
    )

    out = out.join(next_event_hours(grid, upcoming), ["distribuidora_id", "ct_id", "hora"], "left")

    # The events read go up to a year after the end of the batch, so the
    # hours at its start could see almost two years ahead. Same horizon for
    # every hour, whatever the batch size.
    within_year = F.col("horas_hasta_proximo_evento") <= F.lit(LOOKAHEAD_DAYS * 24.0)

    return (
        out
        .withColumn("horas_hasta_proximo_evento", F.when(within_year, F.col("horas_hasta_proximo_evento")))
        .withColumn(
            "en_corte",
            ((F.col("en_corte_calser") + F.col("en_corte_scada")) > 0).cast("int"),
        )
        .withColumn("era", era_expr("hora", params))
        .withColumn("fecha_mes", F.trunc("hora", "month").cast("date"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-labels_ct_hora")
    dq = DQCollector(spark, "job_gold_labels_ct_hora", args.run_id)

    if args.rebuild:
        drop_table(spark, TARGET_TABLE)

    cts = (
        spark.table(require_table(spark, gold_table("dim_ct")))
        .where(F.col("en_estudio"))
        .select("distribuidora_id", "ct_id")
        .localCheckpoint(eager=True)
    )

    fact = spark.table(require_table(spark, gold_table("fact_interrupciones_mt")))
    scada = spark.table(require_table(spark, gold_table("fact_cortes_scada")))

    positive_rows = all_positive_rows(fact, scada, params).localCheckpoint(eager=True)

    starts = (
        fact.where(F.col("variante") == F.lit("principal"))
        .select("distribuidora_id", "ct_id", "inicio_ts")
        .localCheckpoint(eager=True)
    )

    for lo, hi in batches(args, params, params["lotes"]["labels_meses"]):
        logger.info("Labels %s - %s", lo, hi)

        out = build_batch(spark, cts, positive_rows, starts, lo, hi, params)

        write_table(
            out,
            TARGET_TABLE,
            partition_by=["fecha_mes"],
            replace_where=month_range_condition("fecha_mes", lo, hi),
        )

        written = spark.table(TARGET_TABLE).where(
            (F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi))
        )

        sums = written.agg(
            F.count(F.lit(1)).alias("filas"),
            *[F.sum(name).alias(name) for name in label_names(params) + ["en_corte"]],
        ).first()

        ambito = f"{lo:%Y-%m}..{hi:%Y-%m-%d}"
        dq.add("labels_ct_hora", "filas", sums["filas"], ambito=ambito)

        for name in label_names(params) + ["en_corte"]:
            dq.add("labels_ct_hora", f"positivos:{name}", sums[name], sums["filas"], ambito=ambito)

        dq.flush()

    logger.info("Gold completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
