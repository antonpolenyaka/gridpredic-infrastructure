"""
Hourly aggregates of the measurement series of TedisNet, by measurement
family and by the place they describe. It is the heavy step of Gold: it reads
f_tag_interval_value (2.150 million rows in Bronze) once, month by month, and
leaves a table small enough for the features job to run windows over it.

Two scopes (column ambito):

- LOCAL, clave_id = anchor_id: series of the tags of the CT element and
  everything below it (map_tag_ct). Telecontrolled CTs only.
- AGUAS_ARRIBA, clave_id = posicion_id: series of the bays of the switches
  that have cut each transformer (map_aguas_arriba), that is the tags of the
  switch, of its siblings and of the bay itself. In EOSA the feeder currents
  of the substations (AI.INTENSIDAD, 800 tags with series) live there, so
  this is the scope that gives measurements to the CTs without telemetry.

Per (ambito, clave_id, familia, hora) it keeps sums instead of means
(n_muestras, suma, suma2, max, min, n_rancio and the extreme means per tag),
so the features job can add several bays of the same transformer exactly.
Values are taken to a common scale (V -> kV, W -> kW) with the factor of
map_tag_ct.

hora is the end of the one hour bucket: the samples of [09:00, 10:00) are
stored at 10:00, the first hour at which they are all known. The time of a
sample is its grid instant, or the field time of its value when that is
later (see known_time). A second table,
actividad_scada_hora, counts every valid sample per distribuidora and hour:
an hour without samples means the history of the SCADA has a gap (the copy to
Historic* stops when RabbitMQ is down), not a quiet network.
"""

from datetime import timedelta

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold_common import (
    DQ_METRICS_TABLE,
    DQCollector,
    batches,
    bucket_hour,
    drop_table,
    get_spark,
    gold_table,
    known_time,
    load_params,
    logger,
    month_range_condition,
    month_start,
    next_month,
    optional_table,
    parse_args,
    require_table,
    silver_table,
    ts_lit,
    write_table,
)


AGG_TABLE = gold_table("agg_medida_hora")
ACTIVITY_TABLE = gold_table("actividad_scada_hora")

# Margin of Silver months read around each batch, so that a time zone
# correction of a few hours does not lose the samples of the edges. It is
# also the largest delay accepted between a grid instant and the field time
# of the value it holds.
EDGE = timedelta(hours=6)


def scope_members(tag_map: DataFrame, upstream) -> DataFrame:
    """
    (tag_id, ambito, clave_id, familia, factor_escala) for every measurement
    tag with series.
    """
    measures = tag_map.where(F.col("familia_medida").isNotNull() & F.col("tiene_serie"))

    local = measures.where(F.col("anchor_id").isNotNull()).select(
        "tag_id",
        F.lit("LOCAL").alias("ambito"),
        F.col("anchor_id").alias("clave_id"),
        F.col("familia_medida").alias("familia"),
        "factor_escala",
    )

    if upstream is None:
        return local

    positions = upstream.select(F.col("posicion_id").alias("clave_id")).where(
        F.col("clave_id").isNotNull()
    ).distinct()

    siblings = measures.select(
        "tag_id", F.col("posicion_id").alias("clave_id"), "familia_medida", "factor_escala",
    )
    itself = measures.select(
        "tag_id", F.col("elemento_id").alias("clave_id"), "familia_medida", "factor_escala",
    )

    remote = (
        siblings.unionByName(itself)
        .join(F.broadcast(positions), "clave_id")
        .select(
            "tag_id",
            F.lit("AGUAS_ARRIBA").alias("ambito"),
            "clave_id",
            F.col("familia_medida").alias("familia"),
            "factor_escala",
        )
        .distinct()
    )

    return local.unionByName(remote)


def aggregate(values: DataFrame, members: DataFrame) -> DataFrame:
    rancio = (
        F.col("valor_rancio").cast("int") if "valor_rancio" in values.columns else F.lit(0)
    )

    joined = (
        values.select(
            "tag_id",
            "hora",
            F.coalesce(F.col("valor_float"), F.col("valor_int").cast("double")).alias("_v"),
            rancio.alias("_rancio"),
        )
        .where(F.col("_v").isNotNull() & ~F.isnan(F.col("_v")))
        .join(F.broadcast(members), "tag_id")
        .withColumn("_v", F.col("_v") * F.col("factor_escala"))
    )

    per_tag = joined.groupBy("ambito", "clave_id", "familia", "tag_id", "hora").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum("_v").alias("s"),
        F.sum(F.col("_v") * F.col("_v")).alias("s2"),
        F.max("_v").alias("mx"),
        F.min("_v").alias("mn"),
        F.sum("_rancio").alias("nr"),
    )

    return per_tag.groupBy("ambito", "clave_id", "familia", "hora").agg(
        F.sum("n").alias("n_muestras"),
        F.count(F.lit(1)).alias("n_tags"),
        F.sum("s").alias("suma"),
        F.sum("s2").alias("suma2"),
        F.max("mx").alias("max"),
        F.min("mn").alias("min"),
        F.sum("nr").alias("n_rancio"),
        F.max(F.col("s") / F.col("n")).alias("media_tag_max"),
        F.min(F.col("s") / F.col("n")).alias("media_tag_min"),
    )


def add_args(parser):
    parser.add_argument(
        "--rehacer",
        action="store_true",
        help="Process every batch of the window, also the ones already completed",
    )


# The last metric written for a batch; its presence in dq_metrics means the
# batch was written in full (aggregates, activity and metrics).
COMPLETION_METRIC = "horas_con_muestras"


def completed_batches(spark, job: str) -> set:
    """
    Batches (ambito YYYY-MM of their first month) that a previous run of this
    job finished, read from dq_metrics. On a laptop the executor can be lost
    after an hour of I/O and the task is retried: skipping the batches already
    written keeps the retry from starting again at the first month.
    """
    if not spark.catalog.tableExists(DQ_METRICS_TABLE):
        return set()

    rows = (
        spark.table(DQ_METRICS_TABLE)
        .where(
            (F.col("job") == job)
            & (F.col("entidad") == "actividad_scada_hora")
            & (F.col("metrica") == COMPLETION_METRIC)
        )
        .select("ambito")
        .distinct()
        .collect()
    )

    return {row["ambito"] for row in rows if row["ambito"]}


def main():
    args = parse_args(add_args)
    params = load_params(args)
    spark = get_spark("job-gold-agg_medida_hora")
    dq = DQCollector(spark, "job_gold_agg_medida_hora", args.run_id)

    if args.rebuild:
        drop_table(spark, AGG_TABLE)
        drop_table(spark, ACTIVITY_TABLE)

    series = spark.table(require_table(spark, silver_table("f_tag_interval_value")))
    tag_map = spark.table(require_table(spark, gold_table("map_tag_ct")))
    upstream = optional_table(spark, gold_table("map_aguas_arriba"))

    # Cached, not local-checkpointed: a local checkpoint lives only in the
    # executor that made it and is gone when that executor is lost (heartbeat
    # timeout under heavy I/O); a cached table is rebuilt from Delta.
    members = scope_members(tag_map, upstream).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("Measurement tags by scope: %s", {
        row["ambito"]: row["count"] for row in members.groupBy("ambito").count().collect()
    })

    done = set() if (args.rehacer or args.rebuild) else completed_batches(spark, dq.job)

    if done:
        logger.info("%s batches already completed in previous runs, skipped", len(done))

    for lo, hi in batches(args, params, params["lotes"]["medidas_meses"]):
        if f"{lo:%Y-%m}" in done:
            logger.info("Batch %s - %s already completed, skipped", lo, hi)
            continue

        logger.info("Measurement aggregates %s - %s", lo, hi)

        # Samples of [lo - 1 h, hi - 1 h) fill the hours [lo, hi).
        first_month = month_start(lo - timedelta(hours=1) - EDGE)
        last_month = next_month(month_start(hi + EDGE))

        # A sample is known at its grid instant, or at the field time of the
        # value it holds when that is later (ts_origen_futuro: the copy ran
        # late or the RTU clock is ahead). A field time more than EDGE ahead
        # is a broken clock and the sample is left out.
        broken_clock = F.col("ts_origen") > F.col("ts") + F.expr(f"INTERVAL {int(EDGE.total_seconds())} SECONDS")

        values = (
            series
            .where(
                (F.col("fecha_mes") >= F.lit(first_month.isoformat()).cast("date"))
                & (F.col("fecha_mes") < F.lit(last_month.isoformat()).cast("date"))
            )
            .where(~F.coalesce(broken_clock, F.lit(False)))
            .withColumn("ts", known_time(series, params, arrival="ts_origen"))
            .withColumn("hora", bucket_hour("ts"))
            .where((F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi)))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        activity = (
            values.groupBy("distribuidora_id", "hora")
            .agg(
                F.count(F.lit(1)).alias("n_muestras_serie"),
                F.countDistinct("tag_id").alias("n_tags_serie"),
            )
            .withColumn("fecha_mes", F.trunc("hora", "month").cast("date"))
        )

        agg = (
            aggregate(values, members)
            .withColumn("fecha_mes", F.trunc("hora", "month").cast("date"))
            .withColumn("audit_loaded_at", F.current_timestamp())
        )

        condition = month_range_condition("fecha_mes", lo, hi)

        write_table(agg, AGG_TABLE, partition_by=["fecha_mes"], replace_where=condition)
        write_table(activity, ACTIVITY_TABLE, partition_by=["fecha_mes"], replace_where=condition)

        ambito = f"{lo:%Y-%m}"
        written = spark.table(AGG_TABLE).where(
            (F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi))
        )

        for row in written.groupBy("ambito").agg(
            F.count(F.lit(1)).alias("filas"),
            F.countDistinct("clave_id").alias("claves"),
            F.sum("n_muestras").alias("muestras"),
        ).collect():
            dq.add("agg_medida_hora", f"filas:{row['ambito']}", row["filas"], ambito=ambito,
                   detalle=f"{row['claves']} keys, {row['muestras']} samples")

        hours = int((hi - lo).total_seconds() // 3600)
        active = spark.table(ACTIVITY_TABLE).where(
            (F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi))
        ).select("hora").distinct().count()

        dq.add("actividad_scada_hora", "horas_con_muestras", active, hours, minimo_pct=95.0, ambito=ambito)
        dq.flush()

        values.unpersist()

    logger.info("Gold completed: %s and %s", AGG_TABLE, ACTIVITY_TABLE)


if __name__ == "__main__":
    main()
