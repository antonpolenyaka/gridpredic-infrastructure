"""
Silver fact of the regular series of TedisNet (TagIntervalValuesBig and
TagIntervalValues): measures sampled every StoreInterval seconds on a grid
anchored to the hour (sample and hold). Currents, voltages, powers, THD,
homopolar current: the physical features of the model.

Which column is the time of a sample matters here. The procedure that fills
the table (CopyTagValue2TagIntervalValue) inserts the current value of the tag
with UpdateTimestamp = the grid instant and SourceTimestamp = the original
field time of that value. If the value does not change, every sample keeps
the same SourceTimestamp. So:

- ts is UpdateTimestamp, the grid instant. One sample per tag and instant.
- ts_origen is SourceTimestamp and antiguedad_s = ts - ts_origen is how old
  the held value is. valor_rancio marks samples whose value is more than an
  hour old (frozen or stale value, a feature for Gold, not a reason to
  reject). ts_origen_futuro marks a field time later than the grid instant,
  which is a clock error of the RTU.

TagIntervalValuesBig has more than 2.150 million rows (220 GB), so this job
never works on the whole table at once:

- It processes one month at a time. Bronze is not partitioned, but Delta
  keeps min/max statistics of UpdateTimestamp per file and the rows were
  written in Id order, which is time order, so the filter of the month skips
  most files (the Bronze job writes this table in Id chunks with a bounded
  file size for that reason).
- Filters go before deduplication: they are cheap and reduce the volume
  before the window, which is the expensive step.
- The output is partitioned by month and every run overwrites only the
  months it processed (replaceWhere), so a month can be reprocessed without
  touching the rest and a repeated run gives the same result.
- A month counts as done when its last metric (tags_con_datos) is in
  dq_metrics. A new run skips the months already done, so when the job dies
  half way (on a laptop an executor can be lost after an hour of I/O) the
  retry continues from the first month that was not finished instead of
  starting again from 2021. --rehacer forces every month of the range.
- Rejected rows are not copied one by one (around 22 % of 2.150 million):
  f_tag_interval_value_rechazo_diario keeps the count per tag, day and
  reason. The number of hours without a good reading of a tag is itself a
  useful health feature.

Deduplication key: (tag, grid instant). On a sample and hold grid there can
only be one sample per tag and instant. This also removes the copies of the
same row coming from Historic and System (different Id spaces for the Big and
the small table, so the Id is not a valid key across them). Historic wins,
then the freshest value (latest SourceTimestamp), then the highest Id.

Usage:
  spark-submit job_silver_f_tag_interval_value.py --desde 2021-01 --hasta 2026-08
  Without --desde / --hasta the range goes from the first to the last month
  found in Bronze. Add --rehacer to redo the months already completed.
"""

from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQ_METRICS_TABLE,
    DQCollector,
    get_spark,
    keep_first,
    logger,
    parse_args,
    read_tedisnet_union,
    require_table,
    silver_table,
    split_rejects,
    write_table,
)
from job_silver_f_tag_value_change import (
    VALUE_COLUMNS,
    attach_tags,
    quality_rules,
    rename_values,
    value_flags,
)


ENTITY = "f_tag_interval_value"
TARGET_TABLE = silver_table(ENTITY)
REJECTED_DAILY_TABLE = silver_table(f"{ENTITY}_rechazo_diario")

SOURCES = [
    ("HistoricTagIntervalValuesBig", "Historic", 2),
    ("HistoricTagIntervalValues", "Historic", 2),
    ("SystemTagIntervalValuesBig", "System", 1),
    ("SystemTagIntervalValues", "System", 1),
]

FLAGS = [
    "es_estimado", "es_calculado", "es_copiado", "fuera_rango_egu",
    "ts_desde_update", "valor_rancio", "ts_origen_futuro",
]

# A held value older than this is marked valor_rancio. The grid is 300 or
# 600 s in the real database, so one hour is several missed refreshes.
STALE_VALUE_SECONDS = 3600

# Field time later than the grid instant by more than this is a clock error.
CLOCK_TOLERANCE_SECONDS = 60


def add_args(parser):
    parser.add_argument("--desde", help="First month, YYYY-MM")
    parser.add_argument("--hasta", help="Last month, YYYY-MM (inclusive)")
    parser.add_argument(
        "--rehacer",
        action="store_true",
        help="Process every month of the range, also the ones already completed",
    )


# The last metric written for a month; its presence in dq_metrics means the
# month was written in full (fact, daily rejects and metrics).
COMPLETION_METRIC = "tags_con_datos"


def completed_months(spark, job: str) -> set:
    """
    Months (YYYY-MM) that a previous run of this job finished, read from
    dq_metrics. Empty when the table does not exist yet.
    """
    if not spark.catalog.tableExists(DQ_METRICS_TABLE):
        return set()

    rows = (
        spark.table(DQ_METRICS_TABLE)
        .where(
            (F.col("job") == job)
            & (F.col("entidad") == ENTITY)
            & (F.col("metrica") == COMPLETION_METRIC)
        )
        .select("ambito")
        .distinct()
        .collect()
    )

    return {row["ambito"] for row in rows if row["ambito"]}


def month_start(value: str) -> date:
    year, month = value.split("-")
    return date(int(year), int(month), 1)


def next_month(value: date) -> date:
    return date(value.year + (value.month // 12), value.month % 12 + 1, 1)


def months_between(first: date, last: date) -> list:
    months = []
    current = first

    while current <= last:
        months.append(current)
        current = next_month(current)

    return months


def transform_month(df_month: DataFrame, tags: DataFrame):
    """
    Cleans the rows of one month. Returns (kept, rejected with _motivo).
    """
    df = attach_tags(rename_values(df_month, instant="update"), tags)

    kept, rejected = split_rejects(df, quality_rules("_sin_tag"))
    kept = kept.drop("_sin_tag")

    kept, duplicated = keep_first(
        kept,
        ["tag_id", "ts"],
        [
            F.col("_origen_rank").desc(),
            F.col("ts_origen").desc_nulls_last(),
            F.col("id").desc(),
        ],
        "DUPLICATE_NATURAL_KEY",
    )

    antiguedad = F.unix_timestamp("ts") - F.unix_timestamp("ts_origen")

    out = (
        value_flags(kept)
        .withColumn("antiguedad_s", antiguedad)
        .withColumn(
            "valor_rancio",
            F.coalesce(F.col("antiguedad_s") > F.lit(STALE_VALUE_SECONDS), F.lit(False)),
        )
        .withColumn(
            "ts_origen_futuro",
            F.coalesce(F.col("antiguedad_s") < F.lit(-CLOCK_TOLERANCE_SECONDS), F.lit(False)),
        )
        .withColumn("fecha", F.to_date("ts"))
        .withColumn("fecha_mes", F.trunc("ts", "month"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    rejected_all = rejected.drop("_sin_tag").unionByName(
        duplicated, allowMissingColumns=True
    )

    return out, rejected_all


def daily_rejects(rejected: DataFrame) -> DataFrame:
    return (
        rejected
        .withColumn("fecha", F.to_date("ts"))
        .withColumn("fecha_mes", F.trunc("ts", "month"))
        .groupBy("fecha_mes", "fecha", "tag_id", "distribuidora_id", "_motivo")
        .agg(
            F.count(F.lit(1)).alias("filas"),
            F.min("ts").alias("primer_ts"),
            F.max("ts").alias("ultimo_ts"),
        )
        .withColumnRenamed("_motivo", "motivo")
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def resolve_range(df_all: DataFrame, args) -> tuple:
    if args.desde and args.hasta:
        return month_start(args.desde), month_start(args.hasta)

    bounds = df_all.agg(
        F.min("UpdateTimestamp").alias("min_ts"),
        F.max("UpdateTimestamp").alias("max_ts"),
    ).first()

    if bounds["min_ts"] is None:
        return None, None

    first = month_start(args.desde) if args.desde else bounds["min_ts"].date().replace(day=1)
    last = month_start(args.hasta) if args.hasta else bounds["max_ts"].date().replace(day=1)

    return first, last


def main():
    args = parse_args(add_args)
    spark = get_spark("job-silver-f_tag_interval_value")
    dq = DQCollector(spark, "job_silver_f_tag_interval_value", args.run_id)

    df_all = read_tedisnet_union(spark, "", VALUE_COLUMNS, tables=SOURCES)

    # Cached, not local-checkpointed: a local checkpoint lives only in the
    # executor that made it, and when that executor is lost (heartbeat timeout
    # under heavy I/O) every later month fails with
    # CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND. A cached table is rebuilt from Delta.
    tags = spark.table(require_table(spark, silver_table("d_tag"))).persist(StorageLevel.MEMORY_AND_DISK)

    first, last = resolve_range(df_all, args)

    if first is None:
        logger.info("No interval values in Bronze, nothing to do")
        return

    # Rows without UpdateTimestamp (the grid instant) never fall in a month
    # window. Delta keeps null counts per file, so this count is cheap.
    no_grid_ts = df_all.where(F.col("UpdateTimestamp").isNull()).count()
    dq.add(
        ENTITY,
        "sin_update_timestamp",
        no_grid_ts,
        detalle="rows out of every month window, not processed",
    )

    done = set() if args.rehacer else completed_months(spark, dq.job)

    if done:
        logger.info("%s months already completed in previous runs, skipped", len(done))

    for month in months_between(first, last):
        end = next_month(month)
        month_label = month.strftime("%Y-%m")

        if month_label in done:
            logger.info("Month %s already completed, skipped", month_label)
            continue

        logger.info("Processing month %s", month_label)

        df_month = df_all.where(
            (F.col("UpdateTimestamp") >= F.lit(month.isoformat()).cast("timestamp"))
            & (F.col("UpdateTimestamp") < F.lit(end.isoformat()).cast("timestamp"))
        ).persist(StorageLevel.MEMORY_AND_DISK)

        total_in = df_month.count()

        replace_where = f"fecha_mes = DATE'{month.isoformat()}'"

        if total_in == 0:
            logger.info("Month %s is empty", month_label)
            df_month.unpersist()
            continue

        out, rejected = transform_month(df_month, tags)

        out = out.persist(StorageLevel.MEMORY_AND_DISK)

        write_table(out, TARGET_TABLE, partition_by=["fecha_mes"], replace_where=replace_where)

        daily = daily_rejects(rejected).persist(StorageLevel.MEMORY_AND_DISK)

        write_table(daily, REJECTED_DAILY_TABLE, partition_by=["fecha_mes"], replace_where=replace_where)

        kept_count = out.count()

        rejected_by_reason = daily.groupBy(F.col("motivo").alias("_motivo")).agg(
            F.sum("filas").alias("count")
        )

        dq.add(ENTITY, "filas_entrada", total_in, ambito=month_label)
        dq.add(ENTITY, "filas_salida", kept_count, total_in, ambito=month_label)

        rejected_total = 0

        for row in rejected_by_reason.collect():
            rejected_total += row["count"]
            dq.add(ENTITY, f"rechazo:{row['_motivo']}", row["count"], total_in, ambito=month_label)

        dq.add(ENTITY, "rechazo_total", rejected_total, total_in, umbral_pct=30.0, ambito=month_label)

        sums = out.agg(*[F.sum(F.col(flag).cast("long")).alias(flag) for flag in FLAGS]).first()

        for flag in FLAGS:
            dq.add(ENTITY, f"flag:{flag}", sums[flag] or 0, kept_count, ambito=month_label)

        dq.add(
            ENTITY,
            "tags_con_datos",
            out.select("tag_id").distinct().count(),
            ambito=month_label,
        )

        dq.flush()

        daily.unpersist()
        out.unpersist()
        df_month.unpersist()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
