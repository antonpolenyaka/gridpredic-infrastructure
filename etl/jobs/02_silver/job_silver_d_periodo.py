"""
Silver dimension of the Calser periods.

Every Calser table is versioned by a monthly period, and the period id is not
a clean key: there is a literal DEFAULT (the open period), and other Calser
installations mix YYYYMM with four digit years. Silver keeps the original id
for the joins and adds a normalised YYYYMM taken from the start date, plus
the end of the period, so any date can be placed in its period without
trusting the id.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    clean_str,
    get_spark,
    keep_first,
    logger,
    parse_args,
    read_calser_union,
    silver_table,
    split_rejects,
    to_date_col,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_periodo"
TARGET_TABLE = silver_table(ENTITY)


def transform(df_in: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("PERIODO_ID").alias("periodo_id"),
        clean_str("PERIODO_NOMBRE").alias("nombre"),
        to_date_col(df_in, "PERIODO_FECHA_INICIO").alias("fecha_inicio"),
        F.col("PERIODO_TS").try_cast("timestamp").alias("ts"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("periodo_id").isNull()),
    ])

    kept, duplicated = keep_first(
        kept,
        ["distribuidora_id", "periodo_id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    formato = (
        F.when(F.col("periodo_id") == F.lit("DEFAULT"), F.lit("DEFAULT"))
        .when(F.col("periodo_id").rlike(r"^\d{6}$"), F.lit("YYYYMM"))
        .when(F.col("periodo_id").rlike(r"^\d{4}$"), F.lit("YYYY"))
        .otherwise(F.lit("OTRO"))
    )

    # The next period starts where this one ends. DEFAULT is the open period
    # and has no successor.
    window = (
        Window.partitionBy("distribuidora_id")
        .orderBy(F.col("fecha_inicio").asc_nulls_last(), F.col("periodo_id"))
    )

    out = (
        kept
        .withColumn("formato_periodo", formato)
        .withColumn("es_default", F.col("periodo_id") == F.lit("DEFAULT"))
        .withColumn("periodo_norm", F.date_format("fecha_inicio", "yyyyMM"))
        .withColumn("fecha_fin_excl", F.lead("fecha_inicio").over(window))
        .withColumn(
            "id_incoherente",
            (F.col("formato_periodo") == F.lit("YYYYMM"))
            & (F.col("periodo_id") != F.col("periodo_norm")),
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_periodo")
    dq = DQCollector(spark, "job_silver_d_periodo", args.run_id)

    df_in = read_calser_union(spark, "periodos")
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(
        ENTITY, total_in, out.count(), rejected,
        out, ["es_default", "id_incoherente"],
    )
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
