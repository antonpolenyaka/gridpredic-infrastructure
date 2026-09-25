"""
Silver catalog of the Calser generic types (tipo_generico).

tipo_generico holds the whole business vocabulary of Calser in a single
table: classification (CL_*), cause factors (FA_*), event types (TI_*) and
zone types (TZ_*). The column familia keeps the prefix, because
INT_TIPO_EVENTO_ID mixes CL_* and TI_* values and Gold needs to tell them
apart.

The job also checks that the ids the target of the model depends on still
exist in every distribuidora. The vocabulary is closed from the source code,
not from the data, so a missing id is a warning to look at, not an error of
the data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    CALSER_REQUIRED_VOCABULARY,
    DQCollector,
    clean_str,
    get_spark,
    keep_first,
    logger,
    parse_args,
    read_calser_union,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_tipo_generico"
TARGET_TABLE = silver_table(ENTITY)


def transform(df_in: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("TG_ID").alias("id"),
        clean_str("TG_TIPO").alias("tipo"),
        clean_str("TG_DESC").alias("descripcion"),
        F.col("TG_TS").try_cast("timestamp").alias("ts"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull()),
    ])

    kept, duplicated = keep_first(
        kept,
        ["distribuidora_id", "id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    out = (
        kept
        .withColumn("familia", F.regexp_extract(F.col("id"), r"^([A-Z]+)_", 1))
        .withColumn(
            "familia",
            F.when(F.col("familia") == F.lit(""), F.lit(None).cast("string"))
            .otherwise(F.col("familia")),
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def missing_vocabulary(out: DataFrame) -> list:
    """
    Returns (source_database, id) pairs of the required vocabulary that are
    not present in the catalog of a distribuidora.
    """
    spark = out.sparkSession

    required = spark.createDataFrame(
        [(value,) for value in CALSER_REQUIRED_VOCABULARY],
        "id string",
    )

    expected = out.select("distribuidora_id", "source_database").distinct().crossJoin(required)

    return [
        (row["source_database"], row["id"])
        for row in expected.join(
            out.select("distribuidora_id", "id"),
            ["distribuidora_id", "id"],
            "left_anti",
        ).collect()
    ]


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_tipo_generico")
    dq = DQCollector(spark, "job_silver_d_tipo_generico", args.run_id)

    df_in = read_calser_union(spark, "tipo_generico")
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected)

    missing = missing_vocabulary(out)

    for source_database, value in missing:
        dq.add(
            ENTITY,
            f"vocabulario_ausente:{value}",
            1,
            ambito=source_database,
            estado="REVISAR",
            detalle="id required by the target of the model",
        )

    if not missing:
        dq.add(ENTITY, "vocabulario_requerido", len(CALSER_REQUIRED_VOCABULARY), estado="OK")

    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
