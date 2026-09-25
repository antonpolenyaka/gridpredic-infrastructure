"""
Silver dimension of the Calser LV outputs (salidas) of every CT.

Integrates the salidas table of the three Calser databases. Completes the
first Silver example of the repository (job_silver_f_salidas.py) with the
rules of the layer: distribuidora of every row, clean texts, deduplication by
the Calser primary key and a check that the CT of the salida exists in the
same period.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    clean_str,
    get_spark,
    keep_first,
    logger,
    orphan_condition,
    parse_args,
    read_calser_union,
    require_table,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_salida"
TARGET_TABLE = silver_table(ENTITY)


def transform(df_in: DataFrame, cts: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("SALIDA_PERIODO_ID").alias("periodo_id"),
        clean_str("SALIDA_ID").alias("id"),
        clean_str("SALIDA_ABREVIATURA").alias("abreviatura"),
        clean_str("SALIDA_NOMBRE").alias("nombre"),
        clean_str("SALIDA_CT_ID").alias("ct_id"),
        clean_str("SALIDA_USUARIO_ALTA_ID").alias("usuario_alta_id"),
        F.col("SALIDA_TS").try_cast("timestamp").alias("ts"),
    )

    df = orphan_condition(
        df,
        cts.select("distribuidora_id", "periodo_id", F.col("id").alias("ct_id")),
        ["distribuidora_id", "periodo_id", "ct_id"],
        "_sin_ct",
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("periodo_id").isNull()),
        ("ORPHAN_FK", F.col("_sin_ct")),
    ])

    kept, duplicated = keep_first(
        kept.drop("_sin_ct"),
        ["distribuidora_id", "periodo_id", "id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    out = kept.withColumn("audit_loaded_at", F.current_timestamp())

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_salida")
    dq = DQCollector(spark, "job_silver_d_salida", args.run_id)

    df_in = read_calser_union(spark, "salidas")
    total_in = df_in.count()

    cts = spark.table(require_table(spark, silver_table("d_ct")))

    out, rejected = transform(df_in, cts)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected)
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
