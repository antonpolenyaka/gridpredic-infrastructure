"""
Silver dimension of the Calser municipalities (versioned by period).

MUNICIPIO_ID is the INE code. The zone type (MUNICIPIO_TIPO_ZONA_ESTAT) is
the one Calser uses to weight TIEPI/NIEPI and it is a useful feature, so it
is kept as it comes. Names are trimmed and the placeholder names that some
installations use for an unknown municipality are flagged.
"""

from pyspark.sql import DataFrame
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
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_municipio"
TARGET_TABLE = silver_table(ENTITY)

# Seen in the Calser databases of other distribuidoras ("(municipio nulo)").
PLACEHOLDER_NAME_PATTERN = r"(?i)^\(?\s*municipio\s+nulo\s*\)?$"


def transform(df_in: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("MUNICIPIO_PERIODO_ID").alias("periodo_id"),
        clean_str("MUNICIPIO_ID").alias("id"),
        clean_str("MUNICIPIO_PROVINCIA_ID").alias("provincia_id"),
        clean_str("MUNICIPIO_COMARCA_ID").alias("comarca_id"),
        clean_str("MUNICIPIO_NOMBRE").alias("nombre"),
        clean_str("MUNICIPIO_TIPO_ZONA_COMUN").alias("tipo_zona_comun"),
        clean_str("MUNICIPIO_TIPO_ZONA_ESTAT").alias("tipo_zona_estat"),
        F.col("MUNICIPIO_TS").try_cast("timestamp").alias("ts"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("periodo_id").isNull()),
    ])

    kept, duplicated = keep_first(
        kept,
        ["distribuidora_id", "periodo_id", "id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    out = (
        kept
        .withColumn(
            "nombre_invalido",
            F.col("nombre").isNull() | F.col("nombre").rlike(PLACEHOLDER_NAME_PATTERN),
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_municipio")
    dq = DQCollector(spark, "job_silver_d_municipio", args.run_id)

    df_in = read_calser_union(spark, "municipios")
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(
        ENTITY, total_in, out.count(), rejected, out, ["nombre_invalido"],
    )
    dq.add(
        ENTITY,
        "municipios_distintos",
        out.select("distribuidora_id", "id").distinct().count(),
    )
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
