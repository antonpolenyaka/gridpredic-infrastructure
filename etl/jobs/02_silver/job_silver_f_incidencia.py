"""
Silver fact of the Calser incidents (incidencias).

An incident groups interruptions under one cause. It carries the
classification (CL_IMPRE unplanned / CL_PROGR planned) and the cause factor
(FA_CLIEN is excluded from TIEPI), which is exactly what the target of the
model filters on. The operator edits incidents after creating them, so the
Bronze copy is the final classification.

The association interruption to incident is 100 % manual in Calser, and
26 - 28 % of the interruptions of EOSA and Pitarch have no incident. That is
handled on the interruption side (flag sin_incidencia), not here.

FECHA_ALTA can be weeks after the event: it is kept for auditing but it is
leakage for any model and must not become a feature.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    clean_str,
    combine_date_time,
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


ENTITY = "f_incidencia"
TARGET_TABLE = silver_table(ENTITY)


def transform(df_in: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("INCIDENCIA_PERIODO_ID").alias("periodo_id"),
        clean_str("INCIDENCIA_ID").alias("id"),
        clean_str("INCIDENCIA_REFERENCIA").alias("referencia"),
        clean_str("INCIDENCIA_DESC").alias("descripcion"),
        clean_str("INCIDENCIA_OBS").alias("observaciones"),
        combine_date_time(df_in, "INCIDENCIA_FECHA_INICIO", "INCIDENCIA_HORA_INICIO").alias("inicio_ts"),
        combine_date_time(df_in, "INCIDENCIA_FECHA_FIN", "INCIDENCIA_HORA_FIN").alias("fin_ts"),
        F.col("INCIDENCIA_DURACION").cast("long").alias("duracion_s"),
        clean_str("INCIDENCIA_FACTOR").alias("factor_id"),
        clean_str("INCIDENCIA_CLASIFICACION").alias("clasificacion_id"),
        clean_str("INCIDENCIA_TIPO_EQUIPO").alias("tipo_equipo_id"),
        clean_str("INCIDENCIA_RESOLUCION").alias("resolucion"),
        clean_str("INCIDENCIA_EG_ID").alias("estado_id"),
        clean_str("INCIDENCIA_OPERADOR_CC").alias("operador_cc_id"),
        clean_str("INCIDENCIA_OPERARIO_CAMPO").alias("operario_campo_id"),
        clean_str("INCIDENCIA_USUARIO_ID").alias("usuario_id"),
        to_date_col(df_in, "INCIDENCIA_FECHA_ALTA").alias("fecha_alta"),
        F.col("INCIDENCIA_TS").try_cast("timestamp").alias("ts"),
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
        .withColumn("es_imprevista", F.col("clasificacion_id") == F.lit("CL_IMPRE"))
        .withColumn("es_programada", F.col("clasificacion_id") == F.lit("CL_PROGR"))
        .withColumn("es_factor_cliente", F.col("factor_id") == F.lit("FA_CLIEN"))
        .withColumn("sin_inicio", F.col("inicio_ts").isNull())
        .withColumn(
            "intervalo_invalido",
            F.col("fin_ts").isNotNull()
            & F.col("inicio_ts").isNotNull()
            & (F.col("fin_ts") < F.col("inicio_ts")),
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_incidencia")
    dq = DQCollector(spark, "job_silver_f_incidencia", args.run_id)

    df_in = read_calser_union(spark, "incidencias")
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(
        ENTITY, total_in, out.count(), rejected, out,
        ["es_imprevista", "es_programada", "es_factor_cliente",
         "sin_inicio", "intervalo_invalido"],
    )
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
