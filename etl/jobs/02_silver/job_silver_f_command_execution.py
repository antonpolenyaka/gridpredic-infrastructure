"""
Silver fact of the TedisNet telecontrol commands (manoeuvres).

A command execution is updated in the source while it progresses (created,
selected, executed, expired, cancelled), so the same Id arrives several times
with a different state. Only the final state is kept: the row with the
latest UpdateTimestamp, Historic before System.

The table exists here because the power cut processor always writes
IsCommand = false: the only way to know that a cut was a telecontrolled
manoeuvre, and not a fault, is to match the tag value change of the cut
with the one of a command.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    get_spark,
    keep_first,
    logger,
    parse_args,
    read_tedisnet_union,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "f_command_execution"
TARGET_TABLE = silver_table(ENTITY)

COLUMNS = {
    "Id": "bigint",
    "CommandId": "bigint",
    "ValueEnumValueId": "bigint",
    "TargetCommandId": "bigint",
    "StateEnumValueId": "bigint",
    "UserId": "bigint",
    "FieldUserId": "bigint",
    "Created": "timestamp",
    "ExpiresBy": "timestamp",
    "Selected": "timestamp",
    "Executed": "timestamp",
    "Expired": "timestamp",
    "Cancelled": "timestamp",
    "IsFinished": "boolean",
    "TagValueChangeId": "bigint",
    "UpdateTimestamp": "timestamp",
}


def transform(df_in: DataFrame):
    df = df_in.select(
        F.col("Id").alias("id"),
        F.col("CommandId").alias("comando_id"),
        F.col("ValueEnumValueId").alias("valor_enum_id"),
        F.col("TargetCommandId").alias("comando_destino_id"),
        F.col("StateEnumValueId").alias("estado_enum_id"),
        F.col("UserId").alias("usuario_id"),
        F.col("FieldUserId").alias("usuario_campo_id"),
        F.col("Created").alias("creado_ts"),
        F.col("ExpiresBy").alias("expira_ts"),
        F.col("Selected").alias("seleccionado_ts"),
        F.col("Executed").alias("ejecutado_ts"),
        F.col("Expired").alias("expirado_ts"),
        F.col("Cancelled").alias("cancelado_ts"),
        F.col("IsFinished").alias("finalizado"),
        F.col("TagValueChangeId").alias("tag_value_change_id"),
        F.col("UpdateTimestamp").alias("ts_actualizacion"),
        F.col("_origen"),
        F.col("_origen_rank"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull()),
        ("NO_TIMESTAMP", F.col("creado_ts").isNull()),
    ])

    kept, duplicated = keep_first(
        kept,
        ["id"],
        [F.col("ts_actualizacion").desc_nulls_last(), F.col("_origen_rank").desc()],
        "DUPLICATE_ID",
    )

    out = (
        kept
        .withColumn("ejecutado", F.col("ejecutado_ts").isNotNull())
        .withColumn(
            "resultado",
            F.when(F.col("ejecutado_ts").isNotNull(), F.lit("EJECUTADO"))
            .when(F.col("cancelado_ts").isNotNull(), F.lit("CANCELADO"))
            .when(F.col("expirado_ts").isNotNull(), F.lit("EXPIRADO"))
            .when(F.col("seleccionado_ts").isNotNull(), F.lit("SELECCIONADO"))
            .otherwise(F.lit("CREADO")),
        )
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_command_execution")
    dq = DQCollector(spark, "job_silver_f_command_execution", args.run_id)

    df_in = read_tedisnet_union(spark, "CommandExecutions", COLUMNS)
    total_in = df_in.count()

    out, rejected = transform(df_in)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected, out, ["ejecutado"])
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
