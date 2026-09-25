"""
Silver fact of the TedisNet events: tag changes that the SCADA raised as a
warning, an alarm or a trip.

An event is only a pointer to a tag value change plus the acknowledgement of
the operator. Without its tag value change it means nothing, so an event
whose change did not survive f_tag_value_change is rejected. The tag, the
element, the distribuidora and the value are copied from the change, and the
level of the event is resolved from LibTagClass_EnumValues_EventLevels
(class of the tag + enum value), the same way the SCADA does.
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
    require_table,
    silver_table,
    split_rejects,
    tedisnet_table,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "f_evento"
TARGET_TABLE = silver_table(ENTITY)

COLUMNS = {
    "Id": "bigint",
    "TagValueChangeId": "bigint",
    "AckUserId": "bigint",
    "AckTimestamp": "timestamp",
}


def transform(events_in: DataFrame, changes: DataFrame, tags: DataFrame, levels):
    df = events_in.select(
        F.col("Id").alias("id"),
        F.col("TagValueChangeId").alias("tag_value_change_id"),
        F.col("AckUserId").alias("ack_usuario_id"),
        F.col("AckTimestamp").alias("ack_ts"),
        F.col("_origen"),
        F.col("_origen_rank"),
    )

    df = df.join(
        changes.select(
            F.col("id").alias("tag_value_change_id"),
            "tag_id",
            "elemento_id",
            "distribuidora_id",
            "ts",
            "valor_bool",
            "valor_int",
            "valor_enum_id",
            F.lit(True).alias("_cambio_ok"),
        ),
        "tag_value_change_id",
        "left",
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull()),
        ("ORPHAN_FK", F.col("_cambio_ok").isNull()),
    ])

    kept, duplicated = keep_first(
        kept.drop("_cambio_ok"),
        ["id"],
        [F.col("_origen_rank").desc(), F.col("ack_ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    kept = kept.join(
        F.broadcast(tags.select(F.col("id").alias("tag_id"), "clase_id", "clase_nombre")),
        "tag_id",
        "left",
    )

    if levels is not None:
        kept = kept.join(
            F.broadcast(levels.select(
                F.col("TagClassId").cast("bigint").alias("clase_id"),
                F.col("EnumValueId").cast("bigint").alias("valor_enum_id"),
                F.col("EventLevelId").cast("int").alias("nivel_evento_id"),
            )),
            ["clase_id", "valor_enum_id"],
            "left",
        )
    else:
        kept = kept.withColumn("nivel_evento_id", F.lit(None).cast("int"))

    out = (
        kept
        .withColumn("reconocido", F.col("ack_ts").isNotNull())
        .withColumn("fecha", F.to_date("ts"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected.drop("_cambio_ok"), duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_evento")
    dq = DQCollector(spark, "job_silver_f_evento", args.run_id)

    events_in = read_tedisnet_union(spark, "Events", COLUMNS)
    total_in = events_in.count()

    changes = spark.table(require_table(spark, silver_table("f_tag_value_change")))
    tags = spark.table(require_table(spark, silver_table("d_tag")))

    levels_table = tedisnet_table(spark, "LibTagClass_EnumValues_EventLevels")

    out, rejected = transform(
        events_in,
        changes,
        tags,
        spark.table(levels_table) if levels_table else None,
    )
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected, out, ["reconocido"])
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
