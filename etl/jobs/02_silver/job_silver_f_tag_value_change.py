"""
Silver fact of the TedisNet tag value changes: the event stream of the SCADA
(one row every time a signal changes), source of the precursor features
(trips, earth faults, phase faults, communication errors, recloser
operations).

Rules, in this order:

1. Values that are not a real reading go to f_tag_quality_event instead of
   being lost: the number of communication failures per device is a
   precursor feature. They are also out of the value series.
2. Reject: no timestamp, no value, quality not Good (null counts as
   unknown), detail not real, manual source (typed by the operator), tag
   that does not exist in d_tag.
3. Deduplicate by Id (the same change can arrive from Historic and from
   System, and the CDC stream is at least once). Historic wins.
4. Deduplicate by natural key (tag, timestamp, value, quality), in case the
   same change was inserted again with another Id. Conservative: only
   identical rows collapse.

Kept with a flag: estimated values (source 4), calculated values (source 2),
copied values (detail 14, the element state tags the SCADA uses to detect
cuts) and values out of the configured range (detail 11 and 12).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    QUALITY_DETAIL_COPIED,
    QUALITY_DETAIL_NOT_REAL,
    QUALITY_DETAIL_OUT_OF_RANGE,
    QUALITY_GOOD,
    QUALITY_SOURCE_CALCULATED,
    QUALITY_SOURCE_ESTIMATED,
    QUALITY_SOURCE_MANUAL,
    get_spark,
    keep_first,
    logger,
    parse_args,
    read_tedisnet_union,
    require_table,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "f_tag_value_change"
TARGET_TABLE = silver_table(ENTITY)
QUALITY_EVENTS_TABLE = silver_table("f_tag_quality_event")

VALUE_COLUMNS = {
    "Id": "bigint",
    "TagId": "bigint",
    "ValueBool": "boolean",
    "ValueInt": "bigint",
    "ValueFloat": "double",
    "ValueStr": "string",
    "ValueEnumId": "bigint",
    "SourceTimestamp": "timestamp",
    "UpdateTimestamp": "timestamp",
    "QualityId": "int",
    "QualityDetailId": "int",
    "QualitySourceId": "int",
}

FLAGS = ["es_estimado", "es_calculado", "es_copiado", "fuera_rango_egu", "ts_desde_update"]


def rename_values(df: DataFrame) -> DataFrame:
    """
    Common projection of the four value tables of TedisNet, which share the
    same value and quality columns.
    """
    return df.select(
        F.col("Id").alias("id"),
        F.col("TagId").alias("tag_id"),
        F.col("ValueBool").alias("valor_bool"),
        F.col("ValueInt").alias("valor_int"),
        F.col("ValueFloat").alias("valor_float"),
        F.col("ValueStr").alias("valor_str"),
        F.col("ValueEnumId").alias("valor_enum_id"),
        F.coalesce(F.col("SourceTimestamp"), F.col("UpdateTimestamp")).alias("ts"),
        F.col("SourceTimestamp").alias("ts_origen"),
        F.col("UpdateTimestamp").alias("ts_actualizacion"),
        F.col("QualityId").alias("calidad_id"),
        F.col("QualityDetailId").alias("calidad_detalle_id"),
        F.col("QualitySourceId").alias("calidad_fuente_id"),
        F.col("_origen"),
        F.col("_origen_rank"),
        F.col("_tabla_origen"),
    )


def quality_rules(tag_orphan_col: str = None) -> list:
    """
    Reject rules shared by the value tables. The order gives the reason
    when several apply.
    """
    no_value = (
        F.col("valor_bool").isNull()
        & F.col("valor_int").isNull()
        & F.col("valor_float").isNull()
        & F.col("valor_str").isNull()
        & F.col("valor_enum_id").isNull()
    )

    rules = [
        ("NULL_KEY", F.col("tag_id").isNull()),
        ("NO_TIMESTAMP", F.col("ts").isNull()),
        ("EMPTY_VALUE", no_value),
        ("QUALITY_DETAIL_NOT_REAL", F.col("calidad_detalle_id").isin(QUALITY_DETAIL_NOT_REAL)),
        ("QUALITY_BAD", F.col("calidad_id").isNull() | (F.col("calidad_id") != F.lit(QUALITY_GOOD))),
        ("SOURCE_MANUAL", F.col("calidad_fuente_id") == F.lit(QUALITY_SOURCE_MANUAL)),
        ("NOT_A_NUMBER", F.isnan(F.col("valor_float"))),
    ]

    if tag_orphan_col:
        rules.append(("ORPHAN_FK", F.col(tag_orphan_col)))

    return rules


def value_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("es_estimado", F.coalesce(F.col("calidad_fuente_id") == F.lit(QUALITY_SOURCE_ESTIMATED), F.lit(False)))
        .withColumn("es_calculado", F.coalesce(F.col("calidad_fuente_id") == F.lit(QUALITY_SOURCE_CALCULATED), F.lit(False)))
        .withColumn("es_copiado", F.coalesce(F.col("calidad_detalle_id") == F.lit(QUALITY_DETAIL_COPIED), F.lit(False)))
        .withColumn("fuera_rango_egu", F.coalesce(F.col("calidad_detalle_id").isin(QUALITY_DETAIL_OUT_OF_RANGE), F.lit(False)))
        .withColumn("ts_desde_update", F.col("ts_origen").isNull())
    )


def value_signature() -> F.Column:
    """
    Text form of the value, null safe, used by the natural key.
    """
    return F.concat_ws(
        "|",
        F.coalesce(F.col("valor_bool").cast("string"), F.lit("~")),
        F.coalesce(F.col("valor_int").cast("string"), F.lit("~")),
        F.coalesce(F.col("valor_float").cast("string"), F.lit("~")),
        F.coalesce(F.col("valor_str"), F.lit("~")),
        F.coalesce(F.col("valor_enum_id").cast("string"), F.lit("~")),
    )


def attach_tags(df: DataFrame, tags: DataFrame) -> DataFrame:
    """
    Adds the element and the distribuidora of the tag and a _sin_tag flag.
    d_tag has some tens of thousands of rows, so it is broadcast.
    """
    lookup = F.broadcast(
        tags.select(
            F.col("id").alias("tag_id"),
            "elemento_id",
            "distribuidora_id",
            F.lit(True).alias("_tag_ok"),
        )
    )

    return (
        df.join(lookup, "tag_id", "left")
        .withColumn("_sin_tag", F.col("_tag_ok").isNull())
        .drop("_tag_ok")
    )


def transform(df_in: DataFrame, tags: DataFrame):
    df = attach_tags(rename_values(df_in), tags)

    quality_events = (
        df.where(
            F.col("calidad_detalle_id").isin(QUALITY_DETAIL_NOT_REAL)
            & F.col("tag_id").isNotNull()
            & F.col("ts").isNotNull()
        )
        .select(
            "tag_id", "elemento_id", "distribuidora_id", "ts",
            "calidad_id", "calidad_detalle_id", "calidad_fuente_id", "_origen",
        )
        .dropDuplicates(["tag_id", "ts", "calidad_detalle_id"])
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    kept, rejected = split_rejects(df, quality_rules("_sin_tag"))
    kept = kept.drop("_sin_tag")

    kept, dup_id = keep_first(
        kept,
        ["id"],
        [F.col("_origen_rank").desc(), F.col("ts_actualizacion").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    kept, dup_natural = keep_first(
        kept.withColumn("_valor_firma", value_signature()),
        ["tag_id", "ts", "_valor_firma", "calidad_id"],
        [F.col("_origen_rank").desc(), F.col("id")],
        "DUPLICATE_NATURAL_KEY",
    )

    out = (
        value_flags(kept.drop("_valor_firma"))
        .withColumn("fecha", F.to_date("ts"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    rejected_all = union_rejects([
        rejected.drop("_sin_tag"),
        dup_id,
        dup_natural.drop("_valor_firma"),
    ])

    return out, rejected_all, quality_events


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_tag_value_change")
    dq = DQCollector(spark, "job_silver_f_tag_value_change", args.run_id)

    df_in = read_tedisnet_union(spark, "TagValueChanges", VALUE_COLUMNS)
    total_in = df_in.count()

    tags = spark.table(require_table(spark, silver_table("d_tag")))

    out, rejected, quality_events = transform(df_in, tags)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_table(quality_events, QUALITY_EVENTS_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected, out, FLAGS)

    for row in out.groupBy("_origen").count().collect():
        dq.add(ENTITY, "filas_por_origen", row["count"], ambito=row["_origen"])

    for row in (
        spark.table(QUALITY_EVENTS_TABLE)
        .groupBy("calidad_detalle_id").count().collect()
    ):
        dq.add(
            "f_tag_quality_event",
            f"detalle:{row['calidad_detalle_id']}",
            row["count"],
        )

    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
