"""
Silver dimension of the TedisNet tags (signals).

A tag is a signal of a device (measure, state, alarm) attached to an element
of the network. Every value of TagValueChanges and TagIntervalValues points
to a tag, and it is through the tag that a value reaches its element, its CT
and its distribuidora. So this table carries the distribuidora of the tag,
resolved once in d_elemento, and the name of its class (AI.INTENS L1,
DI.DEFECTO DE TIERRA.2, ES.POSICION...), which is what Gold uses to choose
features.

A tag without element, or with an element or device that does not exist, is
rejected: it can not be attached to any CT. The SCADA view SystemTagDetails
drops them in the same way with an inner join.

Only about 4.000 of the 78.000 tags have StoreInterval, and only those have
a regular series in TagIntervalValuesBig. tiene_serie makes that visible.
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
    project,
    require_table,
    silver_table,
    split_rejects,
    tedisnet_table,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_tag"
TARGET_TABLE = silver_table(ENTITY)

TAG_COLUMNS = {
    "Id": "bigint",
    "Name": "string",
    "ShortName": "string",
    "DeviceId": "bigint",
    "TagClassId": "bigint",
    "ElementId": "bigint",
    "StoreInterval": "int",
    "SummaryStoreInterval": "int",
    "ExportCode": "int",
}


def transform(tags_in: DataFrame, elementos: DataFrame, devices, classes):
    df = tags_in.select(
        F.col("Id").alias("id"),
        clean_str("Name").alias("nombre"),
        clean_str("ShortName").alias("nombre_corto"),
        F.col("DeviceId").alias("dispositivo_id"),
        F.col("TagClassId").alias("clase_id"),
        F.col("ElementId").alias("elemento_id"),
        F.col("StoreInterval").alias("store_interval_s"),
        F.col("SummaryStoreInterval").alias("summary_store_interval_s"),
        F.col("ExportCode").alias("export_code"),
    )

    df = orphan_condition(
        df,
        elementos.select(F.col("id").alias("elemento_id")),
        ["elemento_id"],
        "_sin_elemento",
    )

    rules = [
        ("NULL_KEY", F.col("id").isNull()),
        ("ORPHAN_FK", F.col("_sin_elemento")),
    ]

    if devices is not None:
        df = orphan_condition(
            df,
            devices.select(F.col("Id").cast("bigint").alias("dispositivo_id")),
            ["dispositivo_id"],
            "_sin_dispositivo",
        )
        rules.append(("ORPHAN_FK_DEVICE", F.col("_sin_dispositivo")))

    kept, rejected = split_rejects(df, rules)

    kept = kept.drop("_sin_elemento", "_sin_dispositivo")

    kept, duplicated = keep_first(kept, ["id"], [F.col("id")], "DUPLICATE_ID")

    out = kept.join(
        elementos.select(
            F.col("id").alias("elemento_id"),
            F.col("tipo_elemento_id").alias("elemento_tipo_id"),
            F.col("nombre").alias("elemento_nombre"),
            "distribuidora_id",
            "source_database",
        ),
        "elemento_id",
        "left",
    )

    if classes is not None:
        out = out.join(
            F.broadcast(classes.select(
                F.col("Id").cast("bigint").alias("clase_id"),
                clean_str("Name").alias("clase_nombre"),
                clean_str("Units").alias("unidades"),
                F.col("EventTypeId").cast("int").alias("clase_tipo_evento_id"),
            )),
            "clase_id",
            "left",
        )
    else:
        out = (
            out.withColumn("clase_nombre", F.lit(None).cast("string"))
            .withColumn("unidades", F.lit(None).cast("string"))
            .withColumn("clase_tipo_evento_id", F.lit(None).cast("int"))
        )

    out = (
        out
        # Prefix of the class name: AI analog input, DI digital input,
        # DO digital output, ES element state...
        .withColumn("clase_prefijo", F.regexp_extract(F.col("clase_nombre"), r"^([A-Z]+)\.", 1))
        .withColumn("tiene_serie", F.coalesce(F.col("store_interval_s"), F.lit(0)) > F.lit(0))
        .withColumn("clase_desconocida", F.col("clase_nombre").isNull())
        .withColumn("sin_distribuidora", F.col("distribuidora_id").isNull())
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_tag")
    dq = DQCollector(spark, "job_silver_d_tag", args.run_id)

    tags_table = tedisnet_table(spark, "SystemTags")

    if tags_table is None:
        raise ValueError(
            "SystemTags is not in Bronze, run the CDC streaming jobs first"
        )

    tags_in = project(spark.table(tags_table), TAG_COLUMNS)
    total_in = tags_in.count()

    elementos = spark.table(require_table(spark, silver_table("d_elemento")))

    devices_table = tedisnet_table(spark, "SystemDevices")
    classes_table = tedisnet_table(spark, "LibTagClasses")

    out, rejected = transform(
        tags_in,
        elementos,
        spark.table(devices_table) if devices_table else None,
        spark.table(classes_table) if classes_table else None,
    )
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(
        ENTITY, total_in, out.count(), rejected, out,
        ["tiene_serie", "clase_desconocida", "sin_distribuidora"],
    )
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
