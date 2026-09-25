"""
Silver facts of the TedisNet power cuts: f_corte_evento (one row per cut
event) and f_corte_elemento (one row per element affected by an event).

TedisNet does not measure a cut, it deduces it from the topology: when an
element state tag changes, the PowerCutService floods the network from the
source nodes and every transformer that is not reached is Off. What we
learnt from the real databases shapes the rules:

- One tag change does not produce one event. There are up to 69 identical
  events for the same TagValueChangeId, Timestamp, CutStateId and root
  element. They are collapsed into a canonical event (lowest Id) and the
  elements of every copy are moved to it, so no affected element is lost.
  Never count CutEventId in the source.
- Events without TagValueChangeId and without state, or without timestamp,
  are empty and rejected. Events that end up without any element (18,7 % in
  the test database) are kept and flagged: the state change happened even if
  it did not isolate a transformer.
- CutStateId 3 is a communication error, not a cut: flagged (es_error_comm),
  it is a precursor feature and must be out of the target.
- IsCommand is always false in the source. A cut is marked as a manoeuvre
  (es_maniobra) when its tag value change is the one of a command execution.
- ElectricalElementType is not validated in the source (values that are
  really ElementTypeId appear). Outside 1 - 4 it is set to null and flagged.
- An element without node in SystemNodes never enters the flood fill, so it
  can never be Off: tiene_nodo tells Gold which transformers are observable.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_common import (
    CUT_STATE_COMM_ERROR,
    CUT_STATE_OFF,
    CUT_STATE_ON,
    DQCollector,
    ELEMENT_TYPE_TRAFO_CT,
    VALID_ELECTRICAL_ELEMENT_TYPES,
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


EVENT_ENTITY = "f_corte_evento"
ELEMENT_ENTITY = "f_corte_elemento"

EVENT_COLUMNS = {
    "Id": "bigint",
    "TagValueChangeId": "bigint",
    "Timestamp": "timestamp",
    "ProcessedTimestamp": "timestamp",
    "CutStateId": "int",
    "IsCommand": "boolean",
    "RootElementId": "bigint",
}

ELEMENT_COLUMNS = {
    "CutEventId": "bigint",
    "ElementId": "bigint",
    "ElectricalElementType": "int",
}


def clean_events(events_in: DataFrame):
    """
    Returns (events, id_map, rejected):
    - events: one canonical row per natural key.
    - id_map: every surviving source event id and its canonical id.
    """
    df = events_in.select(
        F.col("Id").alias("id"),
        F.col("TagValueChangeId").alias("tag_value_change_id"),
        F.col("Timestamp").alias("ts"),
        F.col("ProcessedTimestamp").alias("ts_procesado"),
        F.col("CutStateId").alias("estado_corte_id"),
        F.col("RootElementId").alias("elemento_raiz_id"),
        F.col("_origen"),
        F.col("_origen_rank"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull()),
        ("EMPTY_EVENT", F.col("estado_corte_id").isNull() & F.col("tag_value_change_id").isNull()),
        ("NO_TIMESTAMP", F.col("ts").isNull()),
        ("NO_STATE", F.col("estado_corte_id").isNull()),
    ])

    kept, dup_id = keep_first(
        kept,
        ["id"],
        [F.col("_origen_rank").desc(), F.col("ts_procesado").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    natural_key = ["tag_value_change_id", "ts", "estado_corte_id", "elemento_raiz_id"]
    window = Window.partitionBy(*natural_key)

    kept = (
        kept
        .withColumn("evento_id", F.min("id").over(window))
        .withColumn("n_eventos_origen", F.count(F.lit(1)).over(window))
    )

    id_map = kept.select(F.col("id").alias("_id_origen"), "evento_id")

    events = kept.where(F.col("id") == F.col("evento_id")).drop("id")

    dup_natural = (
        kept.where(F.col("id") != F.col("evento_id"))
        .withColumn("_motivo", F.lit("DUPLICATE_NATURAL_KEY"))
    )

    return events, id_map, union_rejects([rejected, dup_id, dup_natural])


def clean_elements(elements_in: DataFrame, id_map: DataFrame, elementos: DataFrame, nodes):
    df = elements_in.select(
        F.col("CutEventId").alias("_id_origen"),
        F.col("ElementId").alias("elemento_id"),
        F.col("ElectricalElementType").alias("tipo_electrico_origen"),
        F.col("_origen"),
        F.col("_origen_rank"),
    )

    df = df.join(id_map, "_id_origen", "left")

    known = elementos.select(
        F.col("id").alias("elemento_id"),
        F.col("tipo_elemento_id"),
        F.col("nombre").alias("elemento_nombre"),
        "distribuidora_id",
        F.lit(True).alias("_elemento_ok"),
    )

    df = df.join(known, "elemento_id", "left")

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("elemento_id").isNull() | F.col("_id_origen").isNull()),
        ("ORPHAN_FK", F.col("evento_id").isNull()),
        ("ORPHAN_FK_ELEMENT", F.col("_elemento_ok").isNull()),
    ])

    kept, duplicated = keep_first(
        kept.drop("_elemento_ok"),
        ["evento_id", "elemento_id"],
        [F.col("_origen_rank").desc(), F.col("_id_origen")],
        "DUPLICATE_ID",
    )

    valid_type = F.col("tipo_electrico_origen").isin(VALID_ELECTRICAL_ELEMENT_TYPES)

    kept = (
        kept
        .withColumn(
            "tipo_electrico",
            F.when(valid_type, F.col("tipo_electrico_origen")),
        )
        .withColumn(
            "tipo_electrico_invalido",
            F.col("tipo_electrico_origen").isNotNull() & ~valid_type,
        )
        .withColumn("es_trafo_ct", F.col("tipo_elemento_id") == F.lit(ELEMENT_TYPE_TRAFO_CT))
        # For a TRAFO CT the ShortName is the Calser CT_ID, the key to
        # join the SCADA cut with the Calser interruption.
        .withColumn("ct_id", F.when(F.col("es_trafo_ct"), F.col("elemento_nombre")))
    )

    if nodes is not None:
        with_node = (
            nodes.select(F.col("ElementId").cast("bigint").alias("elemento_id"))
            .where(F.col("elemento_id").isNotNull())
            .distinct()
            .withColumn("tiene_nodo", F.lit(True))
        )

        kept = kept.join(F.broadcast(with_node), "elemento_id", "left").withColumn(
            "tiene_nodo", F.coalesce(F.col("tiene_nodo"), F.lit(False))
        )
    else:
        kept = kept.withColumn("tiene_nodo", F.lit(None).cast("boolean"))

    return kept, union_rejects([rejected.drop("_elemento_ok"), duplicated])


def add_event_flags(events: DataFrame, elements: DataFrame, commands, elementos: DataFrame):
    per_event = elements.groupBy("evento_id").agg(
        F.count(F.lit(1)).alias("n_elementos"),
        F.sum(F.col("es_trafo_ct").cast("long")).alias("n_trafos_ct"),
    )

    root = elementos.select(
        F.col("id").alias("elemento_raiz_id"),
        F.col("distribuidora_id"),
        F.col("source_database"),
    )

    events = (
        events
        .join(per_event, "evento_id", "left")
        .join(root, "elemento_raiz_id", "left")
        .withColumn("n_elementos", F.coalesce(F.col("n_elementos"), F.lit(0)))
        .withColumn("n_trafos_ct", F.coalesce(F.col("n_trafos_ct"), F.lit(0)))
        .withColumn("sin_elementos", F.col("n_elementos") == F.lit(0))
        .withColumn(
            "estado",
            F.when(F.col("estado_corte_id") == F.lit(CUT_STATE_OFF), F.lit("OFF"))
            .when(F.col("estado_corte_id") == F.lit(CUT_STATE_ON), F.lit("ON"))
            .when(F.col("estado_corte_id") == F.lit(CUT_STATE_COMM_ERROR), F.lit("ERROR_COMM"))
            .otherwise(F.lit("OTRO")),
        )
        .withColumn("es_error_comm", F.col("estado_corte_id") == F.lit(CUT_STATE_COMM_ERROR))
        .withColumn("estado_desconocido", F.col("estado") == F.lit("OTRO"))
        .withColumn("evento_colapsado", F.col("n_eventos_origen") > F.lit(1))
    )

    if commands is not None:
        manoeuvres = (
            commands.where(F.col("tag_value_change_id").isNotNull())
            .select("tag_value_change_id")
            .distinct()
            .withColumn("es_maniobra", F.lit(True))
        )

        events = events.join(manoeuvres, "tag_value_change_id", "left").withColumn(
            "es_maniobra", F.coalesce(F.col("es_maniobra"), F.lit(False))
        )
    else:
        events = events.withColumn("es_maniobra", F.lit(None).cast("boolean"))

    return events


def denormalize_elements(elements: DataFrame, events: DataFrame) -> DataFrame:
    """
    Copies the time and the state of the event to every element row. Gold
    pairs Off and On per element, and doing that without a join on 900.000
    rows is worth the redundancy.
    """
    return elements.join(
        events.select(
            "evento_id",
            "ts",
            "estado",
            "estado_corte_id",
            "es_error_comm",
            "es_maniobra",
            F.col("distribuidora_id").alias("_distribuidora_evento"),
        ),
        "evento_id",
        "inner",
    ).withColumn(
        "distribuidora_id",
        F.coalesce(F.col("distribuidora_id"), F.col("_distribuidora_evento")),
    ).drop("_distribuidora_evento")


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_corte")
    dq = DQCollector(spark, "job_silver_f_corte", args.run_id)

    events_in = read_tedisnet_union(spark, "ElectricPowerCutEvents", EVENT_COLUMNS)
    elements_in = read_tedisnet_union(spark, "ElectricPowerCutElementEvents", ELEMENT_COLUMNS)

    events_total = events_in.count()
    elements_total = elements_in.count()

    elementos = spark.table(require_table(spark, silver_table("d_elemento")))

    command_table = silver_table("f_command_execution")
    commands = spark.table(command_table) if spark.catalog.tableExists(command_table) else None

    if commands is None:
        logger.warning("%s not found, es_maniobra stays null", command_table)

    nodes_table = tedisnet_table(spark, "SystemNodes")

    events, id_map, events_rejected = clean_events(events_in)
    events = events.localCheckpoint(eager=True)
    id_map = id_map.localCheckpoint(eager=True)

    elements, elements_rejected = clean_elements(
        elements_in,
        id_map,
        elementos,
        spark.table(nodes_table) if nodes_table else None,
    )
    elements = elements.localCheckpoint(eager=True)

    events = add_event_flags(events, elements, commands, elementos).withColumn(
        "audit_loaded_at", F.current_timestamp()
    ).localCheckpoint(eager=True)

    elements = denormalize_elements(elements, events).withColumn(
        "audit_loaded_at", F.current_timestamp()
    ).drop("_id_origen").localCheckpoint(eager=True)

    write_table(events, silver_table(EVENT_ENTITY))
    write_table(elements, silver_table(ELEMENT_ENTITY))
    write_rejected(events_rejected, EVENT_ENTITY, args.run_id)
    write_rejected(elements_rejected, ELEMENT_ENTITY, args.run_id)

    dq.add_entity_counts(
        EVENT_ENTITY, events_total, events.count(), events_rejected, events,
        ["sin_elementos", "es_error_comm", "es_maniobra", "estado_desconocido", "evento_colapsado"],
    )
    dq.add_entity_counts(
        ELEMENT_ENTITY, elements_total, elements.count(), elements_rejected, elements,
        ["tipo_electrico_invalido", "es_trafo_ct", "tiene_nodo"],
    )

    for row in (
        elements.where(F.col("es_trafo_ct"))
        .groupBy("distribuidora_id", "estado")
        .agg(
            F.count(F.lit(1)).alias("filas"),
            F.countDistinct("elemento_id").alias("trafos"),
        )
        .collect()
    ):
        dq.add(ELEMENT_ENTITY, f"trafos_ct:{row['estado']}", row["filas"],
               ambito=str(row["distribuidora_id"]),
               detalle=f"{row['trafos']} distinct transformers")

    dq.flush()

    logger.info("Silver completed: %s and %s", EVENT_ENTITY, ELEMENT_ENTITY)


if __name__ == "__main__":
    main()
