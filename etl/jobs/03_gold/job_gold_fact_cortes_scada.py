"""
Gold fact of the power cuts seen by the SCADA: one row per Off -> On episode
of a transformer.

TedisNet writes a state per event and element (Off, On, communication error)
but no duration. This job sorts the states of every transformer in time,
keeps the transitions (an Off that follows an Off is the same cut, seen from
another switch) and pairs every Off with the next On.

What comes out of it:

- The alternative label of the TFM, independent of Calser: a cut that lasts
  more than 180 s and is not a manoeuvre. In era 3 (Calser imports from the
  SCADA) both labels should agree; job_gold_dq_checks measures how much.
- Microcuts and reclosures (episodes of 180 s or less). Calser does not
  record them (MinimalDurationImportInterruptions = 180 in the three
  databases), and they are the best precursor available inside the SCADA.

Left out: events flagged ts_incoherente in Silver (impossible field time),
communication errors (they are a separate feature) and elements that are not
the TRAFO CT that dim_ct chose for a CT (keying on the ShortName would also
take other elements with the same name, mapeo_ambiguo, and count their Offs
twice). TedisNet times go through tedisnet_time, so a time zone correction
decided in the configuration applies here too.

Two times per end of the episode: inicio_ts / fin_ts are the field times
(the labels), inicio_conocido_ts / fin_conocido_ts the moment the Off and
the On reached the SCADA (the features, known_time).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    get_spark,
    gold_table,
    known_time,
    load_params,
    logger,
    parse_args,
    require_table,
    silver_table,
    tedisnet_time,
    write_table,
)


TARGET_TABLE = gold_table("fact_cortes_scada")


def build(elements: DataFrame, events: DataFrame, dim: DataFrame, params: dict) -> DataFrame:
    scada = params["scada"]

    incoherent = (
        F.col("ts_incoherente") if "ts_incoherente" in elements.columns else F.lit(False)
    )

    trafos = dim.where(F.col("trafo_elemento_id").isNotNull()).select(
        "distribuidora_id",
        "ct_id",
        F.col("trafo_elemento_id").alias("elemento_id"),
    )

    rows = (
        elements
        .where(F.col("es_trafo_ct"))
        .where(F.col("estado").isin("OFF", "ON"))
        .where(~F.coalesce(incoherent, F.lit(False)))
        .drop("distribuidora_id", "ct_id")
        .join(F.broadcast(trafos), "elemento_id")
        .select(
            "distribuidora_id",
            "ct_id",
            "elemento_id",
            "evento_id",
            tedisnet_time("ts", params).alias("ts"),
            known_time(elements, params).alias("ts_conocido"),
            "estado",
            F.coalesce(F.col("es_maniobra"), F.lit(False)).alias("es_maniobra"),
        )
    )

    ordered = Window.partitionBy("distribuidora_id", "ct_id", "elemento_id").orderBy("ts", "evento_id")

    transitions = (
        rows
        .withColumn("_prev", F.lag("estado").over(ordered))
        .where(F.col("_prev").isNull() | (F.col("_prev") != F.col("estado")))
        .withColumn("_next_estado", F.lead("estado").over(ordered))
        .withColumn("_next_ts", F.lead("ts").over(ordered))
        .withColumn("_next_ts_conocido", F.lead("ts_conocido").over(ordered))
        .withColumn("_next_evento", F.lead("evento_id").over(ordered))
    )

    max_seconds = int(scada["emparejamiento_max_h"]) * 3600

    per_event = events.select(
        F.col("evento_id").alias("evento_off_id"),
        F.col("n_trafos_ct").alias("n_trafos_evento"),
    )

    return (
        transitions
        .where(F.col("estado") == F.lit("OFF"))
        .select(
            "distribuidora_id",
            "ct_id",
            "elemento_id",
            F.col("evento_id").alias("evento_off_id"),
            F.col("ts").alias("inicio_ts"),
            F.when(F.col("_next_estado") == F.lit("ON"), F.col("_next_ts")).alias("fin_ts"),
            F.col("ts_conocido").alias("inicio_conocido_ts"),
            F.when(F.col("_next_estado") == F.lit("ON"), F.col("_next_ts_conocido")).alias("fin_conocido_ts"),
            F.when(F.col("_next_estado") == F.lit("ON"), F.col("_next_evento")).alias("evento_on_id"),
            "es_maniobra",
        )
        .withColumn("duracion_s", F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts"))
        .withColumn("abierto", F.col("fin_ts").isNull())
        # An On days later is usually a missing On in between (RabbitMQ
        # gaps): the pair is kept but its duration is not trusted.
        .withColumn(
            "emparejamiento_dudoso",
            F.coalesce(F.col("duracion_s") > F.lit(max_seconds), F.lit(False)),
        )
        .withColumn(
            "es_microcorte",
            F.coalesce(F.col("duracion_s") <= F.lit(int(scada["microcorte_max_s"])), F.lit(False)),
        )
        .withColumn(
            "es_corte_etiqueta",
            ~F.col("es_maniobra")
            & ~F.col("es_microcorte")
            & ~F.col("emparejamiento_dudoso"),
        )
        .join(per_event, "evento_off_id", "left")
        .withColumn("es_sistemico_scada", F.coalesce(F.col("n_trafos_evento") > 1, F.lit(False)))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-fact_cortes_scada")
    dq = DQCollector(spark, "job_gold_fact_cortes_scada", args.run_id)

    elements = spark.table(require_table(spark, silver_table("f_corte_elemento")))
    events = spark.table(require_table(spark, silver_table("f_corte_evento")))
    dim = spark.table(require_table(spark, gold_table("dim_ct")))

    out = build(elements, events, dim, params).localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)

    for row in (
        out.groupBy("distribuidora_id")
        .agg(
            F.count(F.lit(1)).alias("episodios"),
            F.countDistinct("ct_id").alias("cts"),
            F.sum(F.col("es_microcorte").cast("long")).alias("microcortes"),
            F.sum(F.col("es_maniobra").cast("long")).alias("maniobras"),
            F.sum(F.col("abierto").cast("long")).alias("abiertos"),
            F.sum(F.col("emparejamiento_dudoso").cast("long")).alias("dudosos"),
            F.sum(F.col("es_corte_etiqueta").cast("long")).alias("etiqueta"),
            F.sum(
                (F.unix_timestamp("inicio_conocido_ts") - F.unix_timestamp("inicio_ts") > F.lit(3600)).cast("long")
            ).alias("tardios"),
        )
        .collect()
    ):
        ambito = str(row["distribuidora_id"])
        dq.add("fact_cortes_scada", "episodios_off_on", row["episodios"], ambito=ambito,
               detalle=f"{row['cts']} CTs")
        dq.add("fact_cortes_scada", "microcortes", row["microcortes"], row["episodios"], ambito=ambito)
        dq.add("fact_cortes_scada", "maniobras", row["maniobras"], row["episodios"], ambito=ambito)
        dq.add("fact_cortes_scada", "sin_on", row["abiertos"], row["episodios"], umbral_pct=5.0, ambito=ambito)
        dq.add("fact_cortes_scada", "emparejamiento_dudoso", row["dudosos"], row["episodios"],
               umbral_pct=5.0, ambito=ambito)
        dq.add("fact_cortes_scada", "cortes_etiqueta", row["etiqueta"], row["episodios"], ambito=ambito)
        dq.add("fact_cortes_scada", "off_llegada_tardia", row["tardios"], row["episodios"], ambito=ambito,
               detalle="Off stored by the SCADA more than 1 h after its field time")

    dq.flush()

    logger.info("Gold completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
