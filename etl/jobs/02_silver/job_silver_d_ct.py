"""
Silver dimension of the Calser transformer stations (CT) and the bridge
between Calser and TedisNet.

Two tables:

- d_ct: one row per (distribuidora, period, CT), as Calser versions it.
  Calser multiplies the rows by around 200 because every period keeps a copy
  of the topology, so real CTs are always counted with COUNT(DISTINCT id).
- d_ct_scada: one row per (distribuidora, CT) with the TedisNet element that
  is the same physical transformer. Verified key: Calser CT_ID (text, five
  digits with leading zeros) equals SystemElements.ShortName of an element
  of type 145 "TRAFO CT" under the same distribuidora. Not ExportCode and not
  type 144. The comparison is case sensitive, as the TedisNet collation.

Nothing is imputed here. CT_POTENCIA_INSTAL is 0 in 37 % (EOSA) and 48 %
(Pitarch) of the CTs: the rows are flagged with potencia_cero and Gold
decides how to impute, as Calser does with the administrative power.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    ELEMENT_TYPE_TRAFO_CT,
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
    tedisnet_table,
    to_date_col,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_ct"
TARGET_TABLE = silver_table(ENTITY)
MAP_TABLE = silver_table("d_ct_scada")


def transform_ct(df_in: DataFrame, municipios: DataFrame):
    df = df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("CT_PERIODO_ID").alias("periodo_id"),
        clean_str("CT_ID").alias("id"),
        clean_str("CT_MUNICIPIO_ID").alias("municipio_id"),
        F.col("CT_POTENCIA_INSTAL").cast("double").alias("potencia_instal_kva"),
        F.col("CT_POTENCIA_INSTAL_ADMIN").cast("double").alias("potencia_instal_admin_kva"),
        F.col("CT_POTENCIA_CONTRA_MT").cast("double").alias("potencia_contra_mt_kw"),
        F.col("CT_POTENCIA_TOTAL").cast("double").alias("potencia_total_kva"),
        F.col("CT_NUM_ABONADOS").cast("int").alias("num_abonados"),
        to_date_col(df_in, "CT_FECHA_PES").alias("fecha_pes"),
        to_date_col(df_in, "CT_FECHA_BAJA").alias("fecha_baja"),
        clean_str("CT_NUMERO_SERIE_TRAFO").alias("numero_serie_trafo"),
        clean_str("CT_USUARIO_ALTA_ID").alias("usuario_alta_id"),
        F.col("CT_TS").try_cast("timestamp").alias("ts"),
    )

    df = orphan_condition(
        df,
        municipios.select(
            "distribuidora_id",
            "periodo_id",
            F.col("id").alias("municipio_id"),
        ),
        ["distribuidora_id", "periodo_id", "municipio_id"],
        "_sin_municipio",
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("periodo_id").isNull()),
        ("ORPHAN_FK", F.col("_sin_municipio")),
    ])

    kept, duplicated = keep_first(
        kept.drop("_sin_municipio"),
        ["distribuidora_id", "periodo_id", "id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    out = (
        kept
        .withColumn(
            "potencia_cero",
            F.coalesce(F.col("potencia_instal_kva"), F.lit(0.0)) <= F.lit(0.0),
        )
        .withColumn(
            "potencia_admin_cero",
            F.coalesce(F.col("potencia_instal_admin_kva"), F.lit(0.0)) <= F.lit(0.0),
        )
        .withColumn(
            "abonados_negativos",
            F.col("num_abonados") < F.lit(0),
        )
    )

    return out, union_rejects([rejected, duplicated])


def build_scada_map(cts: DataFrame, elementos: DataFrame, nodes):
    """
    One row per (distribuidora, CT) seen in Calser with its TedisNet trafo.
    If several type 145 elements share the ShortName under the same
    distribuidora the lowest id is taken and the ambiguity is reported.
    """
    trafos = (
        elementos
        .where(F.col("tipo_elemento_id") == F.lit(ELEMENT_TYPE_TRAFO_CT))
        .where(F.col("distribuidora_id").isNotNull())
        .select(
            "distribuidora_id",
            F.col("nombre").alias("ct_id"),
            F.col("id").alias("elemento_id"),
            F.col("ruta").alias("elemento_ruta"),
        )
    )

    if nodes is not None:
        with_node = nodes.select(F.col("ElementId").cast("bigint").alias("elemento_id")) \
            .where(F.col("elemento_id").isNotNull()).distinct() \
            .withColumn("tiene_nodo", F.lit(True))

        trafos = trafos.join(with_node, "elemento_id", "left").withColumn(
            "tiene_nodo", F.coalesce(F.col("tiene_nodo"), F.lit(False))
        )
    else:
        trafos = trafos.withColumn("tiene_nodo", F.lit(None).cast("boolean"))

    window = Window.partitionBy("distribuidora_id", "ct_id")

    trafos = (
        trafos
        .withColumn("n_candidatos", F.count(F.lit(1)).over(window))
        .withColumn(
            "_rn",
            F.row_number().over(window.orderBy(
                F.col("tiene_nodo").desc_nulls_last(),
                F.col("elemento_id"),
            )),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    ct_keys = cts.select("distribuidora_id", "source_database", F.col("id").alias("ct_id")).distinct()

    return (
        ct_keys
        .join(trafos, ["distribuidora_id", "ct_id"], "left")
        .withColumn("tiene_telemetria", F.col("elemento_id").isNotNull())
        .withColumn("mapeo_ambiguo", F.coalesce(F.col("n_candidatos"), F.lit(0)) > F.lit(1))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_ct")
    dq = DQCollector(spark, "job_silver_d_ct", args.run_id)

    df_in = read_calser_union(spark, "cts")
    total_in = df_in.count()

    municipios = spark.table(require_table(spark, silver_table("d_municipio")))

    cts, rejected = transform_ct(df_in, municipios)
    cts = cts.localCheckpoint(eager=True)

    elementos = spark.table(require_table(spark, silver_table("d_elemento")))

    nodes_table = tedisnet_table(spark, "SystemNodes")

    if nodes_table is None:
        logger.warning(
            "SystemNodes is not in Bronze yet (streaming job), tiene_nodo "
            "stays null"
        )

    ct_map = build_scada_map(
        cts,
        elementos,
        spark.table(nodes_table) if nodes_table else None,
    ).localCheckpoint(eager=True)

    out = (
        cts.join(
            ct_map.select(
                "distribuidora_id",
                F.col("ct_id").alias("id"),
                "elemento_id",
                "tiene_telemetria",
            ),
            ["distribuidora_id", "id"],
            "left",
        )
        .withColumn("sin_telemetria", ~F.coalesce(F.col("tiene_telemetria"), F.lit(False)))
        .drop("tiene_telemetria")
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    write_table(out, TARGET_TABLE)
    write_table(ct_map, MAP_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(
        ENTITY, total_in, out.count(), rejected,
        out, ["potencia_cero", "potencia_admin_cero", "sin_telemetria"],
    )

    for row in (
        ct_map.groupBy("source_database")
        .agg(
            F.count(F.lit(1)).alias("cts"),
            F.sum(F.col("tiene_telemetria").cast("long")).alias("con_trafo"),
            F.sum(F.col("mapeo_ambiguo").cast("long")).alias("ambiguos"),
            F.sum(F.col("tiene_nodo").cast("long")).alias("con_nodo"),
        )
        .collect()
    ):
        dq.add("d_ct_scada", "cts_distintos", row["cts"], ambito=row["source_database"])
        dq.add("d_ct_scada", "cts_con_trafo_scada", row["con_trafo"], row["cts"],
               ambito=row["source_database"])
        dq.add("d_ct_scada", "cts_sin_trafo_scada", row["cts"] - (row["con_trafo"] or 0),
               row["cts"], umbral_pct=10.0, ambito=row["source_database"])
        dq.add("d_ct_scada", "mapeo_ambiguo", row["ambiguos"] or 0, row["cts"],
               umbral_pct=1.0, ambito=row["source_database"])
        dq.add("d_ct_scada", "trafo_con_nodo", row["con_nodo"] or 0, row["con_trafo"],
               ambito=row["source_database"])

    dq.flush()

    logger.info("Silver completed: %s and %s", TARGET_TABLE, MAP_TABLE)


if __name__ == "__main__":
    main()
