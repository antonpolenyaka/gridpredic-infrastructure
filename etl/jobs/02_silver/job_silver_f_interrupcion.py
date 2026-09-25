"""
Silver fact of the Calser interruptions: the source of the label of the
model.

What was found in the real databases and how Silver handles it:

- Rows recorded twice (984 in EOSA, 1.952 in Pitarch): collapsed by the
  natural key, only when they are identical in element, level, period, start
  and duration. Two real cuts at the same minute with a different duration
  are both kept.
- Overlapping interruptions of the same CT (1.605 pairs in EOSA, 2.388 in
  Pitarch): not merged here. They get a common grupo_solape_id; the merge
  rule is a modelling decision and is applied in Gold, where it can change
  without rebuilding Silver.
- Zero duration (815 / 1.255 rows) and microcuts (up to 180 s): flagged.
  They are out of the target but they pollute count features if nobody
  tells them apart.
- *_OPTIMIZADA columns: a manual what-if ("if the repair had been faster"),
  not a measurement. They are dropped and never reach Gold.
- INT_FECHA_ALTA and INT_TS are the load date, weeks after the event in
  some cases: kept for auditing, flagged as leakage in the docs.
- The period id is not trusted for ordering (DEFAULT, mixed formats): the
  month of the start is computed from the start date.
- Calser only validates start <= end when saving. An end before the start
  is impossible and is the only interval rule that rejects.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    DURATION_TOLERANCE_SECONDS,
    EXTREME_DURATION_SECONDS,
    MICROCUT_MAX_SECONDS,
    clean_str,
    combine_date_time,
    get_spark,
    keep_first,
    logger,
    orphan_condition,
    parse_args,
    read_calser_union,
    require_table,
    silver_table,
    split_rejects,
    to_date_col,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "f_interrupcion"
TARGET_TABLE = silver_table(ENTITY)

FLAGS = [
    "duracion_cero",
    "es_microcorte",
    "duracion_extrema",
    "duracion_incoherente",
    "sin_fin",
    "sin_incidencia",
    "incidencia_inexistente",
    "en_solape",
    "duplicado_otro_periodo",
    "periodo_incoherente",
    "es_origen_scada",
    "es_incompleta",
]


def select_columns(df_in: DataFrame) -> DataFrame:
    return df_in.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str("INT_PERIODO_ID").alias("periodo_id"),
        clean_str("INT_ID").alias("id"),
        clean_str("INT_REFERENCIA").alias("referencia"),
        clean_str("INT_DESC").alias("descripcion"),
        clean_str("INT_OBS").alias("observaciones"),
        to_date_col(df_in, "INT_FECHA_INICIO").alias("fecha_inicio"),
        combine_date_time(df_in, "INT_FECHA_INICIO", "INT_HORA_INICIO").alias("inicio_ts"),
        combine_date_time(df_in, "INT_FECHA_FIN", "INT_HORA_FIN").alias("fin_ts"),
        F.col("INT_DURACION").cast("long").alias("duracion_s"),
        clean_str("INT_TIPO_EVENTO_ID").alias("tipo_evento_id"),
        clean_str("INT_ELEMENTO_ID").alias("elemento_id"),
        clean_str("INT_SALIDA_ID").alias("salida_id"),
        clean_str("INT_ACOMETIDA_ID").alias("acometida_id"),
        clean_str("INT_ABONADO_ID").alias("abonado_id"),
        clean_str("INT_INCIDENCIA_ID").alias("incidencia_id"),
        clean_str("INT_EG_ID").alias("estado_id"),
        clean_str("INT_USUARIO_ID").alias("usuario_id"),
        to_date_col(df_in, "INT_FECHA_ALTA").alias("fecha_alta"),
        F.col("INT_TS").try_cast("timestamp").alias("ts"),
    )


def add_overlap_groups(df: DataFrame) -> DataFrame:
    """
    Groups the interruptions of the same CT whose intervals intersect.

    Classic interval grouping: sorted by start, a row opens a new group when
    it starts at or after the latest end seen so far in the CT. Touching
    intervals (end == next start) are not an overlap.
    """
    fin_eff = F.coalesce(F.col("fin_ts"), F.col("inicio_ts"))

    ordered = (
        Window.partitionBy("distribuidora_id", "elemento_id")
        .orderBy(F.col("inicio_ts"), fin_eff, F.col("periodo_id"), F.col("id"))
    )

    previous = ordered.rowsBetween(Window.unboundedPreceding, -1)

    df = (
        df
        .withColumn("_fin_eff", fin_eff)
        .withColumn("_max_fin_prev", F.max("_fin_eff").over(previous))
        .withColumn(
            "_nuevo_grupo",
            F.when(
                F.col("_max_fin_prev").isNull()
                | (F.col("inicio_ts") >= F.col("_max_fin_prev")),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "_grupo",
            F.sum("_nuevo_grupo").over(
                ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    group = Window.partitionBy("distribuidora_id", "elemento_id", "_grupo")

    return (
        df
        .withColumn("n_grupo_solape", F.count(F.lit(1)).over(group))
        .withColumn("en_solape", F.col("n_grupo_solape") > F.lit(1))
        .withColumn(
            "grupo_solape_id",
            F.when(
                F.col("en_solape"),
                F.concat_ws(
                    "-",
                    F.col("distribuidora_id").cast("string"),
                    F.col("elemento_id"),
                    F.col("_grupo").cast("string"),
                ),
            ),
        )
        .drop("_fin_eff", "_max_fin_prev", "_nuevo_grupo", "_grupo")
    )


def add_flags(df: DataFrame) -> DataFrame:
    duracion_ts = (
        F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts")
    )

    nivel = (
        F.when(F.col("abonado_id").isNotNull(), F.lit("ABONADO"))
        .when(F.col("acometida_id").isNotNull(), F.lit("ACOMETIDA"))
        .when(F.col("salida_id").isNotNull(), F.lit("SALIDA"))
        .otherwise(F.lit("CT"))
    )

    same_event_other_period = Window.partitionBy(
        "distribuidora_id", "elemento_id", "salida_id", "acometida_id",
        "abonado_id", "inicio_ts", "duracion_s",
    )

    desc_upper = F.upper(F.coalesce(F.col("descripcion"), F.lit("")))

    return (
        df
        # Level of the interruption, same priority Calser applies in the
        # application (Abonado > Acometida > Salida > CT). It is a computed
        # property in Calser, there is no column for it.
        .withColumn("nivel_afectacion", nivel)
        .withColumn(
            "tipo_evento_familia",
            F.regexp_extract(F.col("tipo_evento_id"), r"^([A-Z]+)_", 1),
        )
        .withColumn("duracion_calculada_s", duracion_ts)
        .withColumn("duracion_cero", F.coalesce(F.col("duracion_s"), F.lit(-1)) == F.lit(0))
        .withColumn(
            "es_microcorte",
            (F.col("duracion_s") > F.lit(0))
            & (F.col("duracion_s") <= F.lit(MICROCUT_MAX_SECONDS)),
        )
        .withColumn(
            "duracion_extrema",
            F.coalesce(F.col("duracion_s") > F.lit(EXTREME_DURATION_SECONDS), F.lit(False)),
        )
        .withColumn(
            "duracion_incoherente",
            F.coalesce(
                F.abs(duracion_ts - F.col("duracion_s")) > F.lit(DURATION_TOLERANCE_SECONDS),
                F.lit(False),
            ),
        )
        .withColumn("sin_fin", F.col("fin_ts").isNull())
        .withColumn("sin_incidencia", F.col("incidencia_id").isNull())
        .withColumn("periodo_mes", F.date_format("inicio_ts", "yyyyMM"))
        .withColumn(
            "periodo_incoherente",
            F.col("periodo_id").rlike(r"^\d{6}$")
            & (F.col("periodo_id") != F.col("periodo_mes")),
        )
        .withColumn(
            "duplicado_otro_periodo",
            F.size(F.collect_set("periodo_id").over(same_event_other_period)) > F.lit(1),
        )
        # Origin of the record. Calser writes "SCADA" in the description of
        # the interruptions it imports from TedisNet and "INCOMPLETA" in the
        # orphan ones the operator had to close by hand.
        .withColumn("es_origen_scada", desc_upper.contains("SCADA"))
        .withColumn("es_incompleta", desc_upper.startswith("INCOMPLETA"))
    )


def transform(df_in: DataFrame, cts: DataFrame, incidencias: DataFrame):
    df = select_columns(df_in)

    df = orphan_condition(
        df,
        cts.select("distribuidora_id", "periodo_id", F.col("id").alias("elemento_id")),
        ["distribuidora_id", "periodo_id", "elemento_id"],
        "_sin_ct",
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("periodo_id").isNull()
         | F.col("elemento_id").isNull()),
        ("NO_TIMESTAMP", F.col("inicio_ts").isNull()),
        ("INVALID_INTERVAL", (F.col("fin_ts") < F.col("inicio_ts"))
         | (F.col("duracion_s") < F.lit(0))),
        ("ORPHAN_FK", F.col("_sin_ct")),
    ])

    kept = kept.drop("_sin_ct")

    kept, dup_id = keep_first(
        kept,
        ["distribuidora_id", "periodo_id", "id"],
        [F.col("ts").desc_nulls_last()],
        "DUPLICATE_ID",
    )

    # Natural key. The level columns are part of it: two outputs of the same
    # CT cut at the same minute for the same time are two interruptions.
    # The copy with an incident is preferred, then the first registered.
    natural_key = [
        "distribuidora_id", "periodo_id", "elemento_id", "salida_id",
        "acometida_id", "abonado_id", "inicio_ts", "duracion_s",
    ]

    kept, dup_natural = keep_first(
        kept,
        natural_key,
        [F.col("incidencia_id").isNull().asc(), F.col("id").asc()],
        "DUPLICATE_NATURAL_KEY",
    )

    kept = orphan_condition(
        kept,
        incidencias.select(
            "distribuidora_id", "periodo_id", F.col("id").alias("incidencia_id")
        ),
        ["distribuidora_id", "periodo_id", "incidencia_id"],
        "incidencia_inexistente",
    ).withColumn(
        "incidencia_inexistente",
        F.col("incidencia_id").isNotNull() & F.col("incidencia_inexistente"),
    )

    out = add_overlap_groups(add_flags(kept)).withColumn(
        "audit_loaded_at", F.current_timestamp()
    )

    return out, union_rejects([rejected, dup_id, dup_natural])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_interrupcion")
    dq = DQCollector(spark, "job_silver_f_interrupcion", args.run_id)

    df_in = read_calser_union(spark, "interrupciones")
    total_in = df_in.count()

    cts = spark.table(require_table(spark, silver_table("d_ct")))
    incidencias = spark.table(require_table(spark, silver_table("f_incidencia")))

    out, rejected = transform(df_in, cts, incidencias)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected, out, FLAGS)

    for row in (
        out.groupBy("source_database")
        .agg(
            F.count(F.lit(1)).alias("filas"),
            F.countDistinct("grupo_solape_id").alias("grupos_solape"),
            F.min("inicio_ts").alias("desde"),
            F.max("inicio_ts").alias("hasta"),
        )
        .collect()
    ):
        dq.add(ENTITY, "filas", row["filas"], ambito=row["source_database"],
               detalle=f"{row['desde']} - {row['hasta']}")
        dq.add(ENTITY, "grupos_solape", row["grupos_solape"], ambito=row["source_database"])

    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
