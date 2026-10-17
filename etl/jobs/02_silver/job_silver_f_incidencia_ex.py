"""
Silver fact of the Extremadura module of Calser: what the distribuidora
reports to the Junta de Extremadura about every incident (incidencias_ex)
and how it learnt about it (incidencias_metodos_recogida_ex).

The three databases have the module (1.752 of 2.019 incidents in Distribuidora 1,
and most of Distribuidora 2 and Distribuidora 3). What it adds to f_incidencia:

- casuistica: the regional cause code (21 force majeure, 23 generation,
  25 transport, 28 other distribuidora, 29 private installations,
  30 intentional acts or accidents of third parties, 31 general strikes,
  32 atmospheric, 33 external agents, 34 internal, 35 unknown, 36 not
  classifiable), with its name. Before 2016 almost every incident has 35,
  the default value: it is kept as it is and causas_incidencia decides how
  far to trust it.
- instalacion_origen / codigo_referencia_origen: the installation where the
  incident started (a support, a switch, a CT, a feeder...), and
  instalacion_cabecera / codigo_cabecera, the feeder head that cleared it.
  They are filled from 2016 and in 72 % of the incidents from 2020.
- nivel_tension (AT, MT, BT, MTBT), tipo_dia (working day or holiday),
  plan_mejora, the number of interruptions, the contracted and installed
  power and the customers affected, as declared.
- metodos_recogida: how it was detected (4 telecontrol, 5 remote signalling,
  6 manual action, 7 customer service), several per incident, with the flags
  detectada_telecontrol and aviso_cliente.

Rejects: no key (NULL_KEY), repeated key (DUPLICATE_ID, keeps one row).
Flag: sin_incidencia when the incident is not in f_incidencia.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from causas_incidencia import CASUISTICA, METODO_RECOGIDA, NIVEL_TENSION, TIPO_DIA
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


ENTITY = "f_incidencia_ex"
TARGET_TABLE = silver_table(ENTITY)
INCIDENTS_TABLE = silver_table("f_incidencia")

FLAGS = ["sin_incidencia", "detectada_telecontrol", "aviso_cliente"]

KEYS = ["distribuidora_id", "periodo_id", "id"]


def code_map(mapping: dict):
    """Spark map literal of a {code: name} dictionary."""
    return F.create_map(*[item for code, name in mapping.items() for item in (F.lit(code), F.lit(name))])


def code_name(mapping: dict, column):
    """Name of the code, null when the code is not in the dictionary (ANSI mode safe)."""
    return F.try_element_at(code_map(mapping), column)


def column_or_null(df: DataFrame, name: str, cast: str = None):
    column = F.col(name) if name in df.columns else F.lit(None)
    return column.cast(cast) if cast else column.cast("string")


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("distribuidora_id"),
        F.col("source_database"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_PERIODO_ID")).alias("periodo_id"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_ID")).alias("id"),
        column_or_null(df, "INCIDENCIA_EX_TIPO_CASUISTICA", "int").alias("casuistica"),
        column_or_null(df, "INCIDENCIA_EX_NIVEL_TENSION", "int").alias("nivel_tension_codigo"),
        column_or_null(df, "INCIDENCIA_EX_TIPO_DIA", "int").alias("tipo_dia_codigo"),
        column_or_null(df, "INCIDENCIA_EX_PLAN_MEJORA", "int").alias("plan_mejora"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_INSTALACION_CABECERA")).alias("instalacion_cabecera"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_CODIGO_CABECERA")).alias("codigo_cabecera"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_INSTALACION_ORIGEN")).alias("instalacion_origen"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_CODIGO_REFERENCIA_ORIGEN")).alias("codigo_referencia_origen"),
        clean_str(column_or_null(df, "INCIDENCIA_EX_NUMERO_SOLICITUD")).alias("numero_solicitud"),
        column_or_null(df, "INCIDENCIA_EX_NUMERO_INTERRUPCIONES", "int").alias("numero_interrupciones"),
        column_or_null(df, "INCIDENCIA_EX_POTENCIA_CONTRATADA", "double").alias("potencia_contratada"),
        column_or_null(df, "INCIDENCIA_EX_POTENCIA_INSTALADA", "double").alias("potencia_instalada"),
        column_or_null(df, "INCIDENCIA_EX_NUMERO_CLIENTES_CTD", "int").alias("clientes_ctd"),
        column_or_null(df, "INCIDENCIA_EX_NUMERO_CLIENTES_CTP", "int").alias("clientes_ctp"),
    )


def collection_methods(methods: DataFrame) -> DataFrame:
    """One row per incident with its collection methods, sorted and joined with '+'."""
    if methods is None:
        return None

    df = methods.select(
        F.col("distribuidora_id"),
        clean_str(column_or_null(methods, "PERIODO_ID")).alias("periodo_id"),
        clean_str(column_or_null(methods, "INCIDENCIA_ID")).alias("id"),
        column_or_null(methods, "METODO_RECOGIDA_EX_ID", "int").alias("metodo"),
    ).where(F.col("id").isNotNull() & F.col("metodo").isNotNull())

    name = F.coalesce(code_name(METODO_RECOGIDA, F.col("metodo")), F.col("metodo").cast("string"))
    return df.groupBy(*KEYS).agg(
        F.concat_ws("+", F.array_sort(F.collect_set(name))).alias("metodos_recogida"),
        F.max((F.col("metodo") == 4).cast("int")).cast("boolean").alias("detectada_telecontrol"),
        F.max((F.col("metodo") == 7).cast("int")).cast("boolean").alias("aviso_cliente"),
    )


def transform(ex: DataFrame, methods: DataFrame, incidents: DataFrame = None):
    df = select_columns(ex)

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("id").isNull() | F.col("periodo_id").isNull()),
    ])
    kept, duplicated = keep_first(kept, KEYS, [F.col("casuistica").desc_nulls_last()], "DUPLICATE_ID")

    kept = (
        kept
        .withColumn("casuistica_nombre", code_name(CASUISTICA, F.col("casuistica")))
        .withColumn("nivel_tension", code_name(NIVEL_TENSION, F.col("nivel_tension_codigo")))
        .withColumn("tipo_dia", code_name(TIPO_DIA, F.col("tipo_dia_codigo")))
    )

    summary = collection_methods(methods)

    if summary is not None:
        kept = kept.join(summary, KEYS, "left")
    else:
        kept = (kept.withColumn("metodos_recogida", F.lit(None).cast("string"))
                .withColumn("detectada_telecontrol", F.lit(None).cast("boolean"))
                .withColumn("aviso_cliente", F.lit(None).cast("boolean")))

    if incidents is not None:
        known = incidents.select(*KEYS).distinct().withColumn("_known", F.lit(True))
        kept = kept.join(known, KEYS, "left").withColumn("sin_incidencia", F.col("_known").isNull()).drop("_known")
    else:
        kept = kept.withColumn("sin_incidencia", F.lit(None).cast("boolean"))

    out = (
        kept
        .withColumn("detectada_telecontrol", F.coalesce(F.col("detectada_telecontrol"), F.lit(False)))
        .withColumn("aviso_cliente", F.coalesce(F.col("aviso_cliente"), F.lit(False)))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    return out, union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_incidencia_ex")
    dq = DQCollector(spark, "job_silver_f_incidencia_ex", args.run_id)

    ex = read_calser_union(spark, "incidencias_ex")
    methods = read_calser_union(spark, "incidencias_metodos_recogida_ex", required=False)
    incidents = spark.table(INCIDENTS_TABLE) if spark.catalog.tableExists(INCIDENTS_TABLE) else None
    total_in = ex.count()

    out, rejected = transform(ex, methods, incidents)
    out = out.localCheckpoint(eager=True)

    write_table(out, TARGET_TABLE)
    rejected = write_rejected(rejected, ENTITY, args.run_id)

    dq.add_entity_counts(ENTITY, total_in, out.count(), rejected, out, FLAGS)

    for row in out.groupBy("casuistica_nombre").count().collect():
        dq.add(ENTITY, f"casuistica:{row['casuistica_nombre']}", row["count"], total_in)

    dq.flush()
    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
