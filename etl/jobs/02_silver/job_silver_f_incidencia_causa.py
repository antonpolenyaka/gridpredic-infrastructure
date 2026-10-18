"""
Silver: the cause of every Calser incident of the three distribuidoras, read
with causas_incidencia from everything Calser keeps about it.

Inputs, by incident:
- f_incidencia: classification, factor, equipment type, description,
  observations and resolution, start.
- f_incidencia_ex: the regional cause code (casuistica) and the installation
  where it started (when the module has the incident).
- f_interrupcion: the level of its interruptions (CT, feeder, service
  connection or customer), how many CTs and the longest one, and their
  descriptions; the technical ones (SCADA, INCOMPLETA, FECHA ... MODIFICADA,
  no captadas) are left out.
- interrupciones_ex (Bronze, the three databases): the reasons and the works
  of the planned interruptions.

Output l2_silver.f_incidencia_causa, one row per incident: causa, subcausa,
componente and origen, the rule that decided (regla), where the cause comes
from (fuente_causa: texto, codigo_oficial, clasificacion, factor or ninguna),
meteo_en_texto and maniobra_reparacion, the level of the incident
(nivel_afectacion) and the text the rules read (texto_causa), so any result
can be checked against what the operator wrote.

The rules are in causas_incidencia.py, documented and tested without Spark.
A few thousand incidents fit in the driver: the classification runs in
pandas and only the keys and the result go back to Spark.

One flag needs every distribuidora at once (shared_start): an unplanned
incident that starts in the same second as one of another distribuidora is
marked simultanea_otra_red. The cause stays the one of Calser: on 17.10.2026
those incidents were read as failures of a common provider upstream, and on
18.10.2026 that reading was withdrawn (a general cause, a storm, can hit the
networks of different providers at once, and the operator's cause is the
better record), so the flag only says that the start was shared.
"""

import re

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from causas_incidencia import CAUSAS, classify, normalize
from silver_common import (
    DQCollector,
    clean_str,
    get_spark,
    logger,
    parse_args,
    read_calser_union,
    require_table,
    silver_table,
    write_table,
)


ENTITY = "f_incidencia_causa"
TARGET_TABLE = silver_table(ENTITY)
INCIDENTS_TABLE = silver_table("f_incidencia")
EX_TABLE = silver_table("f_incidencia_ex")
INTERRUPTIONS_TABLE = silver_table("f_interrupcion")

KEYS = ["distribuidora_id", "periodo_id", "id"]

# Descriptions of interruptions written by the import, not by a person.
TECHNICAL_TEXT = re.compile(r"^(incompleta )?scada( fecha (final|inicio) modificada)?$|^no captadas$|^parte$|^str$")

TEXT_OUTPUTS = ["causa", "subcausa", "componente", "origen", "regla", "fuente_causa", "casuistica_nombre",
                "nivel_afectacion", "texto_causa"]
FLAG_OUTPUTS = ["meteo_en_texto", "maniobra_reparacion", "simultanea_otra_red"]


def interruption_summary(interr: DataFrame, interr_ex: DataFrame = None) -> DataFrame:
    """Level, size and texts of the interruptions of every incident."""
    i = interr.where(F.col("incidencia_id").isNotNull()).select(
        "distribuidora_id", "periodo_id", F.col("id").alias("int_id"), "incidencia_id", "descripcion", "duracion_s",
        "elemento_id", "salida_id", "acometida_id", "abonado_id",
    )

    if interr_ex is not None and "INT_EX_ID" in interr_ex.columns:
        ex = interr_ex.select(
            "distribuidora_id",
            clean_str("INT_EX_PERIODO_ID").alias("periodo_id"),
            clean_str("INT_EX_ID").alias("int_id"),
            clean_str("INT_EX_MOTIVOS").alias("motivos"),
            clean_str("INT_EX_TRABAJOS_REALIZAR").alias("trabajos"),
        ).dropDuplicates(["distribuidora_id", "periodo_id", "int_id"])
        i = i.join(ex, ["distribuidora_id", "periodo_id", "int_id"], "left")
    else:
        i = i.withColumn("motivos", F.lit(None).cast("string")).withColumn("trabajos", F.lit(None).cast("string"))

    level = (
        F.when(F.col("abonado_id").isNotNull(), "abonado")
        .when(F.col("acometida_id").isNotNull(), "acometida")
        .when(F.col("salida_id").isNotNull(), "salida")
        .otherwise("ct")
    )
    i = i.withColumn("nivel", level)

    return (
        i.groupBy("distribuidora_id", "periodo_id", F.col("incidencia_id").alias("id"))
        .agg(
            F.count(F.lit(1)).alias("n_interrupciones"),
            *[F.sum((F.col("nivel") == name).cast("int")).alias(f"n_int_{name}")
              for name in ("ct", "salida", "acometida", "abonado")],
            F.countDistinct("elemento_id").alias("n_elementos"),
            F.max("duracion_s").alias("dur_int_max_s"),
            F.sum((F.col("duracion_s") > 180).cast("int")).alias("n_int_mas_180s"),
            F.array_distinct(F.collect_list("descripcion")).alias("int_descripciones"),
            F.array_distinct(F.collect_list("motivos")).alias("motivos"),
            F.array_distinct(F.collect_list("trabajos")).alias("trabajos"),
        )
    )


def joined_text(values, technical=False) -> str:
    if values is None or (isinstance(values, float) and pd.isna(values)):
        return ""

    out = []
    for value in list(values):
        text = normalize(value)
        if text and not (technical and TECHNICAL_TEXT.match(text)) and text not in out:
            out.append(text)
    return " ".join(out)


def level_of(row) -> str:
    for name in ("ct", "salida", "acometida", "abonado"):
        value = row.get(f"n_int_{name}")
        if value is not None and not pd.isna(value) and value > 0:
            return name
    return "sin_interrupciones"


def classify_frame(pdf: pd.DataFrame) -> pd.DataFrame:
    """causas_incidencia.classify for every row of the joined frame."""
    rows = []

    for row in pdf.to_dict("records"):
        record = {
            "clasificacion_id": row.get("clasificacion_id"),
            "factor_id": row.get("factor_id"),
            "tipo_equipo_id": row.get("tipo_equipo_id"),
            "casuistica": row.get("casuistica"),
            "nivel": level_of(row),
            "fecha": str(row.get("inicio_ts") or "")[:10],
            "descripcion": row.get("descripcion"),
            "observaciones": row.get("observaciones"),
            "resolucion": row.get("resolucion"),
            "instalacion_origen": row.get("instalacion_origen"),
            "instalacion_cabecera": row.get("instalacion_cabecera"),
            "int_descripciones": joined_text(row.get("int_descripciones"), technical=True),
            "motivos": joined_text(row.get("motivos")),
            "trabajos": joined_text(row.get("trabajos")),
        }
        result = classify(record)
        result["nivel_afectacion"] = record["nivel"]
        result["texto_causa"] = " | ".join(
            t for t in (normalize(record["descripcion"]), normalize(record["observaciones"]),
                        normalize(record["resolucion"]), record["int_descripciones"], record["motivos"],
                        record["trabajos"], normalize(record["instalacion_origen"])) if t
        )[:1000]
        result.update({key: row[key] for key in KEYS})
        rows.append(result)

    out = pd.DataFrame(rows, columns=KEYS + TEXT_OUTPUTS + FLAG_OUTPUTS)
    for column in FLAG_OUTPUTS:
        out[column] = out[column].fillna(False).astype(bool)
    return out


def shared_start(pdf: pd.DataFrame, out: pd.DataFrame) -> pd.DataFrame:
    """
    out (one row per row of pdf, in the same order) with simultanea_otra_red: true for the unplanned incidents
    that start in the same second as one of another distribuidora (the storm of 28.01.2026 at 08:35 in the
    Distribuidora 1 and the 2, the blackout of 28.04.2025). The cause does not change: it is a flag to look at
    those days together, not a verdict on who failed.
    """
    starts = pd.to_datetime(pdf["inicio_ts"], errors="coerce").dt.floor("s")
    unplanned = pdf["clasificacion_id"].astype(str).str.strip().str.upper().eq("CL_IMPRE") & starts.notna()
    frame = pd.DataFrame({"distribuidora_id": pdf["distribuidora_id"], "inicio": starts})[unplanned]
    networks = frame.groupby("inicio")["distribuidora_id"].nunique()
    shared = starts.isin(set(networks[networks >= 2].index))
    out = out.copy()
    out["simultanea_otra_red"] = (unplanned & shared).to_numpy().astype(bool)
    return out


def build(incidents: DataFrame, ex: DataFrame, summary: DataFrame) -> DataFrame:
    base = incidents.select(
        *KEYS, "source_database", "inicio_ts", "fin_ts", "duracion_s", "clasificacion_id", "factor_id",
        "tipo_equipo_id", "descripcion", "observaciones", "resolucion",
    )

    if ex is not None:
        base = base.join(
            ex.select(*KEYS, "casuistica", "instalacion_origen", "instalacion_cabecera", "nivel_tension", "tipo_dia",
                      "metodos_recogida"),
            KEYS, "left")
    else:
        for name in ("casuistica", "instalacion_origen", "instalacion_cabecera", "nivel_tension", "tipo_dia",
                     "metodos_recogida"):
            base = base.withColumn(name, F.lit(None).cast("int" if name == "casuistica" else "string"))

    base = base.join(summary, KEYS, "left")

    text_columns = [*KEYS, "inicio_ts", "clasificacion_id", "factor_id", "tipo_equipo_id", "casuistica",
                    "descripcion", "observaciones", "resolucion", "instalacion_origen", "instalacion_cabecera",
                    "n_int_ct", "n_int_salida", "n_int_acometida", "n_int_abonado", "int_descripciones", "motivos",
                    "trabajos"]
    pdf = base.select(*text_columns).toPandas()
    result = shared_start(pdf, classify_frame(pdf))

    spark = incidents.sparkSession
    schema = StructType(
        [StructField(key, incidents.schema[key].dataType, True) for key in KEYS]
        + [StructField(name, StringType(), True) for name in TEXT_OUTPUTS]
        + [StructField(name, BooleanType(), True) for name in FLAG_OUTPUTS]
    )
    classified = spark.createDataFrame(result, schema=schema)

    return (
        base.drop("int_descripciones", "motivos", "trabajos", "descripcion", "observaciones", "resolucion")
        .join(classified, KEYS, "left")
        .withColumn("anio", F.year("inicio_ts"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    spark = get_spark("job-silver-f_incidencia_causa")
    dq = DQCollector(spark, "job_silver_f_incidencia_causa", args.run_id)

    incidents = spark.table(require_table(spark, INCIDENTS_TABLE))
    interruptions = spark.table(require_table(spark, INTERRUPTIONS_TABLE))
    ex = spark.table(EX_TABLE) if spark.catalog.tableExists(EX_TABLE) else None
    interr_ex = read_calser_union(spark, "interrupciones_ex", required=False)

    out = build(incidents, ex, interruption_summary(interruptions, interr_ex))
    out = out.localCheckpoint(eager=True)
    write_table(out, TARGET_TABLE)

    total = out.count()
    dq.add(ENTITY, "filas_salida", total, total)
    counts = {row["causa"]: row["count"] for row in out.groupBy("causa").count().collect()}
    for name in CAUSAS:
        dq.add(ENTITY, f"causa:{name}", counts.get(name, 0), total)

    recent = out.where((F.col("anio") >= 2016) & (F.col("nivel_afectacion") == "ct"))
    recent_total = recent.count()
    dq.add(ENTITY, "sin_causa_desde_2016_ct", recent.where(F.col("causa") == "sin_causa").count(), recent_total,
           umbral_pct=5.0, detalle="incidencias de CT desde 2016 sin ninguna causa en texto ni en codigo")
    dq.add(ENTITY, "causa_por_codigo_oficial", out.where(F.col("fuente_causa") == "codigo_oficial").count(), total)
    dq.add(ENTITY, "simultanea_otra_red", out.where(F.col("simultanea_otra_red")).count(), total,
           detalle="incidencias imprevistas que empiezan en el mismo segundo que una de otra distribuidora")

    dq.flush()
    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
