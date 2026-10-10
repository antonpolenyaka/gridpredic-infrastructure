"""
Silver dimension of the weather cells: every municipality of the reference
file with the cell its weather comes from.

- codigo_ine padded to five digits (the INE code of a municipality, as
  MUNICIPIO_ID in Calser) and nombre_normalizado (lower case, no accents,
  letters only) so Gold can match the CTs by code or, when the code
  differs, by name.
- A municipality without coordinates has no cell and is rejected
  (SIN_COORDENADAS); a repeated INE code keeps one row (DUPLICATE_ID).
"""

from pyspark.sql import functions as F

from silver_common import (
    DQCollector,
    get_spark,
    keep_first,
    logger,
    parse_args,
    silver_table,
    split_rejects,
    union_rejects,
    write_rejected,
    write_table,
)


ENTITY = "d_meteo_celda"
TARGET_TABLE = silver_table(ENTITY)
SOURCE_TABLE = "l1_bronze.meteo_openmeteo_celdas"


def normalized_name(column):
    plain = F.translate(F.lower(F.trim(column)), "áàäâéèëêíìïîóòöôúùüûñç", "aaaaeeeeiiiioooouuuunc")
    return F.regexp_replace(plain, "[^a-z]", "")


def transform(df_in):
    df = df_in.select(
        F.lpad(F.trim(F.col("codigo_ine").cast("string")), 5, "0").alias("codigo_ine"),
        F.trim(F.col("municipio")).alias("municipio"),
        normalized_name(F.col("municipio")).alias("nombre_normalizado"),
        F.col("distribuidora_ref").cast("string").alias("distribuidora_ref"),
        F.col("latitud").cast("double").alias("latitud"),
        F.col("longitud").cast("double").alias("longitud"),
        F.trim(F.col("celda_id")).alias("celda_id"),
        F.col("celda_latitud").cast("double").alias("celda_latitud"),
        F.col("celda_longitud").cast("double").alias("celda_longitud"),
    )

    kept, rejected = split_rejects(df, [
        ("NULL_KEY", F.col("codigo_ine").isNull() | (F.col("codigo_ine") == "00000")),
        ("SIN_COORDENADAS", F.col("latitud").isNull() | F.col("longitud").isNull() | F.col("celda_id").isNull()),
    ])
    kept, duplicated = keep_first(kept, ["codigo_ine"], [F.col("municipio")], "DUPLICATE_ID")

    return kept.withColumn("audit_loaded_at", F.current_timestamp()), union_rejects([rejected, duplicated])


def main():
    args = parse_args()
    spark = get_spark("job-silver-d_meteo_celda")
    dq = DQCollector(spark, "job_silver_d_meteo_celda", args.run_id)

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning("%s not found: run the weather Landing and Bronze first. Nothing to do", SOURCE_TABLE)
        return

    df_in = spark.table(SOURCE_TABLE)
    total_in = df_in.count()
    out, rejected = transform(df_in)

    write_table(out, TARGET_TABLE)
    write_rejected(rejected, ENTITY, args.run_id)

    dq.add(ENTITY, "filas_entrada", total_in)
    dq.add(ENTITY, "municipios", out.count(), total_in)
    dq.add(ENTITY, "celdas", out.select("celda_id").distinct().count())
    for row in rejected.groupBy("_motivo").count().collect():
        dq.add(ENTITY, f"rechazo:{row['_motivo']}", row["count"], total_in, umbral_pct=5.0)
    dq.flush()

    logger.info("Silver completed: %s", TARGET_TABLE)


if __name__ == "__main__":
    main()
