"""
Gold training dataset: labels + features, filtered and split in time, frozen
as a version.

- dataset_train: one row per (distribuidora_id, ct_id, hora) of the study,
  with every label and every feature. Rows where the CT is already without
  supply (en_corte) or where the SCADA history has no samples (scada_activo
  = 0) are left out, as the configuration says (dataset.excluir_*): in both
  cases the label carries no information about the features.
- split: train before dataset.train_hasta, valid before
  dataset.valid_hasta, test after. Never random: the model is evaluated on
  the same CTs in the future, which is the real use. The CTs are not split.
  The last dataset.purga_horas hours before each boundary (by default the
  longest horizon of the labels, 6 h) are split purga: their labels look
  into the next period, so the same interruption would be a positive on
  both sides. They are kept to be counted, never to train or evaluate.
- en_muestra_train / peso_muestra: all the positives of y_1_3h in train and
  a deterministic sample of the negatives (dataset.tasa_negativos_train, by a
  hash of the key and the seed), with weight 1 / rate so the probabilities
  can be corrected. Valid and test are not sampled. Using the sample is a
  decision of the training step; the column only makes it reproducible.
- dataset_versions: one row per build with the parameters, the Delta
  versions of every input table, the size and the positives per split. Its
  id is what goes to MLflow with the model and to the cards of docs/.
- feature_metadata gets the share of nulls of every feature in train.
"""

import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from gold_common import (
    DQCollector,
    batches,
    delta_version,
    drop_table,
    get_spark,
    gold_table,
    load_config,
    logger,
    parse_args,
    params_hash,
    require_table,
    silver_table,
    to_datetime,
    ts_lit,
    write_table,
)


TARGET_TABLE = gold_table("dataset_train")
VERSIONS_TABLE = gold_table("dataset_versions")
METADATA_TABLE = gold_table("feature_metadata")

KEYS = ["distribuidora_id", "ct_id", "hora"]
MAIN_LABEL = "y_1_3h"

LINEAGE = [
    gold_table("dim_ct"),
    gold_table("map_tag_ct"),
    gold_table("map_aguas_arriba"),
    gold_table("fact_interrupciones_mt"),
    gold_table("fact_cortes_scada"),
    gold_table("labels_ct_hora"),
    gold_table("agg_medida_hora"),
    gold_table("actividad_scada_hora"),
    gold_table("features_ct_hora"),
    silver_table("d_ct"),
    silver_table("d_ct_scada"),
    silver_table("d_tag"),
    silver_table("f_interrupcion"),
    silver_table("f_incidencia"),
    silver_table("f_tag_interval_value"),
    silver_table("f_tag_value_change"),
    silver_table("f_evento"),
    silver_table("f_corte_evento"),
    silver_table("f_corte_elemento"),
]

VERSIONS_SCHEMA = StructType([
    StructField("dataset_version", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("creado_ts", TimestampType(), False),
    StructField("params_hash", StringType(), False),
    StructField("params_json", StringType(), False),
    StructField("etiqueta_principal", StringType(), False),
    StructField("ventana_desde", StringType(), False),
    StructField("ventana_hasta", StringType(), False),
    StructField("n_features", LongType(), False),
    StructField("features_json", StringType(), False),
    StructField("versiones_delta_json", StringType(), False),
    StructField("filas_json", StringType(), False),
])


def purge_hours(params: dict) -> int:
    """
    Hours left out before each boundary of the split: dataset.purga_horas,
    or the end of the longest horizon of the labels.
    """
    configured = params["dataset"].get("purga_horas")

    if configured is not None:
        return int(configured)

    return max(int(b) for _, b in params["horizontes"].values())


def split_expr(params: dict):
    """
    A row is train only if its whole label window (hora, hora + purge] ends
    by train_hasta; the same for valid and valid_hasta.
    """
    ds = params["dataset"]
    purge = timedelta(hours=purge_hours(params))
    train_end = to_datetime(ds["train_hasta"])
    valid_end = to_datetime(ds["valid_hasta"])

    hora = F.col("hora")

    return (
        F.when(hora <= ts_lit(train_end - purge), F.lit("train"))
        .when(hora < ts_lit(train_end), F.lit("purga"))
        .when(hora <= ts_lit(valid_end - purge), F.lit("valid"))
        .when(hora < ts_lit(valid_end), F.lit("purga"))
        .otherwise(F.lit("test"))
    )


def build_batch(labels, features, feature_names: list, params: dict, lo, hi, version: str):
    ds = params["dataset"]
    rate = float(ds["tasa_negativos_train"])

    in_batch = (F.col("hora") >= ts_lit(lo)) & (F.col("hora") < ts_lit(hi))

    label_columns = [c for c in labels.columns if c.startswith("y_")] + [
        "en_corte", "en_corte_calser", "en_corte_scada", "horas_hasta_proximo_evento", "era",
    ]

    df = labels.where(in_batch).select(*KEYS, *label_columns).join(
        features.where(in_batch).select(*KEYS, *feature_names),
        KEYS,
        "inner",
    )

    if ds.get("excluir_en_corte", True):
        df = df.where(F.col("en_corte") == 0)

    if ds.get("excluir_scada_inactivo", True):
        df = df.where(F.col("scada_activo") == 1)

    draw = (
        F.abs(F.xxhash64(F.col("distribuidora_id"), F.col("ct_id"), F.col("hora"), F.lit(int(ds["semilla"]))))
        % F.lit(1_000_000)
    ) / F.lit(1_000_000.0)

    return (
        df.withColumn("split", split_expr(params))
        .withColumn(
            "en_muestra_train",
            (F.col("split") == F.lit("train")) & ((F.col(MAIN_LABEL) == 1) | (draw < F.lit(rate))),
        )
        .withColumn(
            "peso_muestra",
            F.when(
                (F.col("split") == F.lit("train")) & (F.col(MAIN_LABEL) == 0),
                F.lit(1.0 / rate),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn("dataset_version", F.lit(version))
        .withColumn("fecha_mes", F.trunc("hora", "month").cast("date"))
    )


def main():
    args = parse_args()
    config = load_config(args.config)
    params = config["parametros"]
    spark = get_spark("job-gold-dataset_train")
    dq = DQCollector(spark, "job_gold_dataset_train", args.run_id)

    labels = spark.table(require_table(spark, gold_table("labels_ct_hora")))
    features = spark.table(require_table(spark, gold_table("features_ct_hora")))
    metadata = spark.table(require_table(spark, METADATA_TABLE))

    feature_names = [row["feature"] for row in metadata.select("feature").collect()]
    missing = [name for name in feature_names if name not in features.columns]

    if missing:
        raise ValueError(f"feature_metadata lists columns that features_ct_hora does not have: {missing}")

    created = datetime.now(timezone.utc).replace(tzinfo=None)
    p_hash = params_hash(params)
    version = f"ds_{created:%Y%m%dT%H%M%S}_{p_hash}"

    logger.info("Building %s with %s features", version, len(feature_names))

    # The dataset is a frozen version: it is always built over the whole
    # window and the first batch overwrites the previous one (which stays in
    # the Delta history until a VACUUM). --rebuild drops it instead.
    args.desde = None
    args.hasta = None

    if args.rebuild:
        drop_table(spark, TARGET_TABLE)

    for index, (lo, hi) in enumerate(batches(args, params, params["lotes"]["dataset_meses"])):
        logger.info("Dataset %s - %s", lo, hi)
        out = build_batch(labels, features, feature_names, params, lo, hi, version)
        write_table(
            out,
            TARGET_TABLE,
            partition_by=["split", "fecha_mes"],
            replace_where=None if index == 0 else (
                f"hora >= TIMESTAMP'{lo:%Y-%m-%d %H:%M:%S}' AND hora < TIMESTAMP'{hi:%Y-%m-%d %H:%M:%S}'"
            ),
        )

    dataset = spark.table(TARGET_TABLE)

    sizes = {
        row["split"]: {
            "filas": row["filas"],
            "positivos": row["positivos"],
            "en_muestra_train": row["muestra"],
            "cts": row["cts"],
            "desde": str(row["desde"]),
            "hasta": str(row["hasta"]),
        }
        for row in dataset.groupBy("split").agg(
            F.count(F.lit(1)).alias("filas"),
            F.sum(MAIN_LABEL).alias("positivos"),
            F.sum(F.col("en_muestra_train").cast("int")).alias("muestra"),
            F.countDistinct("distribuidora_id", "ct_id").alias("cts"),
            F.min("hora").alias("desde"),
            F.max("hora").alias("hasta"),
        ).collect()
    }

    versions = {table: delta_version(spark, table) for table in LINEAGE}

    row = (
        version,
        args.run_id,
        created,
        p_hash,
        json.dumps(params, ensure_ascii=False, sort_keys=True),
        MAIN_LABEL,
        str(to_datetime(params["ventana"]["desde"])),
        str(to_datetime(params["ventana"]["hasta"])),
        len(feature_names),
        json.dumps(feature_names, ensure_ascii=False),
        json.dumps(versions, ensure_ascii=False),
        json.dumps(sizes, ensure_ascii=False),
    )

    (
        spark.createDataFrame([row], VERSIONS_SCHEMA)
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(VERSIONS_TABLE)
    )

    # Share of nulls of every feature in the training split.
    train = dataset.where(F.col("split") == F.lit("train"))
    total_train = train.count()

    if total_train:
        nulls = train.select(*[
            F.sum(F.col(name).isNull().cast("long")).alias(name) for name in feature_names
        ]).first().asDict()

        pct = spark.createDataFrame(
            [(name, round(100.0 * (nulls[name] or 0) / total_train, 4)) for name in feature_names],
            "feature string, pct_nulos_train double",
        )

        updated = (
            metadata.drop("pct_nulos_train", "dataset_version", "audit_loaded_at")
            .join(pct, "feature", "left")
            .withColumn("dataset_version", F.lit(version))
            .withColumn("audit_loaded_at", F.current_timestamp())
            .localCheckpoint(eager=True)
        )

        write_table(updated, METADATA_TABLE)

    for split, size in sizes.items():
        dq.add("dataset_train", "filas", size["filas"], ambito=split, detalle=f"{size['desde']} - {size['hasta']}")
        dq.add("dataset_train", f"positivos:{MAIN_LABEL}", size["positivos"], size["filas"], ambito=split)
        dq.add("dataset_train", "en_muestra_train", size["en_muestra_train"], size["filas"], ambito=split)

    dq.add("dataset_train", "features", len(feature_names), detalle=version)
    dq.flush()

    logger.info("Gold completed: %s version %s", TARGET_TABLE, version)


if __name__ == "__main__":
    main()
