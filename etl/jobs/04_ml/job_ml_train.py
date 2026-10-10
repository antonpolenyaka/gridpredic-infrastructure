"""
Training and evaluation of the outage model (horizon 1 - 3 h per CT) on
l3_gold.dataset_train, following docs/model_card.md.

Steps:

1. Dataset version: the one of config_ml.json (dataset_version) or the
   latest of l3_gold.dataset_versions. dataset_train must hold that version,
   otherwise the job stops: the metrics always refer to a frozen dataset.
2. Training matrix: the training sample of Gold (split train,
   en_muestra_train: every positive and a share r of the negatives) is
   collected to the driver as float32. About 0.9 million rows by 246
   features fit in memory; the 43 million rows of train do not, and the
   sample is the reason the column exists.
3. Validation sample: every positive of valid and a deterministic share of
   the negatives (muestra_valid), with weight 1 / share. It is used for the
   early stopping of XGBoost and to choose the configuration of every model
   (busqueda) by its weighted PR-AUC, never with random cross validation.
4. Models: the references tasa_base and naif_historico and the trained
   models logistica, random_forest and xgboost (ml_models.py).
5. Scoring: the chosen models are broadcast to the executors and every row
   of valid (and of test with --evaluar-test) is scored with mapInPandas.
   The scores go to l3_gold.ml_predicciones (partitioned by run_id and
   split), so the notebooks can study them without training again.
6. Metrics (ml_metrics.py): PR-AUC, ROC-AUC, lift and the alerts under a
   budget of N per day and distribuidora (precision, recall by rows and by
   episodes, lead time), for the principal label and the local one, by
   distribuidora, zone type and telemetry. Long table l3_gold.ml_metricas.
7. Examination: mean absolute SHAP of XGBoost on the validation sample, gain,
   impurity importance of the forest, coefficients of the logistic
   regression and the PR-AUC of every feature alone. A feature that ranks
   the positives too well alone is flagged (alerta_leakage): it has to be
   checked before trusting the model. Table l3_gold.ml_importancia.
8. Run: one row in l3_gold.ml_runs with the dataset version, the parameters
   and results of the search and where the models were saved
   (artefactos_uri/<run_id>/).

The test split is scored only with --evaluar-test. It is meant to be used
once, with the configuration already chosen on valid; a second look at test
turns it into a validation set.

Launch it from the Spark master container (the driver needs memory and the
cores of the container; it is not an Airflow task because a long training in
the Airflow container starves its scheduler):

    docker compose exec spark-master bash /app/jobs/04_ml/train.sh
    docker compose exec spark-master bash /app/jobs/04_ml/train.sh --evaluar-test
"""

import argparse
import json
import logging
import os
import pickle
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, StructField, StructType

HERE = os.path.dirname(os.path.abspath(__file__))

if HERE not in sys.path:
    sys.path.insert(0, HERE)

import ml_metrics  # noqa: E402
import ml_models  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ml")

GOLD = "l3_gold"
DATASET_TABLE = f"{GOLD}.dataset_train"
VERSIONS_TABLE = f"{GOLD}.dataset_versions"
RUNS_TABLE = f"{GOLD}.ml_runs"
METRICS_TABLE = f"{GOLD}.ml_metricas"
IMPORTANCE_TABLE = f"{GOLD}.ml_importancia"
PREDICTIONS_TABLE = f"{GOLD}.ml_predicciones"

KEYS = ["distribuidora_id", "ct_id", "hora"]
LEAD = "horas_hasta_proximo_evento"

CONFIG_CANDIDATES = [
    "/app/config/04_ml/config_ml.json",
    "/opt/airflow/config/04_ml/config_ml.json",
    os.path.join(HERE, "..", "..", "config", "04_ml", "config_ml.json"),
]


# ---------------------------------------------------------------------------
# Arguments, configuration and session
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--dataset-version", default=None, help="Version of dataset_versions, latest by default")
    parser.add_argument("--modelos", default=None, help="Comma separated subset of the models of the configuration")
    parser.add_argument("--sin-busqueda", action="store_true", help="Only the first configuration of every model")
    parser.add_argument("--evaluar-test", action="store_true", help="Score and evaluate the test split too")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = (
            "ml_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
        )

    return args


def load_config(path: str = None) -> dict:
    for candidate in [path] if path else CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                config = json.load(file)

            logger.info("ML configuration read from %s", os.path.abspath(candidate))
            return config

    raise FileNotFoundError(f"config_ml.json not found in {CONFIG_CANDIDATES}. Pass --config")


def get_spark(config: dict) -> SparkSession:
    builder = SparkSession.builder.appName("job-ml-train")

    # Executor settings can still be given here; the memory of the driver is
    # fixed by spark-submit (train.sh) because its JVM is already running.
    for key, value in config.get("spark_conf", {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # The executors unpickle the models: they need these modules.
    for module in ("ml_models.py", "ml_metrics.py"):
        spark.sparkContext.addPyFile(os.path.join(HERE, module))

    return spark


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def resolve_version(spark, requested: str = None) -> dict:
    if not spark.catalog.tableExists(VERSIONS_TABLE):
        raise ValueError(f"{VERSIONS_TABLE} does not exist: build the Gold dataset first")

    versions = spark.table(VERSIONS_TABLE)

    if requested:
        rows = versions.where(F.col("dataset_version") == requested).collect()
        if not rows:
            raise ValueError(f"Dataset version {requested} not found in {VERSIONS_TABLE}")
    else:
        rows = versions.orderBy(F.col("creado_ts").desc()).limit(1).collect()

    row = rows[0].asDict()

    present = [r["dataset_version"] for r in spark.table(DATASET_TABLE).select("dataset_version").distinct().collect()]

    if present != [row["dataset_version"]]:
        raise ValueError(
            f"{DATASET_TABLE} holds {present}, not {row['dataset_version']}. Build the dataset again or "
            f"pass the version it holds with --dataset-version"
        )

    return row


def feature_names(version: dict, config: dict, columns: list) -> list:
    names = json.loads(version["features_json"])
    excluded = set(config.get("excluir_features", []))
    missing = [n for n in names if n not in columns]

    if missing:
        raise ValueError(f"dataset_train has no columns {missing}")

    return [n for n in names if n not in excluded]


def sample_rate(version: dict) -> float:
    params = json.loads(version["params_json"])
    return float(params["dataset"]["tasa_negativos_train"])


def feature_columns(features: list) -> list:
    # Aliased so a feature used also as a segment (ct_tipo_zona) can be
    # passed through with its own type in the same select.
    return [F.col(name).cast("float").alias(f"__f{i}") for i, name in enumerate(features)]


def to_matrix(pdf: pd.DataFrame, n_features: int) -> np.ndarray:
    return pdf[[f"__f{i}" for i in range(n_features)]].to_numpy(dtype=np.float32, na_value=np.nan)


def load_train(dataset, features: list, label: str) -> dict:
    df = (
        dataset
        .where((F.col("split") == "train") & F.col("en_muestra_train"))
        .select(F.col(label).cast("int").alias("_y"), F.col("peso_muestra").cast("double").alias("_w"),
                *feature_columns(features))
    )

    start = time.time()
    pdf = df.toPandas()
    X = to_matrix(pdf, len(features))
    y = pdf["_y"].to_numpy(dtype=np.int8)
    w = pdf["_w"].to_numpy(dtype=np.float64)
    logger.info("Training sample: %s rows, %s positives, %.0f s", len(y), int(y.sum()), time.time() - start)
    return {"X": X, "y": y, "w": w}


def load_valid_sample(dataset, features: list, label: str, rate: float, seed: int) -> dict:
    draw = (
        F.abs(F.xxhash64(F.col("distribuidora_id"), F.col("ct_id"), F.col("hora"), F.lit(int(seed))))
        % F.lit(1_000_000)
    ) / F.lit(1_000_000.0)

    df = (
        dataset
        .where((F.col("split") == "valid") & ((F.col(label) == 1) | (draw < F.lit(rate))))
        .select(
            F.col(label).cast("int").alias("_y"),
            F.when(F.col(label) == 1, F.lit(1.0)).otherwise(F.lit(1.0 / rate)).alias("_w"),
            *feature_columns(features),
        )
    )

    start = time.time()
    pdf = df.toPandas()
    X = to_matrix(pdf, len(features))
    y = pdf["_y"].to_numpy(dtype=np.int8)
    w = pdf["_w"].to_numpy(dtype=np.float64)
    logger.info("Validation sample: %s rows, %s positives, %.0f s", len(y), int(y.sum()), time.time() - start)
    return {"Xv": X, "yv": y, "wv": w}


# ---------------------------------------------------------------------------
# State of the network
# ---------------------------------------------------------------------------

CONTEXT_TABLE = f"{GOLD}.ml_contexto_red"
FEATURES_TABLE = f"{GOLD}.features_ct_hora"


def context_sources(features: list, cfg: dict) -> list:
    prefixes = tuple(cfg.get("prefijos", []))
    windows = tuple(f"_{w}" for w in cfg.get("ventanas", []))
    return [f for f in features if f.startswith(prefixes) and f.endswith(windows)]


def network_context(spark, features: list, cfg: dict, run_id: str) -> tuple:
    """
    State of the whole network at every hour, added to every row of the CT:
    for each precursor feature of a short window (value changes of earth
    and phase faults, loss of voltage, trips, reclosures, cuts of the SCADA
    in the last hours), the share of CTs of the distribuidora where it is
    not zero (red_frac_*) and the same share over the three distribuidoras
    (region_frac_*).

    94 % of the interruptions are systemic (a storm, a fault upstream): the
    CT that fails is often one that never failed before, and what announces
    it is that the network around it is already moving. The CT features only
    see its own group and feeder; these see the whole network.

    Computed from features_ct_hora (every CT and hour, also the ones the
    dataset leaves out), so the share does not depend on which CTs were
    already in a cut, which is derived from the labels. Every feature of a
    row uses only data before its hour, and so does a mean of them over the
    CTs of the same hour. Written to ml_contexto_red so the three reads of
    the run (train sample, valid sample, scoring) share one computation.
    """
    sources = context_sources(features, cfg)

    if not sources:
        logger.warning("No feature matches contexto_red, the block is skipped")
        return None, []

    table = FEATURES_TABLE if spark.catalog.tableExists(FEATURES_TABLE) else DATASET_TABLE
    base = spark.table(table)
    sources = [f for f in sources if f in base.columns]

    flags = [(F.coalesce(F.col(f), F.lit(0)) > 0).cast("double").alias(f) for f in sources]
    hourly = base.select("distribuidora_id", "hora", *flags)

    by_distributor = hourly.groupBy("distribuidora_id", "hora").agg(
        *[F.avg(f).alias(f"red_frac_{f}") for f in sources]
    )
    names = [f"red_frac_{f}" for f in sources]

    if cfg.get("region", True):
        region = hourly.groupBy("hora").agg(*[F.avg(f).alias(f"region_frac_{f}") for f in sources])
        by_distributor = by_distributor.join(region, "hora", "left")
        names += [f"region_frac_{f}" for f in sources]

    start = time.time()
    (
        by_distributor.withColumn("run_id", F.lit(run_id))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(CONTEXT_TABLE)
    )
    context = spark.table(CONTEXT_TABLE).drop("run_id")
    logger.info("Network context from %s: %s features from %s sources, %s hours, %.0f s",
                table, len(names), len(sources), context.count(), time.time() - start)
    return context, names


def with_context(dataset, context):
    if context is None:
        return dataset

    return dataset.join(F.broadcast(context), ["distribuidora_id", "hora"], "left")


# ---------------------------------------------------------------------------
# Season and groups of CTs
# ---------------------------------------------------------------------------

CLUSTERS_TABLE = f"{GOLD}.ml_clusters_ct"
CLUSTER_FEATURE = "ct_cluster"
SEASON_FEATURE = "estacion"


def add_season(dataset):
    """
    estacion: 1 winter (December to February), 2 spring, 3 summer, 4
    autumn. mes already carries it for the trees; as its own column it lets
    the season be read in the importances and in the segments, and it is
    what the regulator and the operator talk about.
    """
    month = F.month("hora")
    return dataset.withColumn(
        SEASON_FEATURE,
        F.when(month.isin(12, 1, 2), 1).when(month.isin(3, 4, 5), 2).when(month.isin(6, 7, 8), 3).otherwise(4)
        .cast("double"),
    )


def cluster_cts(spark, dataset, label: str, cfg: dict, run_id: str, seed: int) -> tuple:
    """
    Groups of similar CTs by k-means, so a submodel can be trained for each
    one. Variables (clusters.numericas / categoricas): zone type of the
    municipality, power, customers, outputs, transformers, size of the
    network group, telemetry, voltage, coordinates (a proxy of the weather)
    and distribuidora, plus the interruption rate of the CT in train
    (positives per 10.000 hours). Everything comes from train or is static,
    so the groups do not look at valid or test.

    k is chosen by silhouette among clusters.k, with at least
    clusters.min_cts CTs in the smallest group. Writes ml_clusters_ct and
    returns (DataFrame distribuidora_id, ct_id, ct_cluster; profile of every
    group as pandas).
    """
    numeric = list(cfg.get("numericas", []))
    categorical = list(cfg.get("categoricas", []))
    present = set(dataset.columns)
    numeric = [c for c in numeric if c in present]
    categorical = [c for c in categorical if c in present]

    train = dataset.where(F.col("split") == "train")
    per_ct = train.groupBy("distribuidora_id", "ct_id").agg(
        *[F.avg(F.col(c).cast("double")).alias(c) for c in numeric if c != "distribuidora_id"],
        *[F.max(F.col(c)).alias(c) for c in categorical if c != "distribuidora_id"],
        (F.sum(F.col(label)) * F.lit(10000.0) / F.count(F.lit(1))).alias("tasa_train_10k_h"),
        F.sum(F.col(label)).alias("positivos_train"),
        F.count(F.lit(1)).alias("horas_train"),
    ).toPandas()

    variables = [c for c in numeric if c != "distribuidora_id"]
    if cfg.get("tasa_interrupciones_train", True):
        variables.append("tasa_train_10k_h")

    log = list(cfg.get("log", [])) + ["tasa_train_10k_h"]
    X, columns = ml_models.cluster_matrix(per_ct, variables, log, categorical)
    labels, k, tried = ml_models.fit_clusters(
        X, [int(v) for v in cfg.get("k", [3, 4, 5, 6, 7, 8])], seed, int(cfg.get("min_cts", 20)),
    )
    per_ct[CLUSTER_FEATURE] = labels.astype(int)

    logger.info("Groups of CTs: k = %s of %s (k, silhouette, smallest group): %s", k, len(per_ct), tried)

    profile = per_ct.groupby(CLUSTER_FEATURE).agg(
        cts=("ct_id", "count"),
        positivos_train=("positivos_train", "sum"),
        horas_train=("horas_train", "sum"),
        **{f"{c}_media": (c, "mean") for c in variables if c != "tasa_train_10k_h"},
    )
    profile["tasa_train_10k_h"] = profile["positivos_train"] * 10000.0 / profile["horas_train"]

    for column in categorical:
        mix = per_ct.groupby(CLUSTER_FEATURE)[column].agg(
            lambda v: ", ".join(f"{int(a)}:{n}" for a, n in v.value_counts().sort_index().items())
        )
        profile[f"{column}_reparto"] = mix

    profile = profile.reset_index()

    out = spark.createDataFrame(
        per_ct[["distribuidora_id", "ct_id", CLUSTER_FEATURE, "tasa_train_10k_h", "positivos_train"]]
        .assign(distribuidora_id=lambda d: d["distribuidora_id"].astype("int64"))
    )
    (
        out.withColumn("run_id", F.lit(run_id)).withColumn("k", F.lit(int(k)))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(CLUSTERS_TABLE)
    )

    clusters = spark.table(CLUSTERS_TABLE).select(
        F.col("distribuidora_id").cast(dataset.schema["distribuidora_id"].dataType).alias("distribuidora_id"),
        "ct_id",
        F.col(CLUSTER_FEATURE).cast("double").alias(CLUSTER_FEATURE),
    )
    return clusters, profile, {"k": int(k), "probados": tried, "variables": columns}


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

DIM_CT_TABLE = f"{GOLD}.dim_ct"
WEATHER_TABLE = f"{GOLD}.ml_meteo_celda_hora"
LOCATION_FEATURES = ["ct_lat_municipio", "ct_lon_municipio"]

# Hourly variables of Bronze (Open-Meteo names) and their short names here.
WEATHER_VARIABLES = {
    "wind_gusts_10m": "racha",
    "wind_speed_10m": "viento",
    "precipitation": "lluvia",
    "snowfall": "nieve",
    "temperature_2m": "temp",
    "relative_humidity_2m": "humedad",
    "pressure_msl": "presion",
}


def normalized_name(column):
    """Lower case, no accents, letters only: to match municipality names."""
    plain = F.translate(F.lower(F.trim(column)), "áàäâéèëêíìïîóòöôúùüûñç", "aaaaeeeeiiiioooouuuunc")
    return F.regexp_replace(plain, "[^a-z]", "")


def ct_locations(spark, cfg: dict):
    """
    (distribuidora_id, ct_id, celda_id, ct_lat_municipio, ct_lon_municipio)
    for every CT of dim_ct: its municipality in Calser matched with the
    reference of the weather download by INE code (MUNICIPIO_ID) and, when
    that fails, by name. None when the weather tables are not there.
    """
    cells_table = cfg.get("tabla_celdas", "l1_bronze.meteo_openmeteo_celdas")

    if not (spark.catalog.tableExists(cells_table) and spark.catalog.tableExists(DIM_CT_TABLE)):
        logger.warning("No weather: %s or %s not found", cells_table, DIM_CT_TABLE)
        return None

    ref = spark.table(cells_table).select(
        F.lpad(F.trim(F.col("codigo_ine")), 5, "0").alias("_ine"),
        normalized_name(F.col("municipio")).alias("_nombre"),
        "celda_id",
        F.col("latitud").cast("double").alias("ct_lat_municipio"),
        F.col("longitud").cast("double").alias("ct_lon_municipio"),
    )
    by_ine = ref.dropDuplicates(["_ine"]).drop("_nombre")
    by_name = ref.dropDuplicates(["_nombre"]).drop("_ine")

    dim = spark.table(DIM_CT_TABLE).select(
        "distribuidora_id", "ct_id",
        F.lpad(F.trim(F.col("municipio_id").cast("string")), 5, "0").alias("_ine"),
        normalized_name(F.col("municipio_nombre")).alias("_nombre"),
    ).dropDuplicates(["distribuidora_id", "ct_id"])

    first = dim.join(F.broadcast(by_ine), "_ine", "left")
    second = dim.join(F.broadcast(by_name.select(
        "_nombre",
        F.col("celda_id").alias("_celda_n"),
        F.col("ct_lat_municipio").alias("_lat_n"),
        F.col("ct_lon_municipio").alias("_lon_n"),
    )), "_nombre", "left").select("distribuidora_id", "ct_id", "_celda_n", "_lat_n", "_lon_n")

    located = (
        first.join(second, ["distribuidora_id", "ct_id"], "left")
        .select(
            "distribuidora_id", "ct_id",
            F.coalesce("celda_id", "_celda_n").alias("celda_id"),
            F.coalesce("ct_lat_municipio", "_lat_n").alias("ct_lat_municipio"),
            F.coalesce("ct_lon_municipio", "_lon_n").alias("ct_lon_municipio"),
            F.when(F.col("celda_id").isNotNull(), "ine").when(F.col("_celda_n").isNotNull(), "nombre")
            .otherwise("sin_celda").alias("_cruce"),
        )
    ).toPandas()

    logger.info("CTs located for the weather: %s", located["_cruce"].value_counts().to_dict())
    located = located.drop(columns="_cruce")
    return spark.createDataFrame(located, schema=(
        f"distribuidora_id {spark.table(DIM_CT_TABLE).schema['distribuidora_id'].dataType.simpleString()}, "
        "ct_id string, celda_id string, ct_lat_municipio double, ct_lon_municipio double"
    ))


def with_locations(dataset, locations):
    if locations is None:
        return dataset

    return dataset.join(F.broadcast(locations), ["distribuidora_id", "ct_id"], "left")


def weather_features(spark, cfg: dict, run_id: str) -> tuple:
    """
    Weather of every cell and hour as features, written to
    ml_meteo_celda_hora. A row of Bronze at hora describes the hour that ends
    then, so the row of hora is known at hora, like the rest of the features.

    - met_*: the last hours (gust and wind of the hour, maximum gust of 3, 6
      and 24 h; rain of 1, 3, 6 and 24 h; snow of 24 h; temperature,
      minimum and maximum of 24 h; humidity; pressure and its change in 3
      and 24 h, the passing fronts).
    - metprev_*: the next prevision_h hours (maximum gust, rain and snow).
      The reanalysis is used as a perfect forecast of the next hours: in
      operation that would be the forecast of AEMET or ECMWF, which is good
      at 1 - 3 h. It is an upper bound and it is said so; prevision_h = 0
      leaves it out.
    - met_region_* / metprev_region_*: maximum gust over all the cells, the
      size of the storm.
    """
    hours_table = cfg.get("tabla_horas", "l1_bronze.meteo_openmeteo_hora")

    if not spark.catalog.tableExists(hours_table):
        logger.warning("No weather: %s not found", hours_table)
        return None, []

    raw = spark.table(hours_table)
    present = {k: v for k, v in WEATHER_VARIABLES.items() if k in raw.columns}
    w = raw.select("celda_id", "hora", *[F.col(k).cast("double").alias(v) for k, v in present.items()])
    w = w.withColumn("_t", F.unix_timestamp("hora"))

    def past(hours):
        return Window.partitionBy("celda_id").orderBy("_t").rangeBetween(-(hours * 3600 - 1), 0)

    def ahead(hours):
        return Window.partitionBy("celda_id").orderBy("_t").rangeBetween(1, hours * 3600)

    def at(hours):
        return Window.partitionBy("celda_id").orderBy("_t").rangeBetween(-hours * 3600, -hours * 3600)

    cols, names = [], []

    def add(column, name):
        cols.append(column.alias(name))
        names.append(name)

    if "racha" in present.values():
        add(F.col("racha"), "met_racha_1h")
        for h in (3, 6, 24):
            add(F.max("racha").over(past(h)), f"met_racha_max_{h}h")
    if "viento" in present.values():
        add(F.col("viento"), "met_viento_1h")
    if "lluvia" in present.values():
        add(F.col("lluvia"), "met_lluvia_1h")
        for h in (3, 6, 24):
            add(F.sum("lluvia").over(past(h)), f"met_lluvia_{h}h")
    if "nieve" in present.values():
        add(F.sum("nieve").over(past(24)), "met_nieve_24h")
    if "temp" in present.values():
        add(F.col("temp"), "met_temp_1h")
        add(F.min("temp").over(past(24)), "met_temp_min_24h")
        add(F.max("temp").over(past(24)), "met_temp_max_24h")
    if "humedad" in present.values():
        add(F.col("humedad"), "met_humedad_1h")
    if "presion" in present.values():
        add(F.col("presion"), "met_presion_1h")
        add(F.col("presion") - F.first("presion").over(at(3)), "met_presion_delta_3h")
        add(F.col("presion") - F.first("presion").over(at(24)), "met_presion_delta_24h")

    lead = int(cfg.get("prevision_h", 3))
    if lead > 0:
        if "racha" in present.values():
            add(F.max("racha").over(ahead(lead)), f"metprev_racha_max_{lead}h")
        if "lluvia" in present.values():
            add(F.sum("lluvia").over(ahead(lead)), f"metprev_lluvia_{lead}h")
        if "nieve" in present.values():
            add(F.sum("nieve").over(ahead(lead)), f"metprev_nieve_{lead}h")

    features = w.select("celda_id", "hora", *cols)

    if cfg.get("region", True):
        region_cols = [c for c in ["met_racha_max_3h", f"metprev_racha_max_{lead}h", "met_lluvia_3h"] if c in names]
        def region_name(column):
            if column.startswith("metprev_"):
                return column.replace("metprev_", "metprev_region_", 1)
            return column.replace("met_", "met_region_", 1)

        region = features.groupBy("hora").agg(*[F.max(c).alias(region_name(c)) for c in region_cols])
        region_names = [c for c in region.columns if c != "hora"]
        features = features.join(region, "hora", "left")
        names += region_names

    start = time.time()
    (
        features.withColumn("run_id", F.lit(run_id))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(WEATHER_TABLE)
    )
    table = spark.table(WEATHER_TABLE).drop("run_id")
    logger.info("Weather features: %s columns, %s rows, %.0f s", len(names), table.count(), time.time() - start)
    return table, names


def with_weather(dataset, weather):
    if weather is None:
        return dataset

    return dataset.join(weather, ["celda_id", "hora"], "left")


def with_clusters(dataset, clusters):
    if clusters is None:
        return dataset

    # A CT without rows in train has no group: -1, scored by the global model.
    return dataset.join(F.broadcast(clusters), ["distribuidora_id", "ct_id"], "left").fillna(
        {CLUSTER_FEATURE: -1.0}
    )


def train_cluster_models(scorers: dict, data: dict, cfg: dict, rate: float, seed: int, n_jobs: int) -> list:
    """
    One XGBoost per group with the configuration chosen for the global
    model, early stopping on the rows of the group in the validation sample.
    A group with fewer than submodelos.min_positivos positives in the train
    sample (or none in the validation sample) keeps the global model. Adds
    xgboost_cluster to scorers and returns the comparison per group, global
    model and submodel on the same rows.
    """
    base = scorers["xgboost"]
    column = data["feature_names"].index(CLUSTER_FEATURE)
    groups_train = data["X"][:, column]
    groups_valid = data["Xv"][:, column]
    min_positives = int(cfg.get("min_positivos", 200))
    params = dict(base.params)
    models = {}
    rows = []

    for group in sorted(set(groups_train.tolist())):
        tr = groups_train == group
        va = groups_valid == group
        pos_train = int(data["y"][tr].sum())
        pos_valid = int(data["yv"][va].sum())
        global_ap = ml_metrics.average_precision(data["yv"][va], base.score(data["Xv"][va]), data["wv"][va])
        row = {"modelo": "xgboost_cluster", "config": int(group), "params": {"grupo": int(group), **params},
               "positivos_train": pos_train, "positivos_valid": pos_valid,
               "pr_auc_global_en_grupo": global_ap}

        if group < 0 or pos_train < min_positives or pos_valid == 0:
            row["pr_auc_valid_muestra"] = global_ap
            row["submodelo"] = False
            rows.append(row)
            logger.info("Group %s: %s positives in train, %s in valid, keeps the global model", group, pos_train, pos_valid)
            continue

        sub = {"X": data["X"][tr], "y": data["y"][tr], "w": data["w"][tr],
               "Xv": data["Xv"][va], "yv": data["yv"][va], "wv": data["wv"][va],
               "feature_names": data["feature_names"]}
        model = ml_models.fit_model("xgboost", params, sub, rate, seed, n_jobs)
        ap = ml_metrics.average_precision(data["yv"][va], model.score(data["Xv"][va]), data["wv"][va])
        models[group] = model
        row.update({"pr_auc_valid_muestra": ap, "submodelo": True, **model.info})
        rows.append(row)
        logger.info("Group %s: submodel PR-AUC %.5f against %.5f of the global model on its rows (%s positives in train)",
                    group, ap, global_ap, pos_train)

    scorer = ml_models.ClusterScorer("xgboost_cluster", data["feature_names"], CLUSTER_FEATURE, models, base)
    scorer.params = {"grupos_con_submodelo": sorted(int(g) for g in models), **params}
    scorer.info = {"grupos": len(rows), "submodelos": len(models)}
    scorers["xgboost_cluster"] = scorer
    return rows


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_models(names: list, config: dict, data: dict, rate: float, base_rate: float, only_first: bool) -> tuple:
    """
    Returns ({model: Scorer}, [search rows]). Every configuration of a model
    is fitted on the training sample and scored on the validation sample;
    the one with the best weighted PR-AUC is kept.
    """
    seed = int(config.get("semilla", 0))
    n_jobs = int(config.get("n_jobs", -1))
    use_weights = bool(config.get("usar_peso_muestra", False))
    scorers = {}
    search = []

    for name in names:
        if name in ml_models.REFERENCES:
            scorer = ml_models.build_reference(name, data["feature_names"], base_rate)
            ap = ml_metrics.average_precision(data["yv"], scorer.score(data["Xv"]), data["wv"])
            scorer.info["pr_auc_valid_muestra"] = ap
            search.append({"modelo": name, "config": 0, "params": scorer.params, "pr_auc_valid_muestra": ap})
            logger.info("%s: PR-AUC on the validation sample %.5f", name, ap)
            scorers[name] = scorer
            continue

        grid = config["busqueda"][name]
        grid = grid[:1] if only_first else grid
        best, best_ap = None, -1.0

        for index, params in enumerate(grid):
            logger.info("Fitting %s, configuration %s of %s: %s", name, index + 1, len(grid), params)
            scorer = ml_models.fit_model(name, params, data, rate, seed, n_jobs, use_weights)
            ap = ml_metrics.average_precision(data["yv"], scorer.score(data["Xv"]), data["wv"])
            search.append({"modelo": name, "config": index, "params": params, "pr_auc_valid_muestra": ap,
                           **scorer.info})
            logger.info("%s configuration %s: PR-AUC on the validation sample %.5f (%s)",
                        name, index + 1, ap, scorer.info)

            if ap > best_ap:
                best, best_ap = scorer, ap

        best.info["pr_auc_valid_muestra"] = best_ap
        scorers[name] = best

    return scorers, search


def examine(scorers: dict, data: dict, config: dict) -> pd.DataFrame:
    rows = []
    features = data["feature_names"]
    evaluation = config.get("evaluacion", {})
    limit = int(evaluation.get("filas_shap", 50000))
    threshold = float(evaluation.get("alerta_leakage_pr_auc", 0.3))

    rng = np.random.default_rng(int(config.get("semilla", 0)))
    pick = rng.permutation(len(data["yv"]))[:limit]

    for name, scorer in scorers.items():
        for metric, values in ml_models.importances(scorer, data["Xv"][pick]).items():
            rows += [(name, feature, metric, float(value), False) for feature, value in zip(features, values)]

    univariate = ml_metrics.univariate_ap(data["Xv"], data["yv"], data["wv"])

    for feature, value in zip(features, univariate):
        rows.append(("feature_sola", feature, "pr_auc_univariante", float(value),
                     bool(np.isfinite(value) and value >= threshold)))

    flagged = [f for f, v in zip(features, univariate) if np.isfinite(v) and v >= threshold]

    if flagged:
        logger.warning("Features with a PR-AUC alone of %s or more, check them for leakage: %s", threshold, flagged)

    return pd.DataFrame(rows, columns=["modelo", "feature", "metrica", "valor", "alerta_leakage"])


def save_artifacts(scorers: dict, uri: str, run_id: str, meta: dict, spark) -> str:
    """
    Pickle of every scorer and, for XGBoost, the booster in its own JSON
    format, in uri/run_id/. s3:// goes to MinIO with the endpoint and the
    credentials of the Spark session; anything else is a local folder.
    """
    if not uri:
        return None

    target = f"{uri.rstrip('/')}/{run_id}"
    files = {f"{name}.pkl": pickle.dumps(scorer) for name, scorer in scorers.items()}
    files["run.json"] = json.dumps(meta, ensure_ascii=False, indent=2, default=str).encode("utf-8")

    if "xgboost" in scorers:
        files["xgboost_booster.json"] = bytes(scorers["xgboost"].estimator.get_booster().save_raw("json"))

    try:
        if target.startswith("s3://"):
            import s3fs

            endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint})

            for name, payload in files.items():
                with fs.open(f"{target}/{name}", "wb") as file:
                    file.write(payload)
        else:
            os.makedirs(target, exist_ok=True)

            for name, payload in files.items():
                with open(os.path.join(target, name), "wb") as file:
                    file.write(payload)
    except Exception as error:  # the metrics are worth keeping without the files
        logger.error("Models not saved in %s: %s", target, error)
        return None

    logger.info("Models saved in %s", target)
    return target


# ---------------------------------------------------------------------------
# Scoring and evaluation
# ---------------------------------------------------------------------------

def score_split(spark, dataset, split: str, features: list, scorers: dict, labels: list, segment_columns: list,
                run_id: str, version: str):
    """
    Scores every row of a split on the executors and writes the scores to
    ml_predicciones. Returns the DataFrame of the written rows.
    """
    passthrough = [
        c for c in dict.fromkeys(labels + [LEAD] + segment_columns) if c in dataset.columns and c not in KEYS
    ]

    payload = pickle.dumps({name: scorer.single_thread() for name, scorer in scorers.items()})
    shared = spark.sparkContext.broadcast(payload)
    n_features = len(features)
    model_names = list(scorers)

    key_fields = [dataset.schema[k] for k in KEYS]
    schema = StructType(
        key_fields
        + [StructField(c, DoubleType(), True) for c in passthrough]
        + [StructField(f"p_{m}", FloatType(), True) for m in model_names]
    )

    def score(batches):
        import ml_models as models_module

        models = models_module.cached(run_id, lambda: pickle.loads(shared.value))

        for pdf in batches:
            X = pdf[[f"__f{i}" for i in range(n_features)]].to_numpy(dtype=np.float32, na_value=np.nan)
            out = pdf[KEYS + passthrough].copy()

            for name in model_names:
                out[f"p_{name}"] = models[name].score(X)

            yield out

    scored = (
        dataset.where(F.col("split") == split)
        .select(*KEYS, *[F.col(c).cast("double").alias(c) for c in passthrough], *feature_columns(features))
        .mapInPandas(score, schema)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("dataset_version", F.lit(version))
        .withColumn("split", F.lit(split))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )

    start = time.time()
    writer = scored.write.format("delta").option("mergeSchema", "true")

    if spark.catalog.tableExists(PREDICTIONS_TABLE):
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"run_id = '{run_id}' AND split = '{split}'")
            .saveAsTable(PREDICTIONS_TABLE)
        )
    else:
        writer.mode("overwrite").partitionBy("run_id", "split").saveAsTable(PREDICTIONS_TABLE)

    logger.info("Split %s scored in %.0f s", split, time.time() - start)
    shared.unpersist()

    return spark.table(PREDICTIONS_TABLE).where((F.col("run_id") == run_id) & (F.col("split") == split))


def collect_scores(df, labels: list, segment_columns: list, model_names: list, until: str = None) -> pd.DataFrame:
    if until:
        df = df.where(F.col("hora") < F.lit(until).cast("timestamp"))

    present = [c for c in segment_columns if c in df.columns]
    pdf = df.select(
        "distribuidora_id",
        F.xxhash64("distribuidora_id", "ct_id").alias("ct_key"),
        "hora",
        *[F.col(c).cast("int").alias(c) for c in labels],
        *([F.col(LEAD).cast("float").alias(LEAD)] if LEAD in df.columns else []),
        *[F.col(c).alias(c) for c in present if c != "distribuidora_id"],
        *[F.col(f"p_{m}") for m in model_names],
    ).toPandas()

    return pdf


def add_context(metrics: pd.DataFrame, run_id: str, version: str, split: str) -> pd.DataFrame:
    metrics.insert(0, "split", split)
    metrics.insert(0, "dataset_version", version)
    metrics.insert(0, "run_id", run_id)
    return metrics


def save_frame(spark, pdf: pd.DataFrame, table: str, run_id: str):
    if pdf.empty:
        return

    df = spark.createDataFrame(pdf).withColumn("audit_loaded_at", F.current_timestamp())
    writer = df.write.format("delta").option("mergeSchema", "true")

    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", f"run_id = '{run_id}'").saveAsTable(table)
    else:
        writer.mode("overwrite").saveAsTable(table)


def export_run(config: dict, run_id: str, metrics: pd.DataFrame, importance: pd.DataFrame, search: list,
               label: str, labels: list, budgets: list, profile: pd.DataFrame = None):
    """
    resumen.md (also written to the log), metricas.csv, importancia.csv and
    busqueda.json in exportar_dir/<run_id>/, a local folder of the driver
    container. The launcher of the laptop copies it to _runlogs, so the
    results can be read without Trino.
    """
    budget = 10 if 10 in budgets else budgets[0]
    local = next((c for c in labels if c.endswith("_local")), None)
    report = ml_metrics.report_markdown(metrics, importance, search, label, budget, local, profile=profile)
    logger.info("Summary of run %s\n%s", run_id, report)

    folder = config.get("exportar_dir")

    if not folder:
        return

    try:
        target = os.path.join(folder, run_id)
        os.makedirs(target, exist_ok=True)

        with open(os.path.join(target, "resumen.md"), "w", encoding="utf-8") as file:
            file.write(f"# Ejecucion {run_id}\n\n{report}")

        metrics.to_csv(os.path.join(target, "metricas.csv"), index=False)
        if profile is not None:
            profile.to_csv(os.path.join(target, "grupos_ct.csv"), index=False)
        importance.to_csv(os.path.join(target, "importancia.csv"), index=False)

        with open(os.path.join(target, "busqueda.json"), "w", encoding="utf-8") as file:
            json.dump(search, file, ensure_ascii=False, indent=2, default=str)

        logger.info("Summary files written in %s", target)
    except OSError as error:
        logger.error("Summary files not written in %s: %s", folder, error)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)

    label = config.get("etiqueta", "y_1_3h")
    labels = [label] + [c for c in config.get("etiquetas_evaluacion", []) if c != label]
    evaluation = config.get("evaluacion", {})
    segment_map = evaluation.get("segmentos", {})
    segment_columns = list(segment_map.values())
    budgets = [int(n) for n in evaluation.get("presupuestos_alertas_dia", [10])]
    seed = int(config.get("semilla", 0))

    names = args.modelos.split(",") if args.modelos else config["modelos"]
    unknown = [n for n in names if n not in ml_models.ALL_MODELS]

    if unknown:
        raise ValueError(f"Unknown models {unknown}, known: {ml_models.ALL_MODELS}")

    version = resolve_version(spark, args.dataset_version or config.get("dataset_version"))
    dataset_version = version["dataset_version"]
    columns = spark.table(DATASET_TABLE).columns
    labels = [c for c in labels if c in columns]
    features = feature_names(version, config, columns)
    rate = sample_rate(version)
    sizes = json.loads(version["filas_json"])
    base_rate = sizes["train"]["positivos"] / sizes["train"]["filas"]

    logger.info("Run %s on %s: %s features, label %s, negative rate of the sample %s, base rate %.6f",
                args.run_id, dataset_version, len(features), label, rate, base_rate)

    context_cfg = config.get("contexto_red", {})
    context = None

    if context_cfg.get("activo"):
        context, context_names = network_context(spark, features, context_cfg, args.run_id)
        features = features + context_names

    dataset = with_context(spark.table(DATASET_TABLE), context)

    if config.get("estacion", True):
        dataset = add_season(dataset)
        features = features + [SEASON_FEATURE]

    meteo_cfg = config.get("meteo", {})
    locations = ct_locations(spark, meteo_cfg) if meteo_cfg.get("activo") else None

    if locations is not None:
        dataset = with_locations(dataset, locations)
        features = features + LOCATION_FEATURES

    clusters_cfg = config.get("clusters", {})
    profile, clusters_info = None, None

    if clusters_cfg.get("activo"):
        clusters, profile, clusters_info = cluster_cts(
            spark, with_locations(spark.table(DATASET_TABLE), locations), label, clusters_cfg, args.run_id, seed,
        )
        dataset = with_clusters(dataset, clusters)
        features = features + [CLUSTER_FEATURE]
        segment_map = {**segment_map, "grupo_ct": CLUSTER_FEATURE}
        segment_columns = list(segment_map.values())

    if locations is not None:
        weather, weather_names = weather_features(spark, meteo_cfg, args.run_id)
        dataset = with_weather(dataset, weather)
        features = features + weather_names

    data = load_train(dataset, features, label)
    valid_cfg = config.get("muestra_valid", {})
    data.update(load_valid_sample(dataset, features, label, float(valid_cfg.get("tasa_negativos", 0.02)),
                                  int(valid_cfg.get("semilla", seed + 1))))
    data["feature_names"] = features

    scorers, search = train_models(names, config, data, rate, base_rate, args.sin_busqueda)

    if clusters_cfg.get("activo") and config.get("submodelos", {}).get("activo") and "xgboost" in scorers:
        search += train_cluster_models(scorers, data, config["submodelos"], rate, seed, int(config.get("n_jobs", -1)))
    importance = examine(scorers, data, config)

    meta = {
        "run_id": args.run_id,
        "dataset_version": dataset_version,
        "etiqueta": label,
        "features": features,
        "tasa_negativos_muestra": rate,
        "grupos_ct": clusters_info,
        "modelos": {name: {"params": s.params, **s.info} for name, s in scorers.items()},
    }
    artifacts = save_artifacts(scorers, config.get("artefactos_uri"), args.run_id, meta, spark)

    # Free the matrices before scoring: the driver now only collects scores.
    data.clear()

    splits = ["valid"] + (["test"] if args.evaluar_test else [])
    all_metrics = []

    for split in splits:
        written = score_split(spark, dataset, split, features, scorers, labels, segment_columns, args.run_id,
                              dataset_version)
        until = evaluation.get("test_hasta") if split == "test" else None
        pdf = collect_scores(written, labels, segment_columns, list(scorers), until)
        logger.info("Evaluating %s: %s rows, %s positives", split, len(pdf), int(pdf[label].sum()))

        metrics = ml_metrics.evaluate(
            pdf, [f"p_{m}" for m in scorers], labels, budgets, segment_map, seed=seed,
        )
        all_metrics.append(add_context(metrics, args.run_id, dataset_version, split))
        del pdf

        summary = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label)]
        for metric in ["pr_auc", "roc_auc", f"recall_episodios@{budgets[0]}", f"precision@{budgets[0]}"]:
            values = summary[summary["metrica"] == metric].set_index("modelo")["valor"].round(5).to_dict()
            logger.info("%s %s: %s", split, metric, values)

    metrics = pd.concat(all_metrics, ignore_index=True)
    save_frame(spark, metrics, METRICS_TABLE, args.run_id)
    save_frame(spark, add_context(importance, args.run_id, dataset_version, "valid"), IMPORTANCE_TABLE, args.run_id)

    total = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label)
                    & metrics["metrica"].isin(["pr_auc", "roc_auc"])]
    resumen = {
        f"{row.split}:{row.modelo}:{row.metrica}": row.valor for row in total.itertuples()
    }

    export_run(config, args.run_id, metrics, importance, search, label, labels, budgets, profile)

    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": dataset_version,
        "etiqueta": label,
        "n_features": len(features),
        "modelos_json": json.dumps(meta["modelos"], ensure_ascii=False, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps(config, ensure_ascii=False),
        "resumen_json": json.dumps(resumen, ensure_ascii=False),
        "splits_evaluados": ",".join(splits),
        "artefactos_uri": artifacts or "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)

    logger.info("Run %s completed: %s, %s, %s, %s", args.run_id, RUNS_TABLE, METRICS_TABLE,
                IMPORTANCE_TABLE, PREDICTIONS_TABLE)
    return args.run_id


if __name__ == "__main__":
    main()
