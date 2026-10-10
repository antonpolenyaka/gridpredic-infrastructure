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
from pyspark.sql import SparkSession
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


def load_train(spark, features: list, label: str) -> dict:
    df = (
        spark.table(DATASET_TABLE)
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


def load_valid_sample(spark, features: list, label: str, rate: float, seed: int) -> dict:
    draw = (
        F.abs(F.xxhash64(F.col("distribuidora_id"), F.col("ct_id"), F.col("hora"), F.lit(int(seed))))
        % F.lit(1_000_000)
    ) / F.lit(1_000_000.0)

    df = (
        spark.table(DATASET_TABLE)
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

def score_split(spark, split: str, features: list, scorers: dict, labels: list, segment_columns: list,
                run_id: str, version: str):
    """
    Scores every row of a split on the executors and writes the scores to
    ml_predicciones. Returns the DataFrame of the written rows.
    """
    dataset = spark.table(DATASET_TABLE)
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

    data = load_train(spark, features, label)
    valid_cfg = config.get("muestra_valid", {})
    data.update(load_valid_sample(spark, features, label, float(valid_cfg.get("tasa_negativos", 0.02)),
                                  int(valid_cfg.get("semilla", seed + 1))))
    data["feature_names"] = features

    scorers, search = train_models(names, config, data, rate, base_rate, args.sin_busqueda)
    importance = examine(scorers, data, config)

    meta = {
        "run_id": args.run_id,
        "dataset_version": dataset_version,
        "etiqueta": label,
        "features": features,
        "tasa_negativos_muestra": rate,
        "modelos": {name: {"params": s.params, **s.info} for name, s in scorers.items()},
    }
    artifacts = save_artifacts(scorers, config.get("artefactos_uri"), args.run_id, meta, spark)

    # Free the matrices before scoring: the driver now only collects scores.
    data.clear()

    splits = ["valid"] + (["test"] if args.evaluar_test else [])
    all_metrics = []

    for split in splits:
        written = score_split(spark, split, features, scorers, labels, segment_columns, args.run_id, dataset_version)
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
