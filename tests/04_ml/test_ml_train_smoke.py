"""
End to end test of the training job on a local Spark, without Docker.

Builds a small synthetic l3_gold.dataset_train (two distribuidoras, three
splits, a training sample with peso_muestra) and its row in dataset_versions,
runs job_ml_train with small grids and --evaluar-test, and checks every
output table and the saved models.

Requirements (same versions as the Spark image):
    pip install pyspark==4.2.0 delta-spark==4.4.0 pytest pandas pyarrow scikit-learn xgboost-cpu
Run from the root of the repository:
    python -m pytest tests/04_ml -q
"""

import json
import os
import pickle
import shutil
import sys
import tempfile
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

VERSION = "ds_test_ml"
RATE = 0.3
FEATURES = [
    "hist_interr_365d", "hist_interr_90d", "hist_interr_30d", "ev_defecto_tierra_24h",
    "med_intensidad_max", "ct_tipo_zona", "ct_tiene_medidas", "hora_del_dia",
]
PERIODS = {
    "train": ("2024-11-01", 50),
    "valid": ("2025-06-01", 25),
    "test": ("2026-01-05", 20),
}


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="ml_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("ml-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sql("CREATE DATABASE IF NOT EXISTS l3_gold")
    yield session
    session.stop()


def synthetic_dataset(seed=3) -> pd.DataFrame:
    """
    30 CTs, hourly rows in three periods. The risk of a CT grows with its
    history and with the earth faults of the last 24 h; a risky hour starts
    an episode of three positive hours, as y_1_3h does around a real start.
    """
    rng = np.random.default_rng(seed)
    cts = pd.DataFrame({
        "distribuidora_id": np.repeat([1, 2], 15),
        "ct_id": [f"CT{i:03d}" for i in range(30)],
        "ct_tipo_zona": rng.integers(1, 5, 30).astype(float),
        "ct_tiene_medidas": rng.integers(0, 2, 30).astype(float),
        "hist_interr_365d": rng.poisson(2.0, 30).astype(float),
    })

    frames = []

    for split, (start, days) in PERIODS.items():
        hours = pd.date_range(start, periods=days * 24, freq="h")
        grid = cts.merge(pd.DataFrame({"hora": hours}), how="cross")
        grid["split"] = split
        frames.append(grid)

    df = pd.concat(frames, ignore_index=True).sort_values(["ct_id", "hora"]).reset_index(drop=True)
    n = len(df)

    df["hist_interr_90d"] = np.minimum(df["hist_interr_365d"], rng.poisson(0.5, n))
    df["hist_interr_30d"] = np.minimum(df["hist_interr_90d"], rng.poisson(0.2, n))
    df["ev_defecto_tierra_24h"] = rng.poisson(0.1, n).astype(float)
    df["med_intensidad_max"] = np.where(df["ct_tiene_medidas"] == 1, rng.normal(100, 15, n), np.nan)
    df["hora_del_dia"] = df["hora"].dt.hour.astype(float)

    logit = -8.0 + 0.5 * df["hist_interr_365d"] + 2.5 * df["ev_defecto_tierra_24h"]
    start = rng.random(n) < 1 / (1 + np.exp(-logit))

    y = np.zeros(n, dtype=int)
    lead = np.full(n, np.nan)
    same_ct = df["ct_id"].to_numpy()

    for i in np.flatnonzero(start):
        for back, hours_left in ((0, 1.5), (1, 2.5), (2, 3.0)):
            j = i - back
            if j >= 0 and same_ct[j] == same_ct[i]:
                y[j] = 1
                lead[j] = hours_left

    df["y_1_3h"] = y
    df["y_1_3h_local"] = y * (rng.random(n) < 0.3)
    df["horas_hasta_proximo_evento"] = lead

    draw = rng.random(n)
    df["en_muestra_train"] = (df["split"] == "train") & ((df["y_1_3h"] == 1) | (draw < RATE))
    df["peso_muestra"] = np.where((df["split"] == "train") & (df["y_1_3h"] == 0), 1 / RATE, 1.0)
    df["dataset_version"] = VERSION
    df["fecha_mes"] = df["hora"].dt.to_period("M").dt.to_timestamp().dt.date
    return df


@pytest.fixture(scope="module")
def dataset(spark):
    pdf = synthetic_dataset()
    (
        spark.createDataFrame(pdf)
        .withColumn("distribuidora_id", F.col("distribuidora_id").cast("int"))
        .write.format("delta").mode("overwrite").partitionBy("split", "fecha_mes")
        .saveAsTable("l3_gold.dataset_train")
    )

    sizes = {
        split: {"filas": int(len(g)), "positivos": int(g["y_1_3h"].sum())}
        for split, g in pdf.groupby("split")
    }
    version = pd.DataFrame([{
        "dataset_version": VERSION,
        "run_id": "test",
        "creado_ts": datetime(2026, 10, 10, 6, 0, 0),
        "params_hash": "x",
        "params_json": json.dumps({"dataset": {"tasa_negativos_train": RATE}}),
        "etiqueta_principal": "y_1_3h",
        "ventana_desde": "2024-11-01",
        "ventana_hasta": "2026-01-25",
        "n_features": len(FEATURES),
        "features_json": json.dumps(FEATURES),
        "versiones_delta_json": "{}",
        "filas_json": json.dumps(sizes),
    }])
    spark.createDataFrame(version).write.format("delta").mode("overwrite").saveAsTable("l3_gold.dataset_versions")
    return pdf


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)

    config["n_jobs"] = 2
    config["muestra_valid"]["tasa_negativos"] = 0.5
    config["busqueda"] = {
        "logistica": [{"C": 0.1}, {"C": 1.0}],
        "random_forest": [{"n_estimators": 20, "max_depth": 6, "min_samples_leaf": 5}],
        "xgboost": [
            {"n_estimators": 60, "max_depth": 3, "early_stopping_rounds": 10, "verbose": 0},
            {"n_estimators": 60, "max_depth": 4, "early_stopping_rounds": 10, "verbose": 0},
        ],
    }
    config["evaluacion"].update({"presupuestos_alertas_dia": [2, 5], "filas_shap": 500, "test_hasta": "2026-01-20"})
    config["artefactos_uri"] = os.path.join(workdir, "modelos")
    config["spark_conf"] = {}

    path = os.path.join(workdir, "config_ml_test.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)

    return path


@pytest.fixture(scope="module")
def run(spark, dataset, config_path):
    import job_ml_train

    return job_ml_train.main(["--run-id", "ml_test", "--config", config_path, "--evaluar-test"])


def test_run_row(spark, run):
    row = spark.table("l3_gold.ml_runs").where(F.col("run_id") == run).collect()
    assert len(row) == 1

    row = row[0]
    assert row["dataset_version"] == VERSION
    assert row["splits_evaluados"] == "valid,test"

    models = json.loads(row["modelos_json"])
    assert set(models) == {"tasa_base", "naif_historico", "logistica", "random_forest", "xgboost"}
    assert "mejor_iteracion" in models["xgboost"]

    search = json.loads(row["busqueda_json"])
    assert len([s for s in search if s["modelo"] == "logistica"]) == 2
    assert len([s for s in search if s["modelo"] == "xgboost"]) == 2


def test_predictions_cover_valid_and_test(spark, dataset, run):
    predictions = spark.table("l3_gold.ml_predicciones").where(F.col("run_id") == run)
    counts = {r["split"]: r["count"] for r in predictions.groupBy("split").count().collect()}

    assert counts["valid"] == int((dataset["split"] == "valid").sum())
    assert counts["test"] == int((dataset["split"] == "test").sum())

    sample = predictions.select("p_xgboost", "p_logistica", "p_random_forest").toPandas()
    assert sample.notna().all().all()
    assert ((sample >= 0) & (sample <= 1)).all().all()


def test_metrics(spark, dataset, run):
    metrics = spark.table("l3_gold.ml_metricas").where(F.col("run_id") == run).toPandas()
    total = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == "y_1_3h")]
    pr_auc = total[total["metrica"] == "pr_auc"].set_index(["split", "modelo"])["valor"]

    valid = dataset[dataset["split"] == "valid"]
    assert pr_auc[("valid", "tasa_base")] == pytest.approx(valid["y_1_3h"].mean(), rel=1e-6)

    # The history explains only part of the synthetic risk: the naive
    # reference is above the base rate, the trained models well above.
    assert pr_auc[("valid", "naif_historico")] > pr_auc[("valid", "tasa_base")]

    for model in ("logistica", "random_forest", "xgboost"):
        assert pr_auc[("valid", model)] > 1.5 * pr_auc[("valid", "tasa_base")], model

    # test_hasta leaves the last days of test out of the evaluation.
    test = dataset[(dataset["split"] == "test") & (dataset["hora"] < "2026-01-20")]
    n_test = total[(total["split"] == "test") & (total["metrica"] == "pr_auc")]["n_filas"].iloc[0]
    assert n_test == len(test)

    segments = set(metrics["segmento"])
    assert {"total", "distribuidora", "tipo_zona", "telemetria"} <= segments
    assert {"y_1_3h", "y_1_3h_local"} <= set(metrics["etiqueta"])
    assert "recall_episodios@5" in set(metrics["metrica"])
    assert "antelacion_media_h@5" in set(metrics["metrica"])

    recall = total[(total["split"] == "valid") & (total["metrica"] == "recall_episodios@5")]
    assert recall["valor"].between(0, 1).all()


def test_importance(spark, run):
    importance = spark.table("l3_gold.ml_importancia").where(F.col("run_id") == run).toPandas()
    metrics = set(zip(importance["modelo"], importance["metrica"]))

    assert ("xgboost", "shap_medio_abs") in metrics
    assert ("xgboost", "gain") in metrics
    assert ("random_forest", "importancia_impureza") in metrics
    assert ("logistica", "coef_abs") in metrics
    assert ("feature_sola", "pr_auc_univariante") in metrics

    shap = importance[(importance["modelo"] == "xgboost") & (importance["metrica"] == "shap_medio_abs")]
    top = shap.sort_values("valor", ascending=False)["feature"].head(2).tolist()
    assert "ev_defecto_tierra_24h" in top or "hist_interr_365d" in top


def test_saved_models(workdir, run):
    folder = os.path.join(workdir, "modelos", run)
    files = set(os.listdir(folder))

    assert {"xgboost.pkl", "random_forest.pkl", "logistica.pkl", "run.json", "xgboost_booster.json"} <= files

    with open(os.path.join(folder, "xgboost.pkl"), "rb") as file:
        scorer = pickle.load(file)

    X = np.zeros((3, len(FEATURES)), dtype=np.float32)
    assert scorer.score(X).shape == (3,)


def test_wrong_dataset_version_is_refused(spark, dataset, config_path):
    import job_ml_train

    with pytest.raises(ValueError):
        job_ml_train.main(["--config", config_path, "--dataset-version", "ds_que_no_existe"])
