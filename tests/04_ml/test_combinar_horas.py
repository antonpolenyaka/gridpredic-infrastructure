"""
Test of the combination of two runs of risk hours (job_ml_combinar_horas) on a
local Spark: two models that see the same storms with independent errors,
whose average has to rank better than each of them.

    python -m pytest tests/04_ml/test_combinar_horas.py -q
"""

import json
import os
import shutil
import sys
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import job_ml_combinar_horas as cmb  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "cmb_test"


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="cmb_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("cmb-test")
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


def save(spark, pdf: pd.DataFrame, table: str):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(table)


@pytest.fixture(scope="module")
def tables(spark):
    rng = np.random.default_rng(3)
    hours = pd.date_range("2023-01-01", "2024-12-31 23:00", freq="h")
    frames = []
    for dist in (2, 3):
        storm = rng.gamma(1.0, 1.0, len(hours))
        y = (rng.random(len(hours)) < 0.002 * np.exp(storm)).astype(int)
        frames.append(pd.DataFrame({"distribuidora_id": dist, "hora": hours, "y": y, "storm": storm}))
    base = pd.concat(frames, ignore_index=True)
    # Same signal, independent errors of the same size.
    signal = base["storm"] + 2.0 * base["y"]
    hr = base[["distribuidora_id", "hora"]].copy()
    hr["y_1_3h"] = base["y"]
    hr["estacion"] = 1.0
    hr["p_xgboost"] = 1 / (1 + np.exp(-(signal + rng.normal(0, 1.5, len(base)) - 6)))
    hr["p_logistica"] = 1 / (1 + np.exp(-(signal + rng.normal(0, 3.0, len(base)) - 6)))
    hr["p_naif_racha"] = base["storm"]
    hr["run_id"] = "hr_test"
    hr["split"] = "cv_" + hr["hora"].dt.year.astype(str)
    # Hours of TedisNet left out of its dataset (a CT already cut): not in the combination.
    hr = hr.iloc[10:]
    save(spark, hr, "l3_gold.ml_predicciones_distribuidora_hora")

    mh = base[["distribuidora_id", "hora"]].copy()
    mh["anio"] = mh["hora"].dt.year
    mh["y_principal"] = base["y"]
    mh["y_meteo"] = base["y"]
    mh["modelo"] = "larga__xgboost"
    mh["p"] = 1 / (1 + np.exp(-(signal + rng.normal(0, 1.5, len(base)) - 6)))
    other = mh.assign(modelo="larga__logistica", p=1 / (1 + np.exp(-(signal + rng.normal(0, 3.0, len(base)) - 6))))
    ignored = mh.assign(modelo="corta__xgboost")
    long = pd.concat([mh, other, ignored], ignore_index=True)
    long["run_id"] = "mh_test"
    save(spark, long, "l3_gold.ml_predicciones_meteo_historia")
    return len(hr)


@pytest.fixture(scope="module")
def result(spark, tables, workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config["combinar"] = {"bootstrap_reps": 50}
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    code = cmb.main(["--run-id", RUN_ID, "--config", path, "--hr-run", "hr_test", "--mh-run", "mh_test"])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def test_combination_beats_its_parts(result, tables):
    code, folder = result
    assert code == 0
    table = pd.read_csv(os.path.join(folder, "comparacion.csv")).set_index("modelo")
    both = "media:hr__xgboost+mh__larga__xgboost"
    assert table.loc[both, "pr_auc"] > table.loc["hr__xgboost", "pr_auc"]
    assert table.loc[both, "pr_auc"] > table.loc["mh__larga__xgboost", "pr_auc"]
    assert "rango:hr__logistica+mh__larga__logistica" in table.index
    assert not any(name.startswith("mh__corta") for name in table.index)
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "coincide con y_principal de la historia en el 100,0 %" in text
    assert f"Horas puntuadas por las dos ejecuciones: {tables}" in text


def test_differences_and_metrics(result, spark):
    _, folder = result
    diffs = pd.read_csv(os.path.join(folder, "diferencias.csv"))
    # Every combination against its two parts and the gust rule, plus the four pairs of single models.
    assert len(diffs) == 8 * 3 + 4
    assert spark.table("l3_gold.ml_metricas").where(f"run_id = '{RUN_ID}'").count() > 50
