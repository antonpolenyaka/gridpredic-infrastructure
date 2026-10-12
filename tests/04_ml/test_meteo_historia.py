"""
Test of the risk hours with the whole weather history (job_ml_meteo_historia)
on a local Spark: seven years of synthetic weather where storms bring
interruptions, background failures and upstream cuts the weather does not
announce, and the blackout day.

    python -m pytest tests/04_ml/test_meteo_historia.py -q
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

import job_ml_meteo_historia as mh  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "mh_test"
CELLS = {1: "c1", 2: "c2"}


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def test_labelled_hours_follow_the_rule_of_the_labels():
    hours = mh.labelled_hours(pd.Series(pd.to_datetime(["2024-05-01 10:30"])), [1, 3])
    assert list(hours) == list(pd.to_datetime(["2024-05-01 08:00", "2024-05-01 09:00"]))
    # A start on the hour: (7 + 1, 7 + 3] holds 10:00, (9 + 1, 9 + 3] does not.
    hours = mh.labelled_hours(pd.Series(pd.to_datetime(["2024-05-01 10:00"])), [1, 3])
    assert list(hours) == list(pd.to_datetime(["2024-05-01 07:00", "2024-05-01 08:00"]))


def test_easter_and_holidays():
    assert mh.easter(2019).isoformat() == "2019-04-21"
    assert mh.easter(2024).isoformat() == "2024-03-31"
    assert mh.easter(2025).isoformat() == "2025-04-20"
    days = {d.isoformat() for d in mh.holidays([2024])}
    assert {"2024-03-28", "2024-03-29", "2024-09-08", "2024-10-12", "2024-12-25"} <= days
    assert "2024-04-01" not in days

    cal = mh.calendar(pd.Series(pd.to_datetime(["2024-03-29 10:00", "2024-04-02 10:00", "2024-04-02 20:00",
                                                "2024-04-06 10:00"])), [8, 18])
    assert cal["cal_laborable"].tolist() == [0.0, 1.0, 1.0, 0.0]
    assert cal["cal_horario_laboral"].tolist() == [0.0, 1.0, 0.0, 0.0]
    assert cal["cal_festivo"].tolist() == [1.0, 0.0, 0.0, 0.0]
    assert cal["estacion"].tolist() == [2.0, 2.0, 2.0, 2.0]


def test_history_counts_only_what_was_known():
    grid = pd.DataFrame({"distribuidora_id": 1,
                         "hora": pd.date_range("2024-01-01", "2024-01-20", freq="h")})
    known = pd.DataFrame({"distribuidora_id": [1, 1, 2],
                          "conocido_ts": pd.to_datetime(["2024-01-03 12:00", "2024-01-05 00:00",
                                                         "2024-01-04 00:00"])})
    names = mh.add_history(grid, known, [7, 30])
    assert names == ["hist_incidencias_7d", "hist_incidencias_30d", "hist_horas_desde_ultima"]
    row = grid.set_index("hora")
    assert row.loc["2024-01-03 11:00", "hist_incidencias_7d"] == 0
    assert row.loc["2024-01-03 12:00", "hist_incidencias_7d"] == 1
    assert row.loc["2024-01-06 00:00", "hist_incidencias_7d"] == 2
    assert row.loc["2024-01-06 00:00", "hist_horas_desde_ultima"] == 24
    # Seven days later the first one leaves the window of 7 days, not the one of 30.
    assert row.loc["2024-01-10 12:00", "hist_incidencias_7d"] == 1
    assert row.loc["2024-01-10 12:00", "hist_incidencias_30d"] == 2
    assert row.loc["2024-01-01 00:00", "hist_horas_desde_ultima"] == 365 * 24


def test_blocks_keep_partial_families_apart():
    assert mh.block_of("metd_max_met_racha_max_3h") == "meteo"
    assert mh.block_of("metd_max_met_cape_max_24h") == "conveccion"
    assert mh.block_of("metd_media_met_li_min_6h") == "conveccion"
    assert mh.block_of("metd_max_met_polvo_24h") == "aire"
    assert mh.block_of("metd_max_met_lluvia_72h") == "antecedente"
    assert mh.block_of("metd_media_met_lluvia_previa_30d") == "antecedente"
    assert mh.block_of("metd_max_met_humedad_max_6h") == "antecedente"
    assert mh.block_of("metd_max_met_lluvia_24h") == "meteo"
    assert mh.block_of("metd_media_met_humedad_1h") == "meteo"
    assert mh.block_of("metd_max_met_humedad_suelo_0_7") == "suelo"
    assert mh.block_of("metd_media_met_dir_viento_sin") == "extra"
    assert mh.block_of("metd_max_met_viento_100m_1h") == "extra"
    assert mh.block_of("metd_max_met_tormenta_6h") == "extra"
    assert mh.block_of("metd_max_met_viento_1h") == "meteo"
    assert mh.block_of("cal_hora_sin") == "calendario"
    assert mh.block_of("estacion") == "calendario"
    assert mh.block_of("hist_incidencias_7d") == "historia"
    assert mh.block_of("dist_2") == "distribuidora"


# ---------------------------------------------------------------------------
# The whole job
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="mh_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("mh-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for db in ("l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()


def save(spark, pdf: pd.DataFrame, table: str):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(table)


def synthetic():
    """(weather per cell and hour, principal events with cause)."""
    rng = np.random.default_rng(7)
    hours = pd.date_range("2019-01-01", "2025-12-31 23:00", freq="h")
    weather, events = [], []

    for dist, cell in CELLS.items():
        gust = rng.gamma(4.0, 6.0, len(hours))
        rain = np.where(rng.random(len(hours)) < 0.05, rng.gamma(1.0, 2.0, len(hours)), 0.0)
        storms = rng.choice(np.arange(48, len(hours) - 48), size=7 * 18, replace=False)
        for s in storms:
            gust[s:s + 6] = rng.uniform(80, 110, 6)
            rain[s:s + 6] = rng.uniform(2, 8, 6)
            if rng.random() < 0.6:
                events.append((dist, hours[s] + pd.Timedelta(minutes=int(rng.integers(60, 300))), "meteorologia"))
        # Background failures and upstream cuts, at any time.
        for _ in range(7 * 12):
            events.append((dist, hours[int(rng.integers(0, len(hours)))] + pd.Timedelta(minutes=20),
                           "equipo" if rng.random() < 0.5 else "aguas_arriba"))
        cape = np.where(hours >= pd.Timestamp("2021-03-23"), np.where(gust > 70, 1500.0, 50.0), np.nan)
        series = pd.Series(gust)
        weather.append(pd.DataFrame({
            "celda_id": cell, "hora": hours, "met_racha_1h": gust,
            "met_racha_max_3h": series.rolling(3, min_periods=1).max().to_numpy(),
            "met_lluvia_3h": pd.Series(rain).rolling(3, min_periods=1).sum().to_numpy(),
            "met_temp_1h": 15 + 10 * np.sin(2 * np.pi * (hours.dayofyear.to_numpy() - 100) / 365),
            "met_cape_1h": cape,
            "metprev_racha_max_3h": series[::-1].rolling(3, min_periods=1).max()[::-1].shift(-1).to_numpy(),
        }))

    # The blackout of 28.04.2025 in both distribuidoras.
    for dist in CELLS:
        events.append((dist, pd.Timestamp("2025-04-28 12:33"), "apagon"))

    return pd.concat(weather, ignore_index=True), events


@pytest.fixture(scope="module")
def tables(spark):
    weather, events = synthetic()
    save(spark, weather, "l3_gold.meteo_celda_hora")
    cells = pd.DataFrame([{"distribuidora_id": d, "ct_id": f"{d}-{i}", "celda_id": c}
                          for d, c in CELLS.items() for i in range(3)])
    save(spark, cells, "l3_gold.map_ct_celda")

    facts, causes = [], []
    for k, (dist, start, cause) in enumerate(events):
        period, inc = start.strftime("%Y%m"), f"E{k}"
        for variant in ("principal", "todas"):
            facts.append({"distribuidora_id": dist, "periodo_id": period, "incidencia_id": inc,
                          "ct_id": f"{dist}-{k % 3}", "inicio_ts": start, "conocido_ts": start + pd.Timedelta(days=2),
                          "variante": variant})
        causes.append({"distribuidora_id": dist, "periodo_id": period, "id": inc, "causa": cause})
    # A planned cut only in todas: never a label.
    facts.append({"distribuidora_id": 1, "periodo_id": "202406", "incidencia_id": "P1", "ct_id": "1-0",
                  "inicio_ts": pd.Timestamp("2024-06-03 09:00"), "conocido_ts": pd.Timestamp("2024-06-04 09:00"),
                  "variante": "todas"})
    save(spark, pd.DataFrame(facts), "l3_gold.fact_interrupciones_mt")
    save(spark, pd.DataFrame(causes), "l2_silver.f_incidencia_causa")
    return events


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config["meteo_historia"] = {
        **config.get("meteo_historia", {}),
        "desde": "2019-01-08",
        "hasta": "2026-01-01",
        "anios_evaluacion": [2024, 2025],
        "bootstrap_reps": 50,
        "busqueda": {
            "logistica": [{"C": 0.1}],
            "xgboost": [{"max_depth": 3, "min_child_weight": 5, "learning_rate": 0.1, "subsample": 0.8,
                         "colsample_bytree": 0.8, "n_estimators": 200, "early_stopping_rounds": 20,
                         "verbose": 0}],
        },
        "variantes": [
            {"nombre": "corta", "desde": "2023-01-08", "bloques": ["meteo", "calendario", "historia"],
             "etiqueta": "principal"},
            {"nombre": "larga", "desde": "2019-01-08", "bloques": ["meteo", "calendario", "historia"],
             "etiqueta": "principal"},
            {"nombre": "larga_conveccion", "desde": "2019-01-08",
             "bloques": ["meteo", "conveccion", "calendario", "historia"], "etiqueta": "principal"},
            {"nombre": "larga_etiqueta_meteo", "desde": "2019-01-08", "bloques": ["meteo", "calendario", "historia"],
             "etiqueta": "meteo"},
            # No dust in the weather of the test: skipped, not a copy of larga.
            {"nombre": "larga_aire", "desde": "2019-01-08", "bloques": ["meteo", "aire"], "etiqueta": "principal"},
        ],
        "comparaciones": [["larga__xgboost", "corta__xgboost"], ["larga__logistica", "corta__logistica"],
                          ["larga__logistica", "larga__naif_racha"], ["larga_etiqueta_meteo__logistica",
                                                                      "larga__logistica"]],
    }
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    return path


@pytest.fixture(scope="module")
def result(spark, tables, config_path, workdir):
    code = mh.main(["--run-id", RUN_ID, "--config", config_path])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def read(folder, name):
    return pd.read_csv(os.path.join(folder, name))


def test_report_and_files(result):
    code, folder = result
    assert code == 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    for title in ("## Variantes", "## Comparación en los años comunes", "## PR-AUC por año",
                  "## Diferencias emparejadas"):
        assert title in text
    for name in ("metricas.csv", "comparacion.csv", "diferencias.csv", "filas.csv", "entreno.csv"):
        assert os.path.exists(os.path.join(folder, name))


def test_dataset_rows_and_labels(result, tables):
    _, folder = result
    rows = read(folder, "filas.csv")
    # The blackout day is out: 2025 has 8760 - 24 hours per distribuidora.
    assert rows.loc[rows["anio"] == 2025, "horas"].tolist() == [8736, 8736]
    assert rows.loc[rows["anio"] == 2024, "horas"].tolist() == [8784, 8784]
    # y_meteo leaves out the upstream cuts: fewer starts than y_principal.
    assert (rows["episodios_y_meteo"] <= rows["episodios_y_principal"]).all()
    assert rows["episodios_y_meteo"].sum() < rows["episodios_y_principal"].sum()


def test_long_history_trains_with_more_positives(result):
    _, folder = result
    trained = read(folder, "entreno.csv")
    year = trained[trained["anio"] == 2024].set_index("variante")
    assert year.loc["larga", "positivas_entreno"] > 3 * year.loc["corta", "positivas_entreno"]


def test_models_beat_the_base_rate(result):
    _, folder = result
    table = read(folder, "comparacion.csv")
    best = table[(table["variante"] == "larga") & (table["modelo"].isin(["logistica", "xgboost"]))]["pr_auc"].max()
    base = table[(table["variante"] == "larga") & (table["modelo"] == "tasa_base")]["pr_auc"].item()
    assert best > 2 * base
    assert {"corta", "larga", "larga_conveccion", "larga_etiqueta_meteo"} == set(table["variante"])
    diffs = read(folder, "diferencias.csv")
    assert len(diffs) == 4


def test_metrics_saved(result, spark):
    rows = spark.table("l3_gold.ml_metricas").where(f"run_id = '{RUN_ID}'").count()
    assert rows > 100
    assert spark.table("l3_gold.ml_runs").where(f"run_id = '{RUN_ID}'").count() == 1
    predictions = spark.table("l3_gold.ml_predicciones_meteo_historia").where(f"run_id = '{RUN_ID}'")
    models = {r["modelo"] for r in predictions.select("modelo").distinct().collect()}
    assert {"larga__xgboost", "corta__logistica", "larga__naif_racha"} <= models
    # Two years of two distribuidoras, without the blackout day, for every model.
    assert predictions.where("modelo = 'larga__xgboost'").count() == 2 * (8784 + 8736)
