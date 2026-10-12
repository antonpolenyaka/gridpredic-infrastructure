"""
Test of the risk of the next day (job_ml_riesgo_dia): daily weather, labels,
warnings known the evening before, and the whole job on a local Spark with
seven years of synthetic weather where storm days bring interruptions and
AEMET warns of most of them the day before.

    python -m pytest tests/04_ml/test_riesgo_dia.py -q
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

import job_ml_riesgo_dia as rd  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "dia_test"
CELLS = {1: "c1", 2: "c2"}


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def test_daily_weather_and_days_with_gaps():
    hours = pd.date_range("2024-01-01 00:00", "2024-01-02 23:00", freq="h")
    hourly = pd.DataFrame({"distribuidora_id": 1, "hora": hours,
                           "metd_max_met_racha_1h": np.arange(48, dtype=float),
                           "metd_max_met_lluvia_1h": 0.5, "metd_media_met_temp_1h": 10.0})
    # The second day keeps only 10 hours of weather: a gap, not a calm day.
    hourly.loc[hourly["hora"] >= "2024-01-02 10:00", "metd_max_met_racha_1h"] = np.nan
    days = rd.daily_weather(hourly, 20).set_index("fecha")
    assert days.loc["2024-01-01", "racha_max"] == 23
    assert days.loc["2024-01-01", "lluvia_max"] == pytest.approx(12.0)
    assert days.loc["2024-01-01", "temp_media_min"] == 10.0
    assert np.isnan(days.loc["2024-01-02", "racha_max"]) and np.isnan(days.loc["2024-01-02", "lluvia_max"])


def test_day_labels_and_calendar():
    grid = pd.DataFrame({"distribuidora_id": [1, 1, 1, 2],
                         "fecha": pd.to_datetime(["2024-03-28", "2024-03-29", "2024-03-30", "2024-03-29"])})
    events = pd.DataFrame({"distribuidora_id": [1, 1, 2],
                           "inicio_ts": pd.to_datetime(["2024-03-28 23:50", "2024-03-30 08:00", "2024-03-29 10:00"]),
                           "causa": ["meteorologia", "aguas_arriba", "equipo"]})
    out = rd.day_labels(grid, events, ["aguas_arriba"])
    assert out["y_dia"].tolist() == [1, 0, 1, 1]
    assert out["y_dia_meteo"].tolist() == [1, 0, 0, 1]

    cal = rd.day_calendar(grid["fecha"])
    # Good Friday 2024 is a holiday; 30.03.2024 is a Saturday.
    assert cal["cal_festivo"].tolist() == [1.0, 1.0, 0.0, 1.0]
    assert cal["cal_fin_de_semana"].tolist() == [0.0, 0.0, 1.0, 0.0]


def test_warnings_count_only_when_sent_the_evening_before():
    cells = pd.DataFrame({"distribuidora_id": [1, 2], "celda_id": ["c1", "c2"]})
    warnings = pd.DataFrame([
        # Sent at 19:00 the day before: it counts for 10.01 (cutoff 20:00).
        {"celda_id": "c1", "fenomeno_codigo": "VI", "nivel_num": 2, "tipo_mensaje": "Alert",
         "enviado_ts": "2024-01-09 19:00", "inicio_ts": "2024-01-10 12:00", "fin_ts": "2024-01-10 23:59:59"},
        # Sent at 22:00 for the next two days: too late for 11.01, in time for 12.01.
        {"celda_id": "c1", "fenomeno_codigo": "PR", "nivel_num": 1, "tipo_mensaje": "Alert",
         "enviado_ts": "2024-01-10 22:00", "inicio_ts": "2024-01-11 06:00", "fin_ts": "2024-01-13 00:00:00"},
        # A cancellation and a green level never count.
        {"celda_id": "c1", "fenomeno_codigo": "VI", "nivel_num": 3, "tipo_mensaje": "Cancel",
         "enviado_ts": "2024-01-09 10:00", "inicio_ts": "2024-01-10 00:00", "fin_ts": "2024-01-10 23:59:59"},
        {"celda_id": "c2", "fenomeno_codigo": "TO", "nivel_num": 0, "tipo_mensaje": "Alert",
         "enviado_ts": "2024-01-09 10:00", "inicio_ts": "2024-01-10 00:00", "fin_ts": "2024-01-10 23:59:59"},
    ])
    table, first = rd.warning_days(warnings, cells, 20)
    assert first == pd.Timestamp("2024-01-10")
    rows = table.set_index(["distribuidora_id", "fecha"])
    assert rows.loc[(1, pd.Timestamp("2024-01-10")), "aviso_max_nivel"] == 2
    assert rows.loc[(1, pd.Timestamp("2024-01-10")), "aviso_viento_nivel"] == 2
    assert (1, pd.Timestamp("2024-01-11")) not in rows.index
    assert rows.loc[(1, pd.Timestamp("2024-01-12")), "aviso_lluvia_nivel"] == 1
    # Ending exactly at midnight does not cover 13.01; the green level leaves no row.
    assert (1, pd.Timestamp("2024-01-13")) not in rows.index
    assert 2 not in rows.index.get_level_values(0)


def test_quantile_map_takes_the_forecast_to_the_scale_of_the_reanalysis():
    rng = np.random.default_rng(0)
    reanalysis = rng.gamma(4.0, 8.0, 2000)
    forecast = reanalysis * 1.2 + 5.0
    mapped = rd.quantile_map(forecast, reanalysis, np.quantile(forecast, [0.1, 0.5, 0.9]))
    assert mapped == pytest.approx(np.quantile(reanalysis, [0.1, 0.5, 0.9]), rel=0.02)
    # Too few days to fit: the values go as they are.
    assert rd.quantile_map([1.0, 2.0], [3.0, 4.0], [5.0]).tolist() == [5.0]


def test_top_share_counts_the_positive_days_of_the_top_of_each_year():
    y = np.array([1, 0, 0, 0, 1, 0, 0, 0, 0, 1])
    score = np.array([0.9, 0.1, 0.2, 0.3, 0.05, 0.8, 0.1, 0.2, 0.3, 0.7])
    year = np.array([2024] * 5 + [2025] * 5)
    # The top 20 % is one day per year: 2024 holds its first positive, 2025 does not.
    assert rd.top_share(y, score, year, 0.2) == pytest.approx(1 / 3)
    assert rd.top_share(y, score, year, 0.4) == pytest.approx(2 / 3)


# ---------------------------------------------------------------------------
# The whole job
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="dia_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("dia-test")
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
    """(weather per cell and hour, principal events with cause, warnings of AEMET)."""
    rng = np.random.default_rng(11)
    hours = pd.date_range("2019-01-01", "2025-12-31 23:00", freq="h")
    days = pd.date_range("2019-01-02", "2025-12-30", freq="D")
    weather, events, warnings = [], [], []

    for dist, cell in CELLS.items():
        gust = rng.gamma(4.0, 6.0, len(hours))
        rain = np.where(rng.random(len(hours)) < 0.05, rng.gamma(1.0, 2.0, len(hours)), 0.0)
        for day in rng.choice(days, size=7 * 20, replace=False):
            start = int((pd.Timestamp(day) + pd.Timedelta(hours=10) - hours[0]) / pd.Timedelta(hours=1))
            gust[start:start + 6] = rng.uniform(80, 110, 6)
            rain[start:start + 6] = rng.uniform(2, 8, 6)
            if rng.random() < 0.7:
                events.append((dist, pd.Timestamp(day) + pd.Timedelta(hours=12), "meteorologia"))
            # AEMET warns of most storms the day before at noon.
            if rng.random() < 0.8:
                warnings.append({"celda_id": cell, "fenomeno_codigo": "VI", "nivel_num": 2, "tipo_mensaje": "Alert",
                                 "enviado_ts": pd.Timestamp(day) - pd.Timedelta(hours=12),
                                 "inicio_ts": pd.Timestamp(day) + pd.Timedelta(hours=6),
                                 "fin_ts": pd.Timestamp(day) + pd.Timedelta(hours=22)})
        for _ in range(7 * 15):
            events.append((dist, pd.Timestamp(rng.choice(days)) + pd.Timedelta(hours=int(rng.integers(0, 24))),
                           "equipo" if rng.random() < 0.5 else "aguas_arriba"))
        weather.append(pd.DataFrame({
            "celda_id": cell, "hora": hours, "met_racha_1h": gust, "met_viento_1h": gust * 0.6,
            "met_lluvia_1h": rain, "met_temp_1h": 15 + 10 * np.sin(2 * np.pi * (hours.dayofyear.to_numpy() - 100) / 365),
            "met_humedad_1h": 60.0, "met_presion_1h": 1013.0, "met_presion_delta_24h": 0.0, "met_nieve_24h": 0.0,
        }))

    for dist in CELLS:
        events.append((dist, pd.Timestamp("2025-04-28 12:33"), "apagon"))

    weather = pd.concat(weather, ignore_index=True)
    region = weather.groupby("hora")["met_racha_1h"].max().rolling(3, min_periods=1).max()
    weather["met_region_racha_max_3h"] = weather["hora"].map(region)
    # What a model forecast the day before, from 2024: the same weather with noise and stronger gusts.
    archived = weather["hora"] >= "2024-01-01"
    noise = rng.normal(1.0, 0.15, len(weather))
    weather["metp1_racha_1h"] = np.where(archived, weather["met_racha_1h"] * 1.15 * noise, np.nan)
    weather["metp1_viento_1h"] = np.where(archived, weather["met_viento_1h"] * 1.15 * noise, np.nan)
    weather["metp1_lluvia_1h"] = np.where(archived, weather["met_lluvia_1h"] * noise.clip(0), np.nan)
    weather["metp1_temp_1h"] = np.where(archived, weather["met_temp_1h"] + 1.0, np.nan)
    weather["metp1_humedad_1h"] = np.where(archived, 60.0, np.nan)
    # The archive of warnings starts in the middle of 2019, as the real one in 2018.
    warnings = pd.DataFrame(warnings)
    warnings = warnings[warnings["enviado_ts"] >= "2019-06-18"].reset_index(drop=True)
    return weather, events, warnings


@pytest.fixture(scope="module")
def tables(spark):
    weather, events, warnings = synthetic()
    save(spark, weather, "l3_gold.meteo_celda_hora")
    save(spark, pd.DataFrame([{"distribuidora_id": d, "ct_id": f"{d}-{i}", "celda_id": c}
                              for d, c in CELLS.items() for i in range(3)]), "l3_gold.map_ct_celda")
    facts, causes = [], []
    for k, (dist, start, cause) in enumerate(events):
        period, inc = start.strftime("%Y%m"), f"E{k}"
        facts.append({"distribuidora_id": dist, "periodo_id": period, "incidencia_id": inc, "ct_id": f"{dist}-0",
                      "inicio_ts": start, "conocido_ts": start + pd.Timedelta(days=2), "variante": "principal"})
        causes.append({"distribuidora_id": dist, "periodo_id": period, "id": inc, "causa": cause})
    save(spark, pd.DataFrame(facts), "l3_gold.fact_interrupciones_mt")
    save(spark, pd.DataFrame(causes), "l2_silver.f_incidencia_causa")
    save(spark, warnings, "l2_silver.f_aviso_meteo")
    return events


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config["meteo_dia"] = {
        **config.get("meteo_dia", {}),
        "desde": "2019-01-08",
        "hasta": "2026-01-01",
        "anios_evaluacion": [2023, 2024, 2025],
        "bootstrap_reps": 50,
        "busqueda": {"logistica": [{"C": 0.1}],
                     "xgboost": [{"max_depth": 2, "min_child_weight": 5, "learning_rate": 0.1, "subsample": 0.8,
                                  "colsample_bytree": 0.8, "n_estimators": 200, "early_stopping_rounds": 20,
                                  "verbose": 0}]},
        "variantes": [
            {"nombre": "calendario_historia", "desde": "2019-01-08", "bloques": ["calendario", "historia"]},
            {"nombre": "ayer", "desde": "2019-01-08", "bloques": ["calendario", "historia", "ayer"]},
            {"nombre": "avisos", "desde": "2019-06-25", "bloques": ["calendario", "historia", "ayer", "avisos"]},
            {"nombre": "perfecta", "desde": "2019-01-08", "bloques": ["calendario", "historia", "ayer", "dia"]},
            {"nombre": "perfecta_comun", "desde": "2019-01-08",
             "bloques": ["calendario", "historia", "ayer", "dia_comun"]},
        ],
        "anios_prevision": [2024, 2025],
        "comparaciones": [["perfecta__logistica", "ayer__logistica"], ["avisos__logistica", "ayer__logistica"]],
    }
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    return path


@pytest.fixture(scope="module")
def result(spark, tables, config_path, workdir):
    code = rd.main(["--run-id", RUN_ID, "--config", config_path])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def read(folder, name):
    return pd.read_csv(os.path.join(folder, name))


def test_report_files_and_days(result):
    code, folder = result
    assert code == 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    for title in ("## Variantes", "## Comparación en los años comunes", "## Diferencias emparejadas"):
        assert title in text
    rows = read(folder, "filas.csv")
    # Without the blackout day; 2024 is a leap year.
    assert rows.loc[rows["anio"] == 2025, "dias"].tolist() == [364, 364]
    assert rows.loc[rows["anio"] == 2024, "dias"].tolist() == [366, 366]
    assert (rows["dias_y_dia_meteo"] <= rows["dias_y_dia"]).all()


def test_the_forecast_and_the_warnings_tell_the_days_apart(result):
    _, folder = result
    table = read(folder, "comparacion.csv").set_index(["variante", "modelo"])
    perfect = table.loc[("perfecta", "logistica"), "pr_auc"]
    nothing = table.loc[("calendario_historia", "logistica"), "pr_auc"]
    warned = table.loc[("avisos", "logistica"), "pr_auc"]
    assert perfect > 2 * nothing
    # The warnings announce most storms the evening before: better than the weather of yesterday.
    assert warned > 1.5 * table.loc[("ayer", "logistica"), "pr_auc"]
    assert table.loc[("perfecta", "naif_racha_dia"), "pr_auc"] > nothing
    assert table.loc[("avisos", "naif_aviso"), "dias_positivos_en_top_10pct"] > 0.3
    assert len(read(folder, "diferencias.csv")) == 2


def test_the_real_forecast_scores_the_years_it_covers(result):
    _, folder = result
    table = read(folder, "prevision.csv").set_index(["tiempo_del_dia", "modelo"])
    assert {"reanalisis", "prevision", "prevision_cuantiles", "solo_ayer"} <= set(table.index.get_level_values(0))
    perfect = table.loc[("reanalisis", "logistica"), "pr_auc"]
    mapped = table.loc[("prevision_cuantiles", "logistica"), "pr_auc"]
    # A noisy forecast keeps most of what the weather of the day says, far above knowing only yesterday.
    assert mapped > 2 * table.loc[("solo_ayer", "logistica"), "pr_auc"]
    assert mapped > 0.6 * perfect
    assert len(read(folder, "prevision_diferencias.csv")) >= 2
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "## Con la previsión real del día anterior (2024, 2025)" in text


def test_metrics_and_run_saved(result, spark):
    assert spark.table("l3_gold.ml_metricas").where(f"run_id = '{RUN_ID}'").count() > 100
    run = spark.table("l3_gold.ml_runs").where(f"run_id = '{RUN_ID}'").first()
    assert run is not None and run["etiqueta"] == "meteo_dia:y_dia"


def test_the_models_of_the_hours_never_see_the_forecast_of_the_day_before(spark, tables):
    from job_ml_horas_riesgo import distributor_weather

    _, names = distributor_weather(spark, {"activo": True})
    assert names and not any("metp1_" in n or "metp2_" in n for n in names)
    _, names = distributor_weather(spark, {"activo": True}, prefixes=("met_", "metp1_"))
    assert "metd_max_metp1_racha_1h" in names
