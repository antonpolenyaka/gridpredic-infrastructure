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
    assert out["y_dia_varias"].tolist() == [0, 0, 0, 0]
    assert out["n_dia"].tolist() == [1, 0, 1, 1]
    # Two incidents on 30.03 (one of them with two CT) make a busy day; one incident with two CT does not.
    busy = pd.DataFrame({"distribuidora_id": [1, 1, 1, 2, 2], "periodo_id": [2024] * 5,
                         "incidencia_id": [7, 8, 8, 9, 9],
                         "inicio_ts": pd.to_datetime(["2024-03-30 08:00", "2024-03-30 17:00", "2024-03-30 17:05",
                                                      "2024-03-29 10:00", "2024-03-29 10:30"]),
                         "causa": ["meteorologia"] * 5})
    out = rd.day_labels(grid, busy, ["aguas_arriba"], several=2)
    assert out["y_dia_varias"].tolist() == [0, 0, 1, 0]
    assert out["n_dia"].tolist() == [0, 0, 2, 1]

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


def test_what_the_warnings_announce_and_how_much_they_cover():
    # Distribuidora 1 has 3 CT in c1 and 1 in c2.
    cells = pd.DataFrame({"distribuidora_id": [1, 1], "celda_id": ["c1", "c2"], "n_ct": [3.0, 1.0]})
    sent = "2024-01-09 12:00"
    warnings = pd.DataFrame([
        {"celda_id": "c1", "fenomeno_codigo": "VI", "nivel_num": 1, "tipo_mensaje": "Alert", "enviado_ts": sent,
         "inicio_ts": "2024-01-10 06:00", "fin_ts": "2024-01-10 12:00", "parametro": "RM;Rachas máximas;70 km/h"},
        {"celda_id": "c1", "fenomeno_codigo": "VI", "nivel_num": 2, "tipo_mensaje": "Alert", "enviado_ts": sent,
         "inicio_ts": "2024-01-10 12:00", "fin_ts": "2024-01-11 00:00", "parametro": "RM;Rachas máximas;90 km/h"},
        {"celda_id": "c2", "fenomeno_codigo": "PR", "nivel_num": 1, "tipo_mensaje": "Alert", "enviado_ts": sent,
         "inicio_ts": "2024-01-10 00:00", "fin_ts": "2024-01-10 12:00",
         "parametro": "P2;Precipitación acumulada en 12 horas;40 mm"},
        # Heat counts in the level, not in the share under a warning of the weather that breaks lines.
        {"celda_id": "c2", "fenomeno_codigo": "AT", "nivel_num": 1, "tipo_mensaje": "Alert", "enviado_ts": sent,
         "inicio_ts": "2024-01-10 12:00", "fin_ts": "2024-01-10 20:00", "parametro": "TA;Temperatura máxima;38 ºC"},
    ])
    rows = rd.warning_days(warnings, cells, 20)[0].set_index(["distribuidora_id", "fecha"])
    day = rows.loc[(1, pd.Timestamp("2024-01-10"))]
    assert day["aviso_max_nivel"] == 2 and day["aviso_det_racha_kmh"] == 90
    assert day["aviso_det_lluvia_12h_mm"] == 40 and pd.isna(day["aviso_det_lluvia_1h_mm"])
    # The longest wind warning of the day, 12 hours.
    assert day["aviso_det_viento_horas"] == pytest.approx(12.0)
    assert day["aviso_det_viento_ct"] == pytest.approx(0.75)
    assert day["aviso_det_tiempo_ct"] == pytest.approx(1.0)
    assert pd.isna(day["aviso_det_tormenta_ct"])


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


def test_extremes_from_the_gusts_against_the_climate_of_each_cell():
    hours = pd.date_range("2022-01-02", "2022-01-05 23:00", freq="h")
    hourly = pd.concat([pd.DataFrame({"distribuidora_id": dist, "hora": hours, "metd_max_met_racha_clima_1h": 0.8,
                                      "metd_media_met_racha_clima_1h": 0.5, "metd_media_met_racha_ku_1h": 0.0})
                        for dist in (1, 2)], ignore_index=True)
    # A storm on 03.01: the worst cell at twice its p98 for six hours, the CT weighted mean at 1.5.
    storm = (hourly["hora"] >= "2022-01-03 10:00") & (hourly["hora"] < "2022-01-03 16:00")
    hourly.loc[storm, "metd_max_met_racha_clima_1h"] = 2.0
    hourly.loc[storm, "metd_media_met_racha_clima_1h"] = 1.5
    hourly.loc[storm, "metd_media_met_racha_ku_1h"] = 0.125
    # 18 hours of gusts on 04.01 in 1 are not a day.
    gap = (hourly["distribuidora_id"] == 1) & (hourly["hora"] >= "2022-01-04 00:00") & (hourly["hora"] < "2022-01-04 06:00")
    hourly.loc[gap, ["metd_max_met_racha_clima_1h", "metd_media_met_racha_clima_1h", "metd_media_met_racha_ku_1h"]] = np.nan
    days = pd.date_range("2021-12-28", "2022-01-05", freq="D")
    heat = [30, 36, 37, 20, 36, 36, 36, np.nan, 38]
    daily = pd.concat([pd.DataFrame({"distribuidora_id": dist, "fecha": days, "temp_max": heat}) for dist in (1, 2)],
                      ignore_index=True)
    out = rd.extremes(hourly, daily, 35.0, 20).set_index(["distribuidora_id", "fecha"])

    calm, stormy = out.loc[(2, pd.Timestamp("2022-01-02"))], out.loc[(2, pd.Timestamp("2022-01-03"))]
    assert calm["ext_racha_clima_max"] == pytest.approx(0.8) and calm["ext_racha_ku"] == 0
    assert stormy["ext_racha_clima_max"] == pytest.approx(2.0) and stormy["ext_racha_clima_media"] == pytest.approx(1.5)
    assert stormy["ext_racha_ku"] == pytest.approx(0.75)
    assert pd.isna(out.loc[(1, pd.Timestamp("2022-01-04")), "ext_racha_clima_max"])
    assert out.loc[(2, pd.Timestamp("2022-01-04")), "ext_racha_clima_max"] == pytest.approx(0.8)
    # Days in a row at 35 degrees or more, up to the day; an unknown day breaks the run.
    runs = out.loc[1, "ext_dias_calor"].dropna()
    assert runs.loc["2021-12-29":"2022-01-03"].tolist() == [1.0, 2.0, 0.0, 1.0, 2.0, 3.0]
    assert pd.isna(out.loc[(1, pd.Timestamp("2022-01-04")), "ext_dias_calor"])
    assert out.loc[(1, pd.Timestamp("2022-01-05")), "ext_dias_calor"] == 1.0


def test_extremes_of_the_forecast_with_the_run_of_heat_known_the_evening_before():
    hours = pd.date_range("2024-07-01", "2024-07-04 23:00", freq="h")
    hourly = pd.DataFrame({"distribuidora_id": 1, "hora": hours, "metd_max_metp1_racha_clima_1h": 0.9,
                           "metd_media_metp1_racha_clima_1h": 0.6, "metd_media_metp1_racha_ku_1h": 0.0})
    storm = (hourly["hora"] >= "2024-07-02 14:00") & (hourly["hora"] < "2024-07-02 18:00")
    hourly.loc[storm, ["metd_max_metp1_racha_clima_1h", "metd_media_metp1_racha_clima_1h",
                       "metd_media_metp1_racha_ku_1h"]] = [1.8, 1.4, 0.064]
    forecast = pd.DataFrame({"distribuidora_id": 1, "fecha": pd.date_range("2024-07-01", "2024-07-04", freq="D"),
                             "temp_max": [36.0, 37.0, 30.0, np.nan]})
    # The run of the reanalysis up to each day: unknown on 02.07.
    extreme = pd.DataFrame({"distribuidora_id": 1, "fecha": pd.date_range("2024-06-30", "2024-07-03", freq="D"),
                            "ext_dias_calor": [2.0, 3.0, np.nan, 0.0]})
    out = rd.forecast_extremes(hourly, forecast, extreme, 35.0, 20).set_index("fecha")

    assert out.loc["2024-07-02", "prev_ext_racha_clima_max"] == pytest.approx(1.8)
    assert out.loc["2024-07-02", "prev_ext_racha_clima_media"] == pytest.approx(1.4)
    assert out.loc["2024-07-02", "prev_ext_racha_ku"] == pytest.approx(4 * 0.064)
    assert out.loc["2024-07-01", "prev_ext_racha_ku"] == 0
    # The forecast hottest hour of the day on top of the run up to the day before; a cool day ends the run even
    # when the day before is unknown, and a day without forecast has no run.
    assert out.loc["2024-07-01", "prev_ext_dias_calor"] == 3.0
    assert out.loc["2024-07-02", "prev_ext_dias_calor"] == 4.0
    assert out.loc["2024-07-03", "prev_ext_dias_calor"] == 0.0
    assert pd.isna(out.loc["2024-07-04", "prev_ext_dias_calor"])


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
    for db in ("l1_bronze", "l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()


def save(spark, pdf: pd.DataFrame, table: str):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(table)


def synthetic():
    """(weather per cell and hour, principal events with cause, warnings of AEMET, forecast of a second model)."""
    rng = np.random.default_rng(11)
    # When AEMET sends each warning, drawn apart so the storms and the incidents stay the same.
    lead = np.random.default_rng(12)
    hours = pd.date_range("2019-01-01", "2025-12-31 23:00", freq="h")
    days = pd.date_range("2019-01-02", "2025-12-30", freq="D")
    weather, events, warnings = [], [], []

    for dist, cell in CELLS.items():
        gust = rng.gamma(4.0, 6.0, len(hours))
        rain = np.where(rng.random(len(hours)) < 0.05, rng.gamma(1.0, 2.0, len(hours)), 0.0)
        # The wind blows from anywhere; the storms come from the south-west, one in four from the north-east.
        direction = rng.uniform(0.0, 360.0, len(hours))
        for day in rng.choice(days, size=7 * 20, replace=False):
            start = int((pd.Timestamp(day) + pd.Timedelta(hours=10) - hours[0]) / pd.Timedelta(hours=1))
            gust[start:start + 6] = rng.uniform(80, 110, 6)
            rain[start:start + 6] = rng.uniform(2, 8, 6)
            direction[start:start + 6] = (45.0 if pd.Timestamp(day).day % 4 == 0 else 225.0) + rng.normal(0, 5, 6)
            if rng.random() < 0.7:
                events.append((dist, pd.Timestamp(day) + pd.Timedelta(hours=12), "meteorologia"))
            # AEMET warns of most storms the day before at noon, and of half of those two days before.
            if rng.random() < 0.8:
                warnings.append({"celda_id": cell, "fenomeno_codigo": "VI", "nivel_num": 2, "tipo_mensaje": "Alert",
                                 "enviado_ts": pd.Timestamp(day) - pd.Timedelta(hours=12 if lead.random() < 0.5 else 36),
                                 "inicio_ts": pd.Timestamp(day) + pd.Timedelta(hours=6),
                                 "fin_ts": pd.Timestamp(day) + pd.Timedelta(hours=22)})
        for _ in range(7 * 15):
            events.append((dist, pd.Timestamp(rng.choice(days)) + pd.Timedelta(hours=int(rng.integers(0, 24))),
                           "equipo" if rng.random() < 0.5 else "aguas_arriba"))
        temp = 15 + 10 * np.sin(2 * np.pi * (hours.dayofyear.to_numpy() - 100) / 365)
        # A freezing rain episode: the morning of 10.01.2024, rain at -2 degrees.
        freezing = (hours >= "2024-01-10 03:00") & (hours < "2024-01-10 09:00")
        temp = np.where(freezing, -2.0, temp)
        rain = np.where(freezing, 1.5, rain)
        # The reference evapotranspiration of the week: more in summer, with a dry 2025 (half the rain, more ET0).
        et0_day = 3.0 + 2.5 * np.sin(2 * np.pi * (hours.dayofyear.to_numpy() - 110) / 365)
        dry = hours.year.to_numpy() == 2025
        rain = np.where(dry, rain * 0.5, rain)
        weather.append(pd.DataFrame({
            "celda_id": cell, "hora": hours, "met_racha_1h": gust, "met_viento_1h": gust * 0.6,
            "met_lluvia_1h": rain, "met_temp_1h": temp,
            "met_humedad_1h": 60.0, "met_presion_1h": 1013.0, "met_presion_delta_24h": 0.0, "met_nieve_24h": 0.0,
            "met_dir_viento_sin": np.sin(np.radians(direction)), "met_dir_viento_cos": np.cos(np.radians(direction)),
            "met_et0_7d": 7.0 * np.where(dry, et0_day * 1.3, et0_day),
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
    # Two days before, only gusts and rain, with twice the noise and a storm in four missed (the gust forecast
    # of that day is calm); without the extremes of Gold (metp2_racha_clima_1h), the daily model derives them.
    noise2 = rng.normal(1.0, 0.3, len(weather))
    missed = weather["hora"].dt.floor("D").isin(rng.choice(days, size=len(days) // 4, replace=False))
    weather["metp2_racha_1h"] = np.where(archived, np.where(missed, 25.0, weather["met_racha_1h"] * 1.15) * noise2,
                                         np.nan)
    weather["metp2_lluvia_1h"] = np.where(archived, weather["met_lluvia_1h"] * noise2.clip(0), np.nan)
    # The gusts against the climate of each cell, as Gold computes them for the reanalysis and the forecast.
    p98 = weather[weather["hora"] < "2022-01-01"].groupby("celda_id")["met_racha_1h"].quantile(0.98)
    for prefix in ("met", "metp1"):
        ratio = weather[f"{prefix}_racha_1h"] / weather["celda_id"].map(p98)
        weather[f"{prefix}_racha_clima_1h"] = ratio
        weather[f"{prefix}_racha_ku_1h"] = np.clip(ratio - 1.0, 0.0, None) ** 3
    # A second model of the day before, as Bronze keeps it (the raw names of the Previous Runs API, without the wind
    # speed): the same weather with its own noise and a storm in three missed (a calm, dry forecast that day), from
    # 2024 like ICON-EU.
    noise3 = rng.normal(1.0, 0.2, len(weather))
    missed3 = weather["hora"].dt.floor("D").isin(rng.choice(days, size=len(days) // 3, replace=False)).to_numpy()
    second = weather.loc[archived, ["celda_id", "hora"]].copy()
    second["wind_gusts_10m_previous_day1"] = np.where(missed3, 25.0, weather["met_racha_1h"] * 1.1)[archived] * noise3[archived]
    second["precipitation_previous_day1"] = np.where(missed3, 0.0, weather["met_lluvia_1h"] * noise3.clip(0))[archived]
    second["temperature_2m_previous_day1"] = (weather["met_temp_1h"] - 1.0)[archived]
    second["relative_humidity_2m_previous_day1"] = 55.0
    second["anio"] = second["hora"].dt.year.astype("int32")
    second["modelo"] = "gfs_seamless"
    # The archive of warnings starts in the middle of 2019, as the real one in 2018.
    warnings = pd.DataFrame(warnings)
    warnings = warnings[warnings["enviado_ts"] >= "2019-06-18"].reset_index(drop=True)
    return weather, events, warnings, second.reset_index(drop=True)


@pytest.fixture(scope="module")
def tables(spark):
    weather, events, warnings, second = synthetic()
    save(spark, weather, "l3_gold.meteo_celda_hora")
    save(spark, second, "l1_bronze.meteo_openmeteo_previo_gfs_hora")
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


def ndvi_csv(workdir) -> str:
    """The 16 day NDVI composites of MODIS per cell as bajar_ndvi.py writes them: a seasonal curve with noise."""
    rng = np.random.default_rng(21)
    rows = []
    for platform, offset in (("terra", 0), ("aqua", 8)):
        for year in range(2017, 2026):
            for k in range(23):
                start = pd.Timestamp(year, 1, 1) + pd.Timedelta(days=16 * k + offset)
                if start.year != year:
                    continue
                end = start + pd.Timedelta(days=15)
                season = 0.45 + 0.25 * np.cos(2 * np.pi * (start.dayofyear - 40) / 365.25)
                for cell in CELLS.values():
                    rows.append({"celda_id": cell, "plataforma": platform, "fecha_inicio": start.date().isoformat(),
                                 "fecha_fin": end.date().isoformat(), "ndvi": round(season + rng.normal(0, 0.03), 4),
                                 "n_validos": 480, "n_pixeles": 500, "_item": f"{platform}_{start:%Y%j}"})
    path = os.path.join(workdir, "ndvi_celdas.csv")
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def hrl_csv(workdir) -> str:
    """The leaf type of the cells as celdas_hrl.py writes it: the first network among broadleaved trees."""
    path = os.path.join(workdir, "hrl_celdas.csv")
    pd.DataFrame({"celda_id": ["c1", "c2"], "hrl_copa_media_pct": [55.0, 12.0], "hrl_pct_frondosa": [0.6, 0.1],
                  "hrl_pct_conifera": [0.05, 0.02], "hrl_caducifolio_de_arbol": [0.9, 0.2]}).to_csv(path, index=False)
    return path


def corridor_csv(workdir) -> str:
    """The trees along the power lines of each cell as corredor_lineas.py writes them."""
    path = os.path.join(workdir, "corredor_celdas.csv")
    pd.DataFrame({"celda_id": ["c1", "c2"], "corr_km_linea": [30.0, 10.0], "corr_pct_dosel_8m": [0.05, 0.002],
                  "corr_pct_dosel_5m": [0.1, 0.01]}).to_csv(path, index=False)
    return path


def mfe_csv(workdir) -> str:
    """The canopy of each group of species in the cells as celdas_mfe.py writes it, in % of the cell."""
    path = os.path.join(workdir, "mfe_celdas.csv")
    pd.DataFrame({"celda_id": ["c1", "c2"], "mfe_copa_caducifolia_pct": [20.0, 0.5],
                  "mfe_copa_perennifolia_pct": [3.0, 15.0]}).to_csv(path, index=False)
    return path


def nao_csv(workdir) -> str:
    """The daily NAO index as the job reads it: a sine over the year, so every day has its own value."""
    path = os.path.join(workdir, "nao_diario.csv")
    days = pd.date_range("2018-12-01", "2026-01-31", freq="D")
    pd.DataFrame({"fecha": days, "nao": np.sin(np.arange(len(days)) / 30.0)}).to_csv(path, index=False)
    return path


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
        "modelos": ["tasa_base", "logistica", "xgboost", "poisson", "logistica_firth"],
        "busqueda": {"logistica": [{"C": 0.1}], "poisson": [{"alpha": 1.0}], "logistica_firth": [{"flic": True}],
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
            {"nombre": "perfecta_extremos", "desde": "2019-01-08",
             "bloques": ["calendario", "historia", "ayer", "dia", "extremos"]},
            {"nombre": "comun_avisos", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "ayer", "avisos", "dia_comun"]},
            {"nombre": "comun_avisos_extremos", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "ayer", "avisos", "dia_comun", "extremos"]},
            {"nombre": "comun_avisos_detalle", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "ayer", "avisos", "avisos_detalle", "dia_comun"]},
            {"nombre": "operativa_racha", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "avisos", "dia_comun_48h", "extremos_racha"],
             "variables": ["ayer_racha_max", "ayer_lluvia_max"], "horizonte": 1},
            {"nombre": "operativa_48h", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "avisos_48h", "dia_comun_48h", "extremos_racha"],
             "variables": ["hace2_racha_max", "hace2_lluvia_max"], "horizonte": 2},
            {"nombre": "operativa_72h", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "avisos_72h", "dia_comun_72h", "extremos_racha"],
             "variables": ["hace3_racha_max", "hace3_lluvia_max"], "horizonte": 3},
            {"nombre": "operativa_mas", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "avisos", "dia_comun", "extremos", "extension", "interacciones",
                         "pendiente_red"],
             "variables": ["ayer_racha_max", "ayer_lluvia_max"]},
            {"nombre": "operativa_fenologia", "desde": "2019-06-25",
             "bloques": ["calendario", "historia", "avisos", "dia_comun", "extremos", "fenologia"],
             "variables": ["ayer_racha_max", "ayer_lluvia_max"]},
        ],
        "fenologia": {"csv": ndvi_csv(workdir), "latencia_dias": 7, "clima_hasta": "2022-01-01"},
        "hoja": {"csv": hrl_csv(workdir)},
        "corredor": {"csv": corridor_csv(workdir)},
        "caducas": {"csv": mfe_csv(workdir)},
        "nao": {"csv": nao_csv(workdir)},
        "variante_prevision": ["perfecta_comun", "comun_avisos", "comun_avisos_extremos", "operativa_racha",
                               "operativa_48h", "operativa_72h", "operativa_mas", "operativa_fenologia"],
        "anios_prevision": [2024, 2025],
        # A second model in Bronze and one that is not there (left out with a warning).
        "previsiones_extra": [{"clave": "gfs", "tabla": "l1_bronze.meteo_openmeteo_previo_gfs_hora", "nombre": "GFS"},
                              {"clave": "gem", "tabla": "l1_bronze.meteo_openmeteo_previo_gem_hora", "nombre": "GEM"}],
        "comparaciones_prevision": [["comun_avisos_extremos__logistica__prevision",
                                     "comun_avisos__logistica__prevision"],
                                    ["operativa_48h__logistica__prevision", "operativa_racha__logistica__prevision"],
                                    ["operativa_mas__logistica__prevision",
                                     "comun_avisos_extremos__logistica__prevision"]],
        "comparaciones": [["perfecta__logistica", "ayer__logistica"], ["avisos__logistica", "ayer__logistica"]],
    }
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    return path


@pytest.fixture(scope="module")
def result(spark, tables, config_path, workdir):
    code = rd.main(["--run-id", RUN_ID, "--config", config_path, "--previsiones-extra"])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def read(folder, name):
    return pd.read_csv(os.path.join(folder, name))


def test_report_files_and_days(result):
    code, folder = result
    assert code == 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    for title in ("## Variantes", "## Comparación en los años comunes", "## Diferencias emparejadas",
                  "## Días con 2 incidencias o más"):
        assert title in text
    rows = read(folder, "filas.csv")
    # Without the blackout day; 2024 is a leap year.
    assert rows.loc[rows["anio"] == 2025, "dias"].tolist() == [364, 364]
    assert rows.loc[rows["anio"] == 2024, "dias"].tolist() == [366, 366]
    assert (rows["dias_y_dia_meteo"] <= rows["dias_y_dia"]).all()
    assert (rows["dias_y_dia_varias"] <= rows["dias_y_dia"]).all()


def test_the_forecast_and_the_warnings_tell_the_days_apart(result):
    _, folder = result
    table = read(folder, "comparacion.csv").set_index(["variante", "modelo"])
    perfect = table.loc[("perfecta", "logistica"), "pr_auc"]
    nothing = table.loc[("calendario_historia", "logistica"), "pr_auc"]
    warned = table.loc[("avisos", "logistica"), "pr_auc"]
    assert perfect > 2 * nothing
    # The warnings announce most storms the evening before: better than the weather of yesterday.
    assert warned > 1.5 * table.loc[("ayer", "logistica"), "pr_auc"]
    # A reference is shown once, under the first variant that has its feature.
    references = table.reset_index().set_index("modelo")
    assert references.loc["naif_racha_dia", "pr_auc"] > nothing
    assert references.loc["naif_aviso", "dias_positivos_en_top_10pct"] > 0.3
    # The two models of every variant together by their percentiles in the year.
    assert table.loc[("perfecta", "rango"), "pr_auc"] > 2 * nothing
    # The pairs of the configuration, and Firth against the L2 logistic regression in every variant.
    pairs = set(zip(*[read(folder, "diferencias.csv")[c] for c in ("a", "b")]))
    assert {("perfecta__logistica", "ayer__logistica"), ("avisos__logistica", "ayer__logistica")} <= pairs
    assert ("perfecta__logistica_firth", "perfecta__logistica") in pairs
    # The model of counts (fitted on n_dia) ranks the days like the logistic regression.
    counted = table.loc[("perfecta", "poisson"), "pr_auc"]
    assert counted > 2 * nothing and counted > 0.7 * perfect


def test_the_calibration_of_the_probabilities(result):
    _, folder = result
    table = read(folder, "comparacion.csv").set_index(["variante", "modelo"])
    for column in ("bss", "pendiente_calibracion", "intercepto_calibracion"):
        assert column in table.columns
    # The models that give a probability have a calibration; the mean of two rankings does not.
    for model in ("logistica", "logistica_firth", "poisson", "xgboost"):
        assert np.isfinite(table.loc[("perfecta", model), "bss"])
    assert np.isnan(table.loc[("perfecta", "rango"), "bss"])
    # The weather of the day says much more than the base rate of the years before, whose skill is about zero.
    assert table.loc[("perfecta", "logistica"), "bss"] > 0.05
    assert table.reset_index().set_index("modelo").loc["tasa_base", "bss"] < 0.02
    assert 0.3 < table.loc[("perfecta", "logistica_firth"), "pendiente_calibracion"] < 3
    # Firth ranks like the L2 logistic regression.
    assert table.loc[("perfecta", "logistica_firth"), "pr_auc"] > 0.7 * table.loc[("perfecta", "logistica"), "pr_auc"]
    metrics = read(folder, "metricas.csv")
    measured = set(metrics["metrica"])
    assert {"brier", "log_loss", "prob_media", "bss"} <= measured
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "bss es la mejora del Brier" in text
    forecast = read(folder, "prevision.csv")
    assert forecast["bss"].notna().any()
    differences = read(folder, "prevision_diferencias.csv").set_index(["a", "b"])
    assert ("perfecta_comun__logistica_firth__prevision", "perfecta_comun__logistica__prevision") in differences.index


def test_the_real_forecast_scores_the_years_it_covers(result):
    _, folder = result
    full = read(folder, "prevision.csv")
    # The variants of the forecast, with the models of yesterday once; the one of three days ahead has no
    # forecast in Gold (no metp3_*) and is skipped.
    assert set(full["variante"]) == {"perfecta_comun", "comun_avisos", "comun_avisos_extremos", "ayer",
                                     "operativa_racha", "operativa_48h", "operativa_mas", "operativa_fenologia"}
    table = full[full["variante"].isin(["perfecta_comun", "ayer"])].set_index(["tiempo_del_dia", "modelo"])
    assert {"reanalisis", "prevision", "prevision_cuantiles", "solo_ayer"} <= set(table.index.get_level_values(0))
    warned = full[full["variante"] == "comun_avisos"].set_index(["tiempo_del_dia", "modelo"])
    assert warned.loc[("prevision_cuantiles", "logistica"), "pr_auc"] > full.loc[
        (full["variante"] == "ayer") & (full["modelo"] == "logistica"), "pr_auc"].iloc[0]
    assert ("prevision_cuantiles", "rango") in table.index
    perfect = table.loc[("reanalisis", "logistica"), "pr_auc"]
    mapped = table.loc[("prevision_cuantiles", "logistica"), "pr_auc"]
    # A noisy forecast keeps most of what the weather of the day says, far above knowing only yesterday.
    assert mapped > 2 * table.loc[("solo_ayer", "logistica"), "pr_auc"]
    assert mapped > 0.6 * perfect
    differences = read(folder, "prevision_diferencias.csv")
    assert len(differences) >= 4 and differences["a"].str.startswith("comun_avisos__").any()
    # Between variants on the same forecast (comparaciones_prevision).
    pair = differences[(differences["a"] == "comun_avisos_extremos__logistica__prevision")
                       & (differences["b"] == "comun_avisos__logistica__prevision")]
    assert len(pair) == 1
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "## Con la previsión real del día anterior (2024, 2025)" in text


def test_two_days_ahead_scores_with_the_older_forecast_and_warnings(result):
    _, folder = result
    full = read(folder, "prevision.csv").set_index(["variante", "tiempo_del_dia", "modelo"])
    near = full.loc[("operativa_racha", "prevision", "logistica"), "pr_auc"]
    far = full.loc[("operativa_48h", "prevision", "logistica"), "pr_auc"]
    only_before = full.loc[("ayer", "solo_ayer", "logistica"), "pr_auc"]
    # The forecast of two days before misses a storm in four and warns of half: worse than the day before, still
    # far above knowing only the weather of the days before.
    assert only_before < far < near
    assert far > 0.5 * near
    # On the reanalysis (a perfect forecast) both variants see the same gusts and rain.
    assert full.loc[("operativa_48h", "reanalisis", "logistica"), "pr_auc"] == pytest.approx(
        full.loc[("operativa_racha", "reanalisis", "logistica"), "pr_auc"], rel=0.15)
    differences = read(folder, "prevision_diferencias.csv")
    pair = differences[(differences["a"] == "operativa_48h__logistica__prevision")
                       & (differences["b"] == "operativa_racha__logistica__prevision")]
    assert len(pair) == 1 and pair["dif_p50"].iloc[0] < 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "operativa_48h (48 h)" in text and "dos o tres días antes" in text
    variants = read(folder, "variantes.csv").set_index("variante")
    assert variants.loc["operativa_48h", "horizonte_dias"] == 2 and variants.loc["operativa_racha", "horizonte_dias"] == 1


def test_a_second_model_and_the_mean_of_the_models_score_the_same_days(result):
    _, folder = result
    full = read(folder, "prevision.csv")
    table = full[full["variante"] == "perfecta_comun"].set_index(["tiempo_del_dia", "modelo"])
    sources = set(table.index.get_level_values(0))
    assert {"prevision", "prevision_gfs", "prevision_gfs_cuantiles", "prevision_media", "prevision_media_cuantiles"} <= sources
    # The model that is not in Bronze is left out, not invented.
    assert not any("_gem" in s for s in sources)
    # On the scale of the reanalysis (by quantiles, so the bias of each model does not count): the second model
    # misses a storm in three and is worse than ICON-EU alone; the mean of both keeps most of the storms ICON-EU
    # sees (a missed storm still scores half way), so it sits between them.
    icon = table.loc[("prevision_cuantiles", "logistica"), "pr_auc"]
    second = table.loc[("prevision_gfs_cuantiles", "logistica"), "pr_auc"]
    mean = table.loc[("prevision_media_cuantiles", "logistica"), "pr_auc"]
    assert second < 0.9 * icon
    assert second < mean and mean > 0.8 * icon
    assert ("prevision_media_cuantiles", "rango") in table.index and ("prevision_gfs", "xgboost") in table.index
    differences = read(folder, "prevision_diferencias.csv").set_index(["a", "b"])
    for a in ("perfecta_comun__logistica__prevision_gfs", "perfecta_comun__logistica__prevision_media"):
        assert (a, "perfecta_comun__logistica__prevision") in differences.index
    for key in ("gfs", "media"):
        assert (f"perfecta_comun__rango__prevision_{key}_cuantiles",
                "perfecta_comun__rango__prevision_cuantiles") in differences.index
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "GFS (prevision_gfs, 100 % de los días)" in text and "prevision_media" in text
    # Two days ahead there is no second model: only the three usual weathers of the day.
    far = full[full["variante"] == "operativa_48h"]
    assert set(far["tiempo_del_dia"]) == {"reanalisis", "prevision", "prevision_cuantiles"}


def test_the_second_model_is_joined_from_bronze_and_is_never_a_feature(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    cfg = {**rd.settings(config), "comparar_previsiones_extra": True}
    pdf, blocks, _ = rd.build_dataset(spark, config, cfg)
    # Its daily weather, extremes over the same climate, freezing hours and interactions, from 2024 like ICON-EU,
    # without the wind speed Bronze does not have.
    names = ["prevgfs_racha_max", "prevgfs_lluvia_max", "prevgfs_temp_max", "prevgfs_humedad_media",
             "prevgfs_ext_racha_clima_max", "prevgfs_ext_racha_ku", "prevgfs_ext_dias_calor", "prevgfs_hie_horas",
             "prevgfs_ext_int_racha_lluvia_previa"]
    assert set(names) <= set(pdf.columns) and "prevgfs_viento_max" not in pdf.columns
    assert not any(n.startswith("prevgfs_") for block in blocks.values() for n in block)
    known = pdf[pdf["prevgfs_racha_max"].notna()]
    assert known["fecha"].min() == pd.Timestamp("2024-01-01")
    assert len(known) == pytest.approx(len(pdf[pdf["prev_racha_max"].notna()]), rel=0.01)
    # Over the same p98 of the cell as the forecast of ICON-EU.
    ratio = (known["prevgfs_ext_racha_clima_max"] / known["prevgfs_racha_max"]).median()
    ratio1 = (known["prev_ext_racha_clima_max"] / known["prev_racha_max"]).median()
    assert ratio == pytest.approx(ratio1, rel=0.05)
    # Only the model that is in Bronze stays in the settings for the forecast comparison.
    assert [e["clave"] for e in cfg["previsiones_extra"]] == ["gfs"]
    assert cfg["previsiones_extra"][0]["cobertura"] == pytest.approx(1.0, abs=0.01)
    # Without the flag nothing of it is read or built.
    plain, _, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert not any(c.startswith("prevgfs_") for c in plain.columns)


def test_the_greenness_of_the_network_is_known_with_the_latency_of_the_composites(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    cfg = rd.settings(config)
    pdf, blocks, _ = rd.build_dataset(spark, config, cfg)
    assert set(blocks["fenologia"]) == {"fen_ndvi", "fen_ndvi_anom", "fen_ndvi_cambio_1m", "fen_int_racha_ndvi"}
    assert "prev_fen_int_racha_ndvi" in blocks["prevision"] and "prev2_fen_int_racha_ndvi" in blocks["prevision_48h"]
    assert not any(c.startswith("fen_") for c in blocks["extremos"] + blocks["interacciones"])
    # A composite every eight days and a week of latency: every day of the dataset has a value.
    assert pdf["fen_ndvi"].notna().all()
    # The value of D is the latest composite whose period had ended a week before D, the CT weighted mean of the cells.
    rows = pd.read_csv(cfg["fenologia"]["csv"], parse_dates=["fecha_inicio", "fecha_fin"])
    day = pd.Timestamp("2024-03-10")
    known = rows[rows["fecha_fin"] + pd.Timedelta(days=7) <= day]
    latest = known[known["fecha_fin"] == known["fecha_fin"].max()]
    dist = min(CELLS)
    expected = latest[latest["celda_id"] == CELLS[dist]]["ndvi"].mean()
    found = pdf.set_index(["distribuidora_id", "fecha"]).loc[(dist, day)]
    assert found["fen_ndvi"] == pytest.approx(expected, abs=1e-6)
    assert latest["fecha_fin"].iloc[0] >= day - pd.Timedelta(days=7 + 8)
    # The anomaly is a z-score of the period of the year over the years before 2022: centred there, and it follows
    # the season (greener in winter in this curve), so the raw value and the anomaly are different things.
    before = pdf[pdf["fecha"] < "2022-01-01"]
    assert abs(before["fen_ndvi_anom"].mean()) < 0.3 and 0.6 < before["fen_ndvi_anom"].std() < 1.4
    winter = pdf[pdf["fecha"].dt.month == 2]["fen_ndvi"].mean()
    summer = pdf[pdf["fecha"].dt.month == 8]["fen_ndvi"].mean()
    assert winter > summer + 0.3
    assert (pdf["fen_ndvi_cambio_1m"] > 0).any() and (pdf["fen_ndvi_cambio_1m"] < 0).any()
    # The gust over the greenness, and its forecast twin, are zero on calm days and positive on storm days.
    calm = pdf[pdf["ext_racha_clima_max"] < 0.5]
    storms = pdf[pdf["ext_racha_clima_max"] > 1.2]
    assert (calm["fen_int_racha_ndvi"] == 0).all() and (storms["fen_int_racha_ndvi"] > 0).all()
    archived = pdf[pdf["prev_ext_racha_clima_max"].notna()]
    assert archived["prev_fen_int_racha_ndvi"].notna().all()
    # Without the file there is no block and nothing breaks.
    plain = {**cfg, "fenologia": {"csv": os.path.join(os.path.dirname(cfg["fenologia"]["csv"]), "no_esta.csv")}}
    pdf2, blocks2, _ = rd.build_dataset(spark, config, plain)
    assert "fenologia" not in blocks2 and not any(c.startswith("fen_") for c in pdf2.columns)


def test_the_leaf_type_of_the_network_multiplies_the_gust_in_the_leaf_season(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    pdf, blocks, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert set(blocks["hoja"]) == {"hoja_frondosa", "hoja_int_racha"}
    assert "prev_hoja_int_racha" in blocks["prevision"] and "prev2_hoja_int_racha" in blocks["prevision_48h"]
    by_dist = pdf.groupby("distribuidora_id")["hoja_frondosa"].first()
    assert by_dist[1] == pytest.approx(0.6) and by_dist[2] == pytest.approx(0.1)
    storms = pdf[pdf["ext_racha_clima_max"] > 1.2]
    winter = storms[storms["fecha"].dt.month.isin([12, 1, 2])]
    summer = storms[storms["fecha"].dt.month.isin([6, 7, 8])]
    assert (winter["hoja_int_racha"] == 0).all() and (summer["hoja_int_racha"] > 0).all()
    # The same storm weighs more on the network among broadleaved trees.
    assert summer[summer["distribuidora_id"] == 1]["hoja_int_racha"].mean() > 3 * summer[summer["distribuidora_id"] == 2]["hoja_int_racha"].mean()


def test_the_tall_trees_along_the_lines_multiply_the_gust(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    pdf, blocks, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert set(blocks["corredor"]) == {"corr_dosel_8m", "corr_int_racha"}
    assert "prev_corr_int_racha" in blocks["prevision"]
    shares = pdf.groupby("distribuidora_id")["corr_dosel_8m"].first()
    assert shares[1] == pytest.approx(0.05) and shares[2] == pytest.approx(0.002)
    storms = pdf[pdf["ext_racha_clima_max"] > 1.2]
    assert (storms["corr_int_racha"] > 0).all()
    assert (pdf.loc[pdf["ext_racha_clima_max"] < 0.5, "corr_int_racha"] == 0).all()


def test_the_deciduous_canopy_of_the_forest_map_multiplies_the_gust_in_the_leaf_season(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    pdf, blocks, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert set(blocks["caducas"]) == {"cad_copa", "cad_int_racha"}
    assert "prev_cad_int_racha" in blocks["prevision"]
    shares = pdf.groupby("distribuidora_id")["cad_copa"].first()
    assert shares[1] == pytest.approx(0.20) and shares[2] == pytest.approx(0.005)
    storms = pdf[pdf["ext_racha_clima_max"] > 1.2]
    winter = storms[storms["fecha"].dt.month.isin([12, 1, 2])]
    summer = storms[storms["fecha"].dt.month.isin([6, 7, 8])]
    assert (winter["cad_int_racha"] == 0).all() and (summer["cad_int_racha"] > 0).all()
    # The leaf block of the HRL is still there, next to it.
    assert set(blocks["hoja"]) == {"hoja_frondosa", "hoja_int_racha"}


def test_the_nao_is_known_three_days_late_and_is_the_same_for_every_network(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    pdf, blocks, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert set(blocks["nao"]) == {"nao_dia", "nao_30d"}
    nao = pd.read_csv(json.load(open(config_path))["meteo_dia"]["nao"]["csv"], parse_dates=["fecha"]).set_index("fecha")["nao"]
    day = pd.Timestamp("2024-03-10")
    row = pdf[pdf["fecha"] == day]
    assert row["nao_dia"].nunique() == 1 and row["nao_dia"].iloc[0] == pytest.approx(nao[day - pd.Timedelta(days=3)])
    assert row["nao_30d"].iloc[0] == pytest.approx(nao[day - pd.Timedelta(days=32): day - pd.Timedelta(days=3)].mean())


def test_the_regimes_and_the_fragility_curve_of_the_gust(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    cfg = rd.settings(config)
    pdf, blocks, _ = rd.build_dataset(spark, config, cfg)
    assert set(blocks["regimen"]) == {"reg_temporal", "reg_conveccion", "reg_conveccion_racha", "reg_calor"}
    assert {"prev_reg_temporal", "prev_reg_calor"} <= set(blocks["prevision"])
    month = pdf["fecha"].dt.month
    warm = month.between(5, 9)
    # The storm term only lives in the cold season and the convection and heat terms only in the warm one.
    assert (pdf.loc[warm, "reg_temporal"] == 0).all()
    assert (pdf.loc[~warm, ["reg_conveccion", "reg_calor"]].fillna(0) == 0).all().all()
    wet_storms = pdf[~warm & (pdf["ext_racha_clima_max"] > 1.2) & (pdf["dia_lluvia_max"] >= 5)]
    assert len(wet_storms) and (wet_storms["reg_temporal"] > 0).all()
    assert (pdf["reg_conveccion"].dropna() <= 30).all()
    # The fragility curve is fitted on the days before fragilidad_hasta only and grows with the gust.
    assert set(blocks["fragilidad"]) == {"frag_prob", "frag_logit"}
    assert {"prev_frag_prob", "prev_frag_logit"} <= set(blocks["prevision"])
    ordered = pdf.dropna(subset=["ext_racha_clima_max", "frag_prob"]).sort_values("ext_racha_clima_max")
    assert ordered["frag_prob"].is_monotonic_increasing
    assert ordered["frag_prob"].iloc[-1] > ordered["frag_prob"].iloc[0]
    # Fitted on other days the curve is another one: the years after the cut never shape it.
    late = rd.add_fragility(pdf.copy(), {}, pd.Timestamp("2020-01-01"))
    assert late == ["frag_prob", "frag_logit"]
    assert rd.add_fragility(pdf.copy(), {}, pd.Timestamp("2019-02-01")) == []


def test_metrics_and_run_saved(result, spark):
    assert spark.table("l3_gold.ml_metricas").where(f"run_id = '{RUN_ID}'").count() > 100
    run = spark.table("l3_gold.ml_runs").where(f"run_id = '{RUN_ID}'").first()
    assert run is not None and run["etiqueta"] == "meteo_dia:y_dia"


def test_the_extremes_of_the_forecast_are_never_a_block(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    pdf, blocks, _ = rd.build_dataset(spark, config, rd.settings(config))
    names = ["prev_ext_racha_clima_max", "prev_ext_racha_clima_media", "prev_ext_racha_ku", "prev_ext_dias_calor"]
    assert set(names) <= set(blocks["prevision"])
    assert not any(c.startswith("prev_") for c in blocks["extremos"])
    # Two days ahead: gusts and rain, their extremes derived from the climate of the cells, the warnings sent
    # before 20:00 of D - 2 and the weather of D - 2; nothing of three days ahead without metp3_* in Gold.
    assert set(blocks["dia_comun_48h"]) == {"dia_racha_max", "dia_racha_media_max", "dia_lluvia_max",
                                            "dia_lluvia_media", "dia_lluvia_hora_max"}
    assert {"prev2_racha_max", "prev2_ext_racha_clima_max", "prev2_ext_racha_ku"} <= set(blocks["prevision_48h"])
    assert "prev2_ext_dias_calor" not in pdf.columns and "dia_comun_72h" not in blocks
    assert set(blocks["extremos_racha"]) == {"ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku"}
    assert "hace2_racha_max" in blocks["hace2"] and "aviso48_max_nivel" in blocks["avisos_48h"]
    assert "aviso48_det_viento_ct" in blocks["avisos_detalle_48h"]
    archived = pdf[pdf["fecha"] >= "2019-06-20"]
    # Fewer warnings are known two evenings before than one.
    assert archived["aviso48_max_nivel"].gt(0).sum() < archived["aviso_max_nivel"].gt(0).sum()
    assert (archived["aviso48_max_nivel"].fillna(0) <= archived["aviso_max_nivel"].fillna(0)).all()
    shifted = pdf.set_index(["distribuidora_id", "fecha"])
    day, dist = pd.Timestamp("2024-03-10"), min(CELLS)
    assert shifted.loc[(dist, day), "hace2_racha_max"] == pytest.approx(
        shifted.loc[(dist, day - pd.Timedelta(days=2)), "dia_racha_max"])
    # The derived extremes of two days before go over the same climate as the ones of the day before.
    known = pdf[pdf["prev2_racha_max"].notna()]
    ratio = known["prev2_ext_racha_clima_max"] / known["prev2_racha_max"]
    ratio1 = known["prev_ext_racha_clima_max"] / known["prev_racha_max"]
    assert ratio.median() == pytest.approx(ratio1.median(), rel=0.05)
    # The details of the warnings are a block of their own, 0 without warning once the archive exists.
    assert set(rd.WARNING_DETAILS) == set(blocks["avisos_detalle"])
    assert not any(c.startswith("aviso_det_") for c in blocks["avisos"])
    archived = pdf[pdf["fecha"] >= "2019-06-20"]
    assert archived["aviso_det_viento_ct"].notna().all() and archived["aviso_det_viento_ct"].max() == 1.0
    # Only from 2024, when the forecast starts; the forecast gusts are stronger, so they go further over the climate.
    known = pdf[pdf["prev_ext_racha_clima_max"].notna()]
    assert known["fecha"].min() == pd.Timestamp("2024-01-01")
    assert known["prev_ext_racha_clima_max"].mean() > known["ext_racha_clima_max"].mean()


def test_the_extent_the_interactions_and_the_direction_of_the_gusts(spark, tables, config_path):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    cfg = {**rd.settings(config), "bloque_direccion": True}
    pdf, blocks, _ = rd.build_dataset(spark, config, cfg)
    # The new blocks, apart from the extremes the operational variant already uses.
    assert set(blocks["extension"]) == {"ext_fraccion_p98_max", "ext_fraccion_p98_horas", "ext_fraccion_72_max",
                                        "ext_fraccion_72_horas"}
    assert set(blocks["interacciones"]) == {"ext_int_racha_lluvia_previa", "ext_int_racha_lluvia_dia",
                                            "ext_int_racha_hoja", "ext_int_racha_verano"}
    assert set(blocks["pendiente_red"]) == {f"ext_pend_racha_dist_{d}" for d in CELLS}
    assert set(blocks["direccion"]) == {"ext_dir_anomalia", "ext_dir_int_racha"}
    assert set(blocks["extremos"]) == {"ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku", "ext_dias_calor"}
    assert set(blocks["extremos_racha"]) == {"ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku"}
    # One cell per network: the share under a strong gust is 0 or 1, and 1 exactly when the worst hour is at the p98.
    known = pdf[pdf["ext_racha_clima_max"].notna()]
    assert set(known["ext_fraccion_p98_max"].unique()) <= {0.0, 1.0}
    assert (known["ext_fraccion_p98_max"] == (known["ext_racha_clima_max"] >= 1.0)).all()
    assert (known["ext_fraccion_72_max"] == (known["dia_racha_max"] >= 72.0)).all()
    assert known["ext_fraccion_72_horas"].between(0, 24).all() and known["ext_fraccion_72_horas"].max() >= 6
    assert (known["ext_fraccion_p98_horas"] >= known["ext_fraccion_p98_max"]).all()
    # The interactions multiply the excess of the gust over 0.9 times the p98 by rain and season.
    excess = (known["ext_racha_clima_max"] - 0.9).clip(lower=0.0)
    assert np.allclose(known["ext_int_racha_lluvia_previa"], excess * known["ayer_lluvia_max"], equal_nan=True)
    assert np.allclose(known["ext_int_racha_lluvia_dia"], excess * known["dia_lluvia_max"], equal_nan=True)
    july, january = known[known["fecha"].dt.month == 7], known[known["fecha"].dt.month == 1]
    assert np.allclose(july["ext_int_racha_hoja"], (july["ext_racha_clima_max"] - 0.9).clip(lower=0.0))
    assert np.allclose(july["ext_int_racha_verano"], july["ext_int_racha_hoja"])
    assert (january["ext_int_racha_hoja"] == 0).all() and (january["ext_int_racha_verano"] == 0).all()
    # A slope of the gust per network: the ratio in its own rows, 0 in the others.
    for dist in CELLS:
        own = known[known["distribuidora_id"] == dist]
        assert np.allclose(own[f"ext_pend_racha_dist_{dist}"], own["ext_racha_clima_max"])
        assert (known.loc[known["distribuidora_id"] != dist, f"ext_pend_racha_dist_{dist}"] == 0).all()
    # The direction: the storms blow from the south-west, the ones on days multiple of four from the north-east.
    storms = known[known["dia_racha_max"] >= 80.0]
    usual, odd = storms[storms["fecha"].dt.day % 4 != 0], storms[storms["fecha"].dt.day % 4 == 0]
    assert usual["ext_dir_anomalia"].median() < 25 and odd["ext_dir_anomalia"].median() > 155
    calm = known[known["ext_racha_clima_max"] < 1.0]
    assert (calm["ext_dir_anomalia"] == 0).all() and (calm["ext_dir_int_racha"] == 0).all()
    assert np.allclose(odd["ext_dir_int_racha"], (odd["ext_racha_clima_max"] - 0.9).clip(lower=0.0)
                       * odd["ext_dir_anomalia"] / 90.0)
    # The forecast twins, at every horizon, with the rain of the last day known at that horizon.
    for name in ("ext_fraccion_p98_max", "ext_fraccion_72_horas", "ext_int_racha_hoja", "ext_int_racha_lluvia_previa",
                 "ext_pend_racha_dist_1"):
        assert f"prev_{name}" in blocks["prevision"] and f"prev2_{name}" in blocks["prevision_48h"]
    assert "prev_ext_dir_anomalia" not in pdf.columns and not any("dir" in c for c in blocks["prevision"])
    archived = pdf[pdf["prev2_ext_racha_clima_max"].notna()]
    excess2 = (archived["prev2_ext_racha_clima_max"] - 0.9).clip(lower=0.0)
    assert np.allclose(archived["prev2_ext_int_racha_lluvia_previa"], excess2 * archived["hace2_lluvia_max"],
                       equal_nan=True)
    assert np.allclose(archived["prev_ext_int_racha_lluvia_dia"],
                       (archived["prev_ext_racha_clima_max"] - 0.9).clip(lower=0.0) * archived["prev_lluvia_max"],
                       equal_nan=True)
    # Without the flag, no direction and nothing else changes.
    _, plain, _ = rd.build_dataset(spark, config, rd.settings(config))
    assert "direccion" not in plain and set(plain["extension"]) == set(blocks["extension"])
    # The drought: the balance of 90 and 180 days as a z-score of the month; 2025 is the dry year of the fixture.
    assert set(blocks["sequia"]) == {"seq_3m", "seq_6m"}
    known = pdf[pdf["seq_6m"].notna()]
    assert known["fecha"].min() <= pd.Timestamp("2019-07-01")  # the 180 days need the weather before them
    dry = known[(known["fecha"] >= "2025-06-01") & (known["fecha"] < "2025-12-31")]
    wet = known[(known["fecha"] >= "2023-06-01") & (known["fecha"] < "2023-12-31")]
    assert dry["seq_6m"].mean() < -1.0 and abs(wet["seq_6m"].mean()) < 1.0
    # The freezing rain of 10.01.2024: CT-hours of precipitation at 0,5 degrees or less, in the reanalysis and in
    # the forecast of the day before (one degree warmer, still freezing); nothing the other days.
    assert set(blocks["hielo"]) == {"hie_horas"} and "prev_hie_horas" in blocks["prevision"]
    day = pdf[pdf["fecha"] == "2024-01-10"]
    assert day["hie_horas"].tolist() == pytest.approx([6.0, 6.0]) and day["prev_hie_horas"].tolist() == pytest.approx([6.0, 6.0])
    others = pdf[(pdf["fecha"] != "2024-01-10") & pdf["hie_horas"].notna()]
    assert (others["hie_horas"] == 0).all()


def test_the_models_of_the_hours_never_see_the_forecast_of_the_day_before(spark, tables):
    from job_ml_horas_riesgo import distributor_weather

    _, names = distributor_weather(spark, {"activo": True})
    assert names and not any("metp1_" in n or "metp2_" in n for n in names)
    _, names = distributor_weather(spark, {"activo": True}, prefixes=("met_", "metp1_"))
    assert "metd_max_metp1_racha_1h" in names
