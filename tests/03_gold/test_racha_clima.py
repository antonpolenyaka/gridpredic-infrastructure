"""
Gust of every hour against the climate of its cell in Gold (met_racha_clima_1h
and met_racha_ku_1h): the 98th percentile of the gusts of each cell before
climatologia_hasta, on a local Spark.

    python -m pytest tests/03_gold/test_racha_clima.py -q
"""

import os
import sys
import time

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "03_gold")
if path not in sys.path:
    sys.path.insert(0, path)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("racha-clima-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_gust_against_the_climate_of_its_cell(spark):
    import job_gold_meteo_celda_hora as gold

    hours = pd.date_range("2021-12-01 00:00", "2022-01-02 23:00", freq="h")
    rows = []
    # A windy mountain cell (gusts of 60 - 99 km/h) and a calm one (20 - 59), the same storm of 100 km/h after the
    # climate period, and one hour without gust.
    for cell, low in (("40.30_-5.70", 60.0), ("40.30_-5.90", 20.0)):
        gust = low + (np.arange(len(hours)) % 40)
        frame = pd.DataFrame({"celda_id": cell, "hora": hours, "met_racha_1h": gust.astype(float)})
        frame.loc[frame["hora"] == pd.Timestamp("2022-01-02 12:00"), "met_racha_1h"] = 100.0
        frame.loc[frame["hora"] == pd.Timestamp("2022-01-02 13:00"), "met_racha_1h"] = np.nan
        rows.append(frame)
    features = spark.createDataFrame(pd.concat(rows, ignore_index=True))
    out, names = gold.add_gust_climate(features, ["met_racha_1h"], "2022-01-01")
    assert names == ["met_racha_1h", "met_racha_clima_1h", "met_racha_ku_1h"]
    pdf = out.toPandas().set_index(["celda_id", "hora"]).sort_index()

    storm = pdf.xs(pd.Timestamp("2022-01-02 12:00"), level="hora")
    # p98 of the windy cell is about 98 km/h, of the calm one about 58: the same gust is ordinary up there.
    assert storm.loc["40.30_-5.70", "met_racha_clima_1h"] == pytest.approx(100 / 98, rel=0.02)
    assert storm.loc["40.30_-5.90", "met_racha_clima_1h"] == pytest.approx(100 / 58, rel=0.02)
    ratio = storm.loc["40.30_-5.90", "met_racha_clima_1h"]
    assert storm.loc["40.30_-5.90", "met_racha_ku_1h"] == pytest.approx((ratio - 1) ** 3)
    assert storm.loc["40.30_-5.70", "met_racha_ku_1h"] < 0.001
    assert pd.isna(pdf.loc[("40.30_-5.90", pd.Timestamp("2022-01-02 13:00")), "met_racha_clima_1h"])
    # Below the climate the index is zero.
    assert pdf.loc[("40.30_-5.90", pd.Timestamp("2021-12-01 00:00")), "met_racha_ku_1h"] == 0.0


def test_forecast_gust_against_the_same_climate(spark):
    import job_gold_meteo_celda_hora as gold

    hours = pd.date_range("2021-12-01 00:00", "2022-01-02 23:00", freq="h")
    gust = 20.0 + (np.arange(len(hours)) % 40)
    frame = pd.DataFrame({"celda_id": "40.30_-5.90", "hora": hours, "met_racha_1h": gust,
                          "metp1_racha_1h": np.nan})
    # The forecast of the day before only exists for the last day: 116 km/h, twice the climate of the cell.
    frame.loc[frame["hora"] == pd.Timestamp("2022-01-02 12:00"), "metp1_racha_1h"] = 116.0
    out, names = gold.add_gust_climate(spark.createDataFrame(frame), ["met_racha_1h", "metp1_racha_1h"],
                                       "2022-01-01")
    assert names[-4:] == ["met_racha_clima_1h", "met_racha_ku_1h", "metp1_racha_clima_1h", "metp1_racha_ku_1h"]
    pdf = out.toPandas().set_index("hora").sort_index()
    storm = pdf.loc[pd.Timestamp("2022-01-02 12:00")]
    # The p98 comes from the reanalysis (about 58 km/h), not from the forecast.
    assert storm["metp1_racha_clima_1h"] == pytest.approx(116 / 58, rel=0.02)
    assert storm["metp1_racha_ku_1h"] == pytest.approx((storm["metp1_racha_clima_1h"] - 1) ** 3)
    assert pdf["metp1_racha_clima_1h"].notna().sum() == 1
