"""
CAPE of the archived GFS runs from Bronze to Silver (nearest convective cell
of 0.25 degrees) and to the weather features of Gold, on a local Spark.

    python -m pytest tests/03_gold/test_meteo_cape.py -q
"""

import os
import sys
import time

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in ("02_silver", "03_gold"):
    path = os.path.join(ROOT, "etl", "jobs", folder)
    if path not in sys.path:
        sys.path.insert(0, path)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("cape-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_cape_reaches_silver_and_gold(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_f_meteo_hora as silver

    hours = pd.date_range("2022-06-01 00:00", periods=12, freq="h")
    reanalysis = pd.concat([
        pd.DataFrame({"celda_id": cell, "hora": hours, "temperature_2m": 25.0, "relative_humidity_2m": 50.0,
                      "precipitation": 0.0, "snowfall": 0.0, "wind_speed_10m": 10.0, "wind_gusts_10m": 20.0,
                      "pressure_msl": 1015.0, "modelo": "ecmwf_ifs"})
        for cell in ("39.40_-6.30", "40.20_-5.80")
    ], ignore_index=True)
    # Two convective cells; the second hour of the first one is out of range.
    convective = pd.concat([
        pd.DataFrame({"celda_id": "39.50_-6.25", "hora": hours, "cape": [100.0, 20000.0] + [1500.0] * 10,
                      "lifted_index": -2.0}),
        pd.DataFrame({"celda_id": "40.25_-5.75", "hora": hours, "cape": 10.0, "lifted_index": 4.0}),
    ], ignore_index=True)

    out, _ = silver.transform(spark.createDataFrame(reanalysis))
    out = silver.add_convective(out, spark.createDataFrame(convective))
    rows = {(r["celda_id"], r["hora"]): r for r in out.collect()}

    first = rows[("39.40_-6.30", hours[0].to_pydatetime())]
    assert first["cape_jkg"] == 100.0 and first["indice_levantamiento"] == -2.0
    assert rows[("39.40_-6.30", hours[1].to_pydatetime())]["cape_jkg"] is None
    assert rows[("40.20_-5.80", hours[5].to_pydatetime())]["cape_jkg"] == 10.0

    features, names = gold.weather_features(out, 0, False)
    assert {"met_cape_1h", "met_cape_max_6h", "met_cape_max_24h", "met_li_min_6h"} <= set(names)
    last = {r["celda_id"]: r for r in features.where(features.hora == hours[-1].to_pydatetime()).collect()}
    assert last["39.40_-6.30"]["met_cape_max_24h"] == 1500.0
    assert last["39.40_-6.30"]["met_li_min_6h"] == -2.0
    assert last["40.20_-5.80"]["met_cape_max_6h"] == 10.0
