"""
Active fires of FIRMS from Bronze to Silver (confidence level, wall clock of
Madrid, nearest cell of the study) and to the weather features of Gold (fires
near the cell in the last 24 h, counted once FIRMS would have published them),
on a local Spark.

    python -m pytest tests/03_gold/test_focos_firms.py -q
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

CELLS = [("40.20_-5.90", 40.2, -5.9), ("38.40_-6.80", 38.4, -6.8)]


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("focos-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def bronze_rows():
    def row(lat, lon, clock, confidence, frp, satellite="N", instrument="VIIRS", day="2025-08-12"):
        return {"latitud": str(lat), "longitud": str(lon), "fecha": day, "hora_utc": clock, "satelite": satellite,
                "instrumento": instrument, "confianza": confidence, "frp": str(frp), "dia_noche": "D",
                "fuente": f"{instrument}_SP"}
    return pd.DataFrame([
        row(40.25, -5.95, "1342", "n", 12.0),            # 6 km from the first cell
        row(40.25, -5.95, "1342", "n", 12.0),            # the same pass twice
        row(40.21, -5.92, "1342", "h", 30.0),
        row(40.30, -6.10, "1342", "l", 50.0),            # low confidence: not counted in Gold
        row(39.30, -6.30, "1342", "n", 9.0),             # far from every cell: no cell
        row(40.22, -5.91, "1100", "80", 20.0, "Terra", "MODIS", "2025-08-13"),
        row(None, -5.9, "1200", "n", 1.0),               # no position
    ]).astype("string")


def test_silver_level_clock_and_cell(spark):
    import job_silver_contexto_publico as silver

    out, rejected = silver.hotspots(spark, spark.createDataFrame(bronze_rows()), CELLS)
    rows = out.toPandas().sort_values(["ts", "latitud"]).reset_index(drop=True)
    assert sorted(r["_motivo"] for r in rejected) == ["DUPLICATE_KEY", "NULL_KEY"]
    assert len(rows) == 5
    first = rows.iloc[0]
    # 13:42 UTC in August is 15:42 in Madrid.
    assert first["ts_utc"] == pd.Timestamp("2025-08-12 13:42") and first["ts"] == pd.Timestamp("2025-08-12 15:42")
    assert set(rows.loc[rows["ts_utc"] == pd.Timestamp("2025-08-12 13:42"), "confianza_nivel"]) == {1.0, 2.0, 3.0}
    assert rows.loc[rows["latitud"] == 39.3, "celda_id"].isna().all()
    assert (rows.loc[rows["latitud"] == 40.21, "celda_id"] == "40.20_-5.90").all()
    modis = rows[rows["sensor"] == "MODIS"].iloc[0]
    assert modis["confianza_nivel"] == 3.0 and modis["celda_id"] == "40.20_-5.90"
    assert silver.confidence_level(pd.Series(["l", "n", "h", "10", "50", "95", ""])).tolist()[:6] == [
        1.0, 2.0, 3.0, 1.0, 2.0, 3.0]


def test_the_cell_is_the_nearest_in_km_not_in_degrees(spark):
    import job_silver_contexto_publico as silver

    # A fire 0.08 degrees south of one cell and 0.10 west of another: in degrees the first is nearer,
    # in km the second (0.10 degrees of longitude at 39.5 are 8.6 km, 0.08 of latitude 8.9 km).
    cells = [("39.58_-6.30", 39.58, -6.30), ("39.50_-6.20", 39.50, -6.20)]
    fire = pd.DataFrame([{"latitud": "39.5", "longitud": "-6.3", "fecha": "2025-08-12", "hora_utc": "1342", "satelite": "N",
                          "instrumento": "VIIRS", "confianza": "n", "frp": "5.0", "dia_noche": "D", "fuente": "VIIRS_SP"}]).astype("string")
    out, _ = silver.hotspots(spark, spark.createDataFrame(fire), cells)
    row = out.toPandas().iloc[0]
    assert row["celda_id"] == "39.50_-6.20"
    assert 8.5 < row["distancia_celda_km"] < 8.7


def test_gold_fires_near_the_cell_in_the_last_day(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_contexto_publico as silver

    out, _ = silver.hotspots(spark, spark.createDataFrame(bronze_rows()), CELLS)
    hours = pd.date_range("2025-08-12 00:00", "2025-08-14 12:00", freq="h")
    features = spark.createDataFrame(pd.concat([
        pd.DataFrame({"celda_id": cell, "hora": hours, "met_racha_1h": 20.0}) for cell, _, _ in CELLS
    ], ignore_index=True))
    features, names = gold.add_hotspots(features, ["met_racha_1h"], out, radius_km=15, latency_h=3)
    assert names[-2:] == ["met_focos_24h", "met_focos_frp_max_24h"]
    rows = features.toPandas().set_index(["celda_id", "hora"]).sort_index()
    north = rows.loc["40.20_-5.90"]
    # The fires pass at 15:42 local time: published at 18:42, counted from 19:00.
    assert north.loc["2025-08-12 18:00", "met_focos_24h"] == 0
    assert north.loc["2025-08-12 19:00", "met_focos_24h"] == 2
    assert north.loc["2025-08-12 19:00", "met_focos_frp_max_24h"] == 30.0
    # The MODIS pass of 13.08 at 13:00 local time joins them at 16:00; 24 h after 19:00 the first two leave.
    assert north.loc["2025-08-13 16:00", "met_focos_24h"] == 3
    assert north.loc["2025-08-13 19:00", "met_focos_24h"] == 1
    # Before the first fire of the archive nothing is known; the far cell has none.
    assert pd.isna(north.loc["2025-08-12 10:00", "met_focos_24h"])
    assert rows.loc[("38.40_-6.80", pd.Timestamp("2025-08-12 20:00")), "met_focos_24h"] == 0
