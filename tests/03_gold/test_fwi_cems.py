"""
Fire danger indices of CEMS from Bronze to Silver (nearest point of the grid
of 0.25 degrees for every cell, the consolidated dataset over the
intermediate one) and to Gold (the value of the day before for every hour of
the day), on a local Spark.

    python -m pytest tests/03_gold/test_fwi_cems.py -q
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

CELLS = [("40.20_-5.90", 40.2, -5.9), ("38.40_-6.80", 38.4, -6.8), ("42.00_-2.00", 42.0, -2.0)]


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("fwi-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def bronze_rows():
    def row(day, lat, lon, fwi, piece="2025_consolidated_01-12"):
        return {"fecha": day, "latitud": lat, "longitud": lon, "fwi": fwi, "ffmc": 90.0, "dmc": 100.0, "dc": 600.0,
                "isi": 10.0, "bui": 150.0, "pieza": piece}
    rows = []
    for day, base in (("2025-08-11", 40.0), ("2025-08-12", 60.0)):
        rows += [row(day, 40.30, -6.00, base), row(day, 40.05, -6.00, base + 1),
                 row(day, 38.30, -6.75, base + 5), row(day, 38.55, -6.75, base + 6)]
    rows += [row("2025-08-12", 40.30, -6.00, 99.0, "2025_intermediate_08-12"),   # the consolidated one stays
             row(None, 40.30, -6.00, 1.0)]                                         # no date
    pdf = pd.DataFrame(rows)
    pdf["fwi"] = pdf["fwi"].astype("float64")
    return pdf


def test_silver_nearest_point_and_dataset(spark):
    import job_silver_contexto_publico as silver

    out, rejected = silver.fire_danger(spark, spark.createDataFrame(bronze_rows()), CELLS)
    rows = out.toPandas().sort_values(["celda_id", "fecha"]).reset_index(drop=True)
    assert sorted(r["_motivo"] for r in rejected) == ["DUPLICATE_KEY", "NULL_KEY"]
    # The cell far from the grid gets nothing; the other two, two days each.
    assert sorted(rows["celda_id"].unique()) == ["38.40_-6.80", "40.20_-5.90"]
    north = rows[rows["celda_id"] == "40.20_-5.90"]
    assert north["latitud_punto"].tolist() == [40.3, 40.3] and north["fwi"].tolist() == [40.0, 60.0]
    assert (north["conjunto"] == "consolidated").all()
    assert north["distancia_punto_km"].iloc[0] == pytest.approx(111.2 * ((0.1 ** 2 + (0.1 * 0.7638) ** 2) ** 0.5),
                                                                rel=1e-3)
    south = rows[rows["celda_id"] == "38.40_-6.80"]
    assert south["latitud_punto"].tolist() == [38.3, 38.3] and south["fwi"].tolist() == [45.0, 65.0]
    assert str(rows["fecha"].iloc[0]) == "2025-08-11" and set(rows["anio"]) == {2025}


def test_gold_value_of_the_day_before(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_contexto_publico as silver

    out, _ = silver.fire_danger(spark, spark.createDataFrame(bronze_rows()), CELLS)
    hours = pd.date_range("2025-08-11 00:00", "2025-08-13 23:00", freq="h")
    features = spark.createDataFrame(pd.concat([
        pd.DataFrame({"celda_id": cell, "hora": hours, "met_racha_1h": 20.0}) for cell, _, _ in CELLS
    ], ignore_index=True))
    features, names = gold.add_fire_danger(features, ["met_racha_1h"], out)
    assert names[1:] == ["met_fwi_ayer", "met_ffmc_ayer", "met_dmc_ayer", "met_dc_ayer", "met_isi_ayer",
                         "met_bui_ayer"]
    rows = features.toPandas().set_index(["celda_id", "hora"]).sort_index()
    north = rows.loc["40.20_-5.90"]
    # The index of 11.08 is known on 12.08 from midnight to 23:00; 11.08 itself has no day before.
    assert pd.isna(north.loc["2025-08-11 15:00", "met_fwi_ayer"])
    assert north.loc["2025-08-12 00:00", "met_fwi_ayer"] == 40.0
    assert north.loc["2025-08-12 23:00", "met_fwi_ayer"] == 40.0
    assert north.loc["2025-08-13 08:00", "met_fwi_ayer"] == 60.0
    assert north.loc["2025-08-13 08:00", "met_dc_ayer"] == 600.0
    assert rows.loc["42.00_-2.00", "met_fwi_ayer"].isna().all()
    assert len(rows) == len(hours) * len(CELLS)
