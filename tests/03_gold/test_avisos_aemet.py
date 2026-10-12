"""
Warnings of AEMET from Bronze to Silver (cells inside the polygon of the
zone, wall clock of Madrid) and to the weather features of Gold (level in
force when it was already issued, upcoming warnings), on a local Spark.

    python -m pytest tests/03_gold/test_avisos_aemet.py -q
"""

import os
import sys
import time

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in ("01_bronze", "02_silver", "03_gold", "00_landing"):
    path = os.path.join(ROOT, "etl", "jobs", folder)
    if path not in sys.path:
        sys.path.insert(0, path)
TESTS_BRONZE = os.path.join(ROOT, "tests", "01_bronze")
if TESTS_BRONZE not in sys.path:
    sys.path.insert(0, TESTS_BRONZE)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

CELLS = [("40.30_-6.00", 40.3, -6.0), ("38.40_-6.80", 38.4, -6.8)]


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("avisos-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="module")
def silver_rows(spark):
    import job_bronze_datos_publicos as bronze
    import job_silver_contexto_publico as silver
    from test_datos_publicos import CAP_ALERT

    rows = bronze.parse_cap(CAP_ALERT.encode("utf-8"))
    cancel = {**rows[0], "identificador": "cancelado", "tipo_mensaje": "Cancel", "nivel": "rojo"}
    broken = {**rows[0], "identificador": "sin_poligono", "poligono": None}
    pdf = pd.DataFrame(rows + [cancel, broken]).astype("string")
    out, rejected = silver.weather_warnings(spark, spark.createDataFrame(pdf), CELLS)
    return out, rejected


def test_silver_cells_times_and_levels(silver_rows):
    out, rejected = silver_rows
    rows = out.toPandas()
    # Only the zone of the north of Cáceres holds a cell; the cell of the south is outside it.
    assert set(rows["celda_id"]) == {"40.30_-6.00"}
    alert = rows[rows["identificador"].str.endswith("VI")].iloc[0]
    assert alert["fenomeno_codigo"] == "VI" and alert["fenomeno_nombre"] == "Vientos"
    assert alert["nivel"] == "naranja" and alert["nivel_num"] == 2
    # 06:30 UTC in January is 07:30 in Madrid; the onset already comes in local time.
    assert alert["enviado_ts"] == pd.Timestamp("2024-01-10 07:30")
    assert alert["inicio_ts"] == pd.Timestamp("2024-01-10 12:00")
    assert alert["fin_ts"] == pd.Timestamp("2024-01-10 23:59:59")
    assert [r["_motivo"] for r in rejected] == ["NO_GEOMETRY"]


def test_gold_levels_in_force_and_upcoming(spark, silver_rows):
    import job_gold_meteo_celda_hora as gold
    import job_silver_f_meteo_hora as silver_weather

    out, _ = silver_rows
    hours = pd.date_range("2024-01-10 00:00", "2024-01-11 06:00", freq="h")
    weather = pd.DataFrame({"celda_id": "40.30_-6.00", "hora": hours, "temperature_2m": 8.0,
                            "relative_humidity_2m": 80.0, "precipitation": 0.0, "snowfall": 0.0,
                            "wind_speed_10m": 20.0, "wind_gusts_10m": 60.0, "pressure_msl": 1005.0,
                            "modelo": "ecmwf_ifs"})
    hourly, _ = silver_weather.transform(spark.createDataFrame(weather))
    features, names = gold.weather_features(hourly, 0, False)
    features, names = gold.add_warnings(features, names, out, 6)
    assert {"met_aviso_max_nivel", "met_aviso_viento_nivel", "met_aviso_lluvia_nivel", "met_aviso_tormenta_nivel",
            "met_aviso_nieve_nivel", "met_aviso_prox6h_max_nivel"} <= set(names)

    rows = features.toPandas().set_index("hora")
    # Before the first message of the archive nothing is known.
    assert pd.isna(rows.loc["2024-01-10 07:00", "met_aviso_max_nivel"])
    # Issued at 07:30 for 12:00: upcoming from 08:00 to 11:00, in force from the hour that ends at 13:00.
    assert rows.loc["2024-01-10 08:00", "met_aviso_prox6h_max_nivel"] == 2
    assert rows.loc["2024-01-10 11:00", "met_aviso_prox6h_max_nivel"] == 2
    assert rows.loc["2024-01-10 12:00", "met_aviso_max_nivel"] == 0
    assert rows.loc["2024-01-10 13:00", "met_aviso_max_nivel"] == 2
    assert rows.loc["2024-01-10 13:00", "met_aviso_viento_nivel"] == 2
    assert rows.loc["2024-01-10 13:00", "met_aviso_lluvia_nivel"] == 0
    # The hour that ends at midnight still overlaps it; the cancel message never counts (it was red).
    assert rows.loc["2024-01-11 00:00", "met_aviso_max_nivel"] == 2
    assert rows.loc["2024-01-11 01:00", "met_aviso_max_nivel"] == 0
    assert rows["met_aviso_max_nivel"].max() == 2
