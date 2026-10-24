"""
Auditoria integral A09: in job_silver_f_meteo_hora the flag fuera_rango has to
see the value that was out of range, not the null it becomes after the cleaning.
"""

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "etl", "jobs", "02_silver"))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import job_silver_f_meteo_hora as job  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("meteo-fuera-rango")
               .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


def test_a_value_out_of_range_becomes_null_and_raises_the_flag(spark):
    df = spark.createDataFrame(
        [("39.40_-6.30", "2026-01-01 00:00:00", 999.0), ("39.40_-6.30", "2026-01-01 01:00:00", 25.0),
         ("39.40_-6.30", "2026-01-01 02:00:00", None)],
        "celda_id string, hora string, temperature_2m double")
    out, _ = job.transform(df)
    rows = dict(enumerate(out.orderBy("hora").collect()))
    assert rows[0]["temperatura_c"] is None and rows[0]["fuera_rango"] is True
    assert rows[1]["temperatura_c"] == 25.0 and rows[1]["fuera_rango"] is False
    assert rows[2]["temperatura_c"] is None and rows[2]["fuera_rango"] is False
