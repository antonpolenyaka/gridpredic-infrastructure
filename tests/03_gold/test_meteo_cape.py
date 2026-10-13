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


def test_antecedent_rain_and_humidity(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_f_meteo_hora as silver

    hours = pd.date_range("2022-01-01 00:00", periods=40 * 24, freq="h")
    rain = pd.Series(0.0, index=hours)
    rain[pd.Timestamp("2022-01-03 10:00")] = 4.0     # 37 days before the check: out of every window
    rain[pd.Timestamp("2022-01-20 10:00")] = 6.0     # inside the previous 30 days
    rain[pd.Timestamp("2022-02-07 12:00")] = 2.0     # 47 h before: in the 72 h, not in the 24 h
    rain[pd.Timestamp("2022-02-09 06:00")] = 1.0     # in the last 24 h
    humidity = pd.Series(60.0, index=hours)
    humidity[pd.Timestamp("2022-02-09 07:00")] = 98.0
    frame = pd.DataFrame({"celda_id": "39.40_-6.30", "hora": hours, "temperature_2m": 10.0,
                          "relative_humidity_2m": humidity.to_numpy(), "precipitation": rain.to_numpy(),
                          "snowfall": 0.0, "wind_speed_10m": 10.0, "wind_gusts_10m": 20.0, "pressure_msl": 1015.0,
                          "modelo": "ecmwf_ifs"})

    out, _ = silver.transform(spark.createDataFrame(frame))
    features, names = gold.weather_features(out, 0, False)
    assert {"met_lluvia_72h", "met_lluvia_previa_30d", "met_humedad_max_6h"} <= set(names)

    row = features.where(features.hora == pd.Timestamp("2022-02-09 11:00").to_pydatetime()).first()
    assert row["met_lluvia_24h"] == 1.0
    assert row["met_lluvia_72h"] == 3.0
    # From 30 days to 24 h before: the rain of 20.01 and of 07.02, not the one of 03.01.
    assert row["met_lluvia_previa_30d"] == 8.0
    assert row["met_humedad_max_6h"] == 98.0
    first = features.where(features.hora == hours[0].to_pydatetime()).first()
    assert first["met_lluvia_previa_30d"] is None
    # On 20.01 the window holds 18 of the 29 days: not enough to say it was dry.
    short = features.where(features.hora == pd.Timestamp("2022-01-20 00:00").to_pydatetime()).first()
    assert short["met_lluvia_previa_30d"] is None


def test_extra_variables_reach_silver_and_gold(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_f_meteo_hora as silver

    hours = pd.date_range("2022-06-01 00:00", periods=10 * 24, freq="h")
    reanalysis = pd.DataFrame({"celda_id": "39.40_-6.30", "hora": hours, "temperature_2m": 10.0,
                               "relative_humidity_2m": 80.0, "precipitation": 0.0, "snowfall": 0.0,
                               "wind_speed_10m": 10.0, "wind_gusts_10m": 20.0, "pressure_msl": 1015.0,
                               "modelo": "ecmwf_ifs"})
    code = pd.Series(3.0, index=hours)
    code[pd.Timestamp("2022-06-05 14:00")] = 95.0
    extra = pd.DataFrame({
        "celda_id": "39.50_-6.25", "hora": hours, "wind_direction_10m": 90.0, "wind_speed_100m": 30.0,
        "soil_moisture_0_to_7cm": 0.31, "soil_moisture_28_to_100cm": 0.35, "dew_point_2m": 5.0,
        "vapour_pressure_deficit": 0.5, "cloud_cover_low": 40.0, "et0_fao_evapotranspiration": 0.1,
        "weather_code": code.to_numpy(), "shortwave_radiation": 300.0,
    })
    # A far cell of the coarse grid: never the nearest one.
    far = extra.assign(celda_id="40.25_-5.75", wind_direction_10m=270.0)

    # A cell of the study with no coarse cell within one step (its own still loading): nulls.
    lonely = reanalysis.assign(celda_id="41.20_-7.40")
    out, _ = silver.transform(spark.createDataFrame(pd.concat([reanalysis, lonely], ignore_index=True)))
    out = silver.add_extra(out, spark.createDataFrame(pd.concat([extra, far], ignore_index=True)))
    when = pd.Timestamp("2022-06-05 15:00").to_pydatetime()
    row = out.where((out.hora == when) & (out.celda_id == "39.40_-6.30")).first()
    assert row["direccion_viento_grados"] == 90.0 and row["humedad_suelo_0_7_m3m3"] == 0.31
    assert row["codigo_tiempo"] == 3.0
    alone = out.where((out.hora == when) & (out.celda_id == "41.20_-7.40")).first()
    assert alone["direccion_viento_grados"] is None and alone["punto_rocio_c"] is None
    out = out.where(out.celda_id == "39.40_-6.30")

    features, names = gold.weather_features(out, 0, False)
    expected = {"met_dir_viento_sin", "met_dir_viento_cos", "met_viento_100m_1h", "met_humedad_suelo_0_7",
                "met_humedad_suelo_28_100", "met_rocio_dif_1h", "met_dpv_max_24h", "met_nubes_bajas_6h",
                "met_et0_7d", "met_codigo_max_6h", "met_tormenta_6h"}
    assert expected <= set(names)
    after = features.where(features.hora == pd.Timestamp("2022-06-05 18:00").to_pydatetime()).first()
    assert after["met_dir_viento_sin"] == pytest.approx(1.0) and after["met_dir_viento_cos"] == pytest.approx(0.0)
    assert after["met_rocio_dif_1h"] == 5.0
    assert after["met_tormenta_6h"] == 1.0 and after["met_codigo_max_6h"] == 95.0
    # The series starts on 01.06 at 00:00: 115 hours inside the 7 days.
    assert after["met_et0_7d"] == pytest.approx(0.1 * 115)
    later = features.where(features.hora == pd.Timestamp("2022-06-05 21:00").to_pydatetime()).first()
    assert later["met_tormenta_6h"] == 0.0


def test_forecast_of_the_day_before_reaches_silver_and_gold(spark):
    import job_gold_meteo_celda_hora as gold
    import job_silver_f_meteo_hora as silver

    hours = pd.date_range("2024-03-01 00:00", periods=48, freq="h")
    reanalysis = pd.DataFrame({"celda_id": "39.40_-6.30", "hora": hours, "temperature_2m": 12.0,
                               "relative_humidity_2m": 70.0, "precipitation": 0.0, "snowfall": 0.0,
                               "wind_speed_10m": 15.0, "wind_gusts_10m": 30.0, "pressure_msl": 1012.0,
                               "modelo": "ecmwf_ifs"})
    forecast = pd.DataFrame({
        "celda_id": "39.40_-6.30", "hora": hours, "wind_gusts_10m_previous_day1": 45.0,
        "wind_speed_10m_previous_day1": 20.0, "precipitation_previous_day1": [0.5] * 47 + [500.0],
        "snowfall_previous_day1": 0.0, "temperature_2m_previous_day1": 13.0,
        "relative_humidity_2m_previous_day1": 65.0, "wind_gusts_10m_previous_day2": 50.0,
        "precipitation_previous_day2": 1.0, "wind_gusts_10m": 40.0, "precipitation": 0.2,
    })
    # The forecast of a neighbour cell of the same grid never goes to this one.
    other = forecast.assign(celda_id="39.50_-6.30", wind_gusts_10m_previous_day1=99.0)

    out, _ = silver.transform(spark.createDataFrame(reanalysis))
    out = silver.add_forecast(out, spark.createDataFrame(pd.concat([forecast, other], ignore_index=True)))
    rows = out.toPandas().set_index("hora")
    assert rows["racha_prev1_kmh"].eq(45.0).all() and rows["racha_icon_kmh"].eq(40.0).all()
    assert rows["temperatura_prev1_c"].eq(13.0).all() and rows["racha_prev2_kmh"].eq(50.0).all()
    # 500 mm in an hour is out of range: null, the rest of the hour stays.
    assert pd.isna(rows.iloc[-1]["precipitacion_prev1_mm"]) and rows.iloc[-1]["racha_prev1_kmh"] == 45.0

    features, names = gold.weather_features(out, 3, False)
    assert {"metp1_racha_1h", "metp1_viento_1h", "metp1_lluvia_1h", "metp1_nieve_1h", "metp1_temp_1h",
            "metp1_humedad_1h", "metp2_racha_1h", "metp2_lluvia_1h"} <= set(names)
    first = features.where(features.hora == hours[5].to_pydatetime()).first()
    assert first["metp1_racha_1h"] == 45.0 and first["met_racha_1h"] == 30.0
