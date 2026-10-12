"""
Tests of the weather Landing and Bronze helpers, without Spark or network.

    python -m pytest tests/01_bronze -q
"""

import os
import sys
from datetime import date

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in ("00_landing", "01_bronze"):
    path = os.path.join(ROOT, "etl", "jobs", folder)
    if path not in sys.path:
        sys.path.insert(0, path)

import job_bronze_meteo_openmeteo as bronze
import job_landing_meteo_openmeteo as landing

VARIABLES = ["temperature_2m", "precipitation", "wind_gusts_10m"]


def test_cells_group_close_municipalities():
    rows = [
        {"codigo_ine": "10001", "municipio": "A", "distribuidora": "1", "latitud": "40.25922", "longitud": "-5.97828"},
        {"codigo_ine": "10002", "municipio": "B", "distribuidora": "1", "latitud": "40,27", "longitud": "-6,01"},
        {"codigo_ine": "6001", "municipio": "C", "distribuidora": "3", "latitud": "38.4", "longitud": "-6.9"},
        {"codigo_ine": "10003", "municipio": "Sin", "distribuidora": "1", "latitud": None, "longitud": None},
    ]
    places = landing.municipality_cells(rows, 0.1)

    assert [p["municipio"] for p in places] == ["A", "B", "C"]
    assert places[0]["celda_id"] == places[1]["celda_id"] == "40.30_-6.00"
    assert places[2]["codigo_ine"] == "06001"


def test_year_chunks_and_weight():
    chunks = landing.year_chunks(date(2020, 12, 1), date(2026, 8, 31))

    assert chunks[0] == (2020, date(2020, 12, 1), date(2020, 12, 31))
    assert chunks[-1] == (2026, date(2026, 1, 1), date(2026, 8, 31))
    assert len(chunks) == 7
    assert landing.call_weight(365, 7) == pytest.approx(365 / 14)
    assert landing.call_weight(3, 12) == pytest.approx(1.2)


def test_url_asks_unix_time_in_utc():
    cfg = {"variables": VARIABLES, "modelo": "ecmwf_ifs"}
    url = landing.request_url(40.3, -6.0, date(2025, 1, 1), date(2025, 1, 2), cfg)

    assert "timeformat=unixtime" in url and "timezone=GMT" in url
    assert "hourly=temperature_2m,precipitation,wind_gusts_10m" in url
    assert "models=ecmwf_ifs" in url


def test_local_time_and_change_to_winter_time():
    # 26.10.2025: 00:00 and 01:00 UTC are both 02:00 in Madrid (CEST, then CET).
    utc = pd.date_range("2025-10-25 23:00", periods=4, freq="h", tz="UTC")
    payload = {
        "hourly": {
            "time": [int(t.timestamp()) for t in utc],
            "temperature_2m": [10.0, 9.0, 8.0, 7.0],
            "precipitation": [0.5, 1.0, 2.0, 0.0],
            "wind_gusts_10m": [30.0, 50.0, 40.0, 20.0],
        },
        "_peticion": {"celda_id": "40.30_-6.00"},
    }

    frame = bronze.payload_frame(payload, VARIABLES, "Europe/Madrid")
    assert frame["hora"].tolist() == [pd.Timestamp("2025-10-26 01:00"), pd.Timestamp("2025-10-26 02:00"),
                                      pd.Timestamp("2025-10-26 02:00"), pd.Timestamp("2025-10-26 03:00")]

    merged = bronze.merge_local_hours(frame, VARIABLES).set_index("hora")
    assert len(merged) == 3
    assert merged.loc["2025-10-26 02:00", "precipitation"] == pytest.approx(3.0)
    assert merged.loc["2025-10-26 02:00", "wind_gusts_10m"] == pytest.approx(50.0)
    assert merged.loc["2025-10-26 02:00", "temperature_2m"] == pytest.approx(8.5)


def test_extra_variables_merge_directions_as_vectors():
    utc = pd.date_range("2025-10-25 23:00", periods=4, freq="h", tz="UTC")
    variables = ["wind_direction_10m", "et0_fao_evapotranspiration", "weather_code"]
    payload = {
        "hourly": {
            "time": [int(t.timestamp()) for t in utc],
            "wind_direction_10m": [90.0, 350.0, 10.0, 180.0],
            "et0_fao_evapotranspiration": [0.0, 0.1, 0.2, 0.0],
            "weather_code": [3, 61, 95, 3],
        },
        "_peticion": {"celda_id": "39.50_-6.25"},
    }
    merged = bronze.merge_local_hours(bronze.payload_frame(payload, variables, "Europe/Madrid"), variables)
    merged = merged.set_index("hora")
    assert list(merged.columns) == ["celda_id", *variables]
    # 350 and 10 degrees fold into north, not into south.
    folded = merged.loc["2025-10-26 02:00", "wind_direction_10m"]
    assert min(folded, 360 - folded) == pytest.approx(0.0, abs=1e-6)
    assert merged.loc["2025-10-26 01:00", "wind_direction_10m"] == pytest.approx(90.0)
    assert merged.loc["2025-10-26 02:00", "et0_fao_evapotranspiration"] == pytest.approx(0.3)
    assert merged.loc["2025-10-26 02:00", "weather_code"] == 95


def test_extra_source_config():
    cfg = landing.load_config()["openmeteo_extra"]
    assert cfg["celda_grados"] == 0.25 and len(cfg["variables"]) <= 10
    url = landing.request_url(39.5, -6.25, date(2017, 1, 1), date(2017, 12, 31), cfg)
    assert url.startswith(landing.API) and "wind_direction_10m" in url and "models=ecmwf_ifs" in url
    # Up to 10 variables cost the same as one in the free plan.
    assert landing.call_weight(365, len(cfg["variables"])) == landing.call_weight(365, 1)


def test_convective_source_url_and_model_list():
    cfg = landing.load_config()["openmeteo_convectivo"]
    assert landing.models_of(cfg)[0] == "gfs_seamless"
    url = landing.request_url(39.5, -6.25, date(2022, 6, 1), date(2022, 6, 30), cfg, "gfs_seamless")
    assert url.startswith("https://historical-forecast-api.open-meteo.com/v1/forecast?")
    assert "hourly=cape,lifted_index" in url and "models=gfs_seamless" in url
    # The reanalysis keeps its single model and its API.
    archive = landing.load_config()["openmeteo"]
    assert landing.models_of(archive) == ["ecmwf_ifs"]
    assert landing.request_url(40.3, -6.0, date(2017, 1, 1), date(2017, 1, 2), archive).startswith(landing.API)


def test_stored_window_must_cover_the_year():
    # A first run from 01.12.2020 left a 2020 file with one month: it is asked again for the whole year.
    assert not landing.covers(["2020-12-01", "2020-12-31"], date(2020, 1, 1), date(2020, 12, 31))
    assert landing.covers(["2021-01-01", "2021-12-31"], date(2021, 1, 1), date(2021, 12, 31))
    assert landing.covers(["2026-01-01", "2026-08-31"], date(2026, 1, 1), date(2026, 8, 31))
    assert not landing.covers(None, date(2021, 1, 1), date(2021, 12, 31))


def test_forecast_source_url_and_merge_of_suffixed_variables():
    cfg = landing.load_config()["openmeteo_previo"]
    assert len(cfg["variables"]) <= 10 and cfg["celda_grados"] == 0.1
    url = landing.request_url(40.3, -6.0, date(2024, 1, 1), date(2024, 12, 31), cfg)
    assert url.startswith("https://previous-runs-api.open-meteo.com/v1/forecast?")
    assert "wind_gusts_10m_previous_day1" in url and "models=icon_eu" in url
    assert bronze.forecast_of("precipitation_previous_day2") == "precipitation"
    assert bronze.forecast_of("wind_gusts_10m") == "wind_gusts_10m"

    # The forecast of the hour that repeats when the clock goes back is merged like the variable it forecasts.
    utc = pd.date_range("2025-10-25 23:00", periods=4, freq="h", tz="UTC")
    variables = ["wind_gusts_10m_previous_day1", "precipitation_previous_day1", "temperature_2m_previous_day1"]
    payload = {
        "hourly": {
            "time": [int(t.timestamp()) for t in utc],
            "wind_gusts_10m_previous_day1": [30.0, 50.0, 40.0, None],
            "precipitation_previous_day1": [0.5, 1.0, 2.0, 0.0],
            "temperature_2m_previous_day1": [10.0, 9.0, 8.0, 7.0],
        },
        "_peticion": {"celda_id": "40.30_-6.00"},
    }
    merged = bronze.merge_local_hours(bronze.payload_frame(payload, variables, "Europe/Madrid"), variables)
    merged = merged.set_index("hora")
    assert merged.loc["2025-10-26 02:00", "wind_gusts_10m_previous_day1"] == pytest.approx(50.0)
    assert merged.loc["2025-10-26 02:00", "precipitation_previous_day1"] == pytest.approx(3.0)
    assert merged.loc["2025-10-26 02:00", "temperature_2m_previous_day1"] == pytest.approx(8.5)
    assert pd.isna(merged.loc["2025-10-26 03:00", "wind_gusts_10m_previous_day1"])
