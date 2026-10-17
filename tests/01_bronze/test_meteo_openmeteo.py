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
    # Twelve variables (one, two and three days before, and the run of the day): 1.2 calls of the free plan.
    assert len(cfg["variables"]) == 12 and cfg["celda_grados"] == 0.1
    assert landing.call_weight(3, len(cfg["variables"])) == pytest.approx(1.2)
    url = landing.request_url(40.3, -6.0, date(2024, 1, 1), date(2024, 12, 31), cfg)
    assert url.startswith("https://previous-runs-api.open-meteo.com/v1/forecast?")
    assert "wind_gusts_10m_previous_day1" in url and "wind_gusts_10m_previous_day3" in url and "models=icon_eu" in url
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


def test_a_stored_answer_without_a_new_variable_is_asked_again(tmp_path):
    import json

    class Local:
        def open(self, path, mode="r"):
            return open(path, mode, encoding="utf-8")

    stored = tmp_path / "2024.json"
    stored.write_text(json.dumps({"hourly": {"time": [0], "wind_gusts_10m_previous_day1": [30.0],
                                             "precipitation_previous_day1": [0.0]}}), encoding="utf-8")
    assert landing.missing_variables(Local(), str(stored), ["wind_gusts_10m_previous_day1"]) == []
    assert landing.missing_variables(Local(), str(stored), ["wind_gusts_10m_previous_day1",
                                                            "wind_gusts_10m_previous_day3"]) == [
        "wind_gusts_10m_previous_day3"]


def single_run_answer(lat: float, lon: float, run, gust_of_hour) -> dict:
    """One location of the Single Runs API: 72 hours from the run, unix time in UTC."""
    times = pd.date_range(run, periods=72, freq="h", tz="UTC")
    return {"latitude": lat, "longitude": lon, "elevation": 400.0,
            "hourly": {"time": [int(t.timestamp()) for t in times],
                       "wind_gusts_10m": [gust_of_hour(t) for t in times],
                       "precipitation": [0.1] * 72, "wind_speed_10m": [10.0] * 72,
                       "temperature_2m": [12.0] * 72, "relative_humidity_2m": [80.0] * 72}}


def test_the_forecast_of_the_day_before_from_single_runs(monkeypatch, tmp_path):
    import json
    from datetime import datetime, timezone

    import fsspec
    import job_landing_meteo_pasadas as pasadas

    cfg = landing.load_config()["openmeteo_pasadas_ifs"]
    assert cfg["modelo"] == "ecmwf_ifs" and cfg["sufijo"] == "_previous_day1" and cfg["desde"] == "2024-03-15"
    # The run of 00 UTC of the day before, asked with every cell at once.
    run = pasadas.run_of(date(2025, 1, 15), cfg)
    assert run == datetime(2025, 1, 14, 0, tzinfo=timezone.utc)
    url = pasadas.request_url([("a", (39.5, -6.4)), ("b", (40.1, -5.9))], run, cfg)
    for part in ("single-runs-api.open-meteo.com", "latitude=39.5000,40.1000", "longitude=-6.4000,-5.9000",
                 "models=ecmwf_ifs", "run=2025-01-14T00:00", "forecast_days=3", "timeformat=unixtime"):
        assert part in url

    # The hours of the day in the wall clock of Madrid, with the suffix of the Previous Runs API.
    location = single_run_answer(39.5, -6.4, "2025-01-14 00:00", lambda t: float(t.hour))
    hours = pasadas.hours_of_day(location, date(2025, 1, 15), "Europe/Madrid", ["wind_gusts_10m"], "_previous_day1")
    assert len(hours["time"]) == 24 and list(hours) == ["time", "wind_gusts_10m_previous_day1"]
    # 00:00 in Madrid in winter is 23:00 UTC of the day before.
    assert hours["wind_gusts_10m_previous_day1"][0] == 23.0
    # The day the clock goes back has 25 hours.
    autumn = single_run_answer(39.5, -6.4, "2025-10-25 00:00", lambda t: 1.0)
    assert len(pasadas.hours_of_day(autumn, date(2025, 10, 26), "Europe/Madrid", ["wind_gusts_10m"], "_x")["time"]) == 25

    # The whole Landing on a memory file system: a budget of two runs a day, a run that is not in the archive.
    memory = fsspec.filesystem("memory")
    memory.store.clear()
    memory.pseudo_dirs.clear()
    memory.pseudo_dirs.append("")
    cells = [{"celda_id": "39.50_-6.40", "celda_latitud": 39.5, "celda_longitud": -6.4},
             {"celda_id": "40.10_-5.90", "celda_latitud": 40.1, "celda_longitud": -5.9}]
    monkeypatch.setattr(pasadas, "read_reference", lambda fs, path: [])
    monkeypatch.setattr(pasadas, "municipality_cells", lambda rows, size: cells)
    config = landing.load_config()
    config["openmeteo_pasadas_ifs"] = {**cfg, "landing": "memory://pasadas", "desde": "2025-01-13",
                                       "hasta": "2025-01-16", "max_llamadas_dia": 4, "pausa_s": 0}
    path = str(tmp_path / "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    asked = []

    def fake_fetch(url, retries, wait_s):
        stamp = url.split("run=")[1][:16]
        asked.append(stamp)
        if stamp.startswith("2025-01-12"):
            raise RuntimeError('HTTP 400 for url: {"reason":"The requested model run is not available"}')
        return [single_run_answer(39.5, -6.4, stamp, lambda t: 30.0), single_run_answer(40.1, -5.9, stamp, lambda t: 50.0)]

    monkeypatch.setattr(pasadas, "fetch", fake_fetch)
    clock = {"now": datetime(2026, 10, 17, 19, 0, tzinfo=timezone.utc)}
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        clock["now"] = clock["now"] + pd.Timedelta(seconds=seconds).to_pytimedelta()

    # Two runs fit in the budget of the day (two cells, one call each): it stops with the code of the daily limit.
    code = pasadas.main(["--config", path], fs=memory, clock=lambda: clock["now"], sleep=sleep)
    assert code == landing.DAILY_LIMIT_EXIT and asked == ["2025-01-15T00:00", "2025-01-14T00:00"]
    # Waiting for the next day (UTC) it ends the window; the missing run is noted, never asked again.
    code = pasadas.main(["--config", path, "--esperar"], fs=memory, clock=lambda: clock["now"], sleep=sleep)
    assert code == 0 and asked[2:] == ["2025-01-13T00:00", "2025-01-12T00:00"]
    assert any(s > 3600 for s in slept)
    assert json.loads(memory.cat("/pasadas/_sin_pasada.json")) == {"2025-01-13": "2025-01-12T00:00:00+00:00"}
    usage = json.loads(memory.cat("/pasadas/_llamadas.json"))
    assert usage == {"2026-10-17": 4.0, "2026-10-18": 4.0}
    saved = json.loads(memory.cat("/pasadas/dias/2025-01-16.json"))
    assert saved["_peticion"]["pasada_utc"] == "2025-01-15T00:00:00+00:00"
    assert [u["_peticion"]["celda_id"] for u in saved["ubicaciones"]] == ["39.50_-6.40", "40.10_-5.90"]
    assert pasadas.main(["--config", path], fs=memory, clock=lambda: clock["now"], sleep=sleep) == 0
    assert len(asked) == 4

    # Bronze reads every location of the file as an answer of its own, with the suffixed names.
    variables = [f"{v}{cfg['sufijo']}" for v in cfg["variables"]]
    frames = [bronze.payload_frame(answer, variables, "Europe/Madrid") for answer in bronze.answers_of(saved)]
    hourly = bronze.merge_local_hours(pd.concat(frames, ignore_index=True), variables)
    assert hourly["celda_id"].value_counts().to_dict() == {"39.50_-6.40": 24, "40.10_-5.90": 24}
    assert hourly.groupby("celda_id")["wind_gusts_10m_previous_day1"].max().to_dict() == {
        "39.50_-6.40": 30.0, "40.10_-5.90": 50.0}
    assert (hourly["hora"].dt.date == date(2025, 1, 16)).all()
    assert bronze.answers_of({"hourly": {}}) == [{"hourly": {}}]
