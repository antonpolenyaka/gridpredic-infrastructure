"""
Tests of the live Landing of the risk of tomorrow (job_landing_vivo): the
request of the forecast of ICON-EU, the files of every cell and the warnings
of AEMET in force, without network.

    python -m pytest tests/01_bronze/test_landing_vivo.py -q
"""

import json
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "00_landing")
if path not in sys.path:
    sys.path.insert(0, path)

import job_landing_vivo as vivo  # noqa: E402

KEY = "clave-de-prueba-123"


@pytest.fixture
def fs():
    import fsspec

    memory = fsspec.filesystem("memory")
    memory.store.clear()
    memory.pseudo_dirs.clear()
    memory.pseudo_dirs.append("")
    return memory


@pytest.fixture
def cfg():
    return vivo.load_config()["riesgo_manana"]


def answer(cell_hours: int = 72) -> bytes:
    return json.dumps({"latitude": 39.5, "longitude": -6.4,
                       "hourly": {"time": list(range(cell_hours)), "wind_gusts_10m": [30.0] * cell_hours}}).encode()


def test_the_request_of_the_forecast(cfg):
    url = vivo.forecast_url(39.5, -6.4, cfg["prevision"])
    assert url.startswith("https://api.open-meteo.com/v1/forecast?latitude=39.5000&longitude=-6.4000")
    for part in ("models=icon_eu", "past_days=31", "forecast_days=3", "timezone=GMT", "timeformat=unixtime",
                 "wind_speed_unit=kmh", "hourly=wind_gusts_10m,wind_speed_10m,precipitation,snowfall,"):
        assert part in url
    assert vivo.day_folder("s3://datalake/00_landing/vivo/", date(2026, 10, 14)) == (
        "s3://datalake/00_landing/vivo/20261014")


def test_the_day_to_predict():
    assert vivo.target_day("2026-10-14", "Europe/Madrid") == date(2026, 10, 14)
    tomorrow = datetime.now(ZoneInfo("Europe/Madrid")).date() + timedelta(days=1)
    assert vivo.target_day("", "Europe/Madrid") in (tomorrow, tomorrow + timedelta(days=1))


def test_the_forecast_of_every_cell(fs, cfg, monkeypatch):
    asked = []

    def fake_fetch(url, retries=4, wait_s=30, timeout=300, label=None, **kwargs):
        asked.append(label)
        if "latitude=38.7000" in url:
            raise RuntimeError("HTTP 500 for Open-Meteo")
        return answer()

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)
    cells = {"39.50_-6.40": (39.5, -6.4), "38.70_-6.90": (38.7, -6.9)}
    assert vivo.land_forecast(fs, cfg, "/vivo/20261014", cells) == (1, 1)
    assert fs.ls("/vivo/20261014/prevision", detail=False) == ["/vivo/20261014/prevision/39.50_-6.40.json"]
    saved = json.loads(fs.cat("/vivo/20261014/prevision/39.50_-6.40.json"))
    assert saved["_peticion"]["celda_id"] == "39.50_-6.40" and saved["_peticion"]["modelo"] == "icon_eu"
    assert saved["_peticion"]["dias_futuros"] == 3 and len(saved["hourly"]["time"]) == 72
    assert sorted(asked) == ["Open-Meteo 38.70_-6.90", "Open-Meteo 39.50_-6.40"]


def test_the_warnings_of_aemet(fs, cfg, monkeypatch, caplog):
    replies = {}

    def fake_fetch(url, data=None, headers=None, timeout=None, label=None, **kwargs):
        assert headers["api_key"] == KEY and KEY not in url
        reply = replies[url]
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    api = cfg["avisos"]["api"].format(area="esp")
    folder = "/vivo/20261014"

    monkeypatch.delenv("AEMET_API_KEY", raising=False)
    assert vivo.land_warnings(fs, cfg, folder) == (0, 1)

    monkeypatch.setenv("AEMET_API_KEY", KEY)
    replies[api] = json.dumps({"estado": 200, "datos": "https://opendata.aemet.es/datos/abc"}).encode()
    replies["https://opendata.aemet.es/datos/abc"] = b"tar de avisos"
    with caplog.at_level("INFO"):
        assert vivo.land_warnings(fs, cfg, folder) == (1, 0)
    assert fs.cat(f"{folder}/avisos/esp.tar") == b"tar de avisos"
    assert json.loads(fs.cat(f"{folder}/avisos/esp.json"))["bytes"] == 13

    # Later that evening AEMET has none: only the last answer stays.
    replies[api] = json.dumps({"descripcion": "No hay datos que satisfagan esos criterios", "estado": 404}).encode()
    assert vivo.land_warnings(fs, cfg, folder) == (1, 0)
    assert not fs.exists(f"{folder}/avisos/esp.tar") and not fs.exists(f"{folder}/avisos/esp.json")
    assert json.loads(fs.cat(f"{folder}/avisos/esp.vacio.json"))["area"] == "esp"

    # "No data" with the HTTP code 404 is no warning too; any other error is a failure.
    replies[api] = RuntimeError('HTTP 404 for AEMET avisos esp: {"descripcion": "No hay datos que satisfagan"}')
    assert vivo.land_warnings(fs, cfg, folder) == (1, 0)
    replies[api] = RuntimeError("HTTP 401 for AEMET avisos esp: API key invalido")
    with caplog.at_level("INFO"):
        assert vivo.land_warnings(fs, cfg, folder) == (0, 1)
    assert fs.exists(f"{folder}/avisos/esp.vacio.json")
    assert KEY not in caplog.text


def test_main_fails_when_most_cells_have_no_forecast(fs, cfg, monkeypatch, tmp_path):
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    cells = {f"39.{i}0_-6.40": (39.0 + i / 10, -6.4) for i in range(10)}
    monkeypatch.setattr(vivo, "load_meteo_config", lambda: {"openmeteo": {}})
    monkeypatch.setattr(vivo, "study_cells", lambda fs, meteo: cells)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)

    # One cell of ten fails: within max_fallos.
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: (_ for _ in ()).throw(RuntimeError("HTTP 500"))
                        if "latitude=39.0000" in url else answer())
    assert vivo.main(["--fuente", "prevision", "--fecha", "2026-10-14", "--config", str(path)], fs=fs) == 0
    assert len(fs.ls("/vivo/20261014/prevision", detail=False)) == 9

    # Half of them fail: no forecast for tomorrow.
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: (_ for _ in ()).throw(RuntimeError("HTTP 500"))
                        if url.split("latitude=")[1][3] in "01234" else answer())
    assert vivo.main(["--fuente", "prevision", "--fecha", "2026-10-15", "--config", str(path)], fs=fs) == 1
