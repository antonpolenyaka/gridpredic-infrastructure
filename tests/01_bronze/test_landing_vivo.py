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
    assert saved["_peticion"]["reserva"] is False
    assert saved["_peticion"]["dias_futuros"] == 3 and len(saved["hourly"]["time"]) == 72
    # The cell that fails is asked to the reserve model too before giving up.
    assert sorted(asked) == ["Open-Meteo gfs_seamless 38.70_-6.90", "Open-Meteo icon_eu 38.70_-6.90",
                             "Open-Meteo icon_eu 39.50_-6.40"]


def test_the_reserve_model_when_icon_eu_does_not_answer(fs, cfg, monkeypatch):
    asked = []

    def fake_fetch(url, label=None, **kwargs):
        asked.append(label)
        if "models=icon_eu" in url:
            raise RuntimeError("HTTP 503 for Open-Meteo")
        return answer()

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)
    cells = {f"39.{i}0_-6.40": (39.0 + i / 10, -6.4) for i in range(6)}
    assert vivo.land_forecast(fs, cfg, "/vivo/20261015", cells) == (6, 0)
    saved = json.loads(fs.cat("/vivo/20261015/prevision/39.50_-6.40.json"))
    assert saved["_peticion"]["modelo"] == "gfs_seamless" and saved["_peticion"]["reserva"] is True
    # After three cells in a row without ICON-EU the rest go straight to the reserve.
    assert len([a for a in asked if "icon_eu" in a]) == 3 and len([a for a in asked if "gfs_seamless" in a]) == 6


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


def ensemble_answer(day: date, peak_by_member: list, peak_hour_utc: int = 10) -> bytes:
    """What the Ensemble API gives: the control run and the members, 3 days from the day before the one to predict."""
    start = int(datetime(day.year, day.month, day.day, tzinfo=ZoneInfo("UTC")).timestamp()) - 24 * 3600
    times = [start + 3600 * h for h in range(72)]
    hourly = {"time": times}
    for k, peak in enumerate(peak_by_member):
        values = [20.0 + (peak - 20.0) * (1.0 if (t - start) // 3600 == 24 + peak_hour_utc else 0.0) for t in times]
        hourly["wind_gusts_10m" if k == 0 else f"wind_gusts_10m_member{k:02d}"] = values
    return json.dumps({"latitude": 39.5, "longitude": -6.4, "hourly": hourly}).encode()


def test_the_ensemble_of_every_network(fs, cfg, monkeypatch, tmp_path):
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    config["riesgo_manana"]["ensemble"]["usar_celdas"] = False
    config["riesgo_manana"]["ensemble"]["puntos"] = {"3": [{"nombre": "Hervás", "lat": 40.27, "lon": -5.87}, {"nombre": "Plasencia", "lat": 40.03, "lon": -6.09}],
                                                      "2": [{"nombre": "Casar de Cáceres", "lat": 39.56, "lon": -6.42}]}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
    day = date(2026, 10, 15)
    calls = []

    def fake_fetch(url, **kwargs):
        calls.append(url)
        if "models=icon_eu&" in url and "latitude=39.5600" in url:
            raise RuntimeError("HTTP 400")  # icon_eu fails for this point: the next model answers
        peaks = [60, 80, 100, 40, 55] if "latitude=40.2700" in url else [50, 50, 50, 50, 50]
        return ensemble_answer(day, peaks)

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)
    assert vivo.main(["--fuente", "ensemble", "--fecha", day.isoformat(), "--config", str(path), "--dashboard-dir", str(tmp_path)], fs=fs) == 0
    assert sorted(fs.ls("/vivo/20261015/ensemble", detail=False)) == ["/vivo/20261015/ensemble/2_Casar_de_C_ceres.json", "/vivo/20261015/ensemble/3_Herv_s.json",
                                                                       "/vivo/20261015/ensemble/3_Plasencia.json"]
    assert any("models=icon_seamless" in u and "latitude=39.5600" in u for u in calls)
    with open(tmp_path / "ensemble.json", encoding="utf-8") as file:
        out = json.load(file)
    assert out["fecha"] == "2026-10-15" and out["umbrales_kmh"] == [50.0, 70.0, 90.0]
    d1, d2 = out["distribuidoras"]
    assert d1["id"] == 3 and d1["nombre"] == "Distribuidora 1" and d1["puntos"] == ["Hervás", "Plasencia"] and d1["miembros"] == 5 and d1["modelo"] == "icon_eu"
    # The gust over the network is the worst of the points: the peak at 12:00 local (10:00 UTC) of every member.
    assert len(d1["horas"]) == 24 and d1["horas"][0]["hora"] == 0
    noon = next(h for h in d1["horas"] if h["hora"] == 12)
    # Members over the network: max(Hervás, Plasencia) = 60, 80, 100, 50, 55.
    assert noon["p50"] == 60 and noon["p10"] == 52 and noon["p90"] == 92
    assert d1["pico"] == {"p10": 52, "p50": 60, "p90": 92}
    assert d1["supera"] == {"50": 1.0, "70": 0.4, "90": 0.2}
    assert d1["texto"] == "5 escenarios de ICON-EU EPS en 2 puntos de la red: el 80 % central da el pico de racha entre 52 y 92 km/h (mediana 60); el 100 % supera 50 km/h, el 40 % supera 70 km/h, el 20 % supera 90 km/h; escenarios muy repartidos; más de la mitad supera 50 km/h"
    assert d2["modelo"] == "icon_seamless" and d2["pico"] == {"p10": 50, "p50": 50, "p90": 50} and "muy de acuerdo" in d2["texto"]
    # Without any answer the source fails with 2 and writes nothing.
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: (_ for _ in ()).throw(RuntimeError("HTTP 500")))
    assert vivo.main(["--fuente", "ensemble", "--fecha", "2026-10-16", "--config", str(path), "--dashboard-dir", str(tmp_path)], fs=fs) == 2


def test_the_points_of_the_ensemble_from_the_cells_of_the_reference(fs, monkeypatch):
    rows = [{"codigo_ine": "10001", "municipio": "Abadía", "distribuidora": "1", "latitud": "40.259", "longitud": "-5.978"},
            {"codigo_ine": "10006", "municipio": "Ahigal", "distribuidora": "1", "latitud": "40.190", "longitud": "-6.188"},
            {"codigo_ine": "10007", "municipio": "Albalá", "distribuidora": "1", "latitud": "40.255", "longitud": "-5.981"},  # same cell as Abadía
            {"codigo_ine": "10037", "municipio": "Brozas", "distribuidora": "2", "latitud": "39.613", "longitud": "-6.778"},
            {"codigo_ine": "06143", "municipio": "Valle de Santa Ana", "distribuidora": "3", "latitud": "38.366", "longitud": "-6.789"},
            {"codigo_ine": "", "municipio": "TOTAL", "distribuidora": "", "latitud": None, "longitud": None}]
    monkeypatch.setattr(vivo, "load_meteo_config", lambda: {"openmeteo": {"referencia_municipios": "x", "celda_grados": 0.1}})
    monkeypatch.setattr(vivo, "read_reference", lambda fs, path: rows)
    points = vivo.ensemble_points(fs, {"usar_celdas": True, "referencia_distribuidoras": {"1": "3", "2": "2", "3": "1366"}})
    assert sorted(points) == ["1366", "2", "3"]
    assert [p["nombre"] for p in points["3"]] == ["40.30_-6.00", "40.20_-6.20"] and points["3"][0]["lat"] == 40.3 and points["3"][0]["lon"] == -6.0
    assert points["2"] == [{"nombre": "39.60_-6.80", "lat": 39.6, "lon": -6.8}] and len(points["1366"]) == 1
    assert vivo.ensemble_points(fs, {"usar_celdas": False, "puntos": {"3": [{"nombre": "a", "lat": 1, "lon": 2}]}}) == {"3": [{"nombre": "a", "lat": 1, "lon": 2}]}


def aemet_municipality(ine: str, name: str, day: date, peak: float, peak_hour: int, storm: float = 0.0) -> dict:
    """The hourly forecast of AEMET as it comes: a list with one element, three days, wind and gusts interleaved."""
    def hours(d: date, first: int):
        wind, rain, temp = [], [], []
        for h in range(first, 24):
            gust = peak if h == peak_hour else 15.0 + h / 10
            wind.append({"direccion": ["SE"], "velocidad": [str(int(gust * 0.6))], "periodo": f"{h:02d}"})
            wind.append({"value": str(int(gust)), "periodo": f"{h:02d}"})
            rain.append({"value": "Ip" if h == peak_hour else "0", "periodo": f"{h:02d}"})
            temp.append({"value": "17", "periodo": f"{h:02d}"})
        return {"fecha": f"{d.isoformat()}T00:00:00", "vientoAndRachaMax": wind, "precipitacion": rain, "temperatura": temp,
                "probTormenta": [{"value": "0", "periodo": "0208"}, {"value": str(int(storm)), "periodo": "0814"}, {"value": "0", "periodo": "1420"}]}
    before = day - timedelta(days=1)
    return [{"origen": {}, "elaborado": f"{before.isoformat()}T18:55:08", "nombre": name, "provincia": "Cáceres", "id": ine, "version": "1.0",
             "prediccion": {"dia": [hours(before, 19), hours(day, 0), hours(day + timedelta(days=1), 0)]}}]


def test_the_hourly_forecast_of_aemet_by_municipality(fs, cfg, monkeypatch, tmp_path):
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
    day = date(2026, 10, 15)
    rows = [{"codigo_ine": "10148", "municipio": "Plasencia", "distribuidora": "1", "latitud": "40.03", "longitud": "-6.09"},
            {"codigo_ine": "10103", "municipio": "Hervás", "distribuidora": "1", "latitud": "40.27", "longitud": "-5.87"},
            {"codigo_ine": "10049", "municipio": "Casar de Cáceres", "distribuidora": "2", "latitud": "39.56", "longitud": "-6.42"},
            {"codigo_ine": "", "municipio": "TOTAL", "distribuidora": "", "latitud": None, "longitud": None}]
    monkeypatch.setattr(vivo, "load_meteo_config", lambda: {"openmeteo": {"referencia_municipios": "x", "celda_grados": 0.1}})
    monkeypatch.setattr(vivo, "read_reference", lambda fs, path: rows)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)
    answers = {"10148": aemet_municipality("10148", "Plasencia", day, 46, 12, storm=40), "10103": aemet_municipality("10103", "Hervás", day, 58, 13),
               "10049": aemet_municipality("10049", "Casar de Cáceres", day, 39, 11)}

    def fake_fetch(url, **kwargs):
        if "/municipio/horaria/" in url:
            ine = url.rsplit("/", 1)[1]
            if ine == "10049":
                raise RuntimeError("HTTP 429 for AEMET municipio 10049")
            return json.dumps({"estado": 200, "datos": f"https://opendata.aemet.es/opendata/sh/{ine}"}).encode()
        ine = url.rsplit("/", 1)[1]
        text = json.dumps(answers[ine], ensure_ascii=False)
        return b"\xef\xbb\xbf" + text.encode("utf-8") if ine == "10148" else text.encode("iso-8859-15")  # both ways AEMET serves it

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    monkeypatch.delenv("AEMET_API_KEY", raising=False)
    assert vivo.main(["--fuente", "aemet_municipios", "--fecha", day.isoformat(), "--config", str(path), "--dashboard-dir", str(tmp_path)], fs=fs) == 2
    monkeypatch.setenv("AEMET_API_KEY", "k")
    # One of three municipalities fails (above max_fallos of 0.3): code 1, but what landed is used.
    assert vivo.main(["--fuente", "aemet_municipios", "--fecha", day.isoformat(), "--config", str(path), "--dashboard-dir", str(tmp_path)], fs=fs) == 1
    assert sorted(fs.ls("/vivo/20261015/aemet", detail=False)) == ["/vivo/20261015/aemet/10103.json", "/vivo/20261015/aemet/10148.json"]
    with open(tmp_path / "aemet.json", encoding="utf-8") as file:
        out = json.load(file)
    assert out["fecha"] == "2026-10-15" and [d["id"] for d in out["distribuidoras"]] == [3]
    d1 = out["distribuidoras"][0]
    assert d1["nombre"] == "Distribuidora 1" and d1["municipios"] == 2 and len(d1["horas"]) == 24 and d1["elaborado"] == "2026-10-14T18:55:08"
    assert d1["pico"] == {"racha_kmh": 58, "hora": 13, "municipio": "Hervás"}
    noon = next(h for h in d1["horas"] if h["hora"] == 12)
    assert noon["racha_kmh"] == 46 and noon["lluvia_mm"] == 0.1 and noon["tormenta_pct"] == 40
    assert next(h for h in d1["horas"] if h["hora"] == 3)["tormenta_pct"] == 0
    assert d1["texto"] == "AEMET por municipios (2 municipios, elaborada 18:55): pico de racha 58 km/h a las 13:00 en Hervás; hasta 0,2 mm de lluvia; probabilidad de tormenta hasta el 40 %"
    assert (tmp_path / "aemet_20261015.json").exists()
    assert vivo.aemet_value("Ip") == 0.1 and vivo.aemet_value("") != vivo.aemet_value("") and vivo.aemet_value("12,5") == 12.5


def test_the_evening_does_not_wait_for_aemet_beyond_its_budget(fs, monkeypatch, tmp_path):
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    config["riesgo_manana"]["aemet_municipios"]["tiempo_max_s"] = 5
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
    day = date(2026, 10, 15)
    rows = [{"codigo_ine": f"101{k:02d}", "municipio": f"M{k}", "distribuidora": "1", "latitud": "40.0", "longitud": "-6.0"} for k in range(6)]
    monkeypatch.setattr(vivo, "load_meteo_config", lambda: {"openmeteo": {"referencia_municipios": "x", "celda_grados": 0.1}})
    monkeypatch.setattr(vivo, "read_reference", lambda fs, path: rows)
    clock = {"t": 0.0}
    monkeypatch.setattr(vivo.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(vivo.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + 3.0))  # every municipality costs 3 "seconds"

    def fake_fetch(url, **kwargs):
        if "/municipio/horaria/" in url:
            return json.dumps({"estado": 200, "datos": "https://opendata.aemet.es/opendata/sh/" + url.rsplit("/", 1)[1]}).encode()
        ine = url.rsplit("/", 1)[1]
        return json.dumps(aemet_municipality(ine, "M", day, 40, 12), ensure_ascii=False).encode("utf-8")

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    monkeypatch.setenv("AEMET_API_KEY", "k")
    code = vivo.main(["--fuente", "aemet_municipios", "--fecha", day.isoformat(), "--config", str(path), "--dashboard-dir", str(tmp_path)], fs=fs)
    assert code == 1  # the municipalities left unasked count as failed, above max_fallos
    landed = fs.ls("/vivo/20261015/aemet", detail=False)
    assert 2 <= len(landed) <= 3
    with open(tmp_path / "aemet.json", encoding="utf-8") as file:
        out = json.load(file)
    assert out["distribuidoras"][0]["municipios"] == len(landed) and out["distribuidoras"][0]["pico"]["racha_kmh"] == 40


def test_the_nao_index_of_noaa(fs, monkeypatch):
    text = "year,month,day,nao_index_cdas\n2026,10,13,0.806\n2026,10,14,-0.21\n"
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: text.encode())
    assert vivo.land_nao(fs, {}, "/vivo/20261016")
    with fs.open("/vivo/20261016/nao.csv", "r") as file:
        assert file.read().splitlines()[-1] == "2026,10,14,-0.21"

    # Something that is not the CSV of CPC (a page of error) is not landed.
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: b"<html>maintenance</html>")
    assert not vivo.land_nao(fs, {}, "/vivo/20261017")
    assert not fs.exists("/vivo/20261017/nao.csv")


def test_the_thresholds_of_the_ensemble_stop_at_the_first_one_nobody_reaches():
    assert vivo.threshold_text({"50": 0.3, "70": 0.0, "90": 0.0}, [50, 70, 90]) == "el 30 % supera 50 km/h, ninguno supera 70 km/h"
    assert vivo.threshold_text({"50": 0.0, "70": 0.0, "90": 0.0}, [50, 70, 90]) == "ninguno supera 50 km/h"


def meta_answer(started: datetime, available_after_h: float = 3.0) -> bytes:
    """The meta.json of a domain of Open-Meteo: unix times of the last run and of when it was available."""
    start = int(started.timestamp())
    return json.dumps({"last_run_initialisation_time": start,
                       "last_run_availability_time": start + int(available_after_h * 3600),
                       "last_run_modification_time": start + int(available_after_h * 3600) - 60,
                       "update_interval_seconds": 10800, "temporal_resolution_seconds": 3600}).encode()


def test_the_run_that_open_meteo_serves(fs, cfg, monkeypatch):
    started = datetime.now(ZoneInfo("UTC")) - timedelta(hours=5)
    asked = []

    def fake_fetch(url, **kwargs):
        asked.append(url)
        return meta_answer(started)

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    run = vivo.model_run(cfg["prevision"])
    assert asked == ["https://api.open-meteo.com/data/dwd_icon_eu/static/meta.json"]
    assert run["modelo"] == "icon_eu" and run["dominio"] == "dwd_icon_eu"
    assert run["pasada_utc"] == started.replace(microsecond=0).isoformat()
    assert 4.9 < run["horas_desde_pasada"] < 5.2 and run["actualizacion_s"] == 10800
    # The native grid and IFS are their own domains.
    vivo.model_run(cfg["prevision"], "dwd_icon_eu_native")
    assert asked[-1].endswith("/data/dwd_icon_eu_native/static/meta.json")
    # An answer without a run is an error for model_run and only a warning when the evening saves it.
    monkeypatch.setattr(vivo, "fetch", lambda url, **kwargs: b'{"error": true, "reason": "The service is overloaded"}')
    with pytest.raises(RuntimeError):
        vivo.model_run(cfg["prevision"])
    assert vivo.land_model_run(fs, cfg["prevision"], "/vivo/20261014/prevision_modelo.json") is None
    assert not fs.exists("/vivo/20261014/prevision_modelo.json")


def test_the_shadow_forecasts_never_fail_the_evening(fs, cfg, monkeypatch, tmp_path):
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    cells = {f"39.{i}0_-6.40": (39.0 + i / 10, -6.4) for i in range(5)}
    monkeypatch.setattr(vivo, "load_meteo_config", lambda: {"openmeteo": {}})
    monkeypatch.setattr(vivo, "study_cells", lambda fs, meteo: cells)
    monkeypatch.setattr(vivo.time, "sleep", lambda s: None)
    started = datetime.now(ZoneInfo("UTC")) - timedelta(hours=4)
    asked = []

    def fake_fetch(url, **kwargs):
        asked.append(url)
        if "static/meta.json" in url:
            return meta_answer(started)
        if "models=ecmwf_ifs" in url:
            raise RuntimeError("HTTP 503 The service is overloaded")
        return answer()

    monkeypatch.setattr(vivo, "fetch", fake_fetch)
    # The forecast of the evening and its run; the shadow models are another task and are not asked here.
    assert vivo.main(["--fuente", "prevision", "--fecha", "2026-10-14", "--config", str(path)], fs=fs) == 0
    day = "/vivo/20261014"
    assert len(fs.ls(f"{day}/prevision", detail=False)) == 5
    run = json.loads(fs.cat(f"{day}/prevision_modelo.json"))
    assert run["dominio"] == "dwd_icon_eu" and 3.9 < run["horas_desde_pasada"] < 4.2
    assert not fs.exists(f"{day}/prevision_sombra")
    # The shadow task: the native grid lands in its own folder with its run; IFS is overloaded, and after three
    # cells in a row it is left for the evening instead of waiting for every cell.
    assert vivo.main(["--fuente", "prevision_sombra", "--fecha", "2026-10-14", "--config", str(path)], fs=fs) == 1
    native = sorted(fs.ls(f"{day}/prevision_sombra/icon_eu_nativo", detail=False))
    assert len(native) == 5 and native[0].endswith("/39.00_-6.40.json")
    saved = json.loads(fs.cat(native[0]))
    assert saved["_peticion"]["modelo"] == "dwd_icon_eu_native" and saved["_peticion"]["dias_pasados"] == 0
    assert fs.exists(f"{day}/prevision_sombra/icon_eu_nativo_modelo.json")
    assert not fs.exists(f"{day}/prevision_sombra/ifs")
    assert len([u for u in asked if "models=ecmwf_ifs" in u]) == 3
    shadow_urls = [u for u in asked if "models=dwd_icon_eu_native" in u]
    assert shadow_urls and all("past_days=0" in u and "forecast_days=3" in u for u in shadow_urls)


def test_the_shadow_forecasts_against_the_regular_grid(fs, monkeypatch, tmp_path):
    import job_comparar_sombra as sombra

    def write(path, peak, lat, rain=0.5, start="2026-10-17 22:00"):
        times = [int(datetime(2026, 10, 17, 22, tzinfo=ZoneInfo("UTC")).timestamp()) + 3600 * h for h in range(30)]
        gusts = [20.0] * 30
        gusts[10] = peak
        fs.pipe(path, json.dumps({"latitude": lat, "longitude": -6.4, "elevation": 400.0,
                                  "hourly": {"time": times, "wind_gusts_10m": gusts, "precipitation": [rain] * 30},
                                  "_peticion": {"celda_id": path.split("/")[-1][:-5]}}).encode())

    day = "/vivo/20261018"
    write(f"{day}/prevision/c1.json", 80.0, 39.5)
    write(f"{day}/prevision/c2.json", 50.0, 39.6)
    write(f"{day}/prevision_sombra/icon_eu_nativo/c1.json", 92.0, 39.52)
    write(f"{day}/prevision_sombra/icon_eu_nativo/c2.json", 52.0, 39.61)
    write(f"{day}/prevision_sombra/ifs/c1.json", 70.0, 39.47)
    fs.pipe("/meteo/municipios_celdas.json", json.dumps([
        {"celda_id": "c1", "distribuidora_ref": "1"}, {"celda_id": "c2", "distribuidora_ref": "1"}]).encode())
    config = vivo.load_config()
    config["riesgo_manana"]["landing"] = "/vivo"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    monkeypatch.setattr(sombra, "load_meteo_config", lambda: {"openmeteo": {"landing": "/meteo"}})
    out = tmp_path / "salida"
    code = sombra.main(["--desde", "2026-10-18", "--hasta", "2026-10-18", "--config", str(path), "--salida", str(out)],
                       fs=fs)
    assert code == 0
    import pandas as pd

    summary = pd.read_csv(out / "resumen.csv").set_index("fuente")
    # The worst cell of the network: 92 against 80 on the native grid, 70 (one cell only) against 80 for IFS.
    assert summary.loc["icon_eu_nativo", "red"] == "Distribuidora 1"
    assert summary.loc["icon_eu_nativo", "sesgo_racha_peor_kmh"] == pytest.approx(12.0)
    assert summary.loc["icon_eu_nativo", "dias_dif_mas_10_kmh"] == 1
    assert summary.loc["ifs", "sesgo_racha_peor_kmh"] == pytest.approx(-10.0)
    # The rain of the day is the sum of the 24 local hours of 18.10 (22 UTC of 17.10 is 00:00 in Madrid).
    cells = pd.read_csv(out / "celdas.csv")
    assert cells.loc[(cells["fuente"] == "icon_eu") & (cells["celda_id"] == "c1"), "lluvia"].iloc[0] == pytest.approx(12.0)
    points = pd.read_csv(out / "puntos.csv").set_index(["fuente", "celda_id"])
    assert 2.0 < points.loc[("icon_eu_nativo", "c1"), "km_al_punto_regular"] < 2.5
    assert "Previsión en sombra" in (out / "resumen.md").read_text(encoding="utf-8")
    assert summary.loc["ifs", "celdas_comparadas"] == 1 and summary.loc["ifs", "tardes_incompletas"] == 1
    assert summary.loc["icon_eu_nativo", "tardes_incompletas"] == 0
    # IFS has one cell of two, so its mean is compared on that cell only: 70 against 80, not against the mean 65.
    assert summary.loc["ifs", "sesgo_racha_media_kmh"] == pytest.approx(-10.0)
    # Without shadow files there is nothing to compare.
    assert sombra.main(["--desde", "2026-10-20", "--hasta", "2026-10-20", "--config", str(path)], fs=fs) == 1

    # Another evening: the regular grid lost its worst cell (c1) and gave c3 from the reserve model (GFS);
    # the shadow is compared on c2 only, the one cell both have from ICON-EU.
    def write_19(path, peak, lat, asked=None):
        times = [int(datetime(2026, 10, 18, 22, tzinfo=ZoneInfo("UTC")).timestamp()) + 3600 * h for h in range(30)]
        gusts = [20.0] * 30
        gusts[10] = peak
        fs.pipe(path, json.dumps({"latitude": lat, "longitude": -6.4, "elevation": 400.0,
                                  "hourly": {"time": times, "wind_gusts_10m": gusts, "precipitation": [0.5] * 30},
                                  "_peticion": {"celda_id": path.split("/")[-1][:-5], **(asked or {})}}).encode())

    day = "/vivo/20261019"
    write_19(f"{day}/prevision/c2.json", 50.0, 39.6)
    write_19(f"{day}/prevision/c3.json", 95.0, 39.7, {"modelo": "gfs025", "reserva": True})
    write_19(f"{day}/prevision_sombra/ifs/c1.json", 75.0, 39.47)
    write_19(f"{day}/prevision_sombra/ifs/c2.json", 40.0, 39.62)
    write_19(f"{day}/prevision_sombra/ifs/c3.json", 90.0, 39.7)
    fs.pipe("/meteo/municipios_celdas.json", json.dumps([
        {"celda_id": "c1", "distribuidora_ref": "1"}, {"celda_id": "c2", "distribuidora_ref": "1"},
        {"celda_id": "c3", "distribuidora_ref": "1"}]).encode())
    out2 = tmp_path / "salida2"
    assert sombra.main(["--desde", "2026-10-19", "--hasta", "2026-10-19", "--config", str(path), "--salida", str(out2)],
                       fs=fs) == 0
    late = pd.read_csv(out2 / "resumen.csv").set_index("fuente")
    # Comparing the aggregates of each source with its own cells would give 90 against 95 (-5); on c2, -10.
    assert late.loc["ifs", "sesgo_racha_peor_kmh"] == pytest.approx(-10.0)
    assert late.loc["ifs", "sesgo_racha_media_kmh"] == pytest.approx(-10.0)
    assert late.loc["ifs", "celdas_comparadas"] == 1 and late.loc["ifs", "tardes_incompletas"] == 1
