"""
Tests of the live observations of AEMET (job_landing_observacion): the
records inside the box, the Landing, the stations near every distribuidora
and observado.json for the dashboard, without network.

    python -m pytest tests/01_bronze/test_landing_observacion.py -q
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "00_landing")
if path not in sys.path:
    sys.path.insert(0, path)

import job_landing_observacion as obs  # noqa: E402

NOW = datetime(2026, 7, 20, 13, 25, tzinfo=timezone.utc)  # 15:25 in Madrid


def records():
    """What AEMET gives: the last 24 hours of three stations, one of them outside the box."""
    out = []
    for h in range(14, 38):  # 19.07 14:00 UTC to 20.07 13:00 UTC
        ts = datetime(2026, 7, 19, 0, 0) if h < 24 else datetime(2026, 7, 20, 0, 0)
        hour = h if h < 24 else h - 24
        gust = 8.0 + (12.0 if (h >= 32 and h <= 35) else 0.0)  # 72 km/h from 10:00 to 13:00 UTC of 20.07
        out.append({"idema": "3469A", "ubi": "CÁCERES", "lat": 39.47, "lon": -6.34, "alt": 394, "fint": f"{ts:%Y-%m-%d}T{hour:02d}:00:00",
                    "vmax": gust, "vv": gust * 0.6, "prec": 0.0 if h != 33 else 2.4, "ta": 30.0, "hr": 40})
        out.append({"idema": "4410X", "ubi": "PLASENCIA", "lat": 40.03, "lon": -6.09, "alt": 350, "fint": f"{ts:%Y-%m-%d}T{hour:02d}:00:00",
                    "vmax": 5.0, "vv": "3", "prec": None, "ta": 28.5, "hr": 45})
        out.append({"idema": "3195", "ubi": "MADRID", "lat": 40.41, "lon": -3.68, "alt": 667, "fint": f"{ts:%Y-%m-%d}T{hour:02d}:00:00",
                    "vmax": 20.0, "vv": 10.0, "prec": 0.0, "ta": 35.0, "hr": 20})
    out.append({"idema": "sin", "ubi": "SIN COORDENADAS", "fint": "2026-07-20T13:00:00", "vmax": 1.0})
    return out


@pytest.fixture
def fs():
    import fsspec

    memory = fsspec.filesystem("memory")
    memory.store.clear()
    memory.pseudo_dirs.clear()
    memory.pseudo_dirs.append("")
    return memory


def test_the_records_inside_the_box_and_the_landing(fs):
    cfg = obs.observation_settings(obs.load_config())
    frame = obs.inside(records(), cfg["caja"])
    assert sorted(frame["idema"].unique()) == ["3469A", "4410X"]
    assert len(frame) == 48 and frame["ts_utc"].dt.tz is not None
    assert frame.loc[frame["idema"] == "4410X", "viento_ms"].iloc[0] == 3.0
    assert frame.loc[frame["idema"] == "4410X", "lluvia_mm"].isna().all()
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    path = obs.land(fs, cfg, frame, NOW)
    assert path.endswith("/observacion/20260720/obs_1325.json")
    with fs.open(path, "r") as file:
        saved = json.load(file)
    assert saved["pedido_utc"] == "2026-07-20T13:25:00Z" and len(saved["registros"]) == 48
    assert saved["registros"][0]["ts_utc"].endswith("Z")
    assert obs.decode(b"\xef\xbb\xbf[1]") == "[1]" and obs.decode("CÁCERES".encode("iso-8859-15")) == "CÁCERES"


def test_the_dashboard_sees_the_gust_of_the_day_near_every_network(fs, tmp_path, monkeypatch):
    cfg = obs.observation_settings(obs.load_config())
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    risk = {"distribuidoras": [
        {"id": 2, "nombre": "Pitarch", "municipios": [{"nombre": "Casar de Cáceres", "lat": 39.56, "lon": -6.42}]},
        {"id": 3, "nombre": "EOSA", "municipios": [{"nombre": "Hervás", "lat": 40.27, "lon": -5.86}]},
        {"id": 1366, "nombre": "Valle de Santa Ana", "municipios": [{"nombre": "Valle de Santa Ana", "lat": 38.37, "lon": -6.78}]},
    ]}
    with open(tmp_path / "riesgo.json", "w", encoding="utf-8") as file:
        json.dump(risk, file)
    monkeypatch.setenv("AEMET_API_KEY", "k")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW, records=records()) == 0
    with open(tmp_path / "observado.json", encoding="utf-8") as file:
        out = json.load(file)
    assert out["fecha"] == "2026-07-20" and out["generado"] == "2026-07-20T13:25Z"
    assert [s["idema"] for s in out["estaciones"]] == ["3469A", "4410X"]
    caceres = out["estaciones"][0]
    assert caceres["hora"] == "15:00" and caceres["racha_kmh"] == 29 and caceres["temp_c"] == 30.0
    pitarch, eosa, vsa = out["distribuidoras"]
    # Casar de Cáceres is 12 km from the station of Cáceres; Hervás is 33 km from Plasencia: none within 25 km.
    assert pitarch["estaciones"] == 1 and pitarch["racha_max_kmh"] == 72 and pitarch["hora_racha_max"] == 10
    assert len(pitarch["horas"]) == 16 and pitarch["horas"][0]["hora"] == 0
    assert next(h for h in pitarch["horas"] if h["hora"] == 11)["lluvia_mm"] == 2.4
    assert pitarch["texto"].startswith("racha máxima observada hoy 72 km/h a las 10:00")
    assert eosa["estaciones"] == 0 and eosa["texto"] == "sin estaciones cerca de la red" and eosa["horas"] == []
    assert vsa["estaciones"] == 0
    # Without the key nothing is asked; with a failing API the code says it.
    monkeypatch.delenv("AEMET_API_KEY")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW) == 2
    monkeypatch.setenv("AEMET_API_KEY", "k")
    monkeypatch.setattr(obs, "ask_observations", lambda cfg, key: (_ for _ in ()).throw(RuntimeError("HTTP 429")))
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW) == 1


def forecast(day: str, peak_hour: int, peak: float, window: tuple) -> dict:
    """What dag_riesgo_manana leaves in riesgo_<day>.json, reduced to what the verification reads."""
    profile = [{"hora": h, "racha_max": peak if h == peak_hour else 20.0, "lluvia": 0.0} for h in range(24)]
    return {"fecha": day, "distribuidoras": [
        {"id": 2, "nombre": "Pitarch", "nivel": "alto", "probabilidad": 0.12, "perfil": profile, "franja": {"inicio": window[0], "fin": window[1]},
         "municipios": [{"nombre": "Casar de Cáceres", "lat": 39.56, "lon": -6.42}]},
        {"id": 3, "nombre": "EOSA", "nivel": "bajo", "probabilidad": 0.02, "perfil": profile, "franja": {},
         "municipios": [{"nombre": "Hervás", "lat": 40.27, "lon": -5.86}]},
    ]}


def test_the_forecast_of_the_evening_before_is_verified_against_the_stations(fs, tmp_path, monkeypatch):
    cfg = obs.observation_settings(obs.load_config())
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    for day, hour, peak, window in (("2026-07-19", 11, 60.0, (10, 15)), ("2026-07-20", 14, 80.0, (13, 16))):
        with open(tmp_path / f"riesgo_{day.replace('-', '')}.json", "w", encoding="utf-8") as file:
            json.dump(forecast(day, hour, peak, window), file)
    with open(tmp_path / "riesgo.json", "w", encoding="utf-8") as file:
        json.dump(forecast("2026-07-20", 14, 80.0, (13, 16)), file)
    # The run of the day before landed the same stations one day earlier: with it, yesterday has its 24 hours.
    earlier = []
    for r in records():
        if "fint" in r and r.get("lat") is not None:
            ts = datetime.fromisoformat(r["fint"]) - timedelta(days=1)
            earlier.append({**r, "fint": ts.strftime("%Y-%m-%dT%H:%M:%S")})
    obs.land(fs, cfg, obs.inside(earlier, cfg["caja"]), NOW - timedelta(days=1))
    monkeypatch.setenv("AEMET_API_KEY", "k")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW, records=records()) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        out = json.load(file)
    assert out["ayer"]["fecha"] == "2026-07-19" and out["hoy"]["fecha"] == "2026-07-20" and list(out["dias"]) == ["2026-07-19"]
    pitarch, eosa = out["ayer"]["distribuidoras"]
    # Yesterday: 24 hours observed near Pitarch, peak 72 km/h at 10:00 local against 60 forecast at 11:00, inside the window 10 - 15.
    assert pitarch["horas_observadas"] == 24 and pitarch["racha_observada_max"] == 72 and pitarch["hora_observada"] == 10
    assert pitarch["racha_prevista_max"] == 60 and pitarch["hora_prevista"] == 11 and pitarch["error_kmh"] == 12 and pitarch["pico_en_franja"] is True
    assert eosa["estaciones"] == 0 and "racha_observada_max" not in eosa and "error_kmh" not in eosa and eosa["racha_prevista_max"] == 60
    today = out["hoy"]["distribuidoras"][0]
    assert today["horas_observadas"] == 16 and today["error_kmh"] == 72 - 80 and today["pico_en_franja"] is False
    # The summary only counts the complete days and only the networks with stations.
    assert [s["id"] for s in out["resumen"]] == [2]
    s = out["resumen"][0]
    assert s["dias"] == 1 and s["sesgo_kmh"] == 12.0 and s["error_abs_kmh"] == 12.0 and s["pico_en_franja"] == 1.0 and s["dias_prevision_mayor"] == 0.0
    assert s["texto"] == "en el último día completo, la previsión de la víspera dio de media 12 km/h menos que las estaciones (error medio 12 km/h); el pico observado cayó en la franja prevista el 100 % de los días"
    # A second run of the same day keeps yesterday and does not duplicate it.
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW + timedelta(hours=1), records=records()) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        again = json.load(file)
    assert list(again["dias"]) == ["2026-07-19"] and again["resumen"][0]["dias"] == 1
