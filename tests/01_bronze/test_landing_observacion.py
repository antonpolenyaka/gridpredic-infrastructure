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
    """Three stations, one of them outside the box, over 24 hours (AEMET gives 12: see answer_at for the real shape)."""
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
        {"id": 2, "nombre": "Distribuidora 2", "municipios": [{"nombre": "Casar de Cáceres", "lat": 39.56, "lon": -6.42}]},
        {"id": 3, "nombre": "Distribuidora 1", "municipios": [{"nombre": "Hervás", "lat": 40.27, "lon": -5.86}]},
        {"id": 1366, "nombre": "Distribuidora 3", "municipios": [{"nombre": "Valle de Santa Ana", "lat": 38.37, "lon": -6.78}]},
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
    d2, d1, d3 = out["distribuidoras"]
    # Casar de Cáceres is 12 km from the station of Cáceres; Hervás is 33 km from Plasencia: none within 25 km.
    assert d2["estaciones"] == 1 and d2["racha_max_kmh"] == 72 and d2["hora_racha_max"] == 10
    assert len(d2["horas"]) == 16 and d2["horas"][0]["hora"] == 0
    assert next(h for h in d2["horas"] if h["hora"] == 11)["lluvia_mm"] == 2.4
    assert d2["texto"].startswith("racha máxima observada hoy 72 km/h a las 10:00")
    assert d1["estaciones"] == 0 and d1["texto"] == "sin estaciones cerca de la red" and d1["horas"] == []
    assert d3["estaciones"] == 0
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
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "alto", "probabilidad": 0.12, "perfil": profile, "franja": {"inicio": window[0], "fin": window[1]},
         "aviso_max_nivel": 2.0, "municipios": [{"nombre": "Casar de Cáceres", "lat": 39.56, "lon": -6.42}]},
        {"id": 3, "nombre": "Distribuidora 1", "nivel": "bajo", "probabilidad": 0.02, "perfil": profile, "franja": {},
         "municipios": [{"nombre": "Hervás", "lat": 40.27, "lon": -5.86}]},
    ]}


def test_the_forecast_of_the_evening_before_is_verified_against_the_stations(fs, tmp_path, monkeypatch):
    cfg = obs.observation_settings(obs.load_config())
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    for day, hour, peak, window in (("2026-07-19", 11, 60.0, (10, 15)), ("2026-07-20", 14, 80.0, (13, 16))):
        risk = forecast(day, hour, peak, window)
        if day == "2026-07-19":
            # The evening of the 18th also gave an outlook of the 20th: alto for the Distribuidora 2.
            risk["pasado_manana"] = {"fecha": "2026-07-20", "fecha_texto": "20.07.2026",
                                     "distribuidoras": [{"id": 2, "nombre": "Distribuidora 2", "nivel": "alto", "probabilidad": 0.12}]}
        with open(tmp_path / f"riesgo_{day.replace('-', '')}.json", "w", encoding="utf-8") as file:
            json.dump(risk, file)
    with open(tmp_path / "riesgo.json", "w", encoding="utf-8") as file:
        json.dump(forecast("2026-07-20", 14, 80.0, (13, 16)), file)
    # The band of the ensemble of yesterday: the observed 72 falls inside 55 - 75 for the Distribuidora 2.
    with open(tmp_path / "ensemble_20260719.json", "w", encoding="utf-8") as file:
        json.dump({"fecha": "2026-07-19", "distribuidoras": [{"id": 2, "nombre": "Distribuidora 2", "pico": {"p10": 55, "p50": 63, "p90": 75}, "supera": {"50": 0.9, "70": 0.3}},
                                                            {"id": 3, "nombre": "Distribuidora 1", "pico": {"p10": 50, "p50": 58, "p90": 70}, "supera": {"50": 0.8}}]}, file)
    # The forecast of AEMET by municipality of yesterday: a peak of 60 at 13:00 against the observed 72.
    with open(tmp_path / "aemet_20260719.json", "w", encoding="utf-8") as file:
        json.dump({"fecha": "2026-07-19", "distribuidoras": [{"id": 2, "nombre": "Distribuidora 2", "pico": {"racha_kmh": 60, "hora": 13, "municipio": "x"}}]}, file)
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
    d2, d1 = out["ayer"]["distribuidoras"]
    # Yesterday: 24 hours observed near the Distribuidora 2, peak 72 km/h at 10:00 local against 60 forecast at 11:00, inside the window 10 - 15.
    assert d2["horas_observadas"] == 24 and d2["racha_observada_max"] == 72 and d2["hora_observada"] == 10
    assert d2["racha_prevista_max"] == 60 and d2["hora_prevista"] == 11 and d2["error_kmh"] == 12 and d2["pico_en_franja"] is True
    assert d1["estaciones"] == 0 and "racha_observada_max" not in d1 and "error_kmh" not in d1 and d1["racha_prevista_max"] == 60
    assert d2["banda_p10"] == 55 and d2["banda_p90"] == 75 and d2["pico_en_banda"] is True
    assert d2["racha_aemet_max"] == 60 and d2["hora_aemet"] == 13 and d2["error_aemet_kmh"] == 12 and "racha_aemet_max" not in d1
    assert d2["supera"] == {"50": 0.9, "70": 0.3} and d2["supero"] == {"50": True, "70": True} and "supero" not in d1
    assert d2["aviso_aemet"] == 2 and "aviso_aemet" not in d1
    assert d1["banda_p50"] == 58 and "pico_en_banda" not in d1
    today = out["hoy"]["distribuidoras"][0]
    assert today["horas_observadas"] == 16 and today["error_kmh"] == 72 - 80 and today["pico_en_franja"] is False
    # What the outlook of two evenings before said of today; nothing for yesterday (no outlook pointed at it).
    assert today["nivel_dos_dias"] == "alto" and today["probabilidad_dos_dias"] == 0.12
    assert "nivel_dos_dias" not in d2 and "nivel_dos_dias" not in out["hoy"]["distribuidoras"][1]
    # The summary only counts the complete days and only the networks with stations.
    assert [s["id"] for s in out["resumen"]] == [2]
    s = out["resumen"][0]
    assert s["dias"] == 1 and s["sesgo_kmh"] == 12.0 and s["error_abs_kmh"] == 12.0 and s["pico_en_franja"] == 1.0 and s["dias_prevision_mayor"] == 0.0
    assert s["pico_en_banda"] == 1.0 and s["dias_con_banda"] == 1
    assert s["sesgo_aemet_kmh"] == 12.0 and s["dias_con_aemet"] == 1
    assert s["umbrales"] == {"50": {"dias": 1, "probabilidad_media": 0.9, "parte_superada": 1.0, "brier": 0.01}, "70": {"dias": 1, "probabilidad_media": 0.3, "parte_superada": 1.0, "brier": 0.49}}
    assert s["texto"] == ("en el último día completo, la previsión de la víspera dio de media 12 km/h menos que las estaciones (error medio 12 km/h); "
                          "el pico observado cayó en la franja prevista el 100 % de los días; dentro de la banda de los escenarios el 100 % de los 1 días con banda; "
                          "AEMET por municipios dio de media 12 km/h menos que las estaciones (error medio 12 km/h en 1 días); "
                          "los escenarios daban un 90 % de superar 50 km/h y pasó el 100 % de los días, los escenarios daban un 30 % de superar 70 km/h y pasó el 100 % de los días")
    # A second run of the same day keeps yesterday and does not duplicate it.
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW + timedelta(hours=1), records=records()) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        again = json.load(file)
    assert list(again["dias"]) == ["2026-07-19"] and again["resumen"][0]["dias"] == 1


def answer_at(asked: datetime, gust_of) -> list:
    """What AEMET really gives when asked at `asked`: the last 12 hours of the station near the Distribuidora 2."""
    out = []
    last = asked.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    for back in range(12):
        ts = last - timedelta(hours=back)
        out.append({"idema": "3469A", "ubi": "CÁCERES", "lat": 39.47, "lon": -6.34, "fint": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                    "vmax": gust_of(ts) / 3.6, "vv": 5.0, "prec": 0.0, "ta": 25.0, "hr": 50})
    return out


def morning_storm(ts: datetime) -> float:
    """A gust of 72 km/h at 07:00 UTC (09:00 in Madrid) of every day, 29 km/h the rest."""
    return 72.0 if ts.hour == 7 else 29.0


def landed_runs(fs, cfg: dict, start: datetime, end: datetime, every_h: int = 1):
    """The hourly runs of the DAG between start and end, every one landing the 12 hours AEMET gave it."""
    asked = start
    while asked <= end:
        obs.land(fs, cfg, obs.inside(answer_at(asked, morning_storm), cfg["caja"]), asked)
        asked += timedelta(hours=every_h)


def settings_for(tmp_path) -> dict:
    cfg = obs.observation_settings(obs.load_config())
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    cfg["dashboard_dir"] = str(tmp_path)
    return cfg


def test_answers_of_12_hours_still_give_yesterday_its_24_hours_and_the_morning_peak(fs, tmp_path, monkeypatch):
    cfg = settings_for(tmp_path)
    for day, hour, peak, window in (("2026-07-19", 9, 70.0, (8, 11)), ("2026-07-20", 9, 70.0, (8, 11))):
        with open(tmp_path / f"riesgo_{day.replace('-', '')}.json", "w", encoding="utf-8") as file:
            json.dump(forecast(day, hour, peak, window), file)
    with open(tmp_path / "riesgo.json", "w", encoding="utf-8") as file:
        json.dump(forecast("2026-07-20", 9, 70.0, (8, 11)), file)
    # Hourly runs from the evening of 18.07 up to the run in course (20.07 13:25 UTC).
    landed_runs(fs, cfg, datetime(2026, 7, 18, 20, 25, tzinfo=timezone.utc), NOW - timedelta(hours=1))
    monkeypatch.setenv("AEMET_API_KEY", "k")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW,
                    records=answer_at(NOW, morning_storm)) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        out = json.load(file)
    d2 = out["ayer"]["distribuidoras"][0]
    # The last run of 19.07 UTC alone holds 12 hours, from 12:00 to 23:00 UTC, and misses the peak of the morning.
    assert d2["horas_observadas"] == 24 and d2["racha_observada_max"] == 72 and d2["hora_observada"] == 9
    assert d2["pico_en_franja"] is True and d2["error_kmh"] == 2
    assert [s["id"] for s in out["resumen"]] == [2] and out["resumen"][0]["dias"] == 1
    # Today has its hours from midnight in observado.json, not only the 12 of the run in course.
    with open(tmp_path / "observado.json", encoding="utf-8") as file:
        seen = json.load(file)
    hours = [h["hora"] for h in seen["distribuidoras"][0]["horas"]]
    assert hours[0] == 0 and len(hours) == 16 and seen["distribuidoras"][0]["racha_max_kmh"] == 72
    # Nothing half written is left behind in the dashboard folder.
    assert not [name for name in os.listdir(tmp_path) if name.endswith(".tmp")]


def test_an_unreadable_verification_is_rebuilt_from_the_landing_and_the_old_days_can_be_redone(fs, tmp_path, monkeypatch):
    cfg = settings_for(tmp_path)
    for day in ("2026-07-17", "2026-07-18", "2026-07-19", "2026-07-20"):
        with open(tmp_path / f"riesgo_{day.replace('-', '')}.json", "w", encoding="utf-8") as file:
            json.dump(forecast(day, 9, 70.0, (8, 11)), file)
    landed_runs(fs, cfg, datetime(2026, 7, 16, 20, 25, tzinfo=timezone.utc), NOW - timedelta(hours=1))
    # A file cut in half (a copy restored half way, say): it is not written over with yesterday alone.
    (tmp_path / "verificacion.json").write_text('{"dias": {"2026-07-17": [', encoding="utf-8")
    monkeypatch.setenv("AEMET_API_KEY", "k")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW,
                    records=answer_at(NOW, morning_storm)) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        out = json.load(file)
    assert list(out["dias"]) == ["2026-07-17", "2026-07-18", "2026-07-19"]
    assert all(r[0]["horas_observadas"] == 24 for r in out["dias"].values())
    assert out["resumen"][0]["dias"] == 3
    # The days kept before the change were verified with the last run of the UTC day only (12 hours here):
    # --rehacer-verificacion recomputes them from every run, and a day without its archived forecast keeps its record.
    out["dias"]["2026-07-17"][0]["horas_observadas"] = 12
    out["dias"]["2026-07-10"] = [{"id": 2, "nombre": "Distribuidora 2", "horas_observadas": 11}]
    (tmp_path / "verificacion.json").write_text(json.dumps(out), encoding="utf-8")
    assert obs.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"], "--rehacer-verificacion"], fs=fs,
                    now=NOW, records=answer_at(NOW, morning_storm)) == 0
    with open(tmp_path / "verificacion.json", encoding="utf-8") as file:
        again = json.load(file)
    assert again["dias"]["2026-07-17"][0]["horas_observadas"] == 24
    assert again["dias"]["2026-07-10"][0]["horas_observadas"] == 11


def test_the_dashboard_json_is_replaced_atomically(tmp_path, monkeypatch):
    from job_landing_vivo import write_json_atomic

    target = tmp_path / "rayos.json"
    target.write_text('{"viejo": 1}', encoding="utf-8")
    seen = []
    real_replace = os.replace

    def replace(source, destination):
        # When the target is replaced, the new content is already complete in the temporary file.
        with open(source, encoding="utf-8") as file:
            seen.append(json.load(file))
        assert json.loads(target.read_text(encoding="utf-8")) == {"viejo": 1}
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", replace)
    write_json_atomic(str(target), {"nuevo": 2}, ensure_ascii=False)
    assert seen == [{"nuevo": 2}] and json.loads(target.read_text(encoding="utf-8")) == {"nuevo": 2}
    # A replace refused by the system (a file held open on Windows) still writes the file, in place.
    monkeypatch.setattr(os, "replace", lambda a, b: (_ for _ in ()).throw(PermissionError("in use")))
    write_json_atomic(str(target), {"nuevo": 3})
    assert json.loads(target.read_text(encoding="utf-8")) == {"nuevo": 3}
    assert sorted(os.listdir(tmp_path)) == ["rayos.json"]
