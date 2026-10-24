"""
Tests of the warnings of AEMET during the day (avisos_hoy): the warnings in
force that cover the municipalities of every distribuidora today, the ones
that did not exist when the evening run was made, the guard recomputed with
them and the message to the webhook, once per distribuidora and day. Without
network: the answer of AEMET is a tar built here.

    python -m pytest tests/04_ml/test_avisos_hoy.py -q
"""

import io
import json
import os
import sys
import tarfile
from datetime import datetime, timezone


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import avisos_hoy as ah  # noqa: E402

CAP = """<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>{identifier}</identifier>
  <sender>http://www.aemet.es</sender>
  <sent>{sent}</sent>
  <status>Actual</status>
  <msgType>{kind}</msgType>
  <scope>Public</scope>
  <info>
    <language>es-ES</language>
    <category>Met</category>
    <event>{event}</event>
    <eventCode><valueName>AEMET-Meteoalerta fenomeno</valueName><value>{code}</value></eventCode>
    <onset>{onset}</onset>
    <expires>{expires}</expires>
    <parameter><valueName>AEMET-Meteoalerta nivel</valueName><value>{level}</value></parameter>
    <parameter><valueName>AEMET-Meteoalerta parametro</valueName><value>{parameter}</value></parameter>
    <area>
      <areaDesc>Norte de Cáceres</areaDesc>
      <polygon>{polygon}</polygon>
      <geocode><valueName>AEMET-Meteoalerta zona</valueName><value>701001</value></geocode>
    </area>
  </info>
</alert>
"""

NORTH = "40.5,-7.0 40.5,-5.5 39.9,-5.5 39.9,-7.0 40.5,-7.0"  # holds Plasencia (40.03, -6.09), not Casar de Cáceres (39.56, -6.42)


def cap(identifier, sent, onset, expires, level="amarillo", code="TO;Tormentas", event="Tormentas", parameter="TO; Tormentas; ", kind="Alert", polygon=NORTH):
    return CAP.format(identifier=identifier, sent=sent, onset=onset, expires=expires, level=level, code=code, event=event,
                      parameter=parameter, kind=kind, polygon=polygon).encode("utf-8")


def tar_of(files: dict) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


RISK = {"generado": "14.10.2026 18:33 UTC", "fecha": "2026-10-15", "distribuidoras": [
    {"id": 3, "nombre": "Distribuidora 1", "nivel": "medio", "guardia": {"reforzar": False, "motivo": "normal"},
     "municipios": [{"nombre": "Plasencia", "lat": 40.0312, "lon": -6.0884}, {"nombre": "Hervás", "lat": 40.2708, "lon": -5.8672}]},
    {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "guardia": {"reforzar": False, "motivo": "normal"},
     "municipios": [{"nombre": "Casar de Cáceres", "lat": 39.5611, "lon": -6.4194}]}]}


def test_the_warnings_of_the_day_in_the_municipalities_and_the_guard_recomputed(tmp_path, monkeypatch):
    now = datetime(2026, 10, 15, 9, 20, tzinfo=timezone.utc)
    # A thunderstorm warning sent this morning for this afternoon over the north (Distribuidora 1), one of yesterday evening that the
    # run already saw, a cancelled one and one for tomorrow.
    files = {
        "a.xml": cap("new-storm", "2026-10-15T08:10:00+02:00", "2026-10-15T14:00:00+02:00", "2026-10-15T23:59:00+02:00"),
        "b.xml": cap("old-wind", "2026-10-14T19:00:00+02:00", "2026-10-15T06:00:00+02:00", "2026-10-15T18:00:00+02:00", code="VI;Vientos", event="Vientos",
                     parameter="VI; Rachas máximas; 70 km/h"),
        "c.xml": cap("cancelled", "2026-10-15T08:30:00+02:00", "2026-10-15T14:00:00+02:00", "2026-10-15T23:59:00+02:00", kind="Cancel", level="naranja"),
        "d.xml": cap("tomorrow", "2026-10-15T08:30:00+02:00", "2026-10-16T02:00:00+02:00", "2026-10-16T20:00:00+02:00", level="naranja"),
    }
    rows = ah.parse_aemet_archive(tar_of(files))
    assert len(rows) == 4
    payload = ah.today_payload(RISK, rows, now, "Europe/Madrid")
    d1, d2 = payload["distribuidoras"]
    assert d1["aviso_tormenta_nivel"] == 1 and d1["aviso_viento_nivel"] == 1 and d1["aviso_max_nivel"] == 1
    assert [w["fenomeno"] for w in d1["avisos"]] == ["TO", "VI"] and d1["avisos"][0]["municipios"] == ["Plasencia", "Hervás"]
    assert [w["fenomeno"] for w in d1["nuevos"]] == ["TO"] and d1["nuevos"][0]["inicio"] == "14:00"
    # The guard goes up only because of the new one? No: the wind warning of yesterday already reinforced it in the evening
    # when the run saw it; here the evening said normal, so with the warnings of today it is reinforced and that is new.
    assert d1["guardia"]["reforzar"] and d1["subida"]
    assert d1["texto"].startswith("Distribuidora 1: guardia reforzada desde las 11:20 por aviso nuevo de AEMET para hoy (tormenta amarillo de 14:00 a 15.10 23:59)")
    assert not d2["avisos"] and not d2["guardia"]["reforzar"] and d2["texto"] == "Distribuidora 2: sin avisos nuevos de AEMET desde anoche"
    # The webhook: once per distribuidora and day.
    sent = []
    monkeypatch.setattr(ah.aviso_riesgo, "post", lambda url, message, field, timeout: sent.append(message) or 200)
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    state = tmp_path / "avisos_hoy_aviso.json"
    assert ah.send_raised(payload, {}, str(state)) == "enviado" and len(sent) == 1
    assert sent[0].startswith("Avisos nuevos de AEMET para hoy 15.10.2026:\n- Distribuidora 1: guardia reforzada desde las 11:20") and "Distribuidora 2" not in sent[0]
    assert ah.send_raised(payload, {}, str(state)) == "ya avisado" and len(sent) == 1
    quiet = ah.today_payload(RISK, [rows[1]], now, "Europe/Madrid")
    assert quiet["distribuidoras"][0]["subida"] and not quiet["distribuidoras"][0]["nuevos"]
    assert ah.send_raised(ah.today_payload(RISK, [], now, "Europe/Madrid"), {}, str(state)) == "nada nuevo"


def test_refresh_writes_the_file_next_to_riesgo_json(tmp_path, monkeypatch):
    dashboard = tmp_path / "dashboard"
    dashboard.mkdir()
    (dashboard / "riesgo.json").write_text(json.dumps(RISK), encoding="utf-8")
    landing = tmp_path / "vivo"
    raw = tar_of({"a.xml": cap("new-storm", "2026-10-15T08:10:00+02:00", "2026-10-15T14:00:00+02:00", "2026-10-15T23:59:00+02:00")})
    monkeypatch.setattr(ah, "ask_warnings", lambda url, key, timeout, area: raw)
    monkeypatch.setenv("AEMET_API_KEY", "x")
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    rcfg = {"zona_horaria": "Europe/Madrid", "avisos": {"api": "https://x/{area}", "areas": ["esp"], "clave_env": "AEMET_API_KEY"}, "aviso": {}}
    now = datetime(2026, 10, 15, 9, 20, tzinfo=timezone.utc)
    payload = ah.refresh(rcfg, str(dashboard), str(landing), now)
    assert payload["envio"] == "sin url" and payload["distribuidoras"][0]["subida"]
    written = json.load(open(dashboard / "avisos_hoy.json", encoding="utf-8"))
    assert written["fecha"] == "2026-10-15" and written["distribuidoras"][0]["nuevos"][0]["fenomeno"] == "TO"
    assert os.path.exists(landing / "20261015" / "avisos_hoy" / "esp_1120.tar")
    # riesgo.json of another day: nothing to refresh.
    (dashboard / "riesgo.json").write_text(json.dumps({**RISK, "fecha": "2026-10-14"}), encoding="utf-8")
    assert ah.refresh(rcfg, str(dashboard), str(landing), now) == {}
