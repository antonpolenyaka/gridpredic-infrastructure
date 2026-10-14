"""
Test of the message of the evening (aviso_riesgo): the text from riesgo.json,
the level that triggers it and the webhook, without network.

    python -m pytest tests/04_ml/test_aviso_riesgo.py -q
"""

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import aviso_riesgo as av  # noqa: E402

PAYLOAD = {
    "fecha": "2026-01-15", "fecha_texto": "15.01.2026",
    "reglas": {"texto": "Tras una avería, otra en las 3 h siguientes en el 11 % de los casos."},
    "distribuidoras": [
        {"id": 3, "nombre": "EOSA", "nivel": "muy alto", "probabilidad": 0.31, "veces_tasa_base": 4.1, "esperadas": 0.42,
         "motivos": "racha prevista de 1,6 veces el p98 (x12)", "impacto": {"clientes_esperados": 85.4},
         "franja": {"inicio": 10, "fin": 15, "racha_pico": 92.0},
         "zonas": [{"nombre": "Hervás", "probabilidad": 0.05}, {"nombre": "Montánchez / Torre", "probabilidad": 0.12},
                   {"nombre": "Plasencia", "probabilidad": 0.08}, {"nombre": "Jerte", "probabilidad": 0.02}],
         "brigadas": {"bases": [{"municipio": "Plasencia"}, {"municipio": "Montánchez"}], "posicionamiento": "brigadas en sus bases a las 9:00"}},
        {"id": 2, "nombre": "Pitarch", "nivel": "medio", "probabilidad": 0.06, "veces_tasa_base": 1.5, "esperadas": 0.06,
         "motivos": "ninguno, por debajo de un día medio", "impacto": {}, "franja": {}, "zonas": [], "brigadas": {}},
    ],
}


def test_the_message_names_the_distribuidoras_in_alert_only():
    text = av.message_from(PAYLOAD, "alto")
    assert text.startswith("Riesgo de interrupción de mañana 15.01.2026:")
    assert "- EOSA: MUY ALTO (31 %, 4.1 veces su tasa base); averías esperadas 0.4, clientes esperados sin luz 85" in text
    assert "franja de 10:00 a 16:00 (pico 92 km/h)" in text
    assert "líneas: Montánchez / Torre (12.0 %), Plasencia (8.0 %), Hervás (5.0 %)" in text
    assert "brigadas en Plasencia, Montánchez (brigadas en sus bases a las 9:00)" in text
    assert "motivos: racha prevista" in text and "Pitarch" not in text
    assert "11 % de los casos" in text and text.endswith("http://localhost:8086")
    # With medio as the minimum, Pitarch enters without motives; with muy alto only EOSA; nothing above.
    assert "- Pitarch: MEDIO (6 %" in av.message_from(PAYLOAD, "medio")
    assert "motivos" not in av.message_from(PAYLOAD, "medio").split("- Pitarch")[1]
    assert av.message_from(PAYLOAD, "muy alto").count("\n- ") == 1
    assert av.message_from({"distribuidoras": [{"nombre": "x", "nivel": "bajo"}]}, "alto") == ""
    # With the ensemble of the same day, the spread of the runs goes after the motives; of another day, nothing.
    band = {"fecha": "2026-01-15", "distribuidoras": [{"id": 3, "horas": [{"hora": 0, "p10": 1, "p50": 2, "p90": 3}],
                                                       "texto": "40 escenarios de icon_eu en 21 celdas de la red: el 80 % central da el pico de racha entre 70 y 95 km/h (mediana 84)"}]}
    with_band = av.message_from(PAYLOAD, "alto", band)
    assert "motivos: racha prevista de 1,6 veces el p98 (x12); escenarios: 40 escenarios de icon_eu en 21 celdas de la red: el 80 % central da el pico de racha entre 70 y 95 km/h (mediana 84)." in with_band
    assert "escenarios" not in av.message_from(PAYLOAD, "alto", {**band, "fecha": "2026-01-14"})
    assert not av.reached("raro", "alto") and av.reached("muy alto", "alto") and not av.reached("medio", "alto")


def test_send_posts_json_only_with_the_url(monkeypatch, capsys, tmp_path):
    sent = []

    class Answer:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        sent.append((request.full_url, json.loads(request.data.decode("utf-8")), request.get_header("Content-type"), timeout))
        return Answer()

    monkeypatch.setattr(av.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    assert av.send(PAYLOAD, {}) == "sin url" and not sent
    assert av.send({"distribuidoras": []}, {}) == "sin alerta"
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    assert av.send(PAYLOAD, {"aviso": {"campo": "content", "timeout_s": 7}}) == "enviado"
    assert sent[0][0] == "https://hooks.example/abc" and list(sent[0][1]) == ["content"] and sent[0][3] == 7.0
    assert sent[0][2] == "application/json" and "EOSA" in sent[0][1]["content"]

    def failing(request, timeout):
        raise OSError("down")

    monkeypatch.setattr(av.urllib.request, "urlopen", failing)
    assert av.send(PAYLOAD, {}) == "error: OSError"
    # The command line reads the JSON and never prints the URL.
    path = tmp_path / "riesgo.json"
    path.write_text(json.dumps(PAYLOAD), encoding="utf-8")
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    assert av.main([str(path)]) == 0
    out = capsys.readouterr().out
    assert "sin url" in out and "hooks.example" not in out
