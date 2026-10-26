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
        {"id": 3, "nombre": "Distribuidora 1", "nivel": "muy alto", "probabilidad": 0.31, "veces_tasa_base": 4.1, "esperadas": 0.42,
         "motivos": "racha prevista de 1,6 veces el p98 (x12)", "impacto": {"clientes_esperados": 85.4},
         "franja": {"inicio": 10, "fin": 15, "racha_pico": 92.0},
         "zonas": [{"nombre": "Hervás", "probabilidad": 0.05}, {"nombre": "Montánchez / Torre", "probabilidad": 0.12},
                   {"nombre": "Plasencia", "probabilidad": 0.08}, {"nombre": "Jerte", "probabilidad": 0.02}],
         "brigadas": {"bases": [{"municipio": "Plasencia"}, {"municipio": "Montánchez"}], "posicionamiento": "brigadas en sus bases a las 9:00"}},
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "medio", "probabilidad": 0.06, "veces_tasa_base": 1.5, "esperadas": 0.06,
         "motivos": "ninguno, por debajo de un día medio", "impacto": {}, "franja": {}, "zonas": [], "brigadas": {}},
    ],
}


def test_the_message_names_the_distribuidoras_in_alert_only():
    text = av.message_from(PAYLOAD, "alto")
    assert text.startswith("Riesgo de interrupción de mañana 15.01.2026:")
    assert "- Distribuidora 1: MUY ALTO (31 %, 4,1 veces su tasa base)\n  Averías esperadas 0,4; clientes esperados sin luz 85." in text
    assert "  Franja de 10:00 a 16:00, pico de racha 92 km/h." in text
    assert "  Líneas: Montánchez / Torre (12,0 %), Plasencia (8,0 %), Hervás (5,0 %)." in text
    assert "  Brigadas: Plasencia, Montánchez (brigadas en sus bases a las 9:00)." in text
    assert "  Motivos: racha prevista" in text and "Distribuidora 2" not in text
    assert "11 % de los casos" in text and text.endswith("http://localhost:8086")
    # With medio as the minimum, Distribuidora 2 enters without motives; with muy alto only Distribuidora 1; nothing above.
    assert "- Distribuidora 2: MEDIO (6 %" in av.message_from(PAYLOAD, "medio")
    assert "Motivos" not in av.message_from(PAYLOAD, "medio").split("- Distribuidora 2")[1]
    assert av.message_from(PAYLOAD, "muy alto").count("\n- ") == 1
    assert av.message_from({"distribuidoras": [{"nombre": "x", "nivel": "bajo"}]}, "alto") == ""
    # With the ensemble of the same day, the spread of the runs goes after the motives; of another day, nothing.
    band = {"fecha": "2026-01-15", "distribuidoras": [{"id": 3, "horas": [{"hora": 0, "p10": 1, "p50": 2, "p90": 3}],
                                                       "texto": "40 escenarios de icon_eu en 21 celdas de la red: el 80 % central da el pico de racha entre 70 y 95 km/h (mediana 84)"}]}
    with_band = av.message_from(PAYLOAD, "alto", band)
    assert "  Motivos: racha prevista de 1,6 veces el p98 (x12).\n  Escenarios: 40 escenarios de icon_eu en 21 celdas de la red: el 80 % central da el pico de racha entre 70 y 95 km/h (mediana 84)." in with_band
    assert "Escenarios" not in av.message_from(PAYLOAD, "alto", {**band, "fecha": "2026-01-14"})
    # riesgo.json may carry them itself (attached by the scoring job), with the forecast of AEMET by municipality.
    carried = json.loads(json.dumps(PAYLOAD))
    carried["distribuidoras"][0]["escenarios"] = {"texto": "40 escenarios de icon_eu en 21 celdas de la red: mediana 84"}
    carried["distribuidoras"][0]["aemet"] = {"texto": "AEMET por municipios (40 municipios, elaborada 18:55): pico de racha 70 km/h a las 12:00 en Hervás"}
    text = av.message_from(carried, "alto")
    assert "  Escenarios: 40 escenarios de icon_eu en 21 celdas de la red: mediana 84.\n  AEMET por municipios (40 municipios, elaborada 18:55): pico de racha 70 km/h a las 12:00 en Hervás." in text
    assert not av.reached("raro", "alto") and av.reached("muy alto", "alto") and not av.reached("medio", "alto")
    # A distribuidora below the minimum enters when its guard is reinforced by a wind or thunderstorm warning.
    guarded = json.loads(json.dumps(PAYLOAD))
    guarded["distribuidoras"][1]["guardia"] = {"reforzar": True, "motivo": "aviso de tormenta de AEMET (amarillo)"}
    text = av.message_from(guarded, "alto")
    assert "- Distribuidora 2: MEDIO (6 %" in text and "guardia reforzada (aviso de tormenta de AEMET (amarillo))" in text


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
    assert sent[0][2] == "application/json" and "Distribuidora 1" in sent[0][1]["content"]

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


def test_a_failed_task_goes_to_the_webhook(monkeypatch):
    class TI:
        dag_id, task_id, try_number = "dag_riesgo_manana", "prevision", 2

    context = {"task_instance": TI(), "logical_date": "2026-10-14T18:30:00+00:00", "exception": RuntimeError("The forecast of ICON-EU for 2026-10-15 did not land")}
    text = av.failure_message(context)
    assert text == ("GridPredic: la tarea prevision de dag_riesgo_manana ha fallado (intento 2), ejecución del 14.10.2026 20:30: "
                    "The forecast of ICON-EU for 2026-10-15 did not land. Airflow: http://localhost:8084")
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    assert av.notify_failure(context) == "sin url"
    sent = []
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    monkeypatch.setattr(av, "post", lambda url, message, field, timeout: sent.append((url, message, field)) or 200)
    assert av.notify_failure(context) == "enviado" and sent[0][0] == "https://hooks.example/abc" and sent[0][2] == "text"
    assert "ha fallado" in sent[0][1]


def test_a_storm_over_the_network_is_told_once_an_hour(monkeypatch, tmp_path):
    from datetime import datetime, timezone

    storm = {"generado": "2026-10-14T15:45Z", "fuente": "EUMETSAT MTG LI", "ventana_min": 60, "n_rayos": 12, "distribuidoras": [
        {"id": 3, "nombre": "Distribuidora 1", "nivel": "tormenta sobre la red", "texto": "9 rayos a menos de 10 km de la red en la última hora (líneas: Hervás, Jerte)",
         "lineas": ["Hervás", "Jerte"], "municipios": ["Hervás", "Baños de Montemayor", "Jerte", "Tornavacas", "Cabezuela", "Navaconcejo", "Rebollar"]},
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "sin tormenta", "texto": "sin rayos", "lineas": [], "municipios": []}]}
    text = av.storm_message(storm)
    assert text.startswith("Rayos sobre la red a las 15:45 UTC (satélite MTG):\n- Distribuidora 1: 9 rayos a menos de 10 km de la red en la última hora (líneas: Hervás, Jerte); municipios bajo la tormenta: Hervás, Baños de Montemayor, Jerte, Tornavacas, Cabezuela, Navaconcejo, ....")
    assert "Distribuidora 2" not in text and text.endswith("http://localhost:8086")
    assert av.storm_message({"distribuidoras": [{"nivel": "tormenta cerca"}]}) == ""
    state = tmp_path / "rayos_aviso.json"
    sent = []
    monkeypatch.setattr(av, "post", lambda url, message, field, timeout: sent.append(message) or 200)
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    now = datetime(2026, 10, 14, 15, 45, tzinfo=timezone.utc)
    assert av.send_storm(storm, {}, str(state), now) == "sin url" and not state.exists()
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    assert av.send_storm(storm, {}, str(state), now) == "enviado" and len(sent) == 1 and json.load(open(state))["3"] == "2026-10-14T15:45:00+00:00"
    # Fifteen minutes later the same storm is not told again; after the hour it is; another distribuidora is told at once.
    assert av.send_storm(storm, {}, str(state), now.replace(minute=59)) == "ya avisado" and len(sent) == 1
    assert av.send_storm(storm, {}, str(state), now.replace(hour=16, minute=50)) == "enviado" and len(sent) == 2
    both = json.loads(json.dumps(storm))
    both["distribuidoras"][1]["nivel"] = "tormenta sobre la red"
    assert av.send_storm(both, {"aviso": {"rayos_cada_min": 30}}, str(state), now.replace(hour=16, minute=55)) == "enviado"
    assert "Distribuidora 2" in sent[2] and "Distribuidora 1" not in sent[2]
    assert av.send_storm({"distribuidoras": []}, {}, str(state), now) == "sin tormenta"


def test_the_morning_report_tells_yesterday_and_today_once_a_day(monkeypatch, tmp_path):
    from datetime import datetime, timezone

    verification = {"generado": "2026-10-15T05:20Z", "hoy": {"fecha": "2026-10-15", "distribuidoras": [
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "probabilidad": 0.031, "racha_prevista_max": 50, "hora_prevista": 12, "racha_observada_max": None}]},
        "ayer": {"fecha": "2026-10-14", "distribuidoras": [
            {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "probabilidad": 0.0307, "estaciones": 10, "horas_observadas": 24, "aviso_aemet": 0,
             "racha_prevista_max": 60, "hora_prevista": 11, "racha_observada_max": 44, "hora_observada": 11, "error_kmh": -16, "pico_en_franja": True,
             "racha_aemet_max": 43, "hora_aemet": 11, "banda_p10": 40, "banda_p50": 49, "banda_p90": 60, "pico_en_banda": True},
            {"id": 3, "nombre": "Distribuidora 1", "nivel": "alto", "probabilidad": 0.102, "estaciones": 17, "horas_observadas": 24, "aviso_aemet": 1,
             "racha_prevista_max": 75, "hora_prevista": 14, "racha_observada_max": 81, "hora_observada": 17, "pico_en_franja": False, "pico_en_banda": False}]}}
    risk = {"fecha": "2026-10-15", "distribuidoras": [
        {"id": 3, "nombre": "Distribuidora 1", "nivel": "medio", "probabilidad": 0.052, "franja": {"inicio": 11, "fin": 17, "racha_pico": 48}, "aemet": {"pico": {"racha_kmh": 46}},
         "guardia": {"reforzar": True, "motivo": "aviso de tormenta de AEMET (amarillo)"}},
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "probabilidad": 0.031, "franja": {"inicio": 12, "fin": 18, "racha_pico": 50}}]}
    text = av.morning_message(verification, risk)
    assert text.startswith("GridPredic, parte de la mañana.\nAyer 14.10.2026: Distribuidora 2 BAJO (3 %): pico previsto 60 km/h a las 11 h, medido 44 km/h a las 11 h "
                           "(en la franja, en la banda de los escenarios, AEMET decía 43 km/h); Distribuidora 1 ALTO (10 %): pico previsto 75 km/h a las 14 h, "
                           "medido 81 km/h a las 17 h (fuera de la franja, fuera de la banda de los escenarios, aviso de AEMET de nivel 1).")
    assert ("Hoy 15.10.2026: Distribuidora 1 MEDIO (5 %), franja de 11:00 a 18:00, pico 48 km/h, AEMET 46 km/h, guardia reforzada (aviso de tormenta de AEMET (amarillo)); "
            "Distribuidora 2 BAJO (3 %), franja de 12:00 a 19:00, pico 50 km/h.") in text
    assert text.endswith("http://localhost:8086")
    # Without the risk of today, the block hoy of the verification; with the date of today, a warning; without anything, no message.
    assert "Hoy 15.10.2026: Distribuidora 2 BAJO (3 %)." in av.morning_message(verification, {"fecha": "2026-10-14"})
    assert "Atención: no hay riesgo calculado para hoy (el último es del 14.10.2026)" in av.morning_message(verification, {"fecha": "2026-10-14"}, "2026-10-15")
    assert "Atención" not in av.morning_message(verification, risk, "2026-10-15")
    assert av.morning_message({}, {}) == ""
    folder = tmp_path
    (folder / "verificacion.json").write_text(json.dumps(verification), encoding="utf-8")
    (folder / "riesgo.json").write_text(json.dumps(risk), encoding="utf-8")
    state = folder / "parte_manana.json"
    sent = []
    monkeypatch.setattr(av, "post", lambda url, message, field, timeout: sent.append(message) or 200)
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    rcfg = {"zona_horaria": "Europe/Madrid", "aviso": {"parte_manana_hora": 7}}
    # 06:20 in Madrid is 04:20 UTC in October: too early; 07:20 sends; the next hours do not send again; the next day does.
    assert av.send_morning(str(folder), rcfg, str(state), datetime(2026, 10, 15, 4, 20, tzinfo=timezone.utc)) == "no es la hora" and not sent
    assert av.send_morning(str(folder), rcfg, str(state), datetime(2026, 10, 15, 5, 20, tzinfo=timezone.utc)) == "enviado" and len(sent) == 1
    assert json.load(open(state))["enviado"] == "2026-10-15"
    assert av.send_morning(str(folder), rcfg, str(state), datetime(2026, 10, 15, 9, 20, tzinfo=timezone.utc)) == "ya enviado" and len(sent) == 1
    assert av.send_morning(str(folder), rcfg, str(state), datetime(2026, 10, 16, 5, 20, tzinfo=timezone.utc)) == "enviado" and len(sent) == 2
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    assert av.send_morning(str(folder), rcfg, str(tmp_path / "otro.json"), datetime(2026, 10, 17, 6, 20, tzinfo=timezone.utc)) == "sin url"
    assert av.send_morning(str(tmp_path / "vacio"), rcfg, None, datetime(2026, 10, 17, 6, 20, tzinfo=timezone.utc)) == "sin datos"


def test_the_outlook_of_the_day_after_tomorrow_goes_in_one_line():
    payload = {**PAYLOAD, "pasado_manana": {"fecha": "2026-01-16", "fecha_texto": "16.01.2026", "variante": "operativa_48h",
                                            "distribuidoras": [{"id": 3, "nombre": "Distribuidora 1", "nivel": "alto", "probabilidad": 0.14},
                                                               {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "probabilidad": 0.02}]}}
    text = av.message_from(payload, "alto")
    assert "Pasado mañana 16.01.2026, orientación a dos días: Distribuidora 1 ALTO (14 %)." in text
    assert "Distribuidora 2 BAJO" not in text and text.index("Pasado mañana") < text.index("11 % de los casos")
    # Without any distribuidora reaching the minimum, no line; without the block, nothing changes.
    assert "Pasado mañana" not in av.message_from(payload, "muy alto")
    assert "Pasado mañana" not in av.message_from(PAYLOAD, "alto")
    assert av.outlook_line({}) == ""


def test_every_message_also_goes_to_telegram(monkeypatch, tmp_path):
    calls = []

    class Answer:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(request, timeout=None):
        calls.append((request.full_url, json.loads(request.data.decode("utf-8"))))
        return Answer()

    monkeypatch.setattr(av.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.delenv("AVISO_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    # the token alone is not a channel
    assert av.send(PAYLOAD, {}) == "sin url" and not calls
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100200")
    assert av.send(PAYLOAD, {}) == "enviado"
    url, body = calls[-1]
    assert url == "https://api.telegram.org/bot123:ABC/sendMessage"
    assert body["chat_id"] == "-100200" and body["text"].startswith("Riesgo de interrupción de mañana")
    assert body["disable_web_page_preview"] is True


def test_with_webhook_and_telegram_one_failing_still_sends_and_none_is_an_error(monkeypatch):
    monkeypatch.setenv("AVISO_WEBHOOK_URL", "https://hooks.example/abc")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100200")
    sent = []

    def down(*a, **k):
        raise OSError("down")

    monkeypatch.setattr(av, "post", down)
    monkeypatch.setattr(av, "post_telegram", lambda token, chat, message, timeout: sent.append(message) or 200)
    assert av.send(PAYLOAD, {}) == "enviado" and sent
    monkeypatch.setattr(av, "post_telegram", down)
    assert av.send(PAYLOAD, {}) == "error: OSError"


def test_the_daily_summary_goes_even_without_alert():
    calm = {"fecha": "2026-10-21", "fecha_texto": "21.10.2026", "distribuidoras": [
        {"id": 2, "nombre": "Distribuidora 2", "nivel": "bajo", "probabilidad": 0.019},
        {"id": 3, "nombre": "Distribuidora 1", "nivel": "medio", "probabilidad": 0.05}]}
    assert av.message_from(calm, "alto") == ""
    text = av.summary_message(calm, "alto")
    assert text.startswith("Riesgo de interrupción de mañana 21.10.2026: Distribuidora 1 MEDIO (5 %); Distribuidora 2 BAJO (2 %).")
    assert "Guardia normal" in text and "Cuadro de mando" in text


def test_long_texts_are_cut_for_telegram_at_line_ends():
    text = "\n".join("x" * 100 for _ in range(90))
    pieces = av.chunks(text, 1000)
    assert all(len(p) <= 1000 for p in pieces) and "\n".join(pieces) == text


def test_the_bot_answers_the_commands_of_the_group_only(monkeypatch, tmp_path):
    import bot_telegram as bt

    folder = tmp_path / "data"
    folder.mkdir()
    (folder / "riesgo.json").write_text(json.dumps(PAYLOAD), encoding="utf-8")
    monkeypatch.setattr(av, "STATE_DIR", str(tmp_path / "estado"))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100200")
    updates = [
        {"update_id": 10, "message": {"chat": {"id": -100200}, "text": "/tarde@gridpredic_avisos_bot"}},
        {"update_id": 11, "message": {"chat": {"id": 555}, "text": "/tarde"}},
        {"update_id": 12, "message": {"chat": {"id": -100200}, "text": "hola"}},
        {"update_id": 13, "message": {"chat": {"id": -100200}, "text": "/ultimo", "message_thread_id": 7}},
    ]
    asked = []
    monkeypatch.setattr(bt, "get_updates", lambda token, offset=None, timeout=30.0: asked.append(offset) or (updates if offset is None else []))
    sent = []
    monkeypatch.setattr(av, "post_telegram", lambda token, chat, text, timeout=30.0, thread_id=None: sent.append((chat, text, thread_id)) or 200)
    assert bt.poll(str(folder), {}) == [("tarde", "enviado"), ("ultimo", "enviado")]
    assert sent[0][0] == "-100200" and sent[0][1].startswith("Riesgo de interrupción de mañana 15.01.2026:")
    # nothing sent yet: /ultimo shows how the evening would be, in the topic it was asked in
    assert sent[1][1].startswith("Todavía no se ha enviado ningún aviso") and sent[1][2] == 7
    # the next read starts after the last update, so nothing is answered twice
    assert bt.poll(str(folder), {}) == [] and asked == [None, 14]
    # once a message has been sent, /ultimo repeats it
    av.remember("Rayos sobre la red", "storm message")
    assert bt.answer("ultimo", str(folder), {}).splitlines()[1] == "Rayos sobre la red"
    assert bt.command_of("/manana extra") == "manana" and bt.command_of("tarde") == "" and bt.answer("otra", str(folder), {}) == ""
    assert "/rayos" in bt.answer("ayuda", str(folder), {}) and "falta rayos.json" in bt.answer("rayos", str(folder), {})
