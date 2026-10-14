"""
The message of the evening: a short text with the risk of tomorrow of every
distribuidora, sent to a webhook (Slack, Teams, Discord or any endpoint
that takes a JSON with a text field) when some distribuidora reaches the
level of riesgo_manana.aviso.nivel_minimo. The URL of the webhook comes from
the environment variable aviso.url_env (compose.yaml takes it from .env);
without it nothing is sent. The URL is a secret and is never logged.

    python aviso_riesgo.py /app/dashboard/data/riesgo.json   # prints the message, sends it if the URL is set
"""

import json
import logging
import os
import sys
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger("aviso")

DEFAULTS = {"url_env": "AVISO_WEBHOOK_URL", "nivel_minimo": "alto", "campo": "text", "timeout_s": 30, "rayos_cada_min": 60}
ORDER = ["bajo", "medio", "alto", "muy alto"]


def alert_settings(rcfg: dict) -> dict:
    return {**DEFAULTS, **(rcfg.get("aviso") or {})}


def reached(level: str, minimum: str) -> bool:
    try:
        return ORDER.index(level) >= ORDER.index(minimum)
    except ValueError:
        return False


def message_from(payload: dict, minimum: str = "alto", ensemble: dict = None) -> str:
    """
    The text of the evening from riesgo.json of the dashboard: one line per
    distribuidora in alert with the level, the probability, the expected
    incidents and customers, the window, the zones of most risk, the bases
    of the crews and, with ensemble.json of the same day, the spread of the
    runs; empty when no distribuidora reaches minimum.
    """
    blocks = [d for d in payload.get("distribuidoras", []) if reached(d.get("nivel", "bajo"), minimum)]
    if not blocks:
        return ""
    bands = {str(b.get("id")): b for b in (ensemble or {}).get("distribuidoras", [])} if (ensemble or {}).get("fecha") == payload.get("fecha") else {}
    lines = [f"Riesgo de interrupción de mañana {payload.get('fecha_texto') or payload.get('fecha', '')}:"]
    for d in sorted(blocks, key=lambda b: -(b.get("probabilidad") or 0)):
        imp, fr, plan = d.get("impacto") or {}, d.get("franja") or {}, d.get("brigadas") or {}
        zones = sorted(d.get("zonas", []), key=lambda z: -(z.get("probabilidad") or 0))[:3]
        line = (f"- {d.get('nombre')}: {str(d.get('nivel', '')).upper()} ({100 * float(d.get('probabilidad') or 0):.0f} %, "
                f"{float(d.get('veces_tasa_base') or 0):.1f} veces su tasa base); averías esperadas {float(d.get('esperadas') or 0):.1f}, "
                f"clientes esperados sin luz {float(imp.get('clientes_esperados') or 0):.0f}")
        if fr.get("inicio") is not None:
            line += f"; franja de {int(fr['inicio'])}:00 a {int(fr['fin']) + 1}:00 (pico {float(fr.get('racha_pico') or 0):.0f} km/h)"
        if zones:
            line += "; líneas: " + ", ".join(f"{z.get('nombre')} ({100 * float(z.get('probabilidad') or 0):.1f} %)" for z in zones)
        if plan.get("bases"):
            line += "; brigadas en " + ", ".join(b.get("municipio", "") for b in plan["bases"])
            if plan.get("posicionamiento"):
                line += f" ({plan['posicionamiento']})"
        if d.get("motivos") and not str(d["motivos"]).startswith("ninguno"):
            line += f"; motivos: {d['motivos']}"
        band = bands.get(str(d.get("id"))) or {}
        scenarios = (band.get("texto") if band.get("horas") else None) or (d.get("escenarios") or {}).get("texto")
        if scenarios:
            line += f"; escenarios: {scenarios}"
        if (d.get("aemet") or {}).get("texto"):
            line += f"; {d['aemet']['texto']}"
        lines.append(line + ".")
    rule = (payload.get("reglas") or {}).get("texto")
    if rule:
        lines.append(rule)
    lines.append("Cuadro de mando: http://localhost:8086")
    return "\n".join(lines)


def post(url: str, message: str, field: str = "text", timeout: float = 30.0) -> int:
    """POST {field: message} as JSON; returns the HTTP status. The URL never goes to the log."""
    body = json.dumps({field: message}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json",
                                                               "User-Agent": "GridPredict-TFM/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as answer:
        return int(answer.status)


def read_ensemble(folder: str) -> dict:
    """ensemble.json of the dashboard folder, or None."""
    path = os.path.join(folder or "", "ensemble.json")
    if not folder or not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return None


def send(payload: dict, rcfg: dict, ensemble: dict = None) -> str:
    """
    Builds the message and sends it when the URL is in the environment.
    Returns "enviado", "sin alerta", "sin url" or "error: ...".
    """
    cfg = alert_settings(rcfg)
    message = message_from(payload, cfg["nivel_minimo"], ensemble)
    if not message:
        return "sin alerta"
    url = os.environ.get(cfg["url_env"], "").strip()
    if not url:
        logger.info("No %s in the environment: the message is not sent\n%s", cfg["url_env"], message)
        return "sin url"
    try:
        status = post(url, message, cfg["campo"], float(cfg["timeout_s"]))
    except Exception as error:  # noqa: BLE001
        logger.error("The webhook did not take the message: %s", type(error).__name__)
        return f"error: {type(error).__name__}"
    logger.info("Message of the evening sent (HTTP %s)\n%s", status, message)
    return "enviado"


def storm_message(payload: dict) -> str:
    """The text of a storm over the network from rayos.json: one line per distribuidora with flashes within the first radius."""
    blocks = [d for d in payload.get("distribuidoras", []) if d.get("nivel") == "tormenta sobre la red"]
    if not blocks:
        return ""
    when = str(payload.get("generado", ""))[11:16]
    lines = [f"Rayos sobre la red{(' a las ' + when + ' UTC') if when else ''} (satélite MTG):"]
    for d in blocks:
        line = f"- {d.get('nombre')}: {d.get('texto', '')}"
        if d.get("municipios"):
            line += f"; municipios bajo la tormenta: {', '.join(d['municipios'][:6])}" + (", ..." if len(d["municipios"]) > 6 else "")
        lines.append(line + ".")
    lines.append("Antes del 17 % de las averías hubo rayos a menos de 10 km de la red en la hora previa (1 % en las horas normales). Cuadro de mando: http://localhost:8086")
    return "\n".join(lines)


def send_storm(payload: dict, rcfg: dict, state_path: str, now: datetime = None) -> str:
    """
    The storm message goes to the webhook once per distribuidora and
    aviso.rayos_cada_min minutes (state_path remembers when each was last
    sent), so a storm of two hours gives two messages, not eight. Returns
    "enviado", "sin tormenta", "ya avisado", "sin url" or "error: ...".
    """
    cfg = alert_settings(rcfg)
    every = float(cfg.get("rayos_cada_min", 60))
    now = now or datetime.now(timezone.utc)
    state = {}
    if state_path and os.path.exists(state_path):
        try:
            with open(state_path, encoding="utf-8") as file:
                state = json.load(file)
        except (OSError, ValueError):
            state = {}
    blocks = [d for d in payload.get("distribuidoras", []) if d.get("nivel") == "tormenta sobre la red"]
    if not blocks:
        return "sin tormenta"
    due = []
    for d in blocks:
        last = state.get(str(d.get("id")))
        try:
            fresh = last and (now - datetime.fromisoformat(last)).total_seconds() < every * 60
        except ValueError:
            fresh = False
        if not fresh:
            due.append(d)
    if not due:
        return "ya avisado"
    message = storm_message({**payload, "distribuidoras": due})
    url = os.environ.get(cfg["url_env"], "").strip()
    if not url:
        logger.info("No %s in the environment: the storm message is not sent\n%s", cfg["url_env"], message)
        return "sin url"
    try:
        post(url, message, cfg["campo"], float(cfg["timeout_s"]))
    except Exception as error:  # noqa: BLE001
        logger.error("The webhook did not take the storm message: %s", type(error).__name__)
        return f"error: {type(error).__name__}"
    for d in due:
        state[str(d.get("id"))] = now.isoformat(timespec="seconds")
    if state_path:
        try:
            with open(state_path, "w", encoding="utf-8") as file:
                json.dump(state, file)
        except OSError as error:
            logger.warning("Storm state not written in %s: %s", state_path, error)
    logger.info("Storm message sent\n%s", message)
    return "enviado"


def failure_message(context: dict) -> str:
    """What a task failure of Airflow looks like in the webhook: DAG, task, attempt, day and the error."""
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    attempt = getattr(ti, "try_number", None)
    when = context.get("logical_date") or context.get("execution_date") or context.get("run_id") or ""
    error = str(context.get("exception") or "")[:300]
    return (f"GridPredict: la tarea {task_id} de {dag_id} ha fallado" + (f" (intento {attempt})" if attempt else "")
            + (f", ejecución {str(when)[:16]}" if when else "") + (f": {error}" if error else "") + ". Airflow: http://localhost:8080")


def notify_failure(context: dict) -> str:
    """on_failure_callback of the DAGs: the failure goes to the same webhook as the message of the evening; silent without URL."""
    url = os.environ.get(DEFAULTS["url_env"], "").strip()
    message = failure_message(context)
    if not url:
        logger.warning("Task failed and no %s to tell it: %s", DEFAULTS["url_env"], message)
        return "sin url"
    try:
        post(url, message, DEFAULTS["campo"], float(DEFAULTS["timeout_s"]))
    except Exception as error:  # noqa: BLE001
        logger.error("The webhook did not take the failure: %s", type(error).__name__)
        return f"error: {type(error).__name__}"
    return "enviado"


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    path = argv[0] if argv else "/app/dashboard/data/riesgo.json"
    with open(path, encoding="utf-8") as file:
        payload = json.load(file)
    rcfg = {}
    config_path = argv[1] if len(argv) > 1 else None
    if config_path and os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as file:
            rcfg = json.load(file).get("riesgo_manana", {})
    result = send(payload, rcfg, read_ensemble(os.path.dirname(os.path.abspath(path))))
    print(result)
    return 0 if result in ("enviado", "sin alerta", "sin url") else 1


if __name__ == "__main__":
    sys.exit(main())
