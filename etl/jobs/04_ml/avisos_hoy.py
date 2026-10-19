"""
The warnings of AEMET during the day: what the evening run could not see.

The run of 20:30 scores the day with the warnings sent until then, but one
warning in five of a day is sent later (two in five of the thunderstorm
warnings; docs/investigacion-continua.md, 3d). Every hour, with the
observations, this module asks AEMET for the warnings in force, keeps the
answer in the Landing of the day (<landing>/<day>/avisos_hoy/<area>_<HHMM>.tar,
without touching the avisos/ folder the evening run read), finds the
warnings that cover some hour of today in the municipalities of every
distribuidora (the points of riesgo.json), and leaves
dashboard/data/avisos_hoy.json: by distribuidora the highest level of wind
and thunderstorm warnings for today, the warnings that did not exist when the
evening run was made, and the guard of the day recomputed with them. When a
new wind or thunderstorm warning raises the guard of a distribuidora to
reinforced, it is told once per distribuidora and day to the webhook.

    python3 /app/jobs/04_ml/avisos_hoy.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
for folder in (HERE, os.path.join(HERE, "..", "00_landing"), os.path.join(HERE, "..", "01_bronze"),
               os.path.join(HERE, "..", "02_silver")):
    if folder not in sys.path:
        sys.path.insert(0, folder)

from job_bronze_datos_publicos import parse_aemet_archive, polygon_points  # noqa: E402
from job_landing_meteo_openmeteo import live_filesystem  # noqa: E402
from job_landing_vivo import ask_warnings, day_folder, write_json_atomic  # noqa: E402
from job_silver_contexto_publico import WARNING_LEVELS, inside  # noqa: E402
import aviso_riesgo  # noqa: E402
from aviso_riesgo import LEVEL_NAMES, guard_advice  # noqa: E402

logger = logging.getLogger("avisos_hoy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PHENOMENA = {"VI": "viento", "TO": "tormenta"}

CONFIG_CANDIDATES = ["/app/config/04_ml/config_ml.json", "/opt/airflow/config/04_ml/config_ml.json",
                     os.path.join(HERE, "..", "..", "config", "04_ml", "config_ml.json")]


def load_config(path: str = None) -> dict:
    for candidate in ([path] if path else []) + CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                return json.load(file)
    raise FileNotFoundError("config_ml.json not found")


def parse_stamp(value: str):
    """A CAP time with its offset, or the generado of riesgo.json ("14.10.2026 18:33 UTC"), as an aware datetime; None otherwise."""
    if not value:
        return None
    text = str(value).strip()
    try:
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.strptime(text, "%d.%m.%Y %H:%M UTC").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def fetch_today(fs, folder: str, acfg: dict, now: datetime) -> tuple:
    """(CAP rows of the warnings in force, areas that answered, areas that failed); the tars kept under avisos_hoy/."""
    key = os.environ.get(acfg.get("clave_env", "AEMET_API_KEY"), "").strip()
    if not key:
        raise RuntimeError(f"No {acfg.get('clave_env', 'AEMET_API_KEY')} in the environment")
    rows, answered, failed = [], 0, 0
    for area in acfg.get("areas", ["esp"]):
        try:
            raw = ask_warnings(acfg["api"].format(area=area), key, float(acfg.get("timeout_s", 120)), area)
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("Warnings of AEMET for %s not downloaded: %s", area, str(error)[:200])
            continue
        answered += 1
        if raw is None:
            continue
        path = f"{folder}/avisos_hoy/{area}_{now:%H%M}.tar"
        try:
            fs.makedirs(f"{folder}/avisos_hoy", exist_ok=True)
        except Exception:  # noqa: BLE001
            pass
        with fs.open(path, "wb") as file:
            file.write(raw)
        rows += parse_aemet_archive(raw)
    return rows, answered, failed


def warnings_of_places(rows: list, places: list, day: str, zone: str) -> list:
    """
    The warnings (yellow or more, not cancelled) in force some hour of day
    whose polygon holds some of the places ((lat, lon) of the municipalities):
    one dict per warning and area with fenomeno, nivel, enviado (ISO UTC),
    inicio, fin, zona and municipios.
    """
    tz = ZoneInfo(zone)
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
    end = start + timedelta(days=1)
    found = {}
    for record in rows:
        if (record.get("tipo_mensaje") or "Alert") == "Cancel":
            continue
        level = WARNING_LEVELS.get(str(record.get("nivel") or "").strip().lower())
        if not level or level < 1:
            continue
        onset, expires, sent = parse_stamp(record.get("inicio")), parse_stamp(record.get("fin")), parse_stamp(record.get("enviado"))
        if onset is None or expires is None or onset >= end or expires <= start:
            continue
        points = polygon_points(record.get("poligono"))
        if len(points) < 3:
            continue
        covered = [name for name, lat, lon in places if inside(lat, lon, points)]
        if not covered:
            continue
        code = str(record.get("fenomeno") or "").split(";")[0].strip().upper()
        key = (record.get("identificador"), record.get("zona"), code)
        found[key] = {"fenomeno": code, "nombre": PHENOMENA.get(code, str(record.get("fenomeno") or "").split(";")[-1].strip().lower()),
                      "nivel": level, "nivel_texto": LEVEL_NAMES.get(level, str(level)),
                      "enviado": sent.astimezone(timezone.utc).isoformat(timespec="minutes") if sent else None,
                      "inicio": onset.astimezone(tz).strftime("%H:%M"), "fin": expires.astimezone(tz).strftime("%d.%m %H:%M"),
                      "zona": record.get("zona_nombre"), "municipios": covered, "parametro": record.get("parametro")}
    return sorted(found.values(), key=lambda w: (-w["nivel"], w["fenomeno"]))


def today_payload(risk: dict, rows: list, now: datetime, zone: str) -> dict:
    """avisos_hoy.json: by distribuidora, the wind and thunderstorm levels for today, the new warnings and the guard."""
    day = risk.get("fecha")
    made = parse_stamp(risk.get("generado"))
    out = {"generado": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), "fecha": day,
           "riesgo_generado": risk.get("generado"), "distribuidoras": []}
    for d in risk.get("distribuidoras", []):
        places = [(m.get("nombre"), float(m["lat"]), float(m["lon"])) for m in d.get("municipios", []) if m.get("lat") is not None and m.get("lon") is not None]
        warnings = warnings_of_places(rows, places, day, zone) if day else []
        wind = max([w["nivel"] for w in warnings if w["fenomeno"] == "VI"], default=0)
        storm = max([w["nivel"] for w in warnings if w["fenomeno"] == "TO"], default=0)
        highest = max([w["nivel"] for w in warnings], default=0)
        new = [w for w in warnings if made and parse_stamp(w["enviado"]) and parse_stamp(w["enviado"]) > made]
        evening = d.get("guardia") or {}
        guard = guard_advice(d.get("nivel", "bajo"), float(wind), float(storm))
        raised = bool(guard["reforzar"]) and not bool(evening.get("reforzar"))
        out["distribuidoras"].append({
            "id": d.get("id"), "nombre": d.get("nombre"), "nivel": d.get("nivel"), "aviso_viento_nivel": wind,
            "aviso_tormenta_nivel": storm, "aviso_max_nivel": highest, "avisos": warnings, "nuevos": new,
            "guardia": guard, "guardia_anoche": evening, "subida": raised,
            "texto": guard_text(d.get("nombre"), guard, evening, new, now, zone),
        })
    return out


def guard_text(name: str, guard: dict, evening: dict, new: list, now: datetime, zone: str) -> str:
    stamp = now.astimezone(ZoneInfo(zone)).strftime("%H:%M")
    warnings = "avisos nuevos" if len(new) > 1 else "aviso nuevo"
    if guard.get("reforzar") and not evening.get("reforzar"):
        reasons = ", ".join(f"{w['nombre']} {w['nivel_texto']} de {w['inicio']} a {w['fin']}" for w in new) or guard.get("motivo", "")
        return f"{name}: guardia reforzada desde las {stamp} por {warnings} de AEMET para hoy ({reasons})"
    if new:
        reasons = ", ".join(f"{w['nombre']} {w['nivel_texto']}" for w in new)
        return f"{name}: {warnings} de AEMET para hoy ({reasons}); la guardia ya estaba {'reforzada' if guard.get('reforzar') else 'normal'}"
    return f"{name}: sin avisos nuevos de AEMET desde anoche"


def send_raised(payload: dict, rcfg: dict, state_path: str) -> str:
    """The distribuidoras whose guard went up today, once per distribuidora and day. "enviado", "nada nuevo", "ya avisado", "sin url" or "error: ...". """
    cfg = aviso_riesgo.alert_settings(rcfg)
    state = {}
    if state_path and os.path.exists(state_path):
        try:
            with open(state_path, encoding="utf-8") as file:
                state = json.load(file)
        except (OSError, ValueError):
            state = {}
    day = payload.get("fecha")
    due = [d for d in payload.get("distribuidoras", []) if d.get("subida") and state.get(str(d.get("id"))) != day]
    if not due:
        return "nada nuevo" if not any(d.get("subida") for d in payload.get("distribuidoras", [])) else "ya avisado"
    lines = [f"Avisos nuevos de AEMET para hoy {aviso_riesgo.day_text(day)}:"] + [f"- {d['texto']}." for d in due]
    lines.append(f"Cuadro de mando: {aviso_riesgo.DASHBOARD_URL}")
    message = "\n".join(lines)
    url = os.environ.get(cfg["url_env"], "").strip()
    if not url:
        logger.info("No %s in the environment: the message is not sent\n%s", cfg["url_env"], message)
        return "sin url"
    try:
        aviso_riesgo.post(url, message, cfg["campo"], float(cfg["timeout_s"]))
    except Exception as error:  # noqa: BLE001
        logger.error("The webhook did not take the message: %s", type(error).__name__)
        return f"error: {type(error).__name__}"
    for d in due:
        state[str(d.get("id"))] = day
    if state_path:
        try:
            write_json_atomic(state_path, state)
        except OSError as error:
            logger.warning("State not written in %s: %s", state_path, error)
    logger.info("Raised guard sent\n%s", message)
    return "enviado"


def refresh(rcfg: dict, dashboard: str, landing: str, now: datetime = None, fs=None) -> dict:
    """The whole step: ask AEMET, write avisos_hoy.json next to riesgo.json and tell the webhook. Returns the payload."""
    now = now or datetime.now(timezone.utc)
    zone = rcfg.get("zona_horaria", "Europe/Madrid")
    risk_path = os.path.join(dashboard, "riesgo.json")
    with open(risk_path, encoding="utf-8") as file:
        risk = json.load(file)
    today = now.astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d")
    if risk.get("fecha") != today:
        logger.warning("riesgo.json is of %s, not of today %s: nothing to refresh", risk.get("fecha"), today)
        return {}
    fs = fs or live_filesystem(landing)
    folder = day_folder(landing, datetime.strptime(today, "%Y-%m-%d").date())
    rows, answered, failed = fetch_today(fs, folder, rcfg["avisos"], now.astimezone(ZoneInfo(zone)))
    if not answered:
        raise RuntimeError("AEMET did not answer for any area")
    payload = today_payload(risk, rows, now, zone)
    payload["areas_sin_respuesta"] = failed
    target = os.path.join(dashboard, "avisos_hoy.json")
    write_json_atomic(target, payload, ensure_ascii=False)
    logger.info("avisos_hoy.json written in %s (%s CAP rows): %s", target, len(rows),
                "; ".join(d["texto"] for d in payload["distribuidoras"]))
    payload["envio"] = send_raised(payload, rcfg, os.path.join(dashboard, "avisos_hoy_aviso.json"))
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--dashboard-dir", default=None)
    parser.add_argument("--landing", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    rcfg = config["riesgo_manana"]
    dashboard = args.dashboard_dir or rcfg.get("dashboard_dir", "/app/dashboard/data")
    landing = args.landing or rcfg["landing"]
    payload = refresh(rcfg, dashboard, landing)
    print(json.dumps({"envio": payload.get("envio"), "textos": [d["texto"] for d in payload.get("distribuidoras", [])]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
