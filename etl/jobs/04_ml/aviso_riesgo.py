"""
The message of the evening: a short text with the risk of tomorrow of every
distribuidora, sent to a webhook (Slack, Teams, Discord or any endpoint
that takes a JSON with a text field) when some distribuidora reaches the
level of riesgo_manana.aviso.nivel_minimo, or every evening with
aviso.resumen_diario. The URL of the webhook comes from the environment
variable aviso.url_env (compose.yaml takes it from .env). Every message of the
operation (the evening, the morning report, the new warnings of the day, the
storms and the failed tasks) can also go to a Telegram group: with
TELEGRAM_BOT_TOKEN (the token BotFather gives) and TELEGRAM_CHAT_ID (the group,
a negative number) the same text goes through the sendMessage of the Bot API.
Without a webhook nor Telegram nothing is sent. The URL and the token are
secrets and are never logged.

    python aviso_riesgo.py /app/dashboard/data/riesgo.json   # prints the message, sends it if a channel is set
    python aviso_riesgo.py --telegram-chats                  # the groups the bot is in, to find TELEGRAM_CHAT_ID
    python aviso_riesgo.py --prueba                          # a test message to every channel set
"""

import json
import logging
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger("aviso")

DEFAULTS = {"url_env": "AVISO_WEBHOOK_URL", "nivel_minimo": "alto", "campo": "text", "timeout_s": 30, "rayos_cada_min": 60,
            "parte_manana_hora": 7, "resumen_diario": False}
LEVEL_NAMES = {1: "amarillo", 2: "naranja", 3: "rojo"}
# The addresses the messages point at (the ports of compose.yaml: the dashboard and Airflow). AVISO_CUADRO_URL
# changes the one of the dashboard, for people who read the messages on the phone and cannot open localhost.
DASHBOARD_URL = os.environ.get("AVISO_CUADRO_URL", "").strip() or "http://localhost:8086"
AIRFLOW_URL = "http://localhost:8084"
# The guard: reinforce it the days in alert (alto or muy alto) and the days with a wind or thunderstorm warning of
# AEMET, the policy that covers most customer-hours per day spent in the replay of 2024 - 2026 (caso-de-negocio.md, 2b).
GUARD_LEVELS = ("alto", "muy alto")


def write_state(path: str, state: dict) -> None:
    """A state file, replaced atomically (escritura_atomica.write_json_atomic of the Landing, only the standard library)."""
    landing = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "00_landing")
    if landing not in sys.path:
        sys.path.append(landing)
    from escritura_atomica import write_json_atomic

    write_json_atomic(path, state, ensure_ascii=False)


def guard_advice(level: str, wind: float, storm: float) -> dict:
    """{"reforzar": bool, "motivo": text}: why the guard of the day is reinforced, or "normal"."""
    reasons = []
    if level in GUARD_LEVELS:
        reasons.append(f"nivel {level}")
    if wind is not None and wind == wind and wind >= 1:
        reasons.append(f"aviso de viento de AEMET ({LEVEL_NAMES.get(int(wind), int(wind))})")
    if storm is not None and storm == storm and storm >= 1:
        reasons.append(f"aviso de tormenta de AEMET ({LEVEL_NAMES.get(int(storm), int(storm))})")
    return {"reforzar": bool(reasons), "motivo": "; ".join(reasons) if reasons else "normal"}


ORDER = ["bajo", "medio", "alto", "muy alto"]


def dec(x, digits: int = 1) -> str:
    """A number with the decimal comma of Spanish ("4,1", "0,4")."""
    return f"{float(x or 0):.{digits}f}".replace(".", ",")


def alert_settings(rcfg: dict) -> dict:
    return {**DEFAULTS, **(rcfg.get("aviso") or {})}


def reached(level: str, minimum: str) -> bool:
    try:
        return ORDER.index(level) >= ORDER.index(minimum)
    except ValueError:
        return False


def highlights(d: dict) -> list:
    """
    What stands out of a network's day, as (icon, short text), to read at a glance before the detail: strong
    wind (racha of 50 km/h or more, or 0,9 of the p98 of the cell), rain of 5 mm or more, the warnings of AEMET
    (storm, wind, other), heat of 35 degrees or more. Empty on a calm day.
    """
    out = []
    gust, climate = float(d.get("racha_max_kmh") or 0), float(d.get("racha_clima_max") or 0)
    if gust >= 50 or climate >= 0.9:
        out.append(("💨", f"racha de {gust:.0f} km/h" + (f" ({dec(climate, 2)} veces su p98)" if climate >= 0.9 else "")))
    rain = float(d.get("lluvia_mm") or 0)
    if rain >= 5:
        out.append(("🌧️", f"{dec(rain, 1)} mm de lluvia"))
    storm, wind, top = (float(d.get(k) or 0) for k in ("aviso_tormenta_nivel", "aviso_viento_nivel", "aviso_max_nivel"))
    if storm >= 1:
        out.append(("⛈️", f"aviso {LEVEL_NAMES.get(int(storm), int(storm))} de tormenta"))
    if wind >= 1:
        out.append(("🌬️", f"aviso {LEVEL_NAMES.get(int(wind), int(wind))} de viento"))
    if top >= 1 and storm < 1 and wind < 1:
        out.append(("⚠️", f"aviso {LEVEL_NAMES.get(int(top), int(top))} de AEMET"))
    heat = d.get("temp_max_c")
    if heat is not None and float(heat) >= 35:
        out.append(("🌡️", f"{float(heat):.0f} °C"))
    return out


def icons_of(d: dict) -> str:
    """The icons of highlights(d) in a row (" 💨🌧️"), or "" on a calm day."""
    return "".join(icon for icon, _ in highlights(d))


def maps_link(base: dict) -> str:
    """A Google Maps link to the base (opens the maps app on a phone, with the way there); "" without coordinates."""
    lat, lon = base.get("lat"), base.get("lon")
    if lat is None or lon is None:
        return ""
    return f"https://www.google.com/maps?q={float(lat):.5f},{float(lon):.5f}"


def message_from(payload: dict, minimum: str = "alto", ensemble: dict = None) -> str:
    """
    The text of the evening from riesgo.json of the dashboard: one line per
    distribuidora in alert, or whose guard is reinforced by a wind or
    thunderstorm warning of AEMET, with the level, the probability, the expected
    incidents and customers, the window, the zones of most risk, the bases
    of the crews and, with ensemble.json of the same day, the spread of the
    runs; empty when no distribuidora reaches minimum.
    """
    blocks = [d for d in payload.get("distribuidoras", [])
              if reached(d.get("nivel", "bajo"), minimum) or (d.get("guardia") or {}).get("reforzar")]
    if not blocks:
        return ""
    bands = {str(b.get("id")): b for b in (ensemble or {}).get("distribuidoras", [])} if (ensemble or {}).get("fecha") == payload.get("fecha") else {}
    lines = [f"Riesgo de interrupción de mañana {payload.get('fecha_texto') or payload.get('fecha', '')}:"]
    for d in sorted(blocks, key=lambda b: -(b.get("probabilidad") or 0)):
        imp, fr, plan = d.get("impacto") or {}, d.get("franja") or {}, d.get("brigadas") or {}
        zones = sorted(d.get("zonas", []), key=lambda z: -(z.get("probabilidad") or 0))[:3]
        # One line per distribuidora with the level and the guard, and under it one short line per thing, so the
        # message reads on a phone (Telegram) as well as in Slack or Teams.
        head = (f"- {d.get('nombre')}: {str(d.get('nivel', '')).upper()} ({100 * float(d.get('probabilidad') or 0):.0f} %, "
                f"{dec(d.get('veces_tasa_base'), 1)} veces su tasa base)" + (" " + icons_of(d) if icons_of(d) else ""))
        guard = d.get("guardia") or {}
        if guard.get("reforzar"):
            head += f"; guardia reforzada ({guard.get('motivo', '')})"
        lines.append(head)
        marks = highlights(d)
        if marks:
            lines.append("  " + " · ".join(f"{icon} {text}" for icon, text in marks))
        lines.append(f"  Averías esperadas {dec(d.get('esperadas'), 1)}; clientes esperados sin luz {float(imp.get('clientes_esperados') or 0):.0f}.")
        if fr.get("inicio") is not None:
            lines.append(f"  Franja de {int(fr['inicio'])}:00 a {int(fr['fin']) + 1}:00, pico de racha {float(fr.get('racha_pico') or 0):.0f} km/h.")
        if zones:
            lines.append("  Líneas: " + ", ".join(f"{z.get('nombre')} ({dec(100 * float(z.get('probabilidad') or 0), 1)} %)" for z in zones) + ".")
        if plan.get("bases"):
            # each base with the way there (a maps link), for the crews on the road
            bases = "  Brigadas: " + ", ".join(b.get("municipio", "") + (f" (mapa: {maps_link(b)})" if maps_link(b) else "") for b in plan["bases"])
            if plan.get("posicionamiento"):
                bases += f" ({plan['posicionamiento']})"
            lines.append(bases + ".")
        if d.get("motivos") and not str(d["motivos"]).startswith("ninguno"):
            lines.append(f"  Motivos: {d['motivos']}.")
        band = bands.get(str(d.get("id"))) or {}
        scenarios = (band.get("texto") if band.get("horas") else None) or (d.get("escenarios") or {}).get("texto")
        if scenarios:
            lines.append(f"  Escenarios: {scenarios}.")
        if (d.get("aemet") or {}).get("texto"):
            lines.append(f"  {d['aemet']['texto']}.")
    outlook = outlook_line(payload, minimum)
    if outlook:
        lines.append(outlook)
    rule = (payload.get("reglas") or {}).get("texto")
    if rule:
        lines.append(rule)
    if payload.get("artefactos_error"):
        lines.append(f"El modelo de esta tarde no se ha guardado en el almacén de modelos ({payload['artefactos_error']}).")
    lines.append(f"Cuadro de mando: {DASHBOARD_URL}")
    return "\n".join(lines)


def summary_message(payload: dict, minimum: str = "alto", ensemble: dict = None) -> str:
    """
    The text of an evening with no distribuidora in alert, for aviso.resumen_diario: the same detail of every
    network as an evening in alert (level, what stands out, expected failures, window, lines, crews, reasons,
    scenarios and AEMET), under a first line that says the guard stays normal, so the group knows the evening
    ran and what it saw.
    """
    if not payload.get("distribuidoras"):
        return ""
    text = message_from(payload, "bajo", ensemble)
    head = f"Riesgo de interrupción de mañana {payload.get('fecha_texto') or payload.get('fecha', '')}:"
    return text.replace(head, head[:-1] + ": guardia normal en todas las redes.", 1)


def outlook_line(payload: dict, minimum: str = "alto") -> str:
    """
    One line with the distribuidoras whose outlook of the day after tomorrow
    (pasado_manana of riesgo.json) reaches minimum, as an orientation; empty
    without it.
    """
    block = payload.get("pasado_manana") or {}
    reached_ones = [d for d in block.get("distribuidoras", []) if reached(d.get("nivel", "bajo"), minimum)]
    if not reached_ones:
        return ""
    parts = ", ".join(f"{d.get('nombre')} {str(d.get('nivel', '')).upper()} ({100 * float(d.get('probabilidad') or 0):.0f} %)"
                      for d in reached_ones)
    return f"Pasado mañana {block.get('fecha_texto') or block.get('fecha', '')}, orientación a dos días: {parts}."


def post(url: str, message: str, field: str = "text", timeout: float = 30.0) -> int:
    """POST {field: message} as JSON; returns the HTTP status. The URL never goes to the log."""
    body = json.dumps({field: message}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json",
                                                               "User-Agent": "GridPredic-TFM/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as answer:
        return int(answer.status)


TELEGRAM_API = "https://api.telegram.org"
# Where the last message sent is kept, for the /last command of the Telegram bot (bot_telegram.py): out of
# dashboard/data on purpose, because that folder is served and copied to the public demo.
STATE_DIR = os.environ.get("AVISO_ESTADO_DIR", "/opt/airflow/logs/telegram")
TELEGRAM_MAX = 4000  # sendMessage takes up to 4096 characters; longer texts go in several messages


def telegram_settings() -> tuple:
    """(token, chat_id) from TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID, or None when one of them is missing."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    return (token, chat) if token and chat else None


_OPEN_TAG_RE = re.compile(r"<[a-z]")


def balanced(text: str) -> bool:
    """Whether every HTML tag opened in text is closed in it and no entity (&amp;) is left half written."""
    return len(_OPEN_TAG_RE.findall(text)) == text.count("</") and text.rfind("&") <= text.rfind(";")


def chunks(message: str, size: int = TELEGRAM_MAX) -> list:
    """
    The text cut into pieces of at most size characters, at the end of a line when there is one. A single line
    longer than size is cut at a space that leaves every tag closed and no entity half written, so a piece never
    ends inside <b>, <a> or &amp; (Telegram would refuse the whole piece with "can't parse entities").
    """
    out, rest = [], message
    while len(rest) > size:
        cut = rest.rfind("\n", 0, size)
        if cut <= 0:
            cut = next((i for i in range(size, 0, -1) if rest[i] == " " and balanced(rest[:i])), size)
        out.append(rest[:cut])
        rest = rest[cut:].lstrip("\n ")
    return out + ([rest] if rest else [])


LEVEL_ICONS = {"BAJO": "🟢", "MEDIO": "🟡", "ALTO": "🟠", "MUY ALTO": "🔴"}
_LEVEL_RE = re.compile(r"\b(MUY ALTO|ALTO|MEDIO|BAJO)\b")
_LINK_RE = re.compile(r"^(Cuadro de mando|Airflow): (\S+)$")
_MAP_RE = re.compile(r"([^,:]+?) \(mapa: (https?://\S+?)\)")


def telegram_format(message: str) -> str:
    """
    The same message dressed for Telegram (HTML parse mode): the first line in bold, the levels with their colour
    (🟢 Bajo, 🟡 Medio, 🟠 Alto, 🔴 Muy alto), the name of the network in bold at the head of its block, the lines of
    detail indented with a middle dot, the guard with its icon and the link to the dashboard as a link. The webhook
    keeps the plain text, which is what Slack and Teams show as it comes.
    """
    esc = lambda s: s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")  # noqa: E731
    out = []
    for i, raw in enumerate(message.split("\n")):
        line = esc(raw)
        link = _LINK_RE.match(raw)
        if link:
            net = re.search(r"#distribuidora-(\d+)$", link.group(2))
            label = (f"Abrir la Distribuidora {net.group(1)} en el cuadro de mando" if net else "Abrir el cuadro de mando") \
                if link.group(1) == "Cuadro de mando" else "Abrir Airflow"
            out.append(f'<a href="{esc(link.group(2))}">{label}</a>')
            continue
        line = _LEVEL_RE.sub(lambda m: f"{LEVEL_ICONS[m.group(1)]} {m.group(1).capitalize()}", line)
        line = line.replace("guardia reforzada", "👷 guardia reforzada").replace("Guardia normal", "👷 Guardia normal").replace("guardia normal", "👷 guardia normal")
        if i == 0:
            out.append(f"<b>{line}</b>")
        elif raw.startswith("- "):
            # the name of the network in bold: what goes before its level or its colon
            out.append("• " + re.sub(r"^(.+?)(?=: | 🟢| 🟡| 🟠| 🔴)", r"<b>\1</b>", line[2:], count=1))
        elif raw.startswith("  "):
            # the bases of the crews as links ("📍 Municipio 24"), the rest as it comes
            out.append("   · " + _MAP_RE.sub(lambda m: f'{m.group(1)[:len(m.group(1)) - len(m.group(1).lstrip())]}📍 <a href="{m.group(2)}">{m.group(1).strip()}</a>', line.strip()))
        elif raw.startswith(("Ayer ", "Hoy ", "Pasado mañana ")):
            out.append(f"<b>{line}</b>")
        elif raw.startswith("Atención"):
            out.append(f"⚠️ {line}")
        else:
            out.append(line)
    return "\n".join(out)


def split_by_network(message: str) -> list:
    """
    The message of the evening as one message per network, so the group reads one network at a time on the phone:
    the first one with the header, each one with the block of that network (its line and the detail under it) and a link to it; the
    day after tomorrow and the general rules go in a last message of their own. A message with less than two
    network blocks, or that is not the evening (the morning report, a storm), comes back whole.
    """
    lines = message.split("\n")
    starts = [i for i, line in enumerate(lines) if line.startswith("- ")]
    if len(starts) < 2 or not any(line.startswith("Riesgo de interrupción de") for line in lines[:starts[0]]):
        return [message]
    if not all(lines[i + 1].startswith("  ") for i in starts if i + 1 < len(lines)):
        return [message]
    pre = lines[:starts[0]]
    blocks, end = [], starts[0]
    for k, s in enumerate(starts):
        e = starts[k + 1] if k + 1 < len(starts) else s + 1
        while e < len(lines) and (k + 1 < len(starts) and e < starts[k + 1] or lines[e].startswith("  ")):
            if k + 1 < len(starts) and e >= starts[k + 1]:
                break
            e += 1
        blocks.append(lines[s:e])
        end = e
    tail = lines[end:]
    link = [line for line in tail if line.startswith(("Cuadro de mando:", "Airflow:"))]
    rest = [line for line in tail if line not in link]
    out = []
    for block in blocks:
        name = block[0][2:].split(":")[0].strip()
        # the link of each one goes straight to its network on the dashboard (#distribuidora-1)
        anchor = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        own = [line if "#" in line or not line.startswith("Cuadro de mando:") else f"{line}#{anchor}" for line in link]
        # the header (the day and the guard of all the networks) only in the first one: the others are just their network
        out.append("\n".join((pre if not out else []) + block + own))
    # the day after tomorrow and the general rules, in a message of their own at the end
    if rest:
        out.append("\n".join(rest + link))
    return out


def post_telegram(token: str, chat_id: str, message: str, timeout: float = 30.0, thread_id: int = None, markup: dict = None) -> int:
    """sendMessage of the Telegram Bot API, dressed with telegram_format and without link previews, one message per
    network for the evening (split_by_network); returns the last HTTP status. The token is part of the URL, so
    neither goes to the log."""
    status = 0
    for piece in (p for single in split_by_network(message) for p in chunks(telegram_format(single))):
        fields = {"chat_id": chat_id, "text": piece, "parse_mode": "HTML", "disable_web_page_preview": True}
        if thread_id:
            fields["message_thread_id"] = thread_id
        if markup:
            fields["reply_markup"] = markup
        body = json.dumps(fields, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(f"{TELEGRAM_API}/bot{token}/sendMessage", data=body,
                                         headers={"Content-Type": "application/json", "User-Agent": "GridPredic-TFM/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as answer:
            status = int(answer.status)
    return status


def deliver(message: str, cfg: dict, what: str = "message") -> str:
    """
    The text to every channel that is set: the webhook of cfg["url_env"] and the Telegram group. "enviado" when
    at least one took it (the other, if it failed, goes to the log), "sin url" when there is no channel and
    "error: <type>" when none took it, so the DAG can retry.
    """
    url = os.environ.get(cfg["url_env"], "").strip()
    telegram = telegram_settings()
    if not url and not telegram:
        logger.info("No %s nor Telegram in the environment: the %s is not sent\n%s", cfg["url_env"], what, message)
        return "sin url"
    taken, errors = [], []
    if url:
        try:
            post(url, message, cfg["campo"], float(cfg["timeout_s"]))
            taken.append("webhook")
        except Exception as error:  # noqa: BLE001
            errors.append(type(error).__name__)
            logger.error("The webhook did not take the %s: %s", what, type(error).__name__)
    if telegram:
        try:
            post_telegram(telegram[0], telegram[1], message, float(cfg["timeout_s"]))
            taken.append("Telegram")
        except Exception as error:  # noqa: BLE001
            errors.append(type(error).__name__)
            logger.error("Telegram did not take the %s: %s", what, type(error).__name__)
    if not taken:
        return f"error: {errors[0]}"
    logger.info("The %s went to %s\n%s", what, " and ".join(taken), message)
    if what != "test message":
        remember(message, what)
    return "enviado"


def remember(message: str, what: str) -> None:
    """Keeps the last message sent (ultimo.json in STATE_DIR); a folder that cannot be written is only a warning."""
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        write_state(os.path.join(STATE_DIR, "ultimo.json"),
                    {"texto": message, "que": what, "cuando": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    except Exception as error:  # noqa: BLE001
        logger.warning("Last message not kept in %s: %s", STATE_DIR, type(error).__name__)


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
    if not message and cfg.get("resumen_diario"):
        message = summary_message(payload, cfg["nivel_minimo"], ensemble)
    if not message:
        return "sin alerta"
    return deliver(message, cfg, "message of the evening")


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
    lines.append(f"Antes del 17 % de las averías hubo rayos a menos de 10 km de la red en la hora previa (1,1 % en las ventanas sin incidencias). Cuadro de mando: {DASHBOARD_URL}")
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
    result = deliver(message, cfg, "storm message")
    if result != "enviado":
        return result
    for d in due:
        state[str(d.get("id"))] = now.isoformat(timespec="seconds")
    if state_path:
        try:
            write_state(state_path, state)
        except OSError as error:
            logger.warning("Storm state not written in %s: %s", state_path, error)
    return "enviado"


def day_text(iso: str) -> str:
    """'2026-10-15' -> '15.10.2026'; anything else as it comes."""
    try:
        return datetime.strptime(str(iso)[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return str(iso or "")


def morning_message(verification: dict, risk: dict = None, today_iso: str = None) -> str:
    """
    The text of the morning, grouped by network: for each distribuidora, the level of today with what stands
    out (riesgo.json when its fecha is today, else the block hoy of the verification), its window and the guard,
    and under it what we said for yesterday against what the stations measured (verificacion.json, block ayer).
    With today_iso, a warning when riesgo.json is older than today (the evening run did not leave the risk).
    Empty when there is nothing of yesterday nor of today.
    """
    yesterday = (verification or {}).get("ayer") or {}
    today = (verification or {}).get("hoy") or {}
    lines = []
    if today_iso and (risk or {}).get("fecha") and str(risk["fecha"]) < today_iso:
        lines.append(f"Atención: no hay riesgo calculado para hoy (el último es del {day_text(risk['fecha'])}); "
                     f"mirar dag_riesgo_manana en Airflow ({AIRFLOW_URL}).")
    source = risk if (risk or {}).get("fecha") and risk.get("fecha") == today.get("fecha", risk.get("fecha")) else None
    today_nets = (source or {}).get("distribuidoras") or today.get("distribuidoras") or []
    today_date = (source or {}).get("fecha") or today.get("fecha")
    past = {str(d.get("nombre")): d for d in yesterday.get("distribuidoras") or [] if d.get("racha_observada_max") is not None}
    now_by = {str(d.get("nombre")): d for d in today_nets}
    names = sorted(set(past) | set(now_by), key=lambda s: [int(c) if c.isdigit() else c for c in re.split(r"(\d+)", s)])
    if not names and not lines:
        return ""
    if today_date or yesterday.get("fecha"):
        lines.append("Hoy " + day_text(today_date) + (f" y lo que pasó ayer, {day_text(yesterday.get('fecha'))}" if past else "") + ":"
                     if today_date else f"Ayer {day_text(yesterday.get('fecha'))}:")
    for name in names:
        d = now_by.get(name)
        if d:
            head = f"- {name}: {str(d.get('nivel', '')).upper()} ({100 * float(d.get('probabilidad') or 0):.0f} %)" + (" " + icons_of(d) if icons_of(d) else "")
            if (d.get("guardia") or {}).get("reforzar"):
                head += f"; guardia reforzada ({d['guardia'].get('motivo', '')})"
            lines.append(head)
            fr, parts = d.get("franja") or {}, []
            if fr.get("inicio") is not None:
                parts.append(f"franja de {int(fr['inicio'])}:00 a {int(fr['fin']) + 1}:00, pico {float(fr.get('racha_pico') or 0):.0f} km/h")
            if (d.get("aemet") or {}).get("pico"):
                parts.append(f"AEMET {float(d['aemet']['pico'].get('racha_kmh') or 0):.0f} km/h")
            if parts:
                lines.append("  Hoy: " + ", ".join(parts) + ".")
        else:
            lines.append(f"- {name}")
        y = past.get(name)
        if y:
            text = (f"  Ayer: {str(y.get('nivel', '')).upper()} ({100 * float(y.get('probabilidad') or 0):.0f} %), "
                    f"pico previsto {float(y.get('racha_prevista_max') or 0):.0f} km/h a las {y.get('hora_prevista')} h, "
                    f"medido {float(y['racha_observada_max']):.0f} km/h a las {y.get('hora_observada')} h")
            marks = []
            if y.get("pico_en_franja") is not None:
                marks.append("en la franja" if y["pico_en_franja"] else "fuera de la franja")
            if y.get("pico_en_banda") is not None:
                marks.append("en la banda de los escenarios" if y["pico_en_banda"] else "fuera de la banda de los escenarios")
            if y.get("racha_aemet_max") is not None:
                marks.append(f"AEMET decía {float(y['racha_aemet_max']):.0f} km/h")
            if y.get("aviso_aemet"):
                marks.append(f"aviso de AEMET de nivel {int(y['aviso_aemet'])}")
            lines.append(text + (" (" + ", ".join(marks) + ")" if marks else "") + ".")
    return "GridPredic, parte de la mañana.\n" + "\n".join(lines) + f"\nCuadro de mando: {DASHBOARD_URL}"


def send_morning(folder: str, rcfg: dict, state_path: str, now: datetime = None) -> str:
    """
    The morning report to the webhook once a day, in the run of
    aviso.parte_manana_hora (local hour of the risk configuration) or the
    first run after it; state_path remembers the last day sent. Returns
    "enviado", "no es la hora", "ya enviado", "sin datos", "sin url" or
    "error: ...".
    """
    cfg = alert_settings(rcfg)
    zone = ZoneInfo(rcfg.get("zona_horaria", "Europe/Madrid"))
    now = (now or datetime.now(timezone.utc)).astimezone(zone)
    if now.hour < int(cfg.get("parte_manana_hora", 7)):
        return "no es la hora"
    state = {}
    if state_path and os.path.exists(state_path):
        try:
            with open(state_path, encoding="utf-8") as file:
                state = json.load(file)
        except (OSError, ValueError):
            state = {}
    today = now.strftime("%Y-%m-%d")
    if state.get("enviado") == today:
        return "ya enviado"
    verification, risk = {}, {}
    for name, target in (("verificacion.json", "verification"), ("riesgo.json", "risk")):
        path = os.path.join(folder, name)
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as file:
                    if target == "verification":
                        verification = json.load(file)
                    else:
                        risk = json.load(file)
            except (OSError, ValueError):
                pass
    message = morning_message(verification, risk, today)
    if not message:
        return "sin datos"
    result = deliver(message, cfg, "morning report")
    if result != "enviado":
        return result
    if state_path:
        try:
            write_state(state_path, {"enviado": today})
        except OSError as error:
            logger.warning("Morning state not written in %s: %s", state_path, error)
    return "enviado"


def run_time_text(when) -> str:
    """The logical date of a run as people read it: '2026-10-14T18:30:00+00:00' -> '14.10.2026 20:30' (Madrid).
    A run_id or anything that is not a date comes back as it is."""
    try:
        moment = when if isinstance(when, datetime) else datetime.fromisoformat(str(when).replace("Z", "+00:00"))
    except ValueError:
        return str(when)[:40]
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(ZoneInfo("Europe/Madrid")).strftime("%d.%m.%Y %H:%M")


def failure_message(context: dict) -> str:
    """What a task failure of Airflow looks like in the webhook: DAG, task, attempt, day and the error."""
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    attempt = getattr(ti, "try_number", None)
    when = context.get("logical_date") or context.get("execution_date") or context.get("run_id") or ""
    error = str(context.get("exception") or "")[:300]
    return (f"GridPredic: la tarea {task_id} de {dag_id} ha fallado" + (f" (intento {attempt})" if attempt else "")
            + (f", ejecución del {run_time_text(when)}" if when else "") + (f": {error}" if error else "") + f". Airflow: {AIRFLOW_URL}")


def notify_failure(context: dict) -> str:
    """on_failure_callback of the DAGs: the failure goes to the same channels as the message of the evening; silent without them."""
    message = failure_message(context)
    if not os.environ.get(DEFAULTS["url_env"], "").strip() and not telegram_settings():
        logger.warning("Task failed and no %s nor Telegram to tell it: %s", DEFAULTS["url_env"], message)
        return "sin url"
    return deliver(message, DEFAULTS, "failure")


def telegram_chats(timeout: float = 30.0) -> list:
    """The chats that wrote to the bot or added it lately (getUpdates), as (chat_id, type, title): the way to find
    TELEGRAM_CHAT_ID after adding the bot to the group and writing something there."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("No TELEGRAM_BOT_TOKEN in the environment")
    with urllib.request.urlopen(f"{TELEGRAM_API}/bot{token}/getUpdates", timeout=timeout) as answer:
        updates = json.load(answer).get("result", [])
    seen = {}
    for update in updates:
        for key in ("message", "my_chat_member", "channel_post", "edited_message"):
            chat = (update.get(key) or {}).get("chat")
            if chat:
                seen[chat["id"]] = (chat["id"], chat.get("type", ""), chat.get("title") or chat.get("username") or chat.get("first_name") or "")
    return list(seen.values())


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if argv and argv[0] == "--telegram-chats":
        chats = telegram_chats()
        for chat_id, kind, title in chats:
            print(f"{chat_id}\t{kind}\t{title}")
        if not chats:
            print("El bot no ha visto ningún chat: añádelo al grupo, escribe algo en el grupo y vuelve a probar.")
        return 0
    if argv and argv[0] == "--prueba":
        result = deliver("GridPredic: mensaje de prueba de los avisos. Si lo lees aquí, este canal recibirá la tarde, "
                         f"el parte de la mañana, los avisos nuevos, los rayos y los fallos. Cuadro de mando: {DASHBOARD_URL}",
                         DEFAULTS, "test message")
        print(result)
        return 0 if result == "enviado" else 1
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
