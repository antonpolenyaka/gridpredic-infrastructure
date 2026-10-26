"""
The commands of the Telegram bot: whoever is in the group of TELEGRAM_CHAT_ID asks the bot for a message and the
bot answers with the same text the operation sends, built at that moment from the files of the dashboard. It is
the way to see a message without waiting for its hour (the evening at 20:30, the morning at 07:20) and to show it.

    /forecast   the risk of tomorrow as the evening sends it (with the detail of each network in alert, or the summary)
    /morning    the morning report: yesterday against what the stations measured, and today
    /lightning  the lightning of the last hour by network
    /warnings   the warnings of AEMET of today and the guard
    /last       the last message the system sent, again (or the evening, if it has sent none yet)
    /help       this list

In the group the commands are typed. In a private chat with the bot there is also a menu of buttons (a reply
keyboard) with the same messages, so nothing has to be typed; the bot answers there only to the members of the
group (getChatMember), and to anyone else it says so. A command from any other group is ignored.
dag_telegram_comandos reads the new messages every minute (getUpdates, with the last update read kept in
STATE_DIR), so the answer takes up to a minute. Without TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID it does nothing.

    python bot_telegram.py                    # reads the new commands once and answers them
    python bot_telegram.py --enviar forecast  # sends that message to the group now, without a command
    python bot_telegram.py --registrar        # registers the list of commands, so Telegram suggests them after "/"
"""

import json
import logging
import os
import sys
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aviso_riesgo as av  # noqa: E402

logger = logging.getLogger("bot_telegram")

COMMANDS = {
    "forecast": "el riesgo de mañana, como sale a las 20:30",
    "morning": "el parte de la mañana",
    "lightning": "los rayos de la última hora",
    "warnings": "los avisos de AEMET de hoy",
    "last": "repite el último aviso enviado",
    "help": "qué se le puede pedir al bot",
}
# The buttons of the private chat: what each one says, with its icon, and the command it stands for.
BUTTONS = {
    "📈 Riesgo de mañana": "forecast",
    "☀️ Parte de la mañana": "morning",
    "⚡ Rayos": "lightning",
    "⚠️ Avisos de AEMET": "warnings",
    "🔁 Último aviso": "last",
    "❓ Ayuda": "help",
}
_LABELS = list(BUTTONS)
KEYBOARD = {"keyboard": [_LABELS[0:2], _LABELS[2:4], _LABELS[4:6]], "resize_keyboard": True, "is_persistent": True}
MEMBER_STATES = ("creator", "administrator", "member", "restricted")
CONFIG_PATH = "/opt/airflow/config/04_ml/config_ml.json"


def read_json(path: str) -> dict:
    """The JSON of a file, or {} when it is not there or cannot be read."""
    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return {}


def evening_text(folder: str, rcfg: dict) -> str:
    risk = read_json(os.path.join(folder, "riesgo.json"))
    if not risk:
        return "Todavía no hay riesgo calculado (falta riesgo.json): lo deja la tarde de las 20:30."
    cfg = av.alert_settings(rcfg)
    return (av.message_from(risk, cfg["nivel_minimo"], av.read_ensemble(folder))
            or av.summary_message(risk, cfg["nivel_minimo"]))


def morning_text(folder: str, rcfg: dict, now: datetime) -> str:
    zone = ZoneInfo(rcfg.get("zona_horaria", "Europe/Madrid"))
    today = now.astimezone(zone).strftime("%Y-%m-%d")
    text = av.morning_message(read_json(os.path.join(folder, "verificacion.json")), read_json(os.path.join(folder, "riesgo.json")), today)
    return text or "Todavía no hay parte de la mañana: falta la verificación de ayer y el riesgo de hoy."


def lightning_text(folder: str) -> str:
    payload = read_json(os.path.join(folder, "rayos.json"))
    if not payload:
        return "No hay datos de rayos (falta rayos.json: el DAG de los rayos no ha corrido o no tiene la clave de EUMETSAT)."
    storm = av.storm_message(payload)
    if storm:
        return storm
    when = av.run_time_text(payload.get("generado")) if payload.get("generado") else ""
    lines = [f"Rayos (satélite MTG{', ' + when if when else ''}): ninguna tormenta sobre la red."]
    lines += [f"- {d.get('nombre')}: {d.get('texto', d.get('nivel', ''))}." for d in payload.get("distribuidoras", [])]
    return "\n".join(lines)


def warnings_text(folder: str) -> str:
    payload = read_json(os.path.join(folder, "avisos_hoy.json"))
    if not payload:
        return "Todavía no se han releído los avisos de AEMET de hoy (falta avisos_hoy.json: se piden cada hora, a y 20)."
    lines = [f"Avisos de AEMET para hoy {av.day_text(payload.get('fecha'))}:"]
    lines += [f"- {d.get('texto')}." for d in payload.get("distribuidoras", [])]
    return "\n".join(lines)


def last_text(folder: str, rcfg: dict) -> str:
    last = read_json(os.path.join(av.STATE_DIR, "ultimo.json"))
    if last.get("texto"):
        return f"Último aviso enviado ({av.run_time_text(last.get('cuando'))}):\n{last['texto']}"
    return "Todavía no se ha enviado ningún aviso; así sería el de la tarde:\n" + evening_text(folder, rcfg)


def help_text() -> str:
    return ("GridPredic contesta a estas órdenes, en el grupo o en privado (tarda hasta un minuto):\n"
            + "\n".join(f"/{k} - {v}" for k, v in COMMANDS.items())
            + "\nEn privado también están los botones de abajo.")


def answer(command: str, folder: str, rcfg: dict, now: datetime = None) -> str:
    """The text for a command (without the slash), or "" when it is not one of COMMANDS."""
    now = now or datetime.now(timezone.utc)
    if command == "forecast":
        return evening_text(folder, rcfg)
    if command == "morning":
        return morning_text(folder, rcfg, now)
    if command == "lightning":
        return lightning_text(folder)
    if command == "warnings":
        return warnings_text(folder)
    if command == "last":
        return last_text(folder, rcfg)
    if command in ("help", "start"):
        return help_text()
    return ""


def command_of(text: str) -> str:
    """'/forecast@gridpredic_avisos_bot hoy' -> 'forecast', a button -> its command; '' for anything else."""
    text = (text or "").strip()
    if text in BUTTONS:
        return BUTTONS[text]
    # the same button without its icon (an older keyboard, or typed by hand)
    plain = {label.split(" ", 1)[1]: command for label, command in BUTTONS.items()}
    if text in plain:
        return plain[text]
    if not text.startswith("/"):
        return ""
    return text.split()[0][1:].split("@")[0].lower()


def is_member(token: str, group: str, user_id, timeout: float = 30.0) -> bool:
    """Whether the user is in the group (getChatMember); False when Telegram does not say."""
    try:
        url = f"{av.TELEGRAM_API}/bot{token}/getChatMember?chat_id={group}&user_id={int(user_id)}"
        with urllib.request.urlopen(url, timeout=timeout) as response:
            status = (json.load(response).get("result") or {}).get("status")
    except Exception as error:  # noqa: BLE001
        logger.warning("Telegram did not say whether %s is in the group: %s", user_id, type(error).__name__)
        return False
    return status in MEMBER_STATES


def get_updates(token: str, offset: int = None, timeout: float = 30.0) -> list:
    """The messages the bot has not read yet (getUpdates). The token is part of the URL: never logged."""
    url = f"{av.TELEGRAM_API}/bot{token}/getUpdates?timeout=0" + (f"&offset={offset}" if offset else "")
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response).get("result", [])


def poll(folder: str, rcfg: dict, now: datetime = None) -> list:
    """
    Reads the new messages once, answers the commands of the group and keeps the last update read
    (comandos.json in STATE_DIR). Returns [(command, result)]; [] when there was nothing new or no Telegram.
    """
    telegram = av.telegram_settings()
    if not telegram:
        return []
    token, chat = telegram
    state_path = os.path.join(av.STATE_DIR, "comandos.json")
    state = read_json(state_path)
    updates = get_updates(token, state.get("offset"))
    done = []
    for update in updates:
        state["offset"] = int(update["update_id"]) + 1
        message = update.get("message") or update.get("channel_post") or {}
        where = message.get("chat") or {}
        private = where.get("type") == "private"
        if not private and str(where.get("id")) != str(chat):
            continue
        command = command_of(message.get("text"))
        if not command:
            continue
        if private and not is_member(token, chat, (message.get("from") or {}).get("id")):
            text, markup = "Este bot solo contesta a los miembros del grupo de la guardia de GridPredic.", None
        else:
            text = answer(command, folder, rcfg, now)
            markup = KEYBOARD if private else None
        if not text:
            continue
        try:
            av.post_telegram(token, str(where.get("id")), text, 30.0, message.get("message_thread_id"), markup)
            done.append((command, "enviado"))
        except Exception as error:  # noqa: BLE001
            logger.error("Telegram did not take the answer to /%s: %s", command, type(error).__name__)
            done.append((command, f"error: {type(error).__name__}"))
    if updates:
        try:
            os.makedirs(av.STATE_DIR, exist_ok=True)
            av.write_state(state_path, state)
        except OSError as error:
            logger.warning("Offset of the commands not kept in %s: %s", av.STATE_DIR, type(error).__name__)
    return done


def register(token: str, timeout: float = 30.0) -> int:
    """setMyCommands: the list Telegram shows after typing "/" in the group."""
    body = json.dumps({"commands": [{"command": k, "description": v} for k, v in COMMANDS.items()]}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(f"{av.TELEGRAM_API}/bot{token}/setMyCommands", data=body,
                                     headers={"Content-Type": "application/json", "User-Agent": "GridPredic-TFM/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return int(response.status)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rcfg = read_json(CONFIG_PATH).get("riesgo_manana", {})
    folder = rcfg.get("dashboard_dir", "/app/dashboard/data")
    telegram = av.telegram_settings()
    if not telegram:
        print("Sin TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID no hay nada que hacer.")
        return 0
    if argv and argv[0] == "--registrar":
        print(register(telegram[0]))
        return 0
    if len(argv) > 1 and argv[0] == "--enviar":
        text = answer(argv[1], folder, rcfg)
        if not text:
            print(f"Orden desconocida: {argv[1]} (" + ", ".join(COMMANDS) + ")")
            return 1
        av.post_telegram(telegram[0], telegram[1], text)
        print(text)
        return 0
    print(poll(folder, rcfg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
