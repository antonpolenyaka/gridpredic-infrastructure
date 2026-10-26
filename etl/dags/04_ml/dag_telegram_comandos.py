"""
The commands of the Telegram group, every minute.

One task, atender, that reads the new messages of the bot (bot_telegram.poll) and answers the commands
/forecast, /morning, /lightning, /warnings, /last and /help, typed in the group or pressed as buttons in a private
chat with the bot, with the message built at that moment from the files of the dashboard, so a message can be
seen or shown without waiting for its hour. Only the group of TELEGRAM_CHAT_ID and, in private, its members are
answered; a command older than 15 minutes (the DAG paused, the laptop off) is read but not answered. Python in
the airflow container, without Spark: it does not compete with the evening. The schedule is aviso.comandos_horario
of config_ml.json. Without TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID the run is skipped, and so is a run in which
Telegram does not answer (without network): a failure is never told to the group, which would get one message a
minute; the next run tries again.
"""

import json
import sys
from datetime import timedelta

from airflow.sdk import DAG, CronTriggerTimetable, task
from airflow.sdk.exceptions import AirflowSkipException

CONFIG_PATH = "/opt/airflow/config/04_ml/config_ml.json"
JOBS_DIR = "/app/jobs/04_ml"

with open(CONFIG_PATH) as file:
    settings = json.load(file)["riesgo_manana"]

ZONE = settings.get("zona_horaria", "Europe/Madrid")


@task(retries=0)
def atender() -> None:
    if JOBS_DIR not in sys.path:
        sys.path.insert(0, JOBS_DIR)
    import aviso_riesgo
    import bot_telegram

    if not aviso_riesgo.telegram_settings():
        raise AirflowSkipException("No TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in the airflow container")
    try:
        done = bot_telegram.poll(settings.get("dashboard_dir", "/app/dashboard/data"), settings)
    except OSError as error:
        # without network (the laptop away from the Internet) a red run every minute says nothing new: the next
        # run tries again, and the type of the error is enough (never the message, which could carry the URL)
        raise AirflowSkipException(f"Telegram did not answer: {type(error).__name__}") from None
    if not done:
        raise AirflowSkipException("No new commands")
    print(done)


with DAG(
    dag_id="dag_telegram_comandos",
    description="Órdenes del bot de Telegram (/forecast, /morning, /lightning, /warnings, /last, /help), en el grupo o con botones en privado",
    schedule=CronTriggerTimetable(settings.get("aviso", {}).get("comandos_horario", "* * * * *"), timezone=ZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    dagrun_timeout=timedelta(minutes=2),
    tags=["riesgo", "telegram"],
) as dag:
    atender()
