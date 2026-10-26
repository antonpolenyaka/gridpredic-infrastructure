"""
The commands of the Telegram group, every minute.

One task, atender, that reads the new messages of the bot (bot_telegram.poll) and answers in the group the
commands /tarde, /manana, /rayos, /avisos, /ultimo and /ayuda with the message built at that moment from the
files of the dashboard, so a message can be seen or shown without waiting for its hour. Only the chat of
TELEGRAM_CHAT_ID is answered. Python in the airflow container, without Spark: it does not compete with the
evening. Without TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID the run is skipped. A failed run is not told to the
group (it would be one message a minute): the next run tries again.
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
    done = bot_telegram.poll(settings.get("dashboard_dir", "/app/dashboard/data"), settings)
    if not done:
        raise AirflowSkipException("No new commands")
    print(done)


with DAG(
    dag_id="dag_telegram_comandos",
    description="Órdenes del grupo de Telegram (/tarde, /manana, /rayos, /avisos, /ultimo, /ayuda)",
    schedule=CronTriggerTimetable("* * * * *", timezone=ZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    dagrun_timeout=timedelta(minutes=2),
    tags=["riesgo", "telegram"],
) as dag:
    atender()
