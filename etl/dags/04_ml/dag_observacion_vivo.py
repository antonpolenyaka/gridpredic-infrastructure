"""
What the stations of AEMET are measuring, every hour (riesgo_manana.observacion.horario).

Two tasks. observar runs job_landing_observacion in the airflow
container: the last 24 hours of the conventional stations of AEMET inside
the study box to the Landing (<landing>/observacion/<day>/) and
observado.json for the dashboard, with the strongest gust and the rain of
every hour of the day near the network of every distribuidora (what the
forecast said against what is happening) and the stations for the map.

It needs AEMET_API_KEY in the environment of the airflow container; without
it the run is skipped.

parte_manana sends to the webhook, once a day from riesgo_manana.aviso
.parte_manana_hora (local), the morning report: what we said for yesterday
and what the stations measured, and what the system says for today
(aviso_riesgo.send_morning). Silent without AVISO_WEBHOOK_URL.

avisos_hoy asks AEMET every hour for the warnings in force, finds the ones
that cover the municipalities of every distribuidora today, writes
dashboard/data/avisos_hoy.json (levels of wind and thunderstorm, the
warnings that did not exist when the evening run was made, the guard
recomputed) and tells the webhook once per distribuidora and day when a new
warning raises the guard (avisos_hoy.refresh). One warning in five of a day
is sent after the evening run; two in five of the thunderstorm ones.
"""

import json
import os
import sys
from datetime import timedelta

from airflow.sdk import DAG, CronTriggerTimetable, task
from airflow.sdk.exceptions import AirflowSkipException

CONFIG_PATH = "/opt/airflow/config/04_ml/config_ml.json"
LANDING_DIR = "/app/jobs/00_landing"
JOBS_DIR = "/app/jobs/04_ml"


def failed(context: dict) -> None:
    """A run that failed for good goes to the webhook of the evening (silent without AVISO_WEBHOOK_URL)."""
    if JOBS_DIR not in sys.path:
        sys.path.insert(0, JOBS_DIR)
    import aviso_riesgo

    aviso_riesgo.notify_failure(context)

with open(CONFIG_PATH) as file:
    settings = json.load(file)["riesgo_manana"]

ZONE = settings.get("zona_horaria", "Europe/Madrid")
OBSERVATION = settings.get("observacion", {})


@task(retries=1, retry_delay=timedelta(minutes=5))
def observar() -> None:
    key = OBSERVATION.get("clave_env", "AEMET_API_KEY")
    if not os.environ.get(key):
        raise AirflowSkipException(f"No {key} in the airflow container: no observations")
    if LANDING_DIR not in sys.path:
        sys.path.insert(0, LANDING_DIR)
    import job_landing_observacion

    code = job_landing_observacion.main(["--config", CONFIG_PATH])
    if code != 0:
        raise RuntimeError(f"job_landing_observacion ended with code {code}")


@task(retries=0)
def avisos_hoy() -> None:
    key = settings.get("avisos", {}).get("clave_env", "AEMET_API_KEY")
    if not os.environ.get(key):
        raise AirflowSkipException(f"No {key} in the airflow container: no warnings of the day")
    if JOBS_DIR not in sys.path:
        sys.path.insert(0, JOBS_DIR)
    import avisos_hoy as module

    folder = settings.get("dashboard_dir", "/app/dashboard/data")
    try:
        payload = module.refresh(settings, folder, settings["landing"])
    except RuntimeError as error:
        raise AirflowSkipException(f"AEMET did not answer: {error}") from error
    print("avisos de hoy: " + ("; ".join(d["texto"] for d in payload.get("distribuidoras", [])) if payload else "riesgo.json no es de hoy")
          + f"; envío: {payload.get('envio', '-') if payload else '-'}")


@task(retries=0, trigger_rule="none_failed_min_one_success")
def parte_manana() -> None:
    if JOBS_DIR not in sys.path:
        sys.path.insert(0, JOBS_DIR)
    import aviso_riesgo

    folder = settings.get("dashboard_dir", "/app/dashboard/data")
    result = aviso_riesgo.send_morning(folder, settings, os.path.join(folder, "parte_manana.json"))
    print(f"parte de la mañana: {result}")


with DAG(
    dag_id="dag_observacion_vivo",
    description="Observaciones de las estaciones de AEMET cada hora para el cuadro de mando",
    schedule=CronTriggerTimetable(OBSERVATION.get("horario", "20 * * * *"), timezone=ZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "on_failure_callback": failed},
    dagrun_timeout=timedelta(minutes=30),
    tags=["landing", "riesgo"],
) as dag:
    observar() >> [parte_manana(), avisos_hoy()]
