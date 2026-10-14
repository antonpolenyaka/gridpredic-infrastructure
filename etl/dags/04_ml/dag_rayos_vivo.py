"""
Lightning over the network, live, every riesgo_manana.rayos.horario (15 minutes).

One task, rayos, that runs job_landing_rayos in the airflow container: the
products of the Lightning Imager of MTG of the last hour and a half that
are not yet in the Landing, the flashes inside the study box to
<landing>/rayos/<day>/, and rayos.json for the dashboard (the flashes of
the last hour and how many fell within 10 and 20 km of the network of every
distribuidora, with the lines they touch, from the riesgo.json of
dag_riesgo_manana).

It needs EUMETSAT_CONSUMER_KEY and EUMETSAT_CONSUMER_SECRET in the
environment of the airflow container (compose.yaml takes them from .env);
without them the run is skipped. When the Data Store does not answer the
JSON is rebuilt from what is in the Landing and the task fails, so the
next run tries again. After it, avisar_rayos sends a storm over some
network to the webhook of AVISO_WEBHOOK_URL (aviso_riesgo.send_storm), once
per distribuidora and aviso.rayos_cada_min minutes; skipped without storm,
URL or when already told.
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

with open(CONFIG_PATH) as file:
    settings = json.load(file)["riesgo_manana"]

ZONE = settings.get("zona_horaria", "Europe/Madrid")
LIGHTNING = settings.get("rayos", {})


@task(retries=0)
def rayos() -> None:
    key = LIGHTNING.get("clave_env", "EUMETSAT_CONSUMER_KEY")
    secret = LIGHTNING.get("secreto_env", "EUMETSAT_CONSUMER_SECRET")
    if not os.environ.get(key) or not os.environ.get(secret):
        raise AirflowSkipException(f"No {key} and {secret} in the airflow container: no lightning")
    if LANDING_DIR not in sys.path:
        sys.path.insert(0, LANDING_DIR)
    import job_landing_rayos

    code = job_landing_rayos.main(["--config", CONFIG_PATH])
    if code != 0:
        raise RuntimeError(f"job_landing_rayos ended with code {code}")


@task(retries=0)
def avisar_rayos() -> None:
    """A storm over some network goes to the webhook of the evening, once per distribuidora and hour."""
    if JOBS_DIR not in sys.path:
        sys.path.insert(0, JOBS_DIR)
    import aviso_riesgo

    folder = settings.get("dashboard_dir", "/app/dashboard/data")
    path = os.path.join(folder, "rayos.json")
    if not os.path.exists(path):
        raise AirflowSkipException(f"No {path}: nothing to tell")
    with open(path, encoding="utf-8") as file:
        payload = json.load(file)
    result = aviso_riesgo.send_storm(payload, settings, os.path.join(folder, "rayos_aviso.json"))
    if result != "enviado":
        raise AirflowSkipException(f"Storm message not sent: {result}")


with DAG(
    dag_id="dag_rayos_vivo",
    description="Rayos sobre la red en vivo (Lightning Imager de MTG, EUMETSAT) para el cuadro de mando",
    schedule=CronTriggerTimetable(LIGHTNING.get("horario", "*/15 * * * *"), timezone=ZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=2)},
    dagrun_timeout=timedelta(minutes=14),
    tags=["landing", "riesgo"],
) as dag:
    rayos() >> avisar_rayos()
