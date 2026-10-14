"""
What the stations of AEMET are measuring, every hour (riesgo_manana.observacion.horario).

One task, observar, that runs job_landing_observacion in the airflow
container: the last 24 hours of the conventional stations of AEMET inside
the study box to the Landing (<landing>/observacion/<day>/) and
observado.json for the dashboard, with the strongest gust and the rain of
every hour of the day near the network of every distribuidora (what the
forecast said against what is happening) and the stations for the map.

It needs AEMET_API_KEY in the environment of the airflow container; without
it the run is skipped.
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
    observar()
