"""
Risk of tomorrow, every evening at riesgo_manana.horario (20:30 in Madrid).

- dia: the day to predict, the param fecha (YYYY-MM-DD) or the day after the
  evening the run belongs to (run_after in riesgo_manana.zona_horaria), so a
  run that starts late still predicts the day after its evening.
- prevision and avisos: the forecast of ICON-EU of every cell and the
  warnings of AEMET in force go to the live Landing (job_landing_vivo). The
  forecast is needed: without it the run fails. Without an answer of AEMET
  after three tries avisos is skipped and the day is scored without warnings
  (the row says so).
- puntuar: job_ml_riesgo_manana fits the model on every labelled day and
  writes the risk of every distribuidora in l3_gold.riesgo_dia_siguiente.

The warnings need AEMET_API_KEY in the environment of the airflow container
(compose.yaml takes it from .env).
"""

import json
import os
import sys
import time
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, CronTriggerTimetable, Param, TriggerRule, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

CONFIG_PATH = "/opt/airflow/config/04_ml/config_ml.json"
LANDING_DIR = "/app/jobs/00_landing"
JOBS_DIR = "/app/jobs/04_ml"

# The executors unpickle nothing of these jobs, but the models module goes
# with them as in the other jobs of 04_ml.
PY_FILES = f"{JOBS_DIR}/ml_models.py,{JOBS_DIR}/ml_metrics.py"


with open(CONFIG_PATH) as file:
    settings = json.load(file)["riesgo_manana"]

ZONE = settings.get("zona_horaria", "Europe/Madrid")


def land(source: str, day: str) -> int:
    if LANDING_DIR not in sys.path:
        sys.path.insert(0, LANDING_DIR)
    import job_landing_vivo

    return job_landing_vivo.main(["--fuente", source, "--fecha", day, "--config", CONFIG_PATH])


@task
def dia() -> str:
    context = get_current_context()
    chosen = str(context["params"].get("fecha") or "").strip()
    if chosen:
        return date.fromisoformat(chosen).isoformat()
    evening = context["dag_run"].run_after.astimezone(ZoneInfo(ZONE))
    return (evening.date() + timedelta(days=1)).isoformat()


@task(retries=2, retry_delay=timedelta(minutes=10))
def prevision(day: str) -> None:
    if land("prevision", day) != 0:
        raise RuntimeError(f"The forecast of ICON-EU for {day} did not land")


@task(retries=0)
def avisos(day: str) -> None:
    variable = settings.get("avisos", {}).get("clave_env", "AEMET_API_KEY")
    if not os.environ.get(variable):
        raise AirflowSkipException(f"No {variable} in the airflow container: scored without warnings")
    for attempt in range(3):
        if land("avisos", day) == 0:
            return
        if attempt < 2:
            time.sleep(60 * (attempt + 1))
    raise AirflowSkipException("AEMET did not answer after three tries: scored without warnings")


with DAG(
    dag_id="dag_riesgo_manana",
    description="Riesgo de interrupción de mañana por distribuidora (previsión de ICON-EU y avisos de AEMET)",
    schedule=CronTriggerTimetable(settings.get("horario", "30 20 * * *"), timezone=ZONE),
    catchup=False,
    max_active_runs=1,
    params={"fecha": Param("", type="string", description="Día que se predice, AAAA-MM-DD (vacío: el día siguiente)")},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ml", "riesgo"],
) as dag:

    day = dia()

    puntuar = SparkSubmitOperator(
        task_id="puntuar",
        conn_id="spark_default",
        application=f"{JOBS_DIR}/job_ml_riesgo_manana.py",
        application_args=[
            "--run-id", "{{ run_id }}",
            "--config", CONFIG_PATH,
            "--fecha", "{{ ti.xcom_pull(task_ids='dia') }}",
        ],
        py_files=PY_FILES,
        driver_memory="4g",
        name="job-ml-riesgo-manana",
        # It scores without warnings when avisos is skipped, never without the forecast.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [prevision(day), avisos(day)] >> puntuar
