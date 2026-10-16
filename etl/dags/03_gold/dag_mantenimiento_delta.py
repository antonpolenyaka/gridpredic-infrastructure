"""
OPTIMIZE and VACUUM of every Delta table, on Sunday at 04:10 (far from the
evening run at 20:30 and from the hourly ones at minute 20).
"""

from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, CronTriggerTimetable

JOBS_DIR = "/app/jobs/03_gold"

with DAG(
    dag_id="dag_mantenimiento_delta",
    description="OPTIMIZE y VACUUM semanal de las tablas Delta",
    schedule=CronTriggerTimetable("10 4 * * 0", timezone="Europe/Madrid"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=10)},
    tags=["mantenimiento"],
) as dag:
    SparkSubmitOperator(
        task_id="optimize_vacuum",
        conn_id="spark_default",
        application=f"{JOBS_DIR}/job_mantenimiento_delta.py",
        driver_memory="2g",
        name="job-mantenimiento-delta",
    )
