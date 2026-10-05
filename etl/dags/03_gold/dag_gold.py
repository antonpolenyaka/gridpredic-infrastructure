import json
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

CONFIG_PATH = "/opt/airflow/config/03_gold/config_gold.json"
JOBS_DIR = "/app/jobs/03_gold"

# Module imported by the jobs. The driver finds it because spark-submit adds
# the folder of the application to sys.path; py_files also ships it to the
# executors.
PY_FILES = f"{JOBS_DIR}/gold_common.py"


with open(CONFIG_PATH) as file:
    config = json.load(file)


with DAG(
    dag_id="dag_gold",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    tasks = {}

    for job in config["jobs"]:
        # The jobs read their parameters from the same file the DAG reads.
        application_args = [
            "--run-id", "{{ run_id }}",
            "--config", CONFIG_PATH,
        ] + job.get("args", [])

        if job["name"] == "dq_checks" and config.get("fail_on_review"):
            application_args.append("--fail-on-review")

        tasks[job["name"]] = SparkSubmitOperator(
            task_id=f"gold_{job['name']}",
            conn_id="spark_default",
            application=f"{JOBS_DIR}/job_gold_{job['name']}.py",
            application_args=application_args,
            py_files=PY_FILES,
            # A job only states the settings it changes; the rest keep the
            # defaults of the configuration.
            conf={**config["default_conf"], **job.get("conf", {})},
        )

    for job in config["jobs"]:
        for upstream in job.get("depends_on", []):
            tasks[upstream] >> tasks[job["name"]]
