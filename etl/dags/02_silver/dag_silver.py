import json
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

CONFIG_PATH = "/opt/airflow/config/02_silver/config_silver.json"
JOBS_DIR = "/app/jobs/02_silver"

# Modules imported by the jobs. The driver finds them because spark-submit
# adds the folder of the application to sys.path; py_files also ships them
# to the executors.
PY_FILES = ",".join([
    f"{JOBS_DIR}/silver_common.py",
    f"{JOBS_DIR}/job_silver_f_tag_value_change.py",
])


with open(CONFIG_PATH) as file:
    config = json.load(file)


with DAG(
    dag_id="dag_silver",
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
        application_args = ["--run-id", "{{ run_id }}"] + job.get("args", [])

        if job["name"] == "dq_checks" and config.get("fail_on_review"):
            application_args.append("--fail-on-review")

        tasks[job["name"]] = SparkSubmitOperator(
            task_id=f"silver_{job['name']}",
            conn_id="spark_default",
            application=f"{JOBS_DIR}/job_silver_{job['name']}.py",
            application_args=application_args,
            py_files=PY_FILES,
            conf=job.get("conf", config["default_conf"]),
        )

    for job in config["jobs"]:
        for upstream in job.get("depends_on", []):
            tasks[upstream] >> tasks[job["name"]]
