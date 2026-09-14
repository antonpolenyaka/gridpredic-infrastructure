import json
import re
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

CONFIG_PATH = "/opt/airflow/config/sqlserver_batch.json"
APPLICATION = "/app/jobs/01_bronze/ingest_sqlserver_batch.py"


def to_task_id(database: str, table: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", f"{database}_{table}").lower()


with open(CONFIG_PATH) as file:
    config = json.load(file)


with DAG(
    dag_id="ingest_sqlserver_batch_bronze",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    for source in config["sources"]:
        for database in source["databases"]:
            for table in source["tables"]:

                application_args = [
                    "--database", database,
                    "--table", table["name"],
                    "--extract-mode", table["extract_mode"],
                    "--write-mode", table["write_mode"],
                ]

                if table.get("watermark_column"):
                    application_args += [
                        "--watermark-column",
                        table["watermark_column"],
                    ]

                if table.get("primary_key"):
                    application_args += [
                        "--primary-key",
                        table["primary_key"],
                    ]

                SparkSubmitOperator(
                    task_id=to_task_id(database, table["name"]),
                    conn_id="spark_default",
                    application=APPLICATION,
                    application_args=application_args,
                    conf={
                        "spark.cores.max": "2",
                        "spark.executor.memory": "2g",
                    },
                )
