import json
import re
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

CONFIG_PATH = "/opt/airflow/config/01_bronze/config_bronze_sqlserver.json"
APPLICATION = "/app/jobs/01_bronze/job_bronze_sqlserver_batch.py"

DEFAULT_CONF = {
    "spark.cores.max": "2",
    "spark.executor.memory": "2g",
}

# Optional keys of a table in the config and the argument they become.
OPTIONAL_ARGS = {
    "watermark_column": "--watermark-column",
    "primary_key": "--primary-key",
    "partition_column": "--partition-column",
    "num_partitions": "--num-partitions",
    "chunk_size": "--chunk-size",
    "max_records_per_file": "--max-records-per-file",
    "fetch_size": "--fetch-size",
}


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

                for key, option in OPTIONAL_ARGS.items():
                    if table.get(key) is not None:
                        application_args += [option, str(table[key])]

                SparkSubmitOperator(
                    task_id=to_task_id(database, table["name"]),
                    conn_id="spark_default",
                    application=APPLICATION,
                    application_args=application_args,
                    conf=table.get("conf", DEFAULT_CONF),
                )
