import os

import boto3
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task

SOURCE_FILE = "/app/data/reference_data/municipios.xlsx"
BUCKET = "datalake"
OBJECT_KEY = "00_landing/reference_data/municipios.xlsx"
APPLICATION = "/app/jobs/01_bronze/job_bronze_reference_municipios.py"


@task
def upload_file(file_name: str, bucket: str, object_name: str) -> None:
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3_client.upload_file(file_name, bucket, object_name)


with DAG(
    dag_id="dag_bronze_reference_municipios",
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    upload_task = upload_file(SOURCE_FILE, BUCKET, OBJECT_KEY)

    spark_task = SparkSubmitOperator(
        task_id="reference_municipios",
        conn_id="spark_default",
        application=APPLICATION,
    )

    upload_task >> spark_task
