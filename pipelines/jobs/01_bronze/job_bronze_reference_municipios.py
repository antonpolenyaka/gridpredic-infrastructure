import re

import pandas as pd
from pyspark.sql import SparkSession

SOURCE_PATH = "s3://datalake/00_landing/reference_data/municipios.xlsx"
TARGET_TABLE = "l1_bronze.reference_municipios"


def normalize_delta_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame column names to make them compatible with Delta Lake.
    """
    columns = {
        column: re.sub(r"[ ,;{}()\n\t=]+", "_", column).strip("_").lower()
        for column in df.columns
    }

    return df.rename(columns=columns)


spark = (
    SparkSession.builder.appName("job-bronze-reference-municipios")
    .config("spark.executor.memory", "512m")
    .config("spark.cores.max", "1")
    .getOrCreate()
)

storage_options = {"client_kwargs": {"endpoint_url": "http://minio:9000"}}
pdf = pd.read_excel(
    SOURCE_PATH,
    sheet_name="Municipios",
    dtype=str,
    storage_options=storage_options,
)

pdf = normalize_delta_columns(pdf)

df = spark.createDataFrame(pdf)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)
