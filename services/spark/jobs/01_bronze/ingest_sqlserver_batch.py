import argparse
import logging
import os
import re
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument("--database", required=True)
parser.add_argument("--table", required=True)
parser.add_argument("--extract-mode", choices=["FULL", "INCREMENTAL"], required=True)
parser.add_argument("--write-mode", choices=["OVERWRITE", "APPEND", "MERGE"], required=True)
parser.add_argument("--watermark-column")
parser.add_argument("--primary-key")
args = parser.parse_args()

if args.extract_mode == "INCREMENTAL" and not args.watermark_column:
    parser.error("--watermark-column is required for incremental extraction")

if args.write_mode == "MERGE" and not args.primary_key:
    parser.error("--primary-key is required for merge writes")


def to_snake(value: str) -> str:
    """
    Converts a PascalCase or snake_case string to snake_case.
    """
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)

    return value.lower()


def get_max_value(
    spark: SparkSession,
    table_name: str,
    watermark_col: str,
) -> str:
    """
    Returns the maximum value of a watermark column from a Delta table,
    formatted for use in a SQL expression.
    """
    max_value = (
        spark.table(table_name)
        .select(spark_max(watermark_col).alias("max_value"))
        .first()["max_value"]
    )

    if max_value is None:
        raise ValueError(
            f"Maximum value is null for watermark column '{watermark_col}'"
        )

    if isinstance(max_value, datetime):
        return f"'{max_value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"

    if isinstance(max_value, date):
        return f"'{max_value.strftime('%Y-%m-%d')}'"

    if isinstance(max_value, int):
        return str(max_value)

    raise ValueError(
        f"{type(max_value).__name__} type not supported "
        f"for watermark column '{watermark_col}'"
    )


def check_table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    """
    Checks whether the given table exists in the catalog.
    """
    return spark.catalog.tableExists(table_name)


target_table = (
    f"l1_bronze.{to_snake(args.database)}_{to_snake(args.table)}"
)

spark = (
    SparkSession.builder
    .appName(f"ingest-sqlserver-batch-{target_table}")
    .getOrCreate()
)

logger.info(
    "Starting ingestion: database=%s table=%s target=%s extract_mode=%s write_mode=%s",
    args.database,
    args.table,
    target_table,
    args.extract_mode,
    args.write_mode,
)

target_exists = check_table_exists(
    spark,
    target_table,
)

if not target_exists or args.extract_mode == "FULL":
    dbtable = f"dbo.{args.table}"
else:
    max_value = get_max_value(
        spark,
        target_table,
        args.watermark_column,
    )

    logger.info(
        "Incremental extraction: watermark_column=%s max_value=%s",
        args.watermark_column,
        max_value,
    )

    dbtable = (
        f"(SELECT * FROM dbo.{args.table} "
        f"WHERE {args.watermark_column} > {max_value}) AS query"
    )

jdbc_df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        f"jdbc:sqlserver://sqlserver:1433;"
        f"databaseName={args.database};"
        f"trustServerCertificate=true;",
    )
    .option("dbtable", dbtable)
    .option("user", os.environ["MSSQL_USERNAME"])
    .option("password", os.environ["MSSQL_PASSWORD"])
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

if jdbc_df.isEmpty():
    logger.info(
        "No data to ingest: %s.%s",
        args.database,
        args.table,
    )

else:
    if not target_exists or args.write_mode == "OVERWRITE":
        (
            jdbc_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

    elif args.write_mode == "APPEND":
        (
            jdbc_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

    else:
        delta_table = DeltaTable.forName(
            spark,
            target_table,
        )

        (
            delta_table.alias("target")
            .merge(
                jdbc_df.alias("source"),
                f"target.{args.primary_key} = source.{args.primary_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info(
        "Ingestion completed: %s.%s -> %s",
        args.database,
        args.table,
        target_table,
    )
