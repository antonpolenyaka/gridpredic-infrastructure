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
# Big tables. A plain JDBC read uses a single connection and a single task,
# so HistoricTagIntervalValuesBig (2.150 million rows) would be read by one
# thread and written as one huge file. These options split the read by a
# numeric column and bound the size of the files.
parser.add_argument("--partition-column")
parser.add_argument("--num-partitions", type=int)
parser.add_argument(
    "--chunk-size",
    type=int,
    help="Rows of --partition-column per Delta commit. Each chunk is read "
         "with --num-partitions parallel JDBC connections and committed on "
         "its own, so a failed run resumes from the last chunk.",
)
parser.add_argument("--max-records-per-file", type=int)
parser.add_argument("--fetch-size", type=int, default=10000)
args = parser.parse_args()

if args.extract_mode == "INCREMENTAL" and not args.watermark_column:
    parser.error("--watermark-column is required for incremental extraction")

if args.write_mode == "MERGE" and not args.primary_key:
    parser.error("--primary-key is required for merge writes")

if args.chunk_size and not args.partition_column:
    parser.error("--partition-column is required for chunked extraction")

if args.chunk_size and args.write_mode == "MERGE":
    parser.error("chunked extraction supports OVERWRITE and APPEND only")


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


def jdbc_reader(spark: SparkSession, dbtable: str):
    return (
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
        .option("fetchsize", str(args.fetch_size))
    )


def source_bounds(spark: SparkSession, where: str):
    """
    MIN and MAX of the partition column in the source, as a tiny JDBC query.
    """
    column = args.partition_column
    query = (
        f"(SELECT MIN({column}) AS lo, MAX({column}) AS hi "
        f"FROM dbo.{args.table} {where}) AS bounds"
    )
    row = jdbc_reader(spark, query).load().first()

    return row["lo"], row["hi"]


def writer(df, mode: str):
    w = df.write.format("delta").mode(mode)

    if args.max_records_per_file:
        w = w.option("maxRecordsPerFile", str(args.max_records_per_file))

    if mode == "overwrite":
        w = w.option("overwriteSchema", "true")

    return w


def run_chunked(spark: SparkSession, target_table: str, target_exists: bool) -> None:
    """
    Reads the table in ranges of --chunk-size values of --partition-column and
    commits every range on its own. The rows are written in the order of the
    column (Id, which is time order in the Historic* tables), so every file
    covers a narrow time span and Silver can skip files by their min/max
    statistics.

    INCREMENTAL starts after the highest value already in Bronze, so a run
    that fails half way is resumed by running it again.
    """
    column = args.partition_column
    incremental = target_exists and args.extract_mode == "INCREMENTAL"

    where = ""

    if incremental:
        watermark = get_max_value(spark, target_table, args.watermark_column)
        where = f"WHERE {args.watermark_column} > {watermark}"
        logger.info("Incremental extraction from %s > %s", args.watermark_column, watermark)

    lo, hi = source_bounds(spark, where)

    if lo is None:
        logger.info("No data to ingest: %s.%s", args.database, args.table)
        return

    first_write_overwrites = not target_exists or (
        args.extract_mode == "FULL" and args.write_mode == "OVERWRITE"
    )

    num_partitions = args.num_partitions or 1
    start = int(lo)
    chunk = 0

    while start <= int(hi):
        end = start + args.chunk_size
        chunk += 1

        condition = f"{column} >= {start} AND {column} < {end}"

        if where:
            condition = f"{where[len('WHERE '):]} AND {condition}"

        dbtable = f"(SELECT * FROM dbo.{args.table} WHERE {condition}) AS chunk"

        df = (
            jdbc_reader(spark, dbtable)
            .option("partitionColumn", column)
            .option("lowerBound", str(start))
            .option("upperBound", str(end))
            .option("numPartitions", str(num_partitions))
            .load()
        )

        mode = "overwrite" if first_write_overwrites and chunk == 1 else "append"

        writer(df, mode).saveAsTable(target_table)

        logger.info(
            "Chunk %s committed: %s in [%s, %s) mode=%s (last value %s)",
            chunk, column, start, end, mode, hi,
        )

        start = end


def run_single(spark: SparkSession, target_table: str, target_exists: bool) -> None:
    """
    Original path for every table that fits in one read: FULL or INCREMENTAL
    by watermark, written with OVERWRITE, APPEND or MERGE.
    """
    where = ""

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

        where = f"WHERE {args.watermark_column} > {max_value}"
        dbtable = f"(SELECT * FROM dbo.{args.table} {where}) AS query"

    reader = jdbc_reader(spark, dbtable)

    if args.partition_column and args.num_partitions:
        # Parallel read of a table that still fits in one commit. The bounds
        # are those of the rows that will actually be read: on an incremental
        # run the bounds of the whole table would put every new row in the
        # last partition and leave the other connections idle.
        bound_lo, bound_hi = source_bounds(spark, where)

        if bound_lo is not None:
            reader = (
                reader
                .option("partitionColumn", args.partition_column)
                .option("lowerBound", str(bound_lo))
                .option("upperBound", str(int(bound_hi) + 1))
                .option("numPartitions", str(args.num_partitions))
            )

    jdbc_df = reader.load()

    if jdbc_df.isEmpty():
        logger.info(
            "No data to ingest: %s.%s",
            args.database,
            args.table,
        )
        return

    if not target_exists or args.write_mode == "OVERWRITE":
        writer(jdbc_df, "overwrite").saveAsTable(target_table)

    elif args.write_mode == "APPEND":
        writer(jdbc_df, "append").saveAsTable(target_table)

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


target_table = (
    f"l1_bronze.{to_snake(args.database)}_{to_snake(args.table)}"
)

spark = (
    SparkSession.builder
    .appName(f"job-bronze-sqlserver-batch-{target_table.split('.')[-1]}")
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

if args.chunk_size:
    run_chunked(spark, target_table, target_exists)
else:
    run_single(spark, target_table, target_exists)

logger.info(
    "Ingestion completed: %s.%s -> %s",
    args.database,
    args.table,
    target_table,
)
