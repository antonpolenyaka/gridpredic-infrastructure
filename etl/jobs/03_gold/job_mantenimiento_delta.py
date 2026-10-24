"""
Weekly housekeeping of the Delta tables of the lakehouse: OPTIMIZE packs the
small files that the hourly runs and the streaming MERGE leave behind, and
VACUUM deletes the files no version of the last week points to.

Nothing else in the project ran either, so MinIO kept every old file of the
tables that are rewritten by month (features_ct_hora, labels_ct_hora,
agg_medida_hora...) and thousands of small files in the CDC tables of Bronze.

    spark-submit job_mantenimiento_delta.py [--bases l1_bronze,l2_silver,l3_gold] [--horas 168] [--sin-vacuum]
        [--parar-a 18] [--parar-grandes-a 17] [--grandes-gb 5] [--solo-vacuum l3_gold.dataset_train]

Every step of a table has its own try: an OPTIMIZE that fails (a concurrent
write of the streaming, or the memory of the executor) is logged and the
VACUUM of the same table still runs; the next week tries again.

Two tables only get VACUUM. dataset_train is compacted by partition, and the
months of 2025 ordered by (distribuidora_id, ct_id) with ZORDER, by
experimentos/consultas_trino/compactar_dataset.py, and a plain OPTIMIZE would
merge the ordered files of each month into one again. The big table of
values of TedisNet in Bronze (2.150 million rows) is written in Id order in
files of 5 million rows (max_records_per_file), which is what lets Silver
skip files by their statistics; OPTIMIZE would rewrite it whole for nothing.
Its name in the catalog is the one job_bronze_sqlserver_batch.py builds with
to_snake from the database and the table of config_bronze_sqlserver.json.
"""

import argparse
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mantenimiento_delta")

BIG_BRONZE_TABLE = "l1_bronze.tedis_net_eosa_historic_tag_interval_values_big"
ONLY_VACUUM = f"l3_gold.dataset_train,{BIG_BRONZE_TABLE}"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OPTIMIZE and VACUUM of the Delta tables")
    parser.add_argument("--bases", default="l1_bronze,l2_silver,l3_gold")
    parser.add_argument("--horas", type=int, default=168, help="VACUUM retention in hours (7 days, the Delta minimum)")
    parser.add_argument("--sin-vacuum", action="store_true", help="Only OPTIMIZE")
    parser.add_argument("--parar-a", type=int, default=None,
                        help="Local hour (Madrid) from which no new table is started, to leave Spark free for the evening")
    parser.add_argument("--parar-grandes-a", type=int, default=None,
                        help="Local hour (Madrid) from which a big table gets no OPTIMIZE (VACUUM only): its OPTIMIZE "
                             "takes long and --parar-a is only checked between tables")
    parser.add_argument("--grandes-gb", type=float, default=5.0, help="Size from which a table counts as big for --parar-grandes-a")
    parser.add_argument("--solo-vacuum", default=ONLY_VACUUM,
                        help="Tables (comma separated) that keep their own file layout: VACUUM without OPTIMIZE")
    return parser.parse_args(argv)


def actions(name: str, args: argparse.Namespace) -> list:
    """What the week does with a table: OPTIMIZE unless it keeps its own layout, then VACUUM unless --sin-vacuum."""
    keep = {t.strip() for t in (args.solo_vacuum or "").split(",") if t.strip()}
    steps = [] if name in keep else ["optimize"]
    if not args.sin_vacuum:
        steps.append("vacuum")
    return steps


def late_for_big(steps: list, size_bytes: int, hour: int, args: argparse.Namespace) -> list:
    """The steps without OPTIMIZE when the table is big and it is already --parar-grandes-a or later."""
    if (args.parar_grandes_a is not None and "optimize" in steps and hour >= args.parar_grandes_a
            and size_bytes >= args.grandes_gb * 1e9):
        return [s for s in steps if s != "optimize"]
    return steps


def delta_tables(spark, database: str, errors: list = None) -> list:
    """The Delta tables of the database. A table that cannot be described for any reason other than
    'it is not a Delta table' is added to `errors`, so the run does not count as clean."""
    if not spark.catalog.databaseExists(database):
        return []
    names = []
    for table in spark.catalog.listTables(database):
        if table.tableType == "VIEW":
            continue
        name = f"{database}.{table.name}"
        try:
            if spark.sql(f"DESCRIBE DETAIL {name}").first()["format"] == "delta":
                names.append(name)
        except Exception as error:  # noqa: BLE001
            logger.warning("%s: not described (%s)", name, str(error)[:120])
            if "not a delta table" not in str(error).lower() and errors is not None:
                errors.append(name)
    return names


def files_of(spark, name: str) -> tuple:
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
    return int(detail["numFiles"] or 0), int(detail["sizeInBytes"] or 0)


def run_steps(spark, name: str, steps: list, hours: int) -> list:
    """The steps that worked. Each one has its own try, so a failed OPTIMIZE does not leave the table without VACUUM."""
    ran = []
    for step in steps:
        statement = f"OPTIMIZE {name}" if step == "optimize" else f"VACUUM {name} RETAIN {hours} HOURS"
        try:
            spark.sql(statement)
            ran.append(step)
        except Exception as error:  # noqa: BLE001
            logger.warning("%s: %s failed (%s)", name, step.upper(), str(error)[:200])
    return ran


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("mantenimiento_delta").enableHiveSupport().getOrCreate()
    done, failed = 0, 0
    before_files = after_files = 0
    for database in [b.strip() for b in args.bases.split(",") if b.strip()]:
        discovery_errors = []
        tables = delta_tables(spark, database, discovery_errors)
        failed += len(discovery_errors)
        for name in tables:
            if args.parar_a is not None and datetime.now(ZoneInfo("Europe/Madrid")).hour >= args.parar_a:
                logger.info("Stopping at %s:00 before %s; the next run goes on", args.parar_a, name)
                break
            start = time.time()
            steps = actions(name, args)
            if not steps:
                continue
            try:
                files, size = files_of(spark, name)
            except Exception as error:  # noqa: BLE001
                failed += 1
                logger.warning("%s: skipped (%s)", name, str(error)[:200])
                continue
            hour = datetime.now(ZoneInfo("Europe/Madrid")).hour
            planned = late_for_big(steps, size, hour, args)
            if planned != steps:
                logger.info("%s: %.1f GB at %s:00, no OPTIMIZE after %s:00 for big tables (VACUUM only)", name, size / 1e9, hour,
                            args.parar_grandes_a)
                steps = planned
            if not steps:
                continue
            ran = run_steps(spark, name, steps, args.horas)
            if len(ran) < len(steps):
                failed += 1
            if not ran:
                continue
            try:
                files_after, size_after = files_of(spark, name)
            except Exception:  # noqa: BLE001
                files_after, size_after = files, size
            before_files += files
            after_files += files_after
            done += 1
            logger.info("%s (%s): %s -> %s files, %.1f -> %.1f MB in %.0f s", name, " + ".join(ran), files,
                        files_after, size / 1e6, size_after / 1e6, time.time() - start)
    logger.info("Done: %s tables, %s skipped; current files %s -> %s", done, failed, before_files, after_files)
    spark.stop()
    if failed:
        logger.error("%s tables or steps failed: the run does not count as done (exit code 1)", failed)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
