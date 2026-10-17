"""
Weekly housekeeping of the Delta tables of the lakehouse: OPTIMIZE packs the
small files that the hourly runs and the streaming MERGE leave behind, and
VACUUM deletes the files no version of the last week points to.

Nothing else in the project ran either, so MinIO kept every old file of the
tables that are rewritten by month (features_ct_hora, labels_ct_hora,
agg_medida_hora...) and thousands of small files in the CDC tables of Bronze.

    spark-submit job_mantenimiento_delta.py [--bases l1_bronze,l2_silver,l3_gold] [--horas 168] [--sin-vacuum]
        [--parar-a 18] [--solo-vacuum l3_gold.dataset_train]

A table that fails (a concurrent write of the streaming, say) is logged and
skipped; the next week tries again.

dataset_train only gets VACUUM: it is compacted by partition, and the months
of 2025 ordered by (distribuidora_id, ct_id) with ZORDER, by
experimentos/consultas_trino/compactar_dataset.py, and a plain OPTIMIZE would
merge the ordered files of each month into one again.
"""

import argparse
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mantenimiento_delta")

ONLY_VACUUM = "l3_gold.dataset_train"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OPTIMIZE and VACUUM of the Delta tables")
    parser.add_argument("--bases", default="l1_bronze,l2_silver,l3_gold")
    parser.add_argument("--horas", type=int, default=168, help="VACUUM retention in hours (7 days, the Delta minimum)")
    parser.add_argument("--sin-vacuum", action="store_true", help="Only OPTIMIZE")
    parser.add_argument("--parar-a", type=int, default=None,
                        help="Local hour (Madrid) from which no new table is started, to leave Spark free for the evening")
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


def delta_tables(spark, database: str) -> list:
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
    return names


def files_of(spark, name: str) -> tuple:
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
    return int(detail["numFiles"] or 0), int(detail["sizeInBytes"] or 0)


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("mantenimiento_delta").enableHiveSupport().getOrCreate()
    done, failed = 0, 0
    before_files = after_files = 0
    for database in [b.strip() for b in args.bases.split(",") if b.strip()]:
        for name in delta_tables(spark, database):
            if args.parar_a is not None and datetime.now(ZoneInfo("Europe/Madrid")).hour >= args.parar_a:
                logger.info("Stopping at %s:00 before %s; the next run goes on", args.parar_a, name)
                break
            start = time.time()
            try:
                steps = actions(name, args)
                if not steps:
                    continue
                files, size = files_of(spark, name)
                if "optimize" in steps:
                    spark.sql(f"OPTIMIZE {name}")
                if "vacuum" in steps:
                    spark.sql(f"VACUUM {name} RETAIN {args.horas} HOURS")
                files_after, size_after = files_of(spark, name)
                before_files += files
                after_files += files_after
                done += 1
                logger.info("%s (%s): %s -> %s files, %.1f -> %.1f MB in %.0f s", name, " + ".join(steps), files,
                            files_after, size / 1e6, size_after / 1e6, time.time() - start)
            except Exception as error:  # noqa: BLE001
                failed += 1
                logger.warning("%s: skipped (%s)", name, str(error)[:200])
    logger.info("Done: %s tables, %s skipped; current files %s -> %s", done, failed, before_files, after_files)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
