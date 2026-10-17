"""
Compacta l3_gold.dataset_train partición a partición y ordena en Z por
(distribuidora_id, ct_id) los meses de un periodo, para el experimento de planes
de consulta de la memoria (4.7.3). OPTIMIZE no cambia los datos (dataChange =
false): la versión del dataset sigue valiendo y los ficheros que quedan sin uso
los borra el VACUUM del mantenimiento a los siete días.

Partición a partición porque OPTIMIZE de la tabla entera se quedó sin memoria en
el mantenimiento del 16.10.2026 (con el ejecutor por defecto, 1 GB). Se lanza
dentro de spark-master con más memoria de ejecutor:

    spark-submit --conf spark.executor.memory=6g --conf spark.executor.memoryOverhead=1g --conf spark.cores.max=4 \
        compactar_dataset.py /tmp/gp_planes/compactacion.json [--zorder-desde 2025-01-01] [--zorder-hasta 2026-01-01] \
        [--ficheros-por-mes 8] [--parar-a 18]

El orden Z necesita varios ficheros por mes para que los mínimos y máximos de
cada fichero sirvan de algo: el tamaño máximo de fichero de esos meses se ajusta
para dejar unos --ficheros-por-mes. Deja en el JSON las métricas de cada OPTIMIZE
(ficheros y bytes quitados y añadidos) y el detalle de la tabla antes y después.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("compactar_dataset")

TABLE = "l3_gold.dataset_train"
ZORDER = ["distribuidora_id", "ct_id"]
MIN_FILE = 16 * 1024 * 1024
MAX_FILE = 256 * 1024 * 1024


def detail(spark) -> dict:
    row = spark.sql(f"DESCRIBE DETAIL {TABLE}").first().asDict()
    return {k: (str(v) if not isinstance(v, (int, float, str, type(None))) else v)
            for k, v in row.items() if k in ("numFiles", "sizeInBytes", "partitionColumns", "location")}


def partitions(spark) -> list:
    rows = spark.sql(f"SELECT split, fecha_mes, count(*) AS filas FROM {TABLE} GROUP BY split, fecha_mes").collect()
    return sorted(((r["split"], r["fecha_mes"], r["filas"]) for r in rows), key=lambda r: (r[1], r[0]))


def partition_bytes(spark, split: str, month) -> tuple:
    files = (spark.table(TABLE).where((F.col("split") == split) & (F.col("fecha_mes") == F.lit(month)))
             .select(F.col("_metadata.file_path").alias("f"), F.col("_metadata.file_size").alias("b")).distinct())
    row = files.agg(F.count("f").alias("n"), F.sum("b").alias("b")).first()
    return int(row["n"] or 0), int(row["b"] or 0)


def metrics_of(result) -> dict:
    rows = result.collect()
    if not rows:
        return {}
    value = rows[0]["metrics"]
    return value.asDict(recursive=True) if value is not None else {}


def stop_now(hour) -> bool:
    return hour is not None and datetime.now(ZoneInfo("Europe/Madrid")).hour >= hour


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("salida")
    parser.add_argument("--zorder-desde", default="2025-01-01")
    parser.add_argument("--zorder-hasta", default="2026-01-01")
    parser.add_argument("--ficheros-por-mes", type=int, default=8)
    parser.add_argument("--parar-a", type=int, default=18, help="Hora de Madrid desde la que no empieza otra partición")
    parser.add_argument("--sin-compactar", action="store_true")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("compactar_dataset").enableHiveSupport().getOrCreate()
    report = {"tabla": TABLE, "zorder": ZORDER, "antes": detail(spark), "compactacion": [], "orden_z": []}
    logger.info("Antes: %s", report["antes"])
    parts = partitions(spark)
    report["particiones"] = [{"split": s, "fecha_mes": str(m), "filas": n} for s, m, n in parts]
    start = time.time()

    if not args.sin_compactar:
        for split, month, rows in parts:
            if stop_now(args.parar_a):
                logger.info("Son las %s:00: paro antes de %s %s", args.parar_a, split, month)
                break
            t0 = time.time()
            try:
                result = spark.sql(f"OPTIMIZE {TABLE} WHERE split = '{split}' AND fecha_mes = DATE'{month}'")
                metrics = metrics_of(result)
                error = None
            except Exception as exc:  # noqa: BLE001
                metrics, error = {}, str(exc)[:300]
            item = {"split": split, "fecha_mes": str(month), "filas": rows, "segundos": round(time.time() - t0, 1),
                    "metricas": metrics, "error": error}
            report["compactacion"].append(item)
            logger.info("Compactada %s %s: %s -> %s ficheros en %.0f s%s", split, month,
                        metrics.get("numFilesRemoved"), metrics.get("numFilesAdded"), item["segundos"],
                        f" ERROR {error}" if error else "")
    report["despues_compactar"] = detail(spark)
    report["segundos_compactar"] = round(time.time() - start, 1)
    logger.info("Después de compactar: %s en %.0f s", report["despues_compactar"], report["segundos_compactar"])

    start = time.time()
    since, until = datetime.fromisoformat(args.zorder_desde).date(), datetime.fromisoformat(args.zorder_hasta).date()
    for split, month, rows in parts:
        if not (since <= month < until):
            continue
        if stop_now(args.parar_a):
            logger.info("Son las %s:00: paro el orden Z antes de %s %s", args.parar_a, split, month)
            break
        files, size = partition_bytes(spark, split, month)
        target = int(min(MAX_FILE, max(MIN_FILE, size / max(1, args.ficheros_por_mes))))
        spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(target))
        t0 = time.time()
        try:
            result = spark.sql(f"OPTIMIZE {TABLE} WHERE split = '{split}' AND fecha_mes = DATE'{month}' "
                               f"ZORDER BY ({', '.join(ZORDER)})")
            metrics = metrics_of(result)
            error = None
        except Exception as exc:  # noqa: BLE001
            metrics, error = {}, str(exc)[:300]
        item = {"split": split, "fecha_mes": str(month), "filas": rows, "ficheros_antes": files, "bytes": size,
                "tam_max_fichero": target, "segundos": round(time.time() - t0, 1), "metricas": metrics, "error": error}
        report["orden_z"].append(item)
        logger.info("Orden Z %s %s: %s ficheros (%.0f MB) -> %s con máximo %.0f MB en %.0f s%s", split, month, files,
                    size / 1e6, metrics.get("numFilesAdded"), target / 1e6, item["segundos"],
                    f" ERROR {error}" if error else "")
    spark.conf.unset("spark.databricks.delta.optimize.maxFileSize")
    report["despues_orden_z"] = detail(spark)
    report["segundos_orden_z"] = round(time.time() - start, 1)
    history = spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 400").select("version", "timestamp", "operation").collect()
    report["historia"] = [{"version": r["version"], "timestamp": str(r["timestamp"]), "operation": r["operation"]}
                          for r in history[:40]]
    with open(args.salida, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=1, default=str)
    logger.info("Después del orden Z: %s en %.0f s", report["despues_orden_z"], report["segundos_orden_z"])
    errors = sum(1 for i in report["compactacion"] + report["orden_z"] if i["error"])
    spark.stop()
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
