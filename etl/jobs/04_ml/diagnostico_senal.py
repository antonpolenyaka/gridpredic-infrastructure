"""
Quick check of the signal in dataset_train before trusting a training run.

Prints, in a few dozen lines:

1. Rows and positives per split and positives per year.
2. For a sample of valid (every positive and 2 % of the negatives, weighted):
   the PR-AUC of every feature alone, its mean for the positives and for the
   negatives and the share of non zero values. The top of the list and the
   history features.
3. When the Calser interruptions become known (conocido_ts - inicio_ts) and
   which load dates concentrate them: if the history features are flat, this
   is the first place to look.

    docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/04_ml/diagnostico_senal.py
"""

import json
import os
import sys

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import ml_metrics  # noqa: E402

DATASET = "l3_gold.dataset_train"
LABEL = "y_1_3h"


def main():
    spark = (
        SparkSession.builder.appName("diagnostico-senal")
        .config("spark.cores.max", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    out = []

    ds = spark.table(DATASET)
    version = spark.table("l3_gold.dataset_versions").orderBy(F.col("creado_ts").desc()).first()
    features = json.loads(version["features_json"])

    out.append("== Filas y positivos por split")
    for r in ds.groupBy("split").agg(F.count(F.lit(1)).alias("n"), F.sum(LABEL).alias("pos")).orderBy("split").collect():
        out.append(f"{r['split']:6s} {r['n']:>11d} {r['pos']:>7d}")

    out.append("== Positivos por anio")
    for r in ds.groupBy(F.year("hora").alias("y")).agg(F.sum(LABEL).alias("pos"), F.count(F.lit(1)).alias("n")).orderBy("y").collect():
        out.append(f"{r['y']} {r['pos']:>7d} de {r['n']:>11d}")

    draw = (F.abs(F.xxhash64("distribuidora_id", "ct_id", "hora", F.lit(7))) % F.lit(1000)) / F.lit(1000.0)
    pdf = (
        ds.where((F.col("split") == "valid") & ((F.col(LABEL) == 1) | (draw < 0.02)))
        .select(F.col(LABEL).cast("int").alias("_y"), *[F.col(f).cast("float").alias(f) for f in features])
        .toPandas()
    )
    y = pdf["_y"].to_numpy()
    w = np.where(y == 1, 1.0, 50.0)
    X = pdf[features].to_numpy(dtype=np.float32, na_value=np.nan)
    ap = ml_metrics.univariate_ap(X, y, w)
    prevalence = np.average(y, weights=w)

    rows = []
    for j, f in enumerate(features):
        x = X[:, j]
        pos, neg = x[y == 1], x[y == 0]
        rows.append((f, ap[j], np.nanmean(pos) if np.isfinite(pos).any() else np.nan,
                     np.nanmean(neg) if np.isfinite(neg).any() else np.nan,
                     np.mean(np.nan_to_num(pos) != 0), np.mean(np.nan_to_num(neg) != 0),
                     np.mean(np.isnan(x))))
    rows.sort(key=lambda r: -np.nan_to_num(r[1]))

    out.append(f"== Valid: {int(y.sum())} positivos, prevalencia {prevalence:.6f}. feature, pr_auc sola, "
               "media pos, media neg, frac!=0 pos, frac!=0 neg, frac nulos")
    fmt = "{:38s} {:.6f} {:>10.3f} {:>10.3f} {:.3f} {:.3f} {:.3f}"
    for r in rows[:20]:
        out.append(fmt.format(*r))
    out.append("-- historico")
    for r in rows:
        if r[0].startswith("hist_"):
            out.append(fmt.format(*r))
    flat = [r[0] for r in rows if r[5] == 0 and r[4] == 0]
    out.append(f"-- features siempre 0 o nulas en la muestra: {len(flat)} de {len(features)}")

    fact = spark.table("l3_gold.fact_interrupciones_mt").where(F.col("variante") == "principal") \
        if "variante" in spark.table("l3_gold.fact_interrupciones_mt").columns else spark.table("l3_gold.fact_interrupciones_mt")
    lag = fact.select(((F.unix_timestamp("conocido_ts") - F.unix_timestamp("inicio_ts")) / 86400.0).alias("d"))
    q = lag.approxQuantile("d", [0.1, 0.5, 0.9, 0.99], 0.01)
    out.append(f"== Calser conocido_ts - inicio_ts (dias): p10 {q[0]:.1f}, p50 {q[1]:.1f}, p90 {q[2]:.1f}, p99 {q[3]:.1f}")
    out.append("-- fechas de conocido con mas interrupciones")
    for r in fact.groupBy(F.to_date("conocido_ts").alias("d")).count().orderBy(F.col("count").desc()).limit(5).collect():
        out.append(f"{r['d']} {r['count']}")
    out.append("-- interrupciones por anio de inicio y anio de conocido")
    for r in fact.groupBy(F.year("inicio_ts").alias("yi"), F.year("conocido_ts").alias("yc")).count().orderBy("yi", "yc").collect():
        out.append(f"inicio {r['yi']} conocido {r['yc']}: {r['count']}")

    print("\n".join(["#" * 20, *out, "#" * 20]))


if __name__ == "__main__":
    main()
