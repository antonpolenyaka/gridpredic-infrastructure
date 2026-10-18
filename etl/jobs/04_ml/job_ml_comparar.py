"""
Comparison of training runs with uncertainty, on the predictions already
saved in l3_gold.ml_predicciones (no training).

With a few hundred interruptions a year, a PR-AUC of 0,0008 against one of
0,0019 can be noise. This job says which differences are real:

- The runs are joined on the same rows of valid (distribuidora, CT, hora).
- To keep it fast, every positive and a deterministic share of the
  negatives (--tasa-negativos, the same rows for every run) are kept, with
  weight 1 / share on the negatives, as in the validation sample of the
  training job.
- Block bootstrap by days (ml_metrics.bootstrap_pr_auc): interruptions come
  in storms of several hours, so whole days are resampled. The same replicas
  serve every run and model, so the differences are paired.
- Output: PR-AUC with its 95 % interval for every run and model, in total and
  per distribuidora, and the paired differences that matter (every run
  against the first one, every model against the base rate of its run, the
  per group model against the global one). resumen.md and CSV in
  exportar_dir/comparacion_<timestamp>/, and the summary in the log.

    docker compose exec spark-master spark-submit --py-files /app/jobs/04_ml/ml_metrics.py \\
        /app/jobs/04_ml/job_ml_comparar.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import ml_metrics  # noqa: E402
from job_ml_train import PREDICTIONS_TABLE, RUNS_TABLE, get_spark, load_config, logger  # noqa: E402

MODELS = ["xgboost", "xgboost_cluster", "random_forest", "logistica", "naif_historico", "tasa_base"]
KEYS = ["distribuidora_id", "ct_id", "hora"]


def run_label(config_json: str) -> str:
    """Short name of what a run had switched on."""
    cfg = json.loads(config_json or "{}")
    flags = []
    if cfg.get("contexto_red", {}).get("activo"):
        flags.append("red")
    if cfg.get("clusters", {}).get("activo"):
        flags.append("grupos")
    if cfg.get("meteo", {}).get("activo"):
        flags.append("meteo" + ("" if cfg["meteo"].get("prevision", True) else "_sin_prev"))
    return "+".join(flags) or "base"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--runs", default=None, help="Comma separated run_id, all the runs by default")
    parser.add_argument("--reps", type=int, default=500)
    parser.add_argument("--tasa-negativos", type=float, default=0.02)
    parser.add_argument("--semilla", type=int, default=20261011)
    args, _ = parser.parse_known_args(argv)

    config = load_config(args.config)
    spark = get_spark(config)
    label = config.get("etiqueta", "y_1_3h")

    runs = spark.table(RUNS_TABLE).select("run_id", "creado_ts", "config_json").orderBy("creado_ts").toPandas()
    if args.runs:
        runs = runs[runs["run_id"].isin(args.runs.split(","))]
    else:
        # Only the runs of this job (ml_*) have rows in ml_predicciones by CT and hour: the risk hours (hr_*,
        # mh_*), the days (dia_*, caso_*), the network (red_*, rct_*) and the evenings of Airflow are other units.
        runs = runs[runs["run_id"].str.startswith("ml_")]
    runs["etiqueta"] = [f"{i + 1}_{run_label(c)}" for i, c in enumerate(runs["config_json"])]
    logger.info("Runs to compare: %s", dict(zip(runs["run_id"], runs["etiqueta"])))

    predictions = spark.table(PREDICTIONS_TABLE).where(F.col("split") == "valid")
    draw = (F.abs(F.xxhash64(*[F.col(k) for k in KEYS], F.lit(args.semilla))) % F.lit(1_000_000)) / F.lit(1_000_000.0)

    joined = None
    for row in runs.itertuples():
        part = predictions.where(F.col("run_id") == row.run_id)
        present = [m for m in MODELS if f"p_{m}" in part.columns]
        part = part.where((F.col(label) == 1) | (draw < F.lit(args.tasa_negativos)))
        cols = [F.col(f"p_{m}").alias(f"{row.etiqueta}:{m}") for m in present]

        if joined is None:
            joined = part.select(*KEYS, F.col(label).cast("int").alias("_y"), *cols)
        else:
            joined = joined.join(part.select(*KEYS, *cols), KEYS, "inner")

    pdf = joined.withColumn("_dia", F.to_date("hora")).toPandas()
    # The predictions table keeps the union of the columns of every run: a
    # model a run did not train (xgboost_cluster before the groups) is a column
    # of nulls there, and is left out.
    empty = [c for c in pdf.columns if ":" in c and pdf[c].isna().all()]
    pdf = pdf.drop(columns=empty)
    scores_all = [c for c in pdf.columns if ":" in c]
    pdf = pdf.dropna(subset=scores_all, how="all")
    weight = (pdf["_y"] == 1).map({True: 1.0, False: 1.0 / args.tasa_negativos}).to_numpy()
    logger.info("Rows compared: %s, positives %s, days %s", len(pdf), int(pdf["_y"].sum()), pdf["_dia"].nunique())

    labels = list(runs["etiqueta"])
    first = labels[0]
    pairs = []
    for lab in labels:
        cols = [c for c in scores_all if c.startswith(f"{lab}:")]
        if f"{lab}:xgboost" in cols and f"{lab}:tasa_base" in cols:
            pairs.append((f"{lab}:xgboost", f"{lab}:tasa_base"))
        if f"{lab}:xgboost_cluster" in cols:
            pairs.append((f"{lab}:xgboost_cluster", f"{lab}:xgboost"))
        if lab != first and f"{lab}:xgboost" in cols and f"{first}:xgboost" in scores_all:
            pairs.append((f"{lab}:xgboost", f"{first}:xgboost"))

    tables, diffs = [], []
    segments = [("total", None)] + [(f"distribuidora {d}", d) for d in sorted(pdf["distribuidora_id"].unique())]

    for name, value in segments:
        mask = slice(None) if value is None else (pdf["distribuidora_id"] == value).to_numpy()
        sub = pdf[mask]
        scores = {c: sub[c].fillna(sub[c].min()).to_numpy() for c in scores_all if sub[c].notna().any()}
        table, replicas = ml_metrics.bootstrap_pr_auc(sub["_y"].to_numpy(), weight[mask], sub["_dia"].to_numpy(),
                                                      scores, reps=args.reps, seed=args.semilla)
        table.insert(0, "segmento", name)
        tables.append(table)
        diff = ml_metrics.paired_differences(replicas, [(a, b) for a, b in pairs if a in scores and b in scores])
        diff.insert(0, "segmento", name)
        diffs.append(diff)
        logger.info("Bootstrap done for %s", name)

    intervals = pd.concat(tables, ignore_index=True)
    differences = pd.concat(diffs, ignore_index=True)

    report = "\n\n".join([
        "# Comparación de ejecuciones con intervalos de confianza",
        f"Valid 2025, {len(pdf)} filas (todos los positivos y el {args.tasa_negativos:.0%} de los negativos, "
        f"con peso), {int(pdf['_y'].sum())} positivos, {pdf['_dia'].nunique()} días. Bootstrap por días, "
        f"{args.reps} réplicas, intervalos del 95 %.",
        "## Ejecuciones\n\n" + ml_metrics.markdown_table(runs[["etiqueta", "run_id"]]),
        "## PR-AUC\n\n" + ml_metrics.markdown_table(intervals, digits=5),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=5),
    ]) + "\n"
    logger.info("Comparison\n%s", report)

    folder = config.get("exportar_dir")
    if folder:
        target = os.path.join(folder, "comparacion_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
        os.makedirs(target, exist_ok=True)
        with open(os.path.join(target, "resumen.md"), "w", encoding="utf-8") as file:
            file.write(report)
        intervals.to_csv(os.path.join(target, "intervalos.csv"), index=False)
        differences.to_csv(os.path.join(target, "diferencias.csv"), index=False)
        logger.info("Comparison written in %s", target)


if __name__ == "__main__":
    main()
