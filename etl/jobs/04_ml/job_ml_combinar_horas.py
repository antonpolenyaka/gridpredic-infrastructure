"""
Combination of the risk hours of two runs evaluated by years: the model of
job_ml_horas_riesgo --por-anios (dataset of TedisNet from 2021: SCADA, Calser
and weather, l3_gold.ml_predicciones_distribuidora_hora) and the model of
job_ml_meteo_historia (weather, calendar and history of Calser from 2017,
l3_gold.ml_predicciones_meteo_historia).

Both score every year with models that only saw the years before, so the two
scores of the same distribuidora and hour can be averaged without leakage.
They learn from different things (one sees the state of the network, the
other four more years of storms), and when two rankings make different
mistakes their average usually ranks better than either.

Steps:

1. The predictions of both runs for the same (distribuidora_id, hora), in
   the years both scored. The label is the one of the run of TedisNet
   (etiqueta of config_ml.json, y_1_3h): the target of the TFM.
2. Combinations of every pair of a model of each run (combinar.modelos_hr x
   combinar.modelos_mh): mean of the probabilities and mean of the ranks of
   the year (percentiles), which does not depend on the scale of each model.
3. PR-AUC, lift and alerts under a budget of hours per distribuidora and day,
   pooled and by year, and the block bootstrap by days of the differences of
   every combination against its two parts and against the gust rule.
4. Outputs: ml_metricas with run_id cmb_*, and resumen.md, metricas.csv,
   comparacion.csv and diferencias.csv in exportar_dir/<run_id>/.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_ml_horas_riesgo.py \\
        /app/jobs/04_ml/job_ml_combinar_horas.py --config /app/config/04_ml/config_ml.json \\
        --hr-run <run_id de horas_riesgo por anios> --mh-run <run_id de meteo_historia>
"""

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import ml_metrics  # noqa: E402
from job_ml_horas_riesgo import KEYS, PREDICTIONS_HR, export  # noqa: E402
from job_ml_train import METRICS_TABLE, SEASON_FEATURE, add_context, get_spark, load_config, logger, save_frame  # noqa: E402

PREDICTIONS_MH = "l3_gold.ml_predicciones_meteo_historia"
DEFAULTS = {
    "modelos_hr": ["xgboost", "logistica"],
    "modelos_mh": ["larga__xgboost", "larga__logistica"],
    "referencia": "naif_racha",
    "presupuestos_horas_dia": [1, 2, 4, 6],
    "bootstrap_reps": 500,
}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--hr-run", required=True, help="run_id of job_ml_horas_riesgo --por-anios")
    parser.add_argument("--mh-run", required=True, help="run_id of job_ml_meteo_historia")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "cmb_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def read_runs(spark, hr_run: str, mh_run: str, label: str, cfg: dict) -> tuple:
    """(rows of the hours both runs scored, names of the score columns of each run)."""
    hr = spark.table(PREDICTIONS_HR).where((F.col("run_id") == hr_run) & F.col("split").startswith("cv_"))
    wanted_hr = [m for m in cfg["modelos_hr"] + [cfg["referencia"]] if f"p_{m}" in hr.columns]
    if not wanted_hr:
        raise ValueError(f"The run {hr_run} has none of the models {cfg['modelos_hr']}")
    hr_pdf = hr.select(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "hora", "split", label,
                       SEASON_FEATURE, *[F.col(f"p_{m}").alias(f"p_hr__{m}") for m in wanted_hr]).toPandas()

    mh = spark.table(PREDICTIONS_MH).where((F.col("run_id") == mh_run) & F.col("modelo").isin(cfg["modelos_mh"]))
    mh_pdf = mh.select(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "hora", "modelo", "p",
                       "y_principal").toPandas()
    if mh_pdf.empty:
        raise ValueError(f"The run {mh_run} has none of the models {cfg['modelos_mh']}")
    wide = mh_pdf.pivot_table(index=KEYS, columns="modelo", values="p").reset_index()
    wide.columns = [c if c in KEYS else f"p_mh__{c}" for c in wide.columns]
    labels = mh_pdf.drop_duplicates(KEYS)[KEYS + ["y_principal"]]

    rows = hr_pdf.merge(wide, on=KEYS, how="inner").merge(labels, on=KEYS, how="left")
    rows["hora"] = pd.to_datetime(rows["hora"])
    rows["anio"] = rows["hora"].dt.year
    rows["ct_key"] = rows["distribuidora_id"]
    rows[label] = rows[label].fillna(0).astype(int)
    names_hr = [f"p_hr__{m}" for m in wanted_hr]
    names_mh = [c for c in rows.columns if c.startswith("p_mh__")]
    logger.info("Hours scored by both runs: %s of %s (TedisNet) and %s (history); years %s", len(rows), len(hr_pdf),
                len(wide), sorted(rows["anio"].unique()))
    return rows.sort_values(KEYS).reset_index(drop=True), names_hr, names_mh


def add_combinations(rows: pd.DataFrame, names_hr: list, names_mh: list, reference: str) -> list:
    """Mean of probabilities and of ranks of the year for every pair of models of the two runs."""
    combos = []
    trained_hr = [n for n in names_hr if not n.endswith(f"__{reference}")]

    for a in trained_hr:
        for b in names_mh:
            short = f"{a[2:]}+{b[2:]}"
            rows[f"p_media:{short}"] = (rows[a] + rows[b]) / 2
            ranks = rows.groupby("anio")[[a, b]].rank(pct=True)
            rows[f"p_rango:{short}"] = (ranks[a] + ranks[b]) / 2
            combos += [f"p_media:{short}", f"p_rango:{short}"]

    return combos


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    cfg = {**DEFAULTS, **config.get("combinar", {})}
    label = config.get("etiqueta", "y_1_3h")
    seed = int(config.get("semilla", 0))
    budgets = [int(n) for n in cfg["presupuestos_horas_dia"]]
    budget = 2 if 2 in budgets else budgets[0]
    version = f"{args.hr_run}+{args.mh_run}"

    rows, names_hr, names_mh = read_runs(spark, args.hr_run, args.mh_run, label, cfg)
    if rows.empty:
        raise ValueError("The two runs have no hour in common")
    agreement = float((rows[label] == rows["y_principal"].fillna(0).astype(int)).mean())
    combos = add_combinations(rows, names_hr, names_mh, cfg["referencia"])
    scores = names_hr + names_mh + combos
    segment_map = {"distribuidora": "distribuidora_id", "estacion": SEASON_FEATURE}

    parts = []
    for year in sorted(rows["anio"].unique()):
        metrics = ml_metrics.evaluate(rows[rows["anio"] == year], scores, [label], budgets, segment_map, seed=seed)
        parts.append(add_context(metrics, args.run_id, version, f"cv_{year}"))
    metrics = ml_metrics.evaluate(rows, scores, [label], budgets, segment_map, seed=seed)
    parts.append(add_context(metrics, args.run_id, version, "cv_total"))
    metrics = pd.concat(parts, ignore_index=True)

    total = metrics[(metrics["split"] == "cv_total") & (metrics["segmento"] == "total")]
    comparison = total.pivot_table(index="modelo", columns="metrica", values="valor")
    keep = ["pr_auc", "lift_pr_auc", "roc_auc", f"precision@{budget}", f"recall_episodios@{budget}"]
    comparison = comparison[[c for c in keep if c in comparison.columns]].sort_values("pr_auc", ascending=False)
    comparison = comparison.reset_index()
    per_year = metrics[(metrics["segmento"] == "total") & (metrics["metrica"] == "pr_auc")].pivot_table(
        index="modelo", columns="split", values="valor").reset_index()

    reference = next((n for n in names_hr if n.endswith(f"__{cfg['referencia']}")), None)
    pairs = []
    for combo in combos:
        a, b = combo.split(":", 1)[1].split("+")
        pairs += [(combo[2:], a), (combo[2:], b)]
        if reference:
            pairs.append((combo[2:], reference[2:]))
    pairs += [(a[2:], b[2:]) for a in names_hr for b in names_mh if not a.endswith(f"__{cfg['referencia']}")]
    reps = int(cfg["bootstrap_reps"])
    intervals, replicas = ml_metrics.bootstrap_pr_auc(
        rows[label].to_numpy(), np.ones(len(rows)), rows["hora"].dt.floor("D").to_numpy(),
        {s[2:]: rows[s].to_numpy() for s in scores}, reps, seed)
    differences = ml_metrics.paired_differences(replicas, pairs)

    save_frame(spark, metrics, METRICS_TABLE, args.run_id)
    years = ", ".join(str(y) for y in sorted(rows["anio"].unique()))
    report = "\n\n".join([
        f"# Combinación de las horas de riesgo: {args.run_id}",
        f"Horas puntuadas por las dos ejecuciones: {len(rows)} (años {years}). TedisNet: {args.hr_run}; historia "
        f"meteorológica: {args.mh_run}. Etiqueta {label} del dataset de TedisNet; coincide con y_principal de la "
        f"historia en el {str(round(100 * agreement, 1)).replace('.', ',')} % de las horas. media: media de las probabilidades; rango: media de los "
        f"percentiles del año. Presupuesto de {budget} horas por distribuidora y día.",
        "## Los años juntos\n\n" + ml_metrics.markdown_table(comparison, digits=4),
        "## PR-AUC por año\n\n" + ml_metrics.markdown_table(per_year, digits=4),
        f"## PR-AUC con intervalo del 95 % (bootstrap por días, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals.sort_values("pr_auc", ascending=False), digits=4),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=4),
    ]) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "comparacion": comparison, "intervalos": intervals,
                                         "diferencias": differences})
    logger.info("Combination %s completed", args.run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
