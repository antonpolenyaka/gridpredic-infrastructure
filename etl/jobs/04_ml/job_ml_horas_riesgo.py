"""
Risk hours of every distribuidora: will an interruption start somewhere in
the network of the distribuidora in the next 1 - 3 hours?

The model per CT and hour has a prevalence of 0,04 %: about 4.300 positive
hours in a year of valid, grouped in storms, so its PR-AUC moves a lot from
one run to the next and the differences between configurations cannot be
told apart (job_ml_comparar). 94 % of the interruptions are systemic: what
announces them is the state of the whole network and the weather, not the
CT. This job asks the question at the level where that signal lives, the
distribuidora and the hour, with many more positives per row and the same
temporal split. It is also what the operator plans with: which hours of the
day need a crew ready.

Steps:

1. Dataset (l3_gold.ml_dataset_distribuidora_hora), one row per
   distribuidora and hour:
   - Labels from dataset_train: y_1_3h is 1 when at least one CT of the
     distribuidora has it (the same for the local label), with the number
     of CTs and the shortest horas_hasta_proximo_evento. The split is the
     one of dataset_train (it only depends on the hour).
   - Features from features_ct_hora, every CT and hour, so no feature
     depends on which CTs the dataset left out (en_corte comes from the
     labels): the mean of every CT feature (media_*) and, for the event,
     SCADA and history blocks (horas_riesgo.prefijos_frac), the share of CTs
     where it is not zero (frac_*). Without features_ct_hora (the tests) it
     falls back to dataset_train.
   - Weather of the distribuidora from meteo_celda_hora: the mean over its
     cells weighted by the CTs of every cell (metd_media_*) and the worst
     cell (metd_max_*).
   - distribuidora_id and estacion.
   The table is reused while its key (dataset version and aggregation
   settings) does not change; --reconstruir builds it again.
2. Models: tasa_base, naif_historico (known interruptions of the CTs of the
   distribuidora in 365, 90 and 30 days), the single feature references of
   horas_riesgo.referencias (the strongest gust in the area, what an
   operator would watch) and logistica, random_forest and xgboost with the
   search of config_ml.json. There are only about 150.000 rows: train is
   used whole, without sample, and valid whole for early stopping and to
   choose the configuration.
3. Evaluation on valid (and test with --evaluar-test, until test_hasta):
   PR-AUC, ROC-AUC, lift and the alerts under a budget of N hours per
   distribuidora and day (presupuestos_horas_dia), by distribuidora and
   season. Block bootstrap by days with the PR-AUC interval of every model
   and the paired differences against the references.
4. Outputs: ml_runs, ml_metricas and ml_importancia with run_id hr_*,
   l3_gold.ml_predicciones_distribuidora_hora, the models in
   artefactos_uri/<run_id>/ and resumen.md, metricas.csv, importancia.csv,
   intervalos.csv and diferencias.csv in exportar_dir/<run_id>/.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \\
        /app/jobs/04_ml/job_ml_horas_riesgo.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import ml_metrics  # noqa: E402
import ml_models  # noqa: E402
from job_ml_train import (  # noqa: E402
    DATASET_TABLE,
    FEATURES_TABLE,
    IMPORTANCE_TABLE,
    LEAD,
    METRICS_TABLE,
    RUNS_TABLE,
    SEASON_FEATURE,
    add_context,
    add_season,
    examine,
    feature_names,
    get_spark,
    load_config,
    logger,
    resolve_version,
    save_artifacts,
    save_frame,
)

GOLD = "l3_gold"
DATASET_HR = f"{GOLD}.ml_dataset_distribuidora_hora"
PREDICTIONS_HR = f"{GOLD}.ml_predicciones_distribuidora_hora"
KEYS = ["distribuidora_id", "hora"]
DEFAULT_FRAC = ["ev_", "evred_", "evaa_", "scada_", "calidad_", "corte_error_comm_", "hist_"]
NAIVE = tuple(f"media_{f}" for f in ml_models.NAIVE_FEATURES)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--reconstruir", action="store_true", help="Build the distribuidora-hour dataset again")
    parser.add_argument("--sin-busqueda", action="store_true", help="Only the first configuration of every model")
    parser.add_argument("--evaluar-test", action="store_true", help="Score and evaluate the test split too")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "hr_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

def dataset_key(version: str, source: str, frac_prefixes: list, meteo_cfg: dict, labels: list) -> str:
    settings = {
        "version": version,
        "fuente": source,
        "frac": sorted(frac_prefixes),
        "meteo": bool(meteo_cfg.get("activo")),
        "prevision": bool(meteo_cfg.get("prevision", True)),
        "etiquetas": labels,
    }
    return hashlib.sha1(json.dumps(settings, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def distributor_weather(spark, meteo_cfg: dict):
    """
    (distribuidora_id, hora, metd_media_*, metd_max_*): the weather of every
    distribuidora, the mean of its cells weighted by the number of CTs of
    each cell and the worst cell. None without the weather tables.
    """
    if not meteo_cfg.get("activo"):
        return None, []

    cells_table = meteo_cfg.get("tabla_ct_celda", f"{GOLD}.map_ct_celda")
    weather_table = meteo_cfg.get("tabla_meteo", f"{GOLD}.meteo_celda_hora")

    if not (spark.catalog.tableExists(cells_table) and spark.catalog.tableExists(weather_table)):
        logger.warning("No weather for the risk hours: %s or %s not found", cells_table, weather_table)
        return None, []

    weights = (
        spark.table(cells_table).where(F.col("celda_id").isNotNull())
        .groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id")
        .agg(F.count(F.lit(1)).cast("double").alias("_n"))
    )
    weather = spark.table(weather_table)
    names = [c for c in weather.columns if c.startswith(("met_", "metprev_"))]

    if not meteo_cfg.get("prevision", True):
        names = [c for c in names if not c.startswith("metprev_")]

    joined = weather.select("celda_id", "hora", *names).join(F.broadcast(weights), "celda_id")
    aggs = []

    for name in names:
        value = F.col(name).cast("double")
        aggs.append((F.sum(value * F.col("_n")) / F.sum(F.when(value.isNotNull(), F.col("_n")))).alias(f"metd_media_{name}"))
        aggs.append(F.max(value).alias(f"metd_max_{name}"))

    out = joined.groupBy("distribuidora_id", "hora").agg(*aggs)
    return out, [f"metd_{kind}_{name}" for name in names for kind in ("media", "max")]


def build_dataset(spark, features: list, labels: list, cfg: dict, meteo_cfg: dict, version: str, key: str,
                  source: str):
    """Builds and writes ml_dataset_distribuidora_hora. Returns the feature names."""
    dataset = spark.table(DATASET_TABLE)
    label = labels[0]

    targets = dataset.groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "hora").agg(
        F.first("split").alias("split"),
        *[F.max(F.col(c).cast("int")).alias(c) for c in labels],
        F.sum(F.col(label).cast("int")).alias("n_cts_positivos"),
        F.count(F.lit(1)).alias("n_cts_dataset"),
        F.min(F.col(LEAD).cast("double")).alias(LEAD),
    )

    bounds = dataset.agg(F.min("hora").alias("desde"), F.max("hora").alias("hasta")).first()
    src = spark.table(source).where(F.col("hora").between(bounds["desde"], bounds["hasta"]))
    present = [f for f in features if f in src.columns and f not in ("distribuidora_id", "ct_id", "hora")]
    prefixes = tuple(cfg.get("prefijos_frac", DEFAULT_FRAC))
    frac = [f for f in present if f.startswith(prefixes)]

    aggs = [F.avg(F.col(f).cast("double")).alias(f"media_{f}") for f in present]
    aggs += [F.avg((F.coalesce(F.col(f).cast("double"), F.lit(0.0)) > 0).cast("double")).alias(f"frac_{f}")
             for f in frac]
    network = src.groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "hora").agg(*aggs)
    names = [f"media_{f}" for f in present] + [f"frac_{f}" for f in frac]

    out = targets.join(network, KEYS, "left")
    weather, weather_names = distributor_weather(spark, meteo_cfg)

    if weather is not None:
        out = out.join(weather, KEYS, "left")
        names += weather_names

    start = time.time()
    (
        out.withColumn("dataset_version", F.lit(version)).withColumn("clave", F.lit(key))
        .withColumn("audit_loaded_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DATASET_HR)
    )
    rows = spark.table(DATASET_HR).count()
    logger.info("%s built from %s in %.0f s: %s rows, %s features (%s means, %s shares, %s weather)",
                DATASET_HR, source, time.time() - start, rows, len(names), len(present), len(frac),
                len(weather_names))
    return names


def stored_names(spark, key: str):
    """Feature names of the stored dataset when its key matches, else None."""
    if not spark.catalog.tableExists(DATASET_HR):
        return None

    table = spark.table(DATASET_HR)
    keys = [r["clave"] for r in table.select("clave").distinct().collect()] if "clave" in table.columns else []

    if keys != [key]:
        return None

    return [c for c in table.columns if c.startswith(("media_", "frac_", "metd_"))]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

def to_arrays(pdf: pd.DataFrame, features: list, label: str):
    X = pdf[features].to_numpy(dtype=np.float32, na_value=np.nan)
    y = pdf[label].fillna(0).to_numpy(dtype=np.int8)
    return X, y


def train(names: list, config: dict, cfg: dict, data: dict, base_rate: float, only_first: bool) -> tuple:
    seed = int(config.get("semilla", 0))
    n_jobs = int(config.get("n_jobs", -1))
    grids = {**config.get("busqueda", {}), **cfg.get("busqueda", {})}
    features = data["feature_names"]
    scorers, search = {}, []

    def keep(name, scorer, params, index=0):
        ap = ml_metrics.average_precision(data["yv"], scorer.score(data["Xv"]))
        search.append({"modelo": name, "config": index, "params": params, "pr_auc_valid_muestra": ap, **scorer.info})
        logger.info("%s configuration %s: PR-AUC on valid %.5f", name, index + 1, ap)
        return ap

    for name in names:
        if name == "tasa_base":
            scorer = ml_models.build_reference(name, features, base_rate)
        elif name == "naif_historico":
            scorer = ml_models.build_reference(name, features, base_rate, NAIVE)
        else:
            continue

        scorer.info["pr_auc_valid_muestra"] = keep(name, scorer, scorer.params)
        scorers[name] = scorer

    for name, feature in cfg.get("referencias", {}).items():
        if feature not in features:
            logger.warning("Reference %s skipped: no feature %s", name, feature)
            continue

        scorer = ml_models.FeatureScorer(name, features, feature)
        scorer.info["pr_auc_valid_muestra"] = keep(name, scorer, scorer.params)
        scorers[name] = scorer

    for name in [n for n in names if n in ml_models.TRAINED]:
        grid = grids[name][:1] if only_first else grids[name]
        best, best_ap = None, -1.0

        for index, params in enumerate(grid):
            logger.info("Fitting %s, configuration %s of %s: %s", name, index + 1, len(grid), params)
            scorer = ml_models.fit_model(name, params, data, 1.0, seed, n_jobs)
            ap = keep(name, scorer, params, index)

            if ap > best_ap:
                best, best_ap = scorer, ap

        best.info["pr_auc_valid_muestra"] = best_ap
        scorers[name] = best

    return scorers, search


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def bootstrap(pdf: pd.DataFrame, label: str, models: list, references: list, reps: int, seed: int) -> tuple:
    """PR-AUC intervals and paired differences, in total and per distribuidora."""
    pairs = []
    trained = [m for m in models if m in ml_models.TRAINED]

    for model in trained:
        pairs += [(model, ref) for ref in references]

    if "xgboost" in trained:
        pairs += [("xgboost", m) for m in trained if m != "xgboost"]

    tables, diffs = [], []
    segments = [("total", None)] + [(f"distribuidora {d}", d) for d in sorted(pdf["distribuidora_id"].unique())]

    for name, value in segments:
        sub = pdf if value is None else pdf[pdf["distribuidora_id"] == value]
        scores = {m: sub[f"p_{m}"].to_numpy() for m in models}
        table, replicas = ml_metrics.bootstrap_pr_auc(sub[label].to_numpy(), np.ones(len(sub)),
                                                      sub["hora"].dt.floor("D").to_numpy(), scores, reps, seed)
        table.insert(0, "segmento", name)
        tables.append(table)
        diff = ml_metrics.paired_differences(replicas, pairs)
        diff.insert(0, "segmento", name)
        diffs.append(diff)

    return pd.concat(tables, ignore_index=True), pd.concat(diffs, ignore_index=True)


def describe(pdf: pd.DataFrame, labels: list) -> pd.DataFrame:
    rows = []

    for (split, dist), g in pdf.groupby(["split", "distribuidora_id"]):
        row = {"split": split, "distribuidora": int(dist), "horas": len(g)}
        for label in labels:
            row[f"positivas_{label}"] = int(g[label].sum())
            row[f"prevalencia_{label}"] = float(g[label].mean())
        rows.append(row)

    return pd.DataFrame(rows)


def export(config: dict, run_id: str, report: str, frames: dict):
    folder = config.get("exportar_dir")
    logger.info("Summary of run %s\n%s", run_id, report)

    if not folder:
        return

    try:
        target = os.path.join(folder, run_id)
        os.makedirs(target, exist_ok=True)

        with open(os.path.join(target, "resumen.md"), "w", encoding="utf-8") as file:
            file.write(report)

        for name, frame in frames.items():
            frame.to_csv(os.path.join(target, f"{name}.csv"), index=False)

        logger.info("Summary files written in %s", target)
    except OSError as error:
        logger.error("Summary files not written in %s: %s", folder, error)


def save_predictions(spark, pdf: pd.DataFrame, run_id: str, split: str, models: list, labels: list):
    columns = KEYS + labels + [LEAD, SEASON_FEATURE] + [f"p_{m}" for m in models]
    df = (
        spark.createDataFrame(pdf[[c for c in columns if c in pdf.columns]])
        .withColumn("run_id", F.lit(run_id)).withColumn("split", F.lit(split))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )
    writer = df.write.format("delta").option("mergeSchema", "true")

    if spark.catalog.tableExists(PREDICTIONS_HR):
        writer.mode("overwrite").option("replaceWhere", f"run_id = '{run_id}' AND split = '{split}'") \
            .saveAsTable(PREDICTIONS_HR)
    else:
        writer.mode("overwrite").partitionBy("run_id", "split").saveAsTable(PREDICTIONS_HR)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)

    cfg = config.get("horas_riesgo", {})
    meteo_cfg = config.get("meteo", {})
    evaluation = config.get("evaluacion", {})
    seed = int(config.get("semilla", 0))
    label = config.get("etiqueta", "y_1_3h")

    version = resolve_version(spark, args.dataset_version or config.get("dataset_version"))
    dataset_version = version["dataset_version"]
    columns = spark.table(DATASET_TABLE).columns
    labels = [label] + [c for c in config.get("etiquetas_evaluacion", []) if c != label and c in columns]
    ct_features = feature_names(version, config, columns)

    source = FEATURES_TABLE if spark.catalog.tableExists(FEATURES_TABLE) else DATASET_TABLE
    if source == DATASET_TABLE:
        logger.warning("%s not found: the features of the risk hours are aggregated from %s", FEATURES_TABLE,
                       DATASET_TABLE)

    key = dataset_key(dataset_version, source, cfg.get("prefijos_frac", DEFAULT_FRAC), meteo_cfg, labels)
    names = None if args.reconstruir else stored_names(spark, key)

    if names is None:
        names = build_dataset(spark, ct_features, labels, cfg, meteo_cfg, dataset_version, key, source)
    else:
        logger.info("Reusing %s (key %s)", DATASET_HR, key)

    features = ["distribuidora_id"] + names + [SEASON_FEATURE]
    table = add_season(spark.table(DATASET_HR))
    pdf = table.select(*KEYS, "split", *labels, LEAD, "n_cts_positivos", *names, SEASON_FEATURE).toPandas()
    pdf["distribuidora_id"] = pdf["distribuidora_id"].astype("int64")
    pdf = pdf.sort_values(KEYS).reset_index(drop=True)
    sizes = describe(pdf, labels)
    logger.info("Risk hours dataset: %s rows, %s features\n%s", len(pdf), len(features),
                ml_metrics.markdown_table(sizes, digits=4))

    tr = pdf[pdf["split"] == "train"]
    va = pdf[pdf["split"] == "valid"]
    X, y = to_arrays(tr, features, label)
    Xv, yv = to_arrays(va, features, label)
    data = {"X": X, "y": y, "w": np.ones(len(y)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
            "feature_names": features}
    base_rate = float(y.mean())

    names_models = cfg.get("modelos", config["modelos"])
    scorers, search = train(names_models, config, cfg, data, base_rate, args.sin_busqueda)
    importance = examine(scorers, data, config)
    models = list(scorers)

    meta = {"run_id": args.run_id, "dataset_version": dataset_version, "unidad": "distribuidora_hora",
            "etiqueta": label, "features": features,
            "modelos": {m: {"params": s.params, **s.info} for m, s in scorers.items()}}
    artifacts = save_artifacts(scorers, config.get("artefactos_uri"), args.run_id, meta, spark)

    budgets = [int(n) for n in cfg.get("presupuestos_horas_dia", [1, 2, 4, 6])]
    segment_map = {"distribuidora": "distribuidora_id", "estacion": SEASON_FEATURE}
    splits = ["valid"] + (["test"] if args.evaluar_test else [])
    all_metrics, scored = [], {}

    for split in splits:
        part = pdf[pdf["split"] == split].copy()
        if split == "test" and evaluation.get("test_hasta"):
            part = part[part["hora"] < pd.Timestamp(evaluation["test_hasta"])]

        Xs, _ = to_arrays(part, features, label)
        for model in models:
            part[f"p_{model}"] = scorers[model].score(Xs)

        part["ct_key"] = part["distribuidora_id"]
        save_predictions(spark, part, args.run_id, split, models, labels)
        metrics = ml_metrics.evaluate(part, [f"p_{m}" for m in models], labels, budgets, segment_map, seed=seed)
        all_metrics.append(add_context(metrics, args.run_id, dataset_version, split))
        scored[split] = part

        summary = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label)]
        for metric in ["pr_auc", "roc_auc", f"precision@{budgets[0]}", f"recall_episodios@{budgets[0]}"]:
            values = summary[summary["metrica"] == metric].set_index("modelo")["valor"].round(4).to_dict()
            logger.info("%s %s: %s", split, metric, values)

    metrics = pd.concat(all_metrics, ignore_index=True)
    references = [m for m in models if m not in ml_models.TRAINED]
    reps = int(cfg.get("bootstrap_reps", 500))
    intervals, differences = bootstrap(scored["valid"], label, models, references, reps, seed)

    save_frame(spark, metrics, METRICS_TABLE, args.run_id)
    save_frame(spark, add_context(importance, args.run_id, dataset_version, "valid"), IMPORTANCE_TABLE, args.run_id)

    budget = 2 if 2 in budgets else budgets[0]
    local = next((c for c in labels if c.endswith("_local")), None)
    report = "\n\n".join([
        f"# Horas de riesgo por distribuidora: {args.run_id}",
        f"Unidad: distribuidora y hora. {label} = 1 si empieza una interrupción en algún CT de la distribuidora "
        f"entre 1 y 3 horas después. Dataset {dataset_version}, {len(features)} features. Presupuesto de alertas: "
        f"{budget} horas por distribuidora y día.",
        "## Filas y prevalencia\n\n" + ml_metrics.markdown_table(sizes, digits=4),
        ml_metrics.report_markdown(metrics, importance, search, label, budget, local).strip(),
        f"## PR-AUC en valid con intervalo del 95 % (bootstrap por días, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals, digits=4),
        "## Diferencias emparejadas en valid (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b."
        "\n\n" + ml_metrics.markdown_table(differences, digits=4),
    ]) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "importancia": importance, "intervalos": intervals,
                                         "diferencias": differences, "filas": sizes})

    total = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label)
                    & metrics["metrica"].isin(["pr_auc", "roc_auc"])]
    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": dataset_version,
        "etiqueta": f"distribuidora_hora:{label}",
        "n_features": len(features),
        "modelos_json": json.dumps(meta["modelos"], ensure_ascii=False, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps(config, ensure_ascii=False),
        "resumen_json": json.dumps({f"{r.split}:{r.modelo}:{r.metrica}": r.valor for r in total.itertuples()}),
        "splits_evaluados": ",".join(splits),
        "artefactos_uri": artifacts or "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)

    logger.info("Run %s completed: %s, %s", args.run_id, DATASET_HR, PREDICTIONS_HR)
    return args.run_id


if __name__ == "__main__":
    main()
