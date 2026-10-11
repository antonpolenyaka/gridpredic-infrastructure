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
    parser.add_argument("--sin-prevision", action="store_true",
                        help="Leave out the forecast of the next hours (meteo.prevision false)")
    parser.add_argument("--por-anios", nargs="?", const="config", default=None,
                        help="Rolling evaluation by years (comma separated, horas_riesgo.anios_evaluacion by default)")
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
        # The table always stores the forecast of the next hours.
        "prevision": True,
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

    # The next hours (metprev_*) are always stored; meteo.prevision false
    # leaves them out when training, so both runs share the table.
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
    segments = [("total", None, None)]
    segments += [(f"distribuidora {d}", "distribuidora_id", d) for d in sorted(pdf["distribuidora_id"].unique())]
    if "anio" in pdf.columns:
        segments += [(f"año {a}", "anio", a) for a in sorted(pdf["anio"].unique())]

    for name, column, value in segments:
        sub = pdf if column is None else pdf[pdf[column] == value]
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
# Rolling evaluation by years
# ---------------------------------------------------------------------------

def refit(name: str, scorer, data: dict, seed: int, n_jobs: int):
    """
    The chosen configuration fitted again on the whole training period of
    the fold. XGBoost keeps the number of trees found by early stopping.
    """
    params = dict(scorer.params)

    if name == "xgboost":
        trees = int(scorer.info.get("n_arboles", params.get("n_estimators", 1500)))
        params.update(n_estimators=trees, early_stopping_rounds=trees + 1)

    return ml_models.fit_model(name, params, data, 1.0, seed, n_jobs)


def rolling_evaluation(pdf: pd.DataFrame, features: list, label: str, config: dict, cfg: dict, years: list,
                       only_first: bool, test_until: str = None) -> tuple:
    """
    One fold per year Y of years, never touching test: the models are chosen
    on the last share (fraccion_validacion_interna) of the hours before Y,
    fitted again on all the hours before Y and score the year Y. With about
    30 incidents a year, pooling three years gives about three times the
    events to compare the models with, and every year is scored by a model
    that only saw its past.

    With test_until (only with --evaluar-test) the hours of test before it
    join the pool, so the year of test can be scored once with the same
    procedure.

    Returns (scored rows of every year with the column anio, search rows,
    scorers and inner data of the last fold).
    """
    seed = int(config.get("semilla", 0))
    n_jobs = int(config.get("n_jobs", -1))
    share = float(cfg.get("fraccion_validacion_interna", 0.25))
    names = cfg.get("modelos", config["modelos"])
    pool = pdf[pdf["split"].isin(["train", "valid"])]

    if test_until:
        test = pdf[(pdf["split"] == "test") & (pdf["hora"] < pd.Timestamp(test_until))]
        pool = pd.concat([pool, test]).sort_values(KEYS)

    parts, search_all = [], []
    scorers, inner = {}, {}

    for year in years:
        start = pd.Timestamp(f"{year}-01-01")
        before = pool[pool["hora"] < start]
        target = pool[(pool["hora"] >= start) & (pool["hora"] < pd.Timestamp(f"{year + 1}-01-01"))].copy()

        if before.empty or target.empty:
            logger.warning("Year %s skipped: no hours before it or in it", year)
            continue

        hours = np.sort(before["hora"].unique())
        cut = hours[int(len(hours) * (1 - share))]
        fit_part, check_part = before[before["hora"] < cut], before[before["hora"] >= cut]

        X, y = to_arrays(fit_part, features, label)
        Xv, yv = to_arrays(check_part, features, label)
        inner = {"X": X, "y": y, "w": np.ones(len(y)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
                 "feature_names": features}
        logger.info("Year %s: choosing on %s hours (%s positive) and %s hours from %s (%s positive)", year,
                    len(y), int(y.sum()), len(yv), pd.Timestamp(cut), int(yv.sum()))

        scorers, search = train(names, config, cfg, inner, float(y.mean()), only_first)
        search_all += [{**row, "anio": year} for row in search]

        Xa, ya = to_arrays(before, features, label)
        full = {"X": Xa, "y": ya, "w": np.ones(len(ya)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
                "feature_names": features}

        for name in [n for n in scorers if n in ml_models.TRAINED]:
            scorers[name] = refit(name, scorers[name], full, seed, n_jobs)
        if "tasa_base" in scorers:
            scorers["tasa_base"] = ml_models.build_reference("tasa_base", features, float(ya.mean()))

        Xs, _ = to_arrays(target, features, label)
        for name, scorer in scorers.items():
            target[f"p_{name}"] = scorer.score(Xs)

        target["anio"] = year
        parts.append(target)
        logger.info("Year %s scored: %s hours, %s positive", year, len(target), int(target[label].sum()))

    if not parts:
        raise ValueError(f"No year of {years} could be scored (the year of test needs --evaluar-test)")

    return pd.concat(parts, ignore_index=True), search_all, scorers, inner


def by_year_table(metrics: pd.DataFrame, label: str, budget: int) -> pd.DataFrame:
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label)]
    out = []

    for metric in ["pr_auc", f"recall_episodios@{budget}"]:
        table = rows[rows["metrica"] == metric].pivot_table(index="modelo", columns="split", values="valor")
        table.columns = [f"{metric} {c.replace('cv_', '')}" for c in table.columns]
        out.append(table)

    return pd.concat(out, axis=1).reset_index()


def run_by_years(spark, args, config: dict, cfg: dict, pdf: pd.DataFrame, features: list, labels: list,
                 dataset_version: str, created) -> str:
    label = labels[0]
    seed = int(config.get("semilla", 0))
    raw = args.por_anios if args.por_anios != "config" else None
    years = [int(y) for y in raw.split(",")] if raw else [int(y) for y in cfg.get("anios_evaluacion", [2023, 2024, 2025])]
    budgets = [int(n) for n in cfg.get("presupuestos_horas_dia", [1, 2, 4, 6])]
    budget = 2 if 2 in budgets else budgets[0]
    segment_map = {"distribuidora": "distribuidora_id", "estacion": SEASON_FEATURE}

    test_until = config.get("evaluacion", {}).get("test_hasta") if args.evaluar_test else None
    if test_until is None and args.evaluar_test:
        test_until = str(pdf["hora"].max() + pd.Timedelta(hours=1))

    scored, search, scorers, inner = rolling_evaluation(pdf, features, label, config, cfg, years, args.sin_busqueda,
                                                        test_until)
    scored["ct_key"] = scored["distribuidora_id"]
    models = [c[2:] for c in scored.columns if c.startswith("p_")]
    present_years = sorted(scored["anio"].unique().tolist())

    all_metrics = []
    for year in present_years:
        part = scored[scored["anio"] == year]
        save_predictions(spark, part, args.run_id, f"cv_{year}", models, labels)
        metrics = ml_metrics.evaluate(part, [f"p_{m}" for m in models], labels, budgets, segment_map, seed=seed)
        all_metrics.append(add_context(metrics, args.run_id, dataset_version, f"cv_{year}"))

    metrics = ml_metrics.evaluate(scored, [f"p_{m}" for m in models], labels, budgets, segment_map, seed=seed)
    all_metrics.append(add_context(metrics, args.run_id, dataset_version, "cv_total"))
    metrics = pd.concat(all_metrics, ignore_index=True)

    references = [m for m in models if m not in ml_models.TRAINED]
    reps = int(cfg.get("bootstrap_reps", 500))
    intervals, differences = bootstrap(scored, label, models, references, reps, seed)
    importance = examine(scorers, inner, config)

    save_frame(spark, metrics, METRICS_TABLE, args.run_id)
    save_frame(spark, add_context(importance, args.run_id, dataset_version, f"cv_{present_years[-1]}"),
               IMPORTANCE_TABLE, args.run_id)

    counts = scored.groupby("anio").agg(horas=(label, "size"), horas_positivas=(label, "sum")).reset_index()
    counts["incidencias"] = [
        int(len(np.unique(e[e >= 0]))) for e in
        (ml_metrics.episode_ids(scored[scored["anio"] == a], label).to_numpy() for a in counts["anio"])
    ]
    pooled = metrics[(metrics["split"] == "cv_total") & (metrics["segmento"] == "total") & (metrics["etiqueta"] == label)]
    pooled = pooled.pivot_table(index="modelo", columns="metrica", values="valor")
    keep = ["pr_auc", "lift_pr_auc", "roc_auc", f"precision@{budget}", f"recall_episodios@{budget}",
            f"recall_episodios@{budgets[-1]}", f"antelacion_media_h@{budget}"]
    pooled = pooled[[c for c in keep if c in pooled.columns]].sort_values("pr_auc", ascending=False).reset_index()
    shap = importance[(importance["modelo"] == "xgboost") & (importance["metrica"] == "shap_medio_abs")]

    report = "\n\n".join([
        f"# Horas de riesgo por distribuidora, evaluación por años: {args.run_id}",
        f"Cada año lo puntúa un modelo que solo ha visto los años anteriores (elegido en el último "
        f"{float(cfg.get('fraccion_validacion_interna', 0.25)):.0%} de esas horas y reentrenado con todas). "
        f"{'Incluye test hasta ' + test_until + '. ' if test_until else 'Test no se toca. '}Dataset {dataset_version}, {len(features)} features, presupuesto de {budget} horas "
        f"por distribuidora y día.",
        "## Años evaluados\n\n" + ml_metrics.markdown_table(counts),
        "## Por año\n\n" + ml_metrics.markdown_table(by_year_table(metrics, label, budget), digits=4),
        "## Los años juntos\n\n" + ml_metrics.markdown_table(pooled, digits=4),
        f"## PR-AUC con intervalo del 95 % (bootstrap por días, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals, digits=4),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=4),
        f"## Las 15 features con más peso en XGBoost del año {present_years[-1]} (SHAP medio absoluto)\n\n"
        + ml_metrics.markdown_table(shap.sort_values("valor", ascending=False).head(15)[["feature", "valor"]],
                                    digits=5),
    ]) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "importancia": importance, "intervalos": intervals,
                                         "diferencias": differences, "anios": counts})

    total = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == label) & (metrics["metrica"] == "pr_auc")]
    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": dataset_version,
        "etiqueta": f"distribuidora_hora_por_anios:{label}",
        "n_features": len(features),
        "modelos_json": json.dumps({m: {"params": s.params, **s.info} for m, s in scorers.items()}, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps(config, ensure_ascii=False),
        "resumen_json": json.dumps({f"{r.split}:{r.modelo}:{r.metrica}": r.valor for r in total.itertuples()}),
        "splits_evaluados": ",".join(f"cv_{y}" for y in present_years),
        "artefactos_uri": "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)
    logger.info("Rolling evaluation %s completed: years %s", args.run_id, present_years)
    return args.run_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)

    cfg = config.get("horas_riesgo", {})

    if args.sin_prevision:
        config["meteo"] = {**config.get("meteo", {}), "prevision": False}

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

    if not meteo_cfg.get("prevision", True):
        names = [n for n in names if "metprev_" not in n]
        logger.info("meteo.prevision false: the forecast of the next hours is left out")

    features = ["distribuidora_id"] + names + [SEASON_FEATURE]
    table = add_season(spark.table(DATASET_HR))
    pdf = table.select(*KEYS, "split", *labels, LEAD, "n_cts_positivos", *names, SEASON_FEATURE).toPandas()
    pdf["distribuidora_id"] = pdf["distribuidora_id"].astype("int64")
    pdf = pdf.sort_values(KEYS).reset_index(drop=True)
    sizes = describe(pdf, labels)
    logger.info("Risk hours dataset: %s rows, %s features\n%s", len(pdf), len(features),
                ml_metrics.markdown_table(sizes, digits=4))

    if args.por_anios:
        return run_by_years(spark, args, config, cfg, pdf, features, labels, dataset_version, created)

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
