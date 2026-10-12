"""
Risk hours of every distribuidora with the whole weather history.

job_ml_horas_riesgo learns from dataset_train, which starts in 2021 because
TedisNet does. The weather of Gold now starts in 2017 (Open-Meteo, ECMWF IFS)
and Calser records the CT interruptions the same way since 2016, so a model
with weather, calendar and the history known in Calser can learn from four
more years: about 170 more incidents, three times what the fold of 2023 had.
The learning curve of job_ml_horas_riesgo said that doubling the data would
raise the PR-AUC; this job measures it with the real years.

Steps:

1. Dataset (pandas, a few hundred thousand rows), one row per distribuidora
   and hour from meteo_historia.desde to meteo_historia.hasta:
   - Labels from fact_interrupciones_mt (variante principal, every year, not
     only the window of Gold) with the cause of l2_silver.f_incidencia_causa.
     y_principal is 1 when an interruption of the principal variant starts in
     a CT of the distribuidora between horizonte_h[0] and horizonte_h[1]
     hours later (the y_1_3h of dataset_train at the level of the
     distribuidora). y_meteo is the same without the causes of
     causas_no_meteo (upstream, blackout, third parties...), which the
     weather of the area cannot announce.
   - Weather of the distribuidora (metd_media_*, metd_max_*) as in
     job_ml_horas_riesgo, only the observed hours unless --con-prevision.
     The families that do not cover the whole history (CAPE from 2021) are
     blocks of their own (conveccion, aire, suelo, fuego), so a variant only
     uses them when it asks for them.
   - Calendar (cal_*): hour and day of the year as sine and cosine, working
     day (Monday to Friday and not a national or Extremadura holiday),
     working hours and estacion.
   - History (hist_*): incidents of the principal variant of the
     distribuidora known in the last 7, 30 and 365 days and hours since the
     last one, counted from conocido_ts (when the record existed in Calser).
   - One column per distribuidora (dist_<id>).
   The days of analisis_red.dias_excluidos (the blackout of 28.04.2025) are
   left out.
2. Variants (meteo_historia.variantes): first hour of training, blocks of
   features and label used to train. Every variant runs the rolling
   evaluation of job_ml_horas_riesgo: each year Y is scored by models chosen
   and fitted only on the hours before Y, with the strongest gust in the area
   as reference.
3. Comparison on the years every variant scored, always against y_principal
   (what the operator needs, whatever the label of training): PR-AUC, lift
   and alerts under a budget of hours per distribuidora and day, by year,
   and the block bootstrap by days of the paired differences
   (meteo_historia.comparaciones).
4. Outputs: ml_runs, ml_metricas and ml_importancia with run_id mh_*, the
   score of every variant and model for every hour it scored in
   l3_gold.ml_predicciones_meteo_historia (to compare or combine it with
   other runs hour by hour), and resumen.md, metricas.csv, comparacion.csv,
   diferencias.csv, importancia.csv and filas.csv in
   exportar_dir/<run_id>/.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_ml_horas_riesgo.py \\
        /app/jobs/04_ml/job_ml_meteo_historia.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import json
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
from calendario import easter, holiday_flags, holidays, season, working_days  # noqa: E402,F401
from job_ml_horas_riesgo import KEYS, distributor_weather, export, rolling_evaluation  # noqa: E402
from job_ml_train import (  # noqa: E402
    IMPORTANCE_TABLE,
    METRICS_TABLE,
    RUNS_TABLE,
    SEASON_FEATURE,
    add_context,
    examine,
    get_spark,
    load_config,
    logger,
    save_frame,
)

GOLD = "l3_gold"
FACTS_TABLE = f"{GOLD}.fact_interrupciones_mt"
PREDICTIONS_TABLE = f"{GOLD}.ml_predicciones_meteo_historia"
CAUSES_TABLE = "l2_silver.f_incidencia_causa"
LABELS = ["y_principal", "y_meteo"]
MAIN_LABEL = "y_principal"

# Families of weather features that do not cover the whole history, come
# from another source or are measured on their own (antecedente: the rain of
# the days before and the humid hours): a variant uses them only when it
# names the block.
WEATHER_BLOCKS = {
    "antecedente": ("_lluvia_72h", "_lluvia_previa_30d", "_humedad_max_6h"),
    "avisos": ("_aviso_",),
    "conveccion": ("_cape", "_li_"),
    "aire": ("_polvo", "_pm10"),
    "suelo": ("_humedad_suelo",),
    "extra": ("_dir_viento", "_viento_100m", "_rocio_dif", "_dpv_", "_nubes_bajas", "_et0_", "_codigo_max",
              "_tormenta_"),
    "fuego": ("_fwi", "_ffmc", "_dmc", "_dc_", "_isi", "_bui"),
}

DEFAULTS = {
    "desde": "2017-01-08",
    "hasta": "2026-01-01",
    "horizonte_h": [1, 3],
    "anios_evaluacion": [2022, 2023, 2024, 2025],
    "dias_minimos_entreno": 180,
    "causas_no_meteo": ["aguas_arriba", "apagon", "terceros", "programada", "maniobra", "cliente_bt", "red_bt"],
    "retraso_conocido_dias": 7,
    "ventanas_historia_dias": [7, 30, 365],
    "horario_laboral": [8, 18],
    "modelos": ["tasa_base", "logistica", "xgboost"],
    "referencias": {"naif_racha": "metd_max_met_racha_max_3h"},
    "fraccion_validacion_interna": 0.25,
    "presupuestos_horas_dia": [1, 2, 4, 6],
    "bootstrap_reps": 500,
    "variantes": [
        {"nombre": "corta", "desde": "2021-01-08", "bloques": ["meteo", "calendario", "historia"],
         "etiqueta": "principal"},
        {"nombre": "larga", "desde": "2017-01-08", "bloques": ["meteo", "calendario", "historia"],
         "etiqueta": "principal"},
    ],
    "comparaciones": [["larga__xgboost", "corta__xgboost"], ["larga__logistica", "corta__logistica"]],
}

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--sin-busqueda", action="store_true", help="Only the first configuration of every model")
    parser.add_argument("--con-prevision", action="store_true",
                        help="Add the weather of the next hours (perfect forecast of the reanalysis)")
    parser.add_argument("--variantes", default=None,
                        help="Comma separated names of meteo_historia.variantes to run (all by default)")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "mh_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def settings(config: dict) -> dict:
    cfg = {**DEFAULTS, **config.get("meteo_historia", {})}
    excluded = config.get("analisis_red", {}).get("dias_excluidos", {"2025-04-28": "apagon peninsular"})
    cfg["dias_excluidos"] = sorted(excluded)
    return cfg


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

def calendar(hours: pd.Series, working_hours: list) -> pd.DataFrame:
    """Calendar features of the local hour (the hours of the lakehouse are local time)."""
    hour = hours.dt.hour.to_numpy()
    day_of_year = hours.dt.dayofyear.to_numpy()
    is_holiday = holiday_flags(hours)
    working = working_days(hours)
    lo, hi = int(working_hours[0]), int(working_hours[1])

    return pd.DataFrame({
        "cal_hora_sin": np.sin(2 * np.pi * hour / 24),
        "cal_hora_cos": np.cos(2 * np.pi * hour / 24),
        "cal_dia_anio_sin": np.sin(2 * np.pi * (day_of_year - 1) / 365.25),
        "cal_dia_anio_cos": np.cos(2 * np.pi * (day_of_year - 1) / 365.25),
        "cal_laborable": working.astype("float64"),
        "cal_festivo": is_holiday.astype("float64"),
        "cal_horario_laboral": (working & (hour >= lo) & (hour < hi)).astype("float64"),
        SEASON_FEATURE: season(hours.dt.month).to_numpy(),
    }, index=hours.index)


# ---------------------------------------------------------------------------
# Labels and history
# ---------------------------------------------------------------------------

def labelled_hours(starts: pd.Series, horizon: list) -> pd.DatetimeIndex:
    """
    Hours h of the grid with a start e in (h + a, h + b]: e - b <= h < e - a,
    the rule of positives() in job_gold_labels_ct_hora.
    """
    a, b = int(horizon[0]), int(horizon[1])
    out = []

    for start in pd.to_datetime(starts).dropna():
        first = (start - pd.Timedelta(hours=b)).ceil("h")
        last = (start - pd.Timedelta(hours=a) - pd.Timedelta(microseconds=1)).floor("h")
        if last >= first:
            out.append(pd.date_range(first, last, freq="h"))

    if not out:
        return pd.DatetimeIndex([])

    return out[0].append(out[1:]).unique() if len(out) > 1 else out[0]


def add_labels(grid: pd.DataFrame, events: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """y_principal and y_meteo of every (distribuidora_id, hora) of the grid."""
    excluded = set(cfg["causas_no_meteo"])
    weather_related = ~events["causa"].isin(excluded)
    selections = {"y_principal": events, "y_meteo": events[weather_related]}

    for label, chosen in selections.items():
        grid[label] = 0

        for dist, rows in chosen.groupby("distribuidora_id"):
            hours = labelled_hours(rows["inicio_ts"], cfg["horizonte_h"])
            mask = (grid["distribuidora_id"] == dist) & grid["hora"].isin(hours)
            grid.loc[mask, label] = 1

        grid[label] = grid[label].astype("int8")

    return grid


def add_history(grid: pd.DataFrame, known: pd.DataFrame, windows: list) -> list:
    """
    hist_incidencias_<n>d (incidents known in the last n days, the hour
    included) and hist_horas_desde_ultima (hours since the last known one,
    one year at most) of every row of the grid. known: distribuidora_id,
    conocido_ts, one row per incident.
    """
    names = [f"hist_incidencias_{w}d" for w in windows] + ["hist_horas_desde_ultima"]
    cap = 365 * 24.0

    for name in names:
        grid[name] = 0.0

    seconds = grid["hora"].to_numpy().astype("datetime64[s]").astype(np.int64)

    for dist, index in grid.groupby("distribuidora_id").indices.items():
        times = known.loc[known["distribuidora_id"] == dist, "conocido_ts"].dropna()
        k = np.sort(times.to_numpy().astype("datetime64[s]").astype(np.int64))
        t = seconds[index]
        upto = np.searchsorted(k, t, side="right")

        for w in windows:
            since = np.searchsorted(k, t - int(w) * 86400, side="right")
            grid.iloc[index, grid.columns.get_loc(f"hist_incidencias_{w}d")] = (upto - since).astype("float64")

        last = np.where(upto > 0, k[np.clip(upto - 1, 0, max(len(k) - 1, 0))] if len(k) else 0, -1)
        hours = np.where(last >= 0, (t - last) / 3600.0, cap)
        grid.iloc[index, grid.columns.get_loc("hist_horas_desde_ultima")] = np.minimum(hours, cap)

    return names


def read_events(spark, cfg: dict) -> tuple:
    """
    (events, known): the starts of the principal interruptions with the cause
    of their incident, and when every incident was known in Calser.
    """
    facts = spark.table(FACTS_TABLE).where(F.col("variante") == "principal")
    delay = int(cfg["retraso_conocido_dias"])
    known_col = (F.col("conocido_ts") if "conocido_ts" in facts.columns
                 else F.col("inicio_ts") + F.expr(f"INTERVAL {delay} DAYS"))
    facts = facts.select(
        F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "periodo_id", "incidencia_id", "ct_id",
        "inicio_ts", known_col.alias("conocido_ts"),
    )

    if spark.catalog.tableExists(CAUSES_TABLE):
        causes = spark.table(CAUSES_TABLE).select(
            F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "periodo_id",
            F.col("id").alias("incidencia_id"), "causa",
        ).dropDuplicates(["distribuidora_id", "periodo_id", "incidencia_id"])
        facts = facts.join(causes, ["distribuidora_id", "periodo_id", "incidencia_id"], "left")
    else:
        logger.warning("%s not found: y_meteo is the same as y_principal", CAUSES_TABLE)
        facts = facts.withColumn("causa", F.lit(None).cast("string"))

    events = facts.toPandas()
    events["inicio_ts"] = pd.to_datetime(events["inicio_ts"])
    events["conocido_ts"] = pd.to_datetime(events["conocido_ts"])
    known = (
        events.groupby(["distribuidora_id", "periodo_id", "incidencia_id"], dropna=False)["conocido_ts"].min()
        .reset_index()[["distribuidora_id", "conocido_ts"]]
    )
    return events, known


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

def block_of(name: str) -> str:
    """Block of a feature: calendario, historia, distribuidora or a weather block."""
    if name.startswith("cal_") or name == SEASON_FEATURE:
        return "calendario"
    if name.startswith("hist_"):
        return "historia"
    if name.startswith("dist_"):
        return "distribuidora"
    if name.startswith("int_"):
        return "interacciones"
    for block, parts in WEATHER_BLOCKS.items():
        if any(part in name for part in parts):
            return block
    return "meteo"


def build_dataset(spark, config: dict, cfg: dict, with_forecast: bool) -> tuple:
    """(pdf, {block: feature names}) of the distribuidora-hour history."""
    start, end = pd.Timestamp(cfg["desde"]), pd.Timestamp(cfg["hasta"])
    weather, names = distributor_weather(spark, {**config.get("meteo", {}), "activo": True})

    if weather is None:
        raise ValueError("No weather in Gold (meteo_celda_hora and map_ct_celda): nothing to learn from")

    if not with_forecast:
        names = [n for n in names if "metprev_" not in n]

    weather = weather.where((F.col("hora") >= F.lit(start)) & (F.col("hora") < F.lit(end)))
    pdf = weather.select(*KEYS, *names).toPandas()
    pdf["distribuidora_id"] = pdf["distribuidora_id"].astype("int64")
    pdf["hora"] = pd.to_datetime(pdf["hora"])

    # Every hour of every distribuidora, also the ones the weather misses.
    hours = pd.date_range(start, end - pd.Timedelta(hours=1), freq="h")
    grid = pd.MultiIndex.from_product([sorted(pdf["distribuidora_id"].unique()), hours], names=KEYS).to_frame(
        index=False)
    pdf = grid.merge(pdf, on=KEYS, how="left")

    days = pd.to_datetime(cfg["dias_excluidos"])
    pdf = pdf[~pdf["hora"].dt.normalize().isin(days)].sort_values(KEYS).reset_index(drop=True)

    events, known = read_events(spark, cfg)
    pdf = add_labels(pdf, events, cfg)
    history = add_history(pdf, known, cfg["ventanas_historia_dias"])
    cal = calendar(pdf["hora"], cfg["horario_laboral"])
    pdf = pd.concat([pdf, cal], axis=1)

    dists = []
    for dist in sorted(pdf["distribuidora_id"].unique()):
        pdf[f"dist_{dist}"] = (pdf["distribuidora_id"] == dist).astype("float64")
        dists.append(f"dist_{dist}")

    # The same storm does not hit every network the same way: the weather of
    # interacciones_meteo times the column of each distribuidora lets the
    # logistic regression learn one slope per distribuidora.
    interactions = []
    for feature in [f for f in cfg.get("interacciones_meteo", []) if f in pdf.columns]:
        for dist in dists:
            name = f"int_{dist}__{feature}"
            pdf[name] = pdf[dist] * pdf[feature]
            interactions.append(name)

    pdf["anio"] = pdf["hora"].dt.year
    pdf["ct_key"] = pdf["distribuidora_id"]
    pdf["split"] = "train"

    blocks = {}
    for name in names + list(cal.columns) + history + dists + interactions:
        blocks.setdefault(block_of(name), []).append(name)

    covered = pdf.groupby("anio")[names[0]].apply(lambda s: float(s.notna().mean())) if names else pd.Series()
    logger.info("Share of hours with weather by year: %s", covered.round(3).to_dict())
    return pdf, blocks


def describe(pdf: pd.DataFrame) -> pd.DataFrame:
    """Hours, positive hours and starts of every label by year and distribuidora."""
    rows = []

    for (year, dist), g in pdf.groupby(["anio", "distribuidora_id"]):
        row = {"anio": int(year), "distribuidora": int(dist), "horas": len(g)}
        for label in LABELS:
            row[f"positivas_{label}"] = int(g[label].sum())
            row[f"episodios_{label}"] = int(((g[label] == 1) & (g[label].shift(fill_value=0) == 0)).sum())
        row["con_meteo"] = float(g["metd_max_met_racha_1h"].notna().mean()) if "metd_max_met_racha_1h" in g else 0.0
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

def variant_features(variant: dict, blocks: dict) -> list:
    chosen = list(blocks.get("distribuidora", []))
    for block in variant["bloques"]:
        chosen += blocks.get(block, [])
    return chosen


def missing_blocks(variant: dict, blocks: dict) -> list:
    """Blocks a variant asks for that the dataset does not have (a source not loaded yet)."""
    return [block for block in variant["bloques"] if not blocks.get(block)]


def variant_years(variant: dict, cfg: dict) -> list:
    """Years of evaluation with at least dias_minimos_entreno days of training before them."""
    first = pd.Timestamp(variant["desde"]) + pd.Timedelta(days=int(cfg["dias_minimos_entreno"]))
    return [int(y) for y in cfg["anios_evaluacion"] if pd.Timestamp(f"{int(y)}-01-01") >= first]


def run_variant(pdf: pd.DataFrame, variant: dict, features: list, config: dict, cfg: dict, only_first: bool):
    """(scored rows with p_<variant>__<model>, search rows, scorers, inner data of the last fold)."""
    name = variant["nombre"]
    label = f"y_{variant['etiqueta']}"
    years = variant_years(variant, cfg)

    if not years:
        logger.warning("Variant %s skipped: no year of evaluation has enough training before it", name)
        return None, [], {}, {}

    rows = pdf[pdf["hora"] >= pd.Timestamp(variant["desde"])]
    logger.info("Variant %s: %s features, label %s, training from %s, years %s", name, len(features), label,
                variant["desde"], years)
    scored, search, scorers, inner = rolling_evaluation(rows, features, label, config, cfg, years, only_first,
                                                        names=cfg["modelos"])
    models = [c[2:] for c in scored.columns if c.startswith("p_")]
    keep = KEYS + ["anio", "ct_key", SEASON_FEATURE] + LABELS
    out = scored[keep].copy()

    for model in models:
        out[f"p_{name}__{model}"] = scored[f"p_{model}"].to_numpy()

    search = [{**row, "variante": name} for row in search]
    return out, search, scorers, inner


def training_sizes(pdf: pd.DataFrame, variants: list, cfg: dict) -> pd.DataFrame:
    """Positive hours and episodes each variant trained with before every year it scored."""
    rows = []

    for variant in variants:
        label = f"y_{variant['etiqueta']}"
        for year in variant_years(variant, cfg):
            part = pdf[(pdf["hora"] >= pd.Timestamp(variant["desde"])) & (pdf["hora"] < pd.Timestamp(f"{year}-01-01"))]
            episodes = ml_metrics.episode_ids(part, label)
            rows.append({"variante": variant["nombre"], "anio": year, "etiqueta_entreno": label,
                         "horas_entreno": len(part), "positivas_entreno": int(part[label].sum()),
                         "episodios_entreno": int(len(np.unique(episodes[episodes >= 0])))})

    return pd.DataFrame(rows)


def comparison_table(metrics: pd.DataFrame, budget: int) -> pd.DataFrame:
    """One row per variant and model with the main metrics against both labels."""
    rows = metrics[metrics["segmento"] == "total"]
    wanted = {
        (MAIN_LABEL, "pr_auc"): "pr_auc",
        (MAIN_LABEL, "lift_pr_auc"): "lift",
        (MAIN_LABEL, "roc_auc"): "roc_auc",
        (MAIN_LABEL, f"precision@{budget}"): f"precision@{budget}",
        (MAIN_LABEL, f"recall_episodios@{budget}"): f"recall_episodios@{budget}",
        ("y_meteo", "pr_auc"): "pr_auc_y_meteo",
    }
    out = {}

    for (label, metric), column in wanted.items():
        part = rows[(rows["etiqueta"] == label) & (rows["metrica"] == metric)]
        out[column] = part.set_index("modelo")["valor"]

    table = pd.DataFrame(out).reset_index().rename(columns={"index": "modelo", "modelo": "modelo"})
    split = table["modelo"].str.split("__", n=1, expand=True)
    table.insert(0, "variante", split[0])
    table["modelo"] = split[1]
    return table.sort_values("pr_auc", ascending=False).reset_index(drop=True)


def day_level(scored: pd.DataFrame, columns: list, label: str, shares: tuple = (0.05, 0.1, 0.2),
              seed: int = 0) -> pd.DataFrame:
    """
    The risk of the day: a distribuidora-day is positive when some hour of it
    is, and its score is the highest of its hours. PR-AUC of the days and the
    part of the positive days that falls in the top k % of the days of each
    year (an alert that keeps a crew ready that day). The operator plans the
    day; inside a stormy day the models hardly know which hour comes first.
    """
    days = scored.assign(dia=scored["hora"].dt.floor("D"))
    agg = {label: "max", **{c: "max" for c in columns}}
    per_day = days.groupby(["distribuidora_id", "dia"], as_index=False).agg(agg)
    per_day["anio"] = per_day["dia"].dt.year
    y = per_day[label].to_numpy()
    rows = []

    for column in columns:
        row = {"modelo": column[2:], "dias": len(per_day), "dias_positivos": int(y.sum()),
               "pr_auc_dia": ml_metrics.average_precision(y, per_day[column].to_numpy()),
               "roc_auc_dia": ml_metrics.roc_auc(y, per_day[column].to_numpy())}
        for share in shares:
            caught, total = 0, 0
            for _, year in per_day.groupby("anio"):
                k = max(1, int(np.ceil(share * len(year))))
                # Ties (the constant base rate) are broken at random, not by date.
                tiebreak = np.random.default_rng(seed).random(len(year))
                order = np.lexsort((tiebreak, -year[column].to_numpy(dtype=np.float64)))
                caught += int(year[label].to_numpy()[order[:k]].sum())
                total += int(year[label].sum())
            row[f"dias_positivos_en_top_{int(round(100 * share))}pct"] = caught / total if total else float("nan")
        rows.append(row)

    return pd.DataFrame(rows).sort_values("pr_auc_dia", ascending=False).reset_index(drop=True)


def save_predictions(spark, scored: pd.DataFrame, columns: list, run_id: str):
    """
    Scores of every variant and model for every hour it scored, in long
    format (l3_gold.ml_predicciones_meteo_historia, partitioned by run_id),
    so other runs can be compared or combined with them hour by hour.
    """
    keep = KEYS + ["anio"] + LABELS
    long = scored[keep + columns].melt(id_vars=keep, value_vars=columns, var_name="modelo", value_name="p")
    long = long[long["p"].notna()]
    long["modelo"] = long["modelo"].str[2:]
    long["run_id"] = run_id
    df = spark.createDataFrame(long).withColumn("audit_loaded_at", F.current_timestamp())
    writer = df.write.format("delta").option("mergeSchema", "true")

    if spark.catalog.tableExists(PREDICTIONS_TABLE):
        writer.mode("overwrite").option("replaceWhere", f"run_id = '{run_id}'").saveAsTable(PREDICTIONS_TABLE)
    else:
        writer.mode("overwrite").partitionBy("run_id").saveAsTable(PREDICTIONS_TABLE)
    logger.info("%s rows of predictions written to %s", len(long), PREDICTIONS_TABLE)


def by_distribuidora(metrics: pd.DataFrame) -> pd.DataFrame:
    """PR-AUC of every model in each distribuidora, in the common years."""
    rows = metrics[(metrics["split"] == "cv_comun") & (metrics["segmento"] == "distribuidora")
                   & (metrics["etiqueta"] == MAIN_LABEL) & (metrics["metrica"] == "pr_auc")]
    if rows.empty:
        return pd.DataFrame()
    table = rows.pivot_table(index="modelo", columns="valor_segmento", values="valor")
    table.columns = [f"distribuidora {c}" for c in table.columns]
    return table.reset_index()


def by_year(metrics: pd.DataFrame) -> pd.DataFrame:
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == MAIN_LABEL)
                   & (metrics["metrica"] == "pr_auc")]
    table = rows.pivot_table(index="modelo", columns="split", values="valor").reset_index()
    table.columns = [str(c).replace("cv_", "pr_auc ") for c in table.columns]
    return table


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    cfg = settings(config)
    seed = int(config.get("semilla", 0))
    budgets = [int(n) for n in cfg["presupuestos_horas_dia"]]
    budget = 2 if 2 in budgets else budgets[0]
    version = f"meteo_historia_{cfg['desde']}_{cfg['hasta']}"

    variants = cfg["variantes"]
    if args.variantes:
        wanted = {v.strip() for v in args.variantes.split(",") if v.strip()}
        variants = [v for v in variants if v["nombre"] in wanted]
    if not variants:
        raise ValueError("No variant to run")

    pdf, blocks = build_dataset(spark, config, cfg, args.con_prevision)
    sizes = describe(pdf)
    logger.info("Dataset: %s rows, blocks %s\n%s", len(pdf), {k: len(v) for k, v in blocks.items()},
                ml_metrics.markdown_table(sizes, digits=3))

    # A variant that asks for a block without features would repeat another one.
    for variant in [v for v in variants if missing_blocks(v, blocks)]:
        logger.warning("Variant %s skipped: no features of the blocks %s", variant["nombre"],
                       missing_blocks(variant, blocks))
    variants = [v for v in variants if not missing_blocks(v, blocks)]
    if not variants:
        raise ValueError("No variant has all its blocks")

    outs, search, last = [], [], {}
    for variant in variants:
        features = variant_features(variant, blocks)
        out, rows, scorers, inner = run_variant(pdf, variant, features, config, cfg, args.sin_busqueda)
        if out is None:
            continue
        search += rows
        last[variant["nombre"]] = (scorers, inner)
        outs.append(out)

    if not outs:
        raise ValueError("No variant could be scored")

    # Every row of the years some variant scored, with the scores of each
    # variant (null in the years it did not score).
    scored_years = sorted(set().union(*[set(out["anio"].unique()) for out in outs]))
    scored = pdf.loc[pdf["anio"].isin(scored_years), KEYS + ["anio", "ct_key", SEASON_FEATURE] + LABELS]
    for out in outs:
        scored = scored.merge(out[KEYS + [c for c in out.columns if c.startswith("p_")]], on=KEYS, how="left")

    scored = scored.sort_values(KEYS).reset_index(drop=True)
    # Mean of the two trained models of every variant: two rankings that
    # make different mistakes usually rank better together.
    for variant in [v["nombre"] for v in variants]:
        pair = [f"p_{variant}__logistica", f"p_{variant}__xgboost"]
        if all(c in scored.columns for c in pair):
            scored[f"p_{variant}__media"] = scored[pair].mean(axis=1, skipna=False)
    columns = [c for c in scored.columns if c.startswith("p_")]
    segment_map = {"distribuidora": "distribuidora_id", "estacion": SEASON_FEATURE}
    years = sorted(scored["anio"].unique().tolist())
    common = [y for y in years if scored.loc[scored["anio"] == y, columns].notna().all(axis=None)]

    all_metrics = []
    for year in years:
        part = scored[scored["anio"] == year]
        present = [c for c in columns if part[c].notna().all()]
        metrics = ml_metrics.evaluate(part, present, LABELS, budgets, segment_map, seed=seed)
        all_metrics.append(add_context(metrics, args.run_id, version, f"cv_{year}"))

    pooled = scored[scored["anio"].isin(common)]
    if common:
        metrics = ml_metrics.evaluate(pooled, columns, LABELS, budgets, segment_map, seed=seed)
        all_metrics.append(add_context(metrics, args.run_id, version, "cv_comun"))
    metrics = pd.concat(all_metrics, ignore_index=True)
    comparison = comparison_table(metrics[metrics["split"] == "cv_comun"], budget) if common else pd.DataFrame()
    days = day_level(pooled, columns, MAIN_LABEL) if common else pd.DataFrame()
    per_dist = by_distribuidora(metrics)

    reps = int(cfg["bootstrap_reps"])
    pairs = [(f"p_{a}", f"p_{b}") for a, b in cfg["comparaciones"] if f"p_{a}" in columns and f"p_{b}" in columns]
    intervals, differences = pd.DataFrame(), pd.DataFrame()
    if common and pairs:
        named = sorted({name for pair in pairs for name in pair})
        intervals, replicas = ml_metrics.bootstrap_pr_auc(
            pooled[MAIN_LABEL].to_numpy(), np.ones(len(pooled)), pooled["hora"].dt.floor("D").to_numpy(),
            {name[2:]: pooled[name].to_numpy() for name in named}, reps, seed)
        differences = ml_metrics.paired_differences(replicas, [(a[2:], b[2:]) for a, b in pairs])

    main_variant = next((v["nombre"] for v in variants if v["nombre"] == "larga"), list(last)[-1])
    scorers, inner = last[main_variant]
    importance = examine(scorers, inner, config) if inner else pd.DataFrame()
    trained = training_sizes(pdf, variants, cfg)

    save_frame(spark, metrics, METRICS_TABLE, args.run_id)
    if not importance.empty:
        save_frame(spark, add_context(importance, args.run_id, version, f"cv_{years[-1]}"), IMPORTANCE_TABLE,
                   args.run_id)
    save_predictions(spark, scored, columns, args.run_id)

    variant_rows = pd.DataFrame([{
        "variante": v["nombre"], "desde": v["desde"], "bloques": "+".join(v["bloques"]),
        "etiqueta_entreno": f"y_{v['etiqueta']}", "anios": ",".join(str(y) for y in variant_years(v, cfg)),
        "n_features": len(variant_features(v, blocks)),
    } for v in variants])
    shap = importance[(importance["modelo"] == "xgboost") & (importance["metrica"] == "shap_medio_abs")] \
        if not importance.empty else pd.DataFrame(columns=["feature", "valor"])
    totals = sizes.groupby("anio")[[f"episodios_{label}" for label in LABELS]].sum().reset_index()

    report = "\n\n".join([
        f"# Horas de riesgo con toda la historia meteorológica: {args.run_id}",
        f"Unidad: distribuidora y hora, de {cfg['desde']} a {cfg['hasta']} sin los días excluidos "
        f"({', '.join(cfg['dias_excluidos'])}). y_principal = 1 si empieza una interrupción de la variante "
        f"principal en algún CT de la distribuidora entre {cfg['horizonte_h'][0]} y {cfg['horizonte_h'][1]} horas "
        f"después; y_meteo, lo mismo sin las causas {', '.join(cfg['causas_no_meteo'])}. "
        f"{'Con' if args.con_prevision else 'Sin'} la meteorología de las horas siguientes. Cada año lo puntúa un "
        f"modelo que solo ha visto las horas anteriores; las cifras comparadas son siempre contra y_principal, en "
        f"los años que puntúan todas las variantes ({', '.join(str(y) for y in common) or 'ninguno'}), con un "
        f"presupuesto de {budget} horas por distribuidora y día.",
        "## Episodios por año (inicios de interrupción)\n\n" + ml_metrics.markdown_table(totals, digits=0),
        "## Variantes\n\n" + ml_metrics.markdown_table(variant_rows, digits=0),
        "## Datos de entrenamiento de cada año\n\n" + ml_metrics.markdown_table(trained, digits=0),
        "## Comparación en los años comunes\n\n" + ml_metrics.markdown_table(comparison, digits=4),
        "## Días de riesgo\n\nUn día de una distribuidora cuenta como positivo si alguna de sus horas lo es, con la "
        "puntuación más alta de sus horas. dias_positivos_en_top_k es la parte de los días con avería que caen en el "
        "k % de días con más riesgo de cada año.\n\n" + ml_metrics.markdown_table(days, digits=4),
        "## PR-AUC por año\n\n" + ml_metrics.markdown_table(by_year(metrics), digits=4),
        "## PR-AUC por distribuidora en los años comunes\n\n" + ml_metrics.markdown_table(per_dist, digits=4),
        f"## PR-AUC con intervalo del 95 % (bootstrap por días, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals, digits=4),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=4),
        f"## Las 15 features con más peso en XGBoost de la variante {main_variant}, último año (SHAP medio "
        "absoluto)\n\n" + ml_metrics.markdown_table(
            shap.sort_values("valor", ascending=False).head(15)[["feature", "valor"]], digits=5),
    ]) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "comparacion": comparison, "dias": days,
                                         "distribuidoras": per_dist,
                                         "intervalos": intervals, "diferencias": differences,
                                         "importancia": importance, "filas": sizes, "entreno": trained})

    summary = metrics[(metrics["split"] == "cv_comun") & (metrics["segmento"] == "total")
                      & (metrics["etiqueta"] == MAIN_LABEL) & (metrics["metrica"] == "pr_auc")]
    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": version,
        "etiqueta": f"meteo_historia:{MAIN_LABEL}",
        "n_features": int(max(variant_rows["n_features"])),
        "modelos_json": json.dumps({m: {"params": s.params, **s.info} for m, s in scorers.items()}, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps({**config, "meteo_historia": cfg}, ensure_ascii=False, default=str),
        "resumen_json": json.dumps({r.modelo: r.valor for r in summary.itertuples()}),
        "splits_evaluados": ",".join(f"cv_{y}" for y in years),
        "artefactos_uri": "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)
    logger.info("Run %s completed: variants %s, common years %s", args.run_id, [v["nombre"] for v in variants],
                common)
    return 0


if __name__ == "__main__":
    sys.exit(main())
