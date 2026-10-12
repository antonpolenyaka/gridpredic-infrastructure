"""
Risk of the next day of every distribuidora: the crews of tomorrow.

The hourly models (job_ml_horas_riesgo, job_ml_meteo_historia) look 1 - 3
hours ahead with the weather already observed. The operator plans the crews
the day before, with what is known that evening. This job asks how well the
days with interruptions can be told apart with that, and how much a perfect
forecast of the weather of the day would add (the reanalysis of the day
itself, an upper bound of any real forecast).

Unit: distribuidora and day D (local time), from meteo_dia.desde to
meteo_dia.hasta, without the days of analisis_red.dias_excluidos.

Labels (fact_interrupciones_mt, variante principal, with the cause of
l2_silver.f_incidencia_causa): y_dia = 1 when an interruption starts in some
CT of the distribuidora on D; y_dia_meteo, the same without the causes of
causas_no_meteo (upstream, blackout, third parties...).

Features, every one known before D starts except the block dia:

- cal_*: day of the week (sine and cosine), weekend, holiday (national or
  Extremadura), day of the year (sine and cosine) and estacion.
- hist_*: incidents of the distribuidora known in Calser (conocido_ts)
  before D in the last 7, 30 and 365 days, and days since the last one.
- ayer_*: the weather of D - 1 from the reanalysis, by day: strongest gust
  and wind of the worst cell and of the mean of the cells weighted by their
  CT, rain of the day (worst cell and mean), hottest hour, coldest mean
  hour, mean humidity, lowest pressure and its biggest drop in 24 h, snow
  and the strongest gust of the whole area (the size of the storm).
- aviso_*: the warnings of AEMET for D (l2_silver.f_aviso_meteo) sent
  before aviso_corte_h of D - 1: highest level (1 yellow, 2 orange, 3 red)
  in the cells of the distribuidora, in total and for wind, rain,
  thunderstorms and snow. 0 without warning, null before the archive starts
  (18.06.2018). It is the only real forecast with a long archive.
- dia_*: the same daily weather as ayer_* for D itself, the perfect
  forecast.
- prev_*: the same daily weather for D from what ICON-EU forecast the day
  before (metp1_* of Gold, Previous Runs API, from January 2024). It has no
  history to train with, so it is never a block: the models of the variant
  perfecta_comun (calendar, history, yesterday and the dia_* the forecast
  also gives) are fitted on the reanalysis and then score the days of
  anios_prevision twice, with dia_* as it was and with dia_* replaced by
  prev_*, raw and mapped to the distribution of the reanalysis by quantiles
  (fitted on the other years of the forecast, without the labels). The
  difference is what is lost going from a perfect forecast to a real one.

Variants (meteo_dia.variantes): blocks and first day of training. Every year
Y is scored by models chosen and fitted only on the days before Y
(rolling_evaluation of job_ml_horas_riesgo), with the strongest gust of the
day before, of the day itself and the highest warning as references.

Metrics on the years every variant scored, against y_dia: PR-AUC of the
days, lift, the share of the days with interruptions that falls in the 5,
10 and 20 % of the days of each year with more risk (a crew on alert those
days), PR-AUC by distribuidora and the block bootstrap by dates of the
paired differences (meteo_dia.comparaciones).

Outputs: ml_runs and ml_metricas with run_id dia_*, resumen.md,
metricas.csv, comparacion.csv, diferencias.csv, anios.csv and filas.csv in
exportar_dir/<run_id>/.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_ml_horas_riesgo.py \\
        /app/jobs/04_ml/job_ml_riesgo_dia.py --config /app/config/04_ml/config_ml.json
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
from calendario import holiday_flags, season  # noqa: E402
from job_ml_horas_riesgo import distributor_weather, export, rolling_evaluation  # noqa: E402
from job_ml_meteo_historia import add_history, read_events  # noqa: E402
from job_ml_train import (  # noqa: E402
    METRICS_TABLE,
    RUNS_TABLE,
    SEASON_FEATURE,
    add_context,
    get_spark,
    load_config,
    logger,
    save_frame,
)

WARNINGS_TABLE = "l2_silver.f_aviso_meteo"
CELLS_TABLE = "l3_gold.map_ct_celda"
LABELS = ["y_dia", "y_dia_meteo"]
MAIN_LABEL = "y_dia"
WARNING_GROUPS = {"viento": ["VI"], "lluvia": ["PR"], "tormenta": ["TO"], "nieve": ["NE"]}
TRAINED = ("logistica", "xgboost", "random_forest")

# Daily weather of a distribuidora: name -> (hourly column of distributor_weather, aggregation over the day).
DAILY = {
    "racha_max": ("metd_max_met_racha_1h", "max"),
    "racha_media_max": ("metd_media_met_racha_1h", "max"),
    "viento_max": ("metd_max_met_viento_1h", "max"),
    "viento_media_max": ("metd_media_met_viento_1h", "max"),
    "lluvia_max": ("metd_max_met_lluvia_1h", "sum"),
    "lluvia_media": ("metd_media_met_lluvia_1h", "sum"),
    "lluvia_hora_max": ("metd_max_met_lluvia_1h", "max"),
    "temp_max": ("metd_max_met_temp_1h", "max"),
    "temp_media_min": ("metd_media_met_temp_1h", "min"),
    "humedad_media": ("metd_media_met_humedad_1h", "mean"),
    "presion_min": ("metd_media_met_presion_1h", "min"),
    "presion_caida_24h": ("metd_media_met_presion_delta_24h", "min"),
    "nieve_max": ("metd_max_met_nieve_24h", "max"),
    "region_racha_max": ("metd_max_met_region_racha_max_3h", "max"),
}

# The same daily weather from the forecast of ICON-EU made the day before
# (metp1_* of Gold). Snow, pressure and the size of the storm have no forecast.
FORECAST_DAILY = {
    "racha_max": ("metd_max_metp1_racha_1h", "max"),
    "racha_media_max": ("metd_media_metp1_racha_1h", "max"),
    "viento_max": ("metd_max_metp1_viento_1h", "max"),
    "viento_media_max": ("metd_media_metp1_viento_1h", "max"),
    "lluvia_max": ("metd_max_metp1_lluvia_1h", "sum"),
    "lluvia_media": ("metd_media_metp1_lluvia_1h", "sum"),
    "lluvia_hora_max": ("metd_max_metp1_lluvia_1h", "max"),
    "temp_max": ("metd_max_metp1_temp_1h", "max"),
    "temp_media_min": ("metd_media_metp1_temp_1h", "min"),
    "humedad_media": ("metd_media_metp1_humedad_1h", "mean"),
}

DEFAULTS = {
    "desde": "2017-01-08",
    "hasta": "2026-01-01",
    "anios_evaluacion": [2022, 2023, 2024, 2025],
    "dias_minimos_entreno": 365,
    "causas_no_meteo": ["aguas_arriba", "apagon", "terceros", "programada", "maniobra", "cliente_bt", "red_bt"],
    "retraso_conocido_dias": 7,
    "ventanas_historia_dias": [7, 30, 365],
    "horas_minimas_dia": 20,
    "aviso_corte_h": 20,
    "modelos": ["tasa_base", "logistica", "xgboost"],
    "referencias": {"naif_racha_ayer": "ayer_racha_max", "naif_racha_dia": "dia_racha_max",
                    "naif_aviso": "aviso_max_nivel"},
    "fraccion_validacion_interna": 0.25,
    "purga_horas": 0,
    "fracciones_top": [0.05, 0.1, 0.2],
    "anios_prevision": [2024, 2025],
    "cobertura_minima_prevision": 0.9,
    "bootstrap_reps": 500,
    "busqueda": {
        "logistica": [{"C": 0.003}, {"C": 0.01}, {"C": 0.03}, {"C": 0.1}],
        "xgboost": [{"max_depth": 2, "min_child_weight": 5, "learning_rate": 0.05, "subsample": 0.8,
                     "colsample_bytree": 0.8, "n_estimators": 1500, "early_stopping_rounds": 100, "verbose": 0},
                    {"max_depth": 3, "min_child_weight": 5, "learning_rate": 0.05, "subsample": 0.8,
                     "colsample_bytree": 0.8, "n_estimators": 1500, "early_stopping_rounds": 100, "verbose": 0}],
    },
    "variantes": [
        {"nombre": "calendario_historia", "desde": "2017-01-08", "bloques": ["calendario", "historia"]},
        {"nombre": "ayer", "desde": "2017-01-08", "bloques": ["calendario", "historia", "ayer"]},
        {"nombre": "perfecta", "desde": "2017-01-08", "bloques": ["calendario", "historia", "ayer", "dia"]},
    ],
    "comparaciones": [["ayer__logistica", "calendario_historia__logistica"],
                      ["perfecta__logistica", "ayer__logistica"]],
}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--sin-busqueda", action="store_true", help="Only the first configuration of every model")
    parser.add_argument("--variantes", default=None,
                        help="Comma separated names of meteo_dia.variantes to run (all by default)")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "dia_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def settings(config: dict) -> dict:
    cfg = {**DEFAULTS, **config.get("meteo_dia", {})}
    excluded = config.get("analisis_red", {}).get("dias_excluidos", {"2025-04-28": "apagon peninsular"})
    cfg["dias_excluidos"] = sorted(excluded)
    return cfg


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

def daily_weather(hourly: pd.DataFrame, min_hours: int, mapping: dict = None) -> pd.DataFrame:
    """
    (distribuidora_id, fecha, <mapping>): the weather of every day of every
    distribuidora from its hours (DAILY by default). A day with fewer than
    min_hours hours of weather is left null (a gap of the source is not a
    calm day).
    """
    d = hourly.assign(fecha=hourly["hora"].dt.floor("D"))
    present = {name: spec for name, spec in (mapping or DAILY).items() if spec[0] in d.columns}
    if not present:
        return d[["distribuidora_id", "fecha"]].drop_duplicates().reset_index(drop=True)
    days = d.groupby(["distribuidora_id", "fecha"]).agg(
        _horas=(present[next(iter(present))][0], "count"),
        **{name: (column, how) for name, (column, how) in present.items()},
    ).reset_index()
    short = days["_horas"] < int(min_hours)
    days.loc[short, list(present)] = np.nan
    return days.drop(columns="_horas")


def day_calendar(days: pd.Series) -> pd.DataFrame:
    """Calendar of the day: weekday, weekend, holiday, day of the year and season."""
    weekday = days.dt.dayofweek.to_numpy()
    day_of_year = days.dt.dayofyear.to_numpy()
    return pd.DataFrame({
        "cal_dia_semana_sin": np.sin(2 * np.pi * weekday / 7),
        "cal_dia_semana_cos": np.cos(2 * np.pi * weekday / 7),
        "cal_fin_de_semana": (weekday >= 5).astype("float64"),
        "cal_festivo": holiday_flags(days).astype("float64"),
        "cal_dia_anio_sin": np.sin(2 * np.pi * (day_of_year - 1) / 365.25),
        "cal_dia_anio_cos": np.cos(2 * np.pi * (day_of_year - 1) / 365.25),
        SEASON_FEATURE: season(days.dt.month).to_numpy(),
    }, index=days.index)


def day_labels(grid: pd.DataFrame, events: pd.DataFrame, excluded: list) -> pd.DataFrame:
    """y_dia and y_dia_meteo of every (distribuidora_id, fecha) of the grid."""
    starts = events.dropna(subset=["inicio_ts"]).assign(fecha=lambda e: pd.to_datetime(e["inicio_ts"]).dt.floor("D"))
    weather_related = ~starts["causa"].isin(set(excluded))

    for label, chosen in (("y_dia", starts), ("y_dia_meteo", starts[weather_related])):
        keys = chosen[["distribuidora_id", "fecha"]].drop_duplicates().assign(**{label: 1})
        grid = grid.merge(keys, on=["distribuidora_id", "fecha"], how="left")
        grid[label] = grid[label].fillna(0).astype("int8")

    return grid


def warning_days(warnings: pd.DataFrame, cells: pd.DataFrame, cutoff_h: int) -> tuple:
    """
    (aviso_* by (distribuidora_id, fecha), first day of the archive): the
    highest level of the warnings that cover some hour of the day in a cell
    of the distribuidora and were sent before cutoff_h of the day before.
    Cancellations and green levels do not count.
    """
    if warnings is None or warnings.empty:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"]), None

    w = warnings[(warnings["nivel_num"].fillna(0) >= 1)
                 & (warnings["tipo_mensaje"].fillna("Alert") != "Cancel")].copy()
    first_day = pd.to_datetime(warnings["enviado_ts"]).min().floor("D") + pd.Timedelta(days=1)
    if w.empty:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"]), first_day

    w["inicio_ts"] = pd.to_datetime(w["inicio_ts"])
    w["fin_ts"] = pd.to_datetime(w["fin_ts"])
    w["enviado_ts"] = pd.to_datetime(w["enviado_ts"])
    rows = []
    for record in w[["celda_id", "fenomeno_codigo", "nivel_num", "enviado_ts", "inicio_ts", "fin_ts"]].itertuples(
            index=False):
        first = record.inicio_ts.floor("D")
        # A warning that ends exactly at midnight does not cover the next day.
        last = (record.fin_ts - pd.Timedelta(seconds=1)).floor("D")
        for day in pd.date_range(first, max(first, last), freq="D"):
            # Known the evening before: sent before cutoff_h of D - 1.
            if record.enviado_ts <= day - pd.Timedelta(hours=24 - int(cutoff_h)):
                rows.append((record.celda_id, day, record.fenomeno_codigo, float(record.nivel_num)))

    if not rows:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"]), first_day

    per_cell = pd.DataFrame(rows, columns=["celda_id", "fecha", "fenomeno_codigo", "nivel_num"])
    joined = per_cell.merge(cells[["distribuidora_id", "celda_id"]].drop_duplicates(), on="celda_id")
    out = joined.groupby(["distribuidora_id", "fecha"])["nivel_num"].max().rename("aviso_max_nivel").to_frame()

    for name, codes in WARNING_GROUPS.items():
        part = joined[joined["fenomeno_codigo"].isin(codes)]
        out[f"aviso_{name}_nivel"] = part.groupby(["distribuidora_id", "fecha"])["nivel_num"].max()

    return out.reset_index(), first_day


def block_of(name: str) -> str:
    for prefix, block in (("cal_", "calendario"), ("hist_", "historia"), ("dist_", "distribuidora"),
                          ("ayer_", "ayer"), ("dia_", "dia"), ("aviso_", "avisos"), ("prev_", "prevision")):
        if name.startswith(prefix):
            return block
    return "calendario" if name == SEASON_FEATURE else "otros"


def build_dataset(spark, config: dict, cfg: dict) -> tuple:
    """(pdf, {block: feature names}, first day of the warnings) of the distribuidora-day history."""
    start, end = pd.Timestamp(cfg["desde"]), pd.Timestamp(cfg["hasta"])
    weather, names = distributor_weather(spark, {**config.get("meteo", {}), "activo": True},
                                         prefixes=("met_", "metp1_"))
    if weather is None:
        raise ValueError("No weather in Gold (meteo_celda_hora and map_ct_celda): nothing to learn from")

    wanted = [c for c in {spec[0] for spec in list(DAILY.values()) + list(FORECAST_DAILY.values())} if c in names]
    # The day before the first day needs its weather too.
    hourly = weather.where((F.col("hora") >= F.lit(start - pd.Timedelta(days=1))) & (F.col("hora") < F.lit(end)))
    hourly = hourly.select("distribuidora_id", "hora", *wanted).toPandas()
    hourly["distribuidora_id"] = hourly["distribuidora_id"].astype("int64")
    hourly["hora"] = pd.to_datetime(hourly["hora"])
    daily = daily_weather(hourly, int(cfg["horas_minimas_dia"]))
    weather_names = [c for c in daily.columns if c not in ("distribuidora_id", "fecha")]
    forecast = daily_weather(hourly, int(cfg["horas_minimas_dia"]), FORECAST_DAILY)
    forecast_names = [c for c in forecast.columns if c not in ("distribuidora_id", "fecha")]

    today = daily.rename(columns={c: f"dia_{c}" for c in weather_names})
    yesterday = daily.assign(fecha=daily["fecha"] + pd.Timedelta(days=1)).rename(
        columns={c: f"ayer_{c}" for c in weather_names})

    days = pd.date_range(start, end - pd.Timedelta(days=1), freq="D")
    dists = sorted(daily["distribuidora_id"].unique())
    pdf = pd.MultiIndex.from_product([dists, days], names=["distribuidora_id", "fecha"]).to_frame(index=False)
    pdf = pdf[~pdf["fecha"].isin(pd.to_datetime(cfg["dias_excluidos"]))].reset_index(drop=True)
    pdf = pdf.merge(today, on=["distribuidora_id", "fecha"], how="left")
    pdf = pdf.merge(yesterday, on=["distribuidora_id", "fecha"], how="left")
    if forecast_names:
        pdf = pdf.merge(forecast.rename(columns={c: f"prev_{c}" for c in forecast_names}),
                        on=["distribuidora_id", "fecha"], how="left")

    events, known = read_events(spark, cfg)
    pdf = day_labels(pdf, events, cfg["causas_no_meteo"])

    # History known when the day starts (an incident known at 00:00 counts).
    pdf["hora"] = pdf["fecha"]
    history = add_history(pdf, known, cfg["ventanas_historia_dias"])
    pdf["hist_dias_desde_ultima"] = pdf.pop("hist_horas_desde_ultima") / 24.0
    history = [h for h in history if h != "hist_horas_desde_ultima"] + ["hist_dias_desde_ultima"]

    cal = day_calendar(pdf["fecha"])
    pdf = pd.concat([pdf, cal], axis=1)

    for dist in dists:
        pdf[f"dist_{dist}"] = (pdf["distribuidora_id"] == dist).astype("float64")

    warnings_first = None
    warning_names = []
    if spark.catalog.tableExists(WARNINGS_TABLE) and spark.catalog.tableExists(CELLS_TABLE):
        warnings = spark.table(WARNINGS_TABLE).select(
            "celda_id", "fenomeno_codigo", "nivel_num", "tipo_mensaje", "enviado_ts", "inicio_ts", "fin_ts").toPandas()
        cells = spark.table(CELLS_TABLE).where(F.col("celda_id").isNotNull()).select(
            F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id").distinct().toPandas()
        table, warnings_first = warning_days(warnings, cells, int(cfg["aviso_corte_h"]))
        if warnings_first is not None:
            pdf = pdf.merge(table, on=["distribuidora_id", "fecha"], how="left")
            warning_names = ["aviso_max_nivel"] + [f"aviso_{name}_nivel" for name in WARNING_GROUPS]
            for name in warning_names:
                if name not in pdf.columns:
                    pdf[name] = np.nan
                # 0 without warning once the archive exists, null before it.
                pdf[name] = pdf[name].where(pdf["fecha"] < warnings_first, pdf[name].fillna(0.0))
            logger.info("Warnings of AEMET from %s", warnings_first.date())
    else:
        logger.info("%s not found: no warnings of AEMET in the daily model", WARNINGS_TABLE)

    pdf["anio"] = pdf["fecha"].dt.year
    pdf["ct_key"] = pdf["distribuidora_id"]
    pdf["split"] = "train"

    blocks = {}
    features = ([f"dist_{d}" for d in dists] + list(cal.columns) + history + [f"ayer_{c}" for c in weather_names]
                + [f"dia_{c}" for c in weather_names] + warning_names + [f"prev_{c}" for c in forecast_names])
    for name in features:
        blocks.setdefault(block_of(name), []).append(name)
    # The weather of the day that the forecast also gives: the fair reference of the real forecast.
    blocks["dia_comun"] = [f"dia_{c}" for c in forecast_names if c in weather_names]

    return pdf.sort_values(["distribuidora_id", "fecha"]).reset_index(drop=True), blocks, warnings_first


def describe(pdf: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (year, dist), g in pdf.groupby(["anio", "distribuidora_id"]):
        rows.append({"anio": int(year), "distribuidora": int(dist), "dias": len(g),
                     **{f"dias_{label}": int(g[label].sum()) for label in LABELS},
                     "con_meteo": float(g["dia_racha_max"].notna().mean()) if "dia_racha_max" in g else 0.0})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Variants and metrics
# ---------------------------------------------------------------------------

def variant_features(variant: dict, blocks: dict) -> list:
    chosen = list(blocks.get("distribuidora", []))
    for block in variant["bloques"]:
        chosen += blocks.get(block, [])
    return chosen


def variant_years(variant: dict, cfg: dict) -> list:
    first = pd.Timestamp(variant["desde"]) + pd.Timedelta(days=int(cfg["dias_minimos_entreno"]))
    return [int(y) for y in cfg["anios_evaluacion"] if pd.Timestamp(f"{int(y)}-01-01") >= first]


def top_share(y: np.ndarray, score: np.ndarray, year: np.ndarray, share: float, seed: int = 0) -> float:
    """Part of the positive days that falls in the share of the days with the highest score of each year."""
    rng = np.random.default_rng(seed)
    tie = rng.random(len(y))
    hits = positives = 0
    for value in np.unique(year):
        mask = year == value
        order = np.lexsort((-tie[mask], -np.nan_to_num(score[mask], nan=-np.inf)))
        k = int(round(mask.sum() * share))
        hits += int(y[mask][order[:k]].sum())
        positives += int(y[mask].sum())
    return hits / positives if positives else float("nan")


def day_metrics(scored: pd.DataFrame, columns: list, shares: list, seed: int = 0) -> pd.DataFrame:
    """Long table of metrics, the columns of ml_metrics.evaluate (modelo, etiqueta, segmento, ...)."""
    rows = []
    segments = [("total", "total", np.ones(len(scored), dtype=bool))]
    segments += [("distribuidora", str(d), (scored["distribuidora_id"] == d).to_numpy())
                 for d in sorted(scored["distribuidora_id"].unique())]
    years = scored["anio"].to_numpy()

    for column in columns:
        model = column[2:]
        s_all = scored[column].to_numpy(dtype=np.float64)
        for label in LABELS:
            y_all = scored[label].to_numpy()
            for segment, value, mask in segments:
                y, s = y_all[mask], s_all[mask]
                n, pos = int(mask.sum()), int(y.sum())
                prevalence = float(y.mean()) if n else float("nan")
                ap = ml_metrics.average_precision(y, s)
                found = [("prevalencia", prevalence), ("pr_auc", ap), ("roc_auc", ml_metrics.roc_auc(y, s)),
                         ("lift_pr_auc", ap / prevalence if prevalence else float("nan"))]
                found += [(f"dias_positivos_en_top_{int(round(share * 100))}pct",
                           top_share(y, s, years[mask], float(share), seed)) for share in shares]
                rows += [(model, label, segment, value, metric, float(v), n, pos) for metric, v in found]

    return pd.DataFrame(rows, columns=["modelo", "etiqueta", "segmento", "valor_segmento", "metrica", "valor",
                                       "n_filas", "n_positivos"])


def comparison_table(metrics: pd.DataFrame, shares: list) -> pd.DataFrame:
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == MAIN_LABEL)]
    wanted = ["pr_auc", "lift_pr_auc", "roc_auc"] + [f"dias_positivos_en_top_{int(round(s * 100))}pct" for s in shares]
    table = rows[rows["metrica"].isin(wanted)].pivot_table(index="modelo", columns="metrica", values="valor")
    table = table[[c for c in wanted if c in table.columns]].reset_index()
    split = table["modelo"].str.split("__", n=1, expand=True)
    table.insert(0, "variante", split[0])
    table["modelo"] = split[1]
    meteo = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == "y_dia_meteo")
                    & (metrics["metrica"] == "pr_auc")].set_index("modelo")["valor"]
    table["pr_auc_y_dia_meteo"] = (table["variante"] + "__" + table["modelo"]).map(meteo)
    # A reference (a single feature, the base rate) scores the same in every
    # variant that has its feature: it is shown once.
    trained = table["modelo"].isin(TRAINED)
    table = pd.concat([table[trained], table[~trained].drop_duplicates("modelo")])
    return table.sort_values("pr_auc", ascending=False).reset_index(drop=True)


def forecast_comparison(metrics: pd.DataFrame, shares: list) -> pd.DataFrame:
    """One row per model and weather of the day (reanalysis, forecast, mapped forecast, only yesterday)."""
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == MAIN_LABEL)]
    wanted = ["pr_auc", "lift_pr_auc", "roc_auc"] + [f"dias_positivos_en_top_{int(round(s * 100))}pct" for s in shares]
    table = rows[rows["metrica"].isin(wanted)].pivot_table(index="modelo", columns="metrica", values="valor")
    table = table[[c for c in wanted if c in table.columns]].reset_index()
    parts = table["modelo"].str.split("__", n=1, expand=True)
    yesterday = parts[0] == "ayer"
    table.insert(0, "tiempo_del_dia", np.where(yesterday, "solo_ayer", parts[1]))
    table["modelo"] = np.where(yesterday, parts[1], parts[0])
    return table.sort_values("pr_auc", ascending=False).reset_index(drop=True)


def by_year(metrics: pd.DataFrame) -> pd.DataFrame:
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == MAIN_LABEL)
                   & (metrics["metrica"] == "pr_auc") & metrics["split"].str.startswith("cv_")
                   & (metrics["split"] != "cv_comun")]
    table = rows.pivot_table(index="modelo", columns="split", values="valor").reset_index()
    table.columns = [str(c).replace("cv_", "pr_auc ") for c in table.columns]
    return once(table)


def once(table: pd.DataFrame) -> pd.DataFrame:
    """The rows of the references only once (the first variant that has them)."""
    model = table["modelo"].str.split("__", n=1).str[-1]
    trained = model.isin(TRAINED)
    return pd.concat([table[trained], table[~trained].loc[~model[~trained].duplicated()]]).sort_values(
        "modelo").reset_index(drop=True)


def by_distribuidora(metrics: pd.DataFrame) -> pd.DataFrame:
    rows = metrics[(metrics["split"] == "cv_comun") & (metrics["segmento"] == "distribuidora")
                   & (metrics["etiqueta"] == MAIN_LABEL) & (metrics["metrica"] == "pr_auc")]
    table = rows.pivot_table(index="modelo", columns="valor_segmento", values="valor")
    table.columns = [f"pr_auc {c}" for c in table.columns]
    return once(table.reset_index())


def quantile_map(fit_forecast, fit_reanalysis, values) -> np.ndarray:
    """
    Forecast values taken to the distribution of the reanalysis: the value of
    the reanalysis at the quantile the forecast has in the fitting days. The
    model learnt the scale of the reanalysis (ICON-EU gives stronger gusts
    than ECMWF IFS). Without enough days to fit, the values go as they are.
    """
    a = np.asarray(fit_forecast, dtype=np.float64)
    b = np.asarray(fit_reanalysis, dtype=np.float64)
    a, b = a[~np.isnan(a)], b[~np.isnan(b)]
    values = np.asarray(values, dtype=np.float64)
    if len(a) < 30 or len(b) < 30:
        return values
    qs = np.linspace(0.0, 1.0, 201)
    qa, qb = np.quantile(a, qs), np.quantile(b, qs)
    # Repeated quantiles (many dry days) would make np.interp jump.
    qa = qa + np.arange(len(qa)) * 1e-9
    return np.interp(values, qa, qb)


def forecast_years(pdf: pd.DataFrame, cfg: dict) -> list:
    """Years of anios_prevision where the forecast covers enough days of every distribuidora."""
    if "prev_racha_max" not in pdf.columns:
        return []
    minimum = float(cfg["cobertura_minima_prevision"])
    years = []
    for year in [int(y) for y in cfg["anios_prevision"]]:
        part = pdf[pdf["anio"] == year]
        covered = part.groupby("distribuidora_id")["prev_racha_max"].apply(lambda v: float(v.notna().mean()))
        if len(part) and len(covered) and covered.min() >= minimum:
            years.append(year)
        else:
            logger.warning("Year %s left out of the forecast comparison: coverage by distribuidora %s", year,
                           covered.round(3).to_dict())
    return years


def forecast_evaluation(pdf: pd.DataFrame, variant: dict, blocks: dict, config: dict, cfg: dict, years: list,
                        only_first: bool) -> pd.DataFrame:
    """
    The models of the variant fitted on the reanalysis of the days before
    each year, scoring the days of the year with the weather of the day from
    the reanalysis (p_<model>__reanalisis), from the forecast of ICON-EU of
    the day before (__prevision) and from that forecast mapped by quantiles
    (__prevision_cuantiles). Only the days with a forecast.
    """
    features = variant_features(variant, blocks)
    swap = [c[len("dia_"):] for c in blocks.get("dia_comun", [])]
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]
    parts = []

    for year in years:
        scored, _, scorers, _ = rolling_evaluation(rows, features, MAIN_LABEL, config, cfg, [year], only_first,
                                                   names=cfg["modelos"])
        target = rows[rows["anio"] == year].copy()
        others = rows[rows["anio"].isin([y for y in years if y != year])]
        fitting = others if len(others) else target
        raw, mapped = target.copy(), target.copy()

        for name in swap:
            raw[f"dia_{name}"] = target[f"prev_{name}"].to_numpy()
            values = np.full(len(target), np.nan)
            for dist in target["distribuidora_id"].unique():
                mask = (target["distribuidora_id"] == dist).to_numpy()
                fit = fitting[fitting["distribuidora_id"] == dist]
                values[mask] = quantile_map(fit[f"prev_{name}"], fit[f"dia_{name}"],
                                            target.loc[mask, f"prev_{name}"])
            mapped[f"dia_{name}"] = values

        out = target[["distribuidora_id", "fecha", "anio"] + LABELS].copy()
        for source, frame in (("reanalisis", target), ("prevision", raw), ("prevision_cuantiles", mapped)):
            X = frame[features].to_numpy(dtype=np.float32, na_value=np.nan)
            for model, scorer in scorers.items():
                out[f"p_{model}__{source}"] = scorer.score(X)
        out["con_prevision"] = target[[f"prev_{n}" for n in swap]].notna().all(axis=1).to_numpy()
        parts.append(out)

    if not parts:
        return pd.DataFrame()
    found = pd.concat(parts, ignore_index=True)
    return found[found["con_prevision"]].drop(columns="con_prevision").reset_index(drop=True)


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
    shares = [float(s) for s in cfg["fracciones_top"]]
    version = f"meteo_dia_{cfg['desde']}_{cfg['hasta']}"

    variants = cfg["variantes"]
    if args.variantes:
        wanted = {v.strip() for v in args.variantes.split(",") if v.strip()}
        variants = [v for v in variants if v["nombre"] in wanted]

    pdf, blocks, warnings_first = build_dataset(spark, config, cfg)
    sizes = describe(pdf)
    logger.info("Daily dataset: %s rows, blocks %s\n%s", len(pdf), {k: len(v) for k, v in blocks.items()},
                ml_metrics.markdown_table(sizes, digits=3))

    for variant in [v for v in variants if any(not blocks.get(b) for b in v["bloques"])]:
        logger.warning("Variant %s skipped: no features of the blocks %s", variant["nombre"],
                       [b for b in variant["bloques"] if not blocks.get(b)])
    variants = [v for v in variants if all(blocks.get(b) for b in v["bloques"])]
    if not variants:
        raise ValueError("No variant has all its blocks")

    # The models of the hours get the settings of the days: no purge between
    # days, the grids of meteo_dia.busqueda.
    run_cfg = {**cfg, "purga_horas": float(cfg.get("purga_horas", 0))}
    outs, search, last = [], [], {}
    for variant in variants:
        years = variant_years(variant, cfg)
        if not years:
            logger.warning("Variant %s skipped: no year of evaluation has enough days before it", variant["nombre"])
            continue
        features = variant_features(variant, blocks)
        rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]
        logger.info("Variant %s: %s features, training from %s, years %s", variant["nombre"], len(features),
                    variant["desde"], years)
        scored, found, scorers, _ = rolling_evaluation(rows, features, MAIN_LABEL, config, run_cfg, years,
                                                       args.sin_busqueda, names=cfg["modelos"])
        out = scored[["distribuidora_id", "fecha", "anio"] + LABELS].copy()
        for column in [c for c in scored.columns if c.startswith("p_")]:
            out[f"p_{variant['nombre']}__{column[2:]}"] = scored[column].to_numpy()
        outs.append(out)
        search += [{**row, "variante": variant["nombre"]} for row in found]
        last[variant["nombre"]] = scorers

    if not outs:
        raise ValueError("No variant could be scored")

    scored = outs[0]
    for out in outs[1:]:
        scored = scored.merge(out.drop(columns=["anio"] + LABELS), on=["distribuidora_id", "fecha"], how="outer")
        scored["anio"] = scored["fecha"].dt.year
    scored = scored.drop(columns=LABELS).merge(pdf[["distribuidora_id", "fecha"] + LABELS],
                                               on=["distribuidora_id", "fecha"], how="left")
    columns = [c for c in scored.columns if c.startswith("p_")]
    years = sorted(scored["anio"].unique().tolist())
    common = [y for y in years if scored.loc[scored["anio"] == y, columns].notna().all(axis=None)]

    all_metrics = []
    for year in years:
        part = scored[scored["anio"] == year]
        present = [c for c in columns if part[c].notna().all()]
        all_metrics.append(add_context(day_metrics(part, present, shares, seed), args.run_id, version, f"cv_{year}"))
    pooled = scored[scored["anio"].isin(common)]
    if common:
        all_metrics.append(add_context(day_metrics(pooled, columns, shares, seed), args.run_id, version, "cv_comun"))
    metrics = pd.concat(all_metrics, ignore_index=True)
    comparison = comparison_table(metrics[metrics["split"] == "cv_comun"], shares) if common else pd.DataFrame()

    reps = int(cfg["bootstrap_reps"])
    pairs = [(f"p_{a}", f"p_{b}") for a, b in cfg["comparaciones"] if f"p_{a}" in columns and f"p_{b}" in columns]
    intervals, differences = pd.DataFrame(), pd.DataFrame()
    if common and pairs:
        named = sorted({name for pair in pairs for name in pair})
        intervals, replicas = ml_metrics.bootstrap_pr_auc(
            pooled[MAIN_LABEL].to_numpy(), np.ones(len(pooled)), pooled["fecha"].to_numpy(),
            {name[2:]: pooled[name].to_numpy() for name in named}, reps, seed)
        differences = ml_metrics.paired_differences(replicas, [(a[2:], b[2:]) for a, b in pairs])

    # The real forecast: the models of variante_prevision with the forecast of
    # ICON-EU in place of the weather of the day, on the days it covers.
    forecast_table, forecast_differences = pd.DataFrame(), pd.DataFrame()
    reference = next((v for v in variants if v["nombre"] == cfg.get("variante_prevision", "perfecta_comun")), None)
    years_forecast = forecast_years(pdf, cfg) if reference is not None else []
    if years_forecast:
        found = forecast_evaluation(pdf, reference, blocks, config, run_cfg, years_forecast, args.sin_busqueda)
        before = [c for c in columns if c.startswith("p_ayer__")]
        if not found.empty and before:
            found = found.merge(scored[["distribuidora_id", "fecha"] + before], on=["distribuidora_id", "fecha"],
                                how="left")
        forecast_columns = [c for c in found.columns if c.startswith("p_")]
        if not found.empty:
            forecast_metrics = add_context(day_metrics(found, forecast_columns, shares, seed), args.run_id, version,
                                           "prevision_comun")
            metrics = pd.concat([metrics, forecast_metrics], ignore_index=True)
            forecast_table = forecast_comparison(forecast_metrics, shares)
            wanted = [(f"p_{m}__prevision_cuantiles", f"p_{m}__reanalisis") for m in ("logistica", "xgboost")]
            wanted += [(f"p_{m}__prevision", f"p_{m}__reanalisis") for m in ("logistica", "xgboost")]
            wanted += [(f"p_{m}__prevision_cuantiles", f"p_ayer__{m}") for m in ("logistica", "xgboost")]
            wanted = [(a, b) for a, b in wanted if a in found.columns and b in found.columns]
            if wanted:
                named = sorted({name for pair in wanted for name in pair})
                _, replicas = ml_metrics.bootstrap_pr_auc(
                    found[MAIN_LABEL].to_numpy(), np.ones(len(found)), found["fecha"].to_numpy(),
                    {name[2:]: found[name].to_numpy() for name in named}, reps, seed)
                forecast_differences = ml_metrics.paired_differences(replicas, [(a[2:], b[2:]) for a, b in wanted])
            logger.info("Real forecast of ICON-EU on %s days of %s", len(found), years_forecast)

    save_frame(spark, metrics, METRICS_TABLE, args.run_id)

    variant_rows = pd.DataFrame([{
        "variante": v["nombre"], "desde": v["desde"], "bloques": "+".join(v["bloques"]),
        "anios": ",".join(str(y) for y in variant_years(v, cfg)), "n_features": len(variant_features(v, blocks)),
    } for v in variants])
    per_dist = by_distribuidora(metrics)
    totals = sizes.groupby("anio")[[f"dias_{label}" for label in LABELS]].sum().reset_index()
    top = f"dias_positivos_en_top_{int(round(shares[1] * 100))}pct" if len(shares) > 1 else ""
    warning_text = (f"Los avisos de AEMET cuentan desde el {warnings_first:%d.%m.%Y}, si se enviaron antes de las "
                    f"{int(cfg['aviso_corte_h'])}:00 del día anterior." if warnings_first is not None
                    else "Sin avisos de AEMET en Silver.")

    report = "\n\n".join([
        f"# Riesgo del día siguiente: {args.run_id}",
        f"Unidad: distribuidora y día, de {cfg['desde']} a {cfg['hasta']} sin los días excluidos "
        f"({', '.join(cfg['dias_excluidos'])}). y_dia = 1 si ese día empieza una interrupción de la variante principal "
        f"en algún CT de la distribuidora; y_dia_meteo, lo mismo sin las causas {', '.join(cfg['causas_no_meteo'])}. "
        "Todo lo que usa el modelo se conoce la tarde anterior salvo el bloque dia, el tiempo del propio día sacado del "
        f"reanálisis, que hace de previsión perfecta. {warning_text} Cada año lo puntúa un modelo que solo ha visto "
        f"los días anteriores; las cifras comparadas son de los años que puntúan todas las variantes "
        f"({', '.join(str(y) for y in common) or 'ninguno'}).",
        "## Días con interrupción por año\n\n" + ml_metrics.markdown_table(totals, digits=0),
        "## Variantes\n\n" + ml_metrics.markdown_table(variant_rows, digits=0),
        f"## Comparación en los años comunes\n\n{top} es la parte de los días con avería que caen en el "
        f"{int(round(shares[1] * 100)) if len(shares) > 1 else ''} % de días con más riesgo de cada año.\n\n"
        + ml_metrics.markdown_table(comparison, digits=4),
        "## PR-AUC por año\n\n" + ml_metrics.markdown_table(by_year(metrics), digits=4),
        "## PR-AUC por distribuidora en los años comunes\n\n" + ml_metrics.markdown_table(per_dist, digits=4),
        f"## PR-AUC con intervalo del 95 % (bootstrap por fechas, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals, digits=4),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=4),
    ] + ([
        f"## Con la previsión real del día anterior ({', '.join(str(y) for y in years_forecast)})\n\n"
        f"Los modelos de la variante {reference['nombre']} se entrenan con el reanálisis y puntúan los días con "
        "previsión tres veces: con el tiempo del día sacado del reanálisis (reanalisis), con lo que preveía ICON-EU el "
        "día anterior (prevision) y con esa previsión llevada por cuantiles a la escala del reanálisis "
        "(prevision_cuantiles). ayer es la variante que solo sabe el tiempo del día anterior.\n\n"
        + ml_metrics.markdown_table(forecast_table, digits=4)
        + "\n\n" + ml_metrics.markdown_table(forecast_differences, digits=4),
    ] if not forecast_table.empty else [])) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "comparacion": comparison, "anios": by_year(metrics),
                                         "distribuidoras": per_dist, "intervalos": intervals,
                                         "diferencias": differences, "filas": sizes, "prevision": forecast_table,
                                         "prevision_diferencias": forecast_differences})

    summary = metrics[(metrics["split"] == "cv_comun") & (metrics["segmento"] == "total")
                      & (metrics["etiqueta"] == MAIN_LABEL) & (metrics["metrica"] == "pr_auc")]
    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": version,
        "etiqueta": f"meteo_dia:{MAIN_LABEL}",
        "n_features": int(variant_rows["n_features"].max()),
        "modelos_json": json.dumps({v: {m: {"params": s.params, **s.info} for m, s in sc.items()}
                                    for v, sc in last.items()}, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps({**config, "meteo_dia": cfg}, ensure_ascii=False, default=str),
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
