"""
Risk of the next day of every distribuidora, every evening (dag_riesgo_manana).

What job_ml_riesgo_dia measured on the past, in operation: the variant
riesgo_manana.variante of meteo_dia.variantes (operativa: calendar, history,
the warnings of AEMET, the weather of the day that ICON-EU forecasts, the
gusts against the climate of each cell, and the strongest gust and the rain
of the day before) with the model riesgo_manana.modelo (logistica), fitted
every evening on every labelled day, scores the day to predict D with what
job_landing_vivo left in <riesgo_manana.landing>/<D as YYYYMMDD>:

- prevision/<cell>.json, the forecast of ICON-EU: the weather of D as the
  model learnt it from the reanalysis (dia_* of the block dia_comun), the
  gusts of D against the same climate of each cell that Gold uses (the 98th
  percentile of the reanalysis, met_racha_1h / met_racha_clima_1h) and the
  run of hot days up to D (ext_*), and the strongest gust and the rain of
  the day before (ayer_racha_max, ayer_lluvia_max). The forecast goes as it
  comes, as the research job scored it, not mapped by quantiles. The cells
  without a forecast are left out of the means of their distribuidora
  (cobertura_prevision is the share of its CTs in cells with a forecast).
- avisos/<area>.tar, the warnings of AEMET in force: every warning landed
  counts, all of them are known when the job runs. Without an answer of
  AEMET the day is scored without warnings and the row says so
  (avisos_disponibles false).
- The calendar of D and the incidents known in Calser before D. When the
  copy of Calser in the lakehouse ends more than margen_etiquetas_dias
  before D, the history is left empty and the model takes the median of the
  training days (historia_al_dia false): zero incidents in the last days
  would read as a calm network.

Training days: from the first day of the variant to the last day with
labels (the last incident of Calser minus margen_etiquetas_dias), weather of
the reanalysis in Gold and warnings in the archive. The configuration of the
model is chosen on the last fraccion_validacion_interna of those days and
fitted again on all of them, as every year of the rolling evaluation of
job_ml_riesgo_dia.

Output, one row per distribuidora in riesgo_manana.tabla (the rows of D are
replaced): the probability that an interruption starts on D, the base rate
of the training days of the distribuidora and how many times the
probability is above it, its percentile among the scores of those days and
the level of riesgo_manana.niveles (muy alto, alto, medio, and bajo below),
the reasons (the GROUPS of features that raise the probability most against
an average day, with the factor they multiply its odds by; none below the
base rate), the municipalities with the strongest forecast gust against the
climate of their cell, and the main inputs. Also a row in ml_runs and
resumen.md and prediccion.csv in exportar_dir/<run_id>/.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \\
        /app/jobs/04_ml/job_ml_riesgo_manana.py --config /app/config/04_ml/config_ml.json --fecha 2026-10-14
"""

import argparse
import glob
import json
import math
import os
import re
import sys
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
for folder in (HERE, os.path.join(HERE, "..", "00_landing"), os.path.join(HERE, "..", "01_bronze"),
               os.path.join(HERE, "..", "02_silver")):
    if folder not in sys.path:
        sys.path.insert(0, folder)

import ml_metrics  # noqa: E402
from job_bronze_datos_publicos import parse_aemet_archive, polygon_points  # noqa: E402
from job_bronze_meteo_openmeteo import merge_local_hours, payload_frame  # noqa: E402
from job_landing_meteo_openmeteo import filesystem  # noqa: E402
from job_landing_vivo import day_folder, target_day  # noqa: E402
from job_ml_horas_riesgo import export, refit, to_arrays, train  # noqa: E402
from job_ml_meteo_historia import FACTS_TABLE, add_history, read_events  # noqa: E402
from job_ml_riesgo_dia import (  # noqa: E402
    CELLS_TABLE,
    FORECAST_DAILY,
    MAIN_LABEL,
    WARNING_DETAILS,
    WARNING_GROUPS,
    WARNINGS_TABLE,
    build_dataset,
    daily_weather,
    day_calendar,
    extremes,
    forecast_extremes,
    settings,
    variant_features,
    warning_days,
)
from job_ml_train import RUNS_TABLE, get_spark, load_config, logger, save_frame  # noqa: E402
from job_silver_contexto_publico import WARNING_LEVELS, inside, local_wall_clock  # noqa: E402

INCIDENTS_TABLE = "l2_silver.f_incidencia"
WEATHER_TABLE = "l3_gold.meteo_celda_hora"

DEFAULTS = {
    "variante": "operativa",
    "modelo": "logistica",
    "zona_horaria": "Europe/Madrid",
    "landing": "s3://datalake/00_landing/vivo",
    "tabla": "l3_gold.riesgo_dia_siguiente",
    "margen_etiquetas_dias": 7,
    "niveles": {"muy alto": 0.95, "alto": 0.9, "medio": 0.75},
    "motivos": 3,
    "zonas": 3,
}

# Variable of the forecast of Open-Meteo -> hourly column of Gold without its prefix, physical range of Silver.
LIVE_VARIABLES = {
    "wind_gusts_10m": ("racha_1h", 0.0, 300.0),
    "wind_speed_10m": ("viento_1h", 0.0, 250.0),
    "precipitation": ("lluvia_1h", 0.0, 200.0),
    "snowfall": ("nieve_1h", 0.0, 100.0),
    "temperature_2m": ("temp_1h", -30.0, 55.0),
    "relative_humidity_2m": ("humedad_1h", 0.0, 100.0),
}

# warning_days keeps the warnings sent before cutoff_h of the day before. Every warning landed is known when the
# job runs, the evening before or later: 48 h lets in the ones sent up to the end of D.
LIVE_CUTOFF_H = 48

LEVEL_NAMES = {1: "amarillo", 2: "naranja", 3: "rojo"}

# The features told as one reason. The contributions of features that move together (the gust and the wind, the
# rain of the day and of its wettest hour) are added, so the opposite signs that a regression gives to correlated
# features cancel out. The features of no group (the distribuidora) are never a reason.
GROUPS = {
    "viento": ["dia_racha_max", "dia_racha_media_max", "dia_viento_max", "dia_viento_media_max"],
    "clima": ["ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku"],
    "lluvia": ["dia_lluvia_max", "dia_lluvia_media", "dia_lluvia_hora_max"],
    "calor": ["dia_temp_max", "ext_dias_calor"],
    "frio": ["dia_temp_media_min"],
    "humedad": ["dia_humedad_media"],
    "aviso": ["aviso_max_nivel", "aviso_viento_nivel", "aviso_lluvia_nivel", "aviso_tormenta_nivel",
              "aviso_nieve_nivel"],
    "ayer": ["ayer_racha_max", "ayer_lluvia_max"],
    "historia": ["hist_incidencias_7d", "hist_incidencias_30d", "hist_incidencias_365d", "hist_dias_desde_ultima"],
    "semana": ["cal_dia_semana_sin", "cal_dia_semana_cos"],
    "fin_de_semana": ["cal_fin_de_semana"],
    "festivo": ["cal_festivo"],
    "epoca": ["cal_dia_anio_sin", "cal_dia_anio_cos", "estacion"],
}
GROUP_OF = {feature: group for group, features in GROUPS.items() for feature in features}
WARNING_KINDS = [("aviso_viento_nivel", "viento"), ("aviso_lluvia_nivel", "lluvia"),
                 ("aviso_tormenta_nivel", "tormentas"), ("aviso_nieve_nivel", "nieve")]
MODEL_NAMES = {"logistica": "regresión logística", "xgboost": "XGBoost", "random_forest": "random forest"}

# Columns of riesgo_manana.tabla, in order.
OUTPUT_COLUMNS = [
    ("fecha", "date"), ("distribuidora_id", "bigint"), ("distribuidora", "string"), ("probabilidad", "double"),
    ("tasa_base", "double"), ("veces_tasa_base", "double"), ("percentil", "double"), ("nivel", "string"),
    ("motivos", "string"), ("zonas", "string"), ("racha_max_kmh", "double"), ("racha_clima_max", "double"),
    ("lluvia_mm", "double"), ("temp_max_c", "double"), ("aviso_max_nivel", "double"),
    ("cobertura_prevision", "double"), ("avisos_disponibles", "boolean"), ("historia_al_dia", "boolean"),
    ("calser_hasta", "date"), ("entrenado_hasta", "date"), ("dias_entreno", "bigint"), ("variante", "string"),
    ("modelo", "string"), ("params_json", "string"), ("prevision_pedida_utc", "string"),
    ("avisos_pedidos_utc", "string"), ("run_id", "string"), ("creado_ts", "timestamp"),
]
# Output column -> live feature.
INPUTS = {"racha_max_kmh": "dia_racha_max", "racha_clima_max": "ext_racha_clima_max", "lluvia_mm": "dia_lluvia_max",
          "temp_max_c": "dia_temp_max", "aviso_max_nivel": "aviso_max_nivel"}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--fecha", default="", help="Day to predict, YYYY-MM-DD (tomorrow by default)")
    parser.add_argument("--landing", default=None, help="Folder of the live Landing (riesgo_manana.landing)")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "manana_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def risk_settings(config: dict) -> dict:
    return {**DEFAULTS, **config.get("riesgo_manana", {})}


def number(value: float, digits: int) -> str:
    """A number with a decimal comma, as the texts of the project."""
    return f"{value:.{digits}f}".replace(".", ",")


def moment(stamp: str) -> str:
    """An ISO time in UTC (pedido_utc of the Landing) as 13.10.2026 18:31 UTC."""
    if not stamp:
        return "?"
    return f"{pd.Timestamp(stamp).tz_convert('UTC'):%d.%m.%Y %H:%M} UTC" if pd.Timestamp(stamp).tzinfo else stamp


# ---------------------------------------------------------------------------
# The live Landing
# ---------------------------------------------------------------------------

class LocalFiles:
    """The calls of s3fs that the job makes, on a local folder (the tests, a run outside the lakehouse)."""

    def glob(self, pattern: str) -> list:
        return sorted(glob.glob(pattern))

    def open(self, path: str, mode: str = "r"):
        return open(path, mode) if "b" in mode else open(path, mode, encoding="utf-8")

    def exists(self, path: str) -> bool:
        return os.path.exists(path)


def live_filesystem(path: str):
    return filesystem() if path.startswith("s3://") else LocalFiles()


def read_json(fs, path: str) -> dict:
    with fs.open(path, "r") as file:
        return json.load(file)


def forecast_hours(fs, folder: str, zone: str) -> tuple:
    """
    (celda_id, hora and the columns of LIVE_VARIABLES, latest pedido_utc):
    the forecast of every cell in local wall clock, every row the hour that
    ends at hora as in Bronze, out of range values set to null as in Silver.
    """
    frames, asked = [], []
    for path in sorted(fs.glob(f"{folder}/prevision/*.json")):
        payload = read_json(fs, path)
        hourly = payload.get("hourly") or {}
        variables = [v for v in LIVE_VARIABLES if v in hourly]
        if not hourly.get("time") or not variables or "_peticion" not in payload:
            logger.warning("Forecast file without hours left out: %s", path)
            continue
        frames.append(merge_local_hours(payload_frame(payload, variables, zone), variables))
        asked.append(payload["_peticion"].get("pedido_utc"))

    columns = [name for name, _, _ in LIVE_VARIABLES.values()]
    if not frames:
        return pd.DataFrame(columns=["celda_id", "hora"] + columns), None

    out = pd.concat(frames, ignore_index=True)
    for variable, (name, low, high) in LIVE_VARIABLES.items():
        values = (pd.to_numeric(out.pop(variable), errors="coerce") if variable in out
                  else pd.Series(np.nan, index=out.index))
        out[name] = values.where(values.between(low, high))
    stamps = [a for a in asked if a]
    return out, max(stamps) if stamps else None


def gold_columns(hours: pd.DataFrame, climate: pd.Series) -> pd.DataFrame:
    """
    The forecast with the names of Gold: met_* (read for today and the days
    before: the weather of the day before D and the run of heat) and metp1_*
    (read for D: the forecast made the day before), the same values, plus
    the gust over the 98th percentile of its cell (climate, by celda_id) and
    max(0, ratio - 1)^3.
    """
    out = hours[["celda_id", "hora"]].copy()
    p98 = out["celda_id"].map(climate).astype("float64")
    for prefix in ("met", "metp1"):
        for name, _, _ in LIVE_VARIABLES.values():
            out[f"{prefix}_{name}"] = hours[name].to_numpy(dtype="float64")
        ratio = out[f"{prefix}_racha_1h"] / p98.where(p98 > 0)
        out[f"{prefix}_racha_clima_1h"] = ratio
        out[f"{prefix}_racha_ku_1h"] = np.clip(ratio - 1.0, 0.0, None) ** 3
    return out


def distributor_hours(hours: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    """
    (distribuidora_id, hora, metd_media_*, metd_max_*), as distributor_weather
    of job_ml_horas_riesgo does in Spark: the mean of the cells weighted by
    their CTs over the cells with a value, and the worst cell. weights:
    distribuidora_id, celda_id, n_ct.
    """
    columns = [c for c in hours.columns if c.startswith(("met_", "metp1_"))]
    keys = ["distribuidora_id", "hora"]
    joined = hours.merge(weights[["distribuidora_id", "celda_id", "n_ct"]], on="celda_id")
    out = joined.groupby(keys)[columns].max().add_prefix("metd_max_")
    parts = {}
    for column in columns:
        value = joined[column]
        parts[f"s_{column}"] = (value * joined["n_ct"]).fillna(0.0)
        parts[f"w_{column}"] = joined["n_ct"].where(value.notna(), 0.0)
    sums = pd.concat([joined[keys], pd.DataFrame(parts)], axis=1).groupby(keys).sum()
    for column in columns:
        weight = sums[f"w_{column}"]
        out[f"metd_media_{column}"] = sums[f"s_{column}"] / weight.where(weight > 0)
    return out.reset_index()


def weather_features(hourly: pd.DataFrame, day: pd.Timestamp, cfg: dict) -> pd.DataFrame:
    """
    (distribuidora_id, dia_*, ext_*, ayer_*) of D from the hours of the
    forecast by distribuidora, with the functions of the research job: the
    daily weather of D from the forecast (FORECAST_DAILY), its extremes with
    the run of heat of the days before, and the weather of D - 1.
    """
    min_hours = int(cfg["horas_minimas_dia"])
    heat = float(cfg["calor_umbral_c"])
    daily = daily_weather(hourly, min_hours)
    forecast = daily_weather(hourly, min_hours, FORECAST_DAILY)
    extreme = extremes(hourly, daily, heat, min_hours)
    forecast_extreme = forecast_extremes(hourly, forecast, extreme, heat, min_hours)

    keys = ["distribuidora_id", "fecha"]
    today = forecast[forecast["fecha"] == day].rename(
        columns={c: f"dia_{c}" for c in forecast.columns if c not in keys})
    ext = forecast_extreme[forecast_extreme["fecha"] == day].rename(
        columns={c: c[len("prev_"):] for c in forecast_extreme.columns if c.startswith("prev_")})
    before = daily[daily["fecha"] == day - pd.Timedelta(days=1)].rename(
        columns={c: f"ayer_{c}" for c in daily.columns if c not in keys})

    out = today.drop(columns="fecha")
    for part in (ext, before):
        out = out.merge(part.drop(columns="fecha"), on="distribuidora_id", how="outer")
    return out


def warning_rows(fs, folder: str, areas: list) -> tuple:
    """(CAP rows of every area landed, whether AEMET answered for every area, latest pedido_utc)."""
    rows, answered, asked = [], True, []
    for area in areas:
        archive, empty = f"{folder}/avisos/{area}.tar", f"{folder}/avisos/{area}.vacio.json"
        if fs.exists(archive):
            with fs.open(archive, "rb") as file:
                rows += parse_aemet_archive(file.read())
            stamp = f"{folder}/avisos/{area}.json"
        elif fs.exists(empty):
            stamp = empty
        else:
            answered = False
            continue
        if fs.exists(stamp):
            asked.append(read_json(fs, stamp).get("pedido_utc"))
    stamps = [a for a in asked if a]
    return rows, answered, max(stamps) if stamps else None


def warnings_in_cells(rows: list, cells: list) -> pd.DataFrame:
    """
    The warnings of the CAP rows in every cell whose centre is inside their
    polygon, with the columns of l2_silver.f_aviso_meteo that warning_days
    reads, as job_silver_contexto_publico builds them.
    """
    columns = ["celda_id", "fenomeno_codigo", "nivel_num", "tipo_mensaje", "enviado_ts", "inicio_ts", "fin_ts",
               "parametro"]
    centres = []
    for cell in cells:
        try:
            lat, lon = (float(part) for part in str(cell).split("_"))
        except ValueError:
            continue
        centres.append((cell, lat, lon))

    found = []
    for record in rows:
        if not record.get("identificador") or not record.get("inicio") or not record.get("fin"):
            continue
        points = polygon_points(record.get("poligono"))
        if len(points) < 3:
            continue
        found += [{**record, "celda_id": cell} for cell, lat, lon in centres if inside(lat, lon, points)]

    if not found:
        return pd.DataFrame(columns=columns)
    pdf = pd.DataFrame(found).drop_duplicates(["identificador", "zona", "celda_id"])
    for column, source in (("enviado_ts", "enviado"), ("inicio_ts", "inicio"), ("fin_ts", "fin")):
        pdf[column] = local_wall_clock(pdf[source])
    pdf["fenomeno_codigo"] = pdf["fenomeno"].fillna("").astype(str).str.split(";").str[0].str.strip().str.upper()
    pdf["nivel_num"] = pdf["nivel"].fillna("").astype(str).str.strip().str.lower().map(WARNING_LEVELS)
    pdf = pdf[pdf["inicio_ts"].notna() & pdf["fin_ts"].notna()]
    return pdf[columns].reset_index(drop=True)


def warnings_of_day(warnings: pd.DataFrame, day: pd.Timestamp) -> pd.DataFrame:
    """The warnings in force some hour of D (yellow or more, not cancelled)."""
    if warnings.empty:
        return warnings
    return warnings[(warnings["nivel_num"].fillna(0) >= 1) & (warnings["tipo_mensaje"].fillna("Alert") != "Cancel")
                    & (warnings["inicio_ts"] < day + pd.Timedelta(days=1)) & (warnings["fin_ts"] > day)]


def warning_features(warnings: pd.DataFrame, cells: pd.DataFrame, day: pd.Timestamp, dists: list) -> pd.DataFrame:
    """(distribuidora_id, aviso_*, aviso_det_*) of D, 0 without warning."""
    names = ["aviso_max_nivel"] + [f"aviso_{name}_nivel" for name in WARNING_GROUPS] + WARNING_DETAILS
    out = pd.DataFrame({"distribuidora_id": dists})
    table, _ = warning_days(warnings, cells, LIVE_CUTOFF_H)
    if not table.empty:
        out = out.merge(table[table["fecha"] == day].drop(columns="fecha"), on="distribuidora_id", how="left")
    for name in names:
        out[name] = out[name].fillna(0.0) if name in out.columns else 0.0
    return out[["distribuidora_id"] + names]


# ---------------------------------------------------------------------------
# Lakehouse
# ---------------------------------------------------------------------------

def cell_weights(spark) -> pd.DataFrame:
    """(distribuidora_id, celda_id, n_ct): the CTs of every distribuidora in every cell (map_ct_celda)."""
    return (spark.table(CELLS_TABLE).where(F.col("celda_id").isNotNull())
            .groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id")
            .agg(F.count(F.lit(1)).cast("double").alias("n_ct")).toPandas())


def cell_places(spark) -> pd.DataFrame:
    """(distribuidora_id, celda_id, municipio, n_ct): the municipalities of the CTs of every cell."""
    table = spark.table(CELLS_TABLE).where(F.col("celda_id").isNotNull())
    place = F.col("municipio_nombre") if "municipio_nombre" in table.columns else F.col("celda_id")
    return (table.groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id",
                          F.coalesce(place, F.col("celda_id")).alias("municipio"))
            .agg(F.count(F.lit(1)).alias("n_ct")).toPandas())


def cell_climate(spark) -> pd.Series:
    """The 98th percentile of the hourly gusts of every cell that Gold divides by (met_racha_1h / met_racha_clima_1h)."""
    weather = spark.table(WEATHER_TABLE)
    if not {"met_racha_1h", "met_racha_clima_1h"} <= set(weather.columns):
        logger.warning("%s has no met_racha_clima_1h: no gusts against the climate", WEATHER_TABLE)
        return pd.Series(dtype="float64")
    climate = (weather.where((F.col("met_racha_clima_1h") > 0) & F.col("met_racha_1h").isNotNull())
               .groupBy("celda_id").agg(F.avg(F.col("met_racha_1h") / F.col("met_racha_clima_1h")).alias("p98"))
               .toPandas())
    return climate.set_index("celda_id")["p98"]


def calser_until(spark, events: pd.DataFrame, day: pd.Timestamp) -> pd.Timestamp:
    """The last incident of the copy of Calser before D: any incident of l2_silver.f_incidencia or interruption."""
    last = events.loc[events["inicio_ts"] < day, "inicio_ts"].max()
    if spark.catalog.tableExists(INCIDENTS_TABLE):
        found = (spark.table(INCIDENTS_TABLE).where(F.col("inicio_ts") < F.lit(day.to_pydatetime()))
                 .agg(F.max("inicio_ts").alias("ultimo")).first()["ultimo"])
        if found is not None:
            last = max(pd.Timestamp(found), last) if pd.notna(last) else pd.Timestamp(found)
    return last


def warnings_until(spark) -> pd.Timestamp:
    """The last message of the archive of warnings in Silver (NaT without it)."""
    if not spark.catalog.tableExists(WARNINGS_TABLE):
        return pd.NaT
    found = spark.table(WARNINGS_TABLE).agg(F.max("enviado_ts").alias("ultimo")).first()["ultimo"]
    return pd.Timestamp(found) if found is not None else pd.NaT


def distributor_names(spark) -> dict:
    """{distribuidora_id: name}: the Calser database of its interruptions without the prefix."""
    facts = spark.table(FACTS_TABLE)
    if "source_database" not in facts.columns:
        return {}
    rows = facts.select(F.col("distribuidora_id").cast("long").alias("d"), "source_database").distinct().collect()
    return {int(r["d"]): str(r["source_database"]).replace("Calser_", "") for r in rows if r["source_database"]}


def save_prediction(spark, pdf: pd.DataFrame, table: str, day: pd.Timestamp):
    """The rows of D in the table, replacing the ones of an earlier run for the same day."""
    schema = ", ".join(f"{name} {kind}" for name, kind in OUTPUT_COLUMNS)
    df = spark.createDataFrame(pdf[[name for name, _ in OUTPUT_COLUMNS]], schema=schema)
    df = df.withColumn("audit_loaded_at", F.current_timestamp())
    writer = df.write.format("delta").option("mergeSchema", "true")
    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", f"fecha = DATE'{day:%Y-%m-%d}'").saveAsTable(table)
    else:
        writer.mode("overwrite").saveAsTable(table)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def fit(rows: pd.DataFrame, features: list, name: str, config: dict, cfg: dict) -> tuple:
    """
    (scorer, search rows, matrix of the training days): the configurations of
    the model compared on the last share of the days, the best one fitted
    again on all of them.
    """
    seed = int(config.get("semilla", 0))
    n_jobs = int(config.get("n_jobs", -1))
    days = np.sort(rows["fecha"].unique())
    cut = days[int(len(days) * (1 - float(cfg.get("fraccion_validacion_interna", 0.25))))]
    X, y = to_arrays(rows[rows["fecha"] < cut], features, MAIN_LABEL)
    Xv, yv = to_arrays(rows[rows["fecha"] >= cut], features, MAIN_LABEL)
    inner = {"X": X, "y": y, "w": np.ones(len(y)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
             "feature_names": features}
    logger.info("Choosing %s on %s days (%s with interruptions) and %s days from %s (%s)", name, len(y), int(y.sum()),
                len(yv), pd.Timestamp(cut).date(), int(yv.sum()))
    scorers, search = train([name], config, cfg, inner, float(y.mean()), False, references=False)

    Xa, ya = to_arrays(rows, features, MAIN_LABEL)
    full = {"X": Xa, "y": ya, "w": np.ones(len(ya)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
            "feature_names": features}
    return refit(name, scorers[name], full, seed, n_jobs), search, Xa


def contributions(scorer, X: np.ndarray):
    """
    (rows, features): what every feature adds to the log odds against an
    average day. For the logistic regression, the value imputed, put on the
    log scale and standardised times its coefficient, with the indicator of a
    missing value added to its feature; for XGBoost, its SHAP values. None
    for other models.
    """
    model = getattr(scorer, "estimator", None)
    if hasattr(model, "named_steps"):
        imputer = model.named_steps["imputar"]
        z = model.named_steps["escalar"].transform(model.named_steps["log"].transform(imputer.transform(X)))
        terms = z * model.named_steps["modelo"].coef_[0]
        n = X.shape[1]
        out = terms[:, :n].copy()
        indicator = getattr(imputer, "indicator_", None)
        if indicator is not None:
            for j, feature in enumerate(indicator.features_):
                out[:, feature] += terms[:, n + j]
        return out
    if hasattr(model, "get_booster"):
        import xgboost

        return model.get_booster().predict(xgboost.DMatrix(X), pred_contribs=True)[:, :-1]
    return None


def listed(words: list) -> str:
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " y " + words[-1]


def reason_text(group: str, values: dict) -> str:
    """What a group of features says of the day, from the values of its features (null when unknown)."""
    def known(name):
        return name in values and values[name] is not None and math.isfinite(values[name])

    def value(name, digits):
        return number(values[name], digits)

    if group == "viento":
        return f"racha máxima prevista de {value('dia_racha_max', 0)} km/h" if known("dia_racha_max") \
            else "viento previsto"
    if group == "clima":
        return (f"racha prevista de {value('ext_racha_clima_max', 2)} veces el percentil 98 de su celda"
                if known("ext_racha_clima_max") else "racha frente al clima de cada celda")
    if group == "lluvia":
        text = f"{value('dia_lluvia_max', 1)} mm de lluvia prevista" if known("dia_lluvia_max") else "lluvia prevista"
        if known("dia_lluvia_hora_max") and values["dia_lluvia_hora_max"] > 0:
            text += f" (hasta {value('dia_lluvia_hora_max', 1)} mm en una hora)"
        return text
    if group == "calor":
        text = f"máxima prevista de {value('dia_temp_max', 0)} °C" if known("dia_temp_max") else "calor"
        if known("ext_dias_calor") and values["ext_dias_calor"] >= 2:
            text += f", {value('ext_dias_calor', 0)} días seguidos de calor"
        return text
    if group == "frio":
        return f"mínima media prevista de {value('dia_temp_media_min', 0)} °C" if known("dia_temp_media_min") \
            else "temperatura mínima"
    if group == "humedad":
        return f"humedad media prevista del {value('dia_humedad_media', 0)} %" if known("dia_humedad_media") \
            else "humedad"
    if group == "aviso":
        level = LEVEL_NAMES.get(int(round(values["aviso_max_nivel"]))) if known("aviso_max_nivel") else None
        if not level:
            return "sin avisos de AEMET"
        kinds = [kind for name, kind in WARNING_KINDS if known(name) and values[name] >= 1]
        return f"aviso {level} de AEMET" + (f" por {listed(kinds)}" if kinds else "")
    if group == "ayer":
        parts = ([f"racha de {value('ayer_racha_max', 0)} km/h"] if known("ayer_racha_max") else []) + (
            [f"{value('ayer_lluvia_max', 1)} mm de lluvia"] if known("ayer_lluvia_max") else [])
        return "el día anterior, " + listed(parts) if parts else "el tiempo del día anterior"
    if group == "historia":
        if not (known("hist_incidencias_7d") and known("hist_incidencias_30d")):
            return "historia de incidencias sin dato"
        return (f"{value('hist_incidencias_7d', 0)} incidencias en los últimos 7 días y "
                f"{value('hist_incidencias_30d', 0)} en 30")
    if group == "fin_de_semana":
        return "fin de semana" if known("cal_fin_de_semana") and values["cal_fin_de_semana"] >= 0.5 \
            else "día entre semana"
    if group == "festivo":
        return "festivo" if known("cal_festivo") and values["cal_festivo"] >= 0.5 else "día laborable"
    return {"semana": "día de la semana", "epoca": "época del año"}.get(group, group)


def reasons(terms: np.ndarray, values: np.ndarray, features: list, k: int) -> str:
    """The k groups of features that raise the odds most (more than 5 %), with the factor they multiply them by."""
    totals = {}
    for j, feature in enumerate(features):
        if feature in GROUP_OF:
            totals[GROUP_OF[feature]] = totals.get(GROUP_OF[feature], 0.0) + float(terms[j])
    chosen = sorted([(total, group) for group, total in totals.items() if total > math.log(1.05)], reverse=True)
    named = {feature: float(values[j]) for j, feature in enumerate(features)}
    return "; ".join(f"{reason_text(group, named)} (x{number(math.exp(total), 1)})" for total, group in chosen[:int(k)])


def level_of(percentile: float, levels: dict) -> str:
    for name, threshold in sorted(levels.items(), key=lambda item: -float(item[1])):
        if percentile >= float(threshold):
            return name
    return "bajo"


def windiest_places(hours: pd.DataFrame, places: pd.DataFrame, day: pd.Timestamp, k: int) -> dict:
    """
    {distribuidora_id: text}: the k cells of the distribuidora with the
    strongest forecast gust of D against their climate (the strongest gust
    when there is no climate), named by the municipality with more CTs in
    each.
    """
    of_day = hours[hours["hora"].dt.floor("D") == day]
    if of_day.empty or places.empty:
        return {}
    per_cell = of_day.groupby("celda_id").agg(ratio=("metp1_racha_clima_1h", "max"),
                                              racha=("metp1_racha_1h", "max")).reset_index()
    top = places.sort_values(["distribuidora_id", "celda_id", "n_ct", "municipio"], ascending=[True, True, False, True])
    top = top.drop_duplicates(["distribuidora_id", "celda_id"]).merge(per_cell, on="celda_id")
    texts = {}
    for dist, part in top[top["racha"].notna()].groupby("distribuidora_id"):
        part = part.sort_values(["ratio", "racha"], ascending=False, na_position="last").head(int(k))
        texts[int(dist)] = "; ".join(
            f"{row.municipio} ({number(row.racha, 0)} km/h"
            + (f", {number(row.ratio, 2)} veces su p98)" if pd.notna(row.ratio) else ")")
            for row in part.itertuples())
    return texts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def report_text(day: pd.Timestamp, out: pd.DataFrame, info: dict) -> str:
    shown = pd.DataFrame({
        "distribuidora": out["distribuidora"],
        "nivel": out["nivel"],
        "probabilidad": (100 * out["probabilidad"]).map(lambda v: number(v, 1) + " %"),
        "veces la tasa base": out["veces_tasa_base"].map(lambda v: number(v, 1) if pd.notna(v) else ""),
        "percentil": (100 * out["percentil"]).map(lambda v: number(v, 0)),
        "motivos": out["motivos"],
    })
    params = ", ".join(f"{key} = {str(value).replace('.', ',')}" for key, value in info["params"].items())
    inputs = pd.DataFrame({
        "distribuidora": out["distribuidora"],
        "racha máxima (km/h)": out["racha_max_kmh"].map(lambda v: number(v, 0) if pd.notna(v) else ""),
        "veces el p98": out["racha_clima_max"].map(lambda v: number(v, 2) if pd.notna(v) else ""),
        "lluvia (mm)": out["lluvia_mm"].map(lambda v: number(v, 1) if pd.notna(v) else ""),
        "máxima (°C)": out["temp_max_c"].map(lambda v: number(v, 0) if pd.notna(v) else ""),
        "aviso": out["aviso_max_nivel"].map(lambda v: LEVEL_NAMES.get(int(v), "no") if pd.notna(v) else ""),
        "zonas con más racha frente a su clima": out["zonas"],
    })
    notes = []
    if not info["avisos_disponibles"]:
        notes.append("AEMET no ha dado los avisos: el día se puntúa sin avisos, como si no hubiera ninguno.")
    if not info["historia_al_dia"]:
        notes.append(f"La copia de Calser llega hasta el {info['calser_hasta']:%d.%m.%Y}: la historia de incidencias "
                     "no está al día y el modelo toma la mediana de los días de entrenamiento.")
    for row in out[out["cobertura_prevision"] < 0.999].itertuples():
        notes.append(f"{row.distribuidora}: solo el {number(100 * row.cobertura_prevision, 0)} % de sus CT está en "
                     "celdas con previsión.")
    levels = ", ".join(f"{name} desde el percentil {number(100 * float(v), 0)}"
                       for name, v in sorted(info["niveles"].items(), key=lambda item: -float(item[1])))
    return "\n\n".join([
        f"# Riesgo de mañana: {day:%d.%m.%Y}",
        f"Previsión de ICON-EU pedida el {moment(info['prevision_pedida_utc'])}; {info['avisos_texto']}. Modelo: "
        f"variante {info['variante']}, {MODEL_NAMES.get(info['modelo'], info['modelo'])}"
        + (f" ({params})" if params else "")
        + f", entrenado con {info['dias']} días de distribuidora del {info['desde']:%d.%m.%Y} al "
        f"{info['entrenado_hasta']:%d.%m.%Y} ({info['positivos']} con alguna interrupción).",
        "La probabilidad es la de que empiece al menos una interrupción en algún CT de la distribuidora. El percentil "
        f"la compara con la de los días de entrenamiento de la misma distribuidora y da el nivel ({levels}). Entre "
        "paréntesis, por cuánto multiplica cada motivo la razón de probabilidades frente a un día medio.",
        ml_metrics.markdown_table(shown),
        "## Lo que se prevé\n\n" + ml_metrics.markdown_table(inputs),
    ] + (["## Notas\n\n" + "\n".join(f"- {n}" for n in notes)] if notes else [])) + "\n"


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    rcfg = risk_settings(config)
    cfg = settings(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    zone = rcfg["zona_horaria"]
    day = pd.Timestamp(target_day(args.fecha, zone))
    folder = day_folder(args.landing or rcfg["landing"], day.date())
    fs = live_filesystem(folder)
    logger.info("Risk of %s from %s", day.date(), folder)

    variant = next((v for v in cfg["variantes"] if v["nombre"] == rcfg["variante"]), None)
    if variant is None:
        raise ValueError(f"Variant {rcfg['variante']} not in meteo_dia.variantes")

    # The forecast first: without it there is nothing to score.
    cells_hours, forecast_asked = forecast_hours(fs, folder, zone)
    if cells_hours.empty:
        raise FileNotFoundError(f"No forecast of ICON-EU for {day.date()} in {folder}/prevision: run "
                                "job_landing_vivo.py --fuente prevision first")

    spark = get_spark(config)
    cfg["hasta"] = f"{day:%Y-%m-%d}"
    pdf, blocks, _ = build_dataset(spark, config, cfg)
    missing_blocks = [b for b in variant["bloques"] if not blocks.get(b)]
    if missing_blocks:
        raise ValueError(f"No features of the blocks {missing_blocks} of the variant {variant['nombre']}")
    features = variant_features(variant, blocks)

    # The last day whose label, weather and warnings are known.
    events, known = read_events(spark, cfg)
    calser = calser_until(spark, events, day)
    margin = pd.Timedelta(days=int(rcfg["margen_etiquetas_dias"]))
    ends = [calser.floor("D") - margin, day - pd.Timedelta(days=1)]
    if "dia_racha_max" in pdf.columns:
        ends.append(pdf.loc[pdf["dia_racha_max"].notna(), "fecha"].max())
    if "avisos" in variant["bloques"]:
        ends.append(warnings_until(spark).floor("D"))
    end = min(e for e in ends if pd.notna(e))
    rows = pdf[(pdf["fecha"] >= pd.Timestamp(variant["desde"])) & (pdf["fecha"] <= end)].reset_index(drop=True)
    logger.info("Training days of %s up to %s: %s (%s with interruptions); Calser up to %s", variant["nombre"],
                end.date(), len(rows), int(rows[MAIN_LABEL].sum()), calser)

    name = rcfg["modelo"]
    scorer, search, X_train = fit(rows, features, name, config, cfg)
    train_scores = scorer.score(X_train)

    # Tomorrow, from the live Landing.
    dists = sorted(rows["distribuidora_id"].unique().tolist())
    weights = cell_weights(spark)
    hours = gold_columns(cells_hours, cell_climate(spark))
    live = pd.DataFrame({"distribuidora_id": dists})
    live = live.merge(weather_features(distributor_hours(hours, weights), day, cfg), on="distribuidora_id", how="left")

    areas = rcfg.get("avisos", {}).get("areas", ["esp"])
    cap_rows, answered, warnings_asked = warning_rows(fs, folder, areas)
    warnings = warnings_in_cells(cap_rows, sorted(weights["celda_id"].unique()))
    live = live.merge(warning_features(warnings, weights, day, dists), on="distribuidora_id", how="left")

    live["hora"] = day
    history = add_history(live, known, cfg["ventanas_historia_dias"])
    live["hist_dias_desde_ultima"] = live.pop("hist_horas_desde_ultima") / 24.0
    history = [h for h in history if h != "hist_horas_desde_ultima"] + ["hist_dias_desde_ultima"]
    up_to_date = bool(pd.notna(calser) and calser >= day - margin)
    if not up_to_date:
        live[history] = np.nan
    live = pd.concat([live, day_calendar(pd.Series(day, index=live.index))], axis=1)
    for dist in dists:
        live[f"dist_{dist}"] = (live["distribuidora_id"] == dist).astype("float64")

    absent = [f for f in features if f not in live.columns]
    if absent:
        logger.warning("Features without a value tomorrow (the model takes the median of the training days): %s",
                       absent)
        for feature in absent:
            live[feature] = np.nan
    day_weather = [f for f in features if f.startswith("dia_")]
    if day_weather and live[day_weather].isna().all(axis=1).any():
        raise ValueError(f"The forecast of {folder} does not cover {day.date()} for every distribuidora")

    X = live[features].to_numpy(dtype=np.float32, na_value=np.nan)
    probability = scorer.score(X).astype("float64")
    terms = contributions(scorer, X)
    of_day = cells_hours[(cells_hours["hora"].dt.floor("D") == day) & cells_hours["racha_1h"].notna()]
    covered = weights[weights["celda_id"].isin(set(of_day["celda_id"]))]
    coverage = covered.groupby("distribuidora_id")["n_ct"].sum() / weights.groupby("distribuidora_id")["n_ct"].sum()
    places = windiest_places(hours, cell_places(spark), day, int(rcfg["zonas"]))
    names = distributor_names(spark)

    out_rows = []
    for i, dist in enumerate(live["distribuidora_id"].tolist()):
        mask = (rows["distribuidora_id"] == dist).to_numpy()
        base = float(rows.loc[mask, MAIN_LABEL].mean())
        percentile = float((train_scores[mask] <= probability[i]).mean())
        # Below the base rate nothing is worth telling: what raises it is less than what lowers it.
        if terms is None:
            told = ""
        elif probability[i] < base:
            told = "ninguno, por debajo de un día medio"
        else:
            told = reasons(terms[i], X[i].astype("float64"), features, int(rcfg["motivos"]))
        out_rows.append({
            "fecha": day.date(),
            "distribuidora_id": int(dist),
            "distribuidora": names.get(int(dist), str(dist)),
            "probabilidad": float(probability[i]),
            "tasa_base": base,
            "veces_tasa_base": float(probability[i] / base) if base > 0 else float("nan"),
            "percentil": percentile,
            "nivel": level_of(percentile, rcfg["niveles"]),
            "motivos": told,
            "zonas": places.get(int(dist), ""),
            **{column: float(live.loc[i, source]) if source in live.columns else float("nan")
               for column, source in INPUTS.items()},
            "cobertura_prevision": float(coverage.get(dist, 0.0)),
            "avisos_disponibles": bool(answered),
            "historia_al_dia": up_to_date,
            "calser_hasta": calser.date() if pd.notna(calser) else None,
            "entrenado_hasta": end.date(),
            "dias_entreno": int(mask.sum()),
            "variante": variant["nombre"],
            "modelo": name,
            "params_json": json.dumps(scorer.params, default=str),
            "prevision_pedida_utc": forecast_asked,
            "avisos_pedidos_utc": warnings_asked,
            "run_id": args.run_id,
            "creado_ts": created,
        })
    out = pd.DataFrame(out_rows)
    save_prediction(spark, out, rcfg["tabla"], day)

    in_force = warnings_of_day(warnings, day)
    count = len(in_force[["fenomeno_codigo", "inicio_ts", "fin_ts"]].drop_duplicates())
    if not answered:
        warning_text = "sin respuesta de AEMET"
    elif not count:
        warning_text = "ningún aviso de AEMET para el día en las celdas de la red"
    else:
        warning_text = (f"{count} aviso{'s' if count > 1 else ''} de AEMET para el día en las celdas de la red, el "
                        f"mayor {LEVEL_NAMES[int(in_force['nivel_num'].max())]}")
    info = {
        "variante": variant["nombre"], "modelo": name, "params": scorer.params, "dias": len(rows),
        "positivos": int(rows[MAIN_LABEL].sum()), "desde": pd.Timestamp(variant["desde"]), "entrenado_hasta": end,
        "niveles": rcfg["niveles"], "avisos_disponibles": answered, "historia_al_dia": up_to_date,
        "calser_hasta": calser, "prevision_pedida_utc": forecast_asked, "avisos_texto": warning_text,
    }
    folder_id = re.sub(r"[^A-Za-z0-9_.-]", "_", args.run_id)
    export(config, folder_id, report_text(day, out, info), {"prediccion": out})

    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": f"riesgo_manana_{day:%Y-%m-%d}",
        "etiqueta": f"riesgo_manana:{MAIN_LABEL}",
        "n_features": len(features),
        "modelos_json": json.dumps({variant["nombre"]: {name: {"params": scorer.params, **scorer.info}}}, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps({"riesgo_manana": rcfg, "variante": variant, "features": features},
                                  ensure_ascii=False, default=str),
        "resumen_json": json.dumps({str(r.distribuidora_id): {"probabilidad": r.probabilidad, "nivel": r.nivel}
                                    for r in out.itertuples()}, ensure_ascii=False),
        "splits_evaluados": "",
        "artefactos_uri": "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)
    logger.info("Risk of %s saved in %s:\n%s", day.date(), rcfg["tabla"],
                out[["distribuidora", "probabilidad", "percentil", "nivel", "motivos"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
