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
  copy of Calser in the lakehouse ends more than historia_margen_dias (7)
  before D (historia_al_dia false), zero incidents in the last days would
  read as a calm network, so tomorrow is scored with the median of the
  training days in the history of incidents, the model fitted with it
  (riesgo_manana.historia_desfasada "mediana"). With "sin_historia" the
  model of the evening is fitted without the history block. The notebook 3v
  of investigacion-continua.md measured the two with the variants
  operativa_historia_mediana and operativa_sin_historia of
  job_ml_riesgo_dia: the median loses nothing against the real history and
  fitting without the block costs about 0.01 of PR-AUC, hence the default.

Training days: from the first day of the variant to the last day with
labels (the last incident of Calser minus margen_etiquetas_dias, 45 since
17.10.2026: an interruption gets its incident weeks later, when it is
processed, and the control of Gold measures 41 - 43 days of labels still
missing at the end of each copy in the Distribuidora 1 and 2), weather of
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
climate of their cell, and the main inputs. Also a row in ml_runs and, in
exportar_dir/<run_id>/: resumen.md, prediccion.csv, zonas.csv, the
coefficients of the model when it is linear (coeficientes.csv,
ml_models.linear_terms), the model as fitted (modelo.pkl), riesgo.json and
parte_guardia.xlsx (the two the dashboard gets too).

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \\
        /app/jobs/04_ml/job_ml_riesgo_manana.py --config /app/config/04_ml/config_ml.json --fecha 2026-10-14

A past day can be played again as if it were tomorrow, without the live
Landing: --desde-gold takes the forecast ICON-EU made the day before from
Gold (metp1_* of meteo_celda_hora, the reanalysis for the days before) and
the warnings of AEMET sent before meteo_dia.aviso_corte_h of the day before
from Silver, trains only with the days before and writes the same outputs;
with --solo-archivo the dashboard keeps riesgo.json and parte_guardia.xlsx
of the live day and only gets the dated copies (riesgo_<day>.json), which
index.html shows with ?dia=YYYYMMDD. For a demo of a storm (the forecast of
the evening of 27.01.2026):

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \
        /app/jobs/04_ml/job_ml_riesgo_manana.py --config /app/config/04_ml/config_ml.json \
        --fecha 2026-01-28 --desde-gold --solo-archivo --run-id demo_20260128

No --py-files: the job puts its folder and the Landing, Bronze and Silver
folders in sys.path, the arithmetic is pandas in the driver and Spark only
reads and writes Delta.
"""

import argparse
import hashlib
import json
import tempfile
import math
import os
import pickle
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
import ml_models  # noqa: E402
from job_bronze_datos_publicos import parse_aemet_archive, polygon_points  # noqa: E402
from job_bronze_meteo_openmeteo import merge_local_hours, payload_frame  # noqa: E402
from job_landing_meteo_openmeteo import LocalFiles, live_filesystem  # noqa: E402, F401 (LocalFiles: the tests)
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
    cell_weights,
    nao_features,
    daily_weather,
    day_calendar,
    extremes,
    forecast_extremes,
    settings,
    variant_features,
    warning_days,
)
from job_ml_train import RUNS_TABLE, environment, get_spark, load_config, logger, save_frame, store_artifacts  # noqa: E402
import aviso_riesgo  # noqa: E402
import riesgo_zonas as rz  # noqa: E402
from job_silver_contexto_publico import WARNING_LEVELS, inside, local_wall_clock  # noqa: E402

INCIDENTS_TABLE = "l2_silver.f_incidencia"
WEATHER_TABLE = "l3_gold.meteo_celda_hora"
DIM_TABLE = "l3_gold.dim_ct"
ZONES_TABLE = "l3_gold.riesgo_zona_siguiente"

DEFAULTS = {
    "variante": "operativa",
    "variante_pasado_manana": "operativa_48h",
    "tabla_pasado_manana": "l3_gold.riesgo_pasado_manana",
    "modelo": "logistica",
    "zona_horaria": "Europe/Madrid",
    "landing": "s3://datalake/00_landing/vivo",
    "tabla": "l3_gold.riesgo_dia_siguiente",
    "tabla_zonas": ZONES_TABLE,
    "margen_etiquetas_dias": 45,
    "historia_margen_dias": 7,
    "niveles": {"muy alto": 0.95, "alto": 0.9, "medio": 0.75},
    "motivos": 3,
    "zonas": 3,
    "dashboard_dir": "/app/dashboard/data",
    "riesgo_zonas": {"brigadas": {}, "velocidad_kmh": 60.0, "factor_carretera": 1.3, "bases_fijas": {},
                     "franja_parte_pico": 0.8, "franja_calma_kmh": 40.0,
                     "rutas": {"activo": False, "api": "https://router.project-osrm.org", "timeout_s": 60,
                               "cache": "s3://datalake/00_landing/vivo/rutas"}},
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

LEVEL_NAMES = aviso_riesgo.LEVEL_NAMES
guard_advice = aviso_riesgo.guard_advice

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
    ("lluvia_mm", "double"), ("temp_max_c", "double"), ("aviso_max_nivel", "double"), ("aviso_viento_nivel", "double"),
    ("aviso_tormenta_nivel", "double"), ("guardia_reforzada", "boolean"), ("guardia_motivo", "string"),
    ("cobertura_prevision", "double"), ("avisos_disponibles", "boolean"), ("historia_al_dia", "boolean"),
    ("calser_hasta", "date"), ("entrenado_hasta", "date"), ("dias_entreno", "bigint"), ("variante", "string"),
    ("modelo", "string"), ("params_json", "string"), ("prevision_pedida_utc", "string"),
    ("avisos_pedidos_utc", "string"), ("run_id", "string"), ("creado_ts", "timestamp"),
    ("incidencias_esperadas", "double"), ("ct_esperados", "double"), ("clientes_esperados", "double"),
    ("franja_inicio", "bigint"), ("franja_fin", "bigint"), ("hora_pico", "bigint"),
]
ZONE_COLUMNS = [
    ("fecha", "date"), ("distribuidora_id", "bigint"), ("zona", "string"), ("nombre", "string"),
    ("probabilidad", "double"), ("parte", "double"), ("percentil", "double"), ("nivel", "string"), ("n_ct", "bigint"),
    ("abonados", "double"), ("kva", "double"), ("racha_max_kmh", "double"), ("racha_clima_max", "double"),
    ("lluvia_mm", "double"), ("hist_365d", "double"), ("clientes_esperados", "double"), ("municipios", "string"),
    ("run_id", "string"), ("creado_ts", "timestamp"),
]
# Columns of riesgo_manana.tabla_pasado_manana: the outlook of D + 2, scored the same evening with the variant of
# two days ahead (no zones, no guard: an orientation to plan, not a shift sheet).
OUTLOOK_COLUMNS = [
    ("fecha", "date"), ("distribuidora_id", "bigint"), ("distribuidora", "string"), ("probabilidad", "double"),
    ("tasa_base", "double"), ("veces_tasa_base", "double"), ("percentil", "double"), ("nivel", "string"),
    ("racha_max_kmh", "double"), ("racha_clima_max", "double"), ("lluvia_mm", "double"), ("aviso_max_nivel", "double"),
    ("hoy_racha_max_kmh", "double"), ("cobertura_prevision", "double"), ("entrenado_hasta", "date"),
    ("dias_entreno", "bigint"), ("variante", "string"), ("modelo", "string"), ("prevision_pedida_utc", "string"),
    ("run_id", "string"), ("creado_ts", "timestamp"),
]
# Output column -> live feature.
INPUTS = {"racha_max_kmh": "dia_racha_max", "racha_clima_max": "ext_racha_clima_max", "lluvia_mm": "dia_lluvia_max",
          "temp_max_c": "dia_temp_max", "aviso_max_nivel": "aviso_max_nivel", "aviso_viento_nivel": "aviso_viento_nivel",
          "aviso_tormenta_nivel": "aviso_tormenta_nivel"}



def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--fecha", default="", help="Day to predict, YYYY-MM-DD (tomorrow by default)")
    parser.add_argument("--landing", default=None, help="Folder of the live Landing (riesgo_manana.landing)")
    parser.add_argument("--desde-gold", action="store_true",
                        help="A past day: the forecast of the day before from Gold and the warnings from Silver")
    parser.add_argument("--solo-archivo", action="store_true",
                        help="Only the dated copies in the dashboard (riesgo_<day>.json), not riesgo.json")
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
    """An ISO time in UTC (pedido_utc of the Landing) as 13.10.2026 18:31 UTC; the stamps of a day played again as words."""
    if not stamp:
        return "?"
    if stamp == "gold":
        return "día anterior (previsión guardada en Gold, día repetido)"
    if stamp == "silver":
        return "archivo de avisos de Silver"
    try:
        parsed = pd.Timestamp(stamp)
    except (ValueError, TypeError):
        return stamp
    return f"{parsed.tz_convert('UTC'):%d.%m.%Y %H:%M} UTC" if parsed.tzinfo else stamp


# ---------------------------------------------------------------------------
# The live Landing
# ---------------------------------------------------------------------------

def read_json(fs, path: str) -> dict:
    with fs.open(path, "r") as file:
        return json.load(file)


# The run of ICON-EU is published every three hours and takes about three to arrive: a run older than this when the
# evening asked for it means Open-Meteo stopped publishing new ones (as its DWD ensemble did from 06.10.2026).
STALE_RUN_H = 9.0


def forecast_models(fs, folder: str) -> dict:
    """{model: cells} of the forecast of the evening (_peticion.modelo of every cell): the reserve when ICON-EU failed."""
    counts = {}
    for path in sorted(fs.glob(f"{folder}/prevision/*.json")):
        try:
            model = (read_json(fs, path).get("_peticion") or {}).get("modelo") or "?"
        except (OSError, ValueError):
            continue
        counts[model] = counts.get(model, 0) + 1
    return counts


def forecast_run(fs, folder: str) -> dict:
    """The run of the forecast of the evening (prevision_modelo.json of the Landing), {} when the Landing has none."""
    path = f"{folder}/prevision_modelo.json"
    try:
        return read_json(fs, path) if fs.exists(path) else {}
    except (OSError, ValueError) as error:
        logger.warning("Run of the forecast unreadable in %s: %s", path, error)
        return {}


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


def gold_hours(spark, day: pd.Timestamp, days_before: int = 31) -> tuple:
    """
    The hours of a past day as forecast_hours gives them, from Gold: for D
    the forecast ICON-EU made the day before (metp1_*), for the days before
    the reanalysis (met_*), with the names of LIVE_VARIABLES; "gold" as the
    stamp of the request. Empty without metp1_* for D.
    """
    names = [name for name, _, _ in LIVE_VARIABLES.values()]
    weather = spark.table(WEATHER_TABLE)
    first, last = day - pd.Timedelta(days=days_before), day + pd.Timedelta(days=1)
    wanted = ["celda_id", "hora"] + [f"{p}_{n}" for p in ("met", "metp1") for n in names if f"{p}_{n}" in weather.columns]
    pdf = (weather.where((F.col("hora") >= F.lit(first.to_pydatetime())) & (F.col("hora") < F.lit(last.to_pydatetime())))
           .select(*wanted).toPandas())
    if pdf.empty:
        return pd.DataFrame(columns=["celda_id", "hora"] + names), None
    pdf["hora"] = pd.to_datetime(pdf["hora"])
    of_day = pdf["hora"].dt.floor("D") == day
    out = pdf[["celda_id", "hora"]].copy()
    for name, low, high in LIVE_VARIABLES.values():
        past = pd.to_numeric(pdf[f"met_{name}"], errors="coerce") if f"met_{name}" in pdf else pd.Series(np.nan, index=pdf.index)
        ahead = pd.to_numeric(pdf[f"metp1_{name}"], errors="coerce") if f"metp1_{name}" in pdf else pd.Series(np.nan, index=pdf.index)
        values = ahead.where(of_day, past)
        out[name] = values.where(values.between(low, high))
    if out.loc[of_day, "racha_1h"].notna().sum() == 0:
        logger.warning("Gold has no forecast (metp1_*) for %s", day.date())
        return pd.DataFrame(columns=["celda_id", "hora"] + names), None
    return out.sort_values(["celda_id", "hora"]).reset_index(drop=True), "gold"


def warnings_from_silver(spark, day: pd.Timestamp, cutoff_h: int) -> pd.DataFrame:
    """
    The warnings of AEMET of the archive (l2_silver.f_aviso_meteo, by cell)
    sent before cutoff_h of the day before D, with the columns of
    warnings_in_cells; empty without the table.
    """
    columns = ["celda_id", "fenomeno_codigo", "nivel_num", "tipo_mensaje", "enviado_ts", "inicio_ts", "fin_ts", "parametro"]
    if not spark.catalog.tableExists(WARNINGS_TABLE):
        return pd.DataFrame(columns=columns)
    source = spark.table(WARNINGS_TABLE)
    cutoff = (day - pd.Timedelta(days=1) + pd.Timedelta(hours=int(cutoff_h))).to_pydatetime()
    pdf = (source.where((F.col("enviado_ts") < F.lit(cutoff)) & (F.col("fin_ts") > F.lit((day - pd.Timedelta(days=1)).to_pydatetime())))
           .select(*[c for c in columns if c in source.columns]).toPandas())
    for column in columns:
        if column not in pdf.columns:
            pdf[column] = np.nan if column != "parametro" else ""
    for column in ("enviado_ts", "inicio_ts", "fin_ts"):
        pdf[column] = pd.to_datetime(pdf[column])
    return pdf[columns].reset_index(drop=True)


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

def live_nao(fs, folder: str, nao_cfg: dict) -> dict:
    """
    meteo_dia.nao with csv pointing to a local copy of the file of the repository extended with the NAO landed this
    evening (<folder>/nao.csv, the CSV of CPC). Without that file the configuration is returned as it was.
    """
    nao_cfg = dict(nao_cfg or {})
    landed = f"{folder}/nao.csv"
    try:
        if not fs.exists(landed):
            logger.info("No NAO landed in %s: the block nao stops at the file of the repository", landed)
            return nao_cfg
        with fs.open(landed, "r") as file:
            fresh = pd.read_csv(file)
    except Exception as error:  # noqa: BLE001
        logger.warning("NAO of %s not read (%s)", landed, str(error)[:200])
        return nao_cfg
    fresh = pd.DataFrame({"fecha": pd.to_datetime(fresh[["year", "month", "day"]]), "nao": fresh.iloc[:, 3]})
    base = pd.read_csv(nao_cfg["csv"], parse_dates=["fecha"]) if nao_cfg.get("csv") and os.path.exists(nao_cfg["csv"]) \
        else pd.DataFrame(columns=["fecha", "nao"])
    merged = (pd.concat([base, fresh[fresh["fecha"] >= "2016-10-01"]]).drop_duplicates("fecha", keep="last")
              .sort_values("fecha"))
    path = os.path.join(tempfile.gettempdir(), "nao_diario_vivo.csv")
    merged.to_csv(path, index=False)
    logger.info("NAO index up to %s", merged["fecha"].max().date())
    return {**nao_cfg, "csv": path}


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


def labelled_until(spark, pdf: pd.DataFrame, calser: pd.Timestamp, day: pd.Timestamp, variant: dict,
                   margin_days: int) -> pd.Timestamp:
    """
    The last day whose label, weather and warnings are known: the last
    incident of the copy of Calser (calser) minus margin_days, the day before
    D, the last day with weather in the table and, when the variant has the
    warnings block, the last warning of the archive. The evening (45 days)
    and the business case (its own margin) share it.
    """
    ends = [calser.floor("D") - pd.Timedelta(days=int(margin_days)), day - pd.Timedelta(days=1)]
    if "dia_racha_max" in pdf.columns:
        ends.append(pdf.loc[pdf["dia_racha_max"].notna(), "fecha"].max())
    if "avisos" in variant["bloques"]:
        ends.append(warnings_until(spark).floor("D"))
    return min(e for e in ends if pd.notna(e))


def distributor_names(spark) -> dict:
    """
    {distribuidora_id: "Distribuidora <id>"} for every distribuidora with interruptions: the fallback of
    riesgo_manana.nombres, so a network without a name there never shows the name of its database.
    """
    facts = spark.table(FACTS_TABLE)
    rows = facts.select(F.col("distribuidora_id").cast("long").alias("d")).distinct().collect()
    return {int(r["d"]): f"Distribuidora {int(r['d'])}" for r in rows if r["d"] is not None}


def save_prediction(spark, pdf: pd.DataFrame, table: str, day: pd.Timestamp, columns: list = None):
    """The rows of D in the table, replacing the ones of an earlier run for the same day."""
    columns = columns or OUTPUT_COLUMNS
    schema = ", ".join(f"{name} {kind}" for name, kind in columns)
    df = spark.createDataFrame(pdf[[name for name, _ in columns]], schema=schema)
    df = df.withColumn("audit_loaded_at", F.current_timestamp())
    writer = df.write.format("delta").option("mergeSchema", "true")
    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", f"fecha = DATE'{day:%Y-%m-%d}'").saveAsTable(table)
    else:
        writer.mode("overwrite").saveAsTable(table)


# ---------------------------------------------------------------------------
# Zones and crews (riesgo_zonas)
# ---------------------------------------------------------------------------

def gold_cell_days(spark, min_hours: int) -> pd.DataFrame:
    """
    (celda_id, fecha, racha_max, clima_max, lluvia, temp_max, hora_pico) of
    the reanalysis of every cell and day in Gold; days with fewer than
    min_hours hours (meteo_dia.horas_minimas_dia) are left null.
    """
    weather = spark.table(WEATHER_TABLE)
    clima = F.col("met_racha_clima_1h") if "met_racha_clima_1h" in weather.columns else F.lit(None).cast("double")
    daily = (weather.groupBy("celda_id", F.to_date("hora").alias("fecha"))
             .agg(F.max("met_racha_1h").alias("racha_max"), F.max(clima).alias("clima_max"),
                  F.sum("met_lluvia_1h").alias("lluvia"), F.max("met_temp_1h").alias("temp_max"),
                  F.count("met_racha_1h").alias("horas"), F.max_by("hora", "met_racha_1h").alias("hora_pico"))
             .toPandas())
    daily["fecha"] = pd.to_datetime(daily["fecha"])
    daily["hora_pico"] = pd.to_datetime(daily["hora_pico"])
    short = daily["horas"] < min_hours
    daily.loc[short, ["racha_max", "clima_max", "lluvia", "temp_max"]] = np.nan
    return daily.drop(columns="horas")


def live_cell_days(cells_hours: pd.DataFrame, climate: pd.Series, day: pd.Timestamp, min_hours: int) -> pd.DataFrame:
    """The same daily weather of every cell for D from the live forecast (racha_1h, lluvia_1h, temp_1h)."""
    part = cells_hours[cells_hours["hora"].dt.floor("D") == day]
    out = part.groupby("celda_id").agg(racha_max=("racha_1h", "max"), lluvia=("lluvia_1h", "sum"),
                                       temp_max=("temp_1h", "max"), horas=("racha_1h", "count")).reset_index()
    out = out[out["horas"] >= min_hours].drop(columns="horas")
    p98 = out["celda_id"].map(climate).astype("float64")
    out["clima_max"] = out["racha_max"] / p98.where(p98 > 0)
    out["fecha"] = day
    return out


def fit_zone_model(rows: pd.DataFrame, features: list, config: dict, cfg: dict):
    """The logistic regression of the zone days (label y), fitted like the model of the day with its own grid."""
    grid = cfg.get("busqueda", {}).get("logistica") or [{"C": 0.003}, {"C": 0.01}, {"C": 0.03}, {"C": 0.1}]
    return fit(rows, features, "logistica", config, cfg, label="y", grid=grid)


def road_minutes(fs, zcfg: dict, municipalities: pd.DataFrame, dist: int):
    """
    The minutes by road between the municipalities of a distribuidora (OSRM
    over OpenStreetMap, riesgo_zonas.rutas), kept in the cache folder by the
    set of municipalities so the service is asked once. None when the routes
    are off, the service fails or fs cannot keep the cache: the crews fall
    back to straight lines.
    """
    rcfg = {**DEFAULTS["riesgo_zonas"]["rutas"], **(zcfg.get("rutas") or {})}
    if not rcfg.get("activo") or fs is None:
        return None
    places = municipalities.dropna(subset=["lat", "lon"]).sort_values("municipio")
    if places.empty:
        return None
    names = places["municipio"].astype(str).tolist()
    points = [(round(float(a), 4), round(float(o), 4)) for a, o in zip(places["lat"], places["lon"])]
    key = hashlib.sha1(json.dumps([names, points]).encode("utf-8")).hexdigest()[:12]
    cache = f"{str(rcfg['cache']).rstrip('/')}/{int(dist)}_{key}.json"
    try:
        if fs.exists(cache):
            payload = read_json(fs, cache)
            return pd.DataFrame(payload["minutos"], index=payload["municipios"], columns=payload["municipios"], dtype=float)
    except Exception as error:  # noqa: BLE001
        logger.warning("Road minutes of %s not read from %s: %s", dist, cache, error)
    try:
        matrix = rz.osrm_table(points, rcfg["api"], float(rcfg.get("timeout_s", 60)))
    except Exception as error:  # noqa: BLE001
        logger.warning("OSRM did not give the road minutes of %s (%s): straight lines instead", dist, str(error)[:160])
        return None
    logger.info("Road minutes of %s: %s municipalities from OSRM (%s without route)", dist, len(names),
                int(np.isnan(matrix).sum()))
    try:
        if hasattr(fs, "makedirs"):
            fs.makedirs(os.path.dirname(cache), exist_ok=True)
        with fs.open(cache, "w") as file:
            json.dump({"municipios": names, "minutos": [[None if np.isnan(v) else round(float(v), 2) for v in row]
                                                      for row in matrix], "api": rcfg["api"],
                       "pedido_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}, file, ensure_ascii=False)
    except Exception as error:  # noqa: BLE001
        logger.warning("Road minutes of %s not cached in %s: %s", dist, cache, error)
    return pd.DataFrame(matrix, index=names, columns=names, dtype=float)


def zone_outputs(spark, config: dict, rcfg: dict, cfg: dict, variant: dict, pdf: pd.DataFrame, events: pd.DataFrame,
                 live: pd.DataFrame, probability: np.ndarray, cells_hours: pd.DataFrame, climate: pd.Series,
                 hourly: pd.DataFrame, day: pd.Timestamp, end: pd.Timestamp, names: dict, levels: dict,
                 fs=None, history_in_model: bool = True) -> dict:
    """
    {distribuidora_id: {...}} with the risk of tomorrow below the
    distribuidora: zones (lines), municipalities, CTs, the window of the day,
    the mix of causes, the expected impact and the crews, from riesgo_zonas.
    pdf is the daily table of the training, live the row of tomorrow of every
    distribuidora and probability its probability of the day. Without
    history_in_model the hist_* of the distribuidora stay out of the zone
    model too (the copy of Calser is stale), as in the model of the day.
    """
    zcfg = {**DEFAULTS["riesgo_zonas"], **rcfg.get("riesgo_zonas", {})}
    dim = spark.table(DIM_TABLE)
    wanted = [c for c in ("distribuidora_id", "ct_id", "grupo_red_id", "municipio_nombre", "potencia_kva",
                          "num_abonados", "tipo_zona_codigo", "en_estudio") if c in dim.columns]
    ct = rz.zone_table(dim.select(*wanted).toPandas(), spark.table(CELLS_TABLE).toPandas())
    if ct.empty:
        logger.warning("%s has no network groups: no risk by zones", DIM_TABLE)
        return {}
    facts = spark.table(FACTS_TABLE).where(F.col("variante") == "principal")
    extra = [c for c in ("duracion_winsor_s",) if c in facts.columns]
    if extra:
        durations = facts.select(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "periodo_id",
                                 "incidencia_id", "ct_id", *extra).toPandas()
        durations["ct_id"] = durations["ct_id"].astype(str)
        ev = events.assign(ct_id=events["ct_id"].astype(str)).merge(
            durations, on=["distribuidora_id", "periodo_id", "incidencia_id", "ct_id"], how="left")
    else:
        ev = events
    zones = rz.zones_of(ct)
    starts = rz.local_starts(ev, ct)
    min_hours = int(cfg["horas_minimas_dia"])
    cell_days = gold_cell_days(spark, min_hours)
    weather = rz.zone_weather(ct, cell_days)
    prefixes = ("cal_", "hist_", "aviso_", "dia_", "ext_", "ayer_") if history_in_model else ("cal_", "aviso_", "dia_", "ext_", "ayer_")
    day_features = [c for c in pdf.columns if c.startswith(prefixes) and c in live.columns
                    and c != "hist_horas_desde_ultima"]
    table = rz.zone_days(zones, starts, weather, pdf, day_features, variant["desde"], f"{end:%Y-%m-%d}")
    table = table[table["fecha"] <= end]
    features = rz.zone_features(zones, day_features)
    scorer, search, X_train = fit_zone_model(table, features, config, cfg)
    train_scores = scorer.score(X_train)
    logger.info("Zone model: %s zones, %s zone days (%s with incidents), %s features", len(zones), len(table),
                int(table["y"].sum()), len(features))

    # Tomorrow: the zones with their history at D and the forecast of their cells.
    today = zones[["distribuidora_id", "zona"] + rz.ZONE_FEATURES_SIZE].assign(fecha=day)
    today = today.merge(rz.zone_weather(ct, live_cell_days(cells_hours, climate, day, min_hours)), on=["distribuidora_id", "zona", "fecha"], how="left")
    today = rz.zone_history(today, starts)
    today = today.merge(live[["distribuidora_id"] + day_features], on="distribuidora_id", how="left")
    for dist in sorted(zones["distribuidora_id"].unique()):
        today[f"dist_{dist}"] = (today["distribuidora_id"] == dist).astype(float)
    today["score"] = scorer.score(today[features].to_numpy(dtype=np.float32, na_value=np.nan)).astype("float64")
    shares = rz.ct_shares(ct, ev)
    stats = rz.impact_stats(ct, ev)
    mix = rz.cause_mix(starts, ct, cell_days)
    lags = rz.lag_stats(starts, cell_days, ct)
    cascade = rz.cascade_rule(starts)
    out = {}
    for i, dist in enumerate(live["distribuidora_id"].tolist()):
        mask = (today["distribuidora_id"] == dist).to_numpy()
        if not mask.any():
            continue
        part = today[mask].copy()
        part["probabilidad"] = rz.rescale_to_day(part["score"].to_numpy(), float(probability[i]))
        part["parte"] = part["probabilidad"] / part["probabilidad"].sum() if part["probabilidad"].sum() > 0 else 0.0
        train_mask = (table["distribuidora_id"] == dist).to_numpy()
        percentiles = []
        for zone, score in zip(part["zona"], part["score"]):
            own = train_scores[train_mask & (table["zona"] == zone).to_numpy()]
            percentiles.append(float((own <= score).mean()) if len(own) else 0.0)
        part["percentil"] = percentiles
        part["nivel"] = [rz.level_of(p, levels) for p in part["percentil"]]
        part = part.merge(zones[["distribuidora_id", "zona", "nombre", "municipios", "lat", "lon"]], on=["distribuidora_id", "zona"])
        part = part.merge(stats, on=["distribuidora_id", "zona"], how="left")
        part["clientes_esperados"] = part["probabilidad"] * part["abonados"].fillna(0.0)
        municipalities = rz.municipality_risk(ct[ct["distribuidora_id"] == dist], part, shares)
        zone_level = dict(zip(part["zona"], part["nivel"]))
        zone_p = dict(zip(part["zona"], part["probabilidad"]))
        municipalities["nivel"] = [zone_level.get(max(z, key=lambda k: zone_p.get(k, 0.0)), "bajo") if z else "bajo"
                                   for z in municipalities["zonas"]]
        cts = rz.ct_risk(ct[ct["distribuidora_id"] == dist], part, shares)
        installed = float(ct.loc[ct["distribuidora_id"] == dist, "potencia_kva"].sum())
        impact = rz.impact(part, stats, installed)
        prof = hourly[(hourly["distribuidora_id"] == dist) & (hourly["hora"].dt.floor("D") == day)]
        profile = pd.DataFrame({"hora": prof["hora"].dt.hour.to_numpy(),
                                "racha_max": prof.get("metd_max_metp1_racha_1h", pd.Series(dtype=float)).to_numpy(),
                                "racha_media": prof.get("metd_media_metp1_racha_1h", pd.Series(dtype=float)).to_numpy(),
                                "lluvia": prof.get("metd_max_metp1_lluvia_1h", pd.Series(dtype=float)).to_numpy()})
        window = rz.risk_window(profile, float(zcfg["franja_parte_pico"]), float(zcfg["franja_calma_kmh"]))
        bucket = rz.gust_bucket(float(profile["racha_max"].max()) if len(profile) else None)
        causes = mix[mix["bucket"] == bucket].sort_values("parte", ascending=False)
        crews = int(zcfg.get("brigadas", {}).get(str(int(dist)), 1))
        roads = road_minutes(fs, zcfg, municipalities, dist)
        plan = rz.crew_plan(municipalities, crews, float(zcfg["velocidad_kmh"]), float(zcfg["factor_carretera"]),
                            zcfg.get("bases_fijas", {}).get(str(int(dist))), roads)
        if plan:
            plan["curva"] = rz.crew_curve(municipalities, max(4, crews), speed_kmh=float(zcfg["velocidad_kmh"]),
                                          road_factor=float(zcfg["factor_carretera"]), road_minutes=roads)
            plan["posicionamiento"] = rz.positioning(window, plan)
        zone_rows = part.sort_values("probabilidad", ascending=False)
        # The municipality with the strongest forecast gust against its climate names the zones' weather.
        racha_m = {}
        if "celda_id" in ct.columns:
            cell_day = live_cell_days(cells_hours, climate, day, min_hours).set_index("celda_id")["racha_max"]
            racha_m = (ct[ct["distribuidora_id"] == dist].assign(r=lambda t: t["celda_id"].map(cell_day))
                       .groupby("municipio")["r"].max().to_dict())
        municipalities["racha_max"] = municipalities["municipio"].map(racha_m)
        out[int(dist)] = {
            "nombre": names.get(int(dist), str(dist)),
            "esperadas": impact["incidencias_esperadas"], "impacto": impact, "franja": window,
            "perfil": profile.to_dict("records"),
            "causas": [{"familia": rz.CAUSE_NAMES.get(r.familia, r.familia), "parte": float(r.parte), "n": int(r.n)}
                       for r in causes.itertuples()],
            "causas_bucket": bucket, "brigadas": plan,
            "zonas": [{"zona": r.zona, "nombre": r.nombre, "probabilidad": float(r.probabilidad), "parte": float(r.parte),
                       "percentil": float(r.percentil), "nivel": r.nivel, "n_ct": int(r.z_n_ct), "abonados": float(r.z_abonados),
                       "kva": float(r.z_kva), "racha_max": r.z_racha_max, "clima_max": r.z_clima_max, "lluvia": r.z_lluvia_max,
                       "hist_365d": float(r.z_hist_365d), "hist_dias_desde": float(r.z_hist_dias_desde),
                       "clientes_esperados": float(r.clientes_esperados), "municipios": list(r.municipios),
                       "lat": r.lat, "lon": r.lon, "ct_por_incidencia": r.cts, "horas_por_incidencia": r.horas}
                      for r in zone_rows.itertuples()],
            "municipios": [{"nombre": r.municipio, "codigo_ine": r.codigo_ine, "lat": r.lat, "lon": r.lon, "n_ct": int(r.n_ct),
                            "abonados": float(r.abonados), "kva": float(r.kva), "probabilidad": float(r.probabilidad),
                            "nivel": r.nivel, "zonas": [dict(zip(part["zona"], part["nombre"])).get(z, z) for z in r.zonas],
                            "clientes_esperados": float(r.clientes_esperados), "racha_max": r.racha_max}
                           for r in municipalities.sort_values("probabilidad", ascending=False).itertuples()],
            "cts": [{"ct_id": r.ct_id, "zona": r.zona, "municipio": r.municipio, "abonados": float(r.num_abonados),
                     "kva": float(r.potencia_kva), "probabilidad": float(r.probabilidad)}
                    for r in cts.sort_values("probabilidad", ascending=False).itertuples()],
            "_tabla": zone_rows,
        }
    out["_comun"] = {"retardos": lags, "cascada": cascade, "mezcla_causas": mix, "features": features,
                     "params": scorer.params, "busqueda": search, "n_zonas": len(zones), "dias": len(table),
                     "positivos": int(table["y"].sum())}
    return out


def zone_rows_table(zone_out: dict, day: pd.Timestamp, run_id: str, created) -> pd.DataFrame:
    """The rows of riesgo_zona_siguiente of every distribuidora."""
    rows = []
    for dist, block in zone_out.items():
        if not isinstance(dist, int):
            continue
        for z in block["zonas"]:
            rows.append({"fecha": day.date(), "distribuidora_id": dist, "zona": z["zona"], "nombre": z["nombre"],
                         "probabilidad": z["probabilidad"], "parte": z["parte"], "percentil": z["percentil"], "nivel": z["nivel"],
                         "n_ct": z["n_ct"], "abonados": z["abonados"], "kva": z["kva"], "racha_max_kmh": z["racha_max"],
                         "racha_clima_max": z["clima_max"], "lluvia_mm": z["lluvia"], "hist_365d": z["hist_365d"],
                         "clientes_esperados": z["clientes_esperados"], "municipios": ", ".join(z["municipios"][:5]),
                         "run_id": run_id, "creado_ts": created})
    return pd.DataFrame(rows)


def follow_up(spark, table: str, events: pd.DataFrame, end: pd.Timestamp, levels: dict, names: dict = None) -> dict:
    """
    What happened on the days already predicted: the level and probability of
    every past row of the table against the incidents of Calser (any cause),
    up to the last day with labels. Days without labels yet are kept with
    averias null. names ({distribuidora_id: name}, riesgo_manana.nombres)
    names every row as today, not with the name saved with the old rows.
    """
    names = names or {}

    def name_of(row):
        try:
            return names.get(int(row.distribuidora_id), row.distribuidora)
        except (TypeError, ValueError):
            return row.distribuidora

    if not spark.catalog.tableExists(table):
        return {}
    past = spark.table(table).select("fecha", "distribuidora_id", "distribuidora", "probabilidad", "nivel", "run_id").toPandas()
    if past.empty:
        return {}
    past["fecha"] = pd.to_datetime(past["fecha"])
    past = past.sort_values(["fecha", "distribuidora_id"]).drop_duplicates(["fecha", "distribuidora_id"], keep="last")
    ev = events.copy()
    ev["fecha"] = pd.to_datetime(ev["inicio_ts"]).dt.floor("D")
    counts = (ev.drop_duplicates(["distribuidora_id", "periodo_id", "incidencia_id"])
              .groupby(["distribuidora_id", "fecha"]).size().rename("averias").reset_index())
    past = past.merge(counts, on=["distribuidora_id", "fecha"], how="left")
    labelled = past["fecha"] <= end
    past.loc[labelled, "averias"] = past.loc[labelled, "averias"].fillna(0)
    past.loc[~labelled, "averias"] = np.nan
    done = past[labelled]
    high = done[done["nivel"].isin([n for n in levels if n != "bajo" and float(levels[n]) >= 0.9])]
    low = done[done["nivel"] == "bajo"]
    summary = {"dias": int(len(past)), "dias_con_resultado": int(len(done)),
               "dias_con_averia": int((done["averias"] > 0).sum()) if len(done) else 0,
               "acierto_alto": float((high["averias"] > 0).mean()) if len(high) else None,
               "averia_en_bajo": float((low["averias"] > 0).mean()) if len(low) else None}
    days = [{"fecha": f"{r.fecha:%d.%m.%Y}", "distribuidora": name_of(r), "nivel": r.nivel,
             "probabilidad": float(r.probabilidad), "averias": None if pd.isna(r.averias) else int(r.averias)}
            for r in past.sort_values(["fecha", "distribuidora_id"]).itertuples()]
    return {"resumen": summary, "dias": days}


def dashboard_payload(day: pd.Timestamp, out: pd.DataFrame, info: dict, zone_out: dict, seguimiento: dict,
                      run_id: str, notes: list) -> dict:
    """The JSON the dashboard reads (dashboard/index.html)."""
    common = zone_out.get("_comun", {})
    lags = common.get("retardos", {}).get("meteorologia", {})
    cascade = common.get("cascada", {})
    rule_text = []
    if lags:
        rule_text.append(f"El {100 * lags['en_3h']:.0f} % de las averías por meteorología empiezan a menos de 3 h del pico de racha del día "
                         f"(el {100 * lags['en_6h']:.0f} % a menos de 6 h, {lags['n']} averías).")
    if cascade.get("todas") is not None:
        rule_text.append(f"Tras una avería, otra en las {cascade['horas']:.0f} h siguientes en el {100 * cascade['todas']:.0f} % de los casos"
                         + (f" (el {100 * cascade['meteorologia']:.0f} % si la primera fue por meteorología)." if cascade.get("meteorologia") is not None else "."))
    dists = []
    for r in out.itertuples():
        block = zone_out.get(int(r.distribuidora_id), {})
        dists.append({
            "id": int(r.distribuidora_id), "nombre": r.distribuidora, "nivel": r.nivel, "probabilidad": r.probabilidad,
            "tasa_base": r.tasa_base, "veces_tasa_base": r.veces_tasa_base, "percentil": r.percentil, "motivos": r.motivos,
            "esperadas": block.get("esperadas"), "impacto": block.get("impacto", {}), "franja": block.get("franja", {}),
            "perfil": block.get("perfil", []), "causas": block.get("causas", []), "causas_bucket": block.get("causas_bucket"),
            "brigadas": block.get("brigadas", {}), "zonas": block.get("zonas", []), "municipios": block.get("municipios", []),
            "cts": block.get("cts", []), "racha_max_kmh": r.racha_max_kmh, "racha_clima_max": r.racha_clima_max,
            "lluvia_mm": r.lluvia_mm, "temp_max_c": r.temp_max_c, "aviso_max_nivel": r.aviso_max_nivel,
            "aviso_viento_nivel": getattr(r, "aviso_viento_nivel", None), "aviso_tormenta_nivel": getattr(r, "aviso_tormenta_nivel", None),
            "guardia": {"reforzar": bool(getattr(r, "guardia_reforzada", False)), "motivo": getattr(r, "guardia_motivo", "normal")},
            "cobertura_prevision": r.cobertura_prevision, "zonas_racha": r.zonas,
        })
    return {
        "generado": rz.now_text(), "fecha": f"{day:%Y-%m-%d}", "fecha_texto": f"{day:%d.%m.%Y}", "run_id": run_id,
        "modelo": {"variante": info["variante"], "descripcion": f"{MODEL_NAMES.get(info['modelo'], info['modelo'])}, variante {info['variante']}",
                   "dias": info["dias"], "positivos": info["positivos"], "entrenado_hasta": f"{info['entrenado_hasta']:%d.%m.%Y}",
                   "zonas": {k: v for k, v in common.items() if k in ("n_zonas", "dias", "positivos", "params")}},
        "fuentes": {"prevision_pedida": moment(info["prevision_pedida_utc"]),
                    "repetido": info["prevision_pedida_utc"] == "gold",
                    "pasada_prevision": moment((info.get("pasada_prevision") or {}).get("pasada_utc")),
                    "avisos_texto": info["avisos_texto"],
                    "calser_hasta": f"{info['calser_hasta']:%d.%m.%Y}" if pd.notna(info["calser_hasta"]) else None,
                    "historia_al_dia": info["historia_al_dia"], "avisos_disponibles": info["avisos_disponibles"],
                    "historia_en_modelo": bool(info.get("historia_en_modelo", info["historia_al_dia"]))},
        "distribuidoras": dists, "reglas": {"texto": " ".join(rule_text), "retardos": common.get("retardos", {}),
                                            "cascada": cascade},
        "seguimiento": seguimiento, "notas": notes,
    }


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def fit(rows: pd.DataFrame, features: list, name: str, config: dict, cfg: dict, label: str = MAIN_LABEL,
        grid: list = None) -> tuple:
    """
    (scorer, search rows, matrix of the training days): the configurations of
    the model compared on the last share of the days, the best one fitted
    again on all of them. The grid is the one of the configuration
    (busqueda) unless grid gives another, as the zone model does.
    """
    seed = int(config.get("semilla", 0))
    n_jobs = int(config.get("n_jobs", -1))
    days = np.sort(rows["fecha"].unique())
    cut = days[int(len(days) * (1 - float(cfg.get("fraccion_validacion_interna", 0.25))))]
    X, y = to_arrays(rows[rows["fecha"] < cut], features, label)
    Xv, yv = to_arrays(rows[rows["fecha"] >= cut], features, label)
    inner = {"X": X, "y": y, "w": np.ones(len(y)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
             "feature_names": features}
    logger.info("Choosing %s on %s days (%s with interruptions) and %s days from %s (%s)", name, len(y), int(y.sum()),
                len(yv), pd.Timestamp(cut).date(), int(yv.sum()))
    if grid is None:
        scorers, search = train([name], config, cfg, inner, float(y.mean()), False, references=False)
    else:
        scorers, search = train([name], {**config, "busqueda": {name: grid}}, {"busqueda": {}}, inner, float(y.mean()),
                                False, references=False)

    Xa, ya = to_arrays(rows, features, label)
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


def delta_versions(spark, tables: list) -> dict:
    """
    {table: current Delta version or None} of the tables the evening reads,
    for config_json of ml_runs: with time travel the evening can be repeated
    on the same data. The same as gold_common.delta_version, which the
    evening cannot import (03_gold is not on its path).
    """
    out = {}
    for table in tables:
        try:
            if not spark.catalog.tableExists(table):
                out[table] = None
                continue
            row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
            out[table] = None if row is None else int(row["version"])
        except Exception as error:  # noqa: BLE001 - a missing history is not fatal
            logger.warning("No Delta history for %s: %s", table, error)
            out[table] = None
    return out


def save_model(config: dict, folder_id: str, scorer) -> None:
    """The model of the evening as fitted, modelo.pkl next to resumen.md: to look at it or score again later."""
    folder = config.get("exportar_dir")
    if not folder:
        return
    target = os.path.join(folder, folder_id, "modelo.pkl")
    try:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as file:
            pickle.dump(scorer, file)
        logger.info("Model of the evening saved in %s", target)
    except (OSError, pickle.PicklingError, TypeError, AttributeError) as error:
        logger.warning("Model of the evening not saved in %s: %s", target, error)


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


level_of = rz.level_of


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

def report_notes(out: pd.DataFrame, info: dict) -> list:
    notes = []
    if not info["avisos_disponibles"]:
        notes.append("AEMET no ha dado los avisos: el día se puntúa sin avisos, como si no hubiera ninguno.")
    if not info["historia_al_dia"]:
        how = ("el modelo toma la mediana de los días de entrenamiento" if info.get("historia_en_modelo")
               else "el modelo de esta tarde se entrena sin ella")
        until = (f"llega hasta el {info['calser_hasta']:%d.%m.%Y}" if pd.notna(info.get("calser_hasta"))
                 else "no tiene incidencias")
        notes.append(f"La copia de Calser {until}: la historia de incidencias no está al día y {how}.")
    for row in out[out["cobertura_prevision"] < 0.999].itertuples():
        notes.append(f"{row.distribuidora}: solo el {number(100 * row.cobertura_prevision, 0)} % de sus CT está en "
                     "celdas con previsión.")
    models = info.get("prevision_modelos") or {}
    reserve = {m: n for m, n in models.items() if m != info.get("prevision_modelo_principal", "icon_eu")}
    if reserve:
        total = sum(models.values())
        notes.append("ICON-EU no respondió para " + ", ".join(f"{n} de {total} celdas" for n in reserve.values())
                     + ": su previsión es la de " + ", ".join(reserve) + ", el modelo de reserva.")
    run = info.get("pasada_prevision") or {}
    hours = run.get("horas_desde_pasada")
    if hours is not None and float(hours) > float(info.get("pasada_max_horas", STALE_RUN_H)):
        notes.append(f"La previsión es de la pasada de {moment(run.get('pasada_utc'))}, que tenía "
                     f"{number(float(hours), 0)} horas cuando se pidió: Open-Meteo no ha publicado pasadas nuevas de "
                     "ICON-EU y el día se puntúa con una previsión vieja.")
    if info.get("pasado_manana_error"):
        notes.append(f"La orientación de pasado mañana no se ha calculado ({info['pasado_manana_error']}); el riesgo "
                     "de mañana no cambia.")
    if info.get("artefactos_error"):
        notes.append(f"El modelo de esta tarde no se ha guardado en el almacén de modelos ({info['artefactos_error']}); "
                     "el riesgo se publica igual y el modelo queda en la carpeta de resultados.")
    return notes


def zone_report(block: dict, name: str) -> str:
    """The section of a distribuidora of resumen.md: zones, municipalities, window, causes, impact and crews."""
    zones = pd.DataFrame([{
        "zona (línea)": z["nombre"], "nivel": z["nivel"], "probabilidad": number(100 * z["probabilidad"], 1) + " %",
        "parte del riesgo": number(100 * z["parte"], 0) + " %", "CT": z["n_ct"], "clientes": number(z["abonados"], 0),
        "racha prevista (km/h)": number(z["racha_max"], 0) if z["racha_max"] is not None and pd.notna(z["racha_max"]) else "",
        "veces el p98": number(z["clima_max"], 2) if z["clima_max"] is not None and pd.notna(z["clima_max"]) else "",
        "averías en 365 días": number(z["hist_365d"], 0), "municipios": ", ".join(z["municipios"][:3]),
    } for z in block["zonas"][:12]])
    munis = pd.DataFrame([{"municipio": m["nombre"], "nivel": m["nivel"], "probabilidad": number(100 * m["probabilidad"], 1) + " %",
                           "CT": m["n_ct"], "clientes": number(m["abonados"], 0),
                           "clientes esperados sin luz": number(m["clientes_esperados"], 1)}
                          for m in block["municipios"][:10]])
    imp = block.get("impacto", {})
    fr = block.get("franja", {})
    plan = block.get("brigadas", {})
    parts = [f"## {name}: dónde, cuándo, por qué y cuánto",
             f"Averías esperadas: {number(imp.get('incidencias_esperadas', 0), 2)}; CT esperados sin luz: "
             f"{number(imp.get('ct_esperados', 0), 1)}; clientes esperados sin luz: {number(imp.get('clientes_esperados', 0), 0)} "
             f"({number(imp.get('kva_esperados', 0), 0)} kVA"
             + (f", {number(imp['minutos_tiepi_esperados'], 2)} minutos de TIEPI" if imp.get("minutos_tiepi_esperados") is not None else "")
             + f"). Franja de más riesgo: {fr.get('texto', '')}.",
             "Las zonas son las líneas de las que cuelgan los CT en TedisNet. La probabilidad de cada una reparte la del día "
             "por tamaño, historia y tiempo previsto de sus celdas; el modelo no sabe qué línea romperá, sabe cuáles "
             "tienen más papeletas.",
             ml_metrics.markdown_table(zones) if len(zones) else "Sin zonas.",
             "### Municipios con más riesgo\n\n" + (ml_metrics.markdown_table(munis) if len(munis) else "Sin municipios.")]
    if block.get("causas"):
        parts.append("### Si hay avería, por qué (días con racha " + block.get("causas_bucket", "") + ")\n\n"
                     + ", ".join(f"{c['familia']} {number(100 * c['parte'], 0)} %" for c in block["causas"]))
    if plan.get("bases"):
        bases = "; ".join(f"{b['municipio']} (cubre {b['cubre_ct']} CT, {number(100 * b['parte_riesgo'], 0)} % del riesgo, "
                          f"llegada media {number(b['llegada_media_min'], 0)} min)" for b in plan["bases"])
        curve = "; ".join(f"{c['n']}: {number(c['llegada_media_min'], 0)} min ({', '.join(c['bases'])})" for c in plan.get("curva", []))
        parts.append(f"### Brigadas\n\nCon {plan['n']} brigada{'s' if plan['n'] > 1 else ''}, las bases que minimizan la llegada "
                     f"al riesgo de mañana (tiempos: {plan.get('tiempos', 'línea recta')}): {bases}. "
                     f"Llegada media ponderada por el riesgo: {number(plan['llegada_media_min'], 0)} min; "
                     f"el {number(100 * plan['riesgo_30min'], 0)} % del riesgo a menos de 30 min."
                     + (f" Con las bases fijas ({', '.join(plan['fijas']['municipios'])}): {number(plan['fijas']['llegada_media_min'], 0)} min."
                        if plan.get("fijas") else "")
                     + (f" Posicionamiento: {plan['posicionamiento']}." if plan.get("posicionamiento") else "")
                     + (f" Llegada media según el número de brigadas: {curve}." if curve else ""))
    return "\n\n".join(parts)


def report_text(day: pd.Timestamp, out: pd.DataFrame, info: dict, zone_out: dict = None) -> str:
    zone_out = zone_out or {}
    shown = pd.DataFrame({
        "distribuidora": out["distribuidora"],
        "nivel": out["nivel"],
        "probabilidad": (100 * out["probabilidad"]).map(lambda v: number(v, 1) + " %"),
        "veces la tasa base": out["veces_tasa_base"].map(lambda v: number(v, 1) if pd.notna(v) else ""),
        "percentil": (100 * out["percentil"]).map(lambda v: number(v, 0)),
        "averías esperadas": out["incidencias_esperadas"].map(lambda v: number(v, 2) if pd.notna(v) else ""),
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
    notes = report_notes(out, info)
    levels = ", ".join(f"{name} desde el percentil {number(100 * float(v), 0)}"
                       for name, v in sorted(info["niveles"].items(), key=lambda item: -float(item[1])))
    common = zone_out.get("_comun", {})
    rules = []
    lags = common.get("retardos", {}).get("meteorologia")
    if lags:
        rules.append(f"El {number(100 * lags['en_3h'], 0)} % de las averías por meteorología de la historia empezaron a menos de "
                     f"3 h del pico de racha de su día (el {number(100 * lags['en_6h'], 0)} % a menos de 6 h).")
    cascade = common.get("cascada", {})
    if cascade.get("todas") is not None:
        rules.append(f"Tras una avería, otra en las {number(cascade['horas'], 0)} h siguientes en el "
                     f"{number(100 * cascade['todas'], 0)} % de los casos"
                     + (f", el {number(100 * cascade['meteorologia'], 0)} % si la primera fue por meteorología."
                        if cascade.get("meteorologia") is not None else "."))
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
    ] + [zone_report(zone_out[int(r.distribuidora_id)], r.distribuidora) for r in out.itertuples()
         if int(r.distribuidora_id) in zone_out]
      + (["## Reglas de la historia\n\n" + "\n".join(f"- {t}" for t in rules)] if rules else [])
      + (["## Notas\n\n" + "\n".join(f"- {n}" for n in notes)] if notes else [])) + "\n"


def attach_second_opinions(payload: dict, folder: str) -> dict:
    """
    What the other tasks of the evening left in the dashboard folder for the
    same day: the ensemble of ICON-EU EPS (ensemble.json) and the forecast of
    AEMET by municipality (aemet.json), attached to every distribuidora as
    escenarios and aemet (texto, pico, ...) so the shift sheet and the message
    of the evening carry them. Missing or older files leave nothing.
    """
    if not folder or not os.path.isdir(folder):
        return payload
    for name, key, fields in (("ensemble.json", "escenarios", ("texto", "pico", "supera", "miembros", "modelo")),
                              ("aemet.json", "aemet", ("texto", "pico", "elaborado", "municipios"))):
        path = os.path.join(folder, name)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as file:
                other = json.load(file)
        except (OSError, ValueError) as error:
            logger.warning("%s not read: %s", name, error)
            continue
        if other.get("fecha") != payload.get("fecha"):
            logger.info("%s is of %s, not of %s: not attached", name, other.get("fecha"), payload.get("fecha"))
            continue
        blocks = {str(b.get("id")): b for b in other.get("distribuidoras", [])}
        for d in payload.get("distribuidoras", []):
            block = blocks.get(str(d.get("id")))
            if block and block.get("horas"):
                d[key] = {f: block.get(f) for f in fields if block.get(f) is not None}
    return payload


def two_day_outlook(spark, config: dict, rcfg: dict, cfg: dict, pdf: pd.DataFrame, blocks: dict, end: pd.Timestamp,
                    hourly: pd.DataFrame, cells_hours: pd.DataFrame, weights: pd.DataFrame, warnings: pd.DataFrame,
                    dists: list, known: pd.DataFrame, up_to_date: bool, day: pd.Timestamp, names: dict,
                    forecast_asked: str, run_id: str, created, history_in_model: bool = True) -> pd.DataFrame:
    """
    The outlook of D + 2 (pasado mañana) from the same forecast and warnings
    of the evening: the variant of two days ahead (riesgo_manana.
    variante_pasado_manana, operativa_48h: gusts and rain of the day, their
    extremes, the warnings already sent, the weather of the last known day)
    fitted on the same training days and scored on D + 2. Levels by the same
    percentiles. Empty when the variant, its blocks or the forecast of D + 2
    are missing: an orientation to plan, never a reason to fail the evening.
    Without history_in_model it is fitted without the history of incidents,
    as the model of D.
    """
    wanted = rcfg.get("variante_pasado_manana")
    variant = next((v for v in cfg["variantes"] if v["nombre"] == wanted), None) if wanted else None
    if variant is None or any(not blocks.get(b) for b in variant["bloques"]):
        logger.info("No outlook of D + 2: variant %s not available", wanted)
        return pd.DataFrame()
    day2 = day + pd.Timedelta(days=1)
    features = variant_features(variant, blocks)
    if not history_in_model:
        features = [f for f in features if not f.startswith("hist_")]
    rows = pdf[(pdf["fecha"] >= pd.Timestamp(variant["desde"])) & (pdf["fecha"] <= end)].reset_index(drop=True)
    name = rcfg["modelo"]
    scorer, _, X_train = fit(rows, features, name, config, cfg)
    train_scores = scorer.score(X_train)

    live = pd.DataFrame({"distribuidora_id": dists})
    live = live.merge(weather_features(hourly, day2, cfg), on="distribuidora_id", how="left")
    # The last day of weather known this evening is today (day - 1): the hace2_* of D + 2, as the training builds
    # them (HORIZONS[2], the weather of the scored day minus two). Tomorrow is still a forecast.
    daily = daily_weather(hourly, int(cfg["horas_minimas_dia"]))
    before = daily[daily["fecha"] == day - pd.Timedelta(days=1)].rename(
        columns={c: f"hace2_{c}" for c in daily.columns if c not in ("distribuidora_id", "fecha")})
    live = live.merge(before.drop(columns="fecha"), on="distribuidora_id", how="left")
    warned = warning_features(warnings, weights, day2, dists)
    warned = warned.rename(columns={c: c.replace("aviso_", "aviso48_", 1) for c in warned.columns if c.startswith("aviso_")})
    live = live.merge(warned, on="distribuidora_id", how="left")
    live["hora"] = day2
    history = add_history(live, known, cfg["ventanas_historia_dias"])
    live["hist_dias_desde_ultima"] = live.pop("hist_horas_desde_ultima") / 24.0
    history = [h for h in history if h != "hist_horas_desde_ultima"] + ["hist_dias_desde_ultima"]
    if not up_to_date:
        live[history] = np.nan
    live = pd.concat([live, day_calendar(pd.Series(day2, index=live.index))], axis=1)
    for dist in dists:
        live[f"dist_{dist}"] = (live["distribuidora_id"] == dist).astype("float64")
    for feature in [f for f in features if f not in live.columns]:
        live[feature] = np.nan
    day_weather = [f for f in features if f.startswith("dia_")]
    if not day_weather or live[day_weather].isna().all(axis=1).any():
        logger.info("No outlook of D + 2: the forecast does not cover %s for every distribuidora", day2.date())
        return pd.DataFrame()

    X = live[features].to_numpy(dtype=np.float32, na_value=np.nan)
    probability = scorer.score(X).astype("float64")
    of_day = cells_hours[(cells_hours["hora"].dt.floor("D") == day2) & cells_hours["racha_1h"].notna()]
    covered = weights[weights["celda_id"].isin(set(of_day["celda_id"]))]
    coverage = covered.groupby("distribuidora_id")["n_ct"].sum() / weights.groupby("distribuidora_id")["n_ct"].sum()
    out = []
    for i, dist in enumerate(live["distribuidora_id"].tolist()):
        mask = (rows["distribuidora_id"] == dist).to_numpy()
        base = float(rows.loc[mask, MAIN_LABEL].mean())
        percentile = float((train_scores[mask] <= probability[i]).mean())
        value = lambda column: float(live.loc[i, column]) if column in live.columns else float("nan")  # noqa: E731
        out.append({
            "fecha": day2.date(), "distribuidora_id": int(dist), "distribuidora": names.get(int(dist), str(dist)),
            "probabilidad": float(probability[i]), "tasa_base": base,
            "veces_tasa_base": float(probability[i] / base) if base > 0 else float("nan"),
            "percentil": percentile, "nivel": level_of(percentile, rcfg["niveles"]),
            "racha_max_kmh": value("dia_racha_max"), "racha_clima_max": value("ext_racha_clima_max"),
            "lluvia_mm": value("dia_lluvia_max"), "aviso_max_nivel": value("aviso48_max_nivel"),
            # The strongest gust of today, the last day known this evening (hace2_* of D + 2).
            "hoy_racha_max_kmh": value("hace2_racha_max"),
            "cobertura_prevision": float(coverage.get(dist, 0.0)), "entrenado_hasta": end.date(),
            "dias_entreno": int(mask.sum()), "variante": variant["nombre"], "modelo": name,
            "prevision_pedida_utc": forecast_asked, "run_id": run_id, "creado_ts": created,
        })
    frame = pd.DataFrame(out)
    table = rcfg.get("tabla_pasado_manana")
    if table:
        save_prediction(spark, frame, table, day2, OUTLOOK_COLUMNS)
    logger.info("Outlook of %s (two days ahead, %s):\n%s", day2.date(), variant["nombre"],
                frame[["distribuidora", "probabilidad", "percentil", "nivel"]].to_string(index=False))
    return frame


def outlook_payload(frame: pd.DataFrame, day: pd.Timestamp) -> dict:
    """The block pasado_manana of riesgo.json; None without an outlook."""
    if frame is None or frame.empty:
        return None
    day2 = day + pd.Timedelta(days=1)
    dists = []
    for r in frame.sort_values("probabilidad", ascending=False).itertuples():
        dists.append({
            "id": int(r.distribuidora_id), "nombre": r.distribuidora, "nivel": r.nivel, "probabilidad": float(r.probabilidad),
            "veces_tasa_base": float(r.veces_tasa_base) if pd.notna(r.veces_tasa_base) else None,
            "percentil": float(r.percentil), "racha_max_kmh": float(r.racha_max_kmh) if pd.notna(r.racha_max_kmh) else None,
            "lluvia_mm": float(r.lluvia_mm) if pd.notna(r.lluvia_mm) else None,
            "aviso_max_nivel": float(r.aviso_max_nivel) if pd.notna(r.aviso_max_nivel) else None,
            "cobertura_prevision": float(r.cobertura_prevision),
        })
    variant = str(frame["variante"].iloc[0])
    return {
        "fecha": f"{day2:%Y-%m-%d}", "fecha_texto": f"{day2:%d.%m.%Y}", "variante": variant,
        "distribuidoras": dists,
        "nota": ("Orientación a dos días con la misma previsión de ICON-EU y los avisos de AEMET que ya existen, con el "
                 "modelo de dos tardes antes (racha y lluvia): conserva la mayor parte de la señal del de mañana (5,4 "
                 "frente a 6,9 veces la tasa base en 2024 - 2025) y pierde sobre todo los avisos que AEMET aún no ha "
                 "emitido. Sirve para planificar, no sustituye al parte de mañana."),
    }


INDEX_FILE = "indice.json"
DATED_FILE = re.compile(r"riesgo_(\d{8})\.json")


def day_summary(payload: dict, name: str, dashboard: str) -> dict:
    """One entry of the index of the dashboard: the day, how it was made and the level of every distribuidora."""
    stamp = DATED_FILE.fullmatch(name).group(1)
    sources = payload.get("fuentes") or {}
    forecast = str(sources.get("prevision_pedida") or "")
    files = {kind: f"{kind}_{stamp}.{ext}" for kind, ext in (("parte_guardia", "xlsx"), ("ensemble", "json"),
                                                            ("aemet", "json"))}
    dists = []
    for d in payload.get("distribuidoras") or []:
        guard = d.get("guardia") or {}
        dists.append({
            "id": d.get("id"), "nombre": d.get("nombre"), "nivel": d.get("nivel"), "probabilidad": d.get("probabilidad"),
            "percentil": d.get("percentil"), "aviso_max_nivel": d.get("aviso_max_nivel"),
            "guardia_reforzada": bool(guard.get("reforzar")), "guardia_motivo": guard.get("motivo"),
            "franja": (d.get("franja") or {}).get("texto"), "racha_max_kmh": d.get("racha_max_kmh"),
        })
    outlook = payload.get("pasado_manana") or {}
    return {
        "fecha": payload.get("fecha") or f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:]}",
        "fecha_texto": payload.get("fecha_texto") or f"{stamp[6:]}.{stamp[4:6]}.{stamp[:4]}",
        "dia": stamp, "fichero": name, "generado": payload.get("generado"), "run_id": payload.get("run_id"),
        # fuentes.repetido since the 0.16.0; the days archived before only have the words of moment("gold").
        "repetido": bool(sources["repetido"]) if "repetido" in sources else ("repetido" in forecast or "Gold" in forecast),
        "prevision": forecast,
        "modelo": (payload.get("modelo") or {}).get("descripcion"),
        "distribuidoras": dists,
        "pasado_manana": {"fecha": outlook.get("fecha"), "fecha_texto": outlook.get("fecha_texto"),
                          "distribuidoras": [{"id": d.get("id"), "nombre": d.get("nombre"), "nivel": d.get("nivel"),
                                              "probabilidad": d.get("probabilidad")}
                                             for d in outlook.get("distribuidoras", [])]} if outlook else None,
        "ficheros": {kind: file for kind, file in files.items() if os.path.exists(os.path.join(dashboard, file))},
    }


def dashboard_index(dashboard: str) -> dict:
    """
    The index of the days the dashboard keeps (riesgo_<day>.json, newest
    first) and the live day (riesgo.json), for dias.html: the pages never
    need to know the names of the files.
    """
    days = []
    for name in sorted(os.listdir(dashboard)):
        if not DATED_FILE.fullmatch(name):
            continue
        try:
            with open(os.path.join(dashboard, name), encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, ValueError) as error:
            logger.warning("Index of the dashboard: %s not read (%s)", name, error)
            continue
        days.append(day_summary(payload, name, dashboard))
    live = None
    live_path = os.path.join(dashboard, "riesgo.json")
    if os.path.exists(live_path):
        try:
            with open(live_path, encoding="utf-8") as file:
                payload = json.load(file)
            live = {"fecha": payload.get("fecha"), "fecha_texto": payload.get("fecha_texto"),
                    "generado": payload.get("generado"), "run_id": payload.get("run_id")}
        except (OSError, ValueError) as error:
            logger.warning("Index of the dashboard: riesgo.json not read (%s)", error)
    others = {name: os.path.exists(os.path.join(dashboard, name))
              for name in ("historico.json", "sector.json", "verificacion.json", "observado.json", "rayos.json",
                           "avisos_hoy.json")}
    return {"generado": rz.now_text(), "vivo": live, "dias": sorted(days, key=lambda d: d["fecha"], reverse=True),
            "otros": [name for name, present in others.items() if present]}


def write_index(dashboard: str):
    """indice.json of the dashboard folder; a failure is logged, never fatal."""
    try:
        rz.write_json(dashboard_index(dashboard), os.path.join(dashboard, INDEX_FILE))
        logger.info("Dashboard index written in %s", os.path.join(dashboard, INDEX_FILE))
    except OSError as error:
        logger.error("Dashboard index not written in %s: %s", dashboard, error)


def write_dashboard(config: dict, rcfg: dict, folder_id: str, payload: dict, day: pd.Timestamp, dated_only: bool = False):
    """
    riesgo.json for the dashboard (dashboard_dir, when the folder exists)
    and a copy in the export folder, and parte_guardia.xlsx (the same
    tables for the shift, to print or send) next to each of them. With
    dated_only the dashboard gets riesgo_<day>.json only (a past day played
    again must not replace the live day). The dashboard also gets its index
    (indice.json) rebuilt from the files it has.
    """
    targets = []
    folder = config.get("exportar_dir")
    if folder:
        targets.append(os.path.join(folder, folder_id, "riesgo.json"))
    dashboard = rcfg.get("dashboard_dir")
    if dashboard and os.path.isdir(dashboard):
        targets += ([] if dated_only else [os.path.join(dashboard, "riesgo.json")]) + [os.path.join(dashboard, f"riesgo_{day:%Y%m%d}.json")]
    elif dashboard:
        logger.warning("Dashboard folder %s not found: riesgo.json only in the export folder", dashboard)
    for target in targets:
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            rz.write_json(payload, target)
            logger.info("Dashboard JSON written in %s", target)
        except OSError as error:
            logger.error("Dashboard JSON not written in %s: %s", target, error)
            continue
        sheet = os.path.join(os.path.dirname(target), os.path.basename(target).replace("riesgo", "parte_guardia", 1)
                             .replace(".json", ".xlsx"))
        try:
            if rz.write_shift_sheet(payload, sheet):
                logger.info("Shift sheet written in %s", sheet)
            else:
                logger.warning("openpyxl not installed: no shift sheet")
        except Exception as error:  # noqa: BLE001
            logger.error("Shift sheet not written in %s: %s", sheet, error)
    if dashboard and os.path.isdir(dashboard):
        write_index(dashboard)


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
    if args.desde_gold:
        spark = get_spark(config)
        cells_hours, forecast_asked = gold_hours(spark, day)
        if cells_hours.empty:
            raise FileNotFoundError(f"No forecast of the day before (metp1_*) for {day.date()} in {WEATHER_TABLE}")
        logger.info("Playing %s again from Gold: the forecast of the day before and the warnings of the archive", day.date())
    else:
        cells_hours, forecast_asked = forecast_hours(fs, folder, zone)
        if cells_hours.empty:
            raise FileNotFoundError(f"No forecast of ICON-EU for {day.date()} in {folder}/prevision: run "
                                    "job_landing_vivo.py --fuente prevision first")
        spark = get_spark(config)
    cfg["hasta"] = f"{day:%Y-%m-%d}"
    # The NAO landed this evening extends the file of the repository up to the last day CPC has published
    # (D - 3 of the day scored; block nao, only used when the variant has it).
    cfg["nao"] = live_nao(fs, folder, cfg.get("nao"))
    pdf, blocks, _ = build_dataset(spark, config, cfg)
    missing_blocks = [b for b in variant["bloques"] if not blocks.get(b)]
    if missing_blocks:
        raise ValueError(f"No features of the blocks {missing_blocks} of the variant {variant['nombre']}")
    features = variant_features(variant, blocks)

    # The last day whose label, weather and warnings are known.
    events, known = read_events(spark, cfg)
    calser = calser_until(spark, events, day)
    end = labelled_until(spark, pdf, calser, day, variant, int(rcfg["margen_etiquetas_dias"]))
    rows = pdf[(pdf["fecha"] >= pd.Timestamp(variant["desde"])) & (pdf["fecha"] <= end)].reset_index(drop=True)
    logger.info("Training days of %s up to %s: %s (%s with interruptions); Calser up to %s", variant["nombre"],
                end.date(), len(rows), int(rows[MAIN_LABEL].sum()), calser)
    # Tomorrow's history is only known when the copy of Calser reaches D minus the margin. Without it the model
    # is fitted without the history, so tomorrow is never scored with the median of the training days.
    up_to_date = bool(pd.notna(calser) and calser >= day - pd.Timedelta(days=int(rcfg.get("historia_margen_dias", 7))))
    history_in_model = up_to_date or rcfg.get("historia_desfasada", "mediana") == "mediana"
    if not history_in_model:
        dropped = [f for f in features if f.startswith("hist_")]
        features = [f for f in features if not f.startswith("hist_")]
        logger.warning("Calser up to %s: the model of this evening is fitted without the history of incidents %s",
                       calser, dropped)

    name = rcfg["modelo"]
    scorer, search, X_train = fit(rows, features, name, config, cfg)
    train_scores = scorer.score(X_train)
    # What the model was fitted with: the versions of the tables it read and the environment.
    read_versions = delta_versions(spark, [WEATHER_TABLE, CELLS_TABLE, WARNINGS_TABLE, INCIDENTS_TABLE, FACTS_TABLE,
                                           DIM_TABLE, rcfg["tabla"]])
    fitted_in = environment()

    # Tomorrow, from the live Landing.
    dists = sorted(rows["distribuidora_id"].unique().tolist())
    weights = cell_weights(spark)
    climate = cell_climate(spark)
    hours = gold_columns(cells_hours, climate)
    hourly = distributor_hours(hours, weights)
    live = pd.DataFrame({"distribuidora_id": dists})
    live = live.merge(weather_features(hourly, day, cfg), on="distribuidora_id", how="left")

    areas = rcfg.get("avisos", {}).get("areas", ["esp"])
    if args.desde_gold:
        warnings = warnings_from_silver(spark, day, int(cfg.get("aviso_corte_h", 20)))
        cap_rows, answered, warnings_asked = [], spark.catalog.tableExists(WARNINGS_TABLE), "silver"
    else:
        cap_rows, answered, warnings_asked = warning_rows(fs, folder, areas)
        warnings = warnings_in_cells(cap_rows, sorted(weights["celda_id"].unique()))
    logger.info("Warnings of AEMET (answered for every area: %s): %s CAP rows, %s in the cells of the network, %s in "
                "force on %s", answered, len(cap_rows), len(warnings), len(warnings_of_day(warnings, day)), day.date())
    live = live.merge(warning_features(warnings, weights, day, dists), on="distribuidora_id", how="left")

    teleconnection = nao_features(cfg, pd.Series([day]))
    if len(teleconnection.columns) > 1:
        for column in ("nao_dia", "nao_30d"):
            live[column] = float(teleconnection[column].iloc[0])

    live["hora"] = day
    history = add_history(live, known, cfg["ventanas_historia_dias"])
    live["hist_dias_desde_ultima"] = live.pop("hist_horas_desde_ultima") / 24.0
    history = [h for h in history if h != "hist_horas_desde_ultima"] + ["hist_dias_desde_ultima"]
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
    names = {**distributor_names(spark), **{int(k): str(v) for k, v in rcfg.get("nombres", {}).items()}}
    zone_out = {}
    if spark.catalog.tableExists(DIM_TABLE):
        zone_out = zone_outputs(spark, config, rcfg, cfg, variant, pdf, events, live, probability, cells_hours, climate,
                                hourly, day, end, names, rcfg["niveles"], fs, history_in_model)
    else:
        logger.warning("%s not found: no risk by zones", DIM_TABLE)

    out_rows = []
    for i, dist in enumerate(live["distribuidora_id"].tolist()):
        block = zone_out.get(int(dist), {})
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
            **{f"guardia_{'reforzada' if k == 'reforzar' else k}": v for k, v in guard_advice(
                level_of(percentile, rcfg["niveles"]),
                float(live.loc[i, "aviso_viento_nivel"]) if "aviso_viento_nivel" in live.columns else None,
                float(live.loc[i, "aviso_tormenta_nivel"]) if "aviso_tormenta_nivel" in live.columns else None).items()},
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
            "incidencias_esperadas": block.get("esperadas"),
            "ct_esperados": block.get("impacto", {}).get("ct_esperados"),
            "clientes_esperados": block.get("impacto", {}).get("clientes_esperados"),
            "franja_inicio": block.get("franja", {}).get("inicio"),
            "franja_fin": block.get("franja", {}).get("fin"),
            "hora_pico": block.get("franja", {}).get("pico"),
        })
    out = pd.DataFrame(out_rows)
    save_prediction(spark, out, rcfg["tabla"], day)
    zone_rows = zone_rows_table(zone_out, day, args.run_id, created)
    if not zone_rows.empty:
        save_prediction(spark, zone_rows, rcfg["tabla_zonas"], day, ZONE_COLUMNS)

    outlook = pd.DataFrame()
    outlook_error = None
    if not args.desde_gold:
        try:
            outlook = two_day_outlook(spark, config, rcfg, cfg, pdf, blocks, end, hourly, cells_hours, weights, warnings,
                                      dists, known, up_to_date, day, names, forecast_asked, args.run_id, created,
                                      history_in_model)
        except Exception as error:  # noqa: BLE001
            logger.error("Outlook of D + 2 not computed: %s", error)
            outlook_error = f"{type(error).__name__}: {str(error)[:200]}"

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
        "historia_en_modelo": history_in_model,
        "calser_hasta": calser, "prevision_pedida_utc": forecast_asked, "avisos_texto": warning_text,
        "pasada_prevision": {} if args.desde_gold else forecast_run(fs, folder),
        "prevision_modelos": {} if args.desde_gold else forecast_models(fs, folder),
        "prevision_modelo_principal": rcfg.get("prevision", {}).get("modelo", "icon_eu"),
        "pasada_max_horas": float(rcfg.get("pasada_max_horas", STALE_RUN_H)),
        "pasado_manana_error": outlook_error,
    }
    # The model in the store of models (artefactos_uri/<run_id>/, as job_ml_train): a failure is said, never fatal.
    artifacts, artifacts_error = store_artifacts(
        {name: scorer}, config.get("artefactos_uri"), args.run_id,
        {"run_id": args.run_id, "dataset_version": f"riesgo_manana_{day:%Y-%m-%d}", "etiqueta": f"riesgo_manana:{MAIN_LABEL}",
         "variante": variant["nombre"], "features": features, "modelos": {name: {"params": scorer.params, **scorer.info}},
         "entorno": fitted_in, "versiones_delta": read_versions}, spark)
    info["artefactos_error"] = artifacts_error
    folder_id = re.sub(r"[^A-Za-z0-9_.-]", "_", args.run_id)
    seguimiento = follow_up(spark, rcfg["tabla"], events, end, rcfg["niveles"], names)
    notes = report_notes(out, info)
    payload = attach_second_opinions(dashboard_payload(day, out, info, zone_out, seguimiento, args.run_id, notes), rcfg.get("dashboard_dir"))
    payload["pasado_manana"] = outlook_payload(outlook, day)
    if artifacts_error:
        payload["artefactos_error"] = artifacts_error
    export(config, folder_id, report_text(day, out, info, zone_out),
           {"prediccion": out, "zonas": zone_rows, "coeficientes": pd.DataFrame(ml_models.linear_terms(scorer))})
    save_model(config, folder_id, scorer)
    write_dashboard(config, rcfg, folder_id, payload, day, args.solo_archivo)

    run = pd.DataFrame([{
        "run_id": args.run_id,
        "creado_ts": created,
        "dataset_version": f"riesgo_manana_{day:%Y-%m-%d}",
        "etiqueta": f"riesgo_manana:{MAIN_LABEL}",
        "n_features": len(features),
        "modelos_json": json.dumps({variant["nombre"]: {name: {"params": scorer.params, **scorer.info}}}, default=str),
        "busqueda_json": json.dumps(search, ensure_ascii=False, default=str),
        "config_json": json.dumps({"riesgo_manana": rcfg, "variante": variant, "features": features,
                                   "versiones_delta": read_versions, "entorno": fitted_in},
                                  ensure_ascii=False, default=str),
        "resumen_json": json.dumps({**{str(r.distribuidora_id): {"probabilidad": r.probabilidad, "nivel": r.nivel}
                                       for r in out.itertuples()},
                                    **({"artefactos_error": artifacts_error} if artifacts_error else {})}, ensure_ascii=False),
        "splits_evaluados": "",
        "artefactos_uri": artifacts or "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)
    logger.info("Risk of %s saved in %s:\n%s", day.date(), rcfg["tabla"],
                out[["distribuidora", "probabilidad", "percentil", "nivel", "motivos"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
