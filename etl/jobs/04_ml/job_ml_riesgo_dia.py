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
causas_no_meteo (upstream, blackout, third parties...); y_dia_varias, when
incidencias_dia_varias or more incidents start on D (the busy days that need
more than one crew). The models learn y_dia; the other two only measure them;
n_dia, the number of incidents that start on D, is what the model poisson
learns from (a count instead of a probability, scored on the same metrics).

Features, every one known before D starts except the block dia:

- cal_*: day of the week (sine and cosine), weekend, holiday (national or
  Extremadura), day of the year (sine and cosine) and estacion.
- hist_*: incidents of the distribuidora known in Calser (conocido_ts)
  before D in the last 7, 30 and 365 days, and days since the last one.
- ayer_*: the weather of D - 1 from the reanalysis, by day: strongest gust
  and wind of the worst cell and of the mean of the cells weighted by their
  CT, rain of the day (worst cell and mean), hottest hour, coldest mean
  hour, mean humidity, lowest pressure and its biggest drop in 24 h, snow,
  the strongest gust of the whole area (the size of the storm), the
  active fires of FIRMS near the cells and the highest Fire Weather Index of
  CEMS of the cells (when Gold has them; Gold gives every hour the index of
  the day before, so dia_fwi_max is the one of D - 1, known that evening).
- aviso_*: the warnings of AEMET for D (l2_silver.f_aviso_meteo) sent
  before aviso_corte_h of D - 1: highest level (1 yellow, 2 orange, 3 red)
  in the cells of the distribuidora, in total and for wind, rain,
  thunderstorms and snow. 0 without warning, null before the archive starts
  (18.06.2018). It is the only real forecast with a long archive.
- aviso_det_* (block avisos_detalle): what those warnings announce and how
  much they cover: the strongest gust (km/h) and the most rain (mm in 12 h
  and in one hour) of their parametro, the most hours of D under a wind
  warning and the share of the CT of the distribuidora in cells under a
  wind warning, under any warning of wind, rain, thunderstorms or snow and
  under a thunderstorm warning.
- dia_*: the same daily weather as ayer_* for D itself, the perfect
  forecast.
- ext_*: how far the gusts of D go above the climate of each cell (the 98th
  percentile of its hourly gusts before meteo.climatologia_hasta of Gold,
  met_racha_clima_1h): the highest ratio of the worst cell and of the mean
  of the cells weighted by their CT, and the sum over the hours of the CT
  weighted max(0, ratio - 1)^3, the storm loss index of Klawa and Ulbrich;
  and the days in a row up to D with the hottest hour of the worst cell at
  calor_umbral_c or more. Like dia_*, they come from the reanalysis of D.
- prev_*: the same daily weather for D from what ICON-EU forecast the day
  before (metp1_* of Gold, Previous Runs API, from January 2024). It has no
  history to train with, so it is never a block: the models of the variants
  of variante_prevision (calendar, history, yesterday and the dia_* the
  forecast also gives, with or without warnings and extremes) are fitted on
  the reanalysis and then score the days of anios_prevision twice more, with
  dia_* replaced by prev_*, raw and mapped to the distribution of the
  reanalysis by quantiles (fitted on the other years of the forecast,
  without the labels). The difference is what is lost going from a perfect
  forecast to a real one. prev_ext_* are the extremes of the forecast (the
  forecast gusts over the same climate of each cell, metp1_racha_clima_1h,
  and the run of heat with the forecast hottest hour of D) and take the
  place of ext_* the same way. meteo_dia.comparaciones_prevision compares
  variants on the same weather of the day.
- ext_fraccion_* (block extension, from the literature review of 14.10.2026:
  the area under strong gusts matters more than the strongest point): the
  share of the CT of the distribuidora whose cell has a gust at or above
  its p98 (met_racha_fuerte_1h) and at or above 72 km/h (20 m/s,
  met_racha_72_1h) in the worst hour of D, and the sum of that share over
  the hours of D (CT-hours of strong gust). They come from the reanalysis
  of D, and prev_ext_fraccion_* from the forecasts, like ext_*.
- ext_int_* (block interacciones): the excess of the gust over 0.9 times the
  p98 of its cell (the worst cell, max(0, ratio - 0.9)) times the rain of
  the day before (what is known that evening), the rain of D, the leaf
  season (April to October) and the summer (June to September). The
  literature finds rain and season as multipliers of the gust, not as
  effects of their own. ext_pend_* (block pendiente_red): the gust ratio
  times each distribuidora, a slope of its own per network. Both have their
  forecast twins (prev_ext_int_*, prev_ext_pend_*) built from the forecast
  gust and rain, with the rain of the last day known at that horizon.
- ext_dir_* (block direccion, only with --direccion): how far the wind of
  the hours with gusts at or above the p98 of the cell blows from the
  direction its strong gusts usually come from (the circular mean of the
  reanalysis of the cell), in degrees: the CT weighted mean of the cells,
  averaged over those hours of D (0 on a day without such an hour), and
  that anomaly times the excess of the gust. There is no forecast of the
  direction, so these stay from the reanalysis in the forecast comparison
  (an upper bound).
- seq_* (block sequia): the water balance of the 90 and 180 days before D
  (rain minus the reference evapotranspiration of the reanalysis, per
  distribuidora, ending on D - 1, known that evening) as a z-score of its
  calendar month over the years before sequia_clima_hasta: a long dry spell
  (dead trees, dry fuel) or a long wet one (soaked ground), the SPEI of the
  drought literature computed from what Gold already has.
- hie_horas (block hielo): the CT-hours of D with precipitation at a
  temperature of 0,5 degrees or less (freezing rain or wet snow on the
  conductors), from the reanalysis, with its forecast twin prev_hie_horas.
- fen_* (block fenologia, with meteo_dia.fenologia.csv, the NDVI of MODIS
  per cell of experimentos/fenologia/bajar_ndvi.py): the greenness of the
  network as the latest 16 day composite published latencia_dias before D
  (fen_ndvi), its anomaly against the same period of the years before
  clima_hasta (fen_ndvi_anom) and its change over a month
  (fen_ndvi_cambio_1m); and the gust excess times the greenness
  (fen_int_racha_ndvi, the leaf measured instead of the calendar of the
  interactions), with its forecast twin prev_fen_int_racha_ndvi.
- Two and three days ahead (HORIZONS): Gold also keeps what ICON-EU forecast
  two days before (metp2_*, gusts and rain) and, once landed, three days
  before (metp3_*). They give prev2_* / prev3_* (the daily weather and the
  extremes of the forecast, prev2_ext_*, computed here from the climate of
  each cell when Gold has no metp2_racha_clima_1h), the blocks dia_comun_48h
  and dia_comun_72h (the dia_* those forecasts also give), the warnings of
  AEMET sent before aviso_corte_h of D - 2 and D - 3 (aviso48_*, aviso72_*,
  blocks avisos_48h and avisos_72h) and the last day of weather known that
  evening (hace2_*, hace3_*, the weather of D - 2 and D - 3). A variant with
  "horizonte": 2 (or 3) is scored in the forecast comparison with those
  forecasts and warnings in place of the ones of the day before: what the
  operator would have had two or three evenings before. The history
  (hist_*) is the one known when D starts, a day or two later than that
  evening; with retraso_conocido_dias of a week the difference is small.
- Other weather models (meteo_dia.previsiones_extra, with
  --previsiones-extra): the forecast of the day before of GFS, GEM or any
  other model of the Previous Runs API, landed by
  job_landing_meteo_openmeteo --fuente openmeteo_previo_<clave> and read
  here from its Bronze table, joined per cell and hour (metp1<clave>_*) and
  turned into the same daily weather and extremes (prev<clave>_*). They are
  never features: the forecast comparison scores the variants of
  variante_prevision also with each of them (prevision_<clave>) and with
  the mean of all the models (prevision_media, prevision_media_cuantiles),
  to measure what a second model, or the mean of several, adds to ICON-EU.

Variants (meteo_dia.variantes): blocks, single variables of other blocks
(variables) and first day of training. The variant operativa is the one that
job_ml_riesgo_manana fits and scores every evening: of the day before it
only takes the strongest gust and the rain, which are known that same
evening. Every year Y is scored by models chosen and fitted only on the days
before Y
(rolling_evaluation of job_ml_horas_riesgo), with the strongest gust of the
day before, of the day itself and the highest warning as references.

Every variant also gets rango, the mean of the percentiles in the year of
its logistic regression and its XGBoost.

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
LABELS = ["y_dia", "y_dia_meteo", "y_dia_varias"]
MAIN_LABEL = "y_dia"
WARNING_GROUPS = {"viento": ["VI"], "lluvia": ["PR"], "tormenta": ["TO"], "nieve": ["NE"]}
# What the warnings announce (the value of parametro: "RM;Rachas máximas;80 km/h") and how much of the network and of
# the day they cover.
WARNING_VALUE = r";\s*([\d.,]+)\s*(?:km/h|mm|cm|ºC|°C)"
WARNING_DETAILS = ["aviso_det_racha_kmh", "aviso_det_lluvia_12h_mm", "aviso_det_lluvia_1h_mm", "aviso_det_viento_horas",
                   "aviso_det_viento_ct", "aviso_det_tiempo_ct", "aviso_det_tormenta_ct"]
# Models with one score per variant (the references score the same in every variant that has their feature).
TRAINED = ("logistica", "xgboost", "random_forest", "rango", "logistica_equilibrada", "poisson")

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
    "focos_max": ("metd_max_met_focos_24h", "max"),
    "fwi_max": ("metd_max_met_fwi_ayer", "max"),
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

# Gusts against the climate of each cell (Gold): name -> (hourly column of distributor_weather, aggregation).
# ext_fraccion_*: the CT weighted share of the cells under a strong gust (derived_extent), in the worst hour and
# summed over the hours. ext_dir_*: the anomaly of the direction of the strong gusts (derived_direction).
EXTREME_DAILY = {
    "ext_racha_clima_max": ("metd_max_met_racha_clima_1h", "max"),
    "ext_racha_clima_media": ("metd_media_met_racha_clima_1h", "max"),
    "ext_racha_ku": ("metd_media_met_racha_ku_1h", "sum"),
    "ext_fraccion_p98_max": ("metd_media_met_racha_fuerte_1h", "max"),
    "ext_fraccion_p98_horas": ("metd_media_met_racha_fuerte_1h", "sum"),
    "ext_fraccion_72_max": ("metd_media_met_racha_72_1h", "max"),
    "ext_fraccion_72_horas": ("metd_media_met_racha_72_1h", "sum"),
    "ext_dir_anomalia": ("metd_media_met_dir_anomalia_1h", "mean"),
}
# The same from the forecast of ICON-EU of the day before (prev_ext_*); the direction has no forecast.
FORECAST_EXTREME_DAILY = {
    "ext_racha_clima_max": ("metd_max_metp1_racha_clima_1h", "max"),
    "ext_racha_clima_media": ("metd_media_metp1_racha_clima_1h", "max"),
    "ext_racha_ku": ("metd_media_metp1_racha_ku_1h", "sum"),
    "ext_fraccion_p98_max": ("metd_media_metp1_racha_fuerte_1h", "max"),
    "ext_fraccion_p98_horas": ("metd_media_metp1_racha_fuerte_1h", "sum"),
    "ext_fraccion_72_max": ("metd_media_metp1_racha_72_1h", "max"),
    "ext_fraccion_72_horas": ("metd_media_metp1_racha_72_1h", "sum"),
}
# Precipitation at or below ICE_TEMP_C (derived_extent): the CT weighted share of the cells with it, summed over the
# hours of the day (CT-hours), from the reanalysis and from the forecast of the day before (it has temperature).
ICE_DAILY = {"hie_horas": ("metd_media_met_hielo_1h", "sum")}
FORECAST_ICE_DAILY = {"hie_horas": ("metd_media_metp1_hielo_1h", "sum")}
ICE_TEMP_C = 0.5
# The water balance windows (days) of the drought features and the first year kept out of their climatology.
DROUGHT_WINDOWS = {"seq_3m": 90, "seq_6m": 180}
# A strong gust in km/h (20 m/s: from here on some fault is almost certain in the British overhead networks of
# Dunn et al. 2018) and the share of the p98 from which the interactions and the direction count.
STRONG_GUST_KMH = 72.0
GUST_EXCESS_FROM = 0.9
DIRECTION_FROM = 1.0

# How many days ahead the forecast was made -> the prefix of its hourly
# columns in Gold, of its daily features (prev_*, prev2_*, prev3_*), of the
# warnings sent before aviso_corte_h of D - horizon and of the last day of
# weather known that evening (ayer_* is D - 1, hace2_* is D - 2...).
HORIZONS = {
    1: {"gold": "metp1", "prev": "prev", "aviso": "aviso", "ultimo": "ayer"},
    2: {"gold": "metp2", "prev": "prev2", "aviso": "aviso48", "ultimo": "hace2"},
    3: {"gold": "metp3", "prev": "prev3", "aviso": "aviso72", "ultimo": "hace3"},
}


# Other weather models of the Previous Runs API (meteo_dia.previsiones_extra,
# only with --previsiones-extra): landed as they come by
# job_landing_meteo_openmeteo --fuente openmeteo_previo_<clave> and left in
# Bronze (l1_bronze.meteo_openmeteo_previo_<clave>_hora) without Silver or
# Gold. Their forecast of the day before joins the frame of Gold per cell and
# hour with the prefix metp1<clave>_, gets the same extremes over the climate
# of the cell, and gives the daily prev<clave>_* like metp1_ gives prev_*. They
# are never features: the forecast comparison scores the variants of
# variante_prevision also with each of them (prevision_<clave>) and with the
# mean of all the models (prevision_media, and prevision_media_cuantiles). What
# a second model, or the mean of several, adds to the forecast of ICON-EU.
EXTRA_FORECAST_COLUMNS = {
    "wind_gusts_10m_previous_day1": "racha_1h",
    "wind_speed_10m_previous_day1": "viento_1h",
    "precipitation_previous_day1": "lluvia_1h",
    "temperature_2m_previous_day1": "temp_1h",
    "relative_humidity_2m_previous_day1": "humedad_1h",
}
MEAN_SOURCE = "prevision_media"


def horizon_of(variant: dict) -> int:
    return int(variant.get("horizonte", 1))


def extra_forecasts(cfg: dict) -> list:
    """
    [{clave, tabla, nombre, gold, prev}] of meteo_dia.previsiones_extra when
    the run asks for them (comparar_previsiones_extra): the prefix of their
    hourly columns (metp1<clave>) and of their daily ones (prev<clave>).
    """
    if not cfg.get("comparar_previsiones_extra"):
        return []
    out = []
    for item in cfg.get("previsiones_extra") or []:
        key = str(item["clave"]).strip().lower()
        out.append({**item, "clave": key, "nombre": item.get("nombre", key), "gold": f"metp1{key}", "prev": f"prev{key}"})
    return out


def forecast_prefixes(extras: list = ()) -> list:
    """The prefixes of the hourly forecasts in the frame of Gold: the horizons of ICON-EU and the other models."""
    return [spec["gold"] for spec in HORIZONS.values()] + [extra["gold"] for extra in extras]


def with_extra_forecasts(spark, extras: list, derive):
    """
    The derive hook of distributor_weather with the forecasts of the other
    models (extras) joined to the frame of Gold per cell and hour before
    derive runs: the columns of EXTRA_FORECAST_COLUMNS of each Bronze table
    as metp1<clave>_*. A model whose table is missing, or has no gust, is
    marked (presente False) and left out.
    """
    def hook(weather):
        for extra in extras:
            table = extra["tabla"]
            extra["presente"] = False
            if not spark.catalog.tableExists(table):
                logger.warning("Forecast of %s not in Bronze (%s): the comparison goes on without it",
                               extra["nombre"], table)
                continue
            source = spark.table(table)
            columns = [(raw, f"{extra['gold']}_{name}") for raw, name in EXTRA_FORECAST_COLUMNS.items()
                       if raw in source.columns]
            if not any(name.endswith("_racha_1h") for _, name in columns):
                logger.warning("Forecast of %s (%s) has no gust (wind_gusts_10m_previous_day1): left out",
                               extra["nombre"], table)
                continue
            frame = source.select("celda_id", F.col("hora").cast("timestamp").alias("hora"),
                                  *[F.col(raw).cast("double").alias(name) for raw, name in columns])
            weather = weather.join(frame, ["celda_id", "hora"], "left")
            extra["presente"] = True
            logger.info("Forecast of %s joined from %s: %s", extra["nombre"], table, [n for _, n in columns])
        return derive(weather)
    return hook


def common_block(horizon: int) -> str:
    """The block of the dia_* that the forecast of the horizon also gives."""
    return "dia_comun" if horizon == 1 else f"dia_comun_{24 * horizon}h"


def forecast_mapping(mapping: dict, gold_prefix: str) -> dict:
    """FORECAST_DAILY or FORECAST_EXTREME_DAILY read from another forecast of Gold (metp2_, metp3_)."""
    return {name: (column.replace("metp1_", f"{gold_prefix}_", 1), how) for name, (column, how) in mapping.items()}


def derived_forecast_extremes(weather, prefixes: list = None):
    """
    The frame of Gold with metp2_racha_clima_1h / metp2_racha_ku_1h (and
    metp3_*, and the other models' metp1<clave>_*) when it has the forecast
    gusts of two (three) days before but not their extremes: the same
    climate of each cell that Gold used for met_racha_clima_1h (the gust of
    the hour over that ratio), so the forecasts of every horizon and model
    go over the same p98.
    """
    columns = set(weather.columns)
    if "met_racha_clima_1h" not in columns or "met_racha_1h" not in columns:
        return weather
    climate = F.when(F.col("met_racha_clima_1h") > 0, F.col("met_racha_1h") / F.col("met_racha_clima_1h"))
    for prefix in (prefixes if prefixes is not None else forecast_prefixes()):
        if f"{prefix}_racha_1h" not in columns or f"{prefix}_racha_clima_1h" in columns:
            continue
        ratio = F.col(f"{prefix}_racha_1h") / climate
        weather = (weather.withColumn(f"{prefix}_racha_clima_1h", ratio)
                   .withColumn(f"{prefix}_racha_ku_1h",
                               F.pow(F.greatest(ratio - F.lit(1.0), F.lit(0.0)), 3)))
    return weather


def derived_extent(weather, prefixes: list = None):
    """
    The frame of Gold with <prefix>_racha_fuerte_1h (1 when the gust of the
    hour is at or above the p98 of the cell, 0 below, null without a gust)
    and <prefix>_racha_72_1h (the same at STRONG_GUST_KMH) for the
    reanalysis and every forecast that has its gust: their CT weighted mean
    over the cells is the share of the network under a strong gust.
    """
    columns = set(weather.columns)
    for prefix in ["met"] + list(prefixes if prefixes is not None else forecast_prefixes()):
        gust, ratio = f"{prefix}_racha_1h", f"{prefix}_racha_clima_1h"
        if ratio in columns and f"{prefix}_racha_fuerte_1h" not in columns:
            weather = weather.withColumn(f"{prefix}_racha_fuerte_1h",
                                         F.when(F.col(ratio).isNotNull(), (F.col(ratio) >= 1.0).cast("double")))
        if gust in columns and f"{prefix}_racha_72_1h" not in columns:
            weather = weather.withColumn(f"{prefix}_racha_72_1h",
                                         F.when(F.col(gust).isNotNull(),
                                                (F.col(gust) >= STRONG_GUST_KMH).cast("double")))
        rain, temp = f"{prefix}_lluvia_1h", f"{prefix}_temp_1h"
        if rain in columns and temp in columns and f"{prefix}_hielo_1h" not in columns:
            weather = weather.withColumn(f"{prefix}_hielo_1h",
                                         F.when(F.col(rain).isNotNull() & F.col(temp).isNotNull(),
                                                ((F.col(rain) > 0) & (F.col(temp) <= ICE_TEMP_C)).cast("double")))
    return weather


def drought_features(spark, weather, start: pd.Timestamp, end: pd.Timestamp, climatology_until: pd.Timestamp,
                     windows: dict = None) -> pd.DataFrame:
    """
    (distribuidora_id, fecha, seq_3m, seq_6m): the rain minus the reference
    evapotranspiration of the windows days before D (ending on D - 1, what
    is known that evening) as a z-score of its calendar month, with the mean
    and the spread of the days before climatology_until, per distribuidora.
    Empty without the rain or the evapotranspiration in Gold.
    """
    windows = windows or DROUGHT_WINDOWS
    rain, et0 = "metd_media_met_lluvia_1h", "metd_media_met_et0_7d"
    if rain not in weather.columns or et0 not in weather.columns:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"])
    longest = max(windows.values())
    frame = (weather.where((F.col("hora") >= F.lit(start - pd.Timedelta(days=longest + 10))) & (F.col("hora") < F.lit(end)))
             .select("distribuidora_id", "hora", rain, et0).toPandas())
    if frame.empty:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"])
    frame["distribuidora_id"] = frame["distribuidora_id"].astype("int64")
    frame["fecha"] = pd.to_datetime(frame["hora"]).dt.floor("D")
    daily = frame.groupby(["distribuidora_id", "fecha"]).agg(lluvia=(rain, "sum"), et0_7d=(et0, "max")).reset_index()
    # The 7 day sum of Gold at the end of the day, a seventh of it as the evapotranspiration of the day.
    daily["balance"] = daily["lluvia"] - daily["et0_7d"] / 7.0
    parts = []
    for dist, g in daily.groupby("distribuidora_id"):
        series = g.set_index("fecha")["balance"].sort_index()
        series = series.reindex(pd.date_range(series.index.min(), series.index.max(), freq="D"))
        out = pd.DataFrame({"fecha": series.index, "distribuidora_id": dist})
        for name, days in windows.items():
            # The sum up to D - 1, so the evening before D already knows it.
            out[name] = series.rolling(days, min_periods=int(days * 0.9)).sum().shift(1).to_numpy()
        parts.append(out)
    out = pd.concat(parts, ignore_index=True)
    out["_mes"] = out["fecha"].dt.month
    climate = out[out["fecha"] < climatology_until]
    for name in windows:
        stats = climate.groupby(["distribuidora_id", "_mes"])[name].agg(["mean", "std"]).reset_index()
        out = out.merge(stats, on=["distribuidora_id", "_mes"], how="left")
        out[name] = (out[name] - out["mean"]) / out["std"].where(out["std"] > 0)
        out = out.drop(columns=["mean", "std"])
    out = out.drop(columns="_mes")
    return out[(out["fecha"] >= start) & (out["fecha"] < end)].reset_index(drop=True)


def phenology_features(spark, cfg: dict, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """
    (distribuidora_id, fecha, fen_ndvi, fen_ndvi_anom, fen_ndvi_cambio_1m):
    the greenness of the network from the 16 day NDVI composites of MODIS
    per cell (experimentos/fenologia/bajar_ndvi.py, meteo_dia.fenologia.csv),
    weighted by the CT of each cell: the latest composite whose period had
    ended latencia_dias before D (what is published by that evening), its
    anomaly as a z-score of its 16 day period of the year over the
    composites before clima_hasta, and its change against the composite of
    the same satellite a month before (leaves coming or going). Empty
    without the file or the cells.
    """
    settings = cfg.get("fenologia") or {}
    path = settings.get("csv")
    empty = pd.DataFrame(columns=["distribuidora_id", "fecha"])
    if not path or not os.path.exists(path) or not spark.catalog.tableExists(CELLS_TABLE):
        if path:
            logger.warning("No phenology: %s or %s not found", path, CELLS_TABLE)
        return empty
    latency = int(settings.get("latencia_dias", 7))
    until = pd.Timestamp(settings.get("clima_hasta", "2022-01-01"))
    rows = pd.read_csv(path, usecols=["celda_id", "plataforma", "fecha_inicio", "fecha_fin", "ndvi", "n_validos",
                                      "n_pixeles"])
    rows = rows[rows["ndvi"].notna() & (rows["n_validos"] > 0)]
    if rows.empty:
        return empty
    # A cell on the edge of two MODIS tiles has a row per tile: one value per cell and composite, weighted by the
    # reliable pixels of each part.
    rows = rows.assign(_w=rows["ndvi"] * rows["n_validos"], fecha_fin=rows["fecha_fin"].astype(str))
    rows = (rows.groupby(["celda_id", "plataforma", "fecha_inicio"], as_index=False)
            .agg(_w=("_w", "sum"), n_validos=("n_validos", "sum"), n_pixeles=("n_pixeles", "sum"), fecha_fin=("fecha_fin", "max")))
    rows["ndvi"] = rows["_w"] / rows["n_validos"]
    rows = rows[rows["n_validos"] >= float(settings.get("fraccion_valida", 0.5)) * rows["n_pixeles"]].drop(columns="_w")
    if rows.empty:
        return empty
    weights = (spark.table(CELLS_TABLE).where(F.col("celda_id").isNotNull())
               .groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id")
               .agg(F.count(F.lit(1)).cast("double").alias("n_ct")).toPandas())
    joined = rows.merge(weights, on="celda_id", how="inner")
    if joined.empty:
        logger.warning("No phenology: the cells of %s are not the ones of %s", path, CELLS_TABLE)
        return empty
    joined["_w"] = joined["ndvi"] * joined["n_ct"]
    keys = ["distribuidora_id", "plataforma", "fecha_inicio"]
    per = joined.groupby(keys).agg(_w=("_w", "sum"), n_ct=("n_ct", "sum"), fecha_fin=("fecha_fin", "first")).reset_index()
    total = weights.groupby("distribuidora_id")["n_ct"].sum()
    # A composite that covers less than half of the CT of the network (a tile missing) is left out.
    per = per[per["n_ct"] >= 0.5 * per["distribuidora_id"].map(total)]
    per["ndvi"] = per["_w"] / per["n_ct"]
    per["fecha_inicio"] = pd.to_datetime(per["fecha_inicio"])
    per["fecha_fin"] = pd.to_datetime(per["fecha_fin"])
    per["_periodo"] = (per["fecha_inicio"].dt.dayofyear - 1) // 16
    climate = per[per["fecha_inicio"] < until].groupby(["distribuidora_id", "_periodo"])["ndvi"].agg(["mean", "std"]).reset_index()
    per = per.merge(climate, on=["distribuidora_id", "_periodo"], how="left")
    per["anom"] = (per["ndvi"] - per["mean"]) / per["std"].where(per["std"] > 0)
    per = per.sort_values(["distribuidora_id", "plataforma", "fecha_inicio"])
    # The same satellite two composites before: about a month.
    per["cambio"] = per["ndvi"] - per.groupby(["distribuidora_id", "plataforma"])["ndvi"].shift(2)
    per["conocido"] = per["fecha_fin"] + pd.Timedelta(days=latency)
    per = per.sort_values("conocido")

    days = pd.date_range(start, end - pd.Timedelta(days=1), freq="D")
    parts = []
    for dist, g in per.groupby("distribuidora_id"):
        frame = pd.DataFrame({"fecha": days})
        found = pd.merge_asof(frame, g[["conocido", "ndvi", "anom", "cambio"]].rename(columns={"conocido": "fecha"}),
                              on="fecha", direction="backward", tolerance=pd.Timedelta(days=60))
        found["distribuidora_id"] = dist
        parts.append(found)
    out = pd.concat(parts, ignore_index=True).rename(columns={"ndvi": "fen_ndvi", "anom": "fen_ndvi_anom",
                                                                "cambio": "fen_ndvi_cambio_1m"})
    logger.info("Phenology of %s composites from %s: %.1f %% of the days with a value", len(per), path,
                100 * out["fen_ndvi"].notna().mean())
    return out[["distribuidora_id", "fecha", "fen_ndvi", "fen_ndvi_anom", "fen_ndvi_cambio_1m"]]


def derived_direction(weather):
    """
    The frame of Gold with met_dir_anomalia_1h: in the hours with a gust at
    or above DIRECTION_FROM times the p98 of the cell, the angle (0 - 180
    degrees) between the wind of the hour and the direction the strong
    gusts of the cell usually come from (the circular mean of the hours of
    the reanalysis at or above the p98). Null in the other hours and when
    Gold has no direction.
    """
    columns = set(weather.columns)
    if not {"met_dir_viento_sin", "met_dir_viento_cos", "met_racha_clima_1h"} <= columns:
        return weather
    strong = weather.where(F.col("met_racha_clima_1h") >= 1.0)
    usual = strong.groupBy("celda_id").agg(F.avg("met_dir_viento_sin").alias("_dir_sin"),
                                           F.avg("met_dir_viento_cos").alias("_dir_cos"))
    norm = F.sqrt(F.col("_dir_sin") ** 2 + F.col("_dir_cos") ** 2)
    usual = usual.where(norm > 0).select("celda_id", (F.col("_dir_sin") / norm).alias("_dir_sin"),
                                         (F.col("_dir_cos") / norm).alias("_dir_cos"))
    joined = weather.join(F.broadcast(usual), "celda_id", "left")
    cosine = F.col("met_dir_viento_sin") * F.col("_dir_sin") + F.col("met_dir_viento_cos") * F.col("_dir_cos")
    angle = F.degrees(F.acos(F.greatest(F.least(cosine, F.lit(1.0)), F.lit(-1.0))))
    return joined.withColumn("met_dir_anomalia_1h",
                             F.when(F.col("met_racha_clima_1h") >= DIRECTION_FROM, angle)).drop("_dir_sin", "_dir_cos")


def derive_weather(direction: bool = False, extras: list = ()):
    """
    The derive hook of distributor_weather: the extremes of the forecasts (of
    ICON-EU at every horizon and of the other models, extras), the extent
    and, if asked, the direction.
    """
    prefixes = forecast_prefixes(extras)

    def derive(weather):
        weather = derived_extent(derived_forecast_extremes(weather, prefixes), prefixes)
        return derived_direction(weather) if direction else weather
    return derive


def gust_excess(ratio: pd.Series) -> pd.Series:
    """How far the gust goes above GUST_EXCESS_FROM times the p98 of its cell (0 below, null without a gust)."""
    return (ratio - GUST_EXCESS_FROM).clip(lower=0.0)


def add_interactions(pdf: pd.DataFrame, dists: list, forecasts: dict, extras: list = ()) -> list:
    """
    Adds to pdf, and returns the names of, the interactions of the gust
    (ext_int_*, block interacciones), the slope of the gust per network
    (ext_pend_*, block pendiente_red) and the anomaly of the direction times
    the gust (ext_dir_int_racha, block direccion), each with its forecast
    twin per horizon (<prev>_ext_*, returned too) and per other model
    (prev<clave>_ext_*) so the forecast comparison swaps them like the other
    extremes. The rain of the day before is the one known at the horizon
    (ayer_, hace2_, hace3_).
    """
    month = pdf["fecha"].dt.month
    leaf = month.between(4, 10).astype("float64")
    summer = month.between(6, 9).astype("float64")
    names = []
    versions = [("", "ext_racha_clima_max", "ayer_lluvia_max", "dia_lluvia_max")]
    for horizon in sorted(forecasts):
        spec = HORIZONS[horizon]
        versions.append((f"{spec['prev']}_", f"{spec['prev']}_ext_racha_clima_max",
                         f"{spec['ultimo']}_lluvia_max", f"{spec['prev']}_lluvia_max"))
    for extra in extras:
        versions.append((f"{extra['prev']}_", f"{extra['prev']}_ext_racha_clima_max", "ayer_lluvia_max",
                         f"{extra['prev']}_lluvia_max"))
    for prefix, gust, rain_before, rain_day in versions:
        if gust not in pdf.columns:
            continue
        ratio = pdf[gust]
        excess = gust_excess(ratio)
        new = {
            f"{prefix}ext_int_racha_lluvia_previa": excess * pdf[rain_before] if rain_before in pdf.columns else None,
            f"{prefix}ext_int_racha_lluvia_dia": excess * pdf[rain_day] if rain_day in pdf.columns else None,
            f"{prefix}ext_int_racha_hoja": excess * leaf,
            f"{prefix}ext_int_racha_verano": excess * summer,
        }
        for dist in dists:
            new[f"{prefix}ext_pend_racha_dist_{dist}"] = ratio * pdf[f"dist_{dist}"]
        if prefix == "" and "ext_dir_anomalia" in pdf.columns:
            new["ext_dir_int_racha"] = excess * (pdf["ext_dir_anomalia"] / 90.0)
        # The gust over the greenness of the network (block fenologia): the leaf as measured, not as a calendar.
        if "fen_ndvi" in pdf.columns:
            new[f"{prefix}fen_int_racha_ndvi"] = excess * pdf["fen_ndvi"]
        for name, values in new.items():
            if values is None:
                continue
            pdf[name] = values.astype("float64")
            names.append(name)
    return names


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
    "incidencias_dia_varias": 2,
    "calor_umbral_c": 35.0,
    "modelos": ["tasa_base", "logistica", "xgboost"],
    # The counts the model poisson learns from (n_dia, the incidents that start on D).
    "etiqueta_conteo": "n_dia",
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
        "logistica_equilibrada": [{"C": 0.003}, {"C": 0.01}, {"C": 0.03}, {"C": 0.1}],
        "poisson": [{"alpha": 10.0}, {"alpha": 3.0}, {"alpha": 1.0}, {"alpha": 0.3}],
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
    parser.add_argument("--direccion", action="store_true",
                        help="Also the block direccion (the anomaly of the wind direction, reanalysis only)")
    parser.add_argument("--previsiones-extra", action="store_true",
                        help="Also score the forecast comparison with the other models of meteo_dia.previsiones_extra "
                             "(their Bronze tables) and with the mean of all the models")
    parser.add_argument("--modelos", default=None,
                        help="Comma separated models to train in place of meteo_dia.modelos")
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


def extremes(hourly: pd.DataFrame, daily: pd.DataFrame, heat_c: float, min_hours: int) -> pd.DataFrame:
    """
    (distribuidora_id, fecha, ext_*): the gusts of every day against the
    climate of each cell (EXTREME_DAILY, from met_racha_clima_1h and
    met_racha_ku_1h of Gold) and the days in a row with temp_max of daily at
    heat_c or more, up to the day. A day with fewer than min_hours hours of
    gusts is left null.
    """
    out = daily_weather(hourly, min_hours, EXTREME_DAILY).set_index(["distribuidora_id", "fecha"]).sort_index()
    if "temp_max" in daily.columns:
        def run(values: pd.Series) -> pd.Series:
            count, counts = 0.0, []
            for value in values.to_numpy():
                count = count + 1.0 if value >= heat_c else 0.0
                counts.append(count if value == value else np.nan)
            return pd.Series(counts, index=values.index)

        heat = daily.sort_values(["distribuidora_id", "fecha"]).set_index(["distribuidora_id", "fecha"])["temp_max"]
        out = out.join(heat.groupby(level="distribuidora_id", group_keys=False).apply(run).rename("ext_dias_calor"),
                       how="outer")
    return out.reset_index()


def forecast_extremes(hourly: pd.DataFrame, forecast: pd.DataFrame, extreme: pd.DataFrame, heat_c: float,
                      min_hours: int, mapping: dict = None, prefix: str = "prev") -> pd.DataFrame:
    """
    (distribuidora_id, fecha, prev_ext_*): the extremes of the day from the
    forecast of ICON-EU of the day before (FORECAST_EXTREME_DAILY) and the
    days in a row of heat with the forecast temp_max of the day (forecast)
    on top of the run of the reanalysis up to the day before (ext_dias_calor
    of extreme on D - 1), what is known the evening before. With mapping and
    prefix, the same from another forecast of Gold (prev2_ext_* from metp2_);
    the run of heat only when that forecast has the temperature.
    """
    out = daily_weather(hourly, min_hours, mapping or FORECAST_EXTREME_DAILY)
    out = out.rename(columns={c: f"{prefix}_{c}" for c in out.columns if c not in ("distribuidora_id", "fecha")})
    if "temp_max" in forecast.columns and "ext_dias_calor" in extreme.columns:
        before = extreme[["distribuidora_id", "fecha", "ext_dias_calor"]].assign(
            fecha=extreme["fecha"] + pd.Timedelta(days=1))
        heat = forecast[["distribuidora_id", "fecha", "temp_max"]].merge(before, on=["distribuidora_id", "fecha"],
                                                                         how="left")
        hot = (heat["temp_max"] >= heat_c).to_numpy()
        run = np.where(hot, heat["ext_dias_calor"].to_numpy(dtype=np.float64) + 1.0, 0.0)
        run[heat["temp_max"].isna().to_numpy() | (hot & heat["ext_dias_calor"].isna().to_numpy())] = np.nan
        heat[f"{prefix}_ext_dias_calor"] = run
        out = out.merge(heat[["distribuidora_id", "fecha", f"{prefix}_ext_dias_calor"]],
                        on=["distribuidora_id", "fecha"], how="outer")
    return out


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


def day_labels(grid: pd.DataFrame, events: pd.DataFrame, excluded: list, several: int = 2) -> pd.DataFrame:
    """
    y_dia, y_dia_meteo and y_dia_varias (several or more incidents start that
    day, the days that need more than one crew) of every (distribuidora_id,
    fecha) of the grid.
    """
    starts = events.dropna(subset=["inicio_ts"]).assign(fecha=lambda e: pd.to_datetime(e["inicio_ts"]).dt.floor("D"))
    weather_related = ~starts["causa"].isin(set(excluded))
    incident = [c for c in ("periodo_id", "incidencia_id") if c in starts.columns] or ["inicio_ts"]
    counts = starts.drop_duplicates(["distribuidora_id", "fecha"] + incident).groupby(["distribuidora_id", "fecha"]).size()
    busy = counts[counts >= int(several)].reset_index()[["distribuidora_id", "fecha"]]

    for label, keys in (("y_dia", starts), ("y_dia_meteo", starts[weather_related]), ("y_dia_varias", busy)):
        keys = keys[["distribuidora_id", "fecha"]].drop_duplicates().assign(**{label: 1})
        grid = grid.merge(keys, on=["distribuidora_id", "fecha"], how="left")
        grid[label] = grid[label].fillna(0).astype("int8")
    # n_dia: how many incidents start on D (the counts the model poisson learns from; 0 without any).
    grid = grid.merge(counts.rename("n_dia").reset_index(), on=["distribuidora_id", "fecha"], how="left")
    grid["n_dia"] = grid["n_dia"].fillna(0).astype("int16")

    return grid


def warning_days(warnings: pd.DataFrame, cells: pd.DataFrame, cutoff_h: int, lead_days: int = 1) -> tuple:
    """
    (aviso_* by (distribuidora_id, fecha), first day of the archive): the
    highest level of the warnings that cover some hour of the day in a cell
    of the distribuidora and were sent before cutoff_h of the day before
    (of lead_days days before: 2 for what was known two evenings before).
    Cancellations and green levels do not count. Also the details of those
    warnings (WARNING_DETAILS): the strongest gust and the most rain they
    announce (in 12 h and in one hour), the most hours of the day under a
    wind warning, and the share of the CT of the distribuidora (n_ct of
    cells, 1 per cell without it) in cells under a wind warning, under any
    warning of wind, rain, thunderstorms or snow, and under a thunderstorm
    warning.
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
    text = w["parametro"].fillna("").astype(str) if "parametro" in w.columns else pd.Series("", index=w.index)
    w["valor"] = pd.to_numeric(text.str.extract(WARNING_VALUE)[0].str.replace(",", ".", regex=False), errors="coerce")
    w["clave"] = text.str.split(";").str[0].str.strip().str.upper()
    rows = []
    for record in w[["celda_id", "fenomeno_codigo", "nivel_num", "valor", "clave", "enviado_ts", "inicio_ts",
                     "fin_ts"]].itertuples(index=False):
        first = record.inicio_ts.floor("D")
        # A warning that ends exactly at midnight does not cover the next day.
        last = (record.fin_ts - pd.Timedelta(seconds=1)).floor("D")
        for day in pd.date_range(first, max(first, last), freq="D"):
            # Known the evening before: sent before cutoff_h of D - lead_days.
            if record.enviado_ts <= day - pd.Timedelta(hours=24 * int(lead_days) - int(cutoff_h)):
                hours = (min(record.fin_ts, day + pd.Timedelta(days=1)) - max(record.inicio_ts, day)) / pd.Timedelta(hours=1)
                rows.append((record.celda_id, day, record.fenomeno_codigo, float(record.nivel_num), record.valor,
                             record.clave, max(0.0, float(hours))))

    if not rows:
        return pd.DataFrame(columns=["distribuidora_id", "fecha"]), first_day

    per_cell = pd.DataFrame(rows, columns=["celda_id", "fecha", "fenomeno_codigo", "nivel_num", "valor", "clave",
                                           "horas"])
    weights = cells.drop_duplicates(["distribuidora_id", "celda_id"]).copy()
    if "n_ct" not in weights.columns:
        weights["n_ct"] = 1.0
    weights = weights[["distribuidora_id", "celda_id", "n_ct"]]
    joined = per_cell.merge(weights, on="celda_id")
    keys = ["distribuidora_id", "fecha"]
    out = joined.groupby(keys)["nivel_num"].max().rename("aviso_max_nivel").to_frame()

    for name, codes in WARNING_GROUPS.items():
        part = joined[joined["fenomeno_codigo"].isin(codes)]
        out[f"aviso_{name}_nivel"] = part.groupby(keys)["nivel_num"].max()

    wind = joined[joined["fenomeno_codigo"].isin(WARNING_GROUPS["viento"])]
    rain = joined[joined["fenomeno_codigo"].isin(WARNING_GROUPS["lluvia"])]
    storm = joined[joined["fenomeno_codigo"].isin(WARNING_GROUPS["tormenta"])]
    weather = joined[joined["fenomeno_codigo"].isin([c for codes in WARNING_GROUPS.values() for c in codes])]
    out["aviso_det_racha_kmh"] = wind.groupby(keys)["valor"].max()
    out["aviso_det_lluvia_12h_mm"] = rain[rain["clave"] == "P2"].groupby(keys)["valor"].max()
    out["aviso_det_lluvia_1h_mm"] = rain[rain["clave"] == "P1"].groupby(keys)["valor"].max()
    out["aviso_det_viento_horas"] = wind.groupby(keys)["horas"].max()
    total = weights.groupby("distribuidora_id")["n_ct"].sum()
    for name, part in (("viento", wind), ("tiempo", weather), ("tormenta", storm)):
        covered = part.drop_duplicates(keys + ["celda_id"]).groupby(keys)["n_ct"].sum()
        out[f"aviso_det_{name}_ct"] = covered / covered.index.get_level_values("distribuidora_id").map(total).to_numpy()

    return out.reset_index(), first_day


def block_of(name: str) -> str:
    for prefix, block in (("cal_", "calendario"), ("hist_", "historia"), ("dist_", "distribuidora"),
                          ("ayer_", "ayer"), ("hace2_", "hace2"), ("hace3_", "hace3"), ("dia_", "dia"),
                          ("aviso_det_", "avisos_detalle"), ("aviso_", "avisos"),
                          ("aviso48_det_", "avisos_detalle_48h"), ("aviso48_", "avisos_48h"),
                          ("aviso72_det_", "avisos_detalle_72h"), ("aviso72_", "avisos_72h"),
                          ("prev_", "prevision"), ("prev2_", "prevision_48h"), ("prev3_", "prevision_72h"),
                          ("ext_fraccion_", "extension"), ("ext_int_", "interacciones"),
                          ("ext_pend_", "pendiente_red"), ("ext_dir_", "direccion"), ("ext_", "extremos"),
                          ("seq_", "sequia"), ("hie_", "hielo"), ("fen_", "fenologia")):
        if name.startswith(prefix):
            return block
    return "calendario" if name == SEASON_FEATURE else "otros"


def build_dataset(spark, config: dict, cfg: dict) -> tuple:
    """(pdf, {block: feature names}, first day of the warnings) of the distribuidora-day history."""
    start, end = pd.Timestamp(cfg["desde"]), pd.Timestamp(cfg["hasta"])
    extras = extra_forecasts(cfg)
    gold_prefixes = tuple(f"{prefix}_" for prefix in forecast_prefixes(extras))
    derive = derive_weather(bool(cfg.get("bloque_direccion", False)), extras)
    if extras:
        derive = with_extra_forecasts(spark, extras, derive)
    weather, names = distributor_weather(spark, {**config.get("meteo", {}), "activo": True},
                                         prefixes=("met_",) + gold_prefixes, derive=derive)
    if weather is None:
        raise ValueError("No weather in Gold (meteo_celda_hora and map_ct_celda): nothing to learn from")
    extras = [extra for extra in extras if extra.get("presente")]

    # The daily weather of every forecast of Gold: horizon -> (daily mapping, extremes mapping).
    mappings = {h: (forecast_mapping(FORECAST_DAILY, spec["gold"]), forecast_mapping(FORECAST_EXTREME_DAILY, spec["gold"]))
                for h, spec in HORIZONS.items()}
    sources = list(DAILY.values()) + list(EXTREME_DAILY.values()) + list(ICE_DAILY.values()) + list(FORECAST_ICE_DAILY.values())
    for daily_map, extreme_map in mappings.values():
        sources += list(daily_map.values()) + list(extreme_map.values())
    for extra in extras:
        for mapping in (FORECAST_DAILY, FORECAST_EXTREME_DAILY, FORECAST_ICE_DAILY):
            sources += list(forecast_mapping(mapping, extra["gold"]).values())
    wanted = [c for c in {spec[0] for spec in sources} if c in names]
    # The days before the first day need their weather too (ayer_*, hace2_*, hace3_*).
    hourly = weather.where((F.col("hora") >= F.lit(start - pd.Timedelta(days=max(HORIZONS))))
                           & (F.col("hora") < F.lit(end)))
    hourly = hourly.select("distribuidora_id", "hora", *wanted).toPandas()
    hourly["distribuidora_id"] = hourly["distribuidora_id"].astype("int64")
    hourly["hora"] = pd.to_datetime(hourly["hora"])
    daily = daily_weather(hourly, int(cfg["horas_minimas_dia"]))
    weather_names = [c for c in daily.columns if c not in ("distribuidora_id", "fecha")]
    forecasts = {}
    for horizon, (daily_map, _) in mappings.items():
        frame = daily_weather(hourly, int(cfg["horas_minimas_dia"]), daily_map)
        found = [c for c in frame.columns if c not in ("distribuidora_id", "fecha")]
        if found:
            forecasts[horizon] = (frame, found)

    today = daily.rename(columns={c: f"dia_{c}" for c in weather_names})

    days = pd.date_range(start, end - pd.Timedelta(days=1), freq="D")
    dists = sorted(daily["distribuidora_id"].unique())
    pdf = pd.MultiIndex.from_product([dists, days], names=["distribuidora_id", "fecha"]).to_frame(index=False)
    pdf = pdf[~pdf["fecha"].isin(pd.to_datetime(cfg["dias_excluidos"]))].reset_index(drop=True)
    pdf = pdf.merge(today, on=["distribuidora_id", "fecha"], how="left")
    # The last day of weather known the evening of each horizon: ayer_* (D - 1), hace2_* (D - 2), hace3_* (D - 3).
    before = {}
    for horizon, spec in HORIZONS.items():
        shifted = daily.assign(fecha=daily["fecha"] + pd.Timedelta(days=horizon)).rename(
            columns={c: f"{spec['ultimo']}_{c}" for c in weather_names})
        pdf = pdf.merge(shifted, on=["distribuidora_id", "fecha"], how="left")
        before[horizon] = [f"{spec['ultimo']}_{c}" for c in weather_names]
    extreme = extremes(hourly, daily, float(cfg["calor_umbral_c"]), int(cfg["horas_minimas_dia"]))
    extreme_names = [c for c in extreme.columns if c.startswith("ext_")]
    pdf = pdf.merge(extreme, on=["distribuidora_id", "fecha"], how="left")
    # Freezing precipitation of D (hie_horas) and its forecast twin (prev_hie_horas); the drought of the months before.
    ice_names = []
    ice = daily_weather(hourly, int(cfg["horas_minimas_dia"]), ICE_DAILY)
    if "hie_horas" in ice.columns:
        pdf = pdf.merge(ice, on=["distribuidora_id", "fecha"], how="left")
        ice_names = ["hie_horas"]
        ice_forecast = daily_weather(hourly, int(cfg["horas_minimas_dia"]), FORECAST_ICE_DAILY)
        if "hie_horas" in ice_forecast.columns:
            pdf = pdf.merge(ice_forecast.rename(columns={"hie_horas": "prev_hie_horas"}),
                            on=["distribuidora_id", "fecha"], how="left")
    drought = drought_features(spark, weather, start, end, pd.Timestamp(cfg.get("sequia_clima_hasta", "2022-01-01")))
    drought_names = [c for c in drought.columns if c.startswith("seq_")]
    if drought_names:
        pdf = pdf.merge(drought, on=["distribuidora_id", "fecha"], how="left")
    # The greenness of the network (MODIS NDVI per cell, known with the latency of the composites). An optional
    # block: a problem with its file must not stop the evening run that only uses the operational variant.
    try:
        phenology = phenology_features(spark, cfg, start, end)
    except Exception as error:  # noqa: BLE001
        logger.warning("No phenology (%s): the dataset goes on without the block fenologia", str(error)[:200])
        phenology = pd.DataFrame(columns=["distribuidora_id", "fecha"])
    phenology_names = [c for c in phenology.columns if c.startswith("fen_")]
    if phenology_names:
        pdf = pdf.merge(phenology, on=["distribuidora_id", "fecha"], how="left")
    # A day with gusts but none strong enough to measure the direction: no anomaly, not a missing one.
    if "ext_dir_anomalia" in pdf.columns:
        pdf["ext_dir_anomalia"] = pdf["ext_dir_anomalia"].where(
            pdf["ext_racha_clima_max"].isna(), pdf["ext_dir_anomalia"].fillna(0.0))
    forecast_columns = []
    for horizon, (frame, found) in forecasts.items():
        prefix = HORIZONS[horizon]["prev"]
        pdf = pdf.merge(frame.rename(columns={c: f"{prefix}_{c}" for c in found}),
                        on=["distribuidora_id", "fecha"], how="left")
        forecast_columns += [f"{prefix}_{c}" for c in found]
        forecast_extreme = forecast_extremes(hourly, frame, extreme, float(cfg["calor_umbral_c"]),
                                             int(cfg["horas_minimas_dia"]), mappings[horizon][1], prefix)
        extreme_found = [c for c in forecast_extreme.columns if c.startswith(f"{prefix}_ext_")]
        if extreme_found:
            pdf = pdf.merge(forecast_extreme, on=["distribuidora_id", "fecha"], how="left")
            forecast_columns += extreme_found

    # The same daily weather, extremes and freezing hours from the forecast of the day before of every other model
    # (prev<clave>_*): for the forecast comparison only, never features.
    present = []
    for extra in extras:
        prefix = extra["prev"]
        frame = daily_weather(hourly, int(cfg["horas_minimas_dia"]), forecast_mapping(FORECAST_DAILY, extra["gold"]))
        found = [c for c in frame.columns if c not in ("distribuidora_id", "fecha")]
        if "racha_max" not in found:
            logger.warning("Forecast of %s without gusts in the hours of the dataset: left out", extra["nombre"])
            continue
        pdf = pdf.merge(frame.rename(columns={c: f"{prefix}_{c}" for c in found}), on=["distribuidora_id", "fecha"],
                        how="left")
        forecast_extreme = forecast_extremes(hourly, frame, extreme, float(cfg["calor_umbral_c"]),
                                             int(cfg["horas_minimas_dia"]),
                                             forecast_mapping(FORECAST_EXTREME_DAILY, extra["gold"]), prefix)
        if any(c.startswith(f"{prefix}_ext_") for c in forecast_extreme.columns):
            pdf = pdf.merge(forecast_extreme, on=["distribuidora_id", "fecha"], how="left")
        ice_extra = daily_weather(hourly, int(cfg["horas_minimas_dia"]), forecast_mapping(FORECAST_ICE_DAILY, extra["gold"]))
        if "hie_horas" in ice_extra.columns:
            pdf = pdf.merge(ice_extra.rename(columns={"hie_horas": f"{prefix}_hie_horas"}),
                            on=["distribuidora_id", "fecha"], how="left")
        archived = pdf["prev_racha_max"].notna() if "prev_racha_max" in pdf.columns else pdf[f"{prefix}_racha_max"].notna()
        covered = float(pdf.loc[archived, f"{prefix}_racha_max"].notna().mean()) if archived.any() else 0.0
        logger.info("Forecast of %s: %s daily variables, %.1f %% of the days with a forecast of ICON-EU", extra["nombre"],
                    len(found), 100 * covered)
        present.append({**extra, "cobertura": covered})
    extras = present
    cfg["previsiones_extra"] = [{k: v for k, v in extra.items() if k != "presente"} for extra in extras]

    events, known = read_events(spark, cfg)
    pdf = day_labels(pdf, events, cfg["causas_no_meteo"], int(cfg["incidencias_dia_varias"]))

    # History known when the day starts (an incident known at 00:00 counts).
    pdf["hora"] = pdf["fecha"]
    history = add_history(pdf, known, cfg["ventanas_historia_dias"])
    pdf["hist_dias_desde_ultima"] = pdf.pop("hist_horas_desde_ultima") / 24.0
    history = [h for h in history if h != "hist_horas_desde_ultima"] + ["hist_dias_desde_ultima"]

    cal = day_calendar(pdf["fecha"])
    pdf = pd.concat([pdf, cal], axis=1)

    for dist in dists:
        pdf[f"dist_{dist}"] = (pdf["distribuidora_id"] == dist).astype("float64")
    created = add_interactions(pdf, dists, forecasts, extras)
    interaction_names = [n for n in created if n.startswith(("ext_", "fen_"))]
    other_models = tuple(f"{extra['prev']}_" for extra in extras)
    forecast_columns += [n for n in created if not n.startswith(("ext_", "fen_")) and not n.startswith(other_models)]

    warnings_first = None
    warning_names = []
    if spark.catalog.tableExists(WARNINGS_TABLE) and spark.catalog.tableExists(CELLS_TABLE):
        source = spark.table(WARNINGS_TABLE)
        columns = ["celda_id", "fenomeno_codigo", "nivel_num", "tipo_mensaje", "enviado_ts", "inicio_ts", "fin_ts"]
        warnings = source.select(*columns, *[c for c in ("parametro",) if c in source.columns]).toPandas()
        cells = (spark.table(CELLS_TABLE).where(F.col("celda_id").isNotNull())
                 .groupBy(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "celda_id")
                 .agg(F.count(F.lit(1)).cast("double").alias("n_ct")).toPandas())
        base_names = ["aviso_max_nivel"] + [f"aviso_{name}_nivel" for name in WARNING_GROUPS] + WARNING_DETAILS
        # The warnings known the evening before (aviso_*) and two or three evenings before (aviso48_*, aviso72_*).
        for horizon, spec in HORIZONS.items():
            if horizon > 1 and horizon not in forecasts:
                continue
            table, first = warning_days(warnings, cells, int(cfg["aviso_corte_h"]), horizon)
            if first is None:
                continue
            warnings_first = warnings_first or first
            renamed = {c: c.replace("aviso_", f"{spec['aviso']}_", 1) for c in table.columns
                       if c not in ("distribuidora_id", "fecha")}
            pdf = pdf.merge(table.rename(columns=renamed), on=["distribuidora_id", "fecha"], how="left")
            for base in base_names:
                name = base.replace("aviso_", f"{spec['aviso']}_", 1)
                if name not in pdf.columns:
                    pdf[name] = np.nan
                # 0 without warning once the archive exists, null before it.
                pdf[name] = pdf[name].where(pdf["fecha"] < first, pdf[name].fillna(0.0))
                warning_names.append(name)
        if warnings_first is not None:
            logger.info("Warnings of AEMET from %s", warnings_first.date())
    else:
        logger.info("%s not found: no warnings of AEMET in the daily model", WARNINGS_TABLE)

    pdf["anio"] = pdf["fecha"].dt.year
    pdf["ct_key"] = pdf["distribuidora_id"]
    pdf["split"] = "train"

    blocks = {}
    features = ([f"dist_{d}" for d in dists] + list(cal.columns) + history
                + [name for horizon in sorted(before) for name in before[horizon]]
                + [f"dia_{c}" for c in weather_names] + extreme_names + interaction_names + ice_names + drought_names
                + phenology_names + warning_names + forecast_columns
                + (["prev_hie_horas"] if "prev_hie_horas" in pdf.columns else []))
    for name in features:
        blocks.setdefault(block_of(name), []).append(name)
    # The weather of the day that the forecast of each horizon also gives: the fair reference of the real forecast.
    for horizon, (_, found) in forecasts.items():
        blocks[common_block(horizon)] = [f"dia_{c}" for c in found if c in weather_names]
    blocks.setdefault("dia_comun", [])
    # The extremes of the gusts alone, the ones every forecast gives (the run of heat needs the temperature).
    blocks["extremos_racha"] = [c for c in blocks.get("extremos", []) if "racha" in c]

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
    """The features of the blocks of the variant and its single variables (variables), without repeating."""
    chosen = list(blocks.get("distribuidora", []))
    for block in variant["bloques"]:
        chosen += blocks.get(block, [])
    every = {name for names in blocks.values() for name in names}
    chosen += [name for name in variant.get("variables", []) if name in every and name not in chosen]
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


def busy_days_table(metrics: pd.DataFrame, shares: list) -> pd.DataFrame:
    """The models trained for y_dia against y_dia_varias: how many of the busy days fall in the riskiest days."""
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == "y_dia_varias")]
    wanted = ["pr_auc", "lift_pr_auc"] + [f"dias_positivos_en_top_{int(round(s * 100))}pct" for s in shares]
    table = rows[rows["metrica"].isin(wanted)].pivot_table(index="modelo", columns="metrica", values="valor")
    if table.empty:
        return pd.DataFrame()
    table = table[[c for c in wanted if c in table.columns]].reset_index()
    split = table["modelo"].str.split("__", n=1, expand=True)
    table.insert(0, "variante", split[0])
    table["modelo"] = split[1]
    trained = table["modelo"].isin(TRAINED)
    table = pd.concat([table[trained], table[~trained].drop_duplicates("modelo")])
    return table.sort_values("pr_auc", ascending=False).reset_index(drop=True)


def forecast_comparison(metrics: pd.DataFrame, shares: list) -> pd.DataFrame:
    """
    One row per variant, model and weather of the day (reanalysis, forecast,
    mapped forecast; only yesterday for the variant ayer). The scores are
    named p_<variant>__<model>__<weather> and p_ayer__<model>.
    """
    rows = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == MAIN_LABEL)]
    wanted = ["pr_auc", "lift_pr_auc", "roc_auc"] + [f"dias_positivos_en_top_{int(round(s * 100))}pct" for s in shares]
    table = rows[rows["metrica"].isin(wanted)].pivot_table(index="modelo", columns="metrica", values="valor")
    table = table[[c for c in wanted if c in table.columns]].reset_index()
    parts = table["modelo"].str.split("__", n=2, expand=True).reindex(columns=[0, 1, 2])
    yesterday = parts[0] == "ayer"
    table.insert(0, "variante", parts[0])
    table.insert(1, "tiempo_del_dia", np.where(yesterday, "solo_ayer", parts[2]))
    table["modelo"] = parts[1]
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


def add_rank_mean(scored: pd.DataFrame, pairs: dict) -> pd.DataFrame:
    """
    {new column: (column a, column b)}: the mean of the percentiles of a and b
    in each year. XGBoost and the logistic regression put their probabilities
    at different levels every year; the percentiles weigh both rankings the
    same.
    """
    for name, pair in pairs.items():
        if all(c in scored.columns for c in pair):
            ranks = scored.groupby("anio")[list(pair)].rank(pct=True)
            scored[name] = ranks.mean(axis=1, skipna=False)
    return scored


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


def forecast_years(pdf: pd.DataFrame, cfg: dict, prefix: str = "prev") -> list:
    """
    Years of anios_prevision where the forecast (prev_* by default, prev2_*
    for two days ahead) covers enough days of every distribuidora.
    """
    column = f"{prefix}_racha_max"
    if column not in pdf.columns:
        return []
    minimum = float(cfg["cobertura_minima_prevision"])
    years = []
    for year in [int(y) for y in cfg["anios_prevision"]]:
        part = pdf[pdf["anio"] == year]
        covered = part.groupby("distribuidora_id")[column].apply(lambda v: float(v.notna().mean()))
        if len(part) and len(covered) and covered.min() >= minimum:
            years.append(year)
        else:
            logger.warning("Year %s left out of the forecast comparison (%s): coverage by distribuidora %s", year,
                           column, covered.round(3).to_dict())
    return years


def forecast_evaluation(pdf: pd.DataFrame, variant: dict, blocks: dict, config: dict, cfg: dict, years: list,
                        only_first: bool) -> pd.DataFrame:
    """
    The models of the variant fitted on the reanalysis of the days before
    each year, scoring the days of the year with the weather of the day from
    the reanalysis (p_<model>__reanalisis), from the forecast of ICON-EU of
    the day before (__prevision) and from that forecast mapped by quantiles
    (__prevision_cuantiles). The extremes of the variant (ext_*) go with the
    forecast too when it has them (prev_ext_*). Only the days with a forecast
    of the weather of the day. A variant with horizonte 2 (3) takes the
    forecast of two (three) days before (prev2_*, prev3_*) in place of the
    one of the day before; a dia_* of the variant that this forecast does
    not give stays from the reanalysis and is logged as a perfect forecast.
    """
    features = variant_features(variant, blocks)
    horizon = horizon_of(variant)
    prefix = HORIZONS[horizon]["prev"]
    common = [c[len("dia_"):] for c in blocks.get(common_block(horizon), [])]
    swap = [(f"dia_{name}", f"{prefix}_{name}") for name in common]
    swap += [(c, f"{prefix}_{c}") for c in features
             if c.startswith(("ext_", "hie_", "fen_int_")) and f"{prefix}_{c}" in pdf.columns]
    swapped = {column for column, _ in swap}
    perfect = [c for c in features if c.startswith(("dia_", "ext_", "hie_", "fen_int_")) and c not in swapped]
    if perfect:
        logger.warning("Variant %s at %s day(s): %s stay from the reanalysis (no forecast of them at this horizon)",
                       variant["nombre"], horizon, perfect)
    # The other models forecast the day before only: the same swap with their columns (the ones they have; a
    # variable a model lacks stays from ICON-EU), and the mean of all the models.
    others = {}
    for extra in (extra_forecasts(cfg) if horizon == 1 else []):
        twins = [(column, source.replace(f"{prefix}_", f"{extra['prev']}_", 1)) for column, source in swap]
        twins = [(column, twin) for column, twin in twins if twin in pdf.columns]
        if twins:
            others[extra["clave"]] = dict(twins)
            missing = [column for column, _ in swap if column not in dict(twins)]
            if missing:
                logger.info("Forecast of %s: %s stay from ICON-EU", extra["nombre"], missing)
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]
    parts = []

    def mapped_by_quantiles(target, fitting, pairs):
        """target with the columns of pairs replaced by their source taken by quantiles to the scale of the column."""
        out = target.copy()
        for column, source in pairs:
            values = np.full(len(target), np.nan)
            for dist in target["distribuidora_id"].unique():
                mask = (target["distribuidora_id"] == dist).to_numpy()
                fit = fitting[fitting["distribuidora_id"] == dist]
                values[mask] = quantile_map(fit[source], fit[column], target.loc[mask, source])
            out[column] = values
        return out

    for year in years:
        scored, _, scorers, _ = rolling_evaluation(rows, features, MAIN_LABEL, config, cfg, [year], only_first,
                                                   names=cfg["modelos"])
        target = rows[rows["anio"] == year].copy()
        other_years = rows[rows["anio"].isin([y for y in years if y != year])]
        fitting = (other_years if len(other_years) else target).copy()
        raw = target.copy()
        for column, source in swap:
            raw[column] = target[source].to_numpy()
        frames = {"reanalisis": target, "prevision": raw, "prevision_cuantiles": mapped_by_quantiles(target, fitting, swap)}

        if others:
            mean_pairs = []
            for column, source in swap:
                twins = [source] + [names[column] for names in others.values() if column in names]
                mean_column = f"_media_{column}"
                for frame in (target, fitting):
                    frame[mean_column] = frame[twins].mean(axis=1, skipna=True).to_numpy()
                mean_pairs.append((column, mean_column))
            for key, names in others.items():
                pairs = []
                for column, source in swap:
                    # The model's forecast where it has one, the one of ICON-EU where it does not.
                    filled = f"_{key}_{column}"
                    for frame in (target, fitting):
                        frame[filled] = (frame[names[column]].fillna(frame[source]) if column in names
                                         else frame[source]).to_numpy()
                    pairs.append((column, filled))
                frame = target.copy()
                for column, filled in pairs:
                    frame[column] = target[filled].to_numpy()
                frames[f"prevision_{key}"] = frame
                frames[f"prevision_{key}_cuantiles"] = mapped_by_quantiles(target, fitting, pairs)
            mean = target.copy()
            for column, mean_column in mean_pairs:
                mean[column] = target[mean_column].to_numpy()
            frames[MEAN_SOURCE] = mean
            frames[f"{MEAN_SOURCE}_cuantiles"] = mapped_by_quantiles(target, fitting, mean_pairs)

        out = target[["distribuidora_id", "fecha", "anio"] + LABELS].copy()
        for source, frame in frames.items():
            X = frame[features].to_numpy(dtype=np.float32, na_value=np.nan)
            for model, scorer in scorers.items():
                out[f"p_{model}__{source}"] = scorer.score(X)
        out["con_prevision"] = target[[f"{prefix}_{n}" for n in common]].notna().all(axis=1).to_numpy()
        parts.append(out)

    if not parts:
        return pd.DataFrame()
    found = pd.concat(parts, ignore_index=True)
    found = found[found["con_prevision"]].drop(columns="con_prevision").reset_index(drop=True)
    sources = ["reanalisis", "prevision", "prevision_cuantiles"]
    for key in list(others) + ([MEAN_SOURCE[len("prevision_"):]] if others else []):
        sources += [f"prevision_{key}", f"prevision_{key}_cuantiles"]
    return add_rank_mean(found, {f"p_rango__{source}": (f"p_logistica__{source}", f"p_xgboost__{source}")
                                 for source in sources})


def models_text(cfg: dict) -> str:
    """The sentence of the report about the other weather models of the comparison, empty without them."""
    extras = extra_forecasts(cfg)
    if not extras:
        return ""
    names = ", ".join(f"{e['nombre']} (prevision_{e['clave']}, {100 * float(e.get('cobertura', 1.0)):.0f} % de los días)"
                      for e in extras)
    return (f" Los días con previsión se puntúan también con lo que preveía cada uno de los otros modelos el día antes, "
            f"{names}, y con la media de todos los modelos ({MEAN_SOURCE}), cada uno en crudo y llevado por cuantiles "
            "a la escala del reanálisis (_cuantiles); donde a un modelo le falta una variable o un día, va la de ICON-EU.")


def horizons_text(references: list, cfg: dict) -> str:
    """The sentence of the report about the variants scored two or three days ahead, empty without them."""
    far = [r for r in references if horizon_of(r) > 1]
    if not far:
        return ""
    names = ", ".join(f"{r['nombre']} ({24 * horizon_of(r)} h)" for r in far)
    return (f" Las variantes {names} puntúan con lo que preveía ICON-EU dos o tres días antes (racha y lluvia), los "
            f"avisos de AEMET enviados antes de las {int(cfg['aviso_corte_h'])}:00 de ese día y el tiempo del último día "
            "conocido esa tarde: lo que habría tenido el operador con esa antelación.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    cfg = settings(config)
    if args.direccion:
        cfg["bloque_direccion"] = True
    if args.previsiones_extra:
        cfg["comparar_previsiones_extra"] = True
    if args.modelos:
        cfg["modelos"] = [m.strip() for m in args.modelos.split(",") if m.strip()]
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
    scored = add_rank_mean(scored, {f"p_{v['nombre']}__rango": (f"p_{v['nombre']}__logistica", f"p_{v['nombre']}__xgboost")
                                    for v in variants})
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

    # The real forecast: the models of variante_prevision (one variant or a
    # list) with the forecast of ICON-EU in place of the weather of the day,
    # on the days it covers.
    forecast_table, forecast_differences = pd.DataFrame(), pd.DataFrame()
    chosen = cfg.get("variante_prevision", "perfecta_comun")
    chosen = [chosen] if isinstance(chosen, str) else list(chosen)
    references = [v for v in variants if v["nombre"] in chosen]
    years_by_reference = {r["nombre"]: forecast_years(pdf, cfg, HORIZONS[horizon_of(r)]["prev"]) for r in references}
    references = [r for r in references if years_by_reference[r["nombre"]]]
    years_forecast = sorted({y for years in years_by_reference.values() for y in years})
    if years_forecast:
        found = pd.DataFrame()
        for reference in references:
            part = forecast_evaluation(pdf, reference, blocks, config, run_cfg, years_by_reference[reference["nombre"]],
                                       args.sin_busqueda)
            if part.empty:
                continue
            part = part.rename(columns={c: f"p_{reference['nombre']}__{c[2:]}" for c in part.columns
                                        if c.startswith("p_")})
            found = part if found.empty else found.merge(part.drop(columns=["anio"] + LABELS),
                                                         on=["distribuidora_id", "fecha"], how="inner")
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
            wanted = []
            for name in [r["nombre"] for r in references]:
                for m in ("logistica", "xgboost", "rango"):
                    wanted += [(f"p_{name}__{m}__prevision_cuantiles", f"p_{name}__{m}__reanalisis"),
                               (f"p_{name}__{m}__prevision", f"p_{name}__{m}__reanalisis"),
                               (f"p_{name}__{m}__prevision_cuantiles", f"p_ayer__{m}")]
                    # The other models and the mean of all of them against ICON-EU alone, raw and by quantiles.
                    for key in [extra["clave"] for extra in extra_forecasts(run_cfg)] + [MEAN_SOURCE[len("prevision_"):]]:
                        wanted += [(f"p_{name}__{m}__prevision_{key}", f"p_{name}__{m}__prevision"),
                                   (f"p_{name}__{m}__prevision_{key}_cuantiles", f"p_{name}__{m}__prevision_cuantiles")]
            # Between variants with the same weather of the day (meteo_dia.comparaciones_prevision).
            wanted += [(f"p_{a}", f"p_{b}") for a, b in cfg.get("comparaciones_prevision", [])]
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
        "horizonte_dias": horizon_of(v),
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
        f"## Días con {int(cfg['incidencias_dia_varias'])} incidencias o más\n\nLos mismos modelos frente a "
        "y_dia_varias, los días que empiezan varias incidencias en la distribuidora y piden más de una brigada.\n\n"
        + ml_metrics.markdown_table(busy_days_table(metrics[metrics["split"] == "cv_comun"], shares), digits=4),
        "## PR-AUC por año\n\n" + ml_metrics.markdown_table(by_year(metrics), digits=4),
        "## PR-AUC por distribuidora en los años comunes\n\n" + ml_metrics.markdown_table(per_dist, digits=4),
        f"## PR-AUC con intervalo del 95 % (bootstrap por fechas, {reps} réplicas)\n\n"
        + ml_metrics.markdown_table(intervals, digits=4),
        "## Diferencias emparejadas (a - b)\n\nSi el intervalo cruza el cero, los datos no distinguen a de b.\n\n"
        + ml_metrics.markdown_table(differences, digits=4),
    ] + ([
        f"## Con la previsión real del día anterior ({', '.join(str(y) for y in years_forecast)})\n\n"
        f"Los modelos de {', '.join(r['nombre'] for r in references)} se entrenan con el reanálisis y puntúan los "
        "días con previsión tres veces: con el tiempo del día sacado del reanálisis (reanalisis), con lo que preveía ICON-EU el "
        "día anterior (prevision) y con esa previsión llevada por cuantiles a la escala del reanálisis "
        "(prevision_cuantiles). ayer es la variante que solo sabe el tiempo del día anterior."
        + horizons_text(references, cfg) + models_text(run_cfg) + "\n\n"
        + ml_metrics.markdown_table(forecast_table, digits=4)
        + "\n\n" + ml_metrics.markdown_table(forecast_differences, digits=4),
    ] if not forecast_table.empty else [])) + "\n"
    export(config, args.run_id, report, {"metricas": metrics, "comparacion": comparison, "anios": by_year(metrics),
                                         "distribuidoras": per_dist, "intervalos": intervals,
                                         "diferencias": differences, "filas": sizes, "prevision": forecast_table,
                                         "prevision_diferencias": forecast_differences, "variantes": variant_rows})

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
