"""
Bronze of the hourly weather downloaded by job_landing_meteo_openmeteo.py.

- meteo_openmeteo_hora: one row per (celda_id, hora) with the variables of
  the configuration, in the wall clock of Madrid (zona_horaria), the time
  Calser and TedisNet use. Open-Meteo gives the value at the end of each
  hour: temperature, humidity, pressure and wind speed at that instant,
  precipitation and snowfall summed and gusts the maximum of the hour
  before. So hora is the end of the hour the row describes, the same
  convention as the hourly aggregates of Gold (the row of 10:00 is known at
  10:00).
- Daylight saving: when the clock goes back, two UTC hours become the same
  local hour; they are merged (sum of precipitation and snow, maximum of
  wind and gusts, mean of the rest). When it goes forward the local hour
  does not exist and there is no row, as in the sources.
- meteo_openmeteo_celdas: municipality (INE code, name, distribuidora of the
  reference file, coordinates) and the cell of its weather.
- With --fuente openmeteo_convectivo, meteo_openmeteo_convectivo_hora: CAPE
  and lifted index of the archived GFS runs by cell of 0.25 degrees
  (celda_id of that grid), same hour convention.
- With --fuente openmeteo_extra, meteo_openmeteo_extra_hora: the other
  variables of the reanalysis (wind direction, wind at 100 m, soil moisture,
  dew point, vapour pressure deficit, low clouds, evapotranspiration, weather
  code and radiation) by cell of 0.25 degrees: they change slowly in space,
  and with the coarser grid the whole history fits in the free plan.
- With --fuente openmeteo_previo, meteo_openmeteo_previo_hora: the forecasts
  that ICON-EU gave for every hour one and two days before
  (<variable>_previous_day1 and _previous_day2 of the Previous Runs API,
  archived from January 2024) and the run of the same day, by cell of 0.1
  degrees. It is the weather the operator could know the day before, to
  measure how much of what the reanalysis gives survives with a real
  forecast. A suffixed variable is merged like the variable it forecasts.
  The other models of the Previous Runs API (openmeteo_previo_gfs, _gem,
  _ukmo) go the same way to their own tables.
- With --fuente openmeteo_pasadas_ifs, meteo_openmeteo_previo_ifs_hora: the
  forecast of the day before of IFS HRES 9 km rebuilt from single runs by
  job_landing_meteo_pasadas.py. Its files hold one day of every cell
  (ubicaciones), each location read as an answer of its own.

The whole table is rebuilt in every run (a few million rows).

    docker compose exec spark-master spark-submit /app/jobs/01_bronze/job_bronze_meteo_openmeteo.py
    docker compose exec spark-master spark-submit /app/jobs/01_bronze/job_bronze_meteo_openmeteo.py \
        --fuente openmeteo_convectivo
"""

import argparse
import json
import logging
import os
import re
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "00_landing"))

from job_landing_meteo_openmeteo import filesystem, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("meteo")

SUM_VARIABLES = {"precipitation", "rain", "snowfall", "et0_fao_evapotranspiration"}
MAX_VARIABLES = {"wind_speed_10m", "wind_gusts_10m", "cape", "weather_code", "wind_speed_100m"}
CIRCULAR_VARIABLES = {"wind_direction_10m", "wind_direction_100m"}


def payload_frame(payload: dict, variables: list, zone: str) -> pd.DataFrame:
    """Hourly rows of one Open-Meteo answer, in local wall clock."""
    hourly = payload["hourly"]
    utc = pd.to_datetime(pd.Series(hourly["time"], dtype="int64"), unit="s", utc=True)

    df = pd.DataFrame({v: pd.to_numeric(pd.Series(hourly.get(v)), errors="coerce") for v in variables})
    df["hora"] = utc.dt.tz_convert(zone).dt.tz_localize(None)
    df["celda_id"] = payload["_peticion"]["celda_id"]
    return df


def answers_of(data) -> list:
    """The answers of one file: itself, or every location of a file of single runs (ubicaciones)."""
    if isinstance(data, dict) and "ubicaciones" in data:
        return list(data["ubicaciones"])
    return list(data) if isinstance(data, list) else [data]


def forecast_of(variable: str) -> str:
    """The variable a forecast of the Previous Runs API is about (wind_gusts_10m_previous_day1 -> wind_gusts_10m)."""
    return re.sub(r"_previous_day\d+$", "", variable)


def merge_local_hours(df: pd.DataFrame, variables: list) -> pd.DataFrame:
    """
    One row per (celda_id, hora) after the change to winter time. Directions
    are averaged as vectors (350 and 10 degrees give 0, not 180).
    """
    df = df.copy()
    circular = [v for v in variables if forecast_of(v) in CIRCULAR_VARIABLES]
    for v in circular:
        radians = np.radians(df[v])
        df[f"_{v}_sin"], df[f"_{v}_cos"] = np.sin(radians), np.cos(radians)

    how = {v: ("sum" if forecast_of(v) in SUM_VARIABLES else "max" if forecast_of(v) in MAX_VARIABLES else "mean")
           for v in variables if v not in circular}
    how.update({f"_{v}_{part}": "mean" for v in circular for part in ("sin", "cos")})
    out = df.groupby(["celda_id", "hora"], as_index=False).agg(how)

    for v in circular:
        out[v] = np.degrees(np.arctan2(out.pop(f"_{v}_sin"), out.pop(f"_{v}_cos"))) % 360
    return out[["celda_id", "hora", *variables]]


def main(argv=None):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser()
    parser.add_argument("--fuente", default="openmeteo")
    args, _ = parser.parse_known_args(argv)

    cfg = load_config()[args.fuente]
    # The single runs are asked with the plain names and landed with the suffix of the Previous Runs API (sufijo).
    variables = [f"{v}{cfg.get('sufijo', '')}" for v in cfg["variables"]]
    zone = cfg.get("zona_horaria", "Europe/Madrid")
    landing = cfg["landing"].rstrip("/")

    spark = (
        SparkSession.builder.appName("job-bronze-meteo-openmeteo")
        .config("spark.cores.max", "2")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    fs = filesystem()
    files = sorted(p for p in fs.glob(f"{landing}/*/*.json"))
    logger.info("%s files in %s", len(files), landing)

    if not files:
        raise SystemExit("No weather files in Landing: run job_landing_meteo_openmeteo.py first")

    frames = []
    for path in files:
        with fs.open(path, "r") as file:
            frames += [payload_frame(answer, variables, zone) for answer in answers_of(json.load(file))]

    hourly = merge_local_hours(pd.concat(frames, ignore_index=True), variables)
    hourly["anio"] = hourly["hora"].dt.year.astype("int32")
    hourly["modelo"] = cfg.get("modelo") or ",".join(cfg.get("modelos", []))

    logger.info("%s hourly rows, %s cells, %s to %s, nulls per variable: %s",
                len(hourly), hourly["celda_id"].nunique(), hourly["hora"].min(), hourly["hora"].max(),
                hourly[variables].isna().sum().to_dict())

    (
        spark.createDataFrame(hourly)
        .withColumn("audit_loaded_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .partitionBy("anio").saveAsTable(cfg["tabla_horas"])
    )

    if not cfg.get("tabla_celdas"):
        logger.info("Bronze completed: %s", cfg["tabla_horas"])
        return

    with fs.open(f"{landing}/municipios_celdas.json", "r") as file:
        places = pd.DataFrame(json.load(file))

    (
        spark.createDataFrame(places.astype({"distribuidora_ref": "string", "codigo_ine": "string"}))
        .withColumn("audit_loaded_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(cfg["tabla_celdas"])
    )

    logger.info("Bronze completed: %s and %s", cfg["tabla_horas"], cfg["tabla_celdas"])


if __name__ == "__main__":
    main()
