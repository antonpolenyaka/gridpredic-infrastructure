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

The whole table is rebuilt in every run (a few million rows).

    docker compose exec spark-master spark-submit /app/jobs/01_bronze/job_bronze_meteo_openmeteo.py
"""

import json
import logging
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "00_landing"))

from job_landing_meteo_openmeteo import filesystem, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("meteo")

SUM_VARIABLES = {"precipitation", "rain", "snowfall"}
MAX_VARIABLES = {"wind_speed_10m", "wind_gusts_10m"}


def payload_frame(payload: dict, variables: list, zone: str) -> pd.DataFrame:
    """Hourly rows of one Open-Meteo answer, in local wall clock."""
    hourly = payload["hourly"]
    utc = pd.to_datetime(pd.Series(hourly["time"], dtype="int64"), unit="s", utc=True)

    df = pd.DataFrame({v: pd.to_numeric(pd.Series(hourly.get(v)), errors="coerce") for v in variables})
    df["hora"] = utc.dt.tz_convert(zone).dt.tz_localize(None)
    df["celda_id"] = payload["_peticion"]["celda_id"]
    return df


def merge_local_hours(df: pd.DataFrame, variables: list) -> pd.DataFrame:
    """One row per (celda_id, hora) after the change to winter time."""
    how = {v: ("sum" if v in SUM_VARIABLES else "max" if v in MAX_VARIABLES else "mean") for v in variables}
    return df.groupby(["celda_id", "hora"], as_index=False).agg(how)


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    cfg = load_config()["openmeteo"]
    variables = list(cfg["variables"])
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
            frames.append(payload_frame(json.load(file), variables, zone))

    hourly = merge_local_hours(pd.concat(frames, ignore_index=True), variables)
    hourly["anio"] = hourly["hora"].dt.year.astype("int32")
    hourly["modelo"] = cfg.get("modelo", "ecmwf_ifs")

    logger.info("%s hourly rows, %s cells, %s to %s, nulls per variable: %s",
                len(hourly), hourly["celda_id"].nunique(), hourly["hora"].min(), hourly["hora"].max(),
                hourly[variables].isna().sum().to_dict())

    (
        spark.createDataFrame(hourly)
        .withColumn("audit_loaded_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .partitionBy("anio").saveAsTable(cfg["tabla_horas"])
    )

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
