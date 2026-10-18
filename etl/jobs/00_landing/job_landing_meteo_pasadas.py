"""
Landing of the forecast of the day before from single runs of a model
(Open-Meteo Single Runs API, https://open-meteo.com/en/docs/single-runs-api,
CC BY 4.0, free plan for non commercial use).

The Previous Runs API (job_landing_meteo_openmeteo --fuente openmeteo_previo)
gives ICON-EU, GFS, GEM and UKMO as the forecast made one day before every
hour, but its archive of ECMWF IFS has no gusts (checked on 17.10.2026 for
ecmwf_ifs, ecmwf_ifs025 and ecmwf_ifs04: wind_gusts_10m_previous_day1 is
empty on every day). The Single Runs API keeps whole runs of IFS HRES 9 km
(ecmwf_ifs) with gusts from 14.03.2024, so the forecast of the day before is
rebuilt run by run:

- For every day D of [desde, hasta], the run of D - dias_antes at
  hora_pasada_utc (00 UTC of the day before by default: always published by
  the evening, about 22 to 46 hours before the hours of D, a little older
  than the run of the afternoon the operator reads). The runs of 06 and 18
  UTC of 2024 are not in the archive.
- One request per run with every cell of the study (the municipalities of
  the reference in cells of celda_grados, as the other sources), cut to the
  hours of D in the wall clock of Madrid, with the variables renamed with
  sufijo (_previous_day1). Bronze (job_bronze_meteo_openmeteo) then builds
  l1_bronze.meteo_openmeteo_previo_<clave>_hora with the columns of the
  other models of the Previous Runs API, so the forecast comparison of the
  day model (meteo_dia.previsiones_extra) reads it like GFS or GEM.
- Saved as <landing>/dias/<D>.json: the answer of every location in
  ubicaciones, each with its _peticion (cell). A day already there is not
  asked again, so a run that stops goes on where it was by launching it
  again. A run that the archive does not have is noted in _sin_pasada.json
  and not asked again.
- Budget: a request counts one call of the free plan per location (three
  days of a run, fewer than ten variables), so a run of the 43 cells costs 43
  of the 10.000 calls of a day. The calls of every day (UTC) are kept in
  _llamadas.json and the job stops at max_llamadas_dia, below the limit, so
  the forecast of the evening (dag_riesgo_manana) and the night chain still
  have their calls. With --esperar it sleeps until the next day (UTC) and
  goes on; without it, it ends with code 3 to be launched again.

Run from the Spark master container (pandas, s3fs and the MinIO credentials;
no Spark):

    docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_meteo_pasadas.py \\
        --fuente openmeteo_pasadas_ifs --esperar
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_meteo_openmeteo import (  # noqa: E402
    DAILY_LIMIT_EXIT,
    call_weight,
    fetch,
    filesystem,
    load_config,
    municipality_cells,
    read_reference,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pasadas")

API = "https://single-runs-api.open-meteo.com/v1/forecast"


def run_of(day: date, cfg: dict) -> datetime:
    """The run (UTC) that stands for the forecast of the day before of day."""
    start = datetime(day.year, day.month, day.day, int(cfg.get("hora_pasada_utc", 0)), tzinfo=timezone.utc)
    return start - timedelta(days=int(cfg.get("dias_antes", 1)))


def request_url(cells: list, run: datetime, cfg: dict) -> str:
    """One request for every cell of cells ([(cell, (lat, lon))]) and the whole run."""
    query = {
        "latitude": ",".join(f"{lat:.4f}" for _, (lat, _lon) in cells),
        "longitude": ",".join(f"{lon:.4f}" for _, (_lat, lon) in cells),
        "hourly": ",".join(cfg["variables"]),
        "models": cfg["modelo"],
        "run": run.strftime("%Y-%m-%dT%H:%M"),
        "forecast_days": int(cfg.get("dias_pasada", 3)),
        "timezone": "GMT",
        "timeformat": "unixtime",
        "wind_speed_unit": "kmh",
    }
    return f"{cfg.get('api', API)}?{urllib.parse.urlencode(query, safe=',:')}"


def hours_of_day(location: dict, day: date, zone: str, variables: list, suffix: str) -> dict:
    """
    The hourly answer of a location cut to the hours whose wall clock in zone
    falls on day, with the variables renamed with suffix (wind_gusts_10m ->
    wind_gusts_10m_previous_day1).
    """
    tz = ZoneInfo(zone)
    hourly = location.get("hourly") or {}
    times = hourly.get("time") or []
    keep = [i for i, t in enumerate(times) if datetime.fromtimestamp(int(t), tz=timezone.utc).astimezone(tz).date() == day]
    out = {"time": [times[i] for i in keep]}
    for variable in variables:
        values = hourly.get(variable) or [None] * len(times)
        out[f"{variable}{suffix}"] = [values[i] for i in keep]
    return out


def days_of(cfg: dict) -> list:
    """The days of [desde, hasta], the most recent first when orden is reciente (a stopped run has the newest year)."""
    first, last = date.fromisoformat(cfg["desde"]), date.fromisoformat(cfg["hasta"])
    days = [first + timedelta(days=k) for k in range((last - first).days + 1)]
    return days[::-1] if cfg.get("orden", "reciente") == "reciente" else days


def read_json(fs, path: str, default):
    if not fs.exists(path):
        return default
    with fs.open(path, "r") as file:
        return json.load(file)


def write_json(fs, path: str, payload):
    with fs.open(path, "w") as file:
        json.dump(payload, file)


def seconds_to_next_day(now: datetime) -> float:
    """Seconds from now until five minutes after the next midnight UTC, when the free plan counts again."""
    tomorrow = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1, minutes=5)
    return (tomorrow - now).total_seconds()


def main(argv=None, fs=None, clock=None, sleep=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--fuente", default="openmeteo_pasadas_ifs")
    parser.add_argument("--max-peticiones", type=int, default=None, help="Stop after this many requests")
    parser.add_argument("--esperar", action="store_true",
                        help="At the budget of the day, sleep until the next day (UTC) instead of stopping")
    args = parser.parse_args(argv)
    clock = clock or (lambda: datetime.now(timezone.utc))
    sleep = sleep or time.sleep

    cfg = load_config(args.config)[args.fuente]
    fs = fs or filesystem()
    landing = cfg["landing"].rstrip("/")
    zone = cfg.get("zona_horaria", "Europe/Madrid")
    suffix = cfg.get("sufijo", "_previous_day1")

    places = municipality_cells(read_reference(fs, cfg["referencia_municipios"]), float(cfg["celda_grados"]))
    cells = {}
    for place in places:
        cells.setdefault(place["celda_id"], (place["celda_latitud"], place["celda_longitud"]))
    cells = sorted(cells.items())
    write_json(fs, f"{landing}/municipios_celdas.json", places)

    weight = len(cells) * call_weight(int(cfg.get("dias_pasada", 3)), len(cfg["variables"]))
    budget = float(cfg.get("max_llamadas_dia", 6000))
    usage_path, missing_path = f"{landing}/_llamadas.json", f"{landing}/_sin_pasada.json"
    usage, missing = read_json(fs, usage_path, {}), read_json(fs, missing_path, {})
    days = days_of(cfg)
    logger.info("%s: %s days from %s to %s, %s cells, %.0f calls of the free plan per run, at most %.0f a day",
                args.fuente, len(days), cfg["desde"], cfg["hasta"], len(cells), weight, budget)

    requested = done = 0
    for day in days:
        target = f"{landing}/dias/{day.isoformat()}.json"
        if fs.exists(target) or day.isoformat() in missing:
            done += 1
            continue
        if args.max_peticiones is not None and requested >= args.max_peticiones:
            logger.info("Stopped after %s requests as asked", requested)
            return 0

        while True:
            today = clock().date().isoformat()
            if float(usage.get(today, 0.0)) + weight <= budget:
                break
            if not args.esperar:
                logger.info("Budget of %s spent (%.0f calls): launch again tomorrow; %s days requested now, %s "
                            "already there", today, float(usage.get(today, 0.0)), requested, done)
                return DAILY_LIMIT_EXIT
            wait = seconds_to_next_day(clock())
            logger.info("Budget of %s spent: waiting %.0f min for the next day", today, wait / 60)
            sleep(wait)

        run = run_of(day, cfg)
        try:
            answer = fetch(request_url(cells, run, cfg), int(cfg.get("reintentos", 5)),
                           float(cfg.get("espera_reintento_s", 60)))
        except RuntimeError as error:
            if "HTTP 400" in str(error) and "not available" in str(error):
                missing[day.isoformat()] = run.isoformat()
                write_json(fs, missing_path, missing)
                logger.warning("Run %s of %s not in the archive: day %s left without it", run, cfg["modelo"], day)
                continue
            raise
        finally:
            usage[clock().date().isoformat()] = float(usage.get(clock().date().isoformat(), 0.0)) + weight
            write_json(fs, usage_path, usage)

        locations = answer if isinstance(answer, list) else [answer]
        if len(locations) != len(cells):
            raise RuntimeError(f"{len(locations)} locations in the answer of {day} for {len(cells)} cells")
        saved = {
            "_peticion": {"fuente": args.fuente, "modelo": cfg["modelo"], "dia": day.isoformat(),
                          "pasada_utc": run.isoformat(), "pedido_utc": clock().isoformat(timespec="seconds"),
                          "celdas": [cell for cell, _ in cells]},
            "ubicaciones": [{"latitude": location.get("latitude"), "longitude": location.get("longitude"),
                             "elevation": location.get("elevation"),
                             "hourly": hours_of_day(location, day, zone, cfg["variables"], suffix),
                             "_peticion": {"celda_id": cell, "dia": day.isoformat(), "pasada_utc": run.isoformat()}}
                            for (cell, _), location in zip(cells, locations)],
        }
        hours = len(saved["ubicaciones"][0]["hourly"]["time"])
        if hours < 23:
            raise RuntimeError(f"Only {hours} hours of {day} in the run {run}: is dias_pasada long enough?")
        write_json(fs, target, saved)
        requested += 1
        logger.info("Day %s from the run %s: %s hours of %s cells (%s requested, %s already there)", day,
                    run.strftime("%d.%m.%Y %H UTC"), hours, len(cells), requested, done)
        sleep(float(cfg.get("pausa_s", 40)))

    logger.info("Landing complete: %s days requested now, %s already there or not in the archive", requested, done)
    return 0


if __name__ == "__main__":
    sys.exit(main())
