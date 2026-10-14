"""
Landing of what is known every evening for the risk of the next day
(job_ml_riesgo_manana, dag_riesgo_manana). Configuration: riesgo_manana of
config_ml.json.

- prevision: the forecast of ICON-EU from the Open-Meteo Forecast API (free,
  no key) for every cell of the reanalysis (the municipalities of the
  reference file in cells of celda_grados of config_bronze_meteo.json),
  hourly: gusts, wind, rain, snow, temperature and humidity, from
  dias_pasados days before today to dias_futuros days ahead (today
  included). The days ahead are the forecast of the day to predict; today
  is the weather of the day before it; the days before give the run of hot
  days. One request per cell, saved as it comes with _peticion (cell, model,
  when it was asked).
- avisos: the warnings of adverse weather of AEMET OpenData that are in
  force, the last ones issued for the areas of the configuration
  (avisos_cap/ultimoelaborado), as AEMET gives them (a tar of CAP messages).
  It needs the free key of opendata.aemet.es in the environment variable
  clave_env, which goes in a header and is never written in the log.

Everything goes to <landing>/<day to predict, YYYYMMDD>/prevision/<cell>.json
and avisos/<area>.tar (avisos/<area>.vacio.json when AEMET has none), so the
scoring job reads exactly what was known that evening. A source that is
already there for the day is asked again (the forecast changes during the
day): the last run of the evening wins.

The day to predict is tomorrow in the time zone of the configuration unless
--fecha gives it.

    docker compose exec airflow python /app/jobs/00_landing/job_landing_vivo.py --fuente prevision
    docker compose exec airflow python /app/jobs/00_landing/job_landing_vivo.py --fuente avisos
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_datos_publicos import fetch  # noqa: E402
from job_landing_meteo_openmeteo import filesystem, municipality_cells, read_reference  # noqa: E402
from job_landing_meteo_openmeteo import load_config as load_meteo_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("vivo")

CONFIG_CANDIDATES = [
    "/app/config/04_ml/config_ml.json",
    "/opt/airflow/config/04_ml/config_ml.json",
    os.path.join(HERE, "..", "..", "config", "04_ml", "config_ml.json"),
]
SOURCES = ("prevision", "avisos")


def load_config(path: str = None) -> dict:
    for candidate in [path] if path else CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                return json.load(file)
    raise FileNotFoundError(f"config_ml.json not found in {CONFIG_CANDIDATES}")


def target_day(fecha: str, zone: str) -> date:
    """The day to predict: fecha (YYYY-MM-DD) or tomorrow in the time zone."""
    if fecha:
        return date.fromisoformat(fecha)
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo(zone)).date() + timedelta(days=1)


def day_folder(landing: str, day: date) -> str:
    return f"{landing.rstrip('/')}/{day:%Y%m%d}"


def forecast_url(lat: float, lon: float, prevision: dict) -> str:
    query = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "hourly": ",".join(prevision["variables"]),
        "models": prevision.get("modelo", "icon_eu"),
        "past_days": int(prevision.get("dias_pasados", 31)),
        "forecast_days": int(prevision.get("dias_futuros", 3)),
        "timezone": "GMT",
        "timeformat": "unixtime",
        "wind_speed_unit": "kmh",
    }
    return f"{prevision['api']}?{urllib.parse.urlencode(query, safe=',')}"


def study_cells(fs, meteo: dict) -> dict:
    """{cell: (latitude, longitude)} of the reanalysis: the municipalities of the reference file in cells."""
    places = municipality_cells(read_reference(fs, meteo["referencia_municipios"]), float(meteo["celda_grados"]))
    cells = {}
    for place in places:
        cells.setdefault(place["celda_id"], (place["celda_latitud"], place["celda_longitud"]))
    return cells


def land_forecast(fs, cfg: dict, folder: str, cells: dict) -> tuple:
    """(cells saved, cells failed): the forecast of every cell in <folder>/prevision/<cell>.json."""
    prevision = cfg["prevision"]
    done = failed = 0
    for cell, (lat, lon) in sorted(cells.items()):
        try:
            raw = fetch(forecast_url(lat, lon, prevision), retries=int(prevision.get("reintentos", 4)),
                        wait_s=float(prevision.get("espera_reintento_s", 30)),
                        timeout=float(prevision.get("timeout_s", 120)), label=f"Open-Meteo {cell}")
            payload = json.loads(raw.decode("utf-8"))
            hours = len(payload.get("hourly", {}).get("time", []))
            if not hours:
                raise RuntimeError(f"empty answer {str(payload)[:200]}")
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("Forecast of cell %s not downloaded: %s", cell, str(error)[:200])
            continue
        payload["_peticion"] = {"celda_id": cell, "modelo": prevision.get("modelo", "icon_eu"),
                                "pedido_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                "dias_pasados": int(prevision.get("dias_pasados", 31)),
                                "dias_futuros": int(prevision.get("dias_futuros", 3))}
        with fs.open(f"{folder}/prevision/{cell}.json", "w") as file:
            json.dump(payload, file)
        done += 1
        time.sleep(float(prevision.get("pausa_s", 0.5)))
    logger.info("Forecast of ICON-EU: %s cells saved, %s failed", done, failed)
    return done, failed


def ask_warnings(url: str, key: str, timeout: float, area: str):
    """The tar of the warnings in force of the area, or None when AEMET has none."""
    try:
        answer = json.loads(fetch(url, headers={"api_key": key, "Accept": "application/json"}, timeout=timeout,
                                  label=f"AEMET avisos {area}").decode("utf-8", "replace"))
    except RuntimeError as error:
        # "No data" comes as a JSON with estado 404, sometimes with the HTTP code 404 too.
        if str(error).startswith("HTTP 404") and "No hay datos" in str(error):
            return None
        raise
    state = int(answer.get("estado", 0))
    if state == 404:
        return None
    if state != 200 or not answer.get("datos"):
        raise RuntimeError(f"unexpected answer {str(answer)[:200]}")
    return fetch(answer["datos"], headers={"api_key": key}, timeout=timeout, label=f"AEMET datos {area}")


def land_warnings(fs, cfg: dict, folder: str) -> tuple:
    """(areas saved, areas failed): the warnings in force of every area in <folder>/avisos/<area>.tar."""
    avisos = cfg["avisos"]
    variable = avisos.get("clave_env", "AEMET_API_KEY")
    key = os.environ.get(variable, "").strip()
    if not key:
        logger.error("No %s in the environment: the warnings of AEMET need the free key of opendata.aemet.es",
                     variable)
        return 0, 1
    timeout = float(avisos.get("timeout_s", 120))
    done = failed = 0
    for area in avisos.get("areas", ["esp"]):
        stamp = {"area": area, "pedido_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        archive, empty = f"{folder}/avisos/{area}.tar", f"{folder}/avisos/{area}.vacio.json"
        try:
            raw = ask_warnings(avisos["api"].format(area=area), key, timeout, area)
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("Warnings of AEMET for %s not downloaded: %s", area, str(error)[:200])
            continue
        # Only the last answer of the evening stays: a tar and its stamp, or the empty stamp.
        if raw is None:
            with fs.open(empty, "w") as file:
                json.dump(stamp, file, ensure_ascii=False)
            for old in (archive, f"{folder}/avisos/{area}.json"):
                if fs.exists(old):
                    fs.rm(old)
            logger.info("AEMET %s: no warnings in force", area)
        else:
            with fs.open(archive, "wb") as file:
                file.write(raw)
            with fs.open(f"{folder}/avisos/{area}.json", "w") as file:
                json.dump({**stamp, "bytes": len(raw)}, file, ensure_ascii=False)
            if fs.exists(empty):
                fs.rm(empty)
            logger.info("AEMET %s: %s bytes", area, len(raw))
        done += 1
    return done, failed


def main(argv=None, fs=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fuente", required=True, choices=SOURCES)
    parser.add_argument("--fecha", default="", help="Day to predict, YYYY-MM-DD (tomorrow by default)")
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)["riesgo_manana"]
    fs = fs or filesystem()
    day = target_day(args.fecha, cfg.get("zona_horaria", "Europe/Madrid"))
    folder = day_folder(cfg["landing"], day)
    logger.info("Landing of %s for %s in %s", args.fuente, day, folder)

    if args.fuente == "prevision":
        cells = study_cells(fs, load_meteo_config()["openmeteo"])
        done, failed = land_forecast(fs, cfg, folder, cells)
        # A few cells may fail and be scored with the others; without most of them there is no forecast.
        if done < (1.0 - float(cfg["prevision"].get("max_fallos", 0.1))) * len(cells):
            logger.error("Only %s of %s cells have a forecast", done, len(cells))
            return 1
        return 0

    done, failed = land_warnings(fs, cfg, folder)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
