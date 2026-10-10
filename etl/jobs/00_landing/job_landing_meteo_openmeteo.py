"""
Landing of the hourly weather of the municipalities of the study, from the
Open-Meteo Historical Weather API (https://open-meteo.com, CC BY 4.0, free
plan for non commercial use).

- Model: ECMWF IFS (9 km, hourly, from 2017), the only one of the free
  reanalyses with wind gusts and precipitation together.
- Places: the municipalities of data/reference_data/municipios.xlsx (sheet
  Municipios, already in Landing for the Bronze job of the reference), grouped
  in cells of celda_grados (0.1 degrees, about 11 km, close to the 9 km grid
  of the model). The 73 municipalities fall in 44 cells, so the whole window
  fits in the daily limit of the free plan.
- Window: config desde / hasta. The dataset of Gold covers 01.01.2021 -
  14.08.2026 and the features look back up to 7 days, so the default asks
  from 01.12.2020 to 31.08.2026.
- One request per cell and year, saved as it comes (JSON) in
  s3://datalake/00_landing/meteo/openmeteo/<cell>/<year>.json. A file that is
  already there is not asked again: a run that stops (network, limit of the
  day) is continued by launching it again.
- Limits of the free plan: 600 calls per minute, 5.000 per hour, 10.000 per
  day, and a request counts as one call per location for every 14 days and
  10 variables. pausa_s between requests keeps the hourly count below the
  limit; a 429 waits and retries, and the daily limit stops the run cleanly
  (exit code 3) so it can go on the next day.
- Timestamps are asked as unix time in UTC; Bronze takes them to the wall
  clock of Madrid, which is how Calser and TedisNet store their times.

Run from the Spark master container (it has pandas, s3fs and the MinIO
credentials; no Spark is needed):

    docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_meteo_openmeteo.py
"""

import argparse
import json
import logging
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("meteo")

CONFIG_CANDIDATES = [
    "/app/config/01_bronze/config_bronze_meteo.json",
    "/opt/airflow/config/01_bronze/config_bronze_meteo.json",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "01_bronze",
                 "config_bronze_meteo.json"),
]

API = "https://archive-api.open-meteo.com/v1/archive"
DAILY_LIMIT_EXIT = 3


def load_config(path: str = None) -> dict:
    for candidate in [path] if path else CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                return json.load(file)

    raise FileNotFoundError(f"config_bronze_meteo.json not found in {CONFIG_CANDIDATES}")


def to_float(value):
    try:
        number = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None

    return number if math.isfinite(number) else None


def cell_of(lat: float, lon: float, size: float) -> tuple:
    """(cell id, latitude, longitude of the cell) for a size in degrees."""
    clat = round(round(lat / size) * size, 4)
    clon = round(round(lon / size) * size, 4)
    return f"{clat:.2f}_{clon:.2f}", clat, clon


def municipality_cells(rows: list, size: float) -> list:
    """
    rows: dicts with codigo_ine, municipio, distribuidora, latitud and
    longitud as read from the reference sheet. Returns one dict per
    municipality with its cell; rows without coordinates are left out.
    """
    out = []

    for row in rows:
        lat, lon = to_float(row.get("latitud")), to_float(row.get("longitud"))

        if lat is None or lon is None:
            logger.warning("Municipality without coordinates, no weather: %s", row.get("municipio"))
            continue

        cell, clat, clon = cell_of(lat, lon, size)
        out.append({
            "codigo_ine": str(row.get("codigo_ine") or "").strip().zfill(5),
            "municipio": row.get("municipio"),
            "distribuidora_ref": row.get("distribuidora"),
            "latitud": lat,
            "longitud": lon,
            "celda_id": cell,
            "celda_latitud": clat,
            "celda_longitud": clon,
        })

    return out


def year_chunks(first: date, last: date) -> list:
    """[(year, first day, last day)] covering [first, last]."""
    chunks = []

    for year in range(first.year, last.year + 1):
        lo = max(first, date(year, 1, 1))
        hi = min(last, date(year, 12, 31))
        chunks.append((year, lo, hi))

    return chunks


def request_url(lat: float, lon: float, lo: date, hi: date, cfg: dict) -> str:
    query = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "start_date": lo.isoformat(),
        "end_date": hi.isoformat(),
        "hourly": ",".join(cfg["variables"]),
        "models": cfg.get("modelo", "ecmwf_ifs"),
        "timezone": "GMT",
        "timeformat": "unixtime",
        "wind_speed_unit": "kmh",
    }
    return f"{API}?{urllib.parse.urlencode(query, safe=',')}"


def call_weight(days: int, n_variables: int) -> float:
    """Calls the free plan counts for one location."""
    return max(1.0, days / 14.0) * max(1.0, n_variables / 10.0)


def fetch(url: str, retries: int, wait_s: float) -> dict:
    """GET with retries. Raises SystemExit(3) on the daily limit."""
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "replace")

            if error.code == 429 and "daily" in body.lower():
                logger.error("Daily limit of the free plan reached: %s. Launch again tomorrow", body[:200])
                raise SystemExit(DAILY_LIMIT_EXIT)

            if error.code in (429, 500, 502, 503, 504) and attempt < retries:
                logger.warning("HTTP %s (%s), waiting %.0f s, attempt %s of %s",
                               error.code, body[:120], wait_s * attempt, attempt, retries)
                time.sleep(wait_s * attempt)
                continue

            raise RuntimeError(f"HTTP {error.code} for {url}: {body[:300]}") from error
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt < retries:
                logger.warning("%s, waiting %.0f s, attempt %s of %s", error, wait_s * attempt, attempt, retries)
                time.sleep(wait_s * attempt)
                continue
            raise

    raise RuntimeError(f"No answer for {url}")


def filesystem():
    import s3fs

    endpoint = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint})


def read_reference(fs, path: str) -> list:
    import pandas as pd

    with fs.open(path, "rb") as file:
        pdf = pd.read_excel(file, sheet_name="Municipios", dtype=str)

    columns = {c: c.strip().lower().replace(" ", "_") for c in pdf.columns}
    pdf = pdf.rename(columns=columns)
    return pdf.to_dict("records")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--max-peticiones", type=int, default=None, help="Stop after this many requests")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)["openmeteo"]
    fs = filesystem()
    landing = cfg["landing"].rstrip("/")

    places = municipality_cells(read_reference(fs, cfg["referencia_municipios"]), float(cfg["celda_grados"]))
    cells = {}
    for place in places:
        cells.setdefault(place["celda_id"], (place["celda_latitud"], place["celda_longitud"]))

    with fs.open(f"{landing}/municipios_celdas.json", "w") as file:
        json.dump(places, file, ensure_ascii=False, indent=1)

    first, last = date.fromisoformat(cfg["desde"]), date.fromisoformat(cfg["hasta"])
    chunks = year_chunks(first, last)
    n_variables = len(cfg["variables"])
    total_weight = sum(call_weight((hi - lo).days + 1, n_variables) for _, lo, hi in chunks) * len(cells)

    logger.info("%s municipalities in %s cells, %s to %s, about %.0f calls of the free plan",
                len(places), len(cells), first, last, total_weight)

    done = requested = 0
    pause = float(cfg.get("pausa_s", 25))

    for cell, (lat, lon) in sorted(cells.items()):
        for year, lo, hi in chunks:
            target = f"{landing}/{cell}/{year}.json"

            if fs.exists(target):
                done += 1
                continue

            if args.max_peticiones is not None and requested >= args.max_peticiones:
                logger.info("Stopped after %s requests as asked", requested)
                return 0

            payload = fetch(request_url(lat, lon, lo, hi, cfg), int(cfg.get("reintentos", 5)),
                            float(cfg.get("espera_reintento_s", 60)))

            hours = len(payload.get("hourly", {}).get("time", []))
            if not hours:
                raise RuntimeError(f"Empty answer for cell {cell}, {year}: {str(payload)[:300]}")

            payload["_peticion"] = {"celda_id": cell, "desde": lo.isoformat(), "hasta": hi.isoformat(),
                                    "modelo": cfg.get("modelo", "ecmwf_ifs")}

            with fs.open(target, "w") as file:
                json.dump(payload, file)

            requested += 1
            logger.info("Cell %s, %s: %s hours (%s requested, %s already there)", cell, year, hours, requested, done)
            time.sleep(pause)

    logger.info("Landing complete: %s files requested now, %s already there", requested, done)
    return 0


if __name__ == "__main__":
    sys.exit(main())
