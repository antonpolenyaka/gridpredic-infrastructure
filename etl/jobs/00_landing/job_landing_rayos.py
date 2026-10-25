"""
Lightning over the network, live (dag_rayos_vivo, every 15 minutes).
Configuration: riesgo_manana.rayos of config_ml.json.

The Lightning Imager of MTG publishes the flashes it sees every 10 minutes
(EUMETSAT Data Store, collection LI-2-LFL, one product per 10 minutes, a zip
with a NetCDF of flashes with position, time, duration and radiance). Before
17 % of the incidents of 2024 - 2025 there were flashes within 10 km of the
network in the previous hour, against 1.1 % of the control windows, so a
storm over the network is the most direct sign of the next hour we have
(causas-incidencias.md, apartado 8). The figure is of the network of each
distribuidora; for a single line it is only an indication.

Every run asks the Data Store for the products of the last ventana_min +
retraso_min minutes, downloads the ones that are not yet in the Landing,
keeps the flashes inside the study box (caja: lat_min, lon_min, lat_max,
lon_max) and saves them in <landing>/rayos/<YYYYMMDD>/flashes_<start>_<end>.csv
(one file per product, empty when the product has no flash in the box, so
it is not asked again). Then it writes rayos.json for the dashboard
(dashboard_dir): the flashes of the last ventana_min minutes and, with the
municipalities of riesgo.json of the same folder (what dag_riesgo_manana
left), how many fell within radio_km of the network of every distribuidora
and which lines they touch.

The credentials of the Data Store (a free account at data.eumetsat.int)
come from the environment variables clave_env and secreto_env, go in a
header and are never written in the log. Without them the job does nothing
and returns 2.

    docker compose exec airflow python /app/jobs/00_landing/job_landing_rayos.py
    docker compose exec airflow python /app/jobs/00_landing/job_landing_rayos.py --sin-descarga   # only the JSON
"""

import argparse
import base64
import io
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_meteo_openmeteo import filesystem  # noqa: E402
from job_landing_vivo import load_config, write_json_atomic  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rayos")

DEFAULTS = {
    "api": "https://api.eumetsat.int",
    "coleccion": "EO:EUM:DAT:0691",
    "caja": [38.1, -7.3, 40.6, -5.4],
    "ventana_min": 60,
    "retraso_min": 30,
    # minutes at the end of the window that the Data Store may not have published yet: a gap there is not a gap
    "margen_min": 15,
    "max_productos": 12,
    "radio_km": [10.0, 20.0],
    "clave_env": "EUMETSAT_CONSUMER_KEY",
    "secreto_env": "EUMETSAT_CONSUMER_SECRET",
    "timeout_s": 120,
    "pausa_s": 0.2,
}
EPOCH = pd.Timestamp("2000-01-01")
EARTH_KM = 6371.0
COLUMNS = ["ts_utc", "lat", "lon", "duracion_ms", "radiancia", "confianza", "eventos", "producto"]


def lightning_settings(config: dict) -> dict:
    rcfg = config.get("riesgo_manana", {})
    cfg = {**DEFAULTS, **rcfg.get("rayos", {})}
    cfg["landing"] = rcfg.get("landing", "s3://datalake/00_landing/vivo")
    cfg["dashboard_dir"] = rcfg.get("dashboard_dir", "/app/dashboard/data")
    return cfg


# ---------------------------------------------------------------------------
# Data Store
# ---------------------------------------------------------------------------

class Expired(Exception):
    """The token of the Data Store is no longer valid (HTTP 401)."""


def credentials(cfg: dict) -> tuple:
    key = os.environ.get(cfg["clave_env"], "").strip()
    secret = os.environ.get(cfg["secreto_env"], "").strip()
    return key, secret


def token(cfg: dict, key: str, secret: str) -> str:
    """A bearer token of the Data Store (client credentials). The key goes in a header, never in a URL or a log."""
    request = urllib.request.Request(
        f"{cfg['api']}/token", data=b"grant_type=client_credentials",
        headers={"Authorization": "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode(),
                 "Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(request, timeout=float(cfg["timeout_s"])) as answer:
        return json.loads(answer.read())["access_token"]


def get(url: str, tok: str, timeout: float, label: str) -> bytes:
    """GET with the token, three tries on 429, 5xx and network errors; Expired on 401."""
    for attempt in range(1, 4):
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}",
                                                       "User-Agent": "GridPredic-TFM/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as answer:
                return answer.read()
        except urllib.error.HTTPError as error:
            if error.code == 401:
                raise Expired() from None
            if error.code in (429, 500, 502, 503, 504) and attempt < 3:
                logger.warning("HTTP %s for %s, attempt %s of 3", error.code, label, attempt)
                time.sleep(10 * attempt)
                continue
            raise RuntimeError(f"HTTP {error.code} for {label}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt < 3:
                logger.warning("%s for %s, attempt %s of 3", type(error).__name__, label, attempt)
                time.sleep(10 * attempt)
                continue
            raise RuntimeError(f"{type(error).__name__} for {label}") from None


def search(cfg: dict, tok: str, start: datetime, end: datetime) -> list:
    """Ids of the collection products sensed between start and end (UTC)."""
    query = urllib.parse.urlencode({"format": "json", "pi": cfg["coleccion"], "c": 200, "si": 0,
                                    "dtstart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "dtend": end.strftime("%Y-%m-%dT%H:%M:%SZ")})
    found = json.loads(get(f"{cfg['api']}/data/search-products/1.0.0/os?{query}", tok, float(cfg["timeout_s"]),
                           "Data Store search"))
    return [feature["id"] for feature in found.get("features", [])]


def download(cfg: dict, tok: str, product: str) -> bytes:
    url = (f"{cfg['api']}/data/download/1.0.0/collections/{urllib.parse.quote(cfg['coleccion'], safe='')}"
           f"/products/{urllib.parse.quote(product, safe='')}")
    return get(url, tok, float(cfg["timeout_s"]) * 3, f"Data Store product {product_window(product)[0]}")


def product_window(product: str) -> tuple:
    """(start, end) of the sensing of a product as YYYYMMDDHHMMSS texts: the last two stamps of its id."""
    stamps = re.findall(r"(?<!\d)(\d{14})(?!\d)", product)
    if len(stamps) >= 2:
        return stamps[-2], stamps[-1]
    if stamps:
        return stamps[0], stamps[0]
    return "00000000000000", "00000000000000"


def scaled(dataset) -> np.ndarray:
    values = dataset[:].astype(np.float64)
    fill = dataset.attrs.get("_FillValue")
    if fill is not None:
        values[values == float(np.asarray(fill).ravel()[0])] = np.nan
    scale = dataset.attrs.get("scale_factor")
    offset = dataset.attrs.get("add_offset")
    if scale is not None:
        values = values * float(np.asarray(scale).ravel()[0])
    if offset is not None:
        values = values + float(np.asarray(offset).ravel()[0])
    return values


def flashes(raw: bytes, box: list, product: str = "") -> pd.DataFrame:
    """The flashes of a product (a zip with BODY NetCDFs) inside the box, with the columns of COLUMNS."""
    import h5py

    frames = []
    archive = zipfile.ZipFile(io.BytesIO(raw))
    for name in archive.namelist():
        if "BODY" not in name or not name.endswith(".nc"):
            continue
        with h5py.File(io.BytesIO(archive.read(name)), "r") as h:
            lat, lon = scaled(h["latitude"]), scaled(h["longitude"])
            inside = (lat >= box[0]) & (lat <= box[2]) & (lon >= box[1]) & (lon <= box[3])
            if not inside.any():
                continue
            frames.append(pd.DataFrame({
                "ts_utc": EPOCH + pd.to_timedelta(h["flash_time"][:][inside].astype(np.float64), unit="s"),
                "lat": lat[inside], "lon": lon[inside],
                "duracion_ms": h["flash_duration"][:][inside].astype(float),
                "radiancia": scaled(h["radiance"])[inside],
                "confianza": scaled(h["flash_filter_confidence"])[inside],
                "eventos": h["number_of_events"][:][inside].astype(float),
            }))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS[:-1])
    out["producto"] = product
    return out[COLUMNS]


# ---------------------------------------------------------------------------
# Landing
# ---------------------------------------------------------------------------

def day_folder(landing: str, stamp: str) -> str:
    return f"{landing.rstrip('/')}/rayos/{stamp[:8]}"


def product_path(landing: str, product: str) -> str:
    start, end = product_window(product)
    return f"{day_folder(landing, start)}/flashes_{start}_{end}.csv"


def land(fs, cfg: dict, now: datetime, key: str, secret: str) -> dict:
    """
    The products of the last ventana_min + retraso_min minutes that are not
    in the Landing yet, newest first, at most max_productos per run.
    Returns what happened: products found, downloaded, failed, flashes.
    """
    since = now - timedelta(minutes=float(cfg["ventana_min"]) + float(cfg["retraso_min"]))
    tok = token(cfg, key, secret)
    products = search(cfg, tok, since, now)
    pending = sorted((p for p in products if not fs.exists(product_path(cfg["landing"], p))), reverse=True)
    pending = pending[: int(cfg["max_productos"])]
    out = {"encontrados": len(products), "descargados": 0, "fallidos": 0, "flashes": 0}
    logger.info("Data Store: %s products since %s UTC, %s to download", len(products), since.strftime("%H:%M"),
                len(pending))
    for product in pending:
        try:
            try:
                raw = download(cfg, tok, product)
            except Expired:
                tok = token(cfg, key, secret)
                raw = download(cfg, tok, product)
            frame = flashes(raw, cfg["caja"], product)
        except Exception as error:  # noqa: BLE001
            out["fallidos"] += 1
            logger.error("Product %s not landed: %s", product_window(product)[0], str(error)[:200])
            continue
        path = product_path(cfg["landing"], product)
        with fs.open(path, "w") as file:
            frame.to_csv(file, index=False, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
        out["descargados"] += 1
        out["flashes"] += len(frame)
        time.sleep(float(cfg["pausa_s"]))
    logger.info("Lightning landed: %s products, %s flashes in the box, %s failed", out["descargados"],
                out["flashes"], out["fallidos"])
    return out


def recent_flashes(fs, cfg: dict, now: datetime, minutes: float) -> pd.DataFrame:
    """The flashes of the Landing of the last minutes (today and yesterday, by the end of each product)."""
    since = now - timedelta(minutes=minutes)
    frames = []
    for day in {since.strftime("%Y%m%d"), now.strftime("%Y%m%d")}:
        folder = day_folder(cfg["landing"], day)
        if not fs.exists(folder):
            continue
        for path in fs.ls(folder, detail=False):
            name = os.path.basename(path)
            match = re.match(r"flashes_(\d{14})_(\d{14})\.csv$", name)
            if not match or match.group(2) < since.strftime("%Y%m%d%H%M%S"):
                continue
            with fs.open(path, "r") as file:
                frame = pd.read_csv(file)
            if len(frame):
                frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out["ts_utc"] = pd.to_datetime(out["ts_utc"], utc=True, format="ISO8601")
    out = out[out["ts_utc"] >= pd.Timestamp(since)].sort_values("ts_utc")
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    la1, lo1 = np.radians(np.asarray(lat1, dtype=float))[:, None], np.radians(np.asarray(lon1, dtype=float))[:, None]
    la2, lo2 = np.radians(np.asarray(lat2, dtype=float))[None, :], np.radians(np.asarray(lon2, dtype=float))[None, :]
    h = np.sin((la2 - la1) / 2) ** 2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(h))


def near_network(recent: pd.DataFrame, risk: dict, radii: list) -> list:
    """
    Per distribuidora of riesgo.json (its municipalities with lat, lon and
    zonas): the flashes within every radius of some municipality, the lines
    (zones) within the first radius and a sentence for the dashboard.
    """
    out = []
    for dist in (risk or {}).get("distribuidoras", []):
        places = [m for m in dist.get("municipios", []) if m.get("lat") is not None and m.get("lon") is not None]
        block = {"id": dist.get("id"), "nombre": dist.get("nombre"), "radios_km": list(radii),
                 "cerca": {f"{r:g}": 0 for r in radii}, "lineas": [], "municipios": []}
        if places and len(recent):
            d = haversine_km(recent["lat"], recent["lon"], [m["lat"] for m in places], [m["lon"] for m in places])
            nearest = d.min(axis=1)
            for r in radii:
                block["cerca"][f"{r:g}"] = int((nearest <= r).sum())
            hit = (d <= radii[0]).any(axis=0)
            touched = [places[i] for i in np.flatnonzero(hit)]
            block["municipios"] = [m["nombre"] for m in touched]
            lines = []
            for m in touched:
                for z in m.get("zonas", []):
                    if z not in lines:
                        lines.append(z)
            block["lineas"] = lines
        n = block["cerca"][f"{radii[0]:g}"]
        if n:
            block["texto"] = (f"{n} rayo{'s' if n > 1 else ''} a menos de {radii[0]:g} km de la red en la última hora"
                              + (f" (líneas: {', '.join(block['lineas'][:4])}" + (", ..." if len(block["lineas"]) > 4 else "") + ")"
                                 if block["lineas"] else ""))
            block["nivel"] = "tormenta sobre la red"
        elif len(radii) > 1 and block["cerca"][f"{radii[1]:g}"]:
            m = block["cerca"][f"{radii[1]:g}"]
            block["texto"] = f"{m} rayo{'s' if m > 1 else ''} a menos de {radii[1]:g} km de la red en la última hora"
            block["nivel"] = "tormenta cerca"
        else:
            block["texto"] = "sin rayos cerca de la red en la última hora"
            block["nivel"] = "sin tormenta"
        out.append(block)
    return out


def unknown_without_reading(blocks: list, landed: dict) -> list:
    """
    When the last hour could not be read at all (no product of the window in
    the Landing), "sin tormenta" would read as an observed absence of
    lightning: those blocks say instead that there are no current data. When
    only part of the window was read, they say it is an incomplete reading.
    The flashes already landed still count when they are there.
    """
    if landed.get("error"):
        for block in blocks:
            if block.get("nivel") == "sin tormenta":
                block["nivel"] = "sin datos"
                block["texto"] = "sin datos actuales de rayos: no se pudo leer la última hora"
    elif landed.get("incompleta"):
        # part of the hour was not read: no lightning in what was read is not an hour without lightning
        for block in blocks:
            if block.get("nivel") == "sin tormenta":
                block["nivel"] = "lectura incompleta"
                block["texto"] = f"sin rayos cerca de la red en lo que se pudo leer de la última hora ({landed['incompleta']})"
    return blocks


def landed_intervals(fs, cfg: dict, since: datetime, until: datetime) -> list:
    """The sensing intervals [start, end) of the products in the Landing, clipped to [since, until], merged."""
    spans = []
    for day in sorted({since.strftime("%Y%m%d"), until.strftime("%Y%m%d")}):
        folder = day_folder(cfg["landing"], day)
        if not fs.exists(folder):
            continue
        for path in fs.ls(folder, detail=False):
            match = re.match(r"flashes_(\d{14})_(\d{14})\.csv$", os.path.basename(path))
            if not match:
                continue
            start = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            end = datetime.strptime(match.group(2), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            start, end = max(start, since), min(end, until)
            if end > start:
                spans.append((start, end))
    merged = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def covered_minutes(fs, cfg: dict, now: datetime, minutes: float) -> int:
    """
    The minutes of the last window really read: the union of the sensing
    intervals of the products in the Landing, clipped to the window (a
    product that ends before the window starts does not count, and two
    products over the same minutes count once).
    """
    since = now - timedelta(minutes=minutes)
    seconds = sum((end - start).total_seconds() for start, end in landed_intervals(fs, cfg, since, now))
    return int(seconds // 60)


def window_complete(fs, cfg: dict, now: datetime, minutes: float) -> bool:
    """
    Whether the products in the Landing cover the whole window except its
    last margen_min minutes, which the Data Store may not have published yet
    (a minute of tolerance for the stamps).
    """
    since = now - timedelta(minutes=minutes)
    until = now - timedelta(minutes=float(cfg.get("margen_min", 15)))
    if until <= since:
        return True
    seconds = sum((end - start).total_seconds() for start, end in landed_intervals(fs, cfg, since, until))
    return seconds >= (until - since).total_seconds() - 60


def dashboard_payload(recent: pd.DataFrame, risk: dict, cfg: dict, now: datetime, landed: dict,
                      covered: int = None) -> dict:
    radii = [float(r) for r in cfg["radio_km"]]
    rows = [{"ts": t.strftime("%Y-%m-%dT%H:%MZ"), "lat": round(float(a), 4), "lon": round(float(o), 4),
             "minutos": int((now - t.to_pydatetime()).total_seconds() // 60)}
            for t, a, o in zip(recent["ts_utc"], recent["lat"], recent["lon"])] if len(recent) else []
    return {
        "generado": now.strftime("%Y-%m-%dT%H:%MZ"),
        "ventana_min": float(cfg["ventana_min"]),
        "minutos_leidos": covered,
        "fuente": "EUMETSAT, Lightning Imager de MTG (LI-2-LFL, cada 10 minutos)",
        "caja": list(cfg["caja"]),
        "ultimo_producto": landed,
        "n_rayos": len(rows),
        "rayos": rows,
        "distribuidoras": unknown_without_reading(near_network(recent, risk, radii), landed),
        "regla": ("Antes del 17 % de las incidencias de 2024 - 2025 hubo rayos a menos de 10 km de la red en la hora "
                  "anterior, frente al 1,1 % de las ventanas sin incidencias."),
    }


def read_risk(folder: str) -> dict:
    path = os.path.join(folder, "riesgo.json")
    if not folder or not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError) as error:
        logger.warning("riesgo.json not read: %s", error)
        return {}


def write_dashboard(payload: dict, folder: str) -> bool:
    if not folder or not os.path.isdir(folder):
        logger.warning("Dashboard folder %s not found: rayos.json not written", folder)
        return False
    path = os.path.join(folder, "rayos.json")
    write_json_atomic(path, payload, ensure_ascii=False, indent=1)
    logger.info("rayos.json written in %s (%s flashes in the last %s minutes)", path, payload["n_rayos"],
                int(payload["ventana_min"]))
    return True


def main(argv=None, fs=None, now: datetime = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--dashboard-dir", default=None, help="Folder of the dashboard (riesgo_manana.dashboard_dir)")
    parser.add_argument("--landing", default=None, help="Folder of the live Landing (riesgo_manana.landing)")
    parser.add_argument("--sin-descarga", action="store_true", help="Only rebuild rayos.json from the Landing")
    args = parser.parse_args(argv)

    cfg = lightning_settings(load_config(args.config))
    if args.dashboard_dir:
        cfg["dashboard_dir"] = args.dashboard_dir
    if args.landing:
        cfg["landing"] = args.landing
    now = now or datetime.now(timezone.utc)
    fs = fs or filesystem()
    landed = {}
    if not args.sin_descarga:
        key, secret = credentials(cfg)
        if not key or not secret:
            logger.error("No %s and %s in the environment: the lightning needs the credentials of the Data Store "
                         "of EUMETSAT", cfg["clave_env"], cfg["secreto_env"])
            return 2
        try:
            landed = land(fs, cfg, now, key, secret)
        except Exception as error:  # noqa: BLE001
            logger.error("The Data Store did not answer: %s", str(error)[:200])
            landed = {"error": str(error)[:200]}
    recent = recent_flashes(fs, cfg, now, float(cfg["ventana_min"]))
    covered = covered_minutes(fs, cfg, now, float(cfg["ventana_min"]))
    complete = window_complete(fs, cfg, now, float(cfg["ventana_min"]))
    problem = landed.get("error")
    if not problem and landed.get("fallidos"):
        problem = f"{landed['fallidos']} de {landed['fallidos'] + landed.get('descargados', 0)} productos sin descargar"
    if covered == 0:
        # nothing of the window was read: the last hour was not observed
        if landed.get("fallidos") and not landed.get("descargados") and not landed.get("error"):
            problem = f"no se pudo descargar ninguno de los {landed['fallidos']} productos encontrados"
        landed["error"] = problem or "ningún producto de la última hora en la Landing"
        logger.error("Lightning: %s", landed["error"])
    elif problem or not complete:
        # part of the window was read (now or by the earlier runs): what was read counts, but it is not a whole
        # hour, and a failed download of the last product is not an hour without data
        landed.pop("error", None)
        landed["incompleta"] = (f"lectura incompleta: {problem or 'faltan productos de la ventana'}, "
                                f"{covered} de {int(float(cfg['ventana_min']))} min leídos")
        logger.warning("Lightning: %s", landed["incompleta"])
    payload = dashboard_payload(recent, read_risk(cfg["dashboard_dir"]), cfg, now, landed, covered)
    write_dashboard(payload, cfg["dashboard_dir"])
    for block in payload["distribuidoras"]:
        logger.info("%s: %s", block["nombre"], block["texto"])
    return 1 if landed.get("error") else 0


if __name__ == "__main__":
    sys.exit(main())
