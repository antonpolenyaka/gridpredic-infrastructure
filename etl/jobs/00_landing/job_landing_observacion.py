"""
What the stations of AEMET are measuring now, for the dashboard
(dag_observacion_vivo, every hour). Configuration: riesgo_manana.observacion
of config_ml.json.

AEMET OpenData gives the last 24 hours of every conventional station of
Spain (observacion/convencional/todas: gust, wind, rain, temperature every
hour). Every run saves the records of the stations inside the study box in
<landing>/observacion/<YYYYMMDD>/obs_<HHMM>.json (as AEMET gives them, with
the time of the request) and writes observado.json for the dashboard: per
distribuidora, the strongest gust and the rain of every hour of the day
measured by the stations within radio_km of its municipalities (what the
forecast of the day said against what is happening), and the stations with
their last values for the map. The free key of opendata.aemet.es goes in a
header and is never written in the log.

    docker compose exec airflow python /app/jobs/00_landing/job_landing_observacion.py
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_datos_publicos import fetch  # noqa: E402
from job_landing_meteo_openmeteo import filesystem  # noqa: E402
from job_landing_rayos import haversine_km, read_risk  # noqa: E402
from job_landing_vivo import load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("observacion")

DEFAULTS = {
    "api": "https://opendata.aemet.es/opendata/api/observacion/convencional/todas",
    "clave_env": "AEMET_API_KEY",
    "caja": [38.1, -7.3, 40.6, -5.4],
    "radio_km": 25.0,
    "timeout_s": 120,
}
FIELDS = {"vmax": "racha_ms", "vv": "viento_ms", "prec": "lluvia_mm", "ta": "temp_c", "hr": "humedad"}


def observation_settings(config: dict) -> dict:
    rcfg = config.get("riesgo_manana", {})
    cfg = {**DEFAULTS, **rcfg.get("observacion", {})}
    cfg["landing"] = rcfg.get("landing", "s3://datalake/00_landing/vivo")
    cfg["dashboard_dir"] = rcfg.get("dashboard_dir", "/app/dashboard/data")
    cfg["zona_horaria"] = rcfg.get("zona_horaria", "Europe/Madrid")
    return cfg


def ask_observations(cfg: dict, key: str) -> list:
    """The records of the last 24 hours of every station (two requests: the pointer and the data)."""
    timeout = float(cfg["timeout_s"])
    answer = json.loads(fetch(cfg["api"], headers={"api_key": key, "Accept": "application/json"}, timeout=timeout,
                              label="AEMET observacion").decode("utf-8", "replace"))
    if int(answer.get("estado", 0)) != 200 or not answer.get("datos"):
        raise RuntimeError(f"unexpected answer {str(answer)[:160]}")
    raw = fetch(answer["datos"], headers={"api_key": key}, timeout=timeout, label="AEMET datos observacion")
    return json.loads(decode(raw))


def decode(raw: bytes) -> str:
    """AEMET serves its data as UTF-8 (sometimes with a BOM) or as ISO-8859-15."""
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("iso-8859-15", "replace")


def inside(records: list, box: list) -> pd.DataFrame:
    """The records inside the box as a frame: idema, nombre, lat, lon, ts_utc and the FIELDS."""
    rows = []
    for r in records:
        try:
            lat, lon = float(r["lat"]), float(r["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (box[0] <= lat <= box[2] and box[1] <= lon <= box[3]):
            continue
        row = {"idema": r.get("idema"), "nombre": r.get("ubi"), "lat": lat, "lon": lon, "ts_utc": r.get("fint")}
        for source, name in FIELDS.items():
            try:
                row[name] = float(r[source]) if r.get(source) is not None else np.nan
            except (TypeError, ValueError):
                row[name] = np.nan
        rows.append(row)
    out = pd.DataFrame(rows, columns=["idema", "nombre", "lat", "lon", "ts_utc"] + list(FIELDS.values()))
    if len(out):
        out["ts_utc"] = pd.to_datetime(out["ts_utc"], utc=True, errors="coerce")
        out = out.dropna(subset=["ts_utc"]).sort_values(["idema", "ts_utc"]).reset_index(drop=True)
    return out


def land(fs, cfg: dict, frame: pd.DataFrame, now: datetime) -> str:
    path = f"{cfg['landing'].rstrip('/')}/observacion/{now:%Y%m%d}/obs_{now:%H%M}.json"
    payload = frame.assign(ts_utc=frame["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")).to_dict("records") if len(frame) else []
    with fs.open(path, "w") as file:
        json.dump({"pedido_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "registros": payload}, file, ensure_ascii=False)
    return path


def stations_of(frame: pd.DataFrame, risk: dict, radius: float) -> dict:
    """{distribuidora id: [idema, ...]}: the stations within radius km of some municipality of the distribuidora."""
    out = {}
    stations = frame.drop_duplicates("idema")
    for dist in (risk or {}).get("distribuidoras", []):
        places = [m for m in dist.get("municipios", []) if m.get("lat") is not None and m.get("lon") is not None]
        if not places or stations.empty:
            out[dist.get("id")] = []
            continue
        d = haversine_km(stations["lat"], stations["lon"], [m["lat"] for m in places], [m["lon"] for m in places])
        out[dist.get("id")] = stations.loc[(d.min(axis=1) <= radius), "idema"].tolist()
    return out


def dashboard_payload(frame: pd.DataFrame, risk: dict, cfg: dict, now: datetime) -> dict:
    zone = ZoneInfo(cfg["zona_horaria"])
    local_now = now.astimezone(zone)
    today = local_now.date()
    out = {"generado": now.strftime("%Y-%m-%dT%H:%MZ"), "fecha": today.isoformat(), "fuente": "AEMET OpenData, observación convencional",
           "radio_km": float(cfg["radio_km"]), "estaciones": [], "distribuidoras": []}
    if frame.empty:
        return out
    f = frame.copy()
    f["ts_local"] = f["ts_utc"].dt.tz_convert(zone)
    f["racha_kmh"] = f["racha_ms"] * 3.6
    f["viento_kmh"] = f["viento_ms"] * 3.6
    last = f.sort_values("ts_utc").groupby("idema").tail(1)
    out["estaciones"] = [{"idema": r.idema, "nombre": r.nombre, "lat": round(float(r.lat), 4), "lon": round(float(r.lon), 4),
                          "hora": r.ts_local.strftime("%H:%M"), "racha_kmh": None if pd.isna(r.racha_kmh) else round(float(r.racha_kmh)),
                          "viento_kmh": None if pd.isna(r.viento_kmh) else round(float(r.viento_kmh)),
                          "lluvia_mm": None if pd.isna(r.lluvia_mm) else round(float(r.lluvia_mm), 1),
                          "temp_c": None if pd.isna(r.temp_c) else round(float(r.temp_c), 1)} for r in last.itertuples()]
    by_dist = stations_of(f, risk, float(cfg["radio_km"]))
    today_rows = f[f["ts_local"].dt.date == today]
    for dist in (risk or {}).get("distribuidoras", []):
        ids = by_dist.get(dist.get("id"), [])
        mine = today_rows[today_rows["idema"].isin(ids)]
        hours = mine.assign(h=mine["ts_local"].dt.hour).groupby("h").agg(racha_kmh=("racha_kmh", "max"), lluvia_mm=("lluvia_mm", "max"),
                                                                        temp_c=("temp_c", "max"), estaciones=("idema", "nunique"))
        block = {"id": dist.get("id"), "nombre": dist.get("nombre"), "estaciones": len(ids),
                 "horas": [{"hora": int(h), "racha_kmh": None if pd.isna(r.racha_kmh) else round(float(r.racha_kmh)),
                            "lluvia_mm": None if pd.isna(r.lluvia_mm) else round(float(r.lluvia_mm), 1),
                            "temp_c": None if pd.isna(r.temp_c) else round(float(r.temp_c), 1), "estaciones": int(r.estaciones)}
                           for h, r in hours.iterrows()]}
        if len(hours) and hours["racha_kmh"].notna().any():
            peak = hours["racha_kmh"].idxmax()
            block["racha_max_kmh"] = round(float(hours.loc[peak, "racha_kmh"]))
            block["hora_racha_max"] = int(peak)
            block["texto"] = (f"racha máxima observada hoy {block['racha_max_kmh']} km/h a las {int(peak)}:00 "
                              f"({len(ids)} estaciones a menos de {float(cfg['radio_km']):g} km de la red)")
        else:
            block["texto"] = "sin observaciones de hoy" if ids else "sin estaciones cerca de la red"
        out["distribuidoras"].append(block)
    return out


def write_dashboard(payload: dict, folder: str) -> bool:
    if not folder or not os.path.isdir(folder):
        logger.warning("Dashboard folder %s not found: observado.json not written", folder)
        return False
    path = os.path.join(folder, "observado.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=1)
    logger.info("observado.json written in %s (%s stations, %s distribuidoras)", path, len(payload["estaciones"]),
                len(payload["distribuidoras"]))
    return True


def main(argv=None, fs=None, now: datetime = None, records: list = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--dashboard-dir", default=None)
    parser.add_argument("--landing", default=None)
    args = parser.parse_args(argv)
    cfg = observation_settings(load_config(args.config))
    if args.dashboard_dir:
        cfg["dashboard_dir"] = args.dashboard_dir
    if args.landing:
        cfg["landing"] = args.landing
    now = now or datetime.now(timezone.utc)
    if records is None:
        key = os.environ.get(cfg["clave_env"], "").strip()
        if not key:
            logger.error("No %s in the environment: the observations need the free key of opendata.aemet.es", cfg["clave_env"])
            return 2
        try:
            records = ask_observations(cfg, key)
        except Exception as error:  # noqa: BLE001
            logger.error("AEMET did not answer: %s", str(error)[:200])
            return 1
    frame = inside(records, cfg["caja"])
    logger.info("AEMET: %s records of %s stations inside the box (of %s records)", len(frame), frame["idema"].nunique() if len(frame) else 0, len(records))
    fs = fs or filesystem()
    path = land(fs, cfg, frame, now)
    logger.info("Observations landed in %s", path)
    payload = dashboard_payload(frame, read_risk(cfg["dashboard_dir"]), cfg, now)
    write_dashboard(payload, cfg["dashboard_dir"])
    for block in payload["distribuidoras"]:
        logger.info("%s: %s", block["nombre"], block["texto"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
