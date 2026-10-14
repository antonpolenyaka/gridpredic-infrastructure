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

Every run also verifies the forecast: for yesterday and today, the peak gust
that riesgo_<day>.json of the dashboard folder (what dag_riesgo_manana left
the evening before) forecast for every network against the peak the stations
near it measured, with the hour of both and whether the observed peak fell in
the forecast window. verificacion.json keeps the last verificacion_dias days
and a summary per distribuidora (bias and absolute error of the peak, share
of days with the peak inside the window), so the dashboard can say how much
the forecast of the evening before is worth.

    docker compose exec airflow python /app/jobs/00_landing/job_landing_observacion.py
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
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
    "verificacion_dias": 60,      # days kept in verificacion.json
    "horas_dia_completo": 20,     # a day counts in the summary with at least this many observed hours
}
FIELDS = {"vmax": "racha_ms", "vv": "viento_ms", "prec": "lluvia_mm", "ta": "temp_c", "hr": "humedad"}
COLUMNS = ["idema", "nombre", "lat", "lon", "ts_utc"] + list(FIELDS.values())


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
    out = pd.DataFrame(rows, columns=COLUMNS)
    if len(out):
        out["ts_utc"] = pd.to_datetime(out["ts_utc"], utc=True, errors="coerce")
        out = out.dropna(subset=["ts_utc"]).sort_values(["idema", "ts_utc"]).reset_index(drop=True)
    return out


def landed_frame(fs, path: str) -> pd.DataFrame:
    """A file of the Landing back as a frame (the records already carry the FIELDS names and ts_utc)."""
    with fs.open(path, "r") as file:
        saved = json.load(file)
    out = pd.DataFrame(saved.get("registros", []), columns=COLUMNS)
    if len(out):
        out["ts_utc"] = pd.to_datetime(out["ts_utc"], utc=True, errors="coerce")
        out = out.dropna(subset=["ts_utc"])
    return out


def last_landed(fs, cfg: dict, day: datetime) -> pd.DataFrame:
    """The last file landed on a UTC day (empty when there is none)."""
    folder = f"{cfg['landing'].rstrip('/')}/observacion/{day:%Y%m%d}"
    try:
        files = sorted(f for f in fs.ls(folder, detail=False) if str(f).endswith(".json"))
    except FileNotFoundError:
        return pd.DataFrame(columns=COLUMNS)
    if not files:
        return pd.DataFrame(columns=COLUMNS)
    return landed_frame(fs, files[-1])


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


def archived_forecast(folder: str, day: date) -> dict:
    """riesgo_<day>.json of the dashboard folder: what the evening before said about that day (None without it)."""
    path = os.path.join(folder or "", f"riesgo_{day:%Y%m%d}.json")
    if not folder or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def verify_day(frame: pd.DataFrame, risk: dict, cfg: dict, day: date) -> list:
    """
    One record per distribuidora of riesgo_<day>.json: the peak gust it forecast
    (hour and km/h) against the peak the stations near the network measured
    that local day, and whether the observed peak fell in the forecast window.
    """
    zone = ZoneInfo(cfg["zona_horaria"])
    out = []
    if not risk:
        return out
    f = frame.copy() if len(frame) else pd.DataFrame(columns=COLUMNS)
    if len(f):
        f["ts_local"] = f["ts_utc"].dt.tz_convert(zone)
        f["racha_kmh"] = f["racha_ms"] * 3.6
        f = f[f["ts_local"].dt.date == day]
    by_dist = stations_of(frame, risk, float(cfg["radio_km"])) if len(frame) else {}
    for dist in risk.get("distribuidoras", []):
        ids = by_dist.get(dist.get("id"), [])
        mine = f[f["idema"].isin(ids)] if len(f) else f
        hours = mine.assign(h=mine["ts_local"].dt.hour).groupby("h")["racha_kmh"].max().dropna() if len(mine) else pd.Series(dtype=float)
        rec = {"id": dist.get("id"), "nombre": dist.get("nombre"), "nivel": dist.get("nivel"), "probabilidad": dist.get("probabilidad"),
               "estaciones": len(ids), "horas_observadas": int(len(hours))}
        profile = [(int(p["hora"]), float(p["racha_max"])) for p in (dist.get("perfil") or []) if p.get("racha_max") is not None]
        if profile:
            hour, value = max(profile, key=lambda t: t[1])
            rec["racha_prevista_max"], rec["hora_prevista"] = round(value), hour
        if len(hours):
            rec["racha_observada_max"], rec["hora_observada"] = round(float(hours.max())), int(hours.idxmax())
        window = dist.get("franja") or {}
        if "racha_prevista_max" in rec and "racha_observada_max" in rec:
            rec["error_kmh"] = rec["racha_observada_max"] - rec["racha_prevista_max"]
            if window.get("inicio") is not None and window.get("fin") is not None:
                rec["pico_en_franja"] = bool(int(window["inicio"]) <= rec["hora_observada"] <= int(window["fin"]))
        out.append(rec)
    return out


def verification_summary(days: dict, minimum_hours: int) -> list:
    """Per distribuidora, over the complete days: bias and absolute error of the peak and the share of peaks inside the window."""
    rows = []
    for records in days.values():
        for r in records:
            if r.get("horas_observadas", 0) >= minimum_hours and r.get("error_kmh") is not None:
                rows.append(r)
    if not rows:
        return []
    table = pd.DataFrame(rows)
    out = []
    for dist, g in table.groupby("id", sort=False):
        block = {"id": dist, "nombre": g["nombre"].iloc[0], "dias": int(len(g)), "sesgo_kmh": round(float(g["error_kmh"].mean()), 1),
                 "error_abs_kmh": round(float(g["error_kmh"].abs().mean()), 1),
                 "dias_prevision_mayor": round(float((g["error_kmh"] < 0).mean()), 2)}
        if "pico_en_franja" in g.columns and g["pico_en_franja"].notna().any():
            block["pico_en_franja"] = round(float(g["pico_en_franja"].dropna().astype(bool).mean()), 2)
        days_text = "el último día completo" if block["dias"] == 1 else f"los últimos {block['dias']} días completos"
        block["texto"] = (f"en {days_text}, la previsión de la víspera dio de media {abs(block['sesgo_kmh']):g} km/h "
                          f"{'más' if block['sesgo_kmh'] < 0 else 'menos'} que las estaciones (error medio {block['error_abs_kmh']:g} km/h)"
                          + (f"; el pico observado cayó en la franja prevista el {100 * block['pico_en_franja']:.0f} % de los días" if "pico_en_franja" in block else ""))
        out.append(block)
    return out


def verify(fs, cfg: dict, frame: pd.DataFrame, now: datetime) -> dict:
    """
    verificacion.json: yesterday and today (local) of every riesgo_<day>.json
    found, merged with the days already kept, and the summary. Yesterday is
    complete only with the last file landed the UTC day before, so both are
    read; a missing archive or Landing day just leaves that day out.
    """
    folder = cfg["dashboard_dir"]
    zone = ZoneInfo(cfg["zona_horaria"])
    today = now.astimezone(zone).date()
    yesterday = today - timedelta(days=1)
    path = os.path.join(folder or "", "verificacion.json")
    kept = {}
    if folder and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as file:
                kept = {k: v for k, v in (json.load(file).get("dias") or {}).items()}
        except (OSError, ValueError):
            kept = {}
    both = pd.concat([frame, last_landed(fs, cfg, now - timedelta(days=1))], ignore_index=True) if len(frame) else last_landed(fs, cfg, now - timedelta(days=1))
    if len(both):
        both = both.drop_duplicates(["idema", "ts_utc"])
    today_records = []
    for day in (yesterday, today):
        risk = archived_forecast(folder, day)
        if not risk:
            continue
        records = verify_day(both, risk, cfg, day)
        if day == today:
            today_records = records
        else:
            kept[day.isoformat()] = records
    keep = int(cfg["verificacion_dias"])
    kept = dict(sorted(kept.items())[-keep:])
    payload = {"generado": now.strftime("%Y-%m-%dT%H:%MZ"), "fuente": "previsión de la víspera (riesgo_<día>.json) frente a la racha máxima de las estaciones de AEMET a menos de "
               f"{float(cfg['radio_km']):g} km de la red", "hoy": {"fecha": today.isoformat(), "distribuidoras": today_records},
               "ayer": {"fecha": yesterday.isoformat(), "distribuidoras": kept.get(yesterday.isoformat(), [])},
               "dias": kept, "resumen": verification_summary(kept, int(cfg["horas_dia_completo"]))}
    if folder and os.path.isdir(folder):
        with open(path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=1)
        logger.info("verificacion.json written in %s (%s days kept)", path, len(kept))
    return payload


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
    try:
        checked = verify(fs, cfg, frame, now)
        for block in checked.get("resumen", []):
            logger.info("Verificación %s: %s", block["nombre"], block["texto"])
    except Exception as error:  # noqa: BLE001
        logger.warning("The forecast could not be verified: %s", str(error)[:200])
    return 0


if __name__ == "__main__":
    sys.exit(main())
