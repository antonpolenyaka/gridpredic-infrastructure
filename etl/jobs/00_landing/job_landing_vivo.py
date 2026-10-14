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
- ensemble: the ensemble forecast (ICON-EU EPS, 40 perturbed runs, from the
  Open-Meteo Ensemble API, free, no key) of the gusts of a few points of every
  network (ensemble.puntos), hourly, for the day to predict. The deterministic
  run says one gust; the members say how sure it is. Besides the raw answers,
  it writes ensemble.json for the dashboard (dashboard_dir): per
  distribuidora and hour the p10, p50 and p90 of the gust over its points,
  the peak of the day and the share of members above every threshold of
  ensemble.umbrales_kmh, with a sentence for the page.

Everything goes to <landing>/<day to predict, YYYYMMDD>/prevision/<cell>.json,
avisos/<area>.tar (avisos/<area>.vacio.json when AEMET has none) and
ensemble/<punto>.json, so the scoring job reads exactly what was known that
evening. A source that is already there for the day is asked again (the
forecast changes during the day): the last run of the evening wins.

The day to predict is tomorrow in the time zone of the configuration unless
--fecha gives it.

    docker compose exec airflow python /app/jobs/00_landing/job_landing_vivo.py --fuente prevision
    docker compose exec airflow python /app/jobs/00_landing/job_landing_vivo.py --fuente avisos
    docker compose exec airflow python /app/jobs/00_landing/job_landing_vivo.py --fuente ensemble
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

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
SOURCES = ("prevision", "avisos", "ensemble")
ENSEMBLE_DEFAULTS = {
    "api": "https://ensemble-api.open-meteo.com/v1/ensemble",
    "modelos": ["icon_eu", "icon_seamless", "gfs025"],  # tried in this order until one answers
    "variable": "wind_gusts_10m",
    "dias_futuros": 3,
    "umbrales_kmh": [50, 70, 90],
    "puntos": {},
    "pausa_s": 0.5,
    "reintentos": 3,
    "espera_reintento_s": 20,
    "timeout_s": 120,
}


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


def ensemble_settings(cfg: dict) -> dict:
    return {**ENSEMBLE_DEFAULTS, **(cfg.get("ensemble") or {})}


def ensemble_url(lat: float, lon: float, ecfg: dict, model: str) -> str:
    query = {
        "latitude": f"{lat:.4f}", "longitude": f"{lon:.4f}", "hourly": ecfg["variable"], "models": model,
        "forecast_days": int(ecfg["dias_futuros"]), "timezone": "GMT", "timeformat": "unixtime", "wind_speed_unit": "kmh",
    }
    return f"{ecfg['api']}?{urllib.parse.urlencode(query, safe=',')}"


def members_of(payload: dict, variable: str) -> np.ndarray:
    """The hourly values of every member as a matrix (members x hours): the control run and the perturbed ones."""
    hourly = payload.get("hourly") or {}
    pattern = re.compile(rf"^{re.escape(variable)}(_member\d+)?$")
    rows = [np.asarray(hourly[k], dtype=float) for k in sorted(hourly) if pattern.match(k)]
    return np.vstack(rows) if rows else np.empty((0, 0))


def ask_ensemble(point: dict, ecfg: dict) -> tuple:
    """(payload, model) of the first model that answers with members for the point."""
    last = None
    for model in ecfg["modelos"]:
        try:
            raw = fetch(ensemble_url(float(point["lat"]), float(point["lon"]), ecfg, model), retries=int(ecfg["reintentos"]),
                        wait_s=float(ecfg["espera_reintento_s"]), timeout=float(ecfg["timeout_s"]), label=f"Open-Meteo ensemble {point.get('nombre')}")
            payload = json.loads(raw.decode("utf-8"))
            members = members_of(payload, ecfg["variable"])
            if members.shape[0] < 2 or not payload.get("hourly", {}).get("time"):
                raise RuntimeError(f"no members in the answer of {model}: {str(payload)[:160]}")
            return payload, model
        except Exception as error:  # noqa: BLE001
            last = error
            logger.warning("Ensemble %s of %s: %s", model, point.get("nombre"), str(error)[:160])
    raise RuntimeError(f"no ensemble model answered: {last}")


def land_ensemble(fs, cfg: dict, folder: str, day: date) -> tuple:
    """(points saved, points failed, ensemble.json payload): the members of every point in <folder>/ensemble/<punto>.json."""
    ecfg = ensemble_settings(cfg)
    zone = ZoneInfo(cfg.get("zona_horaria", "Europe/Madrid"))
    names = {str(k): str(v) for k, v in (cfg.get("nombres") or {}).items()}
    out = {"generado": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), "fecha": day.isoformat(), "fuente": "Open-Meteo Ensemble API",
           "variable": ecfg["variable"], "umbrales_kmh": [float(u) for u in ecfg["umbrales_kmh"]], "distribuidoras": []}
    done = failed = 0
    for dist, points in (ecfg.get("puntos") or {}).items():
        stacks = []
        for point in points:
            try:
                payload, model = ask_ensemble(point, ecfg)
            except Exception as error:  # noqa: BLE001
                failed += 1
                logger.error("Ensemble of %s (%s) not downloaded: %s", point.get("nombre"), dist, str(error)[:200])
                continue
            payload["_peticion"] = {"distribuidora_id": dist, "punto": point.get("nombre"), "modelo": model,
                                    "pedido_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            key = re.sub(r"[^A-Za-z0-9_-]+", "_", f"{dist}_{point.get('nombre', '')}").strip("_")
            with fs.open(f"{folder}/ensemble/{key}.json", "w") as file:
                json.dump(payload, file, ensure_ascii=False)
            done += 1
            times = np.asarray(payload["hourly"]["time"], dtype="int64")
            local = [datetime.fromtimestamp(int(t), tz=timezone.utc).astimezone(zone) for t in times]
            mask = np.asarray([t.date() == day for t in local])
            members = members_of(payload, ecfg["variable"])[:, mask]
            if members.shape[1]:
                stacks.append((members, [t.hour for t, m in zip(local, mask) if m], point.get("nombre"), model))
            time.sleep(float(ecfg["pausa_s"]))
        block = {"id": int(dist) if str(dist).isdigit() else dist, "nombre": names.get(str(dist), str(dist)),
                 "puntos": [s[2] for s in stacks], "modelo": stacks[0][3] if stacks else None, "miembros": 0, "horas": []}
        if stacks:
            hours = stacks[0][1]
            width = min(s[0].shape[1] for s in stacks)
            depth = min(s[0].shape[0] for s in stacks)
            # Per member and hour, the worst of the points: the gust over the network, like racha_max of the profile.
            network = np.max(np.stack([s[0][:depth, :width] for s in stacks]), axis=0)
            network = np.where(np.isnan(network), np.nan, network)
            p10, p50, p90 = np.nanpercentile(network, [10, 50, 90], axis=0)
            peaks = np.nanmax(network, axis=1)
            block["miembros"] = int(depth)
            block["horas"] = [{"hora": int(hours[j]), "p10": round(float(p10[j])), "p50": round(float(p50[j])), "p90": round(float(p90[j]))}
                              for j in range(width) if not np.isnan(p50[j])]
            lo, mid, hi = np.nanpercentile(peaks, [10, 50, 90])
            block["pico"] = {"p10": round(float(lo)), "p50": round(float(mid)), "p90": round(float(hi))}
            block["supera"] = {f"{float(u):g}": round(float(np.mean(peaks >= float(u))), 2) for u in ecfg["umbrales_kmh"]}
            above = [u for u in ecfg["umbrales_kmh"] if block["supera"][f"{float(u):g}"] >= 0.5]
            spread = "escenarios muy de acuerdo" if hi - lo <= 15 else ("escenarios bastante de acuerdo" if hi - lo <= 30 else "escenarios muy repartidos")
            block["texto"] = (f"{depth} escenarios de {block['modelo']}: el 80 % central da el pico de racha entre {round(float(lo))} y {round(float(hi))} km/h "
                              f"(mediana {round(float(mid))}); " + ", ".join(f"el {100 * block['supera'][f'{float(u):g}']:.0f} % supera {float(u):g} km/h" for u in ecfg["umbrales_kmh"])
                              + f"; {spread}" + (f"; más de la mitad supera {float(above[-1]):g} km/h" if above else ""))
        else:
            block["texto"] = "sin previsión por conjuntos para el día"
        out["distribuidoras"].append(block)
    logger.info("Ensemble: %s points saved, %s failed", done, failed)
    return done, failed, out


def write_ensemble(payload: dict, folder: str) -> bool:
    if not folder or not os.path.isdir(folder):
        logger.warning("Dashboard folder %s not found: ensemble.json not written", folder)
        return False
    path = os.path.join(folder, "ensemble.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=1)
    logger.info("ensemble.json written in %s", path)
    return True


def main(argv=None, fs=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fuente", required=True, choices=SOURCES)
    parser.add_argument("--fecha", default="", help="Day to predict, YYYY-MM-DD (tomorrow by default)")
    parser.add_argument("--config", default=None)
    parser.add_argument("--dashboard-dir", default=None)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)["riesgo_manana"]
    fs = fs or filesystem()
    day = target_day(args.fecha, cfg.get("zona_horaria", "Europe/Madrid"))
    folder = day_folder(cfg["landing"], day)
    logger.info("Landing of %s for %s in %s", args.fuente, day, folder)

    if args.fuente == "ensemble":
        done, failed, payload = land_ensemble(fs, cfg, folder, day)
        if done:
            write_ensemble(payload, args.dashboard_dir or cfg.get("dashboard_dir", "/app/dashboard/data"))
            for block in payload["distribuidoras"]:
                logger.info("%s: %s", block["nombre"], block["texto"])
        return 0 if done and not failed else (2 if not done else 1)

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
