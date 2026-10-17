"""
The forecast of ICON-EU on its regular grid against the shadow forecasts that
the live Landing keeps every evening since 17.10.2026 (job_landing_vivo,
riesgo_manana.prevision_sombra): ICON-EU on its native grid, which is what
models=icon_eu will serve after 30.11.2026, and IFS HRES 9 km. Notebook 3u of
docs/investigacion-continua.md.

For every evening between --desde and --hasta (the day predicted, the folder
of the Landing) and every cell: the strongest gust and the rain of that day
in the wall clock of Madrid from each source, and the point of the grid the
answer comes from. By network (the cells of its municipalities): the gust of
the worst cell and the mean of the cells, which is what the day model reads,
and the difference of each shadow source with the regular grid: bias, mean
absolute difference, the largest one, the correlation and the days with more
than umbral_kmh of difference. Nothing here scores: it says whether the
gusts of the model of production will jump when the grid changes.

    docker compose exec spark-master python3 /app/jobs/00_landing/job_comparar_sombra.py --desde 2026-10-18
    docker compose cp spark-master:/tmp/ml_resultados/sombra_<fecha> _runlogs/ml_resultados
"""

import argparse
import json
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_meteo_openmeteo import filesystem  # noqa: E402
from job_landing_meteo_openmeteo import load_config as load_meteo_config  # noqa: E402
from job_landing_vivo import load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sombra")

REGULAR = "icon_eu"


def day_values(payload: dict, day: date, zone: str) -> dict:
    """The strongest gust and the rain of day (wall clock of zone) of one answer, and its point of the grid."""
    hourly = payload.get("hourly") or {}
    tz = ZoneInfo(zone)
    times = hourly.get("time") or []
    keep = [i for i, t in enumerate(times) if datetime.fromtimestamp(int(t), tz=timezone.utc).astimezone(tz).date() == day]
    gusts = [hourly.get("wind_gusts_10m", [None] * len(times))[i] for i in keep]
    rain = [hourly.get("precipitation", [None] * len(times))[i] for i in keep]
    gusts = [g for g in gusts if g is not None]
    rain = [r for r in rain if r is not None]
    return {"horas": len(keep), "racha_max": max(gusts) if gusts else np.nan,
            "lluvia": float(sum(rain)) if rain else np.nan, "lat": payload.get("latitude"),
            "lon": payload.get("longitude"), "elevacion": payload.get("elevation")}


def distance_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return np.nan
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def cell_rows(fs, landing: str, days: list, keys: list, zone: str) -> pd.DataFrame:
    """One row per day, cell and source (icon_eu and every shadow key) with the values of the day."""
    rows = []
    for day in days:
        folder = f"{landing.rstrip('/')}/{day:%Y%m%d}"
        sources = {REGULAR: f"{folder}/prevision"} | {key: f"{folder}/prevision_sombra/{key}" for key in keys}
        for source, path in sources.items():
            if not fs.exists(path):
                continue
            for file_path in sorted(fs.glob(f"{path}/*.json")):
                with fs.open(file_path, "r") as file:
                    payload = json.load(file)
                cell = (payload.get("_peticion") or {}).get("celda_id") or os.path.basename(file_path)[:-5]
                rows.append({"fecha": day, "celda_id": cell, "fuente": source, **day_values(payload, day, zone)})
    return pd.DataFrame(rows)


def network_rows(cells: pd.DataFrame, places: pd.DataFrame) -> pd.DataFrame:
    """By day, network and source: the gust of the worst cell, the mean of the cells and the rain of the wettest."""
    joined = cells.merge(places[["celda_id", "red"]].drop_duplicates(), on="celda_id", how="inner")
    return joined.groupby(["fecha", "red", "fuente"], as_index=False).agg(
        celdas=("celda_id", "nunique"), racha_peor_celda=("racha_max", "max"), racha_media=("racha_max", "mean"),
        lluvia_max=("lluvia", "max"))


def compare(networks: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Every shadow source against the regular grid of ICON-EU, by network."""
    regular = networks[networks["fuente"] == REGULAR].set_index(["fecha", "red"])
    rows = []
    for (source, network), g in networks[networks["fuente"] != REGULAR].groupby(["fuente", "red"]):
        g = g.set_index(["fecha", "red"]).join(regular, rsuffix="_regular", how="inner")
        if g.empty:
            continue
        diff = g["racha_peor_celda"] - g["racha_peor_celda_regular"]
        mean_diff = g["racha_media"] - g["racha_media_regular"]
        both = g[["racha_peor_celda", "racha_peor_celda_regular"]].dropna()
        rows.append({"fuente": source, "red": network, "dias": len(g), "sesgo_racha_peor_kmh": float(diff.mean()),
                     "dif_media_abs_peor_kmh": float(diff.abs().mean()), "dif_max_abs_peor_kmh": float(diff.abs().max()),
                     "sesgo_racha_media_kmh": float(mean_diff.mean()),
                     "correlacion_peor": float(both.corr().iloc[0, 1]) if len(both) > 2 else np.nan,
                     f"dias_dif_mas_{threshold:g}_kmh": int((diff.abs() > threshold).sum()),
                     "sesgo_lluvia_max_mm": float((g["lluvia_max"] - g["lluvia_max_regular"]).mean())})
    return pd.DataFrame(rows)


def grid_shift(cells: pd.DataFrame) -> pd.DataFrame:
    """How far the point of each shadow source is from the point of the regular grid, per cell."""
    regular = cells[cells["fuente"] == REGULAR].drop_duplicates("celda_id").set_index("celda_id")
    rows = []
    for (source, cell), g in cells[cells["fuente"] != REGULAR].groupby(["fuente", "celda_id"]):
        if cell not in regular.index:
            continue
        r, s = regular.loc[cell], g.iloc[0]
        rows.append({"fuente": source, "celda_id": cell, "km_al_punto_regular": distance_km(r["lat"], r["lon"], s["lat"], s["lon"]),
                     "dif_elevacion_m": (s["elevacion"] - r["elevacion"]) if pd.notna(s["elevacion"]) and pd.notna(r["elevacion"]) else np.nan})
    return pd.DataFrame(rows)


def places_of(fs, meteo: dict, cfg: dict) -> pd.DataFrame:
    """The cells of every network: municipios_celdas.json of the reanalysis and the names of riesgo_manana."""
    with fs.open(f"{meteo['landing'].rstrip('/')}/municipios_celdas.json", "r") as file:
        places = pd.DataFrame(json.load(file))
    mapping = {str(k): str(v) for k, v in ((cfg.get("ensemble") or {}).get("referencia_distribuidoras") or {}).items()}
    names = {str(k): str(v) for k, v in (cfg.get("nombres") or {}).items()}
    ids = places["distribuidora_ref"].astype(str).str.strip().map(lambda r: mapping.get(r, r))
    places["red"] = ids.map(lambda d: names.get(d, f"Distribuidora {d}"))
    return places


def markdown(table: pd.DataFrame) -> str:
    if table.empty:
        return "_Sin datos._"
    lines = ["| " + " | ".join(table.columns) + " |", "|" + "|".join("---" for _ in table.columns) + "|"]
    for _, r in table.iterrows():
        lines.append("| " + " | ".join(f"{v:.2f}".replace(".", ",") if isinstance(v, float) else str(v) for v in r) + " |")
    return "\n".join(lines)


def main(argv=None, fs=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde", required=True, help="First day predicted, YYYY-MM-DD")
    parser.add_argument("--hasta", default="", help="Last day predicted (tomorrow by default)")
    parser.add_argument("--config", default=None)
    parser.add_argument("--salida", default="")
    parser.add_argument("--umbral-kmh", type=float, default=10.0)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    cfg = config["riesgo_manana"]
    zone = cfg.get("zona_horaria", "Europe/Madrid")
    first = date.fromisoformat(args.desde)
    last = date.fromisoformat(args.hasta) if args.hasta else datetime.now(ZoneInfo(zone)).date() + timedelta(days=1)
    days = [first + timedelta(days=k) for k in range((last - first).days + 1)]
    keys = [s["clave"] for s in cfg.get("prevision_sombra") or []]
    fs = fs or filesystem()

    cells = cell_rows(fs, cfg["landing"], days, keys, zone)
    if cells.empty or not (cells["fuente"] != REGULAR).any():
        logger.error("No shadow forecasts between %s and %s in %s", first, last, cfg["landing"])
        return 1
    networks = network_rows(cells, places_of(fs, load_meteo_config()["openmeteo"], cfg))
    summary = compare(networks, args.umbral_kmh)
    shift = grid_shift(cells)

    out = args.salida or os.path.join(config.get("exportar_dir", "/tmp/ml_resultados"),
                                      f"sombra_{datetime.now():%Y%m%d_%H%M}")
    os.makedirs(out, exist_ok=True)
    cells.to_csv(os.path.join(out, "celdas.csv"), index=False)
    networks.to_csv(os.path.join(out, "redes.csv"), index=False)
    summary.to_csv(os.path.join(out, "resumen.csv"), index=False)
    shift.to_csv(os.path.join(out, "puntos.csv"), index=False)
    shifts = shift.groupby("fuente")["km_al_punto_regular"].agg(["median", "max"]).reset_index() if len(shift) else shift
    text = "\n\n".join([
        f"# Previsión en sombra frente a la rejilla regular de ICON-EU ({first:%d.%m.%Y} - {last:%d.%m.%Y})",
        f"{cells['fecha'].nunique()} tardes. Racha del día en la peor celda de cada red y media de sus celdas, con la "
        f"diferencia de cada fuente menos la rejilla regular (km/h); días con más de {args.umbral_kmh:g} km/h de "
        "diferencia en la peor celda.",
        markdown(summary),
        "## Distancia del punto de cada fuente al de la rejilla regular (km)",
        markdown(shifts),
    ]) + "\n"
    with open(os.path.join(out, "resumen.md"), "w", encoding="utf-8") as file:
        file.write(text)
    logger.info("Comparison of %s evenings written in %s\n%s", cells["fecha"].nunique(), out, text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
