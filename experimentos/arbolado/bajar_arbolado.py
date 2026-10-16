"""
El arbolado de cada celda meteorologica, de dos mapas publicos sin registro:

- ESA WorldCover 2021 v200 (10 m, CC BY 4.0): la cubierta del suelo. Da la
  parte de la celda que es arbolado (clase 10), matorral (20), pastizal (30),
  cultivo (40) y suelo construido (50).
- Altura del dosel de Meta y WRI (Tolan y otros, 2024; 1 m, CC BY 4.0): la
  altura de los arboles pixel a pixel. Da la altura media y el percentil 90
  de los pixeles con arbol y la parte de la celda con dosel de 5, 8 y 12 m o
  mas (los arboles que llegan a un conductor de media tension).

Las celdas son las de la meteorologia (0,1 grados, las de los municipios de
data/reference_data/municipios.xlsx, con la misma regla que la Landing). Los
mosaicos se bajan con curl a una carpeta de trabajo (unos 17 GB de altura del
dosel, 24 mosaicos de 700 MB, y 300 MB de WorldCover) y se leen por ventanas.

    python experimentos\\arbolado\\bajar_arbolado.py D:\\DockerData\\arbolado etl\\config\\01_bronze\\arbolado_celdas.csv

Necesita rasterio, shapely, numpy, pandas y openpyxl (un venv aparte; no van
en la imagen de Spark). Vuelve a usar lo que ya este bajado.
"""

import csv
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds

CELL_DEGREES = 0.1
WORLDCOVER = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/ESA_WorldCover_10m_2021_v200_{tile}_Map.tif"
CHM_BASE = "https://dataforgood-fb-data.s3.amazonaws.com/forests/v1/alsgedi_global_v6_float/"
CLASSES = {"arbol": 10, "matorral": 20, "pastizal": 30, "cultivo": 40, "construido": 50}
HEIGHTS = (5, 8, 12)


def cell_of(lat: float, lon: float, size: float = CELL_DEGREES) -> tuple:
    """La misma regla que job_landing_meteo_openmeteo: el centro de la celda y su id."""
    clat = round(round(lat / size) * size, 4)
    clon = round(round(lon / size) * size, 4)
    return f"{clat:.2f}_{clon:.2f}", clat, clon


def cells_of_places(xlsx: str) -> pd.DataFrame:
    places = pd.read_excel(xlsx, sheet_name="Municipios")
    rows = {}
    for _, row in places.iterrows():
        if pd.isna(row.get("Latitud")) or pd.isna(row.get("Longitud")):
            continue
        cell, clat, clon = cell_of(float(row["Latitud"]), float(row["Longitud"]))
        rows.setdefault(cell, {"celda_id": cell, "lat": clat, "lon": clon, "municipios": 0})
        rows[cell]["municipios"] += 1
    return pd.DataFrame(sorted(rows.values(), key=lambda r: r["celda_id"]))


def worldcover_tiles(cells: pd.DataFrame) -> list:
    """Los mosaicos de 3 grados (esquina suroeste) que cubren las celdas."""
    tiles = set()
    for _, c in cells.iterrows():
        for lat in (c["lat"] - CELL_DEGREES / 2, c["lat"] + CELL_DEGREES / 2):
            for lon in (c["lon"] - CELL_DEGREES / 2, c["lon"] + CELL_DEGREES / 2):
                tlat, tlon = int(np.floor(lat / 3) * 3), int(np.floor(lon / 3) * 3)
                tiles.add(f"{'N' if tlat >= 0 else 'S'}{abs(tlat):02d}{'E' if tlon >= 0 else 'W'}{abs(tlon):03d}")
    return sorted(tiles)


def chm_tiles(index_path: str, cells: pd.DataFrame) -> list:
    """Los mosaicos (quadkeys) del indice de Meta que tocan alguna celda."""
    with open(index_path, encoding="utf-8") as file:
        features = json.load(file)["features"]
    boxes = [(c["lon"] - CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2, c["lon"] + CELL_DEGREES / 2,
              c["lat"] + CELL_DEGREES / 2) for _, c in cells.iterrows()]
    out = []
    for feature in features:
        ring = feature["geometry"]["coordinates"][0]
        lons, lats = [p[0] for p in ring], [p[1] for p in ring]
        tile = (min(lons), min(lats), max(lons), max(lats))
        if any(not (b[2] <= tile[0] or b[0] >= tile[2] or b[3] <= tile[1] or b[1] >= tile[3]) for b in boxes):
            out.append((feature["properties"]["tile"], tile))
    return out


def download(url: str, path: str, expected: int = None) -> str:
    """Baja con curl si no esta (o esta a medias); devuelve el fichero."""
    if os.path.exists(path) and (expected is None or os.path.getsize(path) == expected):
        return path
    part = path + ".part"
    for attempt in range(3):
        code = subprocess.call(["curl", "-sS", "-L", "--retry", "3", "-C", "-", "-o", part, url])
        if code == 0:
            os.replace(part, path)
            return path
        time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"No se pudo bajar {url}")


def size_of(url: str) -> int:
    out = subprocess.run(["curl", "-sS", "-I", "-L", url], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.lower().startswith("content-length:"):
            return int(line.split(":", 1)[1].strip())
    return None


def read_window(src, bounds: tuple, resampling=Resampling.nearest, factor: int = 1):
    """La ventana del raster que cubre la caja (lon_min, lat_min, lon_max, lat_max) en WGS84; None si no la toca."""
    try:
        b = transform_bounds("EPSG:4326", src.crs, *bounds) if src.crs and src.crs.to_epsg() != 4326 else bounds
    except Exception:  # noqa: BLE001
        return None
    left, bottom, right, top = src.bounds
    if b[2] <= left or b[0] >= right or b[3] <= bottom or b[1] >= top:
        return None
    clipped = (max(b[0], left), max(b[1], bottom), min(b[2], right), min(b[3], top))
    win = from_bounds(*clipped, transform=src.transform).round_offsets().round_lengths()
    if win.width < 1 or win.height < 1:
        return None
    shape = (max(1, int(win.height // factor)), max(1, int(win.width // factor)))
    return src.read(1, window=win, out_shape=shape, resampling=resampling, boundless=False)


def cover_stats(paths: list, bounds: tuple) -> dict:
    parts = []
    for path in paths:
        with rasterio.open(path) as src:
            arr = read_window(src, bounds)
            if arr is not None and arr.size:
                parts.append(arr.ravel())
    if not parts:
        return {}
    values = np.concatenate(parts)
    values = values[values != 0]  # en WorldCover, 0 es sin dato
    if not values.size:
        return {}
    out = {f"pct_{name}": float((values == code).mean()) for name, code in CLASSES.items()}
    out["pixeles_cubierta"] = int(values.size)
    return out


def height_stats(paths: list, bounds: tuple, factor: int = 4) -> dict:
    """La altura del dosel leida a 4 m (la media de cada bloque de 4 x 4 pixeles de 1 m)."""
    parts = []
    for path in paths:
        with rasterio.open(path) as src:
            arr = read_window(src, bounds, Resampling.average, factor)
            if arr is not None and arr.size:
                parts.append(arr.ravel().astype(np.float32))
    if not parts:
        return {}
    values = np.concatenate(parts)
    values = values[values < 250]  # 255 sería sin dato
    if not values.size:
        return {}
    trees = values[values >= 1.0]
    out = {"pct_dosel": float((values >= 1.0).mean()),
           "dosel_media_m": float(trees.mean()) if trees.size else 0.0,
           "dosel_p90_m": float(np.percentile(trees, 90)) if trees.size else 0.0,
           "pixeles_dosel": int(values.size)}
    for h in HEIGHTS:
        out[f"pct_dosel_{h}m"] = float((values >= h).mean())
    return out


def main(argv):
    work = argv[0] if argv else "arbolado"
    out_csv = argv[1] if len(argv) > 1 else os.path.join(work, "arbolado_celdas.csv")
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    xlsx = os.path.join(root, "data", "reference_data", "municipios.xlsx")
    os.makedirs(work, exist_ok=True)
    cells = cells_of_places(xlsx)
    print(f"{len(cells)} celdas de {xlsx}")

    wc_paths = []
    for tile in worldcover_tiles(cells):
        url = WORLDCOVER.format(tile=tile)
        path = os.path.join(work, os.path.basename(url))
        print("WorldCover", tile, flush=True)
        wc_paths.append(download(url, path, size_of(url)))

    index = download(CHM_BASE + "tiles.geojson", os.path.join(work, "tiles.geojson"))
    tiles = chm_tiles(index, cells)
    print(f"{len(tiles)} mosaicos de altura del dosel", flush=True)
    jobs = []
    for tile, _ in tiles:
        url = f"{CHM_BASE}chm/{tile}.tif"
        jobs.append((url, os.path.join(work, "chm", f"{tile}.tif")))
    os.makedirs(os.path.join(work, "chm"), exist_ok=True)

    def fetch(job):
        url, path = job
        started = time.time()
        download(url, path, size_of(url))
        print(f"  {os.path.basename(path)} listo en {time.time() - started:.0f} s", flush=True)
        return path

    with ThreadPoolExecutor(max_workers=3) as pool:
        chm_paths = list(pool.map(fetch, jobs))

    rows = []
    for _, c in cells.iterrows():
        bounds = (c["lon"] - CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2, c["lon"] + CELL_DEGREES / 2,
                  c["lat"] + CELL_DEGREES / 2)
        row = {"celda_id": c["celda_id"], "lat": c["lat"], "lon": c["lon"], "municipios": int(c["municipios"])}
        row.update(cover_stats(wc_paths, bounds))
        row.update(height_stats(chm_paths, bounds))
        rows.append(row)
        print(f"  {c['celda_id']}: arbol {row.get('pct_arbol', float('nan')):.3f}, dosel >= 8 m "
              f"{row.get('pct_dosel_8m', float('nan')):.3f}, media {row.get('dosel_media_m', float('nan')):.1f} m", flush=True)

    columns = ["celda_id", "lat", "lon", "municipios"] + [f"pct_{n}" for n in CLASSES] + ["pixeles_cubierta",
               "pct_dosel", "dosel_media_m", "dosel_p90_m"] + [f"pct_dosel_{h}m" for h in HEIGHTS] + ["pixeles_dosel"]
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (round(v, 4) if isinstance(v, float) else v) for k, v in row.items() if k in columns})
    with open(os.path.splitext(out_csv)[0] + ".json", "w", encoding="utf-8") as file:
        json.dump({"generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "worldcover": [os.path.basename(p) for p in wc_paths], "altura_dosel": [t for t, _ in tiles],
                   "celdas": len(rows), "fuentes": {"worldcover": "ESA WorldCover 2021 v200, CC BY 4.0",
                                                    "altura_dosel": "Meta y WRI, Tolan y otros 2024, CC BY 4.0"}},
                  file, ensure_ascii=False, indent=2)
    print("escrito", out_csv)


if __name__ == "__main__":
    main(sys.argv[1:])
