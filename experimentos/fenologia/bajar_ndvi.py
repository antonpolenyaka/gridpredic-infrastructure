"""
El verdor de cada celda meteorologica, de los compuestos de 16 dias de NDVI
de MODIS (MOD13A1 de Terra y MYD13A1 de Aqua, coleccion 6.1, 500 m), que el
Planetary Computer de Microsoft sirve como COG sin cuenta: el catalogo STAC y
la firma de los enlaces (/api/sas/v1/sign) son anonimos.

Para cada compuesto (uno cada 16 dias por satelite, los dos desfasados 8 dias)
y cada celda de 0,1 grados de los municipios de data/reference_data/municipios.xlsx
deja la media del NDVI de los pixeles fiables (pixel_reliability 0 o 1, NDVI
dentro del rango valido) y cuantos pixeles eran. Las celdas de la raya de los
40 grados caen en dos mosaicos (h17v04 y h17v05) y se suman.

    python experimentos\\fenologia\\bajar_ndvi.py etl\\config\\01_bronze\\ndvi_celdas.csv 2017 2026

Necesita rasterio, pandas y openpyxl (el venv del arbolado). Se puede cortar y
volver a lanzar: los compuestos ya escritos en el CSV no se piden otra vez.
"""

import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds

CELL_DEGREES = 0.1
STAC = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
SIGN = "https://planetarycomputer.microsoft.com/api/sas/v1/sign?href="
COLLECTION = "modis-13A1-061"
NDVI_ASSET, QUALITY_ASSET = "500m_16_days_NDVI", "500m_16_days_pixel_reliability"
NDVI_SCALE = 10000.0
VALID = (-2000, 10000)
RELIABLE = (0, 1)  # good and marginal; 2 is snow or ice, 3 cloud, -1 no data
COLUMNS = ["celda_id", "plataforma", "fecha_inicio", "fecha_fin", "ndvi", "n_validos", "n_pixeles"]


def cell_of(lat: float, lon: float, size: float = CELL_DEGREES) -> tuple:
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
        rows.setdefault(cell, {"celda_id": cell, "lat": clat, "lon": clon})
    return pd.DataFrame(sorted(rows.values(), key=lambda r: r["celda_id"]))


def get_json(url: str, body: dict = None, retries: int = 5):
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"} if body else {})
            with urllib.request.urlopen(req, timeout=120) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            if attempt + 1 == retries:
                raise
            # The anonymous signing is rate limited: wait and try again.
            time.sleep((60 if error.code == 429 else 5) * (attempt + 1))
        except (urllib.error.URLError, TimeoutError):
            if attempt + 1 == retries:
                raise
            time.sleep(5 * (attempt + 1))


_SAS = {"query": None, "until": 0.0}


def sign(href: str) -> str:
    """
    The href with the SAS token of its container. The signing endpoint is
    anonymous but rate limited, so one token (valid about 45 minutes) is
    reused for every asset of the collection while it lasts.
    """
    now = time.time()
    if _SAS["query"] is None or now > _SAS["until"]:
        signed = get_json(SIGN + urllib.parse.quote(href, safe=""))
        _SAS["query"] = signed["href"].split("?", 1)[1]
        expiry = signed.get("msft:expiry")
        until = datetime.fromisoformat(expiry.replace("Z", "+00:00")).timestamp() if expiry else now + 30 * 60
        _SAS["until"] = min(until, now + 40 * 60) - 5 * 60
    return f"{href}?{_SAS['query']}"


def search(bbox: list, year: int) -> list:
    """The items of the collection over the box in the year, every page."""
    body = {"collections": [COLLECTION], "bbox": bbox, "limit": 250,
            "datetime": f"{year}-01-01T00:00:00Z/{year}-12-31T23:59:59Z"}
    items, url, payload = [], STAC, body
    while True:
        page = get_json(url, payload)
        items += page.get("features", [])
        nxt = [link for link in page.get("links", []) if link.get("rel") == "next"]
        if not nxt:
            return items
        link = nxt[0]
        url, payload = link["href"], link.get("body", None)
        if payload is None and link.get("method", "GET") == "POST":
            payload = body


def read_box(src, bounds: tuple):
    """(array, window) of the raster inside the WGS84 box; None when it does not touch it."""
    b = transform_bounds("EPSG:4326", src.crs, *bounds)
    left, bottom, right, top = src.bounds
    if b[2] <= left or b[0] >= right or b[3] <= bottom or b[1] >= top:
        return None, None
    clipped = (max(b[0], left), max(b[1], bottom), min(b[2], right), min(b[3], top))
    win = from_bounds(*clipped, transform=src.transform).round_offsets().round_lengths()
    if win.width < 1 or win.height < 1:
        return None, None
    return src.read(1, window=win), win


def cell_pixels(src, win, arr, cell) -> np.ndarray:
    """The values of arr (read with win) that fall in the cell (its box in the raster's CRS)."""
    bounds = (cell["lon"] - CELL_DEGREES / 2, cell["lat"] - CELL_DEGREES / 2,
              cell["lon"] + CELL_DEGREES / 2, cell["lat"] + CELL_DEGREES / 2)
    b = transform_bounds("EPSG:4326", src.crs, *bounds)
    cw = from_bounds(*b, transform=src.transform).round_offsets().round_lengths()
    r0, c0 = int(cw.row_off - win.row_off), int(cw.col_off - win.col_off)
    r1, c1 = r0 + int(cw.height), c0 + int(cw.width)
    r0, c0 = max(r0, 0), max(c0, 0)
    r1, c1 = min(r1, arr.shape[0]), min(c1, arr.shape[1])
    if r1 <= r0 or c1 <= c0:
        return np.empty(0, dtype=arr.dtype)
    return arr[r0:r1, c0:c1].ravel()


def composite_rows(item: dict, cells: pd.DataFrame, box: tuple) -> list:
    """One row per cell the item touches: the mean NDVI of its reliable pixels."""
    props = item["properties"]
    start = datetime.fromisoformat((props.get("start_datetime") or props["datetime"]).replace("Z", "+00:00"))
    end = datetime.fromisoformat((props.get("end_datetime") or props["datetime"]).replace("Z", "+00:00"))
    if end <= start:
        end = start + timedelta(days=15)
    platform = props.get("platform") or {"MOD": "terra", "MYD": "aqua"}.get(item["id"][:3], item["id"][:3])
    env = rasterio.Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
                       GDAL_HTTP_MAX_RETRY=4, GDAL_HTTP_RETRY_DELAY=5)
    rows = []
    with env, rasterio.open(sign(item["assets"][NDVI_ASSET]["href"])) as ndvi, \
            rasterio.open(sign(item["assets"][QUALITY_ASSET]["href"])) as quality:
        values, win = read_box(ndvi, box)
        if values is None:
            return rows
        flags, _ = read_box(quality, box)
        for _, cell in cells.iterrows():
            v = cell_pixels(ndvi, win, values, cell).astype(np.float64)
            q = cell_pixels(quality, win, flags, cell)
            if not v.size:
                continue
            good = (v >= VALID[0]) & (v <= VALID[1]) & np.isin(q, RELIABLE)
            rows.append({"celda_id": cell["celda_id"], "plataforma": platform, "fecha_inicio": start.date().isoformat(),
                         "fecha_fin": end.date().isoformat(), "ndvi": float(v[good].mean() / NDVI_SCALE) if good.any() else None,
                         "n_validos": int(good.sum()), "n_pixeles": int(v.size), "_item": item["id"]})
    return rows


def composite_key(item: dict) -> tuple:
    start = item["properties"].get("start_datetime") or item["properties"].get("datetime") or ""
    return item["id"].split(".")[2], item["id"][:3], start


def pending_composites(items: list, done: set) -> list:
    """
    The composites still to read. The catalogue keeps reprocessed copies of some composites (same tile and dates,
    later production time): the latest of the whole catalogue is chosen first, and a composite with any revision
    already in the CSV is not read again, so a second run never adds an older or a second revision of it.
    """
    latest = {}
    for item in sorted(items, key=lambda i: i["id"].split(".")[-1]):
        latest[composite_key(item)] = item
    saved = {composite_key(i) for i in items if i["id"] in done}
    return sorted((item for key, item in latest.items() if key not in saved),
                  key=lambda i: i["properties"].get("start_datetime") or i["properties"].get("datetime") or "")


def main(argv):
    out_csv = argv[0] if argv else "ndvi_celdas.csv"
    first, last = (int(argv[1]), int(argv[2])) if len(argv) > 2 else (2017, datetime.now(timezone.utc).year)
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    xlsx = argv[3] if len(argv) > 3 else os.path.join(root, "data", "reference_data", "municipios.xlsx")
    cells = cells_of_places(xlsx)
    box = (cells["lon"].min() - CELL_DEGREES, cells["lat"].min() - CELL_DEGREES,
           cells["lon"].max() + CELL_DEGREES, cells["lat"].max() + CELL_DEGREES)
    print(f"{len(cells)} celdas, caja {tuple(round(b, 2) for b in box)}, {first} - {last}", flush=True)

    # A composite counts as done only when all its rows are in the CSV: its id goes to <csv>.completos after the
    # rows are flushed. Rows of a composite cut halfway (a full disk, a killed process) are dropped when resuming,
    # and the composite is read again (tercera ronda de auditoria, A43). A CSV of before this list counts as done.
    marks = out_csv + ".completos"
    done = set()
    if os.path.exists(out_csv):
        old = pd.read_csv(out_csv)
        items_in_csv = set(old["_item"].astype(str)) if "_item" in old.columns else set()
        if os.path.exists(marks):
            with open(marks, encoding="utf-8") as file:
                done = {line.strip() for line in file if line.strip()}
            cut = old["_item"].astype(str).isin(items_in_csv - done) if "_item" in old.columns else None
            if cut is not None and cut.any():
                print(f"{int(cut.sum())} filas de compuestos a medias en {out_csv}: las quito y se vuelven a leer", flush=True)
                tmp = out_csv + ".tmp"
                old[~cut].to_csv(tmp, index=False)
                os.replace(tmp, out_csv)
        else:
            done = items_in_csv
            with open(marks, "w", encoding="utf-8") as file:
                file.writelines(f"{i}\n" for i in sorted(done))
        print(f"{len(done)} compuestos ya en {out_csv}", flush=True)
    os.makedirs(os.path.dirname(os.path.abspath(out_csv)) or ".", exist_ok=True)
    new_file = not os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as file, open(marks, "a", encoding="utf-8") as finished:
        writer = csv.DictWriter(file, fieldnames=COLUMNS + ["_item"])
        if new_file:
            writer.writeheader()
        for year in range(first, last + 1):
            items = pending_composites(search(list(box), year), done)
            print(f"{year}: {len(items)} compuestos por leer", flush=True)
            for k, item in enumerate(items):
                started = time.time()
                try:
                    rows = composite_rows(item, cells, box)
                except Exception as error:  # noqa: BLE001
                    print(f"  {item['id']}: ERROR {str(error)[:160]}", flush=True)
                    continue
                for row in rows:
                    writer.writerow({key: ("" if value is None else value) for key, value in row.items()})
                file.flush()
                finished.write(item["id"] + "\n")
                finished.flush()
                done.add(item["id"])
                if k % 10 == 0:
                    print(f"  {item['id']}: {len(rows)} celdas en {time.time() - started:.1f} s", flush=True)
                time.sleep(0.5)
    print("escrito", out_csv, flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
