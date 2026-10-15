"""
Pasa los recortes del HRL de Copernicus (bajar_hrl.py) a las celdas de 0,1
grados de las redes: la densidad media de copa (Tree Cover Density 2018,
0 - 100 %) y, de los pixeles con arbol, la parte caducifolia (Dominant Leaf
Type 2018: 1 frondosa, 2 conifera, 0 sin arbol, 255 sin dato).

    python celdas_hrl.py <tcd.tif> <dlt.tif> etl\\config\\01_bronze\\hrl_celdas.csv

Los rasteres pueden venir en EPSG:3035 o en EPSG:4326; se leen por ventana
de cada celda. Necesita rasterio, numpy, pandas y openpyxl.
"""

import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import rasterio

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bajar_arbolado import CELL_DEGREES, cells_of_places, read_window  # noqa: E402


def cover_density(src, bounds) -> dict:
    arr = read_window(src, bounds)
    if arr is None or not arr.size:
        return {}
    values = arr[(arr >= 0) & (arr <= 100)].astype(np.float64)
    if not values.size:
        return {}
    return {"hrl_copa_media_pct": float(values.mean()), "hrl_copa_30_pct": float((values >= 30).mean()),
            "hrl_pixeles_copa": int(values.size)}


def leaf_type(src, bounds) -> dict:
    arr = read_window(src, bounds)
    if arr is None or not arr.size:
        return {}
    trees = arr[(arr == 1) | (arr == 2)]
    valid = arr[arr != 255]
    if not valid.size:
        return {}
    return {"hrl_pct_frondosa": float((valid == 1).mean()), "hrl_pct_conifera": float((valid == 2).mean()),
            "hrl_caducifolio_de_arbol": float((trees == 1).mean()) if trees.size else 0.0,
            "hrl_pixeles_hoja": int(valid.size)}


def main(argv):
    tcd, dlt, out_csv = argv[0], argv[1], argv[2]
    root = os.path.dirname(os.path.dirname(HERE))
    xlsx = argv[3] if len(argv) > 3 else os.path.join(root, "data", "reference_data", "municipios.xlsx")
    cells = cells_of_places(xlsx)
    rows = []
    with rasterio.open(tcd) as t, rasterio.open(dlt) as d:
        for _, c in cells.iterrows():
            bounds = (c["lon"] - CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2, c["lon"] + CELL_DEGREES / 2,
                      c["lat"] + CELL_DEGREES / 2)
            row = {"celda_id": c["celda_id"]}
            row.update(cover_density(t, bounds))
            row.update(leaf_type(d, bounds))
            rows.append(row)
            print(row, flush=True)
    frame = pd.DataFrame(rows)
    frame.round(4).to_csv(out_csv, index=False)
    with open(os.path.splitext(out_csv)[0] + ".json", "w", encoding="utf-8") as handle:
        json.dump({"generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "fuentes": {"tcd": os.path.basename(tcd), "dlt": os.path.basename(dlt),
                               "licencia": "Copernicus Land Monitoring Service, acceso libre y abierto"},
                   "celdas": len(rows)}, handle, ensure_ascii=False, indent=2)
    print("escrito", out_csv)


if __name__ == "__main__":
    main(sys.argv[1:])
