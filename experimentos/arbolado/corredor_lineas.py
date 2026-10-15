"""
El arbolado en el corredor de las lineas electricas de OpenStreetMap (power=line y minor_line, las que ya estan en la
Landing de datos publicos por celda): por celda, los km de linea y, en una franja de 15 m a cada lado, la parte con
dosel de 5, 8 y 12 m o mas (altura del dosel de Meta y WRI, 1 m) y la altura media de los arboles.

    python corredor_lineas.py osm_lineas.json chm etl\\config\\01_bronze\\corredor_celdas.csv
"""
import json, os, sys, glob, math
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
from shapely.geometry import LineString, mapping, box
from shapely.ops import transform as shp_transform
HALF_WIDTH_M = 15.0
R = 6378137.0


def to3857(x, y, z=None):
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    return R * np.radians(x), R * np.log(np.tan(np.pi / 4 + np.radians(y) / 2))


def length_km(coords):
    total = 0.0
    for (x1, y1), (x2, y2) in zip(coords[:-1], coords[1:]):
        dx = (x2 - x1) * 111.32 * math.cos(math.radians((y1 + y2) / 2))
        dy = (y2 - y1) * 110.57
        total += math.hypot(dx, dy)
    return total


def main(argv):
    lines = json.load(open(argv[0]))
    tiles = []
    for path in sorted(glob.glob(os.path.join(argv[1], "*.tif"))):
        with rasterio.open(path) as src:
            tiles.append((path, box(*src.bounds)))
    stats = {}
    open_tiles = {}
    for k, line in enumerate(lines):
        if len(line["coords"]) < 2:
            continue
        # Overpass gives the whole way of every line that touches the cell: keep only the part inside the cell.
        clat, clon = (float(v) for v in line["celda_id"].split("_"))
        clipped = LineString(line["coords"]).intersection(box(clon - 0.05, clat - 0.05, clon + 0.05, clat + 0.05))
        parts = [g for g in getattr(clipped, "geoms", [clipped]) if g.geom_type == "LineString" and not g.is_empty]
        if not parts:
            continue
        line = {**line, "coords": [c for g in parts for c in g.coords]} if len(parts) == 1 else line
        if len(parts) > 1:
            for g in parts[1:]:
                lines.append({**line, "coords": list(g.coords)})
            line = {**line, "coords": list(parts[0].coords)}
        geom = shp_transform(to3857, LineString(line["coords"]))
        lat = np.mean([c[1] for c in line["coords"]])
        # 1 m on the ground is 1 / cos(lat) units of EPSG:3857; pieces of about 300 m keep every window small.
        width = HALF_WIDTH_M / math.cos(math.radians(lat))
        step = 300.0 / math.cos(math.radians(lat))
        n = max(1, int(math.ceil(geom.length / step)))
        pieces = [LineString([geom.interpolate(i * geom.length / n), geom.interpolate((i + 1) * geom.length / n)])
                  for i in range(n)]
        values = []
        for piece in pieces:
            corridor = piece.buffer(width)
            for path, bounds in tiles:
                if not bounds.intersects(corridor):
                    continue
                src = open_tiles.setdefault(path, rasterio.open(path))
                part = corridor.intersection(bounds)
                win = from_bounds(*part.bounds, transform=src.transform).round_offsets().round_lengths()
                if win.width < 1 or win.height < 1:
                    continue
                arr = src.read(1, window=win)
                mask = geometry_mask([mapping(part)], out_shape=arr.shape, transform=src.window_transform(win), invert=True)
                v = arr[mask]
                values.append(v[v < 250].astype(np.float32))
        s = stats.setdefault(line["celda_id"], {"km": 0.0, "pix": [], "lineas": 0})
        s["km"] += length_km(line["coords"])
        s["lineas"] += 1
        if values:
            s["pix"].append(np.concatenate(values))
        if k % 50 == 0:
            print(k, "de", len(lines), flush=True)
    rows = []
    for cell, s in sorted(stats.items()):
        pix = np.concatenate(s["pix"]) if s["pix"] else np.empty(0)
        trees = pix[pix >= 1]
        row = {"celda_id": cell, "corr_lineas": s["lineas"], "corr_km_linea": round(s["km"], 2), "corr_pixeles": int(pix.size)}
        if pix.size:
            row.update({"corr_pct_dosel": float((pix >= 1).mean()), "corr_pct_dosel_5m": float((pix >= 5).mean()),
                        "corr_pct_dosel_8m": float((pix >= 8).mean()), "corr_pct_dosel_12m": float((pix >= 12).mean()),
                        "corr_dosel_media_m": float(trees.mean()) if trees.size else 0.0,
                        "corr_ha_dosel_8m": float((pix >= 8).sum()) / 10000.0})
        rows.append(row)
        print(row, flush=True)
    pd.DataFrame(rows).round(4).to_csv(argv[2], index=False)
    print("escrito", argv[2])


if __name__ == "__main__":
    main(sys.argv[1:])
