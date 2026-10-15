"""
La especie dominante del arbolado por celda desde el Mapa Forestal de España
1:50.000 (MFE50 de MITECO, Banco de Datos de la Naturaleza, provincias de
Badajoz y Cáceres, descarga libre):

    https://www.miteco.gob.es/content/dam/miteco/es/biodiversidad/servicios/banco-datos-naturaleza/MFE50_06_tcm30-200070.zip
    https://www.miteco.gob.es/content/dam/miteco/es/biodiversidad/servicios/banco-datos-naturaleza/MFE50_10_tcm30-200099.zip

Lo que el HRL no da (cuaderno 3l): separar las frondosas caducas (melojo,
castaño, quejigo, chopos, fresnos, alisos) de las perennes (encina,
alcornoque, acebuche). Para cada celda de 0,1 grados y cada grupo, la parte
de la celda cubierta por copa de ese grupo: área del polígono dentro de la
celda x fracción de cabida cubierta arbolada (TFCCARB) x ocupación de cada
especie (O1, O2, O3, en décimas).

    python celdas_mfe.py <mfe50_06.shp> <mfe50_10.shp> etl\\config\\01_bronze\\mfe_celdas.csv

Necesita pyogrio, shapely, rasterio, numpy y pandas (y openpyxl para los municipios).
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyogrio
import shapely
from rasterio.warp import transform_geom

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bajar_arbolado import CELL_DEGREES, cells_of_places  # noqa: E402

# Códigos de especie del Inventario Forestal Nacional (SP1 - SP3 del MFE).
DECIDUOUS = {41, 42, 43, 44, 47, 51, 52, 54, 55, 56, 57, 58, 71, 72, 73, 75, 76, 77, 78, 79}
EVERGREEN_BROADLEAF = {45, 46, 64, 65, 66, 68, 69}
EUCALYPTUS = {60, 61, 62, 63}
GROUPS = {"caducifolia": DECIDUOUS, "perennifolia": EVERGREEN_BROADLEAF, "eucalipto": EUCALYPTUS}


def group_of(code: float) -> str:
    code = int(code or 0)
    if code == 0:
        return ""
    for name, codes in GROUPS.items():
        if code in codes:
            return name
    return "conifera" if 20 <= code < 40 else "otra"


def main(argv):
    shapes, out_csv = argv[:-1], argv[-1]
    root = os.path.dirname(os.path.dirname(HERE))
    cells = cells_of_places(os.path.join(root, "data", "reference_data", "municipios.xlsx"))
    layers = []
    columns = ["TFCCARB", "SP1", "O1", "SP2", "O2", "SP3", "O3"]
    for path in shapes:
        meta, _, wkb, fields = pyogrio.raw.read(path, columns=columns)
        geoms = shapely.from_wkb(wkb)
        table = pd.DataFrame({name: values for name, values in zip(meta["fields"], fields)})
        layers.append((meta["crs"], geoms, shapely.STRtree(geoms), table))
        print(os.path.basename(path), len(geoms), meta["crs"], flush=True)
    rows = []
    for _, c in cells.iterrows():
        box = {"type": "Polygon", "coordinates": [[
            (c["lon"] - CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2), (c["lon"] + CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2),
            (c["lon"] + CELL_DEGREES / 2, c["lat"] + CELL_DEGREES / 2), (c["lon"] - CELL_DEGREES / 2, c["lat"] + CELL_DEGREES / 2),
            (c["lon"] - CELL_DEGREES / 2, c["lat"] - CELL_DEGREES / 2)]]}
        cover = defaultdict(float)
        cell_area = None
        for crs, geoms, tree, table in layers:
            shape = shapely.geometry.shape(transform_geom("EPSG:4326", crs, box))
            cell_area = shape.area
            for i in tree.query(shape, predicate="intersects"):
                area = shapely.intersection(geoms[i], shape).area
                poly = table.iloc[i]
                canopy = area * float(poly["TFCCARB"] or 0) / 100.0
                for sp, occ in (("SP1", "O1"), ("SP2", "O2"), ("SP3", "O3")):
                    name = group_of(poly[sp])
                    if name:
                        cover[name] += canopy * float(poly[occ] or 0) / 10.0
        row = {"celda_id": c["celda_id"]}
        total = sum(cover.values())
        for name in list(GROUPS) + ["conifera", "otra"]:
            row[f"mfe_copa_{name}_pct"] = round(100.0 * cover[name] / cell_area, 3) if cell_area else np.nan
        row["mfe_copa_total_pct"] = round(100.0 * total / cell_area, 3) if cell_area else np.nan
        row["mfe_caducifolia_de_copa"] = round(cover["caducifolia"] / total, 4) if total else 0.0
        rows.append(row)
        print(row, flush=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    with open(os.path.splitext(out_csv)[0] + ".json", "w", encoding="utf-8") as handle:
        json.dump({"generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "fuentes": [os.path.basename(p) for p in shapes],
                   "licencia": "MITECO, Mapa Forestal de España 1:50.000, Banco de Datos de la Naturaleza",
                   "celdas": len(rows)}, handle, ensure_ascii=False, indent=2)
    print("escrito", out_csv)


if __name__ == "__main__":
    main(sys.argv[1:])
