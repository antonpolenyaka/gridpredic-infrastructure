"""
Las aves grandes de cada municipio desde el Inventario Español de Especies
Terrestres (MITECO, base de datos de 2015 y malla UTM 10 x 10 km, descarga
libre):

    https://www.miteco.gob.es/content/dam/miteco/es/biodiversidad/temas/inventarios-nacionales/bd_ieet_2015_tcm30-207985.zip
    https://www.miteco.gob.es/content/dam/miteco/es/biodiversidad/temas/inventarios-nacionales/malla10x10_p_tcm30-200057.zip

Para cada municipio con CT (municipios.xlsx), la cuadrícula 10 x 10 en la que
cae y en ella: si hay cigüeña blanca, cuántas especies de aves grandes de las
que se electrocutan o anidan en los apoyos (cigüeñas, rapaces diurnas y
nocturnas, córvidos grandes) y cuántas rapaces.

    python ieet_municipios.py <BD_IEET_2015.mdb> <Malla10km_p.shp> etl\\config\\01_bronze\\especies_municipios.csv

Necesita access-parser, pyogrio, shapely, rasterio y pandas.
"""

import os
import sys

import pandas as pd
import pyogrio
import shapely
from access_parser import AccessParser
from rasterio.warp import transform

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
RAPTORS = {"Accipitriformes", "Falconiformes", "Strigiformes", "Accipitridae", "Falconidae", "Strigidae", "Tytonidae"}
BIG_CORVIDS = {"Corvus corax", "Corvus corone", "Corvus monedula", "Pica pica", "Cyanopica cooki"}


def main(argv):
    mdb, grid_path, out = argv
    table = AccessParser(mdb).parse_table("BD_IEET_2015")
    species = pd.DataFrame({k: table[k] for k in ("Grupo", "Nombre", "Orden", "Familia", "CUTM10x10")})
    birds = species[species["Grupo"].astype(str).str.startswith("Aves")]
    raptor = birds["Orden"].isin(RAPTORS) | birds["Familia"].isin(RAPTORS)
    stork = birds["Nombre"].eq("Ciconia ciconia")
    big = raptor | birds["Orden"].eq("Ciconiiformes") | birds["Nombre"].isin(BIG_CORVIDS)
    per_square = pd.DataFrame({
        "ieet_ciguena": birds[stork].groupby("CUTM10x10").size().clip(upper=1),
        "ieet_aves_grandes": birds[big].groupby("CUTM10x10")["Nombre"].nunique(),
        "ieet_rapaces": birds[raptor].groupby("CUTM10x10")["Nombre"].nunique(),
    }).fillna(0)

    meta, _, wkb, fields = pyogrio.raw.read(grid_path)
    names = list(meta["fields"])
    # CUADRICULA lleva el huso y la letra (29SQD38), como CUTM10x10 en la base de datos; UTMCODE no.
    code_field = next(n for n in ("CUADRICULA", "CUTM10x10") if n in names)
    codes = list(fields[names.index(code_field)])
    squares = shapely.from_wkb(wkb)
    tree = shapely.STRtree(squares)

    places = pd.read_excel(os.path.join(ROOT, "data", "reference_data", "municipios.xlsx"))
    lat = next(c for c in places.columns if c.lower().startswith("lat"))
    lon = next(c for c in places.columns if c.lower().startswith("lon"))
    name = next(c for c in places.columns if "municipio" in c.lower() and "id" not in c.lower())
    xs, ys = transform("EPSG:4326", meta["crs"], places[lon].tolist(), places[lat].tolist())
    rows = []
    for muni, x, y in zip(places[name], xs, ys):
        hit = tree.query(shapely.Point(x, y), predicate="within")
        code = codes[hit[0]] if len(hit) else None
        row = {"municipio": muni, "cuadricula": code}
        row.update(per_square.loc[code].to_dict() if code in per_square.index else
                   {"ieet_ciguena": 0, "ieet_aves_grandes": 0, "ieet_rapaces": 0})
        rows.append(row)
    frame = pd.DataFrame(rows).drop_duplicates("municipio")
    frame.to_csv(out, index=False)
    print(len(frame), "municipios;", frame[["ieet_ciguena", "ieet_aves_grandes", "ieet_rapaces"]].describe().round(1))


if __name__ == "__main__":
    main(sys.argv[1:])
