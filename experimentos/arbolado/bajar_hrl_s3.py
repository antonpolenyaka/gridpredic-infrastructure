"""
El HRL de Copernicus (Tree Cover Density y Dominant Leaf Type, 10 m) por
teselas desde el S3 de Copernicus Data Space (CDSE), sin esperar a la cola de
recortes del CLMS. Baja las teselas de 100 km (EPSG:3035) que tocan la caja de
las redes y las junta en un GeoTIFF por producto, listo para celdas_hrl.py.

Las claves S3 se generan en eodata-s3keysmanager.dataspace.copernicus.eu con
una cuenta de CDSE y van en el .env del repositorio (CDSE_S3_KEY y
CDSE_S3_SECRET). El script nunca las escribe ni las imprime.

    python bajar_hrl_s3.py <dlt_cog.csv> <carpeta> [anio]

dlt_cog.csv es el listado del catalogo de CDSE para Dominant Leaf Type (con la
ruta s3 y la caja de cada tesela); las rutas de Tree Cover Density se sacan de
las mismas teselas. Necesita boto3, rasterio y numpy.
"""

import csv
import os
import sys

import boto3
import rasterio
from rasterio.merge import merge
from rasterio.warp import transform_bounds

ENDPOINT = "https://eodata.dataspace.copernicus.eu"
BUCKET = "eodata"
# Caja de las 43 celdas de las redes con medio grado de margen (lon_min, lat_min, lon_max, lat_max), WGS84.
BOX = (-7.05, 38.25, -5.55, 40.45)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PRODUCTS = {
    "dlt": ("dominant_leaf_type/clms_vlcc_dominant-leaf-type", "DLT"),
    "tcd": ("tree_cover_density/clms_vlcc_tree-cover-density", "TCD"),
}


def env_keys() -> tuple:
    values = {}
    with open(os.path.join(ROOT, ".env"), encoding="utf-8") as handle:
        for line in handle:
            if "=" in line and not line.lstrip().startswith("#"):
                name, value = line.split("=", 1)
                values[name.strip()] = value.strip().strip('"')
    return values["CDSE_S3_KEY"], values["CDSE_S3_SECRET"]


def bbox_of(polygon: str) -> tuple:
    pairs = [p.split() for p in polygon.replace("POLYGON((", "").replace("))", "").split(",")]
    lons = [float(p[0]) for p in pairs]
    lats = [float(p[1]) for p in pairs]
    return min(lons), min(lats), max(lons), max(lats)


def touches(a: tuple, b: tuple) -> bool:
    return a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]


def tiles(listing: str, year: str) -> list:
    out = []
    with open(listing, encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            if row["nominal_date"].startswith(year) and touches(bbox_of(row["bbox"]), BOX):
                out.append(row["s3_path"])
    return sorted(set(out))


def tcd_path(dlt_path: str) -> str:
    return dlt_path.replace(PRODUCTS["dlt"][0], PRODUCTS["tcd"][0]).replace("_DLT_", "_TCD_")


def download(client, s3_path: str, folder: str) -> list:
    prefix = s3_path.replace(f"s3://{BUCKET}/", "")
    got = []
    listing = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    for obj in listing.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith(".tif"):
            continue
        target = os.path.join(folder, os.path.basename(key))
        if not os.path.exists(target) or os.path.getsize(target) != obj["Size"]:
            client.download_file(BUCKET, key, target)
        got.append(target)
        print("  ", os.path.basename(key), round(obj["Size"] / 1e6, 1), "MB", flush=True)
    if not got:
        print("   sin .tif en", prefix, flush=True)
    return got


def mosaic(files: list, out_path: str) -> None:
    sources = [rasterio.open(f) for f in files]
    try:
        bounds = transform_bounds("EPSG:4326", sources[0].crs, *BOX, densify_pts=21)
        data, transform = merge(sources, bounds=bounds)
        profile = sources[0].profile.copy()
        profile.update(driver="GTiff", height=data.shape[1], width=data.shape[2], transform=transform,
                       compress="deflate", tiled=True, BIGTIFF="IF_SAFER")
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(data)
    finally:
        for s in sources:
            s.close()
    print("mosaico", out_path, flush=True)


def main(argv):
    listing, folder = argv[0], argv[1]
    year = argv[2] if len(argv) > 2 else "2021"
    os.makedirs(folder, exist_ok=True)
    key, secret = env_keys()
    client = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=key, aws_secret_access_key=secret,
                          region_name="default")
    dlt_paths = tiles(listing, year)
    print(len(dlt_paths), "teselas de", year, flush=True)
    for name, paths in (("dlt", dlt_paths), ("tcd", [tcd_path(p) for p in dlt_paths])):
        files = []
        for p in paths:
            print(name, p.rsplit("/", 1)[-1], flush=True)
            files += download(client, p, os.path.join(folder, name))
        if files:
            mosaic(files, os.path.join(folder, f"hrl_{name}_{year}.tif"))


if __name__ == "__main__":
    os.makedirs(sys.argv[2], exist_ok=True)
    for sub in ("dlt", "tcd"):
        os.makedirs(os.path.join(sys.argv[2], sub), exist_ok=True)
    main(sys.argv[1:])
