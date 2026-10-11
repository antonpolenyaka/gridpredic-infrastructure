"""
Bronze of the public data downloaded by job_landing_datos_publicos.py. Every
answer is taken to rows as it comes, with the source and the download time;
cleaning and the aggregates are in Silver.

- ine_padron_municipio: one row per series and year of the Padron tables
  (population by municipality and sex), with the INE code and the province
  of the metadata of the series. The rows of the province totals have no
  municipality.
- wikidata_municipio: INE code, name, surface (km2) and altitude (m) of
  every municipality Wikidata knows for the provinces of the study.
- openmeteo_elevacion_punto: the points of the relief grid of every cell and
  the centre of every municipality, with their altitude.
- osm_elemento_celda: one row per OpenStreetMap element of every cell with
  its class (power, highway, landuse, natural, building) and value (line,
  minor_line, cable, pole, track, forest...), the length (km) of the lines and
  the surface (km2) of the areas cut to the cell, so an element that crosses
  two cells counts in each one only for its part, and the position of the
  points. Ways are cut with Liang-Barsky and areas with Sutherland-Hodgman
  on a local plane (equirectangular, enough at 11 km); the rings of a
  multipolygon are assembled from its member ways before cutting.
- osm_consulta_celda: one row per cell downloaded, with its box, the
  endpoint that answered and the date of the OpenStreetMap data.

The tables are rebuilt whole in every run (a few thousand rows).

    docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/01_bronze/job_bronze_datos_publicos.py
"""

import json
import logging
import math
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "00_landing"))

from job_landing_datos_publicos import load_config  # noqa: E402
from job_landing_meteo_openmeteo import filesystem  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("publicos")

KM_PER_DEG_LAT = 110.574
KM_PER_DEG_LON = 111.320

OSM_CLASSES = {
    "power": {"line", "minor_line", "cable", "pole", "tower", "transformer", "substation", "switch"},
    "highway": {"motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential", "track"},
    "landuse": {"forest", "construction"},
    "natural": {"wood", "scrub"},
}
AREA_VALUES = {"forest", "construction", "wood", "scrub"}


# ---------------------------------------------------------------------------
# INE, Wikidata and elevation
# ---------------------------------------------------------------------------

def parse_ine(payload: dict) -> list:
    meta = payload.get("_peticion", {})
    rows = []

    for series in payload.get("series", []):
        values = {m.get("FK_Variable"): m for m in series.get("MetaData", [])}
        municipality = values.get(19, {})
        province = values.get(115, {})
        sex = values.get(18, {})
        code = str(municipality.get("Codigo") or "").strip()

        for point in series.get("Data", []):
            rows.append({
                "tabla_id": meta.get("tabla"),
                "serie_cod": series.get("COD"),
                "serie_nombre": series.get("Nombre"),
                "codigo_ine": code.zfill(5) if code else None,
                "municipio": municipality.get("Nombre"),
                "provincia_codigo": str(province.get("Codigo") or "").zfill(2) or None,
                "sexo": sex.get("Nombre"),
                "anio": point.get("Anyo"),
                "poblacion": point.get("Valor"),
                "secreto": bool(point.get("Secreto", False)),
                "descargado_utc": meta.get("descargado_utc"),
            })

    return rows


def binding_value(row: dict, key: str):
    value = row.get(key, {}).get("value")
    return value if value not in ("", None) else None


def parse_wikidata(payload: dict) -> list:
    meta = payload.get("_peticion", {})
    rows = []

    for row in payload.get("results", {}).get("bindings", []):
        area = binding_value(row, "area")
        elevation = binding_value(row, "elev")
        item = binding_value(row, "item") or ""
        rows.append({
            "codigo_ine": (binding_value(row, "ine") or "").zfill(5),
            "nombre": binding_value(row, "itemLabel"),
            "area_km2": float(area) / 1e6 if area is not None else None,
            "elevacion_m": float(elevation) if elevation is not None else None,
            "wikidata_id": item.rsplit("/", 1)[-1] or None,
            "descargado_utc": meta.get("descargado_utc"),
        })

    return rows


def parse_elevation(payload: dict) -> list:
    meta = payload.get("_peticion", {})
    return [{**point, "descargado_utc": meta.get("descargado_utc")} for point in payload.get("puntos", [])]


# ---------------------------------------------------------------------------
# OSM geometry
# ---------------------------------------------------------------------------

class Plane:
    """Equirectangular projection around the centre of a box, in km."""

    def __init__(self, bbox):
        south, west, north, east = bbox
        self.lat0 = (south + north) / 2.0
        self.lon0 = (west + east) / 2.0
        self.kx = KM_PER_DEG_LON * math.cos(math.radians(self.lat0))
        self.box = (self.x(west), self.y(south), self.x(east), self.y(north))

    def x(self, lon):
        return (lon - self.lon0) * self.kx

    def y(self, lat):
        return (lat - self.lat0) * KM_PER_DEG_LAT

    def points(self, geometry: list) -> list:
        return [(self.x(p["lon"]), self.y(p["lat"])) for p in geometry if "lat" in p and "lon" in p]


def clip_segment(p, q, box):
    """Liang-Barsky: the part of the segment p-q inside box (xmin, ymin, xmax, ymax), or None."""
    xmin, ymin, xmax, ymax = box
    dx, dy = q[0] - p[0], q[1] - p[1]
    t0, t1 = 0.0, 1.0

    for edge_p, edge_q in ((-dx, p[0] - xmin), (dx, xmax - p[0]), (-dy, p[1] - ymin), (dy, ymax - p[1])):
        if edge_p == 0:
            if edge_q < 0:
                return None
            continue
        t = edge_q / edge_p
        if edge_p < 0:
            if t > t1:
                return None
            t0 = max(t0, t)
        else:
            if t < t0:
                return None
            t1 = min(t1, t)

    return (p[0] + t0 * dx, p[1] + t0 * dy), (p[0] + t1 * dx, p[1] + t1 * dy)


def clipped_length(points: list, box) -> float:
    total = 0.0
    for p, q in zip(points, points[1:]):
        part = clip_segment(p, q, box)
        if part:
            (x0, y0), (x1, y1) = part
            total += math.hypot(x1 - x0, y1 - y0)
    return total


def clip_polygon(points: list, box) -> list:
    """Sutherland-Hodgman against the four sides of the box."""
    xmin, ymin, xmax, ymax = box
    sides = [
        (lambda p: p[0] >= xmin, lambda p, q: (xmin, p[1] + (q[1] - p[1]) * (xmin - p[0]) / (q[0] - p[0]))),
        (lambda p: p[0] <= xmax, lambda p, q: (xmax, p[1] + (q[1] - p[1]) * (xmax - p[0]) / (q[0] - p[0]))),
        (lambda p: p[1] >= ymin, lambda p, q: (p[0] + (q[0] - p[0]) * (ymin - p[1]) / (q[1] - p[1]), ymin)),
        (lambda p: p[1] <= ymax, lambda p, q: (p[0] + (q[0] - p[0]) * (ymax - p[1]) / (q[1] - p[1]), ymax)),
    ]
    output = list(points)

    for inside, cross in sides:
        if not output:
            break
        current, output = output, []
        previous = current[-1]
        for point in current:
            if inside(point):
                if not inside(previous):
                    output.append(cross(previous, point))
                output.append(point)
            elif inside(previous):
                output.append(cross(previous, point))
            previous = point

    return output


def ring_area(points: list) -> float:
    if len(points) < 3:
        return 0.0
    total = 0.0
    for (x0, y0), (x1, y1) in zip(points, points[1:] + points[:1]):
        total += x0 * y1 - x1 * y0
    return abs(total) / 2.0


def closed(points: list, tolerance: float = 1e-6) -> bool:
    return len(points) >= 4 and abs(points[0][0] - points[-1][0]) < tolerance and abs(points[0][1] - points[-1][1]) < tolerance


def assemble_rings(lines: list, tolerance: float = 1e-6) -> list:
    """Joins open member ways that share their ends into closed rings; the ones that never close are dropped."""
    pending = [list(line) for line in lines if len(line) >= 2]
    rings = []

    def same(a, b):
        return abs(a[0] - b[0]) < tolerance and abs(a[1] - b[1]) < tolerance

    while pending:
        ring = pending.pop(0)
        changed = True
        while not closed(ring, tolerance) and changed:
            changed = False
            for i, line in enumerate(pending):
                if same(ring[-1], line[0]):
                    ring += line[1:]
                elif same(ring[-1], line[-1]):
                    ring += line[::-1][1:]
                elif same(ring[0], line[-1]):
                    ring = line[:-1] + ring
                elif same(ring[0], line[0]):
                    ring = line[::-1][:-1] + ring
                else:
                    continue
                pending.pop(i)
                changed = True
                break
        if closed(ring, tolerance):
            rings.append(ring)

    return rings


def clipped_area(rings_outer: list, rings_inner: list, box) -> float:
    outer = sum(ring_area(clip_polygon(r[:-1], box)) for r in rings_outer)
    inner = sum(ring_area(clip_polygon(r[:-1], box)) for r in rings_inner)
    return max(outer - inner, 0.0)


def classify(tags: dict):
    for key, values in OSM_CLASSES.items():
        value = tags.get(key)
        if value in values:
            return key, value
    return None, None


def parse_osm(payload: dict) -> tuple:
    """(element rows, query row) of the Overpass answer of one cell."""
    meta = payload.get("_peticion", {})
    cell = meta.get("celda_id")
    bbox = meta.get("bbox")
    plane = Plane(bbox)
    rows = []

    for element in payload.get("elements", []):
        kind = element.get("type")
        tags = element.get("tags", {}) or {}
        base = {"celda_id": cell, "osm_tipo": kind, "osm_id": element.get("id"), "longitud_km": None,
                "area_km2": None, "latitud": None, "longitud": None, "voltage": tags.get("voltage"), "n": None}

        if kind == "count":
            rows.append({**base, "clase": "building", "valor": "count",
                         "n": int(float(tags.get("ways", tags.get("total", 0)) or 0))})
            continue

        key, value = classify(tags)
        if key is None:
            continue
        row = {**base, "clase": key, "valor": value}

        if kind == "node":
            row["latitud"], row["longitud"] = element.get("lat"), element.get("lon")
        elif "center" in element:
            row["latitud"], row["longitud"] = element["center"].get("lat"), element["center"].get("lon")

        if kind == "way" and "geometry" in element:
            points = plane.points(element["geometry"])
            if value in AREA_VALUES and closed(points):
                row["area_km2"] = clipped_area([points], [], plane.box)
            elif value not in AREA_VALUES:
                row["longitud_km"] = clipped_length(points, plane.box)
        elif kind == "relation" and value in AREA_VALUES:
            outer = [plane.points(m.get("geometry", [])) for m in element.get("members", [])
                     if m.get("type") == "way" and m.get("role", "outer") in ("outer", "")]
            inner = [plane.points(m.get("geometry", [])) for m in element.get("members", [])
                     if m.get("type") == "way" and m.get("role") == "inner"]
            row["area_km2"] = clipped_area(assemble_rings(outer), assemble_rings(inner), plane.box)

        rows.append(row)

    south, west, north, east = bbox
    query = {
        "celda_id": cell, "sur": south, "oeste": west, "norte": north, "este": east,
        "area_celda_km2": ring_area([(plane.box[0], plane.box[1]), (plane.box[2], plane.box[1]),
                                     (plane.box[2], plane.box[3]), (plane.box[0], plane.box[3])]),
        "endpoint": meta.get("endpoint"), "descargado_utc": meta.get("descargado_utc"),
        "osm_base": payload.get("osm3s", {}).get("timestamp_osm_base"), "n_elementos": len(rows),
    }
    for row in rows:
        row["descargado_utc"] = meta.get("descargado_utc")
    return rows, query


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def read_json(fs, path):
    with fs.open(path, "r") as file:
        return json.load(file)


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    cfg = load_config()["publicos"]
    landing = cfg["landing"].rstrip("/")
    tables = cfg["tablas"]

    spark = (
        SparkSession.builder.appName("job-bronze-datos-publicos")
        .config("spark.cores.max", "2")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    fs = filesystem()

    def write(rows: list, table: str, label: str):
        if not rows:
            logger.warning("No %s in Landing: %s not written", label, table)
            return
        pdf = pd.DataFrame(rows)
        for column in pdf.columns:
            if pdf[column].dtype == object:
                pdf[column] = pdf[column].astype("string")
        (
            spark.createDataFrame(pdf)
            .withColumn("audit_loaded_at", F.current_timestamp())
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        )
        logger.info("Bronze %s: %s rows of %s", table, len(pdf), label)

    padron = []
    for path in sorted(fs.glob(f"{landing}/ine/padron_*.json")):
        padron += parse_ine(read_json(fs, path))
    write(padron, tables["padron"], "INE Padron")

    wikidata = f"{landing}/wikidata/municipios.json"
    write(parse_wikidata(read_json(fs, wikidata)) if fs.exists(wikidata) else [], tables["wikidata"], "Wikidata")

    elevation = f"{landing}/elevacion/puntos.json"
    write(parse_elevation(read_json(fs, elevation)) if fs.exists(elevation) else [], tables["elevacion"], "elevation")

    elements, queries = [], []
    for path in sorted(fs.glob(f"{landing}/osm/*.json")):
        rows, query = parse_osm(read_json(fs, path))
        elements += rows
        queries.append(query)
    write(elements, tables["osm"], "OSM elements")
    write(queries, tables["osm_resumen"], "OSM cells")

    logger.info("Bronze of the public data completed")


if __name__ == "__main__":
    main()
