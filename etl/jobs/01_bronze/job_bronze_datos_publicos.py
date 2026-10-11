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
- placsp_licitacion_obra: one row per version of a works tender of PLACSP
  (the same tender comes again with every change of state): identifier,
  state, contracting body (type, DIR3, NIF, town, postal code and its
  hierarchy), object, contract type, CPV codes, budget, place (NUTS, town),
  planned period, award and publication dates.
- placsp_entrada_borrada: the entries deleted from the syndication (ref,
  date and reason).
- catastro_edificio: one row per building of the Catastro of every
  municipality: cadastral reference, state, year of construction, date of
  the version, use, dwellings, units, floors, built surface and the centre
  of its box in latitude and longitude (from UTM ETRS89).
- catastro_municipio: the match of the INE and Catastro codes.

The tables are rebuilt whole in every run (a few thousand rows, the
buildings about a hundred thousand).

    docker compose exec spark-master /opt/spark/bin/spark-submit /app/jobs/01_bronze/job_bronze_datos_publicos.py
"""

import gzip
import io
import json
import logging
import math
import os
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

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
# PLACSP (CODICE)
# ---------------------------------------------------------------------------

ATOM_URI = "http://www.w3.org/2005/Atom"
CODICE = {
    "cbc": "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
    "cbc-place-ext": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2",
    "cac-place-ext": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
    "at": "http://purl.org/atompub/tombstones/1.0",
    "ns7": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
}
NS = {"a": ATOM_URI, "cbc": CODICE["cbc"], "cac": CODICE["cac"], "cbcx": CODICE["cbc-place-ext"],
      "cacx": CODICE["cac-place-ext"]}


def text_at(element, path: str):
    if element is None:
        return None
    found = element.find(path, NS)
    if found is None or found.text is None:
        return None
    value = found.text.strip()
    return value or None


def number(value):
    try:
        return float(value) if value not in (None, "") else None
    except ValueError:
        return None


def parse_placsp_entry(entry) -> dict:
    status = entry.find("cacx:ContractFolderStatus", NS)
    party = status.find("cacx:LocatedContractingParty", NS) if status is not None else None
    project = status.find("cac:ProcurementProject", NS) if status is not None else None
    place = project.find("cac:RealizedLocation", NS) if project is not None else None
    period = project.find("cac:PlannedPeriod", NS) if project is not None else None

    ids = {}
    if party is not None:
        for item in party.findall("cac:Party/cac:PartyIdentification/cbc:ID", NS):
            ids.setdefault(item.get("schemeName") or "otro", (item.text or "").strip())
    chain, parent = [], party.find("cacx:ParentLocatedParty", NS) if party is not None else None
    while parent is not None:
        name = text_at(parent, "cac:PartyName/cbc:Name")
        if name:
            chain.append(name)
        parent = parent.find("cacx:ParentLocatedParty", NS)

    duration = project.find("cac:PlannedPeriod/cbc:DurationMeasure", NS) if project is not None else None
    results = status.findall("cac:TenderResult", NS) if status is not None else []
    awards = [text_at(r, "cbc:AwardDate") for r in results]
    awarded = [number(text_at(r, "cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount"))
               for r in results]
    signed = [text_at(r, "cac:Contract/cbc:IssueDate") for r in results]
    notices = [d.text.strip() for d in status.iterfind(
        "cacx:ValidNoticeInfo/cacx:AdditionalPublicationStatus/cacx:AdditionalPublicationDocumentReference/cbc:IssueDate",
        NS) if d.text and d.text.strip()] if status is not None else []
    cpv = []
    if status is not None:
        for code in status.iter(f"{{{CODICE['cbc']}}}ItemClassificationCode"):
            value = (code.text or "").strip()
            if value and value not in cpv:
                cpv.append(value)

    entry_id = text_at(entry, "a:id") or ""
    link = entry.find("a:link", NS)
    return {
        "licitacion_id": entry_id.rstrip("/").rsplit("/", 1)[-1] or None,
        "actualizado": text_at(entry, "a:updated"),
        "titulo": text_at(entry, "a:title"),
        "enlace": link.get("href") if link is not None else None,
        "expediente": text_at(status, "cbc:ContractFolderID"),
        "estado": text_at(status, "cbcx:ContractFolderStatusCode"),
        "organo": text_at(party, "cac:Party/cac:PartyName/cbc:Name"),
        "organo_tipo": text_at(party, "cbc:ContractingPartyTypeCode"),
        "dir3": ids.get("DIR3"),
        "nif": ids.get("NIF"),
        "organo_ciudad": text_at(party, "cac:Party/cac:PostalAddress/cbc:CityName"),
        "organo_cp": text_at(party, "cac:Party/cac:PostalAddress/cbc:PostalZone"),
        "organo_jerarquia": " > ".join(chain) or None,
        "objeto": text_at(project, "cbc:Name"),
        "tipo_contrato": text_at(project, "cbc:TypeCode"),
        "subtipo_contrato": text_at(project, "cbc:SubTypeCode"),
        "cpv": ",".join(cpv) or None,
        "importe_sin_iva": number(text_at(project, "cac:BudgetAmount/cbc:TaxExclusiveAmount")),
        "importe_estimado": number(text_at(project, "cac:BudgetAmount/cbc:EstimatedOverallContractAmount")),
        "nuts": text_at(place, "cbc:CountrySubentityCode"),
        "nuts_nombre": text_at(place, "cbc:CountrySubentity"),
        "lugar_ciudad": text_at(place, "cac:Address/cbc:CityName"),
        "lugar_cp": text_at(place, "cac:Address/cbc:PostalZone"),
        "inicio_previsto": text_at(period, "cbc:StartDate"),
        "fin_previsto": text_at(period, "cbc:EndDate"),
        "duracion": number(duration.text.strip()) if duration is not None and duration.text else None,
        "duracion_unidad": duration.get("unitCode") if duration is not None else None,
        "n_lotes": len(status.findall("cac:ProcurementProjectLot", NS)) if status is not None else 0,
        "procedimiento": text_at(status, "cac:TenderingProcess/cbc:ProcedureCode"),
        "fecha_limite_ofertas": text_at(status, "cac:TenderingProcess/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate"),
        "resultado": text_at(results[0], "cbc:ResultCode") if results else None,
        "fecha_adjudicacion": min([a for a in awards if a], default=None),
        "importe_adjudicado": sum(a for a in awarded if a is not None) if any(a is not None for a in awarded) else None,
        "adjudicatario": text_at(results[0], "cac:WinningParty/cac:PartyName/cbc:Name") if results else None,
        "fecha_contrato": min([s for s in signed if s], default=None),
        "fecha_primera_publicacion": min(notices, default=None),
    }


def parse_placsp(payload: dict) -> tuple:
    """(rows, deleted rows, entries that could not be read) of one landed year of PLACSP."""
    meta = payload.get("_peticion", {})
    namespaces = {**CODICE, **(payload.get("_namespaces") or {})}
    head = f'<feed xmlns="{ATOM_URI}" ' + " ".join(f'xmlns:{p}="{u}"' for p, u in namespaces.items()) + ">"
    rows, errors = [], 0

    for raw in payload.get("entradas", []):
        try:
            root = ET.fromstring(head + raw + "</feed>")
        except ET.ParseError:
            errors += 1
            continue
        entry = root.find("a:entry", NS)
        if entry is None:
            errors += 1
            continue
        row = parse_placsp_entry(entry)
        row["fichero_anio"] = meta.get("anio")
        row["descargado_utc"] = meta.get("descargado_utc")
        rows.append(row)

    deleted = [{"licitacion_id": (d.get("ref") or "").rstrip("/").rsplit("/", 1)[-1] or None, "ref": d.get("ref"),
                "borrado": d.get("when"), "motivo": d.get("tipo"), "fichero_anio": meta.get("anio")}
               for d in payload.get("borradas", [])]
    return rows, deleted, errors


# ---------------------------------------------------------------------------
# Catastro (INSPIRE Buildings)
# ---------------------------------------------------------------------------

BU_CORE = "{http://inspire.jrc.ec.europa.eu/schemas/bu-core2d/2.0}"
BU_EXT = "{http://inspire.jrc.ec.europa.eu/schemas/bu-ext2d/2.0}"
GML = "{http://www.opengis.net/gml/3.2}"
BASE = "{urn:x-inspire:specification:gmlas:BaseTypes:3.2}"
EPSG = re.compile(r"EPSG::?(\d+)")


def utm_to_latlon(x: float, y: float, zone: int) -> tuple:
    """Inverse transverse Mercator (UTM north, GRS80, the ellipsoid of ETRS89), to about 1 m."""
    a, f, k0 = 6378137.0, 1 / 298.257222101, 0.9996
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)
    m = y / k0
    mu = m / (a * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
           + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
           + (151 * e1 ** 3 / 96) * math.sin(6 * mu) + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
    sin, cos, tan = math.sin(phi), math.cos(phi), math.tan(phi)
    n = a / math.sqrt(1 - e2 * sin ** 2)
    t = tan ** 2
    c = ep2 * cos ** 2
    r = a * (1 - e2) / (1 - e2 * sin ** 2) ** 1.5
    d = (x - 500000.0) / (n * k0)
    lat = phi - (n * tan / r) * (d ** 2 / 2 - (5 + 3 * t + 10 * c - 4 * c ** 2 - 9 * ep2) * d ** 4 / 24
                                 + (61 + 90 * t + 298 * c + 45 * t ** 2 - 252 * ep2 - 3 * c ** 2) * d ** 6 / 720)
    lon = math.radians(6 * zone - 183) + (d - (1 + 2 * t + c) * d ** 3 / 6
                                         + (5 - 2 * c + 28 * t - 3 * c ** 2 + 8 * ep2 + 24 * t ** 2) * d ** 5 / 120) / cos
    return math.degrees(lat), math.degrees(lon)


def integer(value):
    try:
        return int(float(value)) if value not in (None, "") else None
    except ValueError:
        return None


def child_text(element, path: str):
    found = element.find(path)
    if found is None or found.text is None:
        return None
    value = found.text.strip()
    return value or None


def parse_catastro(raw: bytes, record: dict) -> list:
    """One row per building of the zip of a municipality (building.gml)."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        name = next((n for n in archive.namelist() if n.lower().endswith(".building.gml")), None)
        if name is None:
            return rows
        with archive.open(name) as gml:
            for _, element in ET.iterparse(gml, events=("end",)):
                if element.tag != f"{BU_EXT}Building":
                    continue
                begin = child_text(element, f"{BU_CORE}dateOfConstruction/{BU_CORE}DateOfEvent/{BU_CORE}beginning")
                end = child_text(element, f"{BU_CORE}dateOfConstruction/{BU_CORE}DateOfEvent/{BU_CORE}end")
                envelope = element.find(f"{GML}boundedBy/{GML}Envelope")
                lat = lon = x = y = None
                if envelope is not None:
                    low = (child_text(envelope, f"{GML}lowerCorner") or "").split()
                    high = (child_text(envelope, f"{GML}upperCorner") or "").split()
                    code = EPSG.search(envelope.get("srsName") or "")
                    if len(low) == 2 and len(high) == 2:
                        x = (float(low[0]) + float(high[0])) / 2
                        y = (float(low[1]) + float(high[1])) / 2
                        if code and 25828 <= int(code.group(1)) <= 25831:
                            lat, lon = utm_to_latlon(x, y, int(code.group(1)) - 25800)
                year = begin[:4] if begin and re.match(r"^\d{4}", begin) else None
                rows.append({
                    "codigo_ine": record.get("codigo_ine"),
                    "codigo_catastro": record.get("codigo_catastro"),
                    "referencia": child_text(element, f"{BU_CORE}inspireId/{BASE}Identifier/{BASE}localId")
                    or (element.get(f"{GML}id") or "").replace("ES.SDGC.BU.", "") or None,
                    "estado": child_text(element, f"{BU_CORE}conditionOfConstruction"),
                    "inicio_construccion": begin,
                    "fin_construccion": end,
                    "anio_construccion": int(year) if year else None,
                    "alta_version": child_text(element, f"{BU_CORE}beginLifespanVersion"),
                    "uso": child_text(element, f"{BU_EXT}currentUse"),
                    "n_unidades": integer(child_text(element, f"{BU_EXT}numberOfBuildingUnits")),
                    "n_viviendas": integer(child_text(element, f"{BU_EXT}numberOfDwellings")),
                    "n_plantas": integer(child_text(element, f"{BU_EXT}numberOfFloorsAboveGround")),
                    "superficie_m2": number(child_text(element, f"{BU_EXT}officialArea/{BU_EXT}OfficialArea/"
                                                                f"{BU_EXT}value")),
                    "x_utm": x, "y_utm": y, "latitud": lat, "longitud": lon,
                })
                element.clear()
    return rows


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

    tenders, deleted = [], []
    for path in sorted(fs.glob(f"{landing}/obras/placsp_*.json.gz")):
        with fs.open(path, "rb") as file:
            payload = json.loads(gzip.decompress(file.read()).decode("utf-8"))
        rows, gone, errors = parse_placsp(payload)
        tenders += rows
        deleted += gone
        logger.info("PLACSP %s: %s versions of works tenders, %s unreadable", path.rsplit("/", 1)[-1], len(rows),
                    errors)
    write(tenders, tables["obras"], "PLACSP works tenders")
    write(deleted, tables["obras_borradas"], "PLACSP deleted entries")

    manifest_path = f"{landing}/catastro/municipios.json"
    buildings, towns = [], []
    if fs.exists(manifest_path):
        manifest = read_json(fs, manifest_path)
        for record in manifest.get("municipios", []):
            path = record.get("fichero") or f"{landing}/catastro/{record['codigo_ine']}.zip"
            if not fs.exists(path):
                continue
            with fs.open(path, "rb") as file:
                rows = parse_catastro(file.read(), record)
            buildings += rows
            stamp = manifest.get("_peticion", {}).get("descargado_utc")
            towns.append({**{k: record.get(k) for k in ("codigo_ine", "municipio", "codigo_catastro",
                                                         "nombre_catastro", "url")},
                          "edificios": len(rows), "descargado_utc": stamp})
            for row in rows:
                row["descargado_utc"] = stamp
    write(buildings, tables["catastro"], "Catastro buildings")
    write(towns, tables["catastro_municipios"], "Catastro municipalities")

    logger.info("Bronze of the public data completed")


if __name__ == "__main__":
    main()
