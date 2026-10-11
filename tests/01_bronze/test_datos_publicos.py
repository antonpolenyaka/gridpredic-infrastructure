"""
Tests of the Landing and Bronze helpers of the public data (INE, Wikidata,
Open-Meteo Elevation and OpenStreetMap), without Spark or network.

    python -m pytest tests/01_bronze -q
"""

import math
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in ("00_landing", "01_bronze"):
    path = os.path.join(ROOT, "etl", "jobs", folder)
    if path not in sys.path:
        sys.path.insert(0, path)

import job_bronze_datos_publicos as bronze
import job_landing_datos_publicos as landing


def test_grid_points_inside_the_cell():
    points = landing.grid_points({"40.30_-6.00": (40.3, -6.0)}, 0.1, 5)

    assert len(points) == 25
    lats = sorted({p["latitud"] for p in points})
    assert lats[0] == pytest.approx(40.26) and lats[-1] == pytest.approx(40.34)
    assert all(abs(p["longitud"] + 6.0) < 0.05 for p in points)


def test_queries():
    query = landing.overpass_query(landing.cell_bbox(40.3, -6.0, 0.1), 120)
    assert "[bbox:40.25,-6.05,40.35,-5.95]" in query
    assert 'way["power"~"^(line|minor_line|cable)$"]' in query
    assert query.strip().endswith("out count;")

    parts = landing.overpass_parts(landing.cell_bbox(40.3, -6.0, 0.1), 120)
    assert set(parts) == {"red", "viario", "cubierta"}
    assert all(q.startswith("[out:json][timeout:120][bbox:40.25,-6.05,40.35,-5.95];") for q in parts.values())
    assert "out count;" in parts["red"] and "highway" in parts["viario"] and "forest" in parts["cubierta"]

    sparql = landing.wikidata_query(["06", "10"])
    assert 'STRSTARTS(?ine, "06") || STRSTARTS(?ine, "10")' in sparql
    assert "P2046" in sparql and "P772" in sparql


def test_parse_ine_takes_the_code_from_the_metadata():
    payload = {
        "_peticion": {"tabla": 2863, "descargado_utc": "2026-10-11T00:00:00+00:00"},
        "series": [
            {"COD": "DPOP1", "Nombre": "Cáceres. Total. Total habitantes. Personas. ",
             "MetaData": [{"FK_Variable": 115, "Nombre": "Cáceres", "Codigo": "10"},
                          {"FK_Variable": 18, "Nombre": "Total", "Codigo": "0"}],
             "Data": [{"Anyo": 2025, "Valor": 387752.0, "Secreto": False}]},
            {"COD": "DPOP2", "Nombre": "Cáceres. Total. Total habitantes. Personas. ",
             "MetaData": [{"FK_Variable": 19, "Nombre": "Cáceres", "Codigo": "10037"},
                          {"FK_Variable": 18, "Nombre": "Total", "Codigo": "0"}],
             "Data": [{"Anyo": 2025, "Valor": 96651.0}, {"Anyo": 2024, "Valor": 96441.0}]},
        ],
    }
    rows = bronze.parse_ine(payload)

    assert len(rows) == 3
    province = [r for r in rows if r["codigo_ine"] is None]
    city = [r for r in rows if r["codigo_ine"] == "10037"]
    assert len(province) == 1 and province[0]["poblacion"] == 387752.0
    assert {r["anio"] for r in city} == {2024, 2025}
    assert city[0]["sexo"] == "Total" and city[0]["tabla_id"] == 2863


def test_parse_wikidata_in_km2():
    payload = {"results": {"bindings": [
        {"item": {"value": "http://www.wikidata.org/entity/Q1638598"}, "ine": {"value": "10001"},
         "itemLabel": {"value": "Abadía"}, "area": {"value": "44850000"}, "elev": {"value": "442"}},
        {"item": {"value": "http://www.wikidata.org/entity/Q1"}, "ine": {"value": "6148"},
         "itemLabel": {"value": "Valle"}},
    ]}}
    rows = bronze.parse_wikidata(payload)

    assert rows[0]["area_km2"] == pytest.approx(44.85)
    assert rows[0]["elevacion_m"] == 442.0 and rows[0]["wikidata_id"] == "Q1638598"
    assert rows[1]["codigo_ine"] == "06148" and rows[1]["area_km2"] is None


def test_segment_and_polygon_clipping():
    box = (0.0, 0.0, 10.0, 10.0)
    # A segment that crosses the box from outside to outside keeps 10 km.
    assert bronze.clipped_length([(-5.0, 5.0), (15.0, 5.0)], box) == pytest.approx(10.0)
    assert bronze.clipped_length([(-5.0, -5.0), (-1.0, -1.0)], box) == pytest.approx(0.0)
    # A square of 10 x 10 shifted 5 km to the right keeps half its surface.
    square = [(5.0, 0.0), (15.0, 0.0), (15.0, 10.0), (5.0, 10.0)]
    assert bronze.ring_area(bronze.clip_polygon(square, box)) == pytest.approx(50.0)


def test_rings_of_a_multipolygon_are_assembled():
    north = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]
    south = [(1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]
    rings = bronze.assemble_rings([north, south])

    assert len(rings) == 1
    assert bronze.ring_area(rings[0][:-1]) == pytest.approx(1.0)
    assert bronze.assemble_rings([[(0.0, 0.0), (5.0, 5.0)]]) == []


def test_parse_osm_cuts_to_the_cell():
    south, west, north, east = 40.25, -6.05, 40.35, -5.95
    lat0 = (south + north) / 2
    km_lon = bronze.KM_PER_DEG_LON * math.cos(math.radians(lat0))
    payload = {
        "_peticion": {"celda_id": "40.30_-6.00", "bbox": [south, west, north, east], "endpoint": "test",
                      "descargado_utc": "2026-10-11T00:00:00+00:00"},
        "osm3s": {"timestamp_osm_base": "2026-10-10T00:00:00Z"},
        "elements": [
            # An overhead MT line that crosses the whole cell from west to east, and beyond.
            {"type": "way", "id": 1, "tags": {"power": "minor_line", "voltage": "20000"},
             "geometry": [{"lat": 40.30, "lon": -6.10}, {"lat": 40.30, "lon": -5.90}]},
            # A forest square of 0,05 x 0,05 degrees fully inside.
            {"type": "way", "id": 2, "tags": {"landuse": "forest"},
             "geometry": [{"lat": 40.26, "lon": -6.04}, {"lat": 40.26, "lon": -5.99}, {"lat": 40.31, "lon": -5.99},
                          {"lat": 40.31, "lon": -6.04}, {"lat": 40.26, "lon": -6.04}]},
            {"type": "node", "id": 3, "lat": 40.3, "lon": -6.0, "tags": {"power": "pole"}},
            {"type": "node", "id": 4, "lat": 40.3, "lon": -6.0, "tags": {"amenity": "bench"}},
            {"type": "count", "id": 0, "tags": {"nodes": "0", "ways": "321", "relations": "0", "total": "321"}},
        ],
    }
    rows, query = bronze.parse_osm(payload)
    by_id = {r["osm_id"]: r for r in rows}

    assert set(by_id) == {0, 1, 2, 3}
    assert by_id[1]["longitud_km"] == pytest.approx(0.1 * km_lon, rel=1e-6)
    assert by_id[1]["voltage"] == "20000"
    assert by_id[2]["area_km2"] == pytest.approx(0.05 * km_lon * 0.05 * bronze.KM_PER_DEG_LAT, rel=1e-6)
    assert by_id[3]["clase"] == "power" and by_id[3]["valor"] == "pole" and by_id[3]["latitud"] == 40.3
    assert by_id[0]["clase"] == "building" and by_id[0]["n"] == 321
    assert query["area_celda_km2"] == pytest.approx(0.1 * km_lon * 0.1 * bronze.KM_PER_DEG_LAT, rel=1e-6)
    assert query["osm_base"] == "2026-10-10T00:00:00Z" and query["n_elementos"] == 4
