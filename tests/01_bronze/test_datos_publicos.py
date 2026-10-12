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


# ---------------------------------------------------------------------------
# PLACSP and Catastro
# ---------------------------------------------------------------------------

FEED_HEAD = (
    '<?xml version="1.0" encoding="UTF-8"?>\n<feed xmlns="http://www.w3.org/2005/Atom" '
    'xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2" '
    'xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2" '
    'xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2" '
    'xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2" '
    'xmlns:at="http://purl.org/atompub/tombstones/1.0">\n'
)


def entry(number, title, type_code, cpv, nuts, postal, dir3=None, state="ADJ", body="Ayuntamiento",
          duration=("3", "MON"), award="2024-03-01", published="2024-01-15", start=None):
    ident = f'<cac:PartyIdentification><cbc:ID schemeName="DIR3">{dir3}</cbc:ID></cac:PartyIdentification>' if dir3 \
        else ""
    period = (f'<cac:PlannedPeriod>{f"<cbc:StartDate>{start}</cbc:StartDate>" if start else ""}'
              f'<cbc:DurationMeasure unitCode="{duration[1]}">{duration[0]}</cbc:DurationMeasure></cac:PlannedPeriod>')
    return (
        f"<entry><id>https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/{number}</id>"
        f'<link href="https://contrataciondelestado.es/x/{number}"/><title>{title}</title>'
        f"<updated>2024-03-02T10:00:00.000+01:00</updated><cac-place-ext:ContractFolderStatus>"
        f"<cbc:ContractFolderID>E{number}</cbc:ContractFolderID>"
        f"<cbc-place-ext:ContractFolderStatusCode>{state}</cbc-place-ext:ContractFolderStatusCode>"
        f"<cac-place-ext:LocatedContractingParty><cbc:ContractingPartyTypeCode>3</cbc:ContractingPartyTypeCode>"
        f"<cac:Party>{ident}<cac:PartyName><cbc:Name>{body}</cbc:Name></cac:PartyName><cac:PostalAddress>"
        f"<cbc:CityName>Pueblo</cbc:CityName><cbc:PostalZone>{postal}</cbc:PostalZone></cac:PostalAddress></cac:Party>"
        f"<cac-place-ext:ParentLocatedParty><cac:PartyName><cbc:Name>Ayuntamientos</cbc:Name></cac:PartyName>"
        f"<cac-place-ext:ParentLocatedParty><cac:PartyName><cbc:Name>Extremadura</cbc:Name></cac:PartyName>"
        f"</cac-place-ext:ParentLocatedParty></cac-place-ext:ParentLocatedParty></cac-place-ext:LocatedContractingParty>"
        f"<cac:ProcurementProject><cbc:Name>{title}</cbc:Name>"
        f'<cbc:TypeCode listURI="http://contrataciondelestado.es/codice/cl/2.08/ContractCode-2.08.gc">{type_code}'
        f"</cbc:TypeCode><cac:BudgetAmount><cbc:TaxExclusiveAmount currencyID=\"EUR\">100000</cbc:TaxExclusiveAmount>"
        f"</cac:BudgetAmount><cac:RequiredCommodityClassification><cbc:ItemClassificationCode>{cpv}"
        f"</cbc:ItemClassificationCode></cac:RequiredCommodityClassification><cac:RealizedLocation>"
        f"<cbc:CountrySubentityCode>{nuts}</cbc:CountrySubentityCode></cac:RealizedLocation>{period}"
        f"</cac:ProcurementProject><cac:TenderResult><cbc:ResultCode>9</cbc:ResultCode>"
        f"<cbc:AwardDate>{award}</cbc:AwardDate><cac:AwardedTenderedProject><cac:LegalMonetaryTotal>"
        f"<cbc:TaxExclusiveAmount>80000</cbc:TaxExclusiveAmount></cac:LegalMonetaryTotal></cac:AwardedTenderedProject>"
        f"</cac:TenderResult><cac-place-ext:ValidNoticeInfo><cac-place-ext:AdditionalPublicationStatus>"
        f"<cac-place-ext:AdditionalPublicationDocumentReference><cbc:IssueDate>{published}</cbc:IssueDate>"
        f"</cac-place-ext:AdditionalPublicationDocumentReference></cac-place-ext:AdditionalPublicationStatus>"
        f"</cac-place-ext:ValidNoticeInfo></cac-place-ext:ContractFolderStatus></entry>"
    )


def test_placsp_filter_keeps_the_works_of_the_territory():
    territory, works = landing.placsp_patterns({"nuts": ["ES43"], "codigos_postales": ["06", "10"],
                                                "tipos_contrato": ["3"], "cpv_prefijos": ["45"]})
    text = FEED_HEAD + "".join([
        entry(1, "Pavimentacion de la calle Mayor", "3", "45233000", "ES432", "10001", dir3="L01100019"),
        entry(2, "Suministro de papel", "1", "30197000", "ES432", "10001"),             # not works
        entry(3, "Obras en Sevilla", "3", "45000000", "ES618", "41001"),                 # other region
        entry(4, "Servicio con obra", "2", "45100000", "ES43", "06800"),                 # CPV 45 in a service
    ]) + ('<at:deleted-entry ref="https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/9" '
          'when="2024-03-03T10:00:00.000+01:00"><at:comment type="ANULADA"/></at:deleted-entry></feed>')
    kept, deleted, read = landing.filter_feed(text, territory, works)
    marks = landing.placsp_marks({"nuts": ["ES43"], "codigos_postales": ["06", "10"]})
    assert landing.filter_feed(text, territory, works, marks) == (kept, deleted, read)

    assert read == 4
    assert [int(e.split("licitacionesPerfilContratante/")[1].split("<")[0]) for e in kept] == [1, 4]
    assert deleted == [{"ref": "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/9",
                        "when": "2024-03-03T10:00:00.000+01:00", "tipo": "ANULADA"}]


def test_parse_placsp_entry():
    payload = {"_peticion": {"anio": 2024, "descargado_utc": "2026-10-11T00:00:00+00:00"},
               "_namespaces": dict(landing.NAMESPACE.findall(FEED_HEAD)),
               "entradas": [entry(1, "Pavimentacion de la calle Mayor", "3", "45233000", "ES432", "10001",
                                  dir3="L01100019", duration=("45", "DAY"), start="2024-04-01"), "<entry><broken>"],
               "borradas": [{"ref": "https://x/licitacionesPerfilContratante/9", "when": "2024-03-03", "tipo": "ANULADA"}]}
    rows, deleted, errors = bronze.parse_placsp(payload)

    assert errors == 1 and len(rows) == 1
    row = rows[0]
    assert row["licitacion_id"] == "1" and row["dir3"] == "L01100019" and row["estado"] == "ADJ"
    assert row["tipo_contrato"] == "3" and row["cpv"] == "45233000" and row["nuts"] == "ES432"
    assert row["importe_sin_iva"] == 100000.0 and row["importe_adjudicado"] == 80000.0
    assert row["duracion"] == 45.0 and row["duracion_unidad"] == "DAY" and row["inicio_previsto"] == "2024-04-01"
    assert row["fecha_adjudicacion"] == "2024-03-01" and row["fecha_primera_publicacion"] == "2024-01-15"
    assert row["organo_jerarquia"] == "Ayuntamientos > Extremadura" and row["fichero_anio"] == 2024
    assert deleted == [{"licitacion_id": "9", "ref": "https://x/licitacionesPerfilContratante/9",
                        "borrado": "2024-03-03", "motivo": "ANULADA", "fichero_anio": 2024}]


def test_catastro_names_and_index():
    assert landing.plain_name("Torno (El)") == landing.plain_name("El Torno") == "eltorno"
    assert landing.plain_name("Granja, La") == "lagranja"
    assert landing.plain_name("BAÑOS DE MONTEMAYOR") == landing.plain_name("Baños de Montemayor")
    feed = ('<feed><entry><title> 10099-HERVAS buildings</title>'
            '<link rel="enclosure" href="https://www.catastro.hacienda.gob.es/INSPIRE/Buildings/10/10099-HERVAS/'
            'A.ES.SDGC.BU.10099.zip" type="application/atom+xml"/></entry></feed>').encode("latin-1")
    index = landing.catastro_index(landing.decode_feed(feed))
    assert index == [{"codigo_catastro": "10099", "nombre_catastro": "HERVAS",
                      "url": "https://www.catastro.hacienda.gob.es/INSPIRE/Buildings/10/10099-HERVAS/"
                             "A.ES.SDGC.BU.10099.zip"}]
    assert landing.decode_feed("BAÑOS".encode("latin-1")) == "BAÑOS"


def test_utm_to_latlon():
    lat, lon = bronze.utm_to_latlon(255396.15675, 4461572.3355, 30)
    assert lat == pytest.approx(40.26888448288353, abs=1e-7) and lon == pytest.approx(-5.876680643358629, abs=1e-7)
    lat, lon = bronze.utm_to_latlon(700000.0, 4400000.0, 29)
    assert lat == pytest.approx(39.72644584411712, abs=1e-7) and lon == pytest.approx(-6.666379316151641, abs=1e-7)


def test_parse_catastro_buildings():
    import io
    import zipfile

    def building(ref, year, use, dwellings, area):
        return (
            f'<gml:featureMember><bu-ext2d:Building gml:id="ES.SDGC.BU.{ref}"><gml:boundedBy>'
            '<gml:Envelope srsName="urn:ogc:def:crs:EPSG::25830"><gml:lowerCorner>255390.882 4461569.2205'
            '</gml:lowerCorner><gml:upperCorner>255401.4315 4461575.4505</gml:upperCorner></gml:Envelope>'
            '</gml:boundedBy><bu-core2d:beginLifespanVersion>2019-05-29T00:00:00</bu-core2d:beginLifespanVersion>'
            '<bu-core2d:conditionOfConstruction>functional</bu-core2d:conditionOfConstruction>'
            f'<bu-core2d:dateOfConstruction><bu-core2d:DateOfEvent><bu-core2d:beginning>{year}-01-01T00:00:00'
            '</bu-core2d:beginning></bu-core2d:DateOfEvent></bu-core2d:dateOfConstruction>'
            f'<bu-core2d:inspireId><base:Identifier><base:localId>{ref}</base:localId></base:Identifier>'
            f'</bu-core2d:inspireId><bu-ext2d:currentUse>{use}</bu-ext2d:currentUse>'
            '<bu-ext2d:numberOfBuildingUnits>2</bu-ext2d:numberOfBuildingUnits>'
            f'<bu-ext2d:numberOfDwellings>{dwellings}</bu-ext2d:numberOfDwellings>'
            '<bu-ext2d:numberOfFloorsAboveGround xsi:nil="true"></bu-ext2d:numberOfFloorsAboveGround>'
            '<bu-ext2d:officialArea><bu-ext2d:OfficialArea><bu-ext2d:value uom="m2">'
            f'{area}</bu-ext2d:value></bu-ext2d:OfficialArea></bu-ext2d:officialArea></bu-ext2d:Building>'
            '</gml:featureMember>'
        )

    gml = (
        '<?xml version="1.0" encoding="ISO-8859-1"?><gml:FeatureCollection gml:id="ES.SDGC.BU" '
        'xmlns:base="urn:x-inspire:specification:gmlas:BaseTypes:3.2" '
        'xmlns:bu-core2d="http://inspire.jrc.ec.europa.eu/schemas/bu-core2d/2.0" '
        'xmlns:bu-ext2d="http://inspire.jrc.ec.europa.eu/schemas/bu-ext2d/2.0" '
        'xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        + building("000100200TK56B", 2019, "1_residential", 4, 320) + building("000100300TK56B", 1970, "3_industrial",
                                                                                 0, 50)
        + "</gml:FeatureCollection>"
    ).encode("latin-1")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("A.ES.SDGC.BU.10099.building.gml", gml)
        archive.writestr("A.ES.SDGC.BU.MD.10099.xml", "<x/>")
    rows = bronze.parse_catastro(buffer.getvalue(), {"codigo_ine": "10096", "codigo_catastro": "10099"})

    assert len(rows) == 2
    first = rows[0]
    assert first["referencia"] == "000100200TK56B" and first["anio_construccion"] == 2019
    assert first["uso"] == "1_residential" and first["n_viviendas"] == 4 and first["n_plantas"] is None
    assert first["superficie_m2"] == 320.0 and first["codigo_ine"] == "10096"
    assert first["latitud"] == pytest.approx(40.2688845, abs=1e-6)
    assert rows[1]["anio_construccion"] == 1970 and rows[1]["n_viviendas"] == 0


def test_ieex_projections_are_copied_to_landing():
    import fsspec

    fs = fsspec.filesystem("memory")
    cfg = landing.load_config()["publicos"]
    assert landing.land_projections(fs, cfg, "memory://landing/publicos") == 2
    import pandas as pd

    with fs.open("memory://landing/publicos/proyecciones/ieex_proyecciones_ame.csv", "rb") as file:
        projection = pd.read_csv(file, dtype={"ame_codigo": str})
    with fs.open("memory://landing/publicos/proyecciones/ieex_ame_municipios.csv", "rb") as file:
        towns = pd.read_csv(file, dtype={"codigo_ine": str, "ame_codigo": str})
    # 31 groups of municipalities, 2024-2039; every municipality of Badajoz and Caceres in one of them.
    assert projection["ame_codigo"].nunique() == 31 and set(projection["anio"]) == set(range(2024, 2040))
    assert len(towns) == 388 and towns["codigo_ine"].is_unique
    assert towns.loc[towns["codigo_ine"] == "10096", "ame_codigo"].item() == "1009"  # Hervas
    jerte = projection[projection["ame_codigo"] == "1012"].set_index("anio")["poblacion"]
    assert jerte[2039] / jerte[2024] - 1 == pytest.approx(0.010, abs=0.001)
    assert fs.exists("memory://landing/publicos/proyecciones/peticion.json")


def test_gbif_query_and_counts():
    cfg = landing.load_config()["publicos"]["aves"]
    params = landing.gbif_params({"scientificName": "Ciconia ciconia"}, 39.4, -6.3, 0.05, cfg)
    assert params["decimalLatitude"] == "39.3500,39.4500" and params["limit"] == "0"
    assert params["facet"] == ["year", "month"] and params["scientificName"] == "Ciconia ciconia"

    payload = {
        "taxones": {
            "ciguena_blanca": {"count": 12, "facets": [
                {"field": "YEAR", "counts": [{"name": "2019", "count": 5}, {"name": "2021", "count": 7}]},
                {"field": "MONTH", "counts": [{"name": "3", "count": 9}, {"name": "6", "count": 3}]}]},
            "aves": {"count": 300, "facets": [{"field": "YEAR", "counts": [{"name": "2021", "count": 300}]}]},
        },
        "_peticion": {"celda_id": "39.40_-6.30", "descargado_utc": "2026-10-12T03:00:00+00:00"},
    }
    rows = bronze.parse_gbif(payload)
    by_key = {(r["taxon"], r["tipo"], r["clave"]): r["registros"] for r in rows}
    assert by_key[("ciguena_blanca", "total", None)] == 12
    assert by_key[("ciguena_blanca", "anio", 2021)] == 7 and by_key[("ciguena_blanca", "mes", 3)] == 9
    assert by_key[("aves", "anio", 2021)] == 300
    assert {r["celda_id"] for r in rows} == {"39.40_-6.30"}


GML3 = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection xmlns:ms="http://mapserver.gis.umn.edu/mapserver" xmlns:gml="http://www.opengis.net/gml"
    xmlns:wfs="http://www.opengis.net/wfs">
  <gml:featureMember>
    <ms:modis.ba.poly gml:id="modis.ba.poly.101">
      <ms:id>101</ms:id><ms:FIREDATE>2022-05-25</ms:FIREDATE><ms:LASTUPDATE>2022-06-01 10:00:00</ms:LASTUPDATE>
      <ms:COUNTRY>ES</ms:COUNTRY><ms:PROVINCE>Caceres</ms:PROVINCE><ms:COMMUNE>Tornavacas</ms:COMMUNE>
      <ms:AREA_HA>512</ms:AREA_HA>
      <ms:msGeometry><gml:Polygon><gml:exterior><gml:LinearRing>
        <gml:posList>40.25 -5.70 40.27 -5.70 40.27 -5.66 40.25 -5.66 40.25 -5.70</gml:posList>
      </gml:LinearRing></gml:exterior></gml:Polygon></ms:msGeometry>
    </ms:modis.ba.poly>
  </gml:featureMember>
</wfs:FeatureCollection>"""

GML2 = """<wfs:FeatureCollection xmlns:ms="http://mapserver.gis.umn.edu/mapserver" xmlns:gml="http://www.opengis.net/gml"
    xmlns:wfs="http://www.opengis.net/wfs">
  <gml:featureMember>
    <ms:modis.ba.poly fid="modis.ba.poly.7">
      <ms:id>7</ms:id><ms:FIREDATE>2020-08-06</ms:FIREDATE><ms:AREA_HA>40.5</ms:AREA_HA>
      <ms:msGeometry><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing>
        <gml:coordinates>-6.52,39.81 -6.50,39.81 -6.50,39.83 -6.52,39.81</gml:coordinates>
      </gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></ms:msGeometry>
    </ms:modis.ba.poly>
  </gml:featureMember>
</wfs:FeatureCollection>"""


def test_parse_effis_in_both_axis_orders():
    first = bronze.parse_effis(GML3)[0]
    assert (first["id"], first["fecha_incendio"], first["municipio"]) == ("101", "2022-05-25", "Tornavacas")
    assert first["area_ha"] == 512.0 and first["n_vertices"] == 5
    assert first["latitud"] == pytest.approx(40.258, abs=1e-3) and first["longitud"] == pytest.approx(-5.684, abs=1e-3)

    second = bronze.parse_effis(GML2.encode("utf-8"))[0]
    assert second["latitud"] == pytest.approx(39.815, abs=1e-3) and second["longitud"] == pytest.approx(-6.51, abs=1e-3)
    assert second["fecha_incendio"] == "2020-08-06" and second["area_ha"] == 40.5


def test_effis_request_variants_and_area_check():
    cfg = landing.load_config()["publicos"]["incendios"]
    box = (-7.0, 39.0, -5.5, 40.5)
    urls = landing.effis_requests(cfg, box)
    assert len(urls) == len(cfg["variantes"])
    assert "bbox=39.0000,-7.0000,40.5000,-5.5000,urn:ogc:def:crs:EPSG::4326" in urls[0]
    assert "bbox=-7.0000,39.0000,-5.5000,40.5000" in urls[1] and "request=GetFeature" in urls[1]
    assert landing.in_area(landing.first_points(GML3), box)
    assert landing.in_area(landing.first_points(GML2), box)
    assert not landing.in_area([(2.1, 41.4), (2.2, 41.5)], box)


CAP_ALERT = """<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>2.49.0.0.724.0.ES.20240110063000.761001VI</identifier>
  <sender>http://www.aemet.es</sender>
  <sent>2024-01-10T06:30:00-00:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-GB</language>
    <event>Moderate wind warning</event>
  </info>
  <info>
    <language>es-ES</language>
    <category>Met</category>
    <event>Aviso de vientos de nivel naranja</event>
    <severity>Severe</severity>
    <certainty>Likely</certainty>
    <eventCode><valueName>AEMET-Meteoalerta fenomeno</valueName><value>VI;Vientos</value></eventCode>
    <effective>2024-01-10T07:30:00+01:00</effective>
    <onset>2024-01-10T12:00:00+01:00</onset>
    <expires>2024-01-10T23:59:59+01:00</expires>
    <parameter><valueName>AEMET-Meteoalerta nivel</valueName><value>naranja</value></parameter>
    <parameter><valueName>AEMET-Meteoalerta parametro</valueName><value>VI; Rachas máximas; 90 km/h</value></parameter>
    <parameter><valueName>AEMET-Meteoalerta probabilidad</valueName><value>40%-70%</value></parameter>
    <area>
      <areaDesc>Norte de Cáceres</areaDesc>
      <polygon>40.0,-6.5 40.6,-6.5 40.6,-5.5 40.0,-5.5 40.0,-6.5</polygon>
      <geocode><valueName>AEMET-Meteoalerta zona</valueName><value>761001</value></geocode>
    </area>
    <area>
      <areaDesc>Litoral de Almería</areaDesc>
      <polygon>36.6,-2.6 36.9,-2.6 36.9,-1.9 36.6,-1.9 36.6,-2.6</polygon>
      <geocode><valueName>AEMET-Meteoalerta zona</valueName><value>610402</value></geocode>
    </area>
  </info>
</alert>
"""


def test_parse_cap_keeps_spanish_and_the_study_area():
    box = (38.0, -7.5, 40.8, -5.0)
    rows = bronze.parse_cap(CAP_ALERT.encode("utf-8"), box)
    assert len(rows) == 1
    row = rows[0]
    assert row["zona"] == "761001" and row["zona_nombre"] == "Norte de Cáceres"
    assert row["evento"] == "Aviso de vientos de nivel naranja"
    assert row["fenomeno"] == "VI;Vientos" and row["nivel"] == "naranja" and row["probabilidad"] == "40%-70%"
    assert row["inicio"] == "2024-01-10T12:00:00+01:00" and row["tipo_mensaje"] == "Alert"
    assert row["lat_min"] == 40.0 and row["lon_max"] == -5.5
    # Without a box every area comes out.
    assert len(bronze.parse_cap(CAP_ALERT.encode("utf-8"))) == 2
    assert bronze.parse_cap(b"<html>not a cap</html>") == []


def test_parse_aemet_archive_reads_nested_packages():
    import gzip
    import io
    import tarfile

    def tar_of(name: str, payload: bytes) -> bytes:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        return buffer.getvalue()

    inner = gzip.compress(tar_of("Z_CAP_C_LEMM_20240110063000_AFAZ761001VI.xml", CAP_ALERT.encode("utf-8")))
    outer = tar_of("20240110.tar.gz", inner)
    rows = bronze.parse_aemet_archive(outer, (38.0, -7.5, 40.8, -5.0))
    assert [r["zona"] for r in rows] == ["761001"]
    assert bronze.parse_aemet_archive(b"") == []


def test_aemet_requests_by_pieces():
    chunks = landing.warning_chunks("2018-06-18", "2018-08-31", 31)
    assert [(str(a), str(b)) for a, b in chunks] == [
        ("2018-06-18", "2018-07-18"), ("2018-07-19", "2018-08-18"), ("2018-08-19", "2018-08-31")]
    cfg = landing.load_config()["publicos"]["avisos"]
    url = landing.aemet_url(cfg, *chunks[0])
    assert url.endswith("/fechaini/2018-06-18T00:00:00UTC/fechafin/2018-07-18T23:59:59UTC")
    assert "api_key" not in url
    # AEMET answers 400 to more than 2 days per request.
    assert int(cfg["dias_por_peticion"]) <= 2
    lo, hi = landing.warning_chunks("2024-01-01", "2024-01-31", int(cfg["dias_por_peticion"]))[0]
    assert (hi - lo).days + 1 <= 2
