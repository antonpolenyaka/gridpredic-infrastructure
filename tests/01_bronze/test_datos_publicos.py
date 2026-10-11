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
