"""
Silver and Gold of the public data on a local Spark: job_silver_contexto_publico
and job_gold_contexto_ct over small Bronze tables built by hand.

    python -m pytest tests/03_gold/test_contexto_ct.py -q
"""

import importlib
import os
import shutil
import sys
import tempfile
import time

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in ("02_silver", "03_gold"):
    path = os.path.join(ROOT, "etl", "jobs", folder)
    if path not in sys.path:
        sys.path.insert(0, path)
GOLD_CONFIG = os.path.join(ROOT, "etl", "config", "03_gold", "config_gold.json")

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="module")
def spark():
    workdir = tempfile.mkdtemp(prefix="contexto_test_")
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("contexto-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for db in ("l1_bronze", "l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()
    shutil.rmtree(workdir, ignore_errors=True)


def save(spark, pdf, table):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(table)


def run(name, argv):
    module = importlib.import_module(name)
    old = sys.argv
    sys.argv = [f"{name}.py", "--run-id", "test_contexto", *argv]
    try:
        module.main()
    finally:
        sys.argv = old


@pytest.fixture(scope="module")
def built(spark):
    padron = []
    for year, a, b in ((2018, 1000, 500), (2019, 1010, 490), (2020, 1020, 480), (2021, 1030, 470),
                       (2022, 1040, 460), (2023, 1050, 450), (2024, 1060, 440)):
        for code, name, value in (("10001", "Uno", a), ("10002", "Dos", b), ("10003", "Valle de Uno", 100),
                                  ("10004", "Torno, El", 200), ("10037", "Cáceres", 900)):
            for sex, factor in (("Total", 1.0), ("Hombres", 0.5)):
                padron.append({"tabla_id": "2863", "serie_cod": f"S{code}{sex}", "serie_nombre": name,
                               "codigo_ine": code, "municipio": name, "provincia_codigo": "10", "sexo": sex,
                               "anio": year, "poblacion": value * factor, "secreto": False})
    # A province total without municipality and a repeated row.
    padron.append({**padron[0], "codigo_ine": None, "municipio": "Cáceres"})
    padron.append(dict(padron[0]))
    save(spark, pd.DataFrame(padron), "l1_bronze.ine_padron_municipio")

    save(spark, pd.DataFrame([
        {"codigo_ine": "10001", "nombre": "Uno", "area_km2": 50.0, "elevacion_m": 400.0, "wikidata_id": "Q1"},
        {"codigo_ine": "10002", "nombre": "Dos", "area_km2": 0.0, "elevacion_m": 600.0, "wikidata_id": "Q2"},
    ]), "l1_bronze.wikidata_municipio")

    save(spark, pd.DataFrame(
        [{"tipo": "celda", "clave": "c1", "punto": i, "latitud": 40.0, "longitud": -6.0, "elevacion_m": 400.0 + 10 * i}
         for i in range(5)]
        + [{"tipo": "municipio", "clave": "10001", "punto": 0, "latitud": 40.0, "longitud": -6.0, "elevacion_m": 410.0}]
    ), "l1_bronze.openmeteo_elevacion_punto")

    base = {"osm_tipo": "way", "longitud_km": None, "area_km2": None, "latitud": None, "longitud": None,
            "voltage": None, "n": None}
    save(spark, pd.DataFrame([
        {**base, "celda_id": "c1", "osm_id": 1, "clase": "power", "valor": "minor_line", "longitud_km": 8.0},
        {**base, "celda_id": "c1", "osm_id": 2, "clase": "power", "valor": "cable", "longitud_km": 2.0},
        {**base, "celda_id": "c1", "osm_id": 3, "clase": "power", "valor": "pole", "osm_tipo": "node"},
        {**base, "celda_id": "c1", "osm_id": 4, "clase": "power", "valor": "tower", "osm_tipo": "node"},
        {**base, "celda_id": "c1", "osm_id": 5, "clase": "landuse", "valor": "forest", "area_km2": 25.0},
        {**base, "celda_id": "c1", "osm_id": 6, "clase": "natural", "valor": "wood", "area_km2": 5.0},
        {**base, "celda_id": "c1", "osm_id": 0, "clase": "building", "valor": "count", "osm_tipo": "count", "n": 40},
    ]), "l1_bronze.osm_elemento_celda")
    # c3 was downloaded and has no power line at all: its shares are null, not an error.
    save(spark, pd.DataFrame([
        {"celda_id": "c1", "sur": 39.95, "oeste": -6.05, "norte": 40.05, "este": -5.95, "area_celda_km2": 100.0,
         "endpoint": "test", "osm_base": "2026-10-10", "n_elementos": 7},
        {"celda_id": "c3", "sur": 38.95, "oeste": -6.05, "norte": 39.05, "este": -5.95, "area_celda_km2": 100.0,
         "endpoint": "test", "osm_base": "2026-10-10", "n_elementos": 0},
    ]), "l1_bronze.osm_consulta_celda")

    save(spark, pd.DataFrame([
        {"distribuidora_id": 1, "ct_id": "A1", "municipio_id": "10001", "municipio_nombre": "Uno",
         "potencia_tiepi_kva": 400.0, "potencia_kva": 400.0, "num_abonados": 100},
        {"distribuidora_id": 1, "ct_id": "A2", "municipio_id": "10001", "municipio_nombre": "Uno",
         "potencia_tiepi_kva": 0.0, "potencia_kva": 250.0, "num_abonados": 50},
        {"distribuidora_id": 2, "ct_id": "B1", "municipio_id": "999", "municipio_nombre": "Dos",
         "potencia_tiepi_kva": 160.0, "potencia_kva": 160.0, "num_abonados": 40},
    ]), "l3_gold.dim_ct")
    save(spark, pd.DataFrame([
        {"distribuidora_id": 1, "ct_id": "A1", "codigo_ine": "10001", "celda_id": "c1"},
        {"distribuidora_id": 1, "ct_id": "A2", "codigo_ine": "10001", "celda_id": "c1"},
        {"distribuidora_id": 2, "ct_id": "B1", "codigo_ine": "00999", "celda_id": "c2"},
    ]), "l3_gold.map_ct_celda")

    tender = {"licitacion_id": None, "actualizado": None, "titulo": None, "enlace": None, "expediente": None,
              "estado": "ADJ", "organo": None, "organo_tipo": "3", "dir3": None, "nif": None, "organo_ciudad": None,
              "organo_cp": "10001", "organo_jerarquia": None, "objeto": None, "tipo_contrato": "3",
              "subtipo_contrato": None, "cpv": "45000000", "importe_sin_iva": 100000.0, "importe_estimado": None,
              "nuts": "ES432", "nuts_nombre": None, "lugar_ciudad": None, "lugar_cp": None, "inicio_previsto": None,
              "fin_previsto": None, "duracion": None, "duracion_unidad": None, "n_lotes": 0, "procedimiento": "1",
              "fecha_limite_ofertas": None, "resultado": None, "fecha_adjudicacion": None, "importe_adjudicado": None,
              "adjudicatario": None, "fecha_contrato": None, "fecha_primera_publicacion": None, "fichero_anio": 2022,
              "descargado_utc": "2026-10-11"}
    save(spark, pd.DataFrame([
        # 1: town council of Uno, published and then awarded; 3 months from the award plus one month.
        {**tender, "licitacion_id": "1", "actualizado": "2022-01-15T10:00:00.000+01:00", "estado": "PUB",
         "titulo": "Pavimentacion de la calle Mayor", "dir3": "L01100019", "cpv": "45233000", "duracion": 3.0,
         "duracion_unidad": "MON", "fecha_primera_publicacion": "2022-01-15"},
        {**tender, "licitacion_id": "1", "actualizado": "2022-03-02T10:00:00.000+01:00",
         "titulo": "Pavimentacion de la calle Mayor", "dir3": "L01100019", "cpv": "45233000", "duracion": 3.0,
         "duracion_unidad": "MON", "fecha_adjudicacion": "2022-03-01", "fecha_primera_publicacion": "2022-01-15"},
        # 2: Diputacion, two towns in the title ("Uno" is inside "Valle de Uno"), 12 months from 01.09.2022.
        {**tender, "licitacion_id": "2", "actualizado": "2022-06-01T10:00:00.000+01:00", "dir3": "L02000010",
         "organo_tipo": "2", "titulo": "Obras de abastecimiento en Valle de Uno y Dos", "inicio_previsto": "2022-09-01",
         "duracion": 12.0, "duracion_unidad": "MON", "fecha_primera_publicacion": "2022-06-01"},
        # 3: not works; 4: another region (the contractor is from here).
        {**tender, "licitacion_id": "3", "actualizado": "2022-07-01T10:00:00.000+01:00", "tipo_contrato": "1",
         "cpv": "39000000", "titulo": "Suministro de mobiliario"},
        {**tender, "licitacion_id": "4", "actualizado": "2022-07-01T10:00:00.000+01:00", "nuts": "ES618",
         "organo_cp": "41001", "titulo": "Obras en Sevilla"},
        # 5: cancelled by its state; 7: cancelled by a deleted entry.
        {**tender, "licitacion_id": "5", "actualizado": "2023-02-01T10:00:00.000+01:00", "estado": "ANUL",
         "dir3": "L01100019", "titulo": "Obras anuladas", "fichero_anio": 2023, "fecha_primera_publicacion": "2023-02-01"},
        {**tender, "licitacion_id": "7", "actualizado": "2023-03-01T10:00:00.000+01:00", "dir3": "L01100024",
         "titulo": "Obras borradas", "fichero_anio": 2023, "fecha_primera_publicacion": "2023-03-01"},
        # 6: Junta, "El Torno" in the title, no duration (6 months from publication plus three).
        {**tender, "licitacion_id": "6", "actualizado": "2023-05-10T10:00:00.000+02:00", "dir3": "A11000000",
         "organo_tipo": "2", "nuts": "ES43", "organo_cp": "06800", "titulo": "Mejora del camino de El Torno",
         "fichero_anio": 2023, "fecha_primera_publicacion": "2023-05-10"},
        # 8: no town in the title.
        {**tender, "licitacion_id": "8", "actualizado": "2023-06-01T10:00:00.000+02:00", "dir3": "L02000010",
         "titulo": "Obras en carreteras provinciales", "fichero_anio": 2023, "fecha_primera_publicacion": "2023-06-01"},
        # 9: the province after the town and the Diputacion are not the capital; 10: the capital itself.
        {**tender, "licitacion_id": "9", "actualizado": "2023-06-01T10:00:00.000+02:00", "dir3": "A11000000",
         "titulo": "Viviendas en Valle de Uno (Cáceres) del plan de la Diputación de Cáceres", "fichero_anio": 2023,
         "fecha_primera_publicacion": "2023-06-01"},
        {**tender, "licitacion_id": "10", "actualizado": "2023-06-01T10:00:00.000+02:00", "dir3": "A11000000",
         "titulo": "Colector de la ciudad de Cáceres", "fichero_anio": 2023, "fecha_primera_publicacion": "2023-06-01"},
    ]), "l1_bronze.placsp_licitacion_obra")
    save(spark, pd.DataFrame([{"licitacion_id": "7", "ref": "x/7", "borrado": "2023-03-05", "motivo": "ANULADA",
                               "fichero_anio": 2023}]), "l1_bronze.placsp_entrada_borrada")

    building = {"codigo_ine": "10001", "codigo_catastro": "10009", "estado": "functional", "inicio_construccion": None,
                "fin_construccion": None, "alta_version": None, "uso": "1_residential", "n_unidades": 1,
                "n_plantas": None, "x_utm": None, "y_utm": None, "latitud": None, "longitud": None,
                "descargado_utc": "2026-10-11"}
    save(spark, pd.DataFrame([
        {**building, "referencia": "R1", "anio_construccion": 1950, "n_viviendas": 2, "superficie_m2": 100.0},
        {**building, "referencia": "R2", "anio_construccion": 2015, "n_viviendas": 4, "superficie_m2": 400.0},
        {**building, "referencia": "R3", "anio_construccion": 2016, "n_viviendas": 1, "superficie_m2": 100.0},
        {**building, "referencia": "R4", "anio_construccion": 2017, "n_viviendas": 2, "superficie_m2": 200.0},
        {**building, "referencia": "R5", "anio_construccion": 2018, "n_viviendas": 3, "superficie_m2": 300.0,
         "uso": "3_industrial"},
        {**building, "referencia": "R6", "anio_construccion": 2020, "n_viviendas": 5, "superficie_m2": 500.0},
        {**building, "referencia": "R7", "anio_construccion": 2019, "n_viviendas": 10, "superficie_m2": 900.0,
         "estado": "ruin"},
        {**building, "referencia": "R2", "anio_construccion": 2014, "n_viviendas": 4, "superficie_m2": 400.0},
        {**building, "referencia": "R8", "anio_construccion": 1500, "n_viviendas": 1, "superficie_m2": 90.0},
        {**building, "codigo_ine": "10002", "referencia": "R9", "anio_construccion": 1990, "n_viviendas": 1,
         "superficie_m2": 80.0},
    ]), "l1_bronze.catastro_edificio")

    save(spark, pd.DataFrame([
        {"ame_codigo": "1012", "ame_nombre": "VALLE DEL JERTE", "anio": "2024", "poblacion": "1000.0"},
        {"ame_codigo": "1012", "ame_nombre": "VALLE DEL JERTE", "anio": "2039", "poblacion": "1010.0"},
        {"ame_codigo": "1012", "ame_nombre": "VALLE DEL JERTE", "anio": "2039", "poblacion": "1005.0"},
        {"ame_codigo": "1009", "ame_nombre": "AMBROZ", "anio": "2024", "poblacion": "-1"},
    ]), "l1_bronze.ieex_proyeccion_ame")
    save(spark, pd.DataFrame([
        {"codigo_ine": "10001", "municipio": "Uno", "ame_codigo": "1012", "ame_nombre": "VALLE DEL JERTE"},
        {"codigo_ine": "6148", "municipio": "Valle", "ame_codigo": "611", "ame_nombre": "SIERRA SUROESTE"},
    ]), "l1_bronze.ieex_ame_municipio")

    run("job_silver_contexto_publico", [])
    run("job_gold_contexto_ct", ["--config", GOLD_CONFIG])
    return spark


def test_population_in_silver(built):
    pop = built.table("l2_silver.f_poblacion_municipio").toPandas()
    assert len(pop) == 35
    assert pop.groupby(["codigo_ine", "anio"]).size().max() == 1
    assert built.table("l2_silver.f_poblacion_municipio_rejected").count() == 1


def test_municipalities_and_cells(built):
    towns = built.table("l2_silver.d_municipio_publico").toPandas().set_index("codigo_ine")
    assert towns.loc["10001", "area_km2"] == 50.0 and towns.loc["10001", "elevacion_centro_m"] == 410.0
    assert pd.isna(towns.loc["10002", "area_km2"])

    cell = built.table("l2_silver.d_celda_entorno").toPandas().set_index("celda_id").loc["c1"]
    assert bool(cell["osm_disponible"])
    assert cell["km_linea_aerea"] == pytest.approx(8.0) and cell["n_apoyos"] == 2
    assert cell["pct_bosque"] == pytest.approx(0.30) and cell["pct_cable"] == pytest.approx(0.2)
    assert cell["desnivel_m"] == pytest.approx(40.0) and cell["n_edificios"] == 40
    empty = built.table("l2_silver.d_celda_entorno").toPandas().set_index("celda_id").loc["c3"]
    assert empty["km_linea_aerea"] == 0 and pd.isna(empty["pct_cable"])


def test_context_uses_last_published_year(built):
    ctx = built.table("l3_gold.contexto_ct_anio").toPandas()
    a1 = ctx[(ctx["ct_id"] == "A1")].set_index("anio")
    # The row of 2024 uses the Padron of 1 January 2023.
    assert a1.loc[2024, "pob_municipio"] == 1050
    assert a1.loc[2024, "pob_crec_1a"] == pytest.approx(1050 / 1040 - 1)
    assert a1.loc[2024, "pob_crec_5a_anual"] == pytest.approx((1050 / 1000) ** 0.2 - 1)
    assert a1.loc[2024, "densidad_hab_km2"] == pytest.approx(1050 / 50)
    assert a1.loc[2024, "kva_municipio_por_hab"] == pytest.approx((400 + 250) / 1050)
    assert a1.loc[2024, "celda_dens_linea_aerea_km_km2"] == pytest.approx(0.08)
    assert a1.loc[2024, "cruce"] == "ine"
    assert pd.isna(a1.loc[2016, "pob_municipio"])

    b1 = ctx[(ctx["ct_id"] == "B1")].set_index("anio")
    # Code 00999 is not in the Padron: matched by name.
    assert b1.loc[2024, "cruce"] == "nombre" and b1.loc[2024, "pob_municipio"] == 450
    assert pd.isna(b1.loc[2024, "celda_dens_linea_aerea_km_km2"])


def test_works_tenders_in_silver(built):
    works = built.table("l2_silver.f_licitacion_obra").toPandas()
    rejected = built.table("l2_silver.f_licitacion_obra_rejected").toPandas()
    assert set(rejected["licitacion_id"]) == {"3", "4"}
    assert dict(zip(rejected["licitacion_id"], rejected["_motivo"])) == {"3": "NOT_WORKS", "4": "OUT_OF_TERRITORY"}

    one = works[works["licitacion_id"] == "1"].iloc[0]
    assert one["codigo_ine"] == "10001" and one["cruce"] == "dir3" and one["peso"] == 1.0 and one["versiones"] == 2
    assert one["estado"] == "ADJ" and bool(one["mov_tierras"]) and not bool(one["anulada"])
    assert str(one["fecha_publicacion"]) == "2022-01-15" and str(one["inicio_ejecucion"]) == "2022-03-31"
    assert str(one["fin_ejecucion"]) == "2022-06-30"

    two = works[works["licitacion_id"] == "2"].set_index("codigo_ine")
    assert set(two.index) == {"10002", "10003"} and (two["peso"] == 0.5).all() and (two["cruce"] == "titulo").all()
    assert str(two.loc["10003", "inicio_ejecucion"]) == "2022-09-01" and bool(two.loc["10003", "mov_tierras"])

    assert works.loc[works["licitacion_id"] == "6", "codigo_ine"].tolist() == ["10004"]
    assert bool(works.loc[works["licitacion_id"] == "5", "anulada"].iloc[0])
    assert bool(works.loc[works["licitacion_id"] == "7", "anulada"].iloc[0])
    assert works.loc[works["licitacion_id"] == "9", "codigo_ine"].tolist() == ["10003"]
    assert works.loc[works["licitacion_id"] == "10", "codigo_ine"].tolist() == ["10037"]
    eight = works[works["licitacion_id"] == "8"].iloc[0]
    assert eight["cruce"] == "sin_municipio" and pd.isna(eight["codigo_ine"]) and eight["provincia"] == "10"


def test_buildings_in_silver(built):
    towns = built.table("l2_silver.f_edificacion_municipio").toPandas()
    uno = towns[towns["codigo_ine"] == "10001"].set_index("anio")
    assert sorted(uno.index) == [1950, 2015, 2016, 2017, 2018, 2020]
    assert uno.loc[2015, "viviendas"] == 4 and uno.loc[2018, "superficie_industrial_m2"] == 300.0
    reasons = built.table("l2_silver.f_edificacion_municipio_rejected").toPandas()["_motivo"].value_counts()
    assert reasons.to_dict() == {"RUIN": 1, "INVALID_VALUE": 1, "DUPLICATE_ID": 1}


def test_works_and_buildings_by_municipality(built):
    act = built.table("l3_gold.obras_municipio_anio").toPandas()
    uno = act[act["codigo_ine"] == "10001"].set_index("anio")
    dos = act[act["codigo_ine"] == "10002"].set_index("anio")
    assert set(act["codigo_ine"]) == {"10001", "10002"}

    assert uno.loc[2022, "obras_publicadas"] == 1 and uno.loc[2022, "obras_publicadas_importe"] == 100000
    assert uno.loc[2023, "obras_publicadas"] == 0 and pd.isna(uno.loc[2021, "obras_publicadas"])
    # The execution needs the year before loaded: 2022 is unknown, 2023 is not.
    assert pd.isna(uno.loc[2022, "obras_en_ejecucion"]) and uno.loc[2023, "obras_en_ejecucion"] == 0
    assert dos.loc[2022, "obras_publicadas"] == 0.5 and dos.loc[2022, "obras_publicadas_mov_tierras"] == 0.5
    assert dos.loc[2023, "obras_en_ejecucion"] == 0.5
    assert dos.loc[2023, "obras_meses_ejecucion"] == pytest.approx(0.5 * 243 / 30.4)
    assert dos.loc[2023, "obras_previstas_meses"] == pytest.approx(0.5 * 243 / 30.4)
    assert dos.loc[2024, "obras_previstas_meses"] == 0 and pd.isna(dos.loc[2025, "obras_previstas_meses"])

    assert uno.loc[2018, "viviendas_nuevas"] == 3 and uno.loc[2019, "viviendas_nuevas"] == 0
    assert uno.loc[2018, "viviendas_parque"] == 12 and uno.loc[2020, "viviendas_parque"] == 17
    assert dos.loc[2020, "viviendas_parque"] == 1


def test_context_with_works_and_buildings(built):
    ctx = built.table("l3_gold.contexto_ct_anio").toPandas()
    a1 = ctx[ctx["ct_id"] == "A1"].set_index("anio")
    assert a1.loc[2023, "obras_publicadas_1a"] == 1 and a1.loc[2023, "obras_importe_1a"] == 100000
    assert a1.loc[2023, "obras_mov_tierras_1a"] == 1 and a1.loc[2024, "obras_publicadas_1a"] == 0
    assert pd.isna(a1.loc[2025, "obras_publicadas_1a"])
    assert a1.loc[2020, "viviendas_nuevas_3a"] == 6 and a1.loc[2020, "m2_nuevos_3a"] == 600
    assert a1.loc[2020, "viviendas_parque_2a"] == 12
    assert a1.loc[2020, "crec_viviendas_5a"] == pytest.approx((12 / 2) ** 0.2 - 1)
    assert a1.loc[2016, "viviendas_nuevas_3a"] == 0

    b1 = ctx[ctx["ct_id"] == "B1"].set_index("anio")
    assert b1.loc[2023, "obras_publicadas_1a"] == 0.5
    assert b1.loc[2023, "obras_previstas_meses"] == pytest.approx(0.5 * 243 / 30.4)


def test_ieex_projection_in_silver(built):
    projection = built.table("l2_silver.f_proyeccion_ame").toPandas().set_index(["ame_codigo", "anio"])
    assert projection.loc[("1012", 2039), "poblacion"] == 1010.0 and len(projection) == 2
    reasons = built.table("l2_silver.f_proyeccion_ame_rejected").toPandas()["_motivo"].value_counts().to_dict()
    assert reasons == {"INVALID_VALUE": 1, "DUPLICATE_ID": 1}
    towns = built.table("l2_silver.d_ame_municipio").toPandas().set_index("codigo_ine")
    assert towns.loc["06148", "ame_codigo"] == "0611" and towns.loc["10001", "ame_codigo"] == "1012"
