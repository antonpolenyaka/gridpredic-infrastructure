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
        for code, name, value in (("10001", "Uno", a), ("10002", "Dos", b)):
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
    save(spark, pd.DataFrame([
        {"celda_id": "c1", "sur": 39.95, "oeste": -6.05, "norte": 40.05, "este": -5.95, "area_celda_km2": 100.0,
         "endpoint": "test", "osm_base": "2026-10-10", "n_elementos": 7},
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

    run("job_silver_contexto_publico", [])
    run("job_gold_contexto_ct", ["--config", GOLD_CONFIG])
    return spark


def test_population_in_silver(built):
    pop = built.table("l2_silver.f_poblacion_municipio").toPandas()
    assert len(pop) == 14
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
