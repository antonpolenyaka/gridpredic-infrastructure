"""
Test of the cause analysis (job_causas_incidencias) on a local Spark, with
the Gold tables of the network analysis test and a small
l2_silver.f_incidencia_causa built with the real cause rules.

    python -m pytest tests/04_ml/test_causas_incidencias.py -q
"""

import json
import os
import shutil
import sys
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
SILVER_DIR = os.path.join(ROOT, "etl", "jobs", "02_silver")
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

for folder in (ML_DIR, SILVER_DIR, os.path.dirname(__file__)):
    if folder not in sys.path:
        sys.path.insert(0, folder)

import causas_incidencia as ci  # noqa: E402
import test_analisis_red as base  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "cau_test"


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="cau_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("cau-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for db in ("l1_bronze", "l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()


def causes() -> pd.DataFrame:
    """The incidents of the network analysis test classified with the real rules, plus two old ones."""
    rows = []
    items = base.incidents() + [
        ("O1", 1, "201203", "CL_IMPRE", "FA_PROPI", "LAERE", "DISPARO LINEA VALDEFUENTES",
         [("A01", "2012-03-01 10:00", 1)]),
        ("O2", 1, "201205", "CL_IMPRE", "FA_CLIEN", "BAJAT", "Fusible fundido en caja de acometida", []),
    ]
    for inc, dist, period, cls, factor, equip, text, cts in items:
        start = min((pd.Timestamp(s) for _, s, _ in cts), default=pd.Timestamp(f"{period[:4]}-{period[4:]}-15 09:00"))
        n_ct = len(cts)
        level = "ct" if n_ct else "abonado"
        record = {"clasificacion_id": cls, "factor_id": factor, "tipo_equipo_id": equip, "casuistica": None,
                  "nivel": level, "fecha": start.strftime("%Y-%m-%d"), "descripcion": text}
        out = ci.classify(record)
        rows.append({
            "distribuidora_id": dist, "source_database": "Calser_EOSA" if dist == 1 else "Calser_Pitarch",
            "periodo_id": period, "id": inc, "inicio_ts": start, "clasificacion_id": cls, "factor_id": factor,
            "tipo_equipo_id": equip, "casuistica": None, "casuistica_nombre": None,
            # The 18 transformer failures of A03 start in the same CT.
            "instalacion_origen": "CT A03" if inc.startswith("S") else None, "instalacion_cabecera": None,
            "tipo_dia": None, "metodos_recogida": None, "n_interrupciones": max(n_ct, 1), "n_int_ct": n_ct,
            "n_elementos": max(n_ct, 1), "dur_int_max_s": 60 * max((m for _, _, m in cts), default=1),
            "n_int_mas_180s": sum(1 for _, _, m in cts if m * 60 > 180), "nivel_afectacion": level,
            "texto_causa": ci.normalize(text), "anio": start.year,
            **{k: out[k] for k in ("causa", "subcausa", "componente", "origen", "regla", "fuente_causa",
                                   "meteo_en_texto", "maniobra_reparacion")},
        })
    pdf = pd.DataFrame(rows)
    pdf["casuistica"] = pdf["casuistica"].astype("float")
    for column in ("casuistica_nombre", "instalacion_cabecera", "tipo_dia", "metodos_recogida", "componente"):
        pdf[column] = pdf[column].astype("string")
    return pdf


@pytest.fixture(scope="module")
def tables(spark):
    dim = base.dim_ct()
    base.save(spark, dim, "l3_gold.dim_ct")
    facts, incs = base.facts_and_incidents()
    base.save(spark, facts, "l3_gold.fact_interrupciones_mt")
    base.save(spark, incs, "l2_silver.f_incidencia")
    weather = base.weather()
    weather["met_temp_1h"] = 20.0
    # CAPE from 2024 only (as the archived GFS runs from 2021): high on the storm of 02.11.2024.
    weather["met_cape_1h"] = np.where(weather["hora"] >= pd.Timestamp("2024-01-01"), 50.0, np.nan)
    storm = (weather["hora"].dt.normalize() == pd.Timestamp("2024-11-02")) & (weather["celda_id"] == "c1")
    weather.loc[storm, "met_cape_1h"] = 2000.0
    base.save(spark, weather, "l3_gold.meteo_celda_hora")
    cells = dim[["distribuidora_id", "ct_id"]].assign(celda_id=dim["distribuidora_id"].map({1: "c1", 2: "c2"}))
    base.save(spark, cells, "l3_gold.map_ct_celda")
    base.save(spark, causes(), "l2_silver.f_incidencia_causa")
    # Public context: A01 - A04, the CTs with most incidents, have more overhead lines than A05 - A08.
    context = [{"distribuidora_id": d, "ct_id": ct, "anio": year,
                "celda_dens_linea_aerea_km_km2": 2.0 + 0.1 * i if i < 4 else 0.2 + 0.01 * i,
                "celda_pct_bosque": 0.1 * i, "celda_ciguena_parte_aves_5a": 0.01 * i, "celda_incendios_5a": 0.0}
               for i, (d, ct) in enumerate(zip(dim["distribuidora_id"], dim["ct_id"])) for year in range(2022, 2026)]
    base.save(spark, pd.DataFrame(context), "l3_gold.contexto_ct_anio")
    return dim


@pytest.fixture(scope="module")
def result(spark, tables, workdir):
    import job_causas_incidencias

    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config.setdefault("analisis_red", {}).update({"anio_desde": 2022, "anio_hasta": 2025})
    config["causas_incidencias"] = {"min_ct_anios": 10}
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)

    code = job_causas_incidencias.main(["--run-id", RUN_ID, "--config", path])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def read(folder, name):
    return pd.read_csv(os.path.join(folder, name))


def test_report_has_every_section(result):
    code, folder = result
    assert code == 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "Error en" not in text
    for number in range(1, 9):
        assert f"## {number}." in text


def test_causes_and_tiepi_weights(result):
    _, folder = result
    summary = read(folder, "causas_resumen.csv").set_index("causa")
    # I1 (wind and a branch) is vegetation, I4 (storm with lightning) weather, I2 third parties.
    assert summary.loc["vegetacion", "desde_2016"] == 1
    assert summary.loc["meteorologia", "desde_2016"] == 1
    assert summary.loc["terceros", "desde_2016"] == 1
    assert summary.loc["sin_causa", "hasta_2015"] == 1
    assert summary.loc["red_bt", "hasta_2015"] == 1
    assert summary["parte_tiepi"].sum() == pytest.approx(1.0)
    # The blackout is out of the regulated TIEPI shares.
    assert summary.loc["apagon", "kva_h_tiepi"] == 0
    full = read(folder, "incidencias_causa.csv")
    assert len(full) == len(causes())
    assert full.loc[full["id"] == "I1", "meteo_en_texto"].item()


def test_storm_days_raise_weather_incidents(result):
    _, folder = result
    table = read(folder, "causas_meteo.csv")
    row = table[(table["condicion"] == "racha >= 80 km/h") & (table["causa"] == "meteorologia")].iloc[0]
    assert row["dias_condicion"] == 2 and row["p_con"] == pytest.approx(0.5)
    assert row["parte_incidencias_en_condicion"] == pytest.approx(1.0)


def test_repeated_places(result):
    _, folder = result
    places = read(folder, "causas_lugares_repetidos.csv")
    top = places.iloc[0]
    assert top["lugar"] == "CT A03" and top["incidencias"] == 18


def test_summary_table_saved(result, spark):
    rows = spark.table("l3_gold.analisis_causas").where(f"run_id = '{RUN_ID}'").count()
    assert rows > 5


def test_cape_condition_only_where_cape_exists(result):
    _, folder = result
    table = read(folder, "causas_meteo.csv")
    rows = table[table["condicion"] == "CAPE >= 1000 J/kg"]
    weather = rows[rows["causa"] == "meteorologia"].iloc[0]
    # Only the days of 2024 and 2025 have CAPE; the storm day is the only one over 1000 J/kg.
    assert weather["dias_condicion"] == 1 and weather["p_con"] == pytest.approx(1.0)


def test_public_data_by_cause(result):
    _, folder = result
    table = read(folder, "causas_datos_publicos.csv")
    lines = table[(table["causa"] == "todas") & (table["variable"] == "celda_dens_linea_aerea_km_km2")]
    assert set(lines["banda"]) >= {"tercio_bajo", "tercio_alto"}
    # I1, I7 and the 18 failures of A03 fall on A01 - A04: more lines, more incidents.
    high = lines.loc[lines["banda"] == "tercio_alto", "p_anual"].item()
    low = lines.loc[lines["banda"] == "tercio_bajo", "p_anual"].item()
    assert high > low
    fires = table[(table["causa"] == "incendio") & (table["variable"] == "celda_incendios_5a")]
    assert set(fires["banda"]) == {"sin_datos"}
