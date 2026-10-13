"""
Test of the yearly risk per CT and feeder (job_ml_riesgo_ct) on a local Spark,
with a synthetic history where some feeders fail every year more than others.

    python -m pytest tests/04_ml/test_riesgo_ct.py -q
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
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

RUN_ID = "rct_test"
N_FEEDERS, CTS_PER_FEEDER = 6, 8


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="rct_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("rct-test")
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


def save(spark, pdf, table):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(table)


@pytest.fixture(scope="module")
def tables(spark):
    rng = np.random.default_rng(7)
    dim, upstream, facts, incidents, scada = [], [], [], [], []
    for f in range(N_FEEDERS):
        for c in range(CTS_PER_FEEDER):
            i = f * CTS_PER_FEEDER + c
            dim.append({
                "distribuidora_id": 1, "ct_id": f"C{i:03d}", "municipio_id": f"1000{f % 3}",
                "municipio_nombre": f"M{f % 3}", "potencia_tiepi_kva": 250.0, "potencia_kva": 250.0,
                "potencia_nominal_kva": 250.0, "num_abonados": 40, "tipo_zona_codigo": 3, "grupo_red_id": f"G{f}",
                "n_ct_grupo": CTS_PER_FEEDER, "trafo_elemento_id": 100 + i, "anchor_id": 1000 + i,
                "tiene_telemetria": True, "observable": True, "vigente_desde": pd.Timestamp("2014-01-01"),
                "vigente_hasta_excl": pd.NaT, "n_trafos_ct": 1, "en_estudio": True, "n_posiciones_aguas_arriba": 1,
            })
            upstream.append({"trafo_elemento_id": 100 + i, "elemento_corte_id": 9000 + f,
                             "elemento_corte_nombre": f"CAB-{f}", "n_cortes": 3, "posicion_id": 7000 + f})

    # Feeder f fails about f times a year; every fault takes half of its CTs.
    n = 0
    for year in range(2015, 2026):
        for f in range(N_FEEDERS):
            for k in range(rng.poisson(0.6 * f + 0.1)):
                n += 1
                start = pd.Timestamp(year=year, month=1, day=1) + pd.Timedelta(hours=int(rng.integers(0, 8000)))
                period = start.strftime("%Y%m")
                incidents.append({"distribuidora_id": 1, "periodo_id": period, "id": f"I{n}", "descripcion": "Averia",
                                  "observaciones": None, "resolucion": None, "tipo_equipo_id": "LAERE",
                                  "_feeder": f})
                hit = rng.choice(CTS_PER_FEEDER, CTS_PER_FEEDER // 2, replace=False)
                for c in hit:
                    minutes = int(rng.integers(20, 180))
                    ct = f"C{f * CTS_PER_FEEDER + c:03d}"
                    row = {"distribuidora_id": 1, "source_database": "Calser_EOSA", "ct_id": ct, "inicio_ts": start,
                           "fin_ts": start + pd.Timedelta(minutes=minutes), "duracion_s": minutes * 60,
                           "periodo_id": period, "incidencia_id": f"I{n}", "clasificacion_id": "CL_IMPRE",
                           "factor_id": "FA_PROPI", "tipo_evento_familia": "TI", "n_cts_incidencia": len(hit),
                           "es_origen_scada": False}
                    facts += [{**row, "variante": "principal"}, {**row, "variante": "todas"}]
                    if year >= 2021:
                        scada.append({"distribuidora_id": 1, "ct_id": ct, "inicio_ts": start - pd.Timedelta(days=30),
                                      "es_microcorte": True, "es_maniobra": False, "es_corte_etiqueta": False})

    # Causes by feeder: fauna in the first three, equipment in two, weather in the last; some upstream cuts.
    family = {0: "fauna", 1: "fauna", 2: "fauna", 3: "equipo", 4: "equipo", 5: "meteorologia"}
    causes = [{"distribuidora_id": 1, "periodo_id": inc["periodo_id"], "id": inc["id"],
               "causa": "aguas_arriba" if k % 7 == 0 else family[int(inc["_feeder"])]}
              for k, inc in enumerate(incidents)]
    for inc in incidents:
        inc.pop("_feeder")
    save(spark, pd.DataFrame(causes), "l2_silver.f_incidencia_causa")
    save(spark, pd.DataFrame(dim), "l3_gold.dim_ct")
    save(spark, pd.DataFrame(upstream), "l3_gold.map_aguas_arriba")
    save(spark, pd.DataFrame(facts), "l3_gold.fact_interrupciones_mt")
    save(spark, pd.DataFrame(incidents), "l2_silver.f_incidencia")
    save(spark, pd.DataFrame(scada), "l3_gold.fact_cortes_scada")
    context = pd.DataFrame([{"distribuidora_id": 1, "ct_id": d["ct_id"], "anio": y, "pob_municipio": 1000.0,
                             "celda_pct_bosque": 0.1 * (int(d["grupo_red_id"][1:]) % 3)}
                            for d in dim for y in range(2016, 2028)])
    save(spark, context, "l3_gold.contexto_ct_anio")
    return dim


@pytest.fixture(scope="module")
def result(spark, tables, workdir):
    import job_ml_riesgo_ct

    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config["riesgo_ct"] = {"anios_evaluacion": [2023, 2024, 2025],
                           "xgboost": {"n_estimators": 60, "max_depth": 3, "learning_rate": 0.1,
                                       "objective": "reg:tweedie", "tweedie_variance_power": 1.3},
                           "bloques": ["historia", "historia+estructura+scada", "todo"],
                           "bloques_por_causa": ["historia"]}
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)

    code = job_ml_riesgo_ct.main(["--run-id", RUN_ID, "--config", path])
    return code, os.path.join(workdir, "resultados", RUN_ID)


def test_run_and_outputs(result):
    code, folder = result
    assert code == 0
    for name in ("resumen.md", "captura.csv", "importancia.csv", "prioridades_ct.csv", "prioridades_cabecera.csv"):
        assert os.path.exists(os.path.join(folder, name)), name


def test_capture_is_a_share_and_beats_chance(result):
    _, folder = result
    metrics = pd.read_csv(os.path.join(folder, "captura.csv"))
    assert set(metrics["anio"]) == {2023, 2024, 2025}
    scored = metrics[metrics["modelo"] != "tasa_base"]
    assert scored["captura"].between(0, 1).all()
    both = scored.merge(metrics[metrics["modelo"] == "oraculo"], on=["anio", "nivel", "top"], suffixes=("", "_o"))
    assert (both["captura"] <= both["captura_o"] + 1e-9).all()
    # Feeders that fail every year: the top 20 % of the feeders holds far more than 20 % of the kVA·h.
    feeder = metrics[(metrics["nivel"] == "cabecera") & (metrics["modelo"] == "todo") & (metrics["top"] == 0.2)]
    assert feeder["captura"].mean() > 0.3


def test_ranking_for_next_year(result, spark):
    _, folder = result
    cts = pd.read_csv(os.path.join(folder, "prioridades_ct.csv"))
    assert set(cts["anio"]) == {2026}
    assert len(cts) == N_FEEDERS * CTS_PER_FEEDER
    feeders = pd.read_csv(os.path.join(folder, "prioridades_cabecera.csv"))
    # The feeders that fail more expect more kVA·h than the ones that fail less.
    expected = feeders.set_index("cabecera")["kva_h_esperado"]
    assert expected[["CAB-3", "CAB-4", "CAB-5"]].mean() > expected[["CAB-0", "CAB-1", "CAB-2"]].mean()
    assert spark.table("l3_gold.ml_riesgo_ct_ranking").where(f"run_id = '{RUN_ID}'").count() == len(cts)


def test_models_per_family_of_causes(result):
    _, folder = result
    metrics = pd.read_csv(os.path.join(folder, "captura.csv"))
    family = metrics[metrics["modelo"] == "por_causa:historia"]
    assert set(family["anio"]) == {2023, 2024, 2025}
    assert family["captura"].between(0, 1).all()
    feeder = family[(family["nivel"] == "cabecera") & (family["top"] == 0.2)]
    assert feeder["captura"].mean() > 0.3
    cts = pd.read_csv(os.path.join(folder, "prioridades_ct.csv"))
    expected = {"esperado_meteo", "esperado_fauna", "esperado_equipo", "esperado_resto"}
    assert expected <= set(cts.columns)
    feeders = pd.read_csv(os.path.join(folder, "prioridades_cabecera.csv")).set_index("cabecera")
    # Fauna is expected on the feeders where the fauna was, equipment where the equipment was.
    assert feeders.loc[["CAB-1", "CAB-2"], "esperado_fauna"].mean() > feeders.loc[["CAB-3", "CAB-4"], "esperado_fauna"].mean()
    assert feeders.loc[["CAB-3", "CAB-4"], "esperado_equipo"].mean() > feeders.loc[["CAB-1", "CAB-2"], "esperado_equipo"].mean()
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "por_causa:historia" in text
