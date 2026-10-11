"""
Test of the network analysis job (job_analisis_red) on a local Spark, with
small Gold and Silver tables built by hand so the main figures can be checked
against a calculation on paper.

    python -m pytest tests/04_ml/test_analisis_red.py -q
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

RUN_ID = "red_test"


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="red_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("red-test")
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


def save(spark, pdf: pd.DataFrame, table: str):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(table)


def dim_ct() -> pd.DataFrame:
    rows = []
    for i in range(1, 9):
        rows.append({
            "distribuidora_id": 1, "ct_id": f"A0{i}", "municipio_id": "10001" if i <= 4 else "10002",
            "municipio_nombre": "Uno" if i <= 4 else "Dos", "potencia_tiepi_kva": 250.0 if i <= 4 else 400.0,
            "potencia_kva": 250.0 if i <= 4 else 400.0, "potencia_nominal_kva": 250.0 if i <= 4 else 400.0,
            "num_abonados": 50, "tipo_zona_codigo": 3 if i <= 4 else 2, "grupo_red_id": "G1" if i <= 4 else "G2",
            "n_ct_grupo": 4, "trafo_elemento_id": 100 + i, "anchor_id": 1000 + i, "tiene_telemetria": True,
            "observable": True, "vigente_desde": pd.Timestamp("2015-01-01"), "vigente_hasta_excl": pd.NaT,
            "n_trafos_ct": 1, "en_estudio": True,
        })
    for i in range(1, 7):
        rows.append({
            "distribuidora_id": 2, "ct_id": f"B0{i}", "municipio_id": "10003", "municipio_nombre": "Tres",
            "potencia_tiepi_kva": 160.0, "potencia_kva": 160.0, "potencia_nominal_kva": 160.0, "num_abonados": 30,
            "tipo_zona_codigo": 3, "grupo_red_id": "G3", "n_ct_grupo": 6, "trafo_elemento_id": 200 + i,
            "anchor_id": 2000 + i, "tiene_telemetria": True, "observable": True,
            "vigente_desde": pd.Timestamp("2015-01-01"), "vigente_hasta_excl": pd.NaT, "n_trafos_ct": 1,
            "en_estudio": True,
        })
    return pd.DataFrame(rows)


def incidents():
    """(incident, distribuidora, period, class, factor, equipment, text, [(ct, start, minutes)])."""
    items = [
        ("I1", 1, "202303", "CL_IMPRE", "FA_PROPI", "LAERE", "Averia en linea por viento fuerte, rama caida",
         [("A01", "2023-03-10 10:00", 30), ("A02", "2023-03-10 10:00", 30),
          ("A03", "2023-03-10 10:00", 120), ("A04", "2023-03-10 10:00", 120)]),
        ("I2", 1, "202307", "CL_IMPRE", "FA_TERCE", "LSUBT", "Excavadora rompe cable subterraneo",
         [("A05", "2023-07-05 11:00", 60)]),
        ("I3", 1, "202402", "CL_PROGR", "FA_DISTR", "CTRAN", "Trabajos programados",
         [("A06", "2024-02-01 09:00", 120)]),
        ("I4", 1, "202411", "CL_IMPRE", "FA_FUMAY", "LAERE", "Tormenta con rayo",
         [(f"A0{i}", "2024-11-02 18:00", 30 * i) for i in range(1, 9)]),
        ("I5", 2, "202306", "CL_IMPRE", "FA_PROPI", "CTRAN", "Defecto en transformador",
         [("B01", "2023-06-01 08:00", 240)]),
        ("I6", 2, "202501", "CL_IMPRE", "FA_CLIEN", "OTROS", "Cliente", [("B02", "2025-01-10 12:00", 60)]),
        ("I7", 1, "202505", "CL_IMPRE", "FA_TRANS", "OTROS", "Falta de alimentacion en subestacion",
         [(f"A0{i}", "2025-05-20 07:00", 60) for i in range(1, 5)]),
        # The blackout of 28.04.2025: shown apart and left out of the rest.
        ("AP", 1, "202504", "CL_IMPRE", "FA_TERCE", "LAERE", "Cero general en la red de transporte",
         [(f"A0{i}", "2025-04-28 12:33", 600) for i in range(1, 9)]),
    ]
    for k in range(18):
        start = pd.Timestamp("2024-01-05 03:00") + pd.Timedelta(days=15 * k)
        items.append((f"S{k}", 1, start.strftime("%Y%m"), "CL_IMPRE", "FA_PROPI", "CTRAN", "Averia trafo",
                      [("A03", start.strftime("%Y-%m-%d %H:%M"), 60)]))
    return items


def facts_and_incidents():
    facts, rows = [], []
    for inc, dist, period, cls, factor, equip, text, cts in incidents():
        db = "Calser_EOSA" if dist == 1 else "Calser_Pitarch"
        start_inc = min(pd.Timestamp(s) for _, s, _ in cts)
        rows.append({"distribuidora_id": dist, "periodo_id": period, "id": inc, "descripcion": text,
                     "observaciones": None, "resolucion": None, "tipo_equipo_id": equip, "factor_id": factor,
                     "clasificacion_id": cls, "inicio_ts": start_inc})
        for ct, start, minutes in cts:
            start = pd.Timestamp(start)
            base = {
                "distribuidora_id": dist, "source_database": db, "ct_id": ct, "inicio_ts": start,
                "fin_ts": start + pd.Timedelta(minutes=minutes), "duracion_s": minutes * 60, "periodo_id": period,
                "incidencia_id": inc, "clasificacion_id": cls, "factor_id": factor, "tipo_evento_familia": "TI",
                "n_cts_incidencia": len(cts), "es_origen_scada": False,
            }
            facts.append({**base, "variante": "todas"})
            if cls == "CL_IMPRE" and factor != "FA_CLIEN":
                facts.append({**base, "variante": "principal"})

    # A CT interruption without incident: it never counts.
    start = pd.Timestamp("2025-05-05 10:00")
    facts.append({"variante": "todas", "distribuidora_id": 1, "source_database": "Calser_EOSA", "ct_id": "A07",
                  "inicio_ts": start, "fin_ts": start + pd.Timedelta(hours=2), "duracion_s": 7200,
                  "periodo_id": "202505", "incidencia_id": None, "clasificacion_id": None, "factor_id": None,
                  "tipo_evento_familia": None, "n_cts_incidencia": None, "es_origen_scada": False})
    return pd.DataFrame(facts), pd.DataFrame(rows)


def weather() -> pd.DataFrame:
    hours = pd.date_range("2023-01-01", "2025-12-31 23:00", freq="h")
    frames = []
    rng = np.random.default_rng(1)
    for cell in ("c1", "c2"):
        gust = rng.uniform(5, 35, len(hours))
        rain = np.zeros(len(hours))
        frames.append(pd.DataFrame({"celda_id": cell, "hora": hours, "met_racha_1h": gust, "met_lluvia_1h": rain}))
    pdf = pd.concat(frames, ignore_index=True)
    storm = pdf["hora"].dt.normalize().isin(pd.to_datetime(["2023-03-10", "2024-11-02"])) & (pdf["celda_id"] == "c1")
    pdf.loc[storm, "met_racha_1h"] = 95.0
    pdf.loc[storm, "met_lluvia_1h"] = 3.0
    return pdf


def measures():
    hours = pd.date_range("2023-01-01", "2025-12-31 23:00", freq="h")
    years = (hours.year - 2023).to_numpy()
    daily = np.sin((hours.hour.to_numpy() - 6) / 24 * 2 * np.pi).clip(0)
    rows = []
    # 1001: % load tag, growing 10 points a year, peak near 100 % in 2025.
    load = 55 + 10 * years + 30 * daily
    rows.append(pd.DataFrame({"ambito": "LOCAL", "clave_id": 1001, "familia": "carga_trafo", "hora": hours,
                              "n_muestras": 4, "n_tags": 1, "suma": 4 * load, "suma2": 4 * load ** 2, "max": load,
                              "min": load, "n_rancio": 0, "media_tag_max": load, "media_tag_min": load}))
    # 1002: total active and reactive power of A02 (250 kVA).
    p = 180 + 20 * daily
    q = 60 + 0 * daily
    for family, value in (("potencia_activa", p), ("potencia_reactiva", q)):
        rows.append(pd.DataFrame({"ambito": "LOCAL", "clave_id": 1002, "familia": family, "hora": hours,
                                  "n_muestras": 4, "n_tags": 1, "suma": 4 * value, "suma2": 4 * value ** 2,
                                  "max": value, "min": value, "n_rancio": 0, "media_tag_max": value,
                                  "media_tag_min": value}))
    # 1003: low voltage, three phases, some hours below 93 %.
    v = 0.23 - 0.02 * (daily > 0.95)
    rows.append(pd.DataFrame({"ambito": "LOCAL", "clave_id": 1003, "familia": "tension", "hora": hours,
                              "n_muestras": 12, "n_tags": 3, "suma": 12 * v, "suma2": 12 * v ** 2, "max": v + 0.001,
                              "min": v - 0.001, "n_rancio": 0, "media_tag_max": v, "media_tag_min": v}))
    # 7001: feeder bay of CAB-1, peak current growing 10 % a year.
    amps = 100 * (1.1 ** years) * (0.5 + 0.5 * daily)
    rows.append(pd.DataFrame({"ambito": "AGUAS_ARRIBA", "clave_id": 7001, "familia": "intensidad", "hora": hours,
                              "n_muestras": 12, "n_tags": 3, "suma": 12 * amps, "suma2": 12 * amps ** 2, "max": amps,
                              "min": amps, "n_rancio": 0, "media_tag_max": amps, "media_tag_min": amps}))
    tags = pd.DataFrame([
        {"anchor_id": 1001, "familia_medida": "carga_trafo", "clase_nombre": "AI.% CARGA TRANSFORMADOR",
         "tiene_serie": True},
        {"anchor_id": 1002, "familia_medida": "potencia_activa", "clase_nombre": "AI.TOTAL INSTANTANEOUS REAL POWER",
         "tiene_serie": True},
        {"anchor_id": 1002, "familia_medida": "potencia_reactiva",
         "clase_nombre": "AI.TOTAL INSTANTANEOUS REACTIVE POWER", "tiene_serie": True},
        *[{"anchor_id": 1003, "familia_medida": "tension", "clase_nombre": f"AI.VOLTAGE L{k}-N", "tiene_serie": True}
          for k in (1, 2, 3)],
    ])
    return pd.concat(rows, ignore_index=True), tags


@pytest.fixture(scope="module")
def tables(spark):
    dim = dim_ct()
    save(spark, dim, "l3_gold.dim_ct")
    facts, incs = facts_and_incidents()
    save(spark, facts, "l3_gold.fact_interrupciones_mt")
    save(spark, incs, "l2_silver.f_incidencia")
    upstream = pd.DataFrame(
        [{"trafo_elemento_id": 100 + i, "elemento_corte_id": 9001 if i <= 4 else 9002,
          "elemento_corte_nombre": "CAB-1" if i <= 4 else "CAB-2", "n_cortes": 5,
          "posicion_id": 7001 if i <= 4 else 7002} for i in range(1, 9)]
        + [{"trafo_elemento_id": 101, "elemento_corte_id": 9003, "elemento_corte_nombre": "CAB-X", "n_cortes": 1,
            "posicion_id": 7003}]
        + [{"trafo_elemento_id": 200 + i, "elemento_corte_id": 9004, "elemento_corte_nombre": "CAB-3", "n_cortes": 3,
            "posicion_id": 7004} for i in range(1, 7)]
    )
    save(spark, upstream, "l3_gold.map_aguas_arriba")
    save(spark, weather(), "l3_gold.meteo_celda_hora")
    cells = dim[["distribuidora_id", "ct_id"]].assign(celda_id=np.where(dim["distribuidora_id"] == 1, "c1", "c2"))
    save(spark, cells, "l3_gold.map_ct_celda")
    agg, tags = measures()
    save(spark, agg, "l3_gold.agg_medida_hora")
    save(spark, tags, "l3_gold.map_tag_ct")
    pes = pd.DataFrame({"distribuidora_id": dim["distribuidora_id"], "periodo_id": "202501", "id": dim["ct_id"],
                        "fecha_pes": pd.to_datetime(["1980-05-01"] * 4 + ["2015-05-01"] * 4 + ["2000-01-01"] * 6)})
    save(spark, pes, "l2_silver.d_ct")
    save(spark, works(), "l3_gold.obras_municipio_anio")
    return dim


def works() -> pd.DataFrame:
    """Uno grows (5 dwellings a year since 2015), Dos has a works that digs the street in 2023 (the year of I2)."""
    rows = []
    for code in ("10001", "10002", "10003"):
        stock = 100.0
        for year in range(2014, 2026):
            new = 5.0 if code == "10001" and year >= 2015 else 0.0
            stock += new
            digs = 4.0 if (code, year) == ("10002", 2023) else 0.0
            rows.append({
                "codigo_ine": code, "anio": year, "obras_publicadas": 1.0 if digs else 0.0,
                "obras_publicadas_importe": 50000.0 if digs else 0.0, "obras_publicadas_mov_tierras": 1.0 if digs else 0.0,
                "obras_en_ejecucion": (1.0 if digs else 0.0) if year >= 2019 else np.nan,
                "obras_meses_ejecucion": digs if year >= 2019 else np.nan,
                "obras_mov_tierras_meses": digs if year >= 2019 else np.nan,
                "obras_previstas_meses": (2.0 if digs else 0.0) if year >= 2019 else np.nan,
                "obras_previstas_mov_tierras_meses": (2.0 if digs else 0.0) if year >= 2019 else np.nan,
                "edificios_nuevos": new, "viviendas_nuevas": new, "m2_nuevos": 100.0 * new,
                "edificios_parque": stock, "viviendas_parque": stock,
            })
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config.setdefault("analisis_red", {}).update({
        "anio_desde": 2022, "anio_hasta": 2025, "anios_persistencia": [2024, 2025],
        "anios_incentivo": [2023, 2024, 2025], "min_horas_carga": 1000, "tokens_min": 1,
    })
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    return path


@pytest.fixture(scope="module")
def result(spark, tables, config_path, workdir):
    import job_analisis_red

    code = job_analisis_red.main(["--run-id", RUN_ID, "--config", config_path])
    folder = os.path.join(workdir, "resultados", RUN_ID)
    return code, folder


def read(folder, name):
    return pd.read_csv(os.path.join(folder, name))


def test_report_has_every_section(result):
    code, folder = result
    assert code == 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "Error en" not in text
    for number in range(1, 12):
        assert f"## {number}." in text


def test_tiepi_by_factor_matches_hand_calculation(result):
    _, folder = result
    table = read(folder, "tiepi_anual.csv")
    eosa = table[(table["distribuidora_id"] == 1) & (table["anio"] == 2023)]
    propias = eosa[(eosa["grupo"] == "imprevista") & (eosa["factor"] == "propias")].iloc[0]
    terceros = eosa[(eosa["grupo"] == "imprevista") & (eosa["factor"] == "terceros")].iloc[0]
    # I1: two CTs of 250 kVA for 0,5 h and two for 2 h; I2: 400 kVA for 1 h. Installed 4 x 250 + 4 x 400.
    assert propias["kva_h"] == pytest.approx(1250.0)
    assert terceros["kva_h"] == pytest.approx(400.0)
    assert propias["tiepi_h"] == pytest.approx(1250.0 / 2600.0)
    assert bool(propias["computa_incentivo"]) and not bool(terceros["computa_incentivo"])

    summary = read(folder, "tiepi_resumen.csv")
    row = summary[(summary["distribuidora"] == "Calser_EOSA") & (summary["anio"] == 2023)].iloc[0]
    assert row["tiepi_h"] == pytest.approx(1650.0 / 2600.0)
    assert row["tiepi_incentivo_h"] == pytest.approx(1250.0 / 2600.0)
    # The client incident of Pitarch and the interruption without incident stay out.
    pit = summary[(summary["distribuidora"] == "Calser_Pitarch") & (summary["anio"] == 2025)]
    assert pit.empty or pit.iloc[0]["tiepi_h"] == pytest.approx(0.0)
    no_incident = table[(table["grupo"] == "sin_incidencia") & (table["anio"] == 2025)]
    assert no_incident["kva_h"].sum() == pytest.approx(800.0)


def test_individual_limits_and_discounts(result):
    _, folder = result
    individual = read(folder, "calidad_individual.csv")
    a03 = individual[(individual["ct_id"] == "A03") & (individual["anio"] == 2024)].iloc[0]
    # 18 one hour interruptions plus 1,5 h of the storm: above 16 interruptions and 14 h.
    assert a03["n_interr"] == 19
    assert a03["horas"] == pytest.approx(19.5)
    assert a03["descuento_cliente_eur"] > 0
    assert a03["descuento_total_eur"] == pytest.approx(50 * a03["descuento_cliente_eur"])


def test_causes_from_text(result):
    _, folder = result
    causes = read(folder, "causas_texto.csv")
    found = set(causes["causa"])
    assert {"meteorologia", "terceros", "red_superior", "trabajos"} <= found
    words = read(folder, "causas_palabras.csv")
    assert "excavadora" in set(words["texto"])


def test_staged_restoration(result):
    _, folder = result
    yearly = read(folder, "reposicion_anual.csv")
    row = yearly[(yearly["distribuidora_id"] == 1) & (yearly["anio"] == 2023)].iloc[0]
    # A01 and A02 came back 90 minutes before A03 and A04.
    assert row["kva_h_maniobra"] == pytest.approx(250.0)
    feeders = read(folder, "economia_telemando_cabeceras.csv")
    assert "CAB-1" in set(feeders["cabecera"])
    assert "CAB-X" not in set(feeders["cabecera"])


def test_persistence_and_pareto(result):
    _, folder = result
    pareto = read(folder, "pareto.csv")
    assert set(pareto["nivel"]) == {"ct", "grupo_red", "cabecera", "municipio"}
    persistence = read(folder, "persistencia.csv")
    assert (persistence["captura"] <= persistence["oraculo"] + 1e-9).all()


def test_weather_dose_response(result):
    _, folder = result
    dose = read(folder, "dosis_respuesta.csv")
    gust = dose[dose["variable"] == "banda_racha"].set_index("banda")
    assert gust.loc["80-100", "p_incidencia"] > gust.loc["0-40", "p_incidencia"]


def test_transformer_load(result):
    _, folder = result
    trends = read(folder, "carga_trafo_tendencia.csv").set_index("anchor_id")
    assert trends.loc[1001, "fuente"] == "carga_trafo"
    assert trends.loc[1001, "pendiente_p95_anual"] == pytest.approx(0.10, abs=0.02)
    assert trends.loc[1001, "prioridad"] in ("ya_al_limite", "cerca_del_limite")
    # sqrt(200^2 + 60^2) / 250 at the daily peak.
    assert trends.loc[1002, "p99_ultimo"] == pytest.approx(np.hypot(200, 60) / 250, abs=0.03)
    voltage = read(folder, "tension_ct.csv")
    assert voltage["horas_bajo_93"].sum() > 0


def test_money_and_tables(result, spark):
    _, folder = result
    incentive = read(folder, "economia_incentivo.csv")
    assert set(incentive["distribuidora"]) >= {"Calser_EOSA"}
    assert incentive["eur_por_minuto_tiepi"].notna().all()
    assert spark.catalog.tableExists("l3_gold.analisis_red_tiepi")
    assert spark.table("l3_gold.analisis_red_tiepi").where(f"run_id = '{RUN_ID}'").count() > 0


def test_excluded_day_is_shown_apart(result):
    _, folder = result
    excluded = read(folder, "tiepi_dias_excluidos.csv")
    row = excluded[excluded["distribuidora"] == "Calser_EOSA"].iloc[0]
    # Every CT of EOSA (2.600 kVA) for 10 h.
    assert row["tiepi_h"] == pytest.approx(10.0)
    summary = read(folder, "tiepi_resumen.csv")
    eosa = summary[(summary["distribuidora"] == "Calser_EOSA") & (summary["anio"] == 2025)].iloc[0]
    assert eosa["tiepi_h"] == pytest.approx(1000.0 / 2600.0)
    causes = read(folder, "causas_texto.csv")
    assert "apagon" not in set(causes["causa"])


def test_feeder_currents_and_customers(result):
    _, folder = result
    feeders = read(folder, "carga_cabeceras_tendencia.csv").set_index("cabecera")
    assert feeders.loc["CAB-1", "crecimiento_anual"] == pytest.approx(0.10, abs=0.01)
    assert feeders.loc["CAB-1", "cts_alimentados"] == 4
    customers = read(folder, "cts_kva_por_abonado.csv")
    assert customers.iloc[0]["kva_por_abonado"] == pytest.approx(250.0 / 50.0)


def test_works_and_third_party_damage(result):
    _, folder = result
    panel = read(folder, "obras_terceros_panel.csv")
    # Three municipalities (one per distribuidora and town) and the years 2022 - 2025 of the analysis.
    assert len(panel) == 12 and set(panel["anio"]) == {2022, 2023, 2024, 2025}
    dos = panel[(panel["codigo_ine"] == 10002) & (panel["anio"] == 2023)].iloc[0]
    assert dos["obra_calle"] == 1 and dos["inc_terceros"] == 1 and dos["inc_maquinaria"] == 1
    assert dos["n_cts"] == 4 and dos["kva_h_terceros"] == pytest.approx(400.0)
    assert panel["inc_terceros"].sum() == 1  # the blackout of 28.04.2025 stays out
    uno = panel[(panel["codigo_ine"] == 10001) & (panel["anio"] == 2023)].iloc[0]
    assert uno["inc_propias"] == 1 and uno["obra_calle"] == 0

    bands = read(folder, "obras_terceros_bandas.csv").set_index("banda")
    assert bands.loc["hasta 6 meses", "terceros_por_100_ct"] == pytest.approx(25.0)
    assert bands.loc["0 (sin obras que abren calle)", "terceros"] == 0
    models = read(folder, "obras_terceros_poisson.csv")
    assert {"inc_terceros", "inc_propias"} <= set(models["incidencias"])

    growth = read(folder, "crecimiento_municipios.csv")
    uno = growth[growth["codigo_ine"] == 10001].iloc[0]
    # 25 dwellings in 2020 - 2024 at 1000 kVA / 200 customers: 125 kVA, 12,5 % of the installed.
    assert uno["viviendas_nuevas_5a"] == 25 and uno["kva_nuevos_5a"] == pytest.approx(125.0)
    assert uno["kva_nuevos_5a_pct"] == pytest.approx(0.125) and uno["crec_viviendas_10a"] == pytest.approx(150 / 100 - 1)
