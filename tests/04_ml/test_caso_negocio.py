"""
Test of the replay of the daily operation (job_ml_caso_negocio) on the
synthetic Gold of the risk of tomorrow: the days of 2024 and 2025 scored by
a model that only saw the years before, with the reanalysis and with the
forecast, the incidents with their CT cuts, the crews and the outputs
(tables, CSVs, historico.json).

    python -m pytest tests/04_ml/test_caso_negocio.py -q
"""

import json
import os
import sys

import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import job_ml_caso_negocio as cn  # noqa: E402
from test_riesgo_manana import config_path, spark, tables, workdir  # noqa: E402,F401

RUN_ID = "caso_test"


@pytest.fixture(scope="module")
def replayed(spark, tables, config_path, workdir):
    with open(config_path, encoding="utf-8") as file:
        config = json.load(file)
    config["caso_negocio"] = {"anios": [2024, 2025], "brigadas": {"1": [1, 2], "2": [1]}, "movilizacion_min": 30.0,
                              "recorte_min": 110.0}
    path = os.path.join(workdir, "config_caso.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    code = cn.main(["--run-id", RUN_ID, "--config", path, "--landing", os.path.join(workdir, "vivo")])
    return code, config


def test_the_replay_scores_every_day_with_the_past_only(replayed, spark):
    code, config = replayed
    assert code == 0
    days = spark.table("l3_gold.caso_negocio_dia").toPandas()
    assert set(days["modo"]) == {"reanalisis", "prevision"}
    assert sorted(days["anio"].unique()) == [2024, 2025]
    one = days[(days["modo"] == "reanalisis") & (days["distribuidora_id"] == 1)]
    # Two years, minus the last days whose label is not known yet (the margin after the last incident of Calser).
    assert 2 * 365 - 14 <= len(one) <= 2 * 365 + 1
    assert set(one["nivel"]) <= {"bajo", "medio", "alto", "muy alto"}
    # The storms of the synthetic data (gusts of 85 - 110 km/h with incidents) are the days in alert.
    assert one["alerta"].mean() < 0.3
    incident_days = one[one["incidencias"] > 0]
    assert len(incident_days) > 10
    assert incident_days["alerta"].mean() > one["alerta"].mean()
    assert (incident_days["clientes_hora"] > 0).all()
    # The cut hours never exceed the raw ones and only differ on days with a cut longer than the cap.
    assert (one["clientes_hora_recortadas"] <= one["clientes_hora"] + 1e-6).all()
    assert (one.loc[one["corte_max_min"] <= 110, "clientes_hora_recortadas"] == one.loc[one["corte_max_min"] <= 110, "clientes_hora"]).all()
    # With the forecast, the same days are scored (the synthetic forecast exists from 2024).
    two = days[(days["modo"] == "prevision") & (days["distribuidora_id"] == 1)]
    assert len(two) == len(one)
    assert (days["probabilidad"].between(0, 1)).all()


def test_detection_and_false_alarms_of_the_policies():
    days = pd.DataFrame({
        "distribuidora_id": [1] * 20, "anio": [2024] * 20, "fecha": [f"2024-01-{i + 1:02d}" for i in range(20)],
        "probabilidad": [0.9, 0.8, 0.7, 0.6] + [0.05] * 16, "alerta": [True] * 4 + [False] * 16,
        "incidencias": [2, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "clientes_hora_recortadas": [500.0, 100.0, 0, 0, 50.0] + [0.0] * 15, "clientes_hora": [500.0, 100.0, 0, 0, 50.0] + [0.0] * 15,
        "aviso_max_nivel": [2, 0, 1, 0, 0] + [0] * 15, "aviso_viento_nivel": [2] + [0] * 19, "aviso_tormenta_nivel": [0, 0, 1] + [0] * 17,
    })
    table = cn.detection_table(days).set_index("política")
    alert = table.loc["GridPredic en alerta (alto o muy alto)"]
    # Three days with an incident, two of them in alert; of the 17 other days, two are in alert.
    assert alert["detección: día con alguna incidencia"] == pytest.approx(2 / 3, abs=0.01)
    assert alert["falsas alarmas: día con alguna incidencia"] == pytest.approx(2 / 17, abs=0.001)
    assert alert["detección: día con dos incidencias o más"] == 1.0 and alert["parte de días marcados"] == 0.2
    both = table.loc["aviso de AEMET de viento o tormenta, amarillo o más"]
    assert both["parte de días marcados"] == 0.1 and both["detección: día con alguna incidencia"] == pytest.approx(1 / 3, abs=0.01)
    orange = table.loc["aviso de AEMET naranja o rojo"]
    assert orange["detección: día con alguna incidencia"] == pytest.approx(1 / 3, abs=0.01) and orange["falsas alarmas: día con alguna incidencia"] == 0.0
    assert table.attrs["positivos"]["los 10 peores días del año por horas-cliente"] == 3


def test_the_summary_the_files_and_the_history(replayed, spark, workdir):
    summary = spark.table("l3_gold.caso_negocio_resumen").toPandas()
    first = summary[(summary["distribuidora_id"] == 1) & (summary["modo"] == "reanalisis")]
    assert sorted(first["brigadas"].unique()) == [1.0, 2.0] and sorted(first["anio"].unique()) == [2024, 2025]
    row = first[(first["brigadas"] == 2.0) & (first["anio"] == 2024)].iloc[0]
    assert row["incidencias"] >= row["incidencias_en_alerta"] >= 0
    assert row["clientes_hora"] >= row["clientes_hora_en_alerta"] >= 0
    assert row["cortes_ct"] >= row["cortes_ct_esperaron_brigada"] >= 0
    assert row["dias_alerta"] >= row["dias_alerta_con_incidencia"]
    assert first[(first["brigadas"] == 1.0)]["llegada_fija_media_min"].iloc[0] >= first[(first["brigadas"] == 2.0)]["llegada_fija_media_min"].iloc[0]
    folder = os.path.join(workdir, "resultados", RUN_ID)
    for name in ("resumen.md", "dias.csv", "cts.csv", "resumen_anual.csv", "historico.json"):
        assert os.path.getsize(os.path.join(folder, name)) > 0
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert "## tiempo del día del reanálisis" in text and "### Brigadas" in text and "### Por nivel previsto (" in text
    assert "horas-cliente (recortadas)" in text and "| sin recortar |" in text
    assert "| probabilidad media |" in text and "Probabilidad de la víspera frente a los días con avería: AUC 0." in text
    assert "### Frente a los avisos de AEMET" in text and "| GridPredic en alerta (alto o muy alto) |" in text
    assert "### Detección y falsas alarmas por día" in text and "| detección: día con alguna incidencia |" in text
    cts = pd.read_csv(os.path.join(folder, "cts.csv"))
    assert (cts["min_ganados_total"].abs() <= cts["dur_min"] + 1e-9).all()
    assert cts.loc[~cts["espero_brigada"], "min_ganados_total"].eq(0).all()
    history = json.load(open(os.path.join(workdir, "dashboard", "historico.json"), encoding="utf-8"))
    assert [d["id"] for d in history["distribuidoras"]] == [1, 2]
    first_days = history["distribuidoras"][0]["dias"]["prevision"]
    days = spark.table("l3_gold.caso_negocio_dia").toPandas()
    assert "aviso_max_nivel" in days.columns and {"a", "av", "at"} <= set(first_days[0])
    assert len(first_days) > 700 and {"f", "n", "p", "i", "ch", "chr", "c", "m", "b"} <= set(first_days[0])
    assert all(d["chr"] <= d["ch"] for d in first_days)
    assert any(d["b"] for d in first_days if d["n"] in ("alto", "muy alto"))
    runs = spark.table("l3_gold.ml_runs").toPandas()
    assert RUN_ID in set(runs["run_id"])
