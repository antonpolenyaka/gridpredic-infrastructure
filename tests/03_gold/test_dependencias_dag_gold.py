"""
The same check as tests/02_silver/test_dependencias_dag.py for dag_gold:
every table a Gold job reads is written by a job upstream in depends_on of
config_gold.json, without cycles and with dq_checks at the end. Without
Spark or Airflow.

    python -m pytest tests/03_gold/test_dependencias_dag_gold.py -q
"""

import importlib.util
import json
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
spec = importlib.util.spec_from_file_location("dependencias_dag", os.path.join(ROOT, "tests", "02_silver", "test_dependencias_dag.py"))
deps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deps)


def test_every_table_read_in_gold_is_written_upstream():
    assert deps.problems("gold") == []
    _, _, writes, reads = deps.layer_graph("gold")
    assert {"l3_gold.dim_ct", "l3_gold.map_tag_ct", "l3_gold.map_aguas_arriba"} <= writes["dim_ct"]
    assert "l3_gold.features_ct_hora" in reads["dataset_train"]


def test_dq_checks_has_to_wait_for_every_table():
    with open(os.path.join(ROOT, "etl", "config", "03_gold", "config_gold.json"), encoding="utf-8") as file:
        config = json.load(file)
    # Before 0.16.0 dq_checks did not wait for dataset_train, meteo_celda_hora and contexto_ct.
    for job in config["jobs"]:
        if job["name"] == "dq_checks":
            job["depends_on"] = [d for d in job["depends_on"] if d not in ("dataset_train", "meteo_celda_hora", "contexto_ct")]
    found = deps.problems("gold", config)
    assert "dq_checks does not wait for meteo_celda_hora" in found and "dq_checks does not wait for contexto_ct" in found
