"""
spark-defaults.conf carries safety values (2 cores and 3 GB per executor)
for a job launched by hand without conf, which otherwise takes the 12 cores
of the worker with 1 GB per executor; the DAGs and the heavy jobs set their
own conf, and the examples of the README and of docs/gold-layer.md for the
heavy jobs carry it. The test reads the files, without Spark or Docker.

    python -m pytest tests/03_gold/test_spark_defaults.py -q
"""

import json
import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def read(*parts) -> str:
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as file:
        return file.read()


def test_the_defaults_have_safety_values():
    conf = {}
    for line in read("infra", "spark", "conf", "spark-defaults.conf").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition(" ")
            conf[key] = value.strip()
    assert conf["spark.cores.max"] == "2" and conf["spark.executor.memory"] == "3g"


def test_every_dag_job_states_its_cores_and_memory():
    for layer in ("02_silver", "03_gold"):
        config = json.loads(read("etl", "config", layer, f"config_{layer[3:]}.json"))
        assert {"spark.cores.max", "spark.executor.memory"} <= set(config["default_conf"]), layer
    assert '"spark.executor.memory": "6g"' in read("etl", "dags", "03_gold", "dag_mantenimiento_delta.py")
    ml = json.loads(read("etl", "config", "04_ml", "config_ml.json"))["spark_conf"]
    assert {"spark.cores.max", "spark.executor.memory"} <= set(ml)


def test_the_manual_examples_of_the_heavy_jobs_carry_their_conf():
    gold = json.loads(read("etl", "config", "03_gold", "config_gold.json"))
    heavy = next(j for j in gold["jobs"] if j["name"] == "features_ct_hora")["conf"]
    for text in (read("README.md"), read("docs", "gold-layer.md")):
        for block in re.findall(r"spark-submit[^`]*?job_gold_features_ct_hora\.py", text):
            assert f"--conf spark.executor.memory={heavy['spark.executor.memory']}" in block
            assert f"--conf spark.cores.max={heavy['spark.cores.max']}" in block
    readme = read("README.md")
    block = re.search(r"spark-submit[^`]*?job_silver_f_tag_interval_value\.py", readme).group(0)
    assert "--conf spark.executor.memory=8g" in block
