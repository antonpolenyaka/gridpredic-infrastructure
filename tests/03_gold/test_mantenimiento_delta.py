"""
What the weekly maintenance does with each Delta table (job_mantenimiento_delta.py):
OPTIMIZE and VACUUM, except dataset_train, which keeps the layout of its manual
compaction (one file per partition and the months of 2025 ordered with ZORDER),
and the big table of values of Bronze, written in Id order: both only get
VACUUM, and a failed OPTIMIZE does not leave a table without VACUUM. The DAG
gives the executor the memory of the manual compaction.

    python -m pytest tests/03_gold/test_mantenimiento_delta.py -q
"""

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "03_gold")
if path not in sys.path:
    sys.path.insert(0, path)

import ast  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402

import job_mantenimiento_delta as job  # noqa: E402
from job_mantenimiento_delta import actions, late_for_big, parse_args, run_steps  # noqa: E402


def to_snake():
    """to_snake of the Bronze job, taken from its source (the job starts Spark when it is imported)."""
    source = os.path.join(ROOT, "etl", "jobs", "01_bronze", "job_bronze_sqlserver_batch.py")
    with open(source, encoding="utf-8") as file:
        tree = ast.parse(file.read())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "to_snake")
    namespace = {"re": re}
    exec(compile(ast.Module(body=[node], type_ignores=[]), source, "exec"), namespace)
    return namespace["to_snake"]


def big_bronze_tables() -> list:
    """The catalog names of the chunked Bronze tables (chunk_size in config_bronze_sqlserver.json)."""
    snake = to_snake()
    with open(os.path.join(ROOT, "etl", "config", "01_bronze", "config_bronze_sqlserver.json"), encoding="utf-8") as file:
        config = json.load(file)
    return [f"l1_bronze.{snake(database)}_{snake(table['name'])}"
            for source in config["sources"] for database in source["databases"]
            for table in source["tables"] if table.get("chunk_size")]


def test_dataset_train_and_the_big_bronze_table_keep_their_layout():
    args = parse_args([])
    assert actions("l3_gold.dataset_train", args) == ["vacuum"]
    assert actions("l3_gold.features_ct_hora", args) == ["optimize", "vacuum"]
    big = big_bronze_tables()
    assert big == ["l1_bronze.tedis_net_eosa_historic_tag_interval_values_big"]
    assert actions(big[0], args) == ["vacuum"]
    # The short name of the first version of this test does not exist in the catalog.
    assert actions("l1_bronze.historictagintervalvaluesbig", args) == ["optimize", "vacuum"]


class FakeSpark:
    def __init__(self, failing: str):
        self.failing, self.statements = failing, []

    def sql(self, statement: str):
        self.statements.append(statement)
        if statement.startswith(self.failing):
            raise RuntimeError("Java heap space")


def test_a_failed_optimize_still_vacuums():
    spark = FakeSpark("OPTIMIZE")
    assert run_steps(spark, "l3_gold.features_ct_hora", ["optimize", "vacuum"], 168) == ["vacuum"]
    assert spark.statements == ["OPTIMIZE l3_gold.features_ct_hora", "VACUUM l3_gold.features_ct_hora RETAIN 168 HOURS"]
    assert run_steps(FakeSpark("VACUUM"), "x", ["optimize", "vacuum"], 168) == ["optimize"]


def test_the_dag_gives_memory_to_the_executor():
    with open(os.path.join(ROOT, "etl", "dags", "03_gold", "dag_mantenimiento_delta.py"), encoding="utf-8") as file:
        text = file.read()
    assert '"spark.executor.memory": "6g"' in text and '"spark.executor.memoryOverhead": "1g"' in text
    assert '"spark.cores.max": "4"' in text
    assert 'application_args=["--parar-a", "18", "--parar-grandes-a", "17"]' in text


def test_no_optimize_of_a_big_table_late_in_the_day():
    args = parse_args(["--parar-grandes-a", "17"])
    big, small = 12e9, 1e9
    assert late_for_big(["optimize", "vacuum"], big, 17, args) == ["vacuum"]
    assert late_for_big(["optimize", "vacuum"], big, 16, args) == ["optimize", "vacuum"]
    assert late_for_big(["optimize", "vacuum"], small, 17, args) == ["optimize", "vacuum"]
    assert late_for_big(["optimize", "vacuum"], big, 23, parse_args([])) == ["optimize", "vacuum"]


def test_flags():
    assert actions("l3_gold.dataset_train", parse_args(["--solo-vacuum", ""])) == ["optimize", "vacuum"]
    assert actions("l3_gold.features_ct_hora", parse_args(["--sin-vacuum"])) == ["optimize"]
    assert actions("l3_gold.dataset_train", parse_args(["--sin-vacuum"])) == []
    two = parse_args(["--solo-vacuum", "l3_gold.dataset_train, l3_gold.ml_predicciones"])
    assert actions("l3_gold.ml_predicciones", two) == ["vacuum"]


class SessionDouble:
    """Just enough of SparkSession for main(): builder chain, stop()."""

    class _Builder:
        def appName(self, _name):
            return self

        def enableHiveSupport(self):
            return self

        def getOrCreate(self):
            return SessionDouble()

    builder = _Builder()

    def stop(self):
        pass


def run_main(monkeypatch, ran_steps, files_error=False):
    monkeypatch.setattr(job, "SparkSession", SessionDouble)
    monkeypatch.setattr(job, "delta_tables", lambda spark, database: ["l1_bronze.una_tabla"])
    if files_error:
        def broken(spark, name):
            raise RuntimeError("DESCRIBE DETAIL failed")
        monkeypatch.setattr(job, "files_of", broken)
    else:
        monkeypatch.setattr(job, "files_of", lambda spark, name: (3, 1000))
    monkeypatch.setattr(job, "run_steps", lambda spark, name, steps, hours: ran_steps(steps))
    return job.main([])


def test_the_run_fails_when_a_step_fails(monkeypatch):
    assert run_main(monkeypatch, lambda steps: []) == 1
    assert run_main(monkeypatch, lambda steps: steps[:1]) == 1


def test_the_run_fails_when_a_table_cannot_be_read(monkeypatch):
    assert run_main(monkeypatch, lambda steps: steps, files_error=True) == 1


def test_the_run_is_clean_when_every_step_worked(monkeypatch):
    assert run_main(monkeypatch, lambda steps: steps) == 0
