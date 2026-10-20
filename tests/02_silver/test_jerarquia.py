"""
The climb up the element hierarchy of d_elemento (resolve_distributor):
ParentElementId is not guaranteed to be acyclic and may point to an element
that does not exist, so a cycle of two elements ends as PROFUNDIDAD_EXCEDIDA
after the maximum depth, an unknown parent as PADRE_INEXISTENTE, and the
other elements resolve as before. The job runs when it is imported, so the
function is taken from its source; a local Spark runs it.

    python -m pytest tests/02_silver/test_jerarquia.py -q
"""

import ast
import logging
import os

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JOB = os.path.join(ROOT, "etl", "jobs", "02_silver", "job_silver_d_elemento.py")


def resolve_distributor(max_depth: int):
    """resolve_distributor of the job with its constants, and a smaller maximum depth for a quick test."""
    with open(JOB, encoding="utf-8") as file:
        tree = ast.parse(file.read())
    keep = [n for n in tree.body if isinstance(n, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == "DISTRIBUTOR_ELEMENT_TYPE_ID" for t in n.targets)]
    keep += [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "resolve_distributor"]
    namespace = {"col": F.col, "lit": F.lit, "DataFrame": DataFrame, "logger": logging.getLogger("test"),
                 "MAX_HIERARCHY_DEPTH": max_depth}
    exec(compile(ast.Module(body=keep, type_ignores=[]), JOB, "exec"), namespace)
    return namespace["resolve_distributor"], namespace["DISTRIBUTOR_ELEMENT_TYPE_ID"]


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("jerarquia-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_a_cycle_and_a_missing_parent_do_not_change_the_rest(spark):
    resolve, distributor = resolve_distributor(max_depth=6)
    rows = [
        (1, distributor, None),   # the distribuidora
        (2, 10, 1), (3, 20, 2),   # a CT and an element under it: resolved
        (4, 20, 5), (5, 20, 4),   # a cycle of two elements
        (6, 20, 99),              # a parent that does not exist
        (7, 10, None),            # a root that is not a distribuidora
    ]
    elements = spark.createDataFrame(rows, "id long, tipo_elemento_id long, parent_id long")
    out = {r["id"]: r for r in resolve(elements, elements).collect()}
    assert sorted(out) == [1, 2, 3, 4, 5, 6, 7]
    assert out[1]["estado_resolucion"] == "RESUELTO" and out[1]["profundidad"] == 0
    assert out[3]["estado_resolucion"] == "RESUELTO" and out[3]["distribuidora_id"] == 1 and out[3]["profundidad"] == 2
    for cycled in (4, 5):
        assert out[cycled]["estado_resolucion"] == "PROFUNDIDAD_EXCEDIDA" and out[cycled]["distribuidora_id"] is None
        assert out[cycled]["profundidad"] == 6
    assert out[6]["estado_resolucion"] == "PADRE_INEXISTENTE" and out[6]["distribuidora_id"] is None
    assert out[7]["estado_resolucion"] == "RAIZ_NO_DISTRIBUIDORA"
