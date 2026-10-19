"""
The closure of the element hierarchy of dim_ct (hierarchy_closure) ends with
a cycle in ParentElementId, and element_scope still picks the nearest anchor
(the nearest CT above an element). A local Spark runs it.

    python -m pytest tests/03_gold/test_jerarquia_gold.py -q
"""

import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "03_gold")
if path not in sys.path:
    sys.path.insert(0, path)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[2]").appName("jerarquia-gold-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_the_closure_ends_with_a_cycle_and_the_anchor_is_the_nearest(spark, monkeypatch):
    import job_gold_dim_ct as dim

    monkeypatch.setattr(dim, "MAX_HIERARCHY_DEPTH", 6)
    ct, trafo = 10, 20
    params = {"ct": {"tipo_ct": ct, "tipo_trafo_ct": trafo}}
    rows = [
        (1, 1, None),          # root
        (2, ct, 1),            # a CT
        (3, ct, 2),            # a CT under the CT
        (4, 30, 3),            # an element under both: its anchor is 3, the nearest
        (5, 30, 6), (6, ct, 5),  # a cycle of two elements, one of them a CT
        (7, 30, 99),           # a parent that does not exist
    ]
    elements = spark.createDataFrame(rows, "id long, tipo_elemento_id long, parent_id long")
    closure = dim.hierarchy_closure(elements)
    levels = closure.groupBy("elemento_id").agg({"nivel": "max"}).collect()
    deepest = {r["elemento_id"]: r["max(nivel)"] for r in levels}
    assert deepest[4] == 3 and deepest[1] == 0
    # The cycle stops at the maximum depth instead of climbing for ever.
    assert deepest[5] == 6 and deepest[6] == 6
    assert deepest[7] == 0
    scope = {r["elemento_id"]: r for r in dim.element_scope(elements, closure, params).collect()}
    anchor = [c for c in scope[4].asDict() if "anchor" in c and "parent" not in c]
    assert anchor, scope[4]
    assert scope[4][anchor[0]] == 3
    assert scope[5][anchor[0]] == 6
