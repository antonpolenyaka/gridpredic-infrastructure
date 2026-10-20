"""
The tables of the lakehouse keep to the Delta table features that Trino
supports. Its Delta Lake connector lists column mapping, deletion vectors,
timestamp without time zone and the vacuum protocol check for readers and
writers; Iceberg compatibility, type widening and v2 checkpoints for readers
only; and append-only, invariants, CHECK constraints and change data feed for
writers only ("No other features are supported", Trino 484 documentation; the
stack runs 483). Liquid clustering and row tracking are writer features:
Trino would still read such a table, but could not write to it. We sort with
ZORDER and not with liquid clustering for another reason: clustering is not
compatible with partitioning, and dataset_train is partitioned by split and
fecha_mes. The test is stricter than the connector: it fails if a job turns
on any table feature other than deletion vectors. It reads the jobs and the
configuration, without Spark.

    python -m pytest tests/03_gold/test_compatibilidad_trino.py -q
"""

import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FORBIDDEN = {
    r"\bCLUSTER\s+BY\b": "liquid clustering (SQL)",
    r"\.clusterBy\(": "liquid clustering (DataFrameWriter)",
    r"enableRowTracking": "row tracking",
    r"checkpointPolicy|v2Checkpoint": "checkpoints v2",
    r"delta\.feature\.(?!deletionVectors)": "another table feature",
}


def sources():
    for folder in ("etl", "infra"):
        for base, _, files in os.walk(os.path.join(ROOT, folder)):
            if "__pycache__" in base:
                continue
            for name in files:
                if name.endswith((".py", ".json", ".sql", ".conf", ".properties", ".yaml", ".yml")):
                    yield os.path.join(base, name)


def test_no_job_turns_on_a_table_feature_other_than_deletion_vectors():
    found = []
    for path in sources():
        with open(path, encoding="utf-8", errors="replace") as file:
            text = file.read()
        for pattern, what in FORBIDDEN.items():
            if re.search(pattern, text):
                found.append(f"{os.path.relpath(path, ROOT)}: {what}")
    assert not found, "Table features other than deletion vectors: " + "; ".join(found)


def test_the_patterns_catch_what_they_should():
    assert re.search(list(FORBIDDEN)[0], "CREATE TABLE t (a INT) USING delta CLUSTER BY (a)")
    assert re.search(list(FORBIDDEN)[1], "df.write.clusterBy('ct_id')")
    assert re.search(list(FORBIDDEN)[4], "'delta.feature.rowTracking' = 'supported'")
    assert not re.search(list(FORBIDDEN)[4], "'delta.feature.deletionVectors' = 'supported'")
