"""
The tables of the lakehouse stay readable by Trino. Its Delta Lake connector
supports a single table feature, deletion vectors ("No other features are
supported", Trino 483 documentation, checked on 17.10.2026): a job that turned
on liquid clustering, row tracking or v2 checkpoints would leave the table
out of reach of Trino and of every query of the dashboard and of the memoria
(4.7.3). The test reads the jobs and the configuration, without Spark.

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


def test_no_job_turns_on_a_table_feature_that_trino_cannot_read():
    found = []
    for path in sources():
        with open(path, encoding="utf-8", errors="replace") as file:
            text = file.read()
        for pattern, what in FORBIDDEN.items():
            if re.search(pattern, text):
                found.append(f"{os.path.relpath(path, ROOT)}: {what}")
    assert not found, "Table features Trino cannot read: " + "; ".join(found)


def test_the_patterns_catch_what_they_should():
    assert re.search(list(FORBIDDEN)[0], "CREATE TABLE t (a INT) USING delta CLUSTER BY (a)")
    assert re.search(list(FORBIDDEN)[1], "df.write.clusterBy('ct_id')")
    assert re.search(list(FORBIDDEN)[4], "'delta.feature.rowTracking' = 'supported'")
    assert not re.search(list(FORBIDDEN)[4], "'delta.feature.deletionVectors' = 'supported'")
