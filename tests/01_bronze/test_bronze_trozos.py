"""
The bounds of every chunk of the chunked JDBC read of Bronze (chunk_plan of
job_bronze_sqlserver_batch.py). The job parses its arguments and starts Spark
when it is imported, so the test takes the function from the source, without
Spark or SQL Server.

    python -m pytest tests/01_bronze/test_bronze_trozos.py -q
"""

import ast
import math
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JOB = os.path.join(ROOT, "etl", "jobs", "01_bronze", "job_bronze_sqlserver_batch.py")


def load(name: str):
    with open(JOB, encoding="utf-8") as file:
        tree = ast.parse(file.read())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = {"math": math}
    exec(compile(ast.Module(body=[node], type_ignores=[]), JOB, "exec"), namespace)
    return namespace[name]


CHUNK = 25_000_000
FILE_ROWS = 5_000_000


def test_the_last_value_is_read_and_the_bound_is_that_of_the_rows():
    plan = load("chunk_plan")
    # A whole chunk keeps its end; the last one ends at the highest value plus one, which still enters.
    assert plan(0, CHUNK, 3 * CHUNK, 4, FILE_ROWS) == (CHUNK, 4)
    upper, _ = plan(2 * CHUNK, 3 * CHUNK, 2 * CHUNK + 1_000_000, 4, FILE_ROWS)
    assert upper == 2 * CHUNK + 1_000_001
    assert 2 * CHUNK + 1_000_000 < upper


def test_connections_follow_the_rows_of_the_chunk():
    plan = load("chunk_plan")
    assert plan(0, CHUNK, 10 * CHUNK, 4, FILE_ROWS)[1] == 4        # 25 million -> 4
    assert plan(0, CHUNK, 999_999, 4, FILE_ROWS)[1] == 1           # 1 million -> 1
    assert plan(0, CHUNK, 11_999_999, 4, FILE_ROWS)[1] == 3        # 12 million -> 3
    # Without a file size or a number of partitions it reads as before.
    assert plan(0, CHUNK, 999_999, 4, None)[1] == 4
    assert plan(0, CHUNK, 999_999, None, FILE_ROWS)[1] == 1
