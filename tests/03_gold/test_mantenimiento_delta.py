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
    assert 'application_args=["--parar-a", "18", "--parar-grandes-a", "17",' in text


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
    monkeypatch.setattr(job, "delta_tables", lambda spark, database, errors=None: ["l1_bronze.una_tabla"])
    if files_error:
        def broken(spark, name):
            raise RuntimeError("DESCRIBE DETAIL failed")
        monkeypatch.setattr(job, "files_of", broken)
    else:
        monkeypatch.setattr(job, "files_of", lambda spark, name: (3, 1000))
    monkeypatch.setattr(job, "run_steps", lambda spark, name, steps, hours, big=False, stop_hour=None: ran_steps(steps))
    return job.main([])


def test_the_run_fails_when_a_step_fails(monkeypatch):
    assert run_main(monkeypatch, lambda steps: []) == 1
    assert run_main(monkeypatch, lambda steps: steps[:1]) == 1


def test_the_run_fails_when_a_table_cannot_be_read(monkeypatch):
    assert run_main(monkeypatch, lambda steps: steps, files_error=True) == 1


def test_the_run_is_clean_when_every_step_worked(monkeypatch):
    assert run_main(monkeypatch, lambda steps: steps) == 0


class CatalogDouble:
    def __init__(self, error):
        self.error = error

    def databaseExists(self, _database):
        return True

    def listTables(self, _database):
        class Table:
            name = "una_tabla"
            tableType = "MANAGED"
        return [Table()]


class DescribeDouble:
    """spark.sql('DESCRIBE DETAIL ...') that fails with the given error."""

    def __init__(self, error):
        self.catalog = CatalogDouble(error)
        self.error = error

    def sql(self, _statement):
        raise RuntimeError(self.error)


def test_a_table_that_cannot_be_described_counts_as_a_failure():
    errors = []
    assert job.delta_tables(DescribeDouble("MinIO unavailable while describing Delta table"), "l1_bronze", errors) == []
    assert errors == ["l1_bronze.una_tabla"]


def test_a_table_that_is_not_delta_is_discarded_without_failure():
    errors = []
    assert job.delta_tables(DescribeDouble("`x` is not a Delta table."), "l1_bronze", errors) == []
    assert errors == []


def test_the_run_fails_when_discovery_fails(monkeypatch):
    monkeypatch.setattr(job, "SparkSession", SessionDouble)
    monkeypatch.setattr(SessionDouble, "catalog", CatalogDouble("x"), raising=False)
    monkeypatch.setattr(SessionDouble, "sql", lambda self, statement: (_ for _ in ()).throw(RuntimeError("MinIO unavailable")), raising=False)
    assert job.main([]) == 1


def test_a_retry_of_the_same_day_goes_on_after_the_tables_already_done(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(job, "SparkSession", SessionDouble)
    monkeypatch.setattr(job, "delta_tables", lambda spark, database, errors=None:
                        ["l1_bronze.a", "l1_bronze.b", "l1_bronze.c"] if database == "l1_bronze" else [])
    monkeypatch.setattr(job, "files_of", lambda spark, name: (3, 1000))

    def first(spark, name, steps, hours, big=False, stop_hour=None):
        seen.append(name)
        if name == "l1_bronze.b":
            raise SystemExit("executor lost")
        return steps
    monkeypatch.setattr(job, "run_steps", first)
    try:
        job.main(["--progreso", str(tmp_path)])
    except SystemExit:
        pass
    assert seen == ["l1_bronze.a", "l1_bronze.b"]
    seen.clear()
    monkeypatch.setattr(job, "run_steps", lambda spark, name, steps, hours, big=False, stop_hour=None: seen.append(name) or steps)
    assert job.main(["--progreso", str(tmp_path)]) == 0
    assert seen == ["l1_bronze.b", "l1_bronze.c"]


def test_without_progress_folder_nothing_is_skipped(tmp_path):
    assert job.done_today(None, "2026-10-25") == set()
    job.mark_done(None, "2026-10-25", "x")
    job.mark_done(str(tmp_path), "2026-10-25", "l2_silver.x")
    assert job.done_today(str(tmp_path), "2026-10-25") == {"l2_silver.x"}
    assert job.done_today(str(tmp_path), "2026-10-26") == set()


class PartitionedSpark:
    def __init__(self, columns, values):
        self.columns, self.values, self.statements = columns, values, []

    def sql(self, statement):
        self.statements.append(statement)
        outer = self

        class Result:
            def first(self):
                return {"partitionColumns": outer.columns}

            def collect(self):
                return [(v,) for v in outer.values]
        return Result()


def test_a_big_partitioned_table_is_optimized_one_partition_at_a_time():
    spark = PartitionedSpark(["mes"], ["2025-02", "2025-01"])
    assert run_steps(spark, "l2_silver.grande", ["optimize", "vacuum"], 168, big=True) == ["optimize", "vacuum"]
    assert [s for s in spark.statements if s.startswith(("OPTIMIZE", "VACUUM"))] == [
        "OPTIMIZE l2_silver.grande WHERE `mes` = '2025-01'", "OPTIMIZE l2_silver.grande WHERE `mes` = '2025-02'",
        "VACUUM l2_silver.grande RETAIN 168 HOURS"]


def test_a_big_table_without_partitions_only_gets_vacuum():
    spark = PartitionedSpark([], [])
    assert run_steps(spark, "l2_silver.grande", ["optimize", "vacuum"], 168, big=True) == ["optimize", "vacuum"]
    assert [s for s in spark.statements if s.startswith(("OPTIMIZE", "VACUUM"))] == ["VACUUM l2_silver.grande RETAIN 168 HOURS"]


def test_the_dag_resumes_and_tolerates_long_pauses():
    with open(os.path.join(ROOT, "etl", "dags", "03_gold", "dag_mantenimiento_delta.py"), encoding="utf-8") as file:
        text = file.read()
    assert '"--progreso"' in text and '"spark.network.timeout": "600s"' in text and '"retries": 2' in text


def test_the_partitions_of_a_big_table_stop_at_the_late_hour(monkeypatch):
    spark = PartitionedSpark(["fecha_mes"], ["2025-01", "2025-02"])

    class Late:
        @staticmethod
        def now(tz=None):
            import datetime as real
            return real.datetime(2026, 10, 25, 17, 5)
    monkeypatch.setattr(job, "datetime", Late)
    assert run_steps(spark, "l2_silver.f_tag_interval_value", ["optimize", "vacuum"], 168, big=True, stop_hour=17) == ["vacuum"]
    assert [s for s in spark.statements if s.startswith(("OPTIMIZE", "VACUUM"))] == [
        "VACUUM l2_silver.f_tag_interval_value RETAIN 168 HOURS"]


def test_a_failed_month_does_not_stop_the_other_months():
    class OneMonthFails(PartitionedSpark):
        def sql(self, statement):
            if statement.endswith("'2025-01'"):
                self.statements.append(statement)
                raise RuntimeError("executor lost")
            return super().sql(statement)
    spark = OneMonthFails(["fecha_mes"], ["2025-01", "2025-02"])
    assert run_steps(spark, "l2_silver.f_tag_interval_value", ["optimize", "vacuum"], 168, big=True) == ["vacuum"]
    assert "OPTIMIZE l2_silver.f_tag_interval_value WHERE `fecha_mes` = '2025-02'" in spark.statements
