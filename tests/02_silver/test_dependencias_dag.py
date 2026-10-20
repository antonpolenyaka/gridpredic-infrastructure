"""
The task graph of dag_silver and dag_gold comes from depends_on of
config_silver.json and config_gold.json, and the end to end tests run the
jobs in an order written by hand, so they pass even if the DAG puts a job
before the table it reads. This test reads the source of every job, without
Spark or Airflow: the tables it writes (write_table, saveAsTable,
write_rejected) and the ones it reads (silver_table("x"), gold_table("x"),
"l2_silver.x", f"{GOLD}.x" and the constants that hold them), and asks that
the job that writes every table read is upstream in depends_on; also that
every name exists, that there are no cycles and that dq_checks comes after
every other job. Gold runs the same check (tests/03_gold/test_dependencias_dag_gold.py).

What the CI does not cover: the DAG are never loaded by Airflow in a test,
nor the notebooks or the dashboard.

    python -m pytest tests/02_silver/test_dependencias_dag.py -q
"""

import ast
import json
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PREFIX = {"BRONZE": "l1_bronze", "SILVER": "l2_silver", "GOLD": "l3_gold"}
LAYERS = {
    "silver": ("02_silver", "job_silver_", "l2_silver"),
    "gold": ("03_gold", "job_gold_", "l3_gold"),
}


class Tables:
    """The tables one job writes and reads, from its source."""

    def __init__(self, path: str):
        with open(path, encoding="utf-8") as file:
            self.tree = ast.parse(file.read())
        self.constants = {}
        for node in self.tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                self.constants[node.targets[0].id] = node.value
        self.loops = self._loops()

    def _loops(self) -> dict:
        """{name: [expression, ...]} bound by a for over a literal tuple or list."""
        out = {}
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.For) or not isinstance(node.iter, (ast.Tuple, ast.List)):
                continue
            for item in node.iter.elts:
                if isinstance(node.target, ast.Name):
                    out.setdefault(node.target.id, []).append(item)
                elif isinstance(node.target, ast.Tuple) and isinstance(item, ast.Tuple):
                    for target, value in zip(node.target.elts, item.elts):
                        if isinstance(target, ast.Name):
                            out.setdefault(target.id, []).append(value)
        return out

    def strings(self, node, depth: int = 0) -> set:
        """Every value a string expression can take (constants, loops, f-strings with the layer names)."""
        if depth > 10:
            return set()
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return {node.value}
        if isinstance(node, ast.Name):
            if node.id in PREFIX:
                return {PREFIX[node.id]}
            if node.id in self.constants:
                return self.strings(self.constants[node.id], depth + 1)
            return set().union(*[self.strings(v, depth + 1) for v in self.loops.get(node.id, [])]) if node.id in self.loops else set()
        if isinstance(node, ast.JoinedStr):
            parts = [{""}]
            for value in node.values:
                options = self.strings(value.value if isinstance(value, ast.FormattedValue) else value, depth + 1)
                if not options:
                    return set()
                parts.append(options)
            out = {""}
            for options in parts:
                out = {a + b for a in out for b in options}
            return out
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.args:
            layer = {"silver_table": "l2_silver", "gold_table": "l3_gold"}.get(node.func.id)
            if layer:
                return {f"{layer}.{entity}" for entity in self.strings(node.args[0], depth + 1)}
        return set()

    @staticmethod
    def is_table(name: str) -> bool:
        return name.split(".")[0] in PREFIX.values() and name.count(".") == 1

    def written(self) -> set:
        out = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name == "write_table" and len(node.args) > 1:
                out |= self.strings(node.args[1])
            elif name == "saveAsTable" and node.args:
                out |= self.strings(node.args[0])
            elif name == "write_rejected" and len(node.args) > 1:
                out |= {f"l2_silver.{entity}_rejected" for entity in self.strings(node.args[1])}
        return {t for t in out if self.is_table(t)}

    def read(self) -> set:
        out = set()
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.Constant, ast.JoinedStr, ast.Call)):
                out |= {t for t in self.strings(node) if self.is_table(t)}
        return out - self.written()


def layer_graph(layer: str, config: dict = None):
    """(jobs of the config, {job: depends_on}, {job: written}, {job: read}) of a layer."""
    folder, prefix, _ = LAYERS[layer]
    if config is None:
        with open(os.path.join(ROOT, "etl", "config", folder, f"config_{layer}.json"), encoding="utf-8") as file:
            config = json.load(file)
    jobs = [job["name"] for job in config["jobs"]]
    depends = {job["name"]: list(job.get("depends_on", [])) for job in config["jobs"]}
    writes, reads = {}, {}
    for job in jobs:
        path = os.path.join(ROOT, "etl", "jobs", folder, f"{prefix}{job}.py")
        assert os.path.exists(path), f"{layer}: no file for the job {job}"
        tables = Tables(path)
        writes[job], reads[job] = tables.written(), tables.read()
    return jobs, depends, writes, reads


def upstream(job: str, depends: dict) -> set:
    seen, todo = set(), list(depends.get(job, []))
    while todo:
        name = todo.pop()
        if name not in seen:
            seen.add(name)
            todo += depends.get(name, [])
    return seen


def problems(layer: str, config: dict = None) -> list:
    """What is wrong with depends_on of a layer, as text; empty when the DAG is right."""
    jobs, depends, writes, reads = layer_graph(layer, config)
    out = []
    for job, names in depends.items():
        out += [f"{job} depends on {name}, which is not a job" for name in names if name not in depends]
    for job in jobs:
        if job in upstream(job, depends):
            out.append(f"{job} is in a cycle")
    writer = {table: job for job, tables in writes.items() for table in tables}
    for job in jobs:
        before = upstream(job, depends)
        for table in sorted(reads[job]):
            source = writer.get(table)
            if source and source != job and source not in before:
                out.append(f"{job} reads {table}, written by {source}, which is not upstream")
    if "dq_checks" in depends:
        missing = set(jobs) - {"dq_checks"} - upstream("dq_checks", depends)
        out += [f"dq_checks does not wait for {name}" for name in sorted(missing)]
    return out


def test_every_table_read_in_silver_is_written_upstream():
    assert problems("silver") == []
    jobs, _, writes, reads = layer_graph("silver")
    # The analysis sees what it has to see: the tables of contexto_publico come from loops and constants.
    assert "l2_silver.d_meteo_celda" in reads["contexto_publico"]
    assert {"l2_silver.f_corte_evento", "l2_silver.f_corte_elemento"} <= writes["f_corte"]
    assert "l2_silver.d_ct_scada" in writes["d_ct"]


def test_a_missing_dependency_is_found():
    with open(os.path.join(ROOT, "etl", "config", "02_silver", "config_silver.json"), encoding="utf-8") as file:
        config = json.load(file)
    # The configuration before 0.16.0: contexto_publico did not wait for d_meteo_celda.
    for job in config["jobs"]:
        if job["name"] == "contexto_publico":
            job["depends_on"] = [d for d in job.get("depends_on", []) if d != "d_meteo_celda"]
    found = problems("silver", config)
    assert any(p.startswith("contexto_publico reads l2_silver.d_meteo_celda") for p in found), found
