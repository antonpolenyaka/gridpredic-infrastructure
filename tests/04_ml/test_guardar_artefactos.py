"""
store_artifacts of job_ml_train.py never stops the run: a model that cannot
be serialised or a store that cannot be written comes back as an error text,
and a model that can be saved lands with run.json. Without Spark (a local
folder needs no session).

    python -m pytest tests/04_ml/test_guardar_artefactos.py -q
"""

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "04_ml")
if path not in sys.path:
    sys.path.insert(0, path)

import job_ml_train  # noqa: E402


class Plain:
    params = {}


class NotPicklable:
    params = {}

    def __reduce__(self):
        raise TypeError("cannot pickle this scorer")


def test_a_scorer_that_cannot_be_pickled_is_an_error_not_a_crash(tmp_path):
    target, error = job_ml_train.store_artifacts({"logistica": NotPicklable()}, str(tmp_path), "r1", {"a": 1}, None)
    assert target is None and error.startswith("TypeError") and "cannot pickle" in error
    assert not os.path.exists(tmp_path / "r1" / "run.json")


def test_a_scorer_that_can_be_pickled_is_saved(tmp_path):
    target, error = job_ml_train.store_artifacts({"logistica": Plain()}, str(tmp_path), "r2", {"entorno": {"python": "3.10"}}, None)
    assert error is None and target == os.path.join(str(tmp_path), "r2")
    assert json.load(open(os.path.join(target, "run.json"), encoding="utf-8"))["entorno"]["python"] == "3.10"
    assert os.path.exists(os.path.join(target, "logistica.pkl"))
    assert job_ml_train.store_artifacts({"x": Plain()}, "", "r3", {}, None) == (None, None)
