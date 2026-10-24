import json
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "etl", "jobs", "00_landing"))
import escritura_atomica as ea  # noqa: E402


def test_a_failure_writing_the_temporary_leaves_the_previous_file_untouched(tmp_path):
    target = tmp_path / "data.json"
    target.write_text(json.dumps({"previous": True}), encoding="utf-8")

    def full_disk(path):
        with open(path, "w", encoding="utf-8") as file:
            file.write('{"new":')
        raise OSError(28, "No space left on device")

    with pytest.raises(OSError):
        ea.replace_atomically(str(target), full_disk)
    assert json.loads(target.read_text(encoding="utf-8")) == {"previous": True}
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]


def test_a_refused_replace_keeps_the_previous_file_and_raises(tmp_path, monkeypatch):
    target = tmp_path / "data.json"
    target.write_text(json.dumps({"previous": True}), encoding="utf-8")
    calls = []

    def refuse(src, dst):
        calls.append(dst)
        raise PermissionError("held open")

    monkeypatch.setattr(os, "replace", refuse)
    monkeypatch.setattr(ea.time, "sleep", lambda s: None)
    with pytest.raises(PermissionError):
        ea.write_json_atomic(str(target), {"new": True})
    assert json.loads(target.read_text(encoding="utf-8")) == {"previous": True}
    assert len(calls) == 4
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]


def test_a_replace_refused_once_is_retried(tmp_path, monkeypatch):
    target = tmp_path / "data.json"
    real = os.replace
    state = {"n": 0}

    def flaky(src, dst):
        state["n"] += 1
        if state["n"] == 1:
            raise PermissionError("held open")
        real(src, dst)

    monkeypatch.setattr(os, "replace", flaky)
    monkeypatch.setattr(ea.time, "sleep", lambda s: None)
    ea.write_json_atomic(str(target), {"new": True})
    assert json.loads(target.read_text(encoding="utf-8")) == {"new": True}


def test_the_normal_path_replaces_the_file(tmp_path):
    target = tmp_path / "data.json"
    ea.write_json_atomic(str(target), {"new": 1})
    assert json.loads(target.read_text(encoding="utf-8")) == {"new": 1}
    assert os.listdir(tmp_path) == ["data.json"]
