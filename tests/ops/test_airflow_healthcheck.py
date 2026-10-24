"""
Segunda ronda de auditoría (A50): the healthcheck of the Airflow container reads the health JSON and does not take
an HTTP 200 as healthy when the scheduler or the metadatabase say otherwise. curl is replaced by a fake one that
prints the answer of each case.
"""
import json
import os
import shutil
import subprocess

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCRIPT = os.path.join(ROOT, "infra", "airflow", "healthcheck.sh")

pytestmark = pytest.mark.skipif(os.name == "nt" or not shutil.which("sh") or not shutil.which("python3"),
                                reason="needs sh and python3")


def run(tmp_path, body: str, curl_code: int = 0) -> int:
    fake = tmp_path / "bin"
    fake.mkdir(exist_ok=True)
    (tmp_path / "body.json").write_text(body, encoding="utf-8", newline="\n")
    curl = fake / "curl"
    curl.write_text(f'#!/bin/sh\ncat "{tmp_path / "body.json"}"\nexit {curl_code}\n', encoding="utf-8", newline="\n")
    curl.chmod(0o755)
    env = {**os.environ, "PATH": f"{fake}{os.pathsep}{os.environ.get('PATH', '')}"}
    return subprocess.run(["sh", SCRIPT], env=env, capture_output=True, text=True).returncode


def health(scheduler="healthy", database="healthy", processor="healthy", triggerer=None):
    return json.dumps({"metadatabase": {"status": database},
                       "scheduler": {"status": scheduler, "latest_scheduler_heartbeat": "2026-10-24T18:00:00+00:00"},
                       "triggerer": {"status": triggerer, "latest_triggerer_heartbeat": None},
                       "dag_processor": {"status": processor, "latest_dag_processor_heartbeat": None}})


def test_healthy_when_the_scheduler_and_the_database_are(tmp_path):
    assert run(tmp_path, health()) == 0
    # the dag-processor and the triggerer flap on this laptop and are not required
    assert run(tmp_path, health(processor="unhealthy", triggerer="unhealthy")) == 0


def test_not_healthy_when_the_scheduler_or_the_database_are_not(tmp_path):
    assert run(tmp_path, health(scheduler="unhealthy")) != 0
    assert run(tmp_path, health(database="unhealthy")) != 0
    assert run(tmp_path, json.dumps({"metadatabase": {"status": "healthy"}})) != 0


def test_a_json_that_does_not_parse_or_a_failed_request_is_not_healthy(tmp_path):
    assert run(tmp_path, "<html>not json</html>") != 0
    assert run(tmp_path, health(), curl_code=22) != 0
