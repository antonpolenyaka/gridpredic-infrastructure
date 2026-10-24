"""
The Linux stop scripts must not announce success when Docker or the Spark query fails
(audit of 24.10.2026, RV-01 and RV-02). The scripts run from a temporary copy with a fake
`docker` and a fake `curl` first in the PATH, so nothing real is stopped.

    python -m pytest tests/ops/test_scripts_exit_codes.py -q
"""

import os
import shutil
import stat
import subprocess

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

FAKE_DOCKER = """#!/usr/bin/env bash
echo "docker $*" >> "$FAKE_LOG"
case "$1" in
    info) exit 0 ;;
    stop) exit "${FAKE_STOP_RC:-0}" ;;
    rm) exit "${FAKE_RM_RC:-0}" ;;
    ps) if [ "${FAKE_DPS_RC:-0}" -ne 0 ]; then exit "$FAKE_DPS_RC"; fi; echo "gridpredic-neo4j"; exit 0 ;;
    compose)
        case "$*" in
            *" ps --status running"*) exit "${FAKE_PS_RC:-0}" ;;
            *" stop "*) exit "${FAKE_STOP_RC:-0}" ;;
            *" ps -a "*) echo "spark-master Up"; exit 0 ;;
        esac ;;
esac
exit 0
"""

# The stop refuses inside the evening window (20:15 - 21:00); the tests freeze the hour outside it unless they test it.
FAKE_DATE = """#!/usr/bin/env bash
if [ "$1" = "+%H%M" ]; then echo "${FAKE_HM:-1000}"; exit 0; fi
exec "$REAL_DATE" "$@"
"""

FAKE_CURL = """#!/usr/bin/env bash
if [ "${FAKE_CURL_RC:-0}" -ne 0 ]; then exit "$FAKE_CURL_RC"; fi
echo '{"activeapps": []}'
"""

pytestmark = pytest.mark.skipif(shutil.which("bash") is None or os.name == "nt",
                                reason="needs bash on Linux or macOS (the Windows scripts have their own checks)")


def run_script(tmp_path, name, args=(), **env_extra):
    work = tmp_path / "repo"
    bindir = tmp_path / "bin"
    work.mkdir()
    bindir.mkdir()
    shutil.copy(os.path.join(ROOT, name), work / name)
    (work / "compose.yaml").write_text("services: {}\n", newline="\n")
    for tool, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL), ("date", FAKE_DATE)):
        path = bindir / tool
        path.write_text(body, newline="\n")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = dict(os.environ, PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}", FAKE_LOG=str(log),
               REAL_DATE=shutil.which("date") or "/bin/date", **env_extra)
    result = subprocess.run(["bash", str(work / name), *args], env=env, capture_output=True, text=True, timeout=60)
    calls = log.read_text() if log.exists() else ""
    return result, calls


def test_stop_does_not_stop_when_spark_and_compose_ps_both_fail(tmp_path):
    result, calls = run_script(tmp_path, "stopGridPredic_linux.sh", FAKE_CURL_RC="7", FAKE_PS_RC="1")
    assert result.returncode == 1
    assert " stop " not in calls


def test_stop_goes_on_when_spark_does_not_answer_and_its_master_is_not_running(tmp_path):
    result, calls = run_script(tmp_path, "stopGridPredic_linux.sh", FAKE_CURL_RC="7", FAKE_PS_RC="0")
    assert result.returncode == 0
    assert " stop " in calls


def test_stop_fails_when_docker_stop_fails(tmp_path):
    result, calls = run_script(tmp_path, "stopGridPredic_linux.sh", FAKE_STOP_RC="7")
    assert result.returncode == 1
    assert " stop " in calls  # it failed in the stop, not before it
    assert "parado" not in result.stdout.split("ERROR")[-1]


def test_stop_is_clean_when_everything_works(tmp_path):
    result, calls = run_script(tmp_path, "stopGridPredic_linux.sh")
    assert result.returncode == 0 and " stop " in calls


def test_stop_refuses_inside_the_evening_window(tmp_path):
    result, calls = run_script(tmp_path, "stopGridPredic_linux.sh", FAKE_HM="2030")
    assert result.returncode != 0
    assert " stop " not in calls


def test_neo4j_stop_fails_when_docker_stop_fails(tmp_path):
    result, _ = run_script(tmp_path, "stopNeo4j_linux.sh", FAKE_STOP_RC="7")
    assert result.returncode == 1
    assert "Hecho" not in result.stdout


def test_neo4j_stop_with_borrar_fails_when_rm_fails(tmp_path):
    result, _ = run_script(tmp_path, "stopNeo4j_linux.sh", ("--borrar",), FAKE_RM_RC="7")
    assert result.returncode == 1
    assert "Hecho" not in result.stdout


def test_neo4j_stop_fails_when_docker_ps_fails(tmp_path):
    # Auditoria integral A21: a failed query is not "the container does not exist"
    result, calls = run_script(tmp_path, "stopNeo4j_linux.sh", FAKE_DPS_RC="71")
    assert result.returncode == 1
    assert "No existe" not in result.stdout and "docker stop" not in calls
