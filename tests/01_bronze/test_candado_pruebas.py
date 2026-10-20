"""
The lock of tests/conftest.py: while one process holds it, a second one
cannot take it, and it is free again when the first lets it go.

    python -m pytest tests/01_bronze/test_candado_pruebas.py -q
"""

import importlib.util
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
spec = importlib.util.spec_from_file_location("conftest_candado", os.path.join(ROOT, "tests", "conftest.py"))
candado = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candado)

OTHER = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location("c", sys.argv[1])
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
print("libre" if c.take_lock(sys.argv[2]) else "ocupado")
"""


def other_process(path: str) -> str:
    out = subprocess.run([sys.executable, "-c", OTHER, os.path.join(ROOT, "tests", "conftest.py"), path],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def test_a_second_battery_finds_the_lock_taken(tmp_path):
    path = str(tmp_path / "candado.lock")
    held = candado.take_lock(path)
    assert held is not None
    assert other_process(path) == "ocupado"
    held.close()
    assert other_process(path) == "libre"


def test_a_lock_that_cannot_be_opened_is_not_fatal(tmp_path):
    import pytest

    missing = str(tmp_path / "no_existe" / "candado.lock")
    with pytest.raises(candado.LockUnavailable):
        candado.take_lock(missing)
    assert candado.LOCKED_EXIT_CODE != 3 and os.path.basename(candado.LOCK_PATH).startswith("gridpredic_pytest_")
