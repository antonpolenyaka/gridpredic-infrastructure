"""
Settings shared by every test that starts a local Spark.

The JVM of PySpark takes its heap from PYSPARK_SUBMIT_ARGS when it starts,
not from the SparkSession builder (in a process with several sessions the
first one fixes it for all). With the default of 1 GB the Gold tests run
out of heap now and then on the CI runners (dim_ct builds and broadcasts
several tables), so the heap is raised here before any session exists; a
value already in the environment is respected.

Two batteries at once on the laptop start two local Sparks of 3 GB each,
next to the virtual machine of Docker, and the second one can take the
memory of the evening run. A lock of the operating system in the temporary
folder (msvcrt on Windows, fcntl elsewhere, released when the process ends,
even if it dies) makes a second battery stop at once with a message.
GRIDPREDIC_SIN_CANDADO=1 turns it off.
"""

import os
import tempfile

import pytest

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 3g pyspark-shell"

LOCK_PATH = os.path.join(tempfile.gettempdir(), "gridpredic_pytest.lock")
_lock = None


def take_lock(path: str = LOCK_PATH):
    """The open lock file, or None when another process holds the lock."""
    handle = open(path, "a+")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def pytest_configure(config):
    global _lock
    if os.environ.get("GRIDPREDIC_SIN_CANDADO") == "1" or hasattr(config, "workerinput"):
        return
    _lock = take_lock()
    if _lock is None:
        pytest.exit(f"Ya hay otra batería de pruebas en marcha en esta máquina (candado {LOCK_PATH}): "
                    "dos a la vez arrancan dos Spark locales de 3 GB. Espera a que acabe "
                    "o pon GRIDPREDIC_SIN_CANDADO=1 si sabes lo que haces.", returncode=3)
