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

import getpass
import os
import tempfile
import warnings

import pytest

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 3g pyspark-shell"

def lock_name() -> str:
    """One lock per user, so a file left by another account never blocks this one."""
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - no user name: a shared lock
        user = "todos"
    return os.path.join(tempfile.gettempdir(), f"gridpredic_pytest_{''.join(c for c in user if c.isalnum()) or 'todos'}.lock")


LOCK_PATH = lock_name()
# 3 is INTERNAL_ERROR in pytest; this exit code is ours.
LOCKED_EXIT_CODE = 9
_lock = None


class LockUnavailable(Exception):
    """The lock file cannot be opened at all (permissions): the battery goes on without it."""


def take_lock(path: str = LOCK_PATH):
    """The open lock file, or None when another process holds the lock; LockUnavailable if it cannot be opened."""
    try:
        handle = open(path, "a+")
    except OSError as error:
        raise LockUnavailable(str(error)) from error
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
    try:
        _lock = take_lock()
    except LockUnavailable as error:
        warnings.warn(f"Sin candado de pruebas ({error}): no se comprueba si hay otra batería en marcha.", stacklevel=1)
        return
    if _lock is None:
        pytest.exit(f"Ya hay otra batería de pruebas en marcha en esta máquina (candado {LOCK_PATH}): "
                    "dos a la vez arrancan dos Spark locales de 3 GB. Espera a que acabe "
                    "o pon GRIDPREDIC_SIN_CANDADO=1 si sabes lo que haces.", returncode=LOCKED_EXIT_CODE)
