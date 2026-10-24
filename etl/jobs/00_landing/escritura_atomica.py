"""
Atomic writes of the files the dashboard reads (dashboard/data): the content
goes to a temporary file in the same folder and os.replace puts it over the
target, so a page (or a copy) never reads a half written file. If the
replace is refused (a file held open on Windows), it is retried a few times
and, if it is still refused, the temporary is removed, the file in place is
kept as it was and the error goes up: never a half written file in place.
If the temporary itself cannot be
written (no space left on the device, for example) the error goes up and the
file in place is not touched. Only the standard library, so
the Landing jobs and the ML jobs share it (riesgo_zonas.py adds this folder
to sys.path).
"""

import json
import logging
import os
import time

logger = logging.getLogger("escritura_atomica")


def temporary_of(path: str) -> str:
    folder = os.path.dirname(os.path.abspath(path))
    return os.path.join(folder, f".{os.path.basename(path)}.{os.getpid()}.tmp")


def remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def replace_atomically(path: str, write, retries: int = 3, wait_s: float = 0.5) -> None:
    """write(target_path) writes the whole file; it goes to a temporary first and then replaces path."""
    temporary = temporary_of(path)
    try:
        write(temporary)
    except BaseException:
        # the temporary could not be written (no space left, a failing dump): the file in place stays as it was
        remove_quietly(temporary)
        raise
    for attempt in range(retries + 1):
        try:
            os.replace(temporary, path)
            return
        except OSError as error:
            if attempt == retries:
                # the previous file stays whole; whoever called knows it was not published
                logger.error("%s not replaced (%s): the previous file is kept", path, error)
                remove_quietly(temporary)
                raise
            logger.warning("%s not replaced yet (%s), retrying", path, error)
            time.sleep(wait_s)


def write_json_atomic(path: str, payload, **dump_args) -> None:
    """json.dump of payload to path, replaced atomically (dump_args go to json.dump)."""
    def write(target: str):
        with open(target, "w", encoding="utf-8") as file:
            json.dump(payload, file, **dump_args)
            file.flush()
            os.fsync(file.fileno())

    replace_atomically(path, write)
