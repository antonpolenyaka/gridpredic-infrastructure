"""
No block of variables of the yearly risk by CT (job_ml_riesgo_ct.py) may use
a column of gold_common.FORBIDDEN_FEATURES, the columns that look at the
future or at the label (n_posiciones_aguas_arriba counts the bays of the
whole history, future cuts included). Without Spark.

    python -m pytest tests/04_ml/test_riesgo_ct_bloques.py -q
"""

import os
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for folder in (("etl", "jobs", "04_ml"), ("etl", "jobs", "03_gold")):
    path = os.path.join(ROOT, *folder)
    if path not in sys.path:
        sys.path.append(path)

from gold_common import FORBIDDEN_FEATURES  # noqa: E402
import job_ml_riesgo_ct as rc  # noqa: E402


def grid_with_every_column() -> pd.DataFrame:
    """A grid with the columns of every fixed block, history and SCADA columns, and the forbidden ones."""
    fixed = [c for name, columns in rc.block_columns(pd.DataFrame()).items() for c in columns]
    named = fixed + ["kva_h_1a", "interrupciones_3a", "horas_1a", "tiepi_ct_1a", "anios_desde_ultima",
                     "microcortes_1a", "cortes_scada_1a", "episodios_scada_1a"] + sorted(FORBIDDEN_FEATURES)
    return pd.DataFrame(columns=list(dict.fromkeys(named)))


def test_no_block_uses_a_forbidden_column():
    blocks = rc.block_columns(grid_with_every_column())
    for name, columns in blocks.items():
        assert not set(columns) & FORBIDDEN_FEATURES, f"block {name} uses {sorted(set(columns) & FORBIDDEN_FEATURES)}"
    every = rc.features_of("todo", blocks)
    assert "n_posiciones_aguas_arriba" not in every and "kva" in every
    for name in rc.DEFAULTS["bloques"] + rc.DEFAULTS["bloques_por_causa"]:
        assert not set(rc.features_of(name, blocks)) & FORBIDDEN_FEATURES, name
