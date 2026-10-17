"""
What the weekly maintenance does with each Delta table (job_mantenimiento_delta.py):
OPTIMIZE and VACUUM, except dataset_train, which keeps the layout of its manual
compaction (one file per partition and the months of 2025 ordered with ZORDER)
and only gets VACUUM.

    python -m pytest tests/03_gold/test_mantenimiento_delta.py -q
"""

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "03_gold")
if path not in sys.path:
    sys.path.insert(0, path)

from job_mantenimiento_delta import actions, parse_args  # noqa: E402


def test_dataset_train_keeps_its_layout():
    args = parse_args([])
    assert actions("l3_gold.dataset_train", args) == ["vacuum"]
    assert actions("l3_gold.features_ct_hora", args) == ["optimize", "vacuum"]
    assert actions("l1_bronze.historictagintervalvaluesbig", args) == ["optimize", "vacuum"]


def test_flags():
    assert actions("l3_gold.dataset_train", parse_args(["--solo-vacuum", ""])) == ["optimize", "vacuum"]
    assert actions("l3_gold.features_ct_hora", parse_args(["--sin-vacuum"])) == ["optimize"]
    assert actions("l3_gold.dataset_train", parse_args(["--sin-vacuum"])) == []
    two = parse_args(["--solo-vacuum", "l3_gold.dataset_train, l3_gold.ml_predicciones"])
    assert actions("l3_gold.ml_predicciones", two) == ["vacuum"]
