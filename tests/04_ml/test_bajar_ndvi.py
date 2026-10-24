"""Segunda ronda de auditoría (A43): resuming the NDVI download never adds an older or a second revision of a composite."""
import os
import sys

import pytest

pytest.importorskip("rasterio")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "experimentos", "fenologia"))
import bajar_ndvi  # noqa: E402


def item(production: str, day: str = "001", tile: str = "h17v05") -> dict:
    start = {"001": "2024-01-01T00:00:00Z", "017": "2024-01-17T00:00:00Z"}[day]
    return {"id": f"MOD13Q1.A2024{day}.{tile}.061.{production}", "properties": {"start_datetime": start}}


def test_the_latest_revision_of_the_catalogue_and_nothing_again_once_saved():
    old, new = item("2024020000000"), item("2024200000000")
    other = item("2024020000000", day="017")
    assert [i["id"] for i in bajar_ndvi.pending_composites([new, old, other], set())] == [new["id"], other["id"]]
    # the first run saved the old revision: the second one does not add the new one on top of it
    assert [i["id"] for i in bajar_ndvi.pending_composites([new, old, other], {old["id"]})] == [other["id"]]
    assert bajar_ndvi.pending_composites([new, old, other], {new["id"], other["id"]}) == []
