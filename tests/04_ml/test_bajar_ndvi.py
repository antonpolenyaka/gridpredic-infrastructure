"""
Segunda y tercera ronda de auditoría (A43): resuming the NDVI download never adds an older or a second revision of a
composite, and a composite cut halfway is read again. rasterio is only needed to read the rasters, never here: when it
is not installed a stand-in module lets the script be imported.
"""
import os
import sys
import types

import pandas as pd
import pytest

try:
    import rasterio  # noqa: F401
except ImportError:
    stub = types.ModuleType("rasterio")
    stub.warp = types.SimpleNamespace(transform_bounds=None)
    stub.windows = types.SimpleNamespace(from_bounds=None)
    sys.modules["rasterio"] = stub
    sys.modules["rasterio.warp"] = stub.warp
    sys.modules["rasterio.windows"] = stub.windows
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


def test_a_composite_cut_halfway_is_dropped_and_read_again(tmp_path, monkeypatch):
    out = str(tmp_path / "ndvi.csv")
    cells = pd.DataFrame({"celda_id": ["a", "b"], "lat": [39.0, 39.1], "lon": [-6.0, -6.1]})
    composite = item("2024020000000")
    monkeypatch.setattr(bajar_ndvi, "cells_of_places", lambda xlsx: cells)
    monkeypatch.setattr(bajar_ndvi, "search", lambda box, year: [composite])
    monkeypatch.setattr(bajar_ndvi.time, "sleep", lambda s: None)
    reads = []

    def rows(item, cells, box):
        reads.append(item["id"])
        return [{"celda_id": c, "plataforma": "terra", "fecha_inicio": "2024-01-01", "fecha_fin": "2024-01-16",
                 "ndvi": 0.5, "n_validos": 3, "n_pixeles": 4, "_item": item["id"]} for c in cells["celda_id"]]

    monkeypatch.setattr(bajar_ndvi, "composite_rows", rows)
    real_writer = bajar_ndvi.csv.DictWriter

    class FullDisk(real_writer):
        written = 0

        def writerow(self, row):
            if row.get("_item") == "_item":  # the header
                return super().writerow(row)
            FullDisk.written += 1
            if FullDisk.written == 2:  # the second cell of the composite: the disk is full
                raise OSError(28, "No space left on device")
            return super().writerow(row)

    monkeypatch.setattr(bajar_ndvi.csv, "DictWriter", FullDisk)
    with pytest.raises(OSError):
        bajar_ndvi.main([out, "2024", "2024", "x.xlsx"])
    assert len(pd.read_csv(out)) == 1  # one cell of two written
    monkeypatch.setattr(bajar_ndvi.csv, "DictWriter", real_writer)
    bajar_ndvi.main([out, "2024", "2024", "x.xlsx"])
    saved = pd.read_csv(out)
    assert reads == [composite["id"], composite["id"]]
    assert sorted(saved["celda_id"]) == ["a", "b"] and len(saved) == 2
    with open(out + ".completos", encoding="utf-8") as file:
        assert file.read().split() == [composite["id"]]
    # a third run has nothing to read
    bajar_ndvi.main([out, "2024", "2024", "x.xlsx"])
    assert len(reads) == 2 and len(pd.read_csv(out)) == 2


def test_a_csv_of_before_the_list_of_finished_composites_counts_as_done(tmp_path, monkeypatch):
    out = str(tmp_path / "ndvi.csv")
    composite = item("2024020000000")
    pd.DataFrame([{"celda_id": "a", "plataforma": "terra", "fecha_inicio": "2024-01-01", "fecha_fin": "2024-01-16",
                   "ndvi": 0.5, "n_validos": 3, "n_pixeles": 4, "_item": composite["id"]}]).to_csv(out, index=False)
    monkeypatch.setattr(bajar_ndvi, "cells_of_places",
                        lambda xlsx: pd.DataFrame({"celda_id": ["a"], "lat": [39.0], "lon": [-6.0]}))
    monkeypatch.setattr(bajar_ndvi, "search", lambda box, year: [composite])
    monkeypatch.setattr(bajar_ndvi, "composite_rows", lambda *a: (_ for _ in ()).throw(AssertionError("read again")))
    bajar_ndvi.main([out, "2024", "2024", "x.xlsx"])
    assert len(pd.read_csv(out)) == 1
