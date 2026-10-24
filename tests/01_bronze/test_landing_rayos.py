"""
Tests of the live lightning (job_landing_rayos): the flashes of a product of
the Lightning Imager (a zip with a NetCDF body, built here with h5py), the
Landing of the products that are missing, the flashes of the last hour and
rayos.json for the dashboard with the distances to the network, without
network and without credentials.

    python -m pytest tests/01_bronze/test_landing_rayos.py -q
"""

import io
import json
import os
import sys
import zipfile
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
path = os.path.join(ROOT, "etl", "jobs", "00_landing")
if path not in sys.path:
    sys.path.insert(0, path)

import job_landing_rayos as rayos  # noqa: E402

NOW = datetime(2026, 7, 20, 15, 7, tzinfo=timezone.utc)


def product_id(start: datetime, minutes: int = 10) -> str:
    end = start + timedelta(minutes=minutes)
    return ("W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_"
            f"{end + timedelta(minutes=2):%Y%m%d%H%M%S}_L2PF_OPE_{start:%Y%m%d%H%M%S}_{end:%Y%m%d%H%M%S}_N__O_0001_0000")


def product(points: list, start: datetime) -> bytes:
    """A zip with one BODY NetCDF (HDF5) of flashes at points [(ts, lat, lon)], packed as the real product."""
    h5py = pytest.importorskip("h5py")

    body = io.BytesIO()
    with h5py.File(body, "w") as h:
        lat = h.create_dataset("latitude", data=np.array([round(p[1] / 0.0001) for p in points], dtype="int32"))
        lat.attrs["scale_factor"] = np.float64(0.0001)
        lat.attrs["add_offset"] = np.float64(0.0)
        lat.attrs["_FillValue"] = np.int32(-2147483647)
        lon = h.create_dataset("longitude", data=np.array([round(p[2] / 0.0001) for p in points], dtype="int32"))
        lon.attrs["scale_factor"] = np.float64(0.0001)
        h.create_dataset("flash_time", data=np.array([(p[0] - rayos.EPOCH.to_pydatetime().replace(tzinfo=timezone.utc)).total_seconds()
                                                      for p in points], dtype="float64"))
        h.create_dataset("flash_duration", data=np.array([120] * len(points), dtype="int32"))
        rad = h.create_dataset("radiance", data=np.array([50] * len(points), dtype="int32"))
        rad.attrs["scale_factor"] = np.float64(0.1)
        conf = h.create_dataset("flash_filter_confidence", data=np.array([90] * len(points), dtype="int8"))
        conf.attrs["scale_factor"] = np.float64(0.01)
        h.create_dataset("number_of_events", data=np.array([3] * len(points), dtype="int32"))
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr(f"W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_{start:%Y%m%d%H%M%S}.nc",
                   body.getvalue())
        z.writestr("EOPMetadata.xml", "<xml/>")
    return raw.getvalue()


@pytest.fixture
def fs():
    import fsspec

    memory = fsspec.filesystem("memory")
    memory.store.clear()
    memory.pseudo_dirs.clear()
    memory.pseudo_dirs.append("")
    return memory


@pytest.fixture
def cfg(tmp_path):
    cfg = rayos.lightning_settings(rayos.load_config())
    cfg["landing"] = "memory://datalake/00_landing/vivo"
    cfg["dashboard_dir"] = str(tmp_path)
    cfg["pausa_s"] = 0.0
    return cfg


def test_the_flashes_of_a_product_inside_the_box(cfg):
    start = NOW - timedelta(minutes=20)
    raw = product([(start + timedelta(minutes=1), 39.47, -6.37), (start + timedelta(minutes=2), 41.0, -3.0),
                   (start + timedelta(minutes=3), 38.5, -6.9)], start)
    frame = rayos.flashes(raw, cfg["caja"], product_id(start))
    assert len(frame) == 2  # the one in Madrid is outside the box
    assert frame["lat"].round(2).tolist() == [39.47, 38.5]
    assert frame["radiancia"].tolist() == [5.0, 5.0] and frame["confianza"].tolist() == [0.9, 0.9]
    assert frame["ts_utc"].iloc[0].to_pydatetime().replace(tzinfo=timezone.utc) == start + timedelta(minutes=1)
    assert list(frame.columns) == rayos.COLUMNS
    assert rayos.product_window(product_id(start)) == (f"{start:%Y%m%d%H%M%S}", f"{start + timedelta(minutes=10):%Y%m%d%H%M%S}")
    assert rayos.product_path(cfg["landing"], product_id(start)).endswith(
        f"/rayos/{start:%Y%m%d}/flashes_{start:%Y%m%d%H%M%S}_{start + timedelta(minutes=10):%Y%m%d%H%M%S}.csv")
    assert rayos.flashes(product([], start), cfg["caja"]).empty


def test_the_landing_downloads_only_what_is_missing_and_the_json_tells_the_lines(fs, cfg, monkeypatch):
    starts = [NOW - timedelta(minutes=m) for m in (80, 70, 60, 50, 40, 30, 20)]
    products = {product_id(s): product([(s + timedelta(minutes=4), 39.30 + 0.01 * i, -6.10)], s)
                for i, s in enumerate(starts)}
    asked = []
    monkeypatch.setattr(rayos, "token", lambda cfg, key, secret: "tok")
    monkeypatch.setattr(rayos, "search", lambda cfg, tok, start, end: list(products))
    monkeypatch.setattr(rayos, "download", lambda cfg, tok, p: asked.append(p) or products[p])
    # One product is already in the Landing: it is not asked again.
    done = product_id(starts[0])
    with fs.open(rayos.product_path(cfg["landing"], done), "w") as file:
        rayos.flashes(products[done], cfg["caja"], done).to_csv(file, index=False, date_format="%Y-%m-%dT%H:%M:%S.%fZ")

    landed = rayos.land(fs, cfg, NOW, "k", "s")
    assert landed == {"encontrados": 7, "descargados": 6, "fallidos": 0, "flashes": 6}
    assert len(asked) == 6 and done not in asked
    assert len(fs.ls(f"{cfg['landing']}/rayos/{NOW:%Y%m%d}")) == 7

    # The last hour: the products that end after NOW - 60 min (the one of -70 ends at -60: in), by flash time.
    recent = rayos.recent_flashes(fs, cfg, NOW, 60)
    assert len(recent) == 5
    assert recent["ts_utc"].min().to_pydatetime() >= NOW - timedelta(minutes=60)

    # riesgo.json of dag_riesgo_manana gives the municipalities: Montánchez under the flashes, Zafra 100 km south.
    risk = {"distribuidoras": [
        {"id": 3, "nombre": "Distribuidora 1", "municipios": [{"nombre": "Montánchez", "lat": 39.32, "lon": -6.12, "zonas": ["Montánchez / Torre"]},
                                                   {"nombre": "Alcuéscar", "lat": 39.18, "lon": -6.23, "zonas": ["Alcuéscar"]}]},
        {"id": 2, "nombre": "Distribuidora 2", "municipios": [{"nombre": "Zafra", "lat": 38.42, "lon": -6.42, "zonas": ["Zafra"]}]},
        {"id": 1366, "nombre": "Distribuidora 3", "municipios": []},
    ]}
    with open(os.path.join(cfg["dashboard_dir"], "riesgo.json"), "w", encoding="utf-8") as file:
        json.dump(risk, file)
    assert rayos.main(["--sin-descarga", "--dashboard-dir", cfg["dashboard_dir"], "--landing", cfg["landing"]],
                      fs=fs, now=NOW) == 0
    with open(os.path.join(cfg["dashboard_dir"], "rayos.json"), encoding="utf-8") as file:
        out = json.load(file)
    assert out["n_rayos"] == 5 and len(out["rayos"]) == 5
    assert out["rayos"][0]["minutos"] == 56 and out["rayos"][-1]["minutos"] == 16
    d1, d2, d3 = out["distribuidoras"]
    assert d1["cerca"] == {"10": 5, "20": 5} and d1["lineas"] == ["Montánchez / Torre"]
    assert d1["nivel"] == "tormenta sobre la red" and d1["texto"].startswith("5 rayos a menos de 10 km")
    assert d2["cerca"] == {"10": 0, "20": 0} and d2["nivel"] == "sin tormenta"
    assert d3["cerca"] == {"10": 0, "20": 0} and d3["lineas"] == []
    assert out["generado"] == "2026-07-20T15:07Z"


def test_without_credentials_nothing_is_asked(fs, cfg, monkeypatch, tmp_path):
    monkeypatch.delenv("EUMETSAT_CONSUMER_KEY", raising=False)
    monkeypatch.delenv("EUMETSAT_CONSUMER_SECRET", raising=False)
    assert rayos.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW) == 2
    assert not os.path.exists(os.path.join(tmp_path, "rayos.json"))
    # When the Data Store fails the JSON still comes out of the Landing and the code says it.
    monkeypatch.setenv("EUMETSAT_CONSUMER_KEY", "k")
    monkeypatch.setenv("EUMETSAT_CONSUMER_SECRET", "s")
    monkeypatch.setattr(rayos, "token", lambda cfg, key, secret: (_ for _ in ()).throw(RuntimeError("HTTP 503 for token")))
    assert rayos.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW) == 1
    with open(os.path.join(tmp_path, "rayos.json"), encoding="utf-8") as file:
        out = json.load(file)
    assert out["n_rayos"] == 0 and out["ultimo_producto"]["error"].startswith("HTTP 503")
    assert out["distribuidoras"] == []


def test_products_found_but_none_downloaded_is_no_data_and_an_error(fs, cfg, monkeypatch, tmp_path):
    # Auditoria integral A04: six products found and six failed downloads must not read as "sin tormenta"
    monkeypatch.setenv("EUMETSAT_CONSUMER_KEY", "k")
    monkeypatch.setenv("EUMETSAT_CONSUMER_SECRET", "s")
    monkeypatch.setattr(rayos, "token", lambda cfg, key, secret: "tok")
    found = [product_id(NOW - timedelta(minutes=10 * (i + 1))) for i in range(6)]
    monkeypatch.setattr(rayos, "search", lambda cfg, tok, since, now: found)
    monkeypatch.setattr(rayos, "download", lambda cfg, tok, product: (_ for _ in ()).throw(TimeoutError("timed out")))
    monkeypatch.setattr(rayos.time, "sleep", lambda s: None)
    risk = {"distribuidoras": [{"id": 2, "nombre": "Distribuidora 2", "municipios": [{"nombre": "Zafra", "lat": 38.42, "lon": -6.42, "zonas": ["Zafra"]}]}]}
    with open(os.path.join(tmp_path, "riesgo.json"), "w", encoding="utf-8") as file:
        json.dump(risk, file)
    assert rayos.main(["--dashboard-dir", str(tmp_path), "--landing", cfg["landing"]], fs=fs, now=NOW) == 1
    with open(os.path.join(tmp_path, "rayos.json"), encoding="utf-8") as file:
        out = json.load(file)
    assert out["ultimo_producto"]["fallidos"] == 6 and out["ultimo_producto"]["descargados"] == 0
    assert out["ultimo_producto"]["error"].startswith("no se pudo descargar")
    assert out["distribuidoras"][0]["nivel"] == "sin datos"
