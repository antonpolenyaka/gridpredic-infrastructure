"""
Test of the risk of tomorrow (job_ml_riesgo_manana): the forecast of the live
Landing with the names of Gold and by distribuidora, the weather, extremes
and warnings of the day to predict, the reasons, and the whole job on a
local Spark with five years of synthetic weather in Gold and a storm with an
orange warning forecast for one of the two distribuidoras.

    python -m pytest tests/04_ml/test_riesgo_manana.py -q
"""

import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import job_ml_riesgo_manana as rm  # noqa: E402
import ml_models  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

CELLS = {1: "39.50_-6.40", 2: "38.70_-6.90"}
DAY = pd.Timestamp("2026-01-15")
RUN_ID = "manana_test"
ZONE = "Europe/Madrid"


def payload(cell: str, start_utc: str, end_utc: str, gust, rain=0.0, temp=12.0) -> dict:
    """
    An answer of the Open-Meteo Forecast API for a cell (unixtime, GMT), with
    _peticion as job_landing_vivo saves it. gust, rain and temp are numbers
    or functions of the local hour.
    """
    times = pd.date_range(start_utc, end_utc, freq="h", inclusive="left", tz="UTC")
    local = times.tz_convert(ZONE).tz_localize(None)

    def values(spec):
        return [float(spec(t)) if callable(spec) else float(spec) for t in local]

    gusts = values(gust)
    return {
        "latitude": float(cell.split("_")[0]), "longitude": float(cell.split("_")[1]),
        "hourly": {"time": (times.asi8 // 10**9).tolist(), "wind_gusts_10m": gusts,
                   "wind_speed_10m": [0.6 * g for g in gusts], "precipitation": values(rain),
                   "snowfall": [0.0] * len(times), "temperature_2m": values(temp),
                   "relative_humidity_2m": [70.0] * len(times)},
        "_peticion": {"celda_id": cell, "modelo": "icon_eu", "pedido_utc": "2026-01-14T19:31:00+00:00",
                      "dias_pasados": 31, "dias_futuros": 3},
    }


def box_of(cell: str, half: float = 0.1) -> str:
    lat, lon = (float(x) for x in cell.split("_"))
    corners = [(lat - half, lon - half), (lat + half, lon - half), (lat + half, lon + half), (lat - half, lon + half),
               (lat - half, lon - half)]
    return " ".join(f"{a:.2f},{b:.2f}" for a, b in corners)


def cap_row(identifier: str, cell: str, sent: str, onset: str, expires: str, level="naranja", code="VI;Vientos",
            parameter="VI; Rachas máximas; 90 km/h", kind="Alert") -> dict:
    """A row of parse_cap: one warning of AEMET over a square around the cell."""
    return {"identificador": identifier, "tipo_mensaje": kind, "enviado": sent, "inicio": onset, "fin": expires,
            "fenomeno": code, "nivel": level, "parametro": parameter, "zona": "700601", "zona_nombre": "Zona",
            "poligono": box_of(cell)}


CAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>2.49.0.0.724.0.ES.20260114090000.700601VI</identifier>
  <sender>http://www.aemet.es</sender>
  <sent>2026-01-14T09:00:00+01:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>es-ES</language>
    <category>Met</category>
    <event>Aviso de vientos de nivel naranja</event>
    <eventCode><valueName>AEMET-Meteoalerta fenomeno</valueName><value>VI;Vientos</value></eventCode>
    <onset>2026-01-15T06:00:00+01:00</onset>
    <expires>2026-01-15T22:00:00+01:00</expires>
    <parameter><valueName>AEMET-Meteoalerta nivel</valueName><value>naranja</value></parameter>
    <parameter><valueName>AEMET-Meteoalerta parametro</valueName><value>VI; Rachas máximas; 100 km/h</value></parameter>
    <area>
      <areaDesc>Meseta cacereña</areaDesc>
      <polygon>POLYGON</polygon>
      <geocode><valueName>AEMET-Meteoalerta zona</valueName><value>700601</value></geocode>
    </area>
  </info>
</alert>
"""


def tar_of(name: str, data: bytes) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def test_the_forecast_with_the_names_of_gold_and_by_distribuidora(tmp_path):
    folder = tmp_path / "20260115"
    (folder / "prevision").mkdir(parents=True)
    windy = payload(CELLS[1], "2026-01-14 00:00", "2026-01-17 00:00",
                    lambda t: 100.0 if t == pd.Timestamp("2026-01-15 12:00") else 30.0)
    windy["hourly"]["wind_gusts_10m"][5] = 999.0
    calm = payload(CELLS[2], "2026-01-14 00:00", "2026-01-17 00:00", 20.0)
    for cell, answer in ((CELLS[1], windy), (CELLS[2], calm)):
        (folder / "prevision" / f"{cell}.json").write_text(json.dumps(answer))
    (folder / "prevision" / "roto.json").write_text(json.dumps({"hourly": {}}))

    hours, asked = rm.forecast_hours(rm.LocalFiles(), str(folder), ZONE)
    assert asked == "2026-01-14T19:31:00+00:00"
    assert sorted(hours["celda_id"].unique()) == sorted(CELLS.values())
    one = hours[hours["celda_id"] == CELLS[1]].set_index("hora").sort_index()
    # Local wall clock of Madrid: 00:00 UTC is 01:00 in winter. The gust out of range is null, as in Silver.
    assert one.index.min() == pd.Timestamp("2026-01-14 01:00")
    assert one.loc[pd.Timestamp("2026-01-15 12:00"), "racha_1h"] == 100.0
    assert one["racha_1h"].isna().sum() == 1

    gold = rm.gold_columns(hours, pd.Series({CELLS[1]: 50.0, CELLS[2]: 40.0}))
    noon = gold[(gold["celda_id"] == CELLS[1]) & (gold["hora"] == pd.Timestamp("2026-01-15 12:00"))].iloc[0]
    assert noon["met_racha_1h"] == noon["metp1_racha_1h"] == 100.0
    assert noon["metp1_racha_clima_1h"] == pytest.approx(2.0) and noon["metp1_racha_ku_1h"] == pytest.approx(1.0)

    weights = pd.DataFrame({"distribuidora_id": [1, 1, 2], "celda_id": [CELLS[1], CELLS[2], CELLS[2]],
                            "n_ct": [3.0, 1.0, 2.0]})
    by_dist = rm.distributor_hours(gold, weights).set_index(["distribuidora_id", "hora"])
    row = by_dist.loc[(1, pd.Timestamp("2026-01-15 12:00"))]
    assert row["metd_max_metp1_racha_1h"] == 100.0
    assert row["metd_media_metp1_racha_1h"] == pytest.approx((3 * 100.0 + 20.0) / 4)
    # Without the gust of a cell, the mean of the others.
    missing = one.index[one["racha_1h"].isna()][0]
    assert by_dist.loc[(1, missing), "metd_media_met_racha_1h"] == pytest.approx(20.0)
    assert by_dist.loc[(2, pd.Timestamp("2026-01-15 12:00")), "metd_max_metp1_racha_1h"] == 20.0


def test_the_weather_of_the_day_its_extremes_and_the_day_before():
    hours = pd.date_range("2026-01-12 00:00", "2026-01-16 23:00", freq="h")
    day = hours.floor("D")
    gust = np.where(day == DAY, 40.0, 30.0)
    gust[hours == pd.Timestamp("2026-01-15 12:00")] = 100.0
    gust[hours == pd.Timestamp("2026-01-14 18:00")] = 60.0
    rain = np.where((day == DAY - pd.Timedelta(days=1)) & (hours.hour < 5), 1.0, 0.0)
    temp = np.select([day == pd.Timestamp("2026-01-12"), day == DAY], [20.0, 37.0], 36.0)
    cells = pd.DataFrame({"celda_id": CELLS[1], "hora": hours, "racha_1h": gust, "viento_1h": 0.6 * gust,
                          "lluvia_1h": rain, "nieve_1h": 0.0, "temp_1h": temp, "humedad_1h": 70.0})
    gold = rm.gold_columns(cells, pd.Series({CELLS[1]: 50.0}))
    hourly = rm.distributor_hours(gold, pd.DataFrame({"distribuidora_id": [1], "celda_id": [CELLS[1]],
                                                      "n_ct": [4.0]}))
    with open(CONFIG, encoding="utf-8") as file:
        cfg = rm.settings(json.load(file))
    row = rm.weather_features(hourly, DAY, cfg).set_index("distribuidora_id").loc[1]

    assert row["dia_racha_max"] == 100.0 and row["dia_temp_max"] == 37.0
    assert row["ayer_racha_max"] == 60.0 and row["ayer_lluvia_max"] == pytest.approx(5.0)
    assert row["ext_racha_clima_max"] == pytest.approx(2.0) and row["ext_racha_ku"] == pytest.approx(1.0)
    # Hot on the 13th and the 14th, and the forecast of the 15th too.
    assert row["ext_dias_calor"] == 3.0


def test_the_warnings_of_the_day_in_the_cells():
    rows = [
        cap_row("viento", CELLS[1], "2026-01-14T10:00:00+01:00", "2026-01-15T06:00:00+01:00",
                "2026-01-15T22:00:00+01:00"),
        # Sent after 20:00 of the day before: it is known when the job runs, so it counts.
        cap_row("tarde", CELLS[2], "2026-01-14T21:00:00+01:00", "2026-01-15T00:00:00+01:00",
                "2026-01-16T00:00:00+01:00", level="amarillo", parameter="VI; Rachas máximas; 70 km/h"),
        cap_row("lluvia_pasado", CELLS[2], "2026-01-14T10:00:00+01:00", "2026-01-16T00:00:00+01:00",
                "2026-01-16T23:59:59+01:00", level="amarillo", code="PR;Lluvias", parameter="P2; 12 h; 40 mm"),
        cap_row("anulado", CELLS[2], "2026-01-14T12:00:00+01:00", "2026-01-15T06:00:00+01:00",
                "2026-01-15T22:00:00+01:00", level="rojo", kind="Cancel"),
        {**cap_row("sin_poligono", CELLS[1], "2026-01-14T10:00:00+01:00", "2026-01-15T06:00:00+01:00",
                   "2026-01-15T22:00:00+01:00", level="rojo"), "poligono": None},
    ]
    warnings = rm.warnings_in_cells(rows, [CELLS[1], CELLS[2], "sin_coordenadas"])
    assert sorted(warnings["celda_id"].unique()) == sorted(CELLS.values())
    assert set(warnings["fenomeno_codigo"]) == {"VI", "PR"}
    weights = pd.DataFrame({"distribuidora_id": [1, 2], "celda_id": [CELLS[1], CELLS[2]], "n_ct": [3.0, 2.0]})
    features = rm.warning_features(warnings, weights, DAY, [1, 2]).set_index("distribuidora_id")

    assert features.loc[1, "aviso_max_nivel"] == 2 and features.loc[1, "aviso_viento_nivel"] == 2
    assert features.loc[1, "aviso_det_racha_kmh"] == 90
    assert features.loc[2, "aviso_viento_nivel"] == 1 and features.loc[2, "aviso_lluvia_nivel"] == 0
    assert features.loc[2, "aviso_det_viento_ct"] == 1.0
    in_force = rm.warnings_of_day(warnings, DAY)
    assert sorted(in_force["nivel_num"].tolist()) == [1, 2]
    # Without warnings the day has zeros.
    empty = rm.warning_features(rm.warnings_in_cells([], [CELLS[1]]), weights, DAY, [1, 2])
    assert (empty.drop(columns="distribuidora_id") == 0).all(axis=None)


def test_reasons_levels_and_texts():
    features = ["dist_1", "aviso_viento_nivel", "aviso_max_nivel", "dia_racha_max", "dia_viento_max",
                "cal_dia_anio_sin", "cal_dia_anio_cos", "hist_incidencias_7d", "hist_incidencias_30d", "cal_festivo",
                "dia_lluvia_max", "dia_lluvia_hora_max"]
    terms = np.array([2.0, 0.7, 0.5, 1.0, -0.2, 0.3, 0.2, 0.01, 0.0, 0.4, 0.3, -0.3])
    values = np.array([1.0, 2.0, 2.0, 96.4, 50.0, 0.5, 0.5, np.nan, np.nan, 0.0, 0.0, 0.0])
    assert rm.reasons(terms, values, features, 3) == (
        "aviso naranja de AEMET por viento (x3,3); racha máxima prevista de 96 km/h (x2,2); época del año (x1,6)")
    # A holiday flag at 0 that raises the risk is a working day. The rain of the day and of its wettest hour cancel
    # out, and a group under 5 % is never a reason; the distribuidora is not one either.
    everything = rm.reasons(terms, values, features, 9)
    assert everything.endswith("; día laborable (x1,5)") and "lluvia" not in everything
    assert rm.reason_text("historia", {"hist_incidencias_7d": np.nan, "hist_incidencias_30d": np.nan}) == (
        "historia de incidencias sin dato")
    assert rm.reason_text("aviso", {"aviso_max_nivel": 0.0}) == "sin avisos de AEMET"
    assert rm.reason_text("aviso", {"aviso_max_nivel": 3.0, "aviso_viento_nivel": 3.0, "aviso_lluvia_nivel": 0.0,
                                    "aviso_tormenta_nivel": 2.0}) == "aviso rojo de AEMET por viento y tormentas"
    assert rm.reason_text("lluvia", {"dia_lluvia_max": 12.34, "dia_lluvia_hora_max": 4.0}) == (
        "12,3 mm de lluvia prevista (hasta 4,0 mm en una hora)")
    assert rm.reason_text("ayer", {"ayer_racha_max": 61.0, "ayer_lluvia_max": 2.26}) == (
        "el día anterior, racha de 61 km/h y 2,3 mm de lluvia")
    assert rm.reason_text("calor", {"dia_temp_max": 38.0, "ext_dias_calor": 3.0}) == (
        "máxima prevista de 38 °C, 3 días seguidos de calor")
    levels = {"muy alto": 0.95, "alto": 0.9, "medio": 0.75}
    assert [rm.level_of(p, levels) for p in (0.97, 0.95, 0.92, 0.8, 0.5)] == ["muy alto", "muy alto", "alto",
                                                                             "medio", "bajo"]


def test_the_contributions_add_up_to_the_score_of_the_logistic_regression():
    rng = np.random.default_rng(3)
    X = rng.normal(size=(500, 3)).astype(np.float32)
    X[::7, 1] = np.nan
    y = (X[:, 0] + rng.normal(size=500) > 1).astype(np.int8)
    data = {"X": X, "y": y, "w": np.ones(500), "Xv": X, "yv": y, "wv": np.ones(500), "feature_names": ["a", "b", "c"]}
    scorer = ml_models.fit_model("logistica", {"C": 1.0}, data, 1.0, 0, 1)
    terms = rm.contributions(scorer, X[:20])
    p = scorer.score(X[:20]).astype(np.float64)
    intercept = scorer.estimator.named_steps["modelo"].intercept_[0]
    assert terms.shape == (20, 3)
    assert np.allclose(terms.sum(axis=1) + intercept, np.log(p / (1 - p)), atol=1e-3)
    # The first feature carries the signal.
    assert np.abs(terms[:, 0]).mean() > np.abs(terms[:, 2]).mean()


def test_the_windiest_places_of_every_distribuidora():
    hours = pd.DataFrame({"celda_id": [CELLS[1], CELLS[1], CELLS[2]],
                          "hora": pd.to_datetime(["2026-01-15 10:00", "2026-01-15 12:00", "2026-01-15 12:00"]),
                          "metp1_racha_1h": [80.0, 100.0, 20.0], "metp1_racha_clima_1h": [1.6, 2.0, 0.5]})
    places = pd.DataFrame({"distribuidora_id": [1, 1, 1, 2], "celda_id": [CELLS[1], CELLS[1], CELLS[2], CELLS[2]],
                           "municipio": ["Montánchez", "Torre de Santa María", "Zafra", "Zafra"],
                           "n_ct": [3, 1, 2, 2]})
    texts = rm.windiest_places(hours, places, DAY, 3)
    assert texts[1] == "Montánchez (100 km/h, 2,00 veces su p98); Zafra (20 km/h, 0,50 veces su p98)"
    assert texts[2] == "Zafra (20 km/h, 0,50 veces su p98)"
    assert rm.windiest_places(hours, places, DAY + pd.Timedelta(days=1), 3) == {}


def test_the_road_minutes_are_asked_once_and_kept(tmp_path, monkeypatch):
    municipalities = pd.DataFrame({"municipio": ["Zafra", "Montánchez", "Torre"], "lat": [38.42, 39.22, 39.3],
                                   "lon": [-6.42, -6.15, -6.1], "n_ct": [3, 2, 1], "probabilidad": [0.1, 0.2, 0.1]})
    asked = []

    def fake_osrm(points, api, timeout):
        asked.append((points, api, timeout))
        n = len(points)
        return np.where(np.eye(n) == 1, 0.0, 33.0)

    monkeypatch.setattr(rm.rz, "osrm_table", fake_osrm)
    zcfg = {"rutas": {"activo": True, "api": "https://osrm.example", "timeout_s": 7, "cache": str(tmp_path / "rutas")}}
    fs = rm.LocalFiles()
    first = rm.road_minutes(fs, zcfg, municipalities, 3)
    assert list(first.index) == ["Montánchez", "Torre", "Zafra"] and first.loc["Zafra", "Torre"] == 33.0
    assert asked[0][1:] == ("https://osrm.example", 7.0) and asked[0][0][0] == (39.22, -6.15)
    # The second time it comes from the cache, with the same values.
    second = rm.road_minutes(fs, zcfg, municipalities, 3)
    assert len(asked) == 1 and second.equals(first)
    assert len(list((tmp_path / "rutas").glob("3_*.json"))) == 1
    # Off, or a service that fails: None, and the crews use straight lines.
    assert rm.road_minutes(fs, {"rutas": {"activo": False}}, municipalities, 3) is None
    monkeypatch.setattr(rm.rz, "osrm_table", lambda *a: (_ for _ in ()).throw(RuntimeError("down")))
    assert rm.road_minutes(fs, {"rutas": {**zcfg["rutas"], "cache": str(tmp_path / "otra")}}, municipalities, 3) is None
    assert rm.road_minutes(None, zcfg, municipalities, 3) is None


# ---------------------------------------------------------------------------
# The whole job on a local Spark
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="manana_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("manana-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for db in ("l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()


def save(spark, pdf: pd.DataFrame, table: str):
    spark.createDataFrame(pdf).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true").saveAsTable(table)


def synthetic():
    """(weather per cell and hour in Gold, principal events with cause, warnings of AEMET in Silver)."""
    rng = np.random.default_rng(5)
    hours = pd.date_range("2021-01-01", "2025-12-31 23:00", freq="h")
    days = pd.date_range("2021-01-02", "2025-12-29", freq="D")
    weather, events, warnings = [], [], []

    for dist, cell in CELLS.items():
        gust = rng.gamma(4.0, 6.0, len(hours))
        rain = np.where(rng.random(len(hours)) < 0.05, rng.gamma(1.0, 2.0, len(hours)), 0.0)
        for day in rng.choice(days, size=5 * 20, replace=False):
            start = int((pd.Timestamp(day) + pd.Timedelta(hours=10) - hours[0]) / pd.Timedelta(hours=1))
            gust[start:start + 6] = rng.uniform(85, 110, 6)
            rain[start:start + 6] = rng.uniform(2, 8, 6)
            if rng.random() < 0.7:
                events.append((dist, pd.Timestamp(day) + pd.Timedelta(hours=12), "meteorologia"))
            if rng.random() < 0.8:
                warnings.append({"celda_id": cell, "fenomeno_codigo": "VI", "nivel_num": 2, "tipo_mensaje": "Alert",
                                 "enviado_ts": pd.Timestamp(day) - pd.Timedelta(hours=12),
                                 "inicio_ts": pd.Timestamp(day) + pd.Timedelta(hours=6),
                                 "fin_ts": pd.Timestamp(day) + pd.Timedelta(hours=22),
                                 "parametro": "VI; Rachas máximas; 90 km/h"})
        for _ in range(5 * 15):
            events.append((dist, pd.Timestamp(rng.choice(days)) + pd.Timedelta(hours=int(rng.integers(0, 24))),
                           "equipo" if rng.random() < 0.5 else "aguas_arriba"))
        weather.append(pd.DataFrame({
            "celda_id": cell, "hora": hours, "met_racha_1h": gust, "met_viento_1h": gust * 0.6,
            "met_lluvia_1h": rain, "met_temp_1h": 15 + 10 * np.sin(2 * np.pi * (hours.dayofyear.to_numpy() - 100) / 365),
            "met_humedad_1h": 60.0,
        }))

    weather = pd.concat(weather, ignore_index=True)
    archived = weather["hora"] >= "2024-01-01"
    noise = rng.normal(1.0, 0.15, len(weather))
    for name in ("racha", "viento", "lluvia"):
        weather[f"metp1_{name}_1h"] = np.where(archived, weather[f"met_{name}_1h"] * noise.clip(0), np.nan)
    weather["metp1_temp_1h"] = np.where(archived, weather["met_temp_1h"] + 1.0, np.nan)
    weather["metp1_humedad_1h"] = np.where(archived, 60.0, np.nan)
    p98 = weather[weather["hora"] < "2022-01-01"].groupby("celda_id")["met_racha_1h"].quantile(0.98)
    for prefix in ("met", "metp1"):
        ratio = weather[f"{prefix}_racha_1h"] / weather["celda_id"].map(p98)
        weather[f"{prefix}_racha_clima_1h"] = ratio
        weather[f"{prefix}_racha_ku_1h"] = np.clip(ratio - 1.0, 0.0, None) ** 3
    warnings = pd.DataFrame(warnings)
    return weather, events, warnings[warnings["enviado_ts"] >= "2021-06-18"].reset_index(drop=True), p98


@pytest.fixture(scope="module")
def tables(spark):
    weather, events, warnings, p98 = synthetic()
    save(spark, weather, "l3_gold.meteo_celda_hora")
    # CT i of every distribuidora: three in the first municipality of the cell and, in the first one, a fourth in another.
    places = [(1, 0, CELLS[1], "Montánchez"), (1, 1, CELLS[1], "Montánchez"), (1, 2, CELLS[1], "Montánchez"),
              (1, 3, CELLS[1], "Torre de Santa María"), (2, 0, CELLS[2], "Zafra"), (2, 1, CELLS[2], "Zafra"),
              (2, 2, CELLS[2], "Zafra")]
    save(spark, pd.DataFrame([{"distribuidora_id": d, "ct_id": f"{d}-{i}", "celda_id": c, "municipio_nombre": m,
                               "ct_lat_municipio": 39.3 + 0.3 * (d == 2) + 0.1 * (m == "Torre de Santa María"),
                               "ct_lon_municipio": -6.1 - 0.3 * (d == 2), "codigo_ine": f"100{d}{i}"}
                              for d, i, c, m in places]), "l3_gold.map_ct_celda")
    # The lines of TedisNet: two in the first distribuidora (the incidents cut the first one), one in the second.
    save(spark, pd.DataFrame([{"distribuidora_id": d, "ct_id": f"{d}-{i}", "grupo_red_id": (10 if i < 2 else 11) if d == 1 else 20,
                               "municipio_nombre": m, "potencia_kva": 250.0, "num_abonados": 40 + i,
                               "tipo_zona_codigo": 4 if d == 1 else 3, "en_estudio": True}
                              for d, i, c, m in places]), "l3_gold.dim_ct")
    facts, causes = [], []
    names = {1: "Calser_EOSA", 2: "Calser_Pitarch"}
    for k, (dist, start, cause) in enumerate(events):
        period, inc = start.strftime("%Y%m"), f"E{k}"
        # The incidents of the first distribuidora cut the CTs of its first line (two CTs), every third one also the third CT.
        cts = [f"{dist}-0", f"{dist}-1"] + ([f"{dist}-2"] if k % 3 == 0 else []) if dist == 1 else [f"{dist}-0"]
        for ct_id in cts:
            facts.append({"distribuidora_id": dist, "source_database": names[dist], "periodo_id": period,
                          "incidencia_id": inc, "ct_id": ct_id, "inicio_ts": start, "duracion_winsor_s": 5400.0,
                          "conocido_ts": start + pd.Timedelta(days=2), "variante": "principal"})
        causes.append({"distribuidora_id": dist, "periodo_id": period, "id": inc, "causa": cause})
    save(spark, pd.DataFrame(facts), "l3_gold.fact_interrupciones_mt")
    save(spark, pd.DataFrame(causes), "l2_silver.f_incidencia_causa")
    save(spark, warnings, "l2_silver.f_aviso_meteo")
    # Calser keeps loading: the last incident of any kind is two days before the day to predict.
    save(spark, pd.DataFrame({"distribuidora_id": [1, 2], "periodo_id": ["202601", "202601"], "id": ["B1", "B2"],
                              "inicio_ts": pd.to_datetime(["2026-01-10 08:00", "2026-01-13 17:00"])}),
         "l2_silver.f_incidencia")
    return p98


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    config["spark_conf"] = {}
    config["riesgo_manana"]["dashboard_dir"] = os.path.join(workdir, "dashboard")
    config["riesgo_manana"]["riesgo_zonas"] = {"brigadas": {"1": 1, "2": 1}, "bases_fijas": {"1": ["Torre de Santa María"]}}
    os.makedirs(config["riesgo_manana"]["dashboard_dir"])
    config["meteo_dia"] = {
        **config.get("meteo_dia", {}),
        "desde": "2021-01-08",
        "hasta": "2026-01-01",
        "busqueda": {"logistica": [{"C": 0.1}, {"C": 1.0}]},
        "variantes": [{"nombre": "operativa", "desde": "2021-06-25",
                       "bloques": ["calendario", "historia", "avisos", "dia_comun", "extremos"],
                       "variables": ["ayer_racha_max", "ayer_lluvia_max"]}],
    }
    path = os.path.join(workdir, "config.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)
    return path


def land(workdir: str, day: pd.Timestamp, storm: bool, warned: bool) -> str:
    """The live Landing of the day: 31 days before today and 3 days ahead, a storm on the day in the first cell."""
    landing = os.path.join(workdir, "vivo")
    folder = os.path.join(landing, f"{day:%Y%m%d}")
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(os.path.join(folder, "prevision"))
    today = day - pd.Timedelta(days=1)
    start, end = f"{today - pd.Timedelta(days=31):%Y-%m-%d}", f"{today + pd.Timedelta(days=3):%Y-%m-%d}"

    def gust(t):
        return 105.0 if storm and t.floor("D") == day and 10 <= t.hour < 16 else 25.0

    def rain(t):
        return 4.0 if storm and t.floor("D") == day and 10 <= t.hour < 16 else 0.0

    for cell, answer in ((CELLS[1], payload(CELLS[1], start, end, gust, rain)),
                         (CELLS[2], payload(CELLS[2], start, end, 20.0))):
        with open(os.path.join(folder, "prevision", f"{cell}.json"), "w", encoding="utf-8") as file:
            json.dump(answer, file)
    if warned:
        os.makedirs(os.path.join(folder, "avisos"))
        xml = CAP_XML.replace("POLYGON", box_of(CELLS[1]))
        with open(os.path.join(folder, "avisos", "esp.tar"), "wb") as file:
            file.write(tar_of("Z_CAP_C_LEMM_20260114090000_AFAZ700601VI.xml", xml.encode("utf-8")))
        with open(os.path.join(folder, "avisos", "esp.json"), "w", encoding="utf-8") as file:
            json.dump({"area": "esp", "pedido_utc": "2026-01-14T19:32:00+00:00"}, file)
    return landing


@pytest.fixture(scope="module")
def first(spark, tables, config_path, workdir):
    landing = land(workdir, DAY, storm=True, warned=True)
    code = rm.main(["--run-id", RUN_ID, "--config", config_path, "--fecha", f"{DAY:%Y-%m-%d}", "--landing", landing])
    rows = spark.table("l3_gold.riesgo_dia_siguiente").toPandas().set_index("distribuidora_id")
    return code, rows, landing


def test_the_storm_with_a_warning_is_the_riskiest_day(first):
    code, rows, _ = first
    assert code == 0
    assert sorted(rows.index) == [1, 2]
    assert (rows["fecha"].astype(str) == "2026-01-15").all()
    storm, calm = rows.loc[1], rows.loc[2]
    assert storm["distribuidora"] == "EOSA" and calm["distribuidora"] == "Pitarch"
    assert storm["probabilidad"] > 3 * calm["probabilidad"]
    assert storm["nivel"] in ("muy alto", "alto") and storm["percentil"] >= 0.9
    assert storm["veces_tasa_base"] > 2
    # The reasons are the storm: the gusts over the climate, the warning, the gust or the rain of the day. Below the
    # base rate there is nothing to tell.
    assert storm["motivos"].count("(x") == 3
    assert any(word in storm["motivos"].split(";")[0] for word in ("percentil 98", "aviso naranja", "racha", "lluvia"))
    assert calm["motivos"] == "ninguno, por debajo de un día medio"
    assert storm["zonas"].startswith("Montánchez (105 km/h")
    assert storm["racha_max_kmh"] == 105.0 and storm["aviso_max_nivel"] == 2.0 and calm["aviso_max_nivel"] == 0.0
    assert storm["lluvia_mm"] == pytest.approx(24.0)
    # The guard: reinforced by the level and by the wind warning on the storm; normal on the calm day.
    assert storm["guardia_reforzada"] and "aviso de viento de AEMET (naranja)" in storm["guardia_motivo"]
    assert not calm["guardia_reforzada"] and calm["guardia_motivo"] == "normal"
    assert storm["aviso_viento_nivel"] == 2.0 and calm["aviso_tormenta_nivel"] == 0.0
    assert rows["avisos_disponibles"].all() and rows["historia_al_dia"].all()
    assert (rows["cobertura_prevision"] == 1.0).all()
    assert str(storm["calser_hasta"]) == "2026-01-13"
    # Labels, weather and warnings: the archive of warnings ends first.
    assert str(storm["entrenado_hasta"]) <= "2025-12-29"
    assert storm["dias_entreno"] > 1500 and storm["modelo"] == "logistica" and storm["variante"] == "operativa"


def test_the_zones_split_the_risk_of_the_day(first, spark, workdir):
    _, rows, _ = first
    zones = spark.table("l3_gold.riesgo_zona_siguiente").toPandas()
    assert sorted(zones["distribuidora_id"].unique()) == [1, 2]
    one = zones[zones["distribuidora_id"] == 1].sort_values("probabilidad", ascending=False)
    assert len(one) == 2 and set(one["zona"]) == {"10", "11"}
    # The probability that some line fails is the probability of the day of the distribuidora.
    assert 1 - np.prod(1 - one["probabilidad"].to_numpy()) == pytest.approx(rows.loc[1, "probabilidad"], rel=1e-3)
    assert one["parte"].sum() == pytest.approx(1.0)
    # The line that every incident cuts carries more risk than the one cut by one in three.
    assert one.iloc[0]["zona"] == "10" and one.iloc[0]["parte"] > 0.5
    assert rows.loc[1, "incidencias_esperadas"] == pytest.approx(one["probabilidad"].sum())
    assert rows.loc[1, "clientes_esperados"] > 0 and rows.loc[1, "hora_pico"] == 10 or rows.loc[1, "hora_pico"] in (10, 11, 12, 13, 14, 15)
    assert rows.loc[1, "franja_inicio"] <= 10 and rows.loc[1, "franja_fin"] >= 15
    payload = json.load(open(os.path.join(workdir, "dashboard", "riesgo.json"), encoding="utf-8"))
    assert payload["fecha"] == "2026-01-15" and len(payload["distribuidoras"]) == 2
    eosa = next(d for d in payload["distribuidoras"] if d["id"] == 1)
    assert [z["zona"] for z in eosa["zonas"]][0] == "10"
    assert eosa["municipios"][0]["nombre"] == "Montánchez" and eosa["municipios"][0]["probabilidad"] > 0
    assert eosa["brigadas"]["n"] == 1 and eosa["brigadas"]["bases"][0]["municipio"] in ("Montánchez", "Torre de Santa María")
    # The base of the configuration is farther from the risk than the best one.
    assert eosa["brigadas"]["fijas"]["municipios"] == ["Torre de Santa María"]
    assert eosa["brigadas"]["fijas"]["llegada_media_min"] >= eosa["brigadas"]["llegada_media_min"]
    assert eosa["franja"]["inicio"] is not None and eosa["causas"] and eosa["impacto"]["ct_esperados"] > 0
    assert len(eosa["cts"]) == 4 and eosa["cts"][0]["probabilidad"] >= eosa["cts"][-1]["probabilidad"]
    assert payload["reglas"]["cascada"]["n"] > 0 and "pico de racha" in payload["reglas"]["texto"]
    assert os.path.exists(os.path.join(workdir, "dashboard", "riesgo_20260115.json"))
    assert eosa["brigadas"]["posicionamiento"].startswith("brigadas en sus bases a las")
    assert [c["n"] for c in eosa["brigadas"]["curva"]][:2] == [1, 2] and eosa["brigadas"]["bases"][0]["municipios"]
    for name in ("parte_guardia.xlsx", "parte_guardia_20260115.xlsx"):
        assert os.path.getsize(os.path.join(workdir, "dashboard", name)) > 5000
    text = open(os.path.join(workdir, "resultados", RUN_ID, "resumen.md"), encoding="utf-8").read()
    assert "## EOSA: dónde, cuándo, por qué y cuánto" in text and "### Brigadas" in text


def test_the_summary_and_the_run(first, spark, workdir):
    folder = os.path.join(workdir, "resultados", RUN_ID)
    text = open(os.path.join(folder, "resumen.md"), encoding="utf-8").read()
    assert text.startswith("# Riesgo de mañana: 15.01.2026")
    assert "pedida el 14.01.2026 19:31 UTC" in text
    assert "1 aviso de AEMET para el día en las celdas de la red, el mayor naranja" in text
    assert "## Lo que se prevé" in text and "## Notas" not in text
    assert len(pd.read_csv(os.path.join(folder, "prediccion.csv"))) == 2
    runs = spark.table("l3_gold.ml_runs").where(f"run_id = '{RUN_ID}'").toPandas()
    assert len(runs) == 1 and runs.loc[0, "etiqueta"] == "riesgo_manana:y_dia"
    assert "operativa" in runs.loc[0, "modelos_json"]


def test_a_new_run_of_the_same_day_replaces_it_and_says_when_aemet_is_missing(first, spark, config_path, workdir):
    _, before, _ = first
    landing = land(workdir, DAY, storm=True, warned=False)
    assert rm.main(["--run-id", RUN_ID + "_2", "--config", config_path, "--fecha", f"{DAY:%Y-%m-%d}",
                    "--landing", landing]) == 0
    rows = spark.table("l3_gold.riesgo_dia_siguiente").toPandas().set_index("distribuidora_id")
    assert sorted(rows.index) == [1, 2] and (rows["run_id"] == RUN_ID + "_2").all()
    assert not rows["avisos_disponibles"].any()
    assert rows.loc[1, "aviso_max_nivel"] == 0.0
    # Without the orange warning the storm is still risky, but less.
    assert rows.loc[1, "probabilidad"] < before.loc[1, "probabilidad"]
    text = open(os.path.join(workdir, "resultados", RUN_ID + "_2", "resumen.md"), encoding="utf-8").read()
    assert "AEMET no ha dado los avisos" in text


def test_an_old_copy_of_calser_leaves_the_history_empty(first, spark, config_path, workdir):
    later = DAY + pd.Timedelta(days=40)
    landing = land(workdir, later, storm=False, warned=False)
    assert rm.main(["--run-id", RUN_ID + "_3", "--config", config_path, "--fecha", f"{later:%Y-%m-%d}",
                    "--landing", landing]) == 0
    rows = spark.table("l3_gold.riesgo_dia_siguiente").toPandas()
    assert len(rows) == 4
    new = rows[rows["fecha"].astype(str) == f"{later:%Y-%m-%d}"]
    assert not new["historia_al_dia"].any()
    text = open(os.path.join(workdir, "resultados", RUN_ID + "_3", "resumen.md"), encoding="utf-8").read()
    assert "La copia de Calser llega hasta el 13.01.2026" in text


def test_a_past_day_is_played_again_from_gold_without_touching_the_live_day(first, spark, config_path, workdir):
    """A storm day of 2025 with the forecast of the day before (metp1_*) and the warnings of the archive."""
    weather = spark.table("l3_gold.meteo_celda_hora").toPandas()
    weather["hora"] = pd.to_datetime(weather["hora"])
    of_2025 = weather[(weather["hora"] >= "2025-01-01") & (weather["hora"] < "2025-12-01") & (weather["celda_id"] == CELLS[1])]
    storm_day = of_2025.loc[of_2025["metp1_racha_1h"].idxmax(), "hora"].floor("D")
    live = os.path.join(workdir, "dashboard", "riesgo.json")
    before = open(live, encoding="utf-8").read()
    code = rm.main(["--run-id", "repetido", "--config", config_path, "--fecha", f"{storm_day:%Y-%m-%d}", "--desde-gold",
                    "--solo-archivo", "--landing", os.path.join(workdir, "vivo")])
    assert code == 0
    rows = spark.table("l3_gold.riesgo_dia_siguiente").toPandas()
    played = rows[rows["fecha"].astype(str) == f"{storm_day:%Y-%m-%d}"].set_index("distribuidora_id")
    assert sorted(played.index) == [1, 2] and (played["run_id"] == "repetido").all()
    storm = played.loc[1]
    # The forecast of the day before, the warning sent the day before, training only with the days before.
    assert storm["racha_max_kmh"] == pytest.approx(of_2025["metp1_racha_1h"].max(), abs=0.5)
    assert storm["aviso_max_nivel"] == 2.0 and storm["avisos_disponibles"]
    assert pd.Timestamp(storm["entrenado_hasta"]) < storm_day
    assert storm["prevision_pedida_utc"] == "gold" and storm["nivel"] in ("alto", "muy alto")
    # The dashboard keeps the live day and gets the dated copy.
    assert open(live, encoding="utf-8").read() == before
    dated = os.path.join(workdir, "dashboard", f"riesgo_{storm_day:%Y%m%d}.json")
    payload = json.load(open(dated, encoding="utf-8"))
    assert payload["fecha"] == f"{storm_day:%Y-%m-%d}" and "Gold" in payload["fuentes"]["prevision_pedida"]
    assert os.path.exists(os.path.join(workdir, "dashboard", f"parte_guardia_{storm_day:%Y%m%d}.xlsx"))
    # The index of the dashboard lists the live day and the days played again, newest first, with the files each has.
    index = json.load(open(os.path.join(workdir, "dashboard", "indice.json"), encoding="utf-8"))
    live_day = json.loads(before)["fecha"]
    assert index["vivo"]["fecha"] == live_day and [d["fecha"] for d in index["dias"]] == sorted(
        [d["fecha"] for d in index["dias"]], reverse=True)
    played_entry = next(d for d in index["dias"] if d["dia"] == f"{storm_day:%Y%m%d}")
    assert played_entry["repetido"] and played_entry["run_id"] == "repetido"
    assert played_entry["ficheros"]["parte_guardia"] == f"parte_guardia_{storm_day:%Y%m%d}.xlsx"
    assert "ensemble" not in played_entry["ficheros"]
    entry_1 = next(d for d in played_entry["distribuidoras"] if d["id"] == 1)
    assert entry_1["nivel"] in ("alto", "muy alto") and entry_1["guardia_reforzada"] is True and entry_1["franja"]
    live_entry = next(d for d in index["dias"] if d["dia"] == live_day.replace("-", ""))
    assert not live_entry["repetido"] and "historico.json" not in index["otros"]


def test_without_the_forecast_there_is_nothing_to_score(config_path, workdir):
    with pytest.raises(FileNotFoundError):
        rm.main(["--run-id", "sin_prevision", "--config", config_path, "--fecha", "2026-06-01",
                 "--landing", os.path.join(workdir, "vivo")])


def test_the_second_opinions_of_the_evening_are_attached_to_the_payload(tmp_path):

    payload = {"fecha": "2026-10-15", "distribuidoras": [{"id": 3, "nombre": "EOSA"}, {"id": 2, "nombre": "Pitarch"}]}
    with open(tmp_path / "ensemble.json", "w", encoding="utf-8") as file:
        json.dump({"fecha": "2026-10-15", "distribuidoras": [{"id": 3, "horas": [{"hora": 0, "p10": 1, "p50": 2, "p90": 3}], "texto": "40 escenarios", "pico": {"p10": 30, "p50": 40, "p90": 55},
                                                             "supera": {"50": 0.2}, "miembros": 40, "modelo": "icon_eu"},
                                                            {"id": 2, "horas": [], "texto": "sin previsión por conjuntos para el día"}]}, file)
    with open(tmp_path / "aemet.json", "w", encoding="utf-8") as file:
        json.dump({"fecha": "2026-10-14", "distribuidoras": [{"id": 3, "horas": [{"hora": 0, "racha_kmh": 10}], "texto": "de ayer"}]}, file)
    out = rm.attach_second_opinions(payload, str(tmp_path))
    eosa, pitarch = out["distribuidoras"]
    assert eosa["escenarios"] == {"texto": "40 escenarios", "pico": {"p10": 30, "p50": 40, "p90": 55}, "supera": {"50": 0.2}, "miembros": 40, "modelo": "icon_eu"}
    assert "escenarios" not in pitarch and "aemet" not in eosa  # no hours for Pitarch; the AEMET file is of another day
    assert rm.attach_second_opinions({"fecha": "2026-10-15", "distribuidoras": [{"id": 3}]}, str(tmp_path / "no")) == {"fecha": "2026-10-15", "distribuidoras": [{"id": 3}]}
    sheets = rm.rz.shift_sheets({**out, "fecha_texto": "15.10.2026"})
    assert sheets["Resumen"]["escenarios de ICON-EU EPS"].tolist() == ["40 escenarios", None]


def test_the_index_of_the_dashboard_survives_a_broken_file(tmp_path):
    folder = str(tmp_path)
    good = {"fecha": "2026-02-03", "fecha_texto": "03.02.2026", "generado": "02.02.2026 18:31 UTC", "run_id": "scheduled__x",
            "fuentes": {"prevision_pedida": "02.02.2026 18:31 UTC"}, "modelo": {"descripcion": "regresión logística"},
            "distribuidoras": [{"id": 3, "nombre": "EOSA", "nivel": "alto", "probabilidad": 0.12, "percentil": 0.95,
                                "aviso_max_nivel": 1.0, "guardia": {"reforzar": True, "motivo": "nivel alto"},
                                "franja": {"texto": "de 6:00 a 14:00"}, "racha_max_kmh": 88.0}]}
    with open(os.path.join(folder, "riesgo_20260203.json"), "w", encoding="utf-8") as file:
        json.dump(good, file)
    with open(os.path.join(folder, "riesgo_20260101.json"), "w", encoding="utf-8") as file:
        file.write("{ not json")
    with open(os.path.join(folder, "riesgo_prueba.json"), "w", encoding="utf-8") as file:
        json.dump({}, file)
    rm.write_index(folder)
    index = json.load(open(os.path.join(folder, "indice.json"), encoding="utf-8"))
    assert [d["dia"] for d in index["dias"]] == ["20260203"] and index["vivo"] is None
    entry = index["dias"][0]
    assert entry["fecha_texto"] == "03.02.2026" and not entry["repetido"] and entry["ficheros"] == {}
    assert entry["distribuidoras"][0]["guardia_motivo"] == "nivel alto"
