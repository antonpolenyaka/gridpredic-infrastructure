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
    places = [(1, CELLS[1], "Montánchez")] * 3 + [(1, CELLS[1], "Torre de Santa María")] + [(2, CELLS[2], "Zafra")] * 3
    save(spark, pd.DataFrame([{"distribuidora_id": d, "ct_id": f"{d}-{i}", "celda_id": c, "municipio_nombre": m}
                              for i, (d, c, m) in enumerate(places)]), "l3_gold.map_ct_celda")
    facts, causes = [], []
    names = {1: "Calser_EOSA", 2: "Calser_Pitarch"}
    for k, (dist, start, cause) in enumerate(events):
        period, inc = start.strftime("%Y%m"), f"E{k}"
        facts.append({"distribuidora_id": dist, "source_database": names[dist], "periodo_id": period,
                      "incidencia_id": inc, "ct_id": f"{dist}-0", "inicio_ts": start,
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
    assert rows["avisos_disponibles"].all() and rows["historia_al_dia"].all()
    assert (rows["cobertura_prevision"] == 1.0).all()
    assert str(storm["calser_hasta"]) == "2026-01-13"
    # Labels, weather and warnings: the archive of warnings ends first.
    assert str(storm["entrenado_hasta"]) <= "2025-12-29"
    assert storm["dias_entreno"] > 1500 and storm["modelo"] == "logistica" and storm["variante"] == "operativa"


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


def test_without_the_forecast_there_is_nothing_to_score(config_path, workdir):
    with pytest.raises(FileNotFoundError):
        rm.main(["--run-id", "sin_prevision", "--config", config_path, "--fecha", "2026-06-01",
                 "--landing", os.path.join(workdir, "vivo")])
