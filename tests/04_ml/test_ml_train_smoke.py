"""
End to end test of the training job on a local Spark, without Docker.

Builds a small synthetic l3_gold.dataset_train (two distribuidoras, three
splits, a training sample with peso_muestra) and its row in dataset_versions,
runs job_ml_train with small grids and --evaluar-test, and checks every
output table and the saved models.

Requirements (same versions as the Spark image):
    pip install pyspark==4.2.0 delta-spark==4.4.0 pytest pandas pyarrow scikit-learn xgboost-cpu
Run from the root of the repository:
    python -m pytest tests/04_ml -q
"""

import json
import os
import pickle
import shutil
import sys
import tempfile
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
SILVER_DIR = os.path.join(ROOT, "etl", "jobs", "02_silver")
GOLD_DIR = os.path.join(ROOT, "etl", "jobs", "03_gold")
GOLD_CONFIG = os.path.join(ROOT, "etl", "config", "03_gold", "config_gold.json")
CONFIG = os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json")

for folder in (ML_DIR, SILVER_DIR, GOLD_DIR):
    if folder not in sys.path:
        sys.path.insert(0, folder)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

os.environ["TZ"] = "UTC"
time.tzset()

VERSION = "ds_test_ml"
RATE = 0.3
FEATURES = [
    "hist_interr_365d", "hist_interr_90d", "hist_interr_30d", "ev_defecto_tierra_24h",
    "med_intensidad_max", "ct_tipo_zona", "ct_tiene_medidas", "hora_del_dia",
]
PERIODS = {
    "train": ("2024-11-01", 50),
    "valid": ("2025-06-01", 25),
    "test": ("2026-01-05", 20),
}


@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="ml_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("ml-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sql("CREATE DATABASE IF NOT EXISTS l3_gold")
    session.sql("CREATE DATABASE IF NOT EXISTS l1_bronze")
    session.sql("CREATE DATABASE IF NOT EXISTS l2_silver")
    yield session
    session.stop()


def synthetic_dataset(seed=3) -> pd.DataFrame:
    """
    30 CTs, hourly rows in three periods. The risk of a CT grows with its
    history and with the earth faults of the last 24 h; a risky hour starts
    an episode of three positive hours, as y_1_3h does around a real start.
    """
    rng = np.random.default_rng(seed)
    cts = pd.DataFrame({
        "distribuidora_id": np.repeat([1, 2], 15),
        "ct_id": [f"CT{i:03d}" for i in range(30)],
        "ct_tipo_zona": rng.integers(1, 5, 30).astype(float),
        "ct_tiene_medidas": rng.integers(0, 2, 30).astype(float),
        "hist_interr_365d": rng.poisson(2.0, 30).astype(float),
    })

    frames = []

    for split, (start, days) in PERIODS.items():
        hours = pd.date_range(start, periods=days * 24, freq="h")
        grid = cts.merge(pd.DataFrame({"hora": hours}), how="cross")
        grid["split"] = split
        frames.append(grid)

    df = pd.concat(frames, ignore_index=True).sort_values(["ct_id", "hora"]).reset_index(drop=True)
    n = len(df)

    df["hist_interr_90d"] = np.minimum(df["hist_interr_365d"], rng.poisson(0.5, n))
    df["hist_interr_30d"] = np.minimum(df["hist_interr_90d"], rng.poisson(0.2, n))
    df["ev_defecto_tierra_24h"] = rng.poisson(0.1, n).astype(float)
    df["med_intensidad_max"] = np.where(df["ct_tiene_medidas"] == 1, rng.normal(100, 15, n), np.nan)
    df["hora_del_dia"] = df["hora"].dt.hour.astype(float)

    logit = -8.0 + 0.5 * df["hist_interr_365d"] + 2.5 * df["ev_defecto_tierra_24h"]
    start = rng.random(n) < 1 / (1 + np.exp(-logit))

    y = np.zeros(n, dtype=int)
    lead = np.full(n, np.nan)
    same_ct = df["ct_id"].to_numpy()

    for i in np.flatnonzero(start):
        for back, hours_left in ((0, 1.5), (1, 2.5), (2, 3.0)):
            j = i - back
            if j >= 0 and same_ct[j] == same_ct[i]:
                y[j] = 1
                lead[j] = hours_left

    df["y_1_3h"] = y
    df["y_1_3h_local"] = y * (rng.random(n) < 0.3)
    df["horas_hasta_proximo_evento"] = lead

    draw = rng.random(n)
    df["en_muestra_train"] = (df["split"] == "train") & ((df["y_1_3h"] == 1) | (draw < RATE))
    df["peso_muestra"] = np.where((df["split"] == "train") & (df["y_1_3h"] == 0), 1 / RATE, 1.0)
    df["dataset_version"] = VERSION
    df["fecha_mes"] = df["hora"].dt.to_period("M").dt.to_timestamp().dt.date
    return df


@pytest.fixture(scope="module")
def dataset(spark):
    pdf = synthetic_dataset()
    (
        spark.createDataFrame(pdf)
        .withColumn("distribuidora_id", F.col("distribuidora_id").cast("int"))
        .write.format("delta").mode("overwrite").partitionBy("split", "fecha_mes")
        .saveAsTable("l3_gold.dataset_train")
    )

    sizes = {
        split: {"filas": int(len(g)), "positivos": int(g["y_1_3h"].sum())}
        for split, g in pdf.groupby("split")
    }
    version = pd.DataFrame([{
        "dataset_version": VERSION,
        "run_id": "test",
        "creado_ts": datetime(2026, 10, 10, 6, 0, 0),
        "params_hash": "x",
        "params_json": json.dumps({"dataset": {"tasa_negativos_train": RATE}}),
        "etiqueta_principal": "y_1_3h",
        "ventana_desde": "2024-11-01",
        "ventana_hasta": "2026-01-25",
        "n_features": len(FEATURES),
        "features_json": json.dumps(FEATURES),
        "versiones_delta_json": "{}",
        "filas_json": json.dumps(sizes),
    }])
    spark.createDataFrame(version).write.format("delta").mode("overwrite").saveAsTable("l3_gold.dataset_versions")
    load_weather(spark, pdf)
    return pdf


def load_weather(spark, pdf):
    """
    dim_ct with the municipality of every CT and the weather tables of Bronze.
    CT000 - CT019 match the reference by INE code, CT020 - CT027 only by name
    (accents and case differ), CT028 and CT029 do not match.
    """
    cts = pdf[["distribuidora_id", "ct_id"]].drop_duplicates().reset_index(drop=True)
    cts["municipio_id"] = ["10001" if i < 10 else "10002" if i < 20 else "99999" for i in range(len(cts))]
    cts["municipio_nombre"] = ["Abadía" if i < 10 else "Ahigal" if i < 20 else "ALBALÁ" if i < 28 else "Ninguno"
                               for i in range(len(cts))]
    spark.createDataFrame(cts).withColumn("distribuidora_id", F.col("distribuidora_id").cast("int")) \
        .write.format("delta").mode("overwrite").saveAsTable("l3_gold.dim_ct")

    cells = pd.DataFrame([
        {"codigo_ine": "10001", "municipio": "Abadía", "distribuidora_ref": "1", "latitud": 40.26, "longitud": -5.98,
         "celda_id": "40.30_-6.00", "celda_latitud": 40.3, "celda_longitud": -6.0},
        {"codigo_ine": "10002", "municipio": "Ahigal", "distribuidora_ref": "1", "latitud": 40.19, "longitud": -6.19,
         "celda_id": "40.20_-6.20", "celda_latitud": 40.2, "celda_longitud": -6.2},
        {"codigo_ine": "10007", "municipio": "Albalá", "distribuidora_ref": "1", "latitud": 39.26, "longitud": -6.19,
         "celda_id": "39.30_-6.20", "celda_latitud": 39.3, "celda_longitud": -6.2},
    ])
    spark.createDataFrame(cells).write.format("delta").mode("overwrite").saveAsTable("l1_bronze.meteo_openmeteo_celdas")

    rng = np.random.default_rng(9)
    hours = pd.date_range(pdf["hora"].min() - pd.Timedelta(days=2), pdf["hora"].max() + pd.Timedelta(days=1), freq="h")
    frames = []
    for cell in cells["celda_id"]:
        n = len(hours)
        frames.append(pd.DataFrame({
            "celda_id": cell, "hora": hours,
            "temperature_2m": rng.normal(12, 6, n), "relative_humidity_2m": rng.uniform(30, 100, n),
            "precipitation": rng.exponential(0.2, n), "snowfall": np.zeros(n),
            "wind_speed_10m": rng.gamma(2, 6, n), "wind_gusts_10m": rng.gamma(2, 12, n),
            "pressure_msl": rng.normal(1015, 6, n),
        }))
    weather = pd.concat(frames, ignore_index=True)
    weather["anio"] = weather["hora"].dt.year
    weather["modelo"] = "ecmwf_ifs"
    weather["audit_loaded_at"] = pd.Timestamp("2026-10-10 23:00")
    spark.createDataFrame(weather).write.format("delta").mode("overwrite").saveAsTable("l1_bronze.meteo_openmeteo_hora")


@pytest.fixture(scope="module")
def config_path(workdir):
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)

    config["n_jobs"] = 2
    config["muestra_valid"]["tasa_negativos"] = 0.5
    config["busqueda"] = {
        "logistica": [{"C": 0.1}, {"C": 1.0}],
        "random_forest": [{"n_estimators": 20, "max_depth": 6, "min_samples_leaf": 5}],
        "xgboost": [
            {"n_estimators": 60, "max_depth": 3, "early_stopping_rounds": 10, "verbose": 0},
            {"n_estimators": 60, "max_depth": 4, "early_stopping_rounds": 10, "verbose": 0},
        ],
    }
    config["evaluacion"].update({"presupuestos_alertas_dia": [2, 5], "filas_shap": 500, "test_hasta": "2026-01-20"})
    config["artefactos_uri"] = os.path.join(workdir, "modelos")
    config["exportar_dir"] = os.path.join(workdir, "resultados")
    # The synthetic dataset only has 24 h event features and no features_ct_hora:
    # the context is built from dataset_train with that window.
    config["contexto_red"] = {"activo": True, "prefijos": ["ev_"], "ventanas": ["24h"], "region": True}
    config["clusters"].update({"k": [2, 3], "min_cts": 5})
    config["submodelos"] = {"activo": True, "min_positivos": 20}
    config["spark_conf"] = {}

    path = os.path.join(workdir, "config_ml_test.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file)

    return path


def run_weather_layers():
    """Silver and Gold of the weather, as the DAGs would run them."""
    import importlib

    for name, argv in (("job_silver_d_meteo_celda", []), ("job_silver_f_meteo_hora", []),
                       ("job_gold_meteo_celda_hora", ["--config", GOLD_CONFIG])):
        module = importlib.import_module(name)
        old = sys.argv
        sys.argv = [f"{name}.py", "--run-id", "test_ml", *argv]
        try:
            module.main()
        finally:
            sys.argv = old


@pytest.fixture(scope="module")
def run(spark, dataset, config_path):
    import job_ml_train

    run_weather_layers()

    return job_ml_train.main(["--run-id", "ml_test", "--config", config_path, "--evaluar-test"])


def test_run_row(spark, run):
    row = spark.table("l3_gold.ml_runs").where(F.col("run_id") == run).collect()
    assert len(row) == 1

    row = row[0]
    assert row["dataset_version"] == VERSION
    assert row["splits_evaluados"] == "valid,test"

    models = json.loads(row["modelos_json"])
    assert set(models) == {"tasa_base", "naif_historico", "logistica", "random_forest", "xgboost", "xgboost_cluster"}
    assert "mejor_iteracion" in models["xgboost"]

    search = json.loads(row["busqueda_json"])
    assert len([s for s in search if s["modelo"] == "logistica"]) == 2
    assert len([s for s in search if s["modelo"] == "xgboost"]) == 2


def test_predictions_cover_valid_and_test(spark, dataset, run):
    predictions = spark.table("l3_gold.ml_predicciones").where(F.col("run_id") == run)
    counts = {r["split"]: r["count"] for r in predictions.groupBy("split").count().collect()}

    assert counts["valid"] == int((dataset["split"] == "valid").sum())
    assert counts["test"] == int((dataset["split"] == "test").sum())

    sample = predictions.select("p_xgboost", "p_logistica", "p_random_forest").toPandas()
    assert sample.notna().all().all()
    assert ((sample >= 0) & (sample <= 1)).all().all()


def test_metrics(spark, dataset, run):
    metrics = spark.table("l3_gold.ml_metricas").where(F.col("run_id") == run).toPandas()
    total = metrics[(metrics["segmento"] == "total") & (metrics["etiqueta"] == "y_1_3h")]
    pr_auc = total[total["metrica"] == "pr_auc"].set_index(["split", "modelo"])["valor"]

    valid = dataset[dataset["split"] == "valid"]
    assert pr_auc[("valid", "tasa_base")] == pytest.approx(valid["y_1_3h"].mean(), rel=1e-6)

    # The history explains only part of the synthetic risk: the naive
    # reference is above the base rate, the trained models well above.
    assert pr_auc[("valid", "naif_historico")] > pr_auc[("valid", "tasa_base")]

    for model in ("logistica", "random_forest", "xgboost"):
        assert pr_auc[("valid", model)] > 1.5 * pr_auc[("valid", "tasa_base")], model

    # test_hasta leaves the last days of test out of the evaluation.
    test = dataset[(dataset["split"] == "test") & (dataset["hora"] < "2026-01-20")]
    n_test = total[(total["split"] == "test") & (total["metrica"] == "pr_auc")]["n_filas"].iloc[0]
    assert n_test == len(test)

    segments = set(metrics["segmento"])
    assert {"total", "distribuidora", "tipo_zona", "telemetria"} <= segments
    assert {"y_1_3h", "y_1_3h_local"} <= set(metrics["etiqueta"])
    assert "recall_episodios@5" in set(metrics["metrica"])
    assert "antelacion_media_h@5" in set(metrics["metrica"])

    recall = total[(total["split"] == "valid") & (total["metrica"] == "recall_episodios@5")]
    assert recall["valor"].between(0, 1).all()


def test_importance(spark, run):
    importance = spark.table("l3_gold.ml_importancia").where(F.col("run_id") == run).toPandas()
    metrics = set(zip(importance["modelo"], importance["metrica"]))

    assert ("xgboost", "shap_medio_abs") in metrics
    assert ("xgboost", "gain") in metrics
    assert ("random_forest", "importancia_impureza") in metrics
    assert ("logistica", "coef_abs") in metrics
    assert ("feature_sola", "pr_auc_univariante") in metrics

    features = set(importance["feature"])
    assert {"red_frac_ev_defecto_tierra_24h", "region_frac_ev_defecto_tierra_24h"} <= features

    shap = importance[(importance["modelo"] == "xgboost") & (importance["metrica"] == "shap_medio_abs")]
    top = shap.sort_values("valor", ascending=False)["feature"].head(2).tolist()
    assert "ev_defecto_tierra_24h" in top or "hist_interr_365d" in top


def test_saved_models(workdir, run):
    folder = os.path.join(workdir, "modelos", run)
    files = set(os.listdir(folder))

    assert {"xgboost.pkl", "random_forest.pkl", "logistica.pkl", "run.json", "xgboost_booster.json"} <= files

    with open(os.path.join(folder, "xgboost.pkl"), "rb") as file:
        scorer = pickle.load(file)

    # plus the network context (2), estacion, the coordinates of the
    # municipality (2), ct_cluster and the weather
    names = scorer.feature_names
    assert names[:len(FEATURES)] == FEATURES
    assert {"estacion", "ct_cluster", "ct_lat_municipio", "met_racha_max_6h", "metprev_racha_max_3h",
            "met_region_racha_max_3h"} <= set(names)
    assert len([n for n in names if n.startswith("met")]) >= 18
    X = np.zeros((3, len(scorer.feature_names)), dtype=np.float32)
    assert scorer.score(X).shape == (3,)


def test_exported_summary(workdir, run):
    folder = os.path.join(workdir, "resultados", run)

    assert {"resumen.md", "metricas.csv", "importancia.csv", "busqueda.json"} <= set(os.listdir(folder))

    with open(os.path.join(folder, "resumen.md"), encoding="utf-8") as file:
        report = file.read()

    for heading in ("## valid: y_1_3h", "## test: y_1_3h", "solo eventos locales", "PR-AUC por distribuidora",
                    "SHAP medio absoluto", "## Búsqueda"):
        assert heading in report, heading

    assert "| xgboost |" in report
    assert len(pd.read_csv(os.path.join(folder, "metricas.csv"))) > 100


def test_groups_of_cts(spark, dataset, run, workdir):
    groups = spark.table("l3_gold.ml_clusters_ct").where(F.col("run_id") == run).toPandas()
    assert len(groups) == 30
    assert groups["ct_cluster"].nunique() in (2, 3)

    predictions = spark.table("l3_gold.ml_predicciones").where(F.col("run_id") == run)
    assert "p_xgboost_cluster" in predictions.columns
    assert "ct_cluster" in predictions.columns

    metrics = spark.table("l3_gold.ml_metricas").where(F.col("run_id") == run).toPandas()
    by_group = metrics[(metrics["segmento"] == "grupo_ct") & (metrics["metrica"] == "pr_auc")]
    assert set(by_group["modelo"]) >= {"xgboost", "xgboost_cluster"}

    search = json.loads(spark.table("l3_gold.ml_runs").where(F.col("run_id") == run).first()["busqueda_json"])
    rows = [s for s in search if s["modelo"] == "xgboost_cluster"]
    assert len(rows) == groups["ct_cluster"].nunique()
    assert all("pr_auc_global_en_grupo" in r for r in rows)

    with open(os.path.join(workdir, "resultados", run, "resumen.md"), encoding="utf-8") as file:
        report = file.read()
    assert "## Grupos de CT" in report
    assert "por grupo de CT" in report


def test_weather_features(spark, dataset, run):
    weather = spark.table("l3_gold.meteo_celda_hora").toPandas().sort_values(["celda_id", "hora"])
    cell = weather[weather["celda_id"] == "40.30_-6.00"].set_index("hora")
    raw = spark.table("l1_bronze.meteo_openmeteo_hora").where(F.col("celda_id") == "40.30_-6.00") \
        .toPandas().set_index("hora").sort_index()

    when = raw.index[100]
    assert cell.loc[when, "met_racha_1h"] == pytest.approx(raw.loc[when, "wind_gusts_10m"])
    # The last 6 hours end at hora (included) and the forecast starts after it.
    assert cell.loc[when, "met_racha_max_6h"] == pytest.approx(raw["wind_gusts_10m"].iloc[95:101].max())
    assert cell.loc[when, "metprev_racha_max_3h"] == pytest.approx(raw["wind_gusts_10m"].iloc[101:104].max())
    assert cell.loc[when, "met_lluvia_24h"] == pytest.approx(raw["precipitation"].iloc[77:101].sum())
    assert cell.loc[when, "met_presion_delta_3h"] == pytest.approx(
        raw["pressure_msl"].iloc[100] - raw["pressure_msl"].iloc[97])

    groups = spark.table("l3_gold.ml_clusters_ct").toPandas()
    assert len(groups) == 30

    located = spark.table("l3_gold.map_ct_celda").toPandas().set_index("ct_id")
    assert located["cruce"].value_counts().to_dict() == {"ine": 20, "nombre": 8, "sin_celda": 2}
    assert located.loc["CT025", "celda_id"] == "39.30_-6.20"

    silver = spark.table("l2_silver.f_meteo_hora")
    assert {"racha_kmh", "precipitacion_mm", "fuera_rango", "racha_menor_viento"} <= set(silver.columns)
    metrics = spark.table("l2_silver.dq_metrics").where(F.col("entidad") == "f_meteo_hora").toPandas()
    assert "huecos_horas" in set(metrics["metrica"])


def test_wrong_dataset_version_is_refused(spark, dataset, config_path):
    import job_ml_train

    with pytest.raises(ValueError):
        job_ml_train.main(["--config", config_path, "--dataset-version", "ds_que_no_existe"])
