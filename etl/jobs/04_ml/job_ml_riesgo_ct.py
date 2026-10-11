"""
Risk of every CT and feeder for the next year: which ones will have the
unplanned interruptions of the year (where to send the inspection, the pruning
or the reinforcement). It is the question an operator asks once a year, with a
horizon the history of Calser can teach.

Unit: CT and year Y. Label: kVA·h of the unplanned interruptions of the CT in Y
(principal variant of fact_interrupciones_mt, without the days of
analisis_red.dias_excluidos) and whether it had any.

Features, all known before Y starts:

- historia: kVA·h, interruptions and hours of the last 1, 2 and 3 years of the
  CT, and the kVA·h per installed kVA of its feeder (cabecera), network group
  and municipality, because the fault of a line takes all its CTs; years since
  the last interruption of the CT.
- estructura: kVA, customers, zone type, telemetry, CTs of its group, CTs and
  kVA of its feeder, upstream bays.
- scada: cuts, microcuts (180 s or less) and reclosures of the CT and of its
  feeder in the year before (fact_cortes_scada; null before the history of
  the SCADA starts).
- publicos: l3_gold.contexto_ct_anio (population and its change, density,
  altitude, network per inhabitant, relief and OpenStreetMap exposure of the
  cell).
- clima: hours with gusts of 70 km/h or more and days with 30 mm of rain or
  more per year in the cell of the CT, mean of the years before Y.

Evaluation by rolling origin: for each Y of riesgo_ct.anios_evaluacion the
model learns from the years before and ranks the CTs of Y. The metric is the
capture: the share of the kVA·h of Y that falls in the top k % of the ranking,
against the naive ranking (kVA·h of the two years before) and the oracle (the
best possible order). The same at feeder level, adding the scores of its CTs.
Ablation by blocks of features. Model: XGBoost with Tweedie objective on the
kVA·h (most CTs have none in a year and a few concentrate most of them).

Last, a model with every year ranks the CTs and feeders for the year after the
last one with data: prioridades_ct.csv and prioridades_cabecera.csv, and
l3_gold.ml_riesgo_ct_ranking.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,/app/jobs/04_ml/job_analisis_red.py \\
        /app/jobs/04_ml/job_ml_riesgo_ct.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import json
import math
import os
import sys
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))

if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_analisis_red import Data, Report, md_table, pct, read_table, settings  # noqa: E402
from job_ml_train import GOLD, RUNS_TABLE, get_spark, load_config, logger, save_frame  # noqa: E402


SCADA_TABLE = f"{GOLD}.fact_cortes_scada"
CONTEXT_TABLE = f"{GOLD}.contexto_ct_anio"
WEATHER_TABLE = f"{GOLD}.meteo_celda_hora"
CELL_TABLE = f"{GOLD}.map_ct_celda"
RANKING_TABLE = f"{GOLD}.ml_riesgo_ct_ranking"

DEFAULTS = {
    "anio_primero": 2018,
    "anios_evaluacion": [2021, 2022, 2023, 2024, 2025],
    "fracciones_top": [0.05, 0.1, 0.2],
    "umbral_racha_kmh": 70,
    "umbral_lluvia_mm": 30,
    "xgboost": {"n_estimators": 300, "max_depth": 4, "learning_rate": 0.05, "subsample": 0.8,
                "colsample_bytree": 0.8, "min_child_weight": 5, "objective": "reg:tweedie",
                "tweedie_variance_power": 1.3},
    "bloques": ["historia", "historia+estructura", "historia+estructura+scada", "historia+estructura+publicos",
                "historia+estructura+clima", "todo"],
}

LEVELS = {"ct": ["distribuidora_id", "ct_id"], "cabecera": ["distribuidora_id", "cabecera"],
          "grupo": ["distribuidora_id", "grupo_red_id"], "municipio": ["distribuidora_id", "municipio_id"]}

STRUCTURE = ["kva", "num_abonados", "zona", "tiene_telemetria", "n_ct_grupo", "cts_cabecera", "kva_cabecera",
             "n_posiciones_aguas_arriba"]
PUBLIC = ["pob_municipio", "pob_crec_1a", "pob_crec_5a_anual", "densidad_hab_km2", "elevacion_municipio_m",
          "kva_municipio_por_hab", "abonados_municipio_por_hab", "celda_elev_media_m", "celda_elev_std_m",
          "celda_desnivel_m", "celda_dens_linea_aerea_km_km2", "celda_dens_apoyos_km2", "celda_dens_carreteras_km_km2",
          "celda_dens_pistas_km_km2", "celda_pct_bosque", "celda_pct_matorral", "celda_dens_edificios_km2",
          "celda_pct_cable", "celda_km2_obras"]
CLIMATE = ["clima_horas_racha", "clima_dias_lluvia"]


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    args, _ = parser.parse_known_args(argv)
    if not args.run_id:
        args.run_id = "rct_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    return args


# ---------------------------------------------------------------------------
# Table CT x year
# ---------------------------------------------------------------------------

def yearly_events(data: Data) -> pd.DataFrame:
    ev = data.events[data.events["variante"] == "principal"]
    return ev.groupby(["distribuidora_id", "ct_id", "anio"]).agg(
        kva_h=("kva_h", "sum"), interrupciones=("ct_id", "size"), horas=("dur_h", "sum")).reset_index()


def history(per_ct: pd.DataFrame, grid: pd.DataFrame, dim: pd.DataFrame, level: str, years: tuple) -> pd.DataFrame:
    """Sum of kVA·h (and interruptions) of the level in the years Y-k for k in years, for every row of grid."""
    keys = LEVELS[level]
    events = per_ct.merge(dim[["distribuidora_id", "ct_id", *[k for k in keys if k not in ("distribuidora_id",
                                                                                         "ct_id")]]],
                          on=["distribuidora_id", "ct_id"], how="left") if level != "ct" else per_ct
    by_level = events.groupby(keys + ["anio"])[["kva_h", "interrupciones", "horas"]].sum().reset_index()
    out = grid[keys + ["anio"]].drop_duplicates()
    for span in years:
        parts = []
        for k in range(1, span + 1):
            shifted = by_level.assign(anio=by_level["anio"] + k)
            parts.append(shifted)
        window = pd.concat(parts).groupby(keys + ["anio"])[["kva_h", "interrupciones", "horas"]].sum()
        window.columns = [f"{c}_{level}_{span}a" for c in window.columns]
        out = out.merge(window.reset_index(), on=keys + ["anio"], how="left")
    return out.fillna(0.0)


def scada_counts(spark, dim: pd.DataFrame) -> pd.DataFrame:
    df = read_table(spark, SCADA_TABLE, ["distribuidora_id", "ct_id", "inicio_ts", "es_microcorte", "es_maniobra",
                                         "es_corte_etiqueta"])
    if df is None:
        return pd.DataFrame()
    pdf = df.where(~F.coalesce(F.col("es_maniobra"), F.lit(False))).groupBy(
        "distribuidora_id", "ct_id", F.year("inicio_ts").alias("anio")).agg(
        F.sum(F.col("es_microcorte").cast("int")).alias("microcortes"),
        F.sum(F.col("es_corte_etiqueta").cast("int")).alias("cortes_scada"),
        F.count(F.lit(1)).alias("episodios_scada"),
    ).toPandas()
    pdf["ct_id"] = pdf["ct_id"].astype(str)
    return pdf


def climate_features(spark, cfg: dict) -> pd.DataFrame:
    weather = read_table(spark, WEATHER_TABLE, ["celda_id", "hora", "met_racha_1h", "met_lluvia_1h"])
    if weather is None:
        return pd.DataFrame()
    gust, rain = float(cfg["umbral_racha_kmh"]), float(cfg["umbral_lluvia_mm"])
    per_day = weather.groupBy("celda_id", F.to_date("hora").alias("dia")).agg(
        F.sum((F.col("met_racha_1h") >= gust).cast("int")).alias("horas_racha"),
        F.sum("met_lluvia_1h").alias("lluvia"), F.count(F.lit(1)).alias("horas"))
    per_year = per_day.groupBy("celda_id", F.year("dia").alias("anio")).agg(
        F.sum("horas_racha").alias("horas_racha"), F.sum((F.col("lluvia") >= rain).cast("int")).alias("dias_lluvia"),
        F.sum("horas").alias("horas")).toPandas()
    # Only years with most of their hours.
    return per_year[per_year["horas"] >= 0.9 * 8760]


def build_table(data: Data, cfg: dict, last_year: int) -> pd.DataFrame:
    spark = data.spark
    dim = data.dim.copy()
    dim["municipio_id"] = dim["municipio_id"].astype(str)
    per_feeder = dim.groupby(["distribuidora_id", "cabecera"]).agg(cts_cabecera=("ct_id", "nunique"),
                                                                  kva_cabecera=("kva", "sum")).reset_index()
    dim = dim.merge(per_feeder, on=["distribuidora_id", "cabecera"], how="left")
    upstream = read_table(spark, f"{GOLD}.dim_ct", ["distribuidora_id", "ct_id", "n_posiciones_aguas_arriba"])
    if upstream is not None:
        up = upstream.toPandas()
        up["ct_id"] = up["ct_id"].astype(str)
        dim = dim.drop(columns=[c for c in ["n_posiciones_aguas_arriba"] if c in dim.columns]).merge(
            up, on=["distribuidora_id", "ct_id"], how="left")
    else:
        dim["n_posiciones_aguas_arriba"] = np.nan

    years = list(range(int(cfg["anio_primero"]), last_year + 2))
    rows = []
    for year in years:
        lo, hi = pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year + 1, month=1, day=1)
        valid = dim[(dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi))
                    & (dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo))]
        rows.append(valid.assign(anio=year))
    grid = pd.concat(rows, ignore_index=True)

    per_ct = yearly_events(data)
    labels = per_ct.rename(columns={"kva_h": "y_kva_h", "interrupciones": "y_interrupciones", "horas": "y_horas"})
    grid = grid.merge(labels, on=["distribuidora_id", "ct_id", "anio"], how="left")
    grid[["y_kva_h", "y_interrupciones", "y_horas"]] = grid[["y_kva_h", "y_interrupciones", "y_horas"]].fillna(0.0)
    grid.loc[grid["anio"] > last_year, ["y_kva_h", "y_interrupciones", "y_horas"]] = np.nan

    for level in LEVELS:
        hist = history(per_ct, grid, dim, level, (1, 2, 3))
        grid = grid.merge(hist, on=LEVELS[level] + ["anio"], how="left")

    # Network levels in kVA·h per installed kVA, so a big feeder does not look worse only by its size.
    for level in ("cabecera", "grupo", "municipio"):
        size = grid.groupby(LEVELS[level] + ["anio"])["kva"].transform("sum").replace(0, np.nan)
        for span in (1, 2, 3):
            grid[f"tiepi_{level}_{span}a"] = grid[f"kva_h_{level}_{span}a"] / size

    last = per_ct[per_ct["kva_h"] > 0][["distribuidora_id", "ct_id", "anio"]].rename(columns={"anio": "_ultimo"})
    since = grid[["distribuidora_id", "ct_id", "anio"]].merge(last, on=["distribuidora_id", "ct_id"], how="left")
    since = since[since["_ultimo"] < since["anio"]]
    since = since.groupby(["distribuidora_id", "ct_id", "anio"])["_ultimo"].max().reset_index()
    grid = grid.merge(since, on=["distribuidora_id", "ct_id", "anio"], how="left")
    grid["anios_desde_ultima"] = (grid["anio"] - grid["_ultimo"]).clip(upper=10).fillna(10.0)
    grid = grid.drop(columns=["_ultimo"])

    scada = scada_counts(spark, dim)
    if not scada.empty:
        first_scada = int(scada["anio"].min())
        s = scada.assign(anio=scada["anio"] + 1)
        grid = grid.merge(s.rename(columns={"microcortes": "microcortes_ct_1a", "cortes_scada": "cortes_scada_ct_1a",
                                            "episodios_scada": "episodios_scada_ct_1a"}),
                          on=["distribuidora_id", "ct_id", "anio"], how="left")
        feeder = s.merge(dim[["distribuidora_id", "ct_id", "cabecera"]], on=["distribuidora_id", "ct_id"])
        feeder = feeder.groupby(["distribuidora_id", "cabecera", "anio"])[["microcortes", "cortes_scada"]].sum()
        feeder.columns = ["microcortes_cabecera_1a", "cortes_scada_cabecera_1a"]
        grid = grid.merge(feeder.reset_index(), on=["distribuidora_id", "cabecera", "anio"], how="left")
        scada_cols = ["microcortes_ct_1a", "cortes_scada_ct_1a", "episodios_scada_ct_1a", "microcortes_cabecera_1a",
                      "cortes_scada_cabecera_1a"]
        known = grid["anio"] > first_scada
        for column in scada_cols:
            grid[column] = grid[column].where(~known | grid[column].notna(), 0.0).where(known, np.nan)

    context = read_table(spark, CONTEXT_TABLE)
    if context is not None:
        ctx = context.select("distribuidora_id", "ct_id", "anio", *[c for c in PUBLIC if c in context.columns]
                             ).toPandas()
        ctx["ct_id"] = ctx["ct_id"].astype(str)
        grid = grid.merge(ctx, on=["distribuidora_id", "ct_id", "anio"], how="left")

    clim = climate_features(spark, cfg)
    cells = read_table(spark, CELL_TABLE, ["distribuidora_id", "ct_id", "celda_id"])
    if not clim.empty and cells is not None:
        cell_of = cells.toPandas()
        cell_of["ct_id"] = cell_of["ct_id"].astype(str)
        grid = grid.merge(cell_of, on=["distribuidora_id", "ct_id"], how="left")
        rows = []
        for year in sorted(grid["anio"].unique()):
            before = clim[clim["anio"] < year]
            if before.empty:
                continue
            mean = before.groupby("celda_id")[["horas_racha", "dias_lluvia"]].mean().reset_index()
            rows.append(mean.assign(anio=year))
        if rows:
            means = pd.concat(rows).rename(columns={"horas_racha": "clima_horas_racha", "dias_lluvia": "clima_dias_lluvia"})
            grid = grid.merge(means, on=["celda_id", "anio"], how="left")

    for column in STRUCTURE + PUBLIC + CLIMATE:
        if column not in grid.columns:
            grid[column] = np.nan
    grid["tiene_telemetria"] = grid["tiene_telemetria"].astype(float)
    return grid


def block_columns(grid: pd.DataFrame) -> dict:
    hist = [c for c in grid.columns if c.startswith(("kva_h_", "interrupciones_", "horas_", "tiepi_"))
            and not c.startswith(("horas_racha",))] + ["anios_desde_ultima"]
    scada = [c for c in grid.columns if c.startswith(("microcortes_", "cortes_scada_", "episodios_scada_"))]
    return {"historia": hist, "estructura": STRUCTURE, "scada": scada, "publicos": PUBLIC, "clima": CLIMATE}


def features_of(name: str, blocks: dict) -> list:
    parts = list(blocks) if name == "todo" else name.split("+")
    out = []
    for part in parts:
        out += [c for c in blocks[part] if c not in out]
    return out


# ---------------------------------------------------------------------------
# Models and metrics
# ---------------------------------------------------------------------------

def fit_predict(train: pd.DataFrame, test: pd.DataFrame, features: list, params: dict, seed: int):
    from xgboost import XGBRegressor

    model = XGBRegressor(**params, random_state=seed, n_jobs=4, tree_method="hist")
    model.fit(train[features].astype(float).to_numpy(), train["y_kva_h"].astype(float).to_numpy())
    return model, model.predict(test[features].astype(float).to_numpy())


def capture(values: np.ndarray, scores: np.ndarray, fraction: float) -> float:
    total = float(np.sum(values))
    if total <= 0:
        return float("nan")
    k = max(1, int(math.ceil(fraction * len(values))))
    order = np.lexsort((np.arange(len(scores)), -np.asarray(scores, dtype=float)))
    return float(np.sum(np.asarray(values)[order[:k]])) / total


def feeder_frame(test: pd.DataFrame, columns: list) -> pd.DataFrame:
    return test.groupby(["distribuidora_id", "cabecera"])[["y_kva_h", *columns]].sum().reset_index()


def evaluate(grid: pd.DataFrame, cfg: dict, seed: int) -> tuple:
    blocks = block_columns(grid)
    params = dict(cfg["xgboost"])
    fractions = [float(f) for f in cfg["fracciones_top"]]
    rows, importance, scored = [], [], []

    for year in cfg["anios_evaluacion"]:
        train = grid[(grid["anio"] < year) & grid["y_kva_h"].notna()]
        test = grid[grid["anio"] == year].copy()
        if train.empty or test.empty or test["y_kva_h"].sum() <= 0:
            continue
        test["s_naif"] = test["kva_h_ct_2a"]
        test["s_naif_cabecera"] = test["tiepi_cabecera_2a"]
        test["s_oraculo"] = test["y_kva_h"]
        names = ["naif", "naif_cabecera", "oraculo"]

        for block in cfg["bloques"]:
            features = features_of(block, blocks)
            model, pred = fit_predict(train, test, features, params, seed)
            test[f"s_{block}"] = pred
            names.append(block)
            if block == "todo":
                gain = model.get_booster().get_score(importance_type="gain")
                importance += [{"anio": year, "feature": features[int(k[1:])], "gain": v} for k, v in gain.items()]

        feeders = feeder_frame(test, [f"s_{n}" for n in names])
        # The naive ranking of a feeder is its own history, not the sum of its CTs.
        feeder_hist = test.groupby(["distribuidora_id", "cabecera"])["kva_h_cabecera_2a"].first().reset_index()
        feeders = feeders.drop(columns=["s_naif_cabecera"]).merge(
            feeder_hist.rename(columns={"kva_h_cabecera_2a": "s_naif_cabecera"}), on=["distribuidora_id", "cabecera"])
        for name in names:
            for fraction in fractions:
                rows.append({"anio": year, "nivel": "ct", "modelo": name, "top": fraction,
                             "captura": capture(test["y_kva_h"].to_numpy(), test[f"s_{name}"].to_numpy(), fraction),
                             "con_interrupcion_en_top": float(np.mean(
                                 test.loc[test[f"s_{name}"].rank(ascending=False, method="first")
                                          <= max(1, math.ceil(fraction * len(test))), "y_kva_h"] > 0))})
                rows.append({"anio": year, "nivel": "cabecera", "modelo": name, "top": fraction,
                             "captura": capture(feeders["y_kva_h"].to_numpy(), feeders[f"s_{name}"].to_numpy(),
                                                fraction)})
        base = float((test["y_kva_h"] > 0).mean())
        rows.append({"anio": year, "nivel": "ct", "modelo": "tasa_base", "top": 1.0, "captura": 1.0,
                     "con_interrupcion_en_top": base})
        scored.append(test[["distribuidora_id", "ct_id", "cabecera", "anio", "y_kva_h",
                            *[f"s_{n}" for n in names]]])

    return pd.DataFrame(rows), pd.DataFrame(importance), (pd.concat(scored) if scored else pd.DataFrame())


def rank_next_year(grid: pd.DataFrame, cfg: dict, seed: int, last_year: int) -> tuple:
    blocks = block_columns(grid)
    features = features_of("todo", blocks)
    train = grid[grid["y_kva_h"].notna()]
    test = grid[grid["anio"] == last_year + 1].copy()
    if train.empty or test.empty:
        return pd.DataFrame(), pd.DataFrame()
    _, pred = fit_predict(train, test, features, dict(cfg["xgboost"]), seed)
    test["kva_h_esperado"] = pred
    test["puesto"] = test["kva_h_esperado"].rank(ascending=False, method="first").astype(int)
    cts = test.sort_values("puesto")[["anio", "distribuidora_id", "ct_id", "municipio_nombre", "cabecera",
                                      "grupo_red_id", "kva", "num_abonados", "kva_h_ct_2a", "tiepi_cabecera_2a",
                                      "kva_h_esperado", "puesto"]]
    feeders = test.groupby(["anio", "distribuidora_id", "cabecera"]).agg(
        cts=("ct_id", "nunique"), kva=("kva", "sum"), kva_h_esperado=("kva_h_esperado", "sum"),
        kva_h_2a=("kva_h_cabecera_2a", "first")).reset_index().sort_values("kva_h_esperado", ascending=False)
    return cts, feeders


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    cfg = dict(DEFAULTS)
    cfg.update(config.get("riesgo_ct", {}))
    spark = get_spark(config)
    seed = int(config.get("semilla", 20261005))

    data = Data(spark, settings(config))
    last_year = int(data.events.loc[data.events["variante"] == "principal", "anio"].max())
    logger.info("Risk per CT and year %s: history up to %s", args.run_id, last_year)

    grid = build_table(data, cfg, last_year)
    logger.info("Table CT x year: %s rows, %s columns, years %s - %s", len(grid), grid.shape[1],
                grid["anio"].min(), grid["anio"].max())

    metrics, importance, scored = evaluate(grid, cfg, seed)
    cts, feeders = rank_next_year(grid, cfg, seed, last_year)

    report = Report(config.get("exportar_dir"), args.run_id)
    report.csv("captura.csv", metrics)
    report.csv("importancia.csv", importance)
    report.csv("puntuaciones.csv", scored)
    report.csv("prioridades_ct.csv", cts)
    report.csv("prioridades_cabecera.csv", feeders)
    databases = data.databases

    if not metrics.empty:
        mean = metrics[metrics["modelo"] != "tasa_base"].groupby(["nivel", "modelo", "top"])["captura"].mean()
        table = mean.unstack("top").reset_index()
        table.columns = [c if isinstance(c, str) else f"top_{int(round(100 * c))}pct" for c in table.columns]
        order = {m: i for i, m in enumerate(["oraculo", "naif", "naif_cabecera", *cfg["bloques"]])}
        table = table.sort_values(["nivel", "modelo"], key=lambda s: s.map(order) if s.name == "modelo" else s)
        shown = table.copy()
        for column in [c for c in shown.columns if c.startswith("top_")]:
            shown[column] = shown[column].map(pct)
        per_year = metrics[(metrics["top"] == 0.1) & metrics["modelo"].isin(["naif", "todo", "oraculo"])].pivot_table(
            index=["nivel", "anio"], columns="modelo", values="captura").reset_index()
        for column in ("naif", "todo", "oraculo"):
            if column in per_year:
                per_year[column] = per_year[column].map(pct)
        hits = metrics[(metrics["nivel"] == "ct") & (metrics["top"] == 0.1)].groupby("modelo")[
            "con_interrupcion_en_top"].mean()
        base = metrics.loc[metrics["modelo"] == "tasa_base", "con_interrupcion_en_top"].mean()
        report.section(
            "Captura del año siguiente",
            "Parte de los kVA·h imprevistos de cada año que cae en el top k % del ranking hecho con los años "
            f"anteriores (media de {', '.join(str(y) for y in sorted(metrics['anio'].unique()))}). naif ordena por "
            "los kVA·h del CT en los dos años anteriores y naif_cabecera por los de su cabecera; el oráculo es el "
            "máximo posible.\n\n" + md_table(shown)
            + "\nPor año, top 10 %:\n\n" + md_table(per_year)
            + f"\nCT del top 10 % que tienen alguna interrupción ese año: modelo todo {pct(hits.get('todo'))}, naif "
            f"{pct(hits.get('naif'))}; un CT cualquiera, {pct(base)}.\n"
        )

    if not importance.empty:
        gain = importance.groupby("feature")["gain"].mean().sort_values(ascending=False).head(15).reset_index()
        report.section("Variables que más pesan (modelo todo, ganancia media)", md_table(gain))

    if not cts.empty:
        top_cts = cts.head(15).assign(distribuidora=cts.head(15)["distribuidora_id"].map(databases))
        top_feeders = feeders.head(10).assign(distribuidora=feeders.head(10)["distribuidora_id"].map(databases))
        report.section(
            f"Prioridades para {last_year + 1}",
            "Ranking del modelo con todos los años: kVA·h imprevistos esperados de cada CT y de cada cabecera "
            "(suma de sus CT).\n\n"
            + md_table(top_feeders, ["distribuidora", "cabecera", "cts", "kva", "kva_h_2a", "kva_h_esperado"])
            + "\n" + md_table(top_cts, ["distribuidora", "ct_id", "municipio_nombre", "cabecera", "kva",
                                        "kva_h_ct_2a", "kva_h_esperado", "puesto"])
        )
        try:
            out = cts.copy()
            out.insert(0, "run_id", args.run_id)
            for column in out.columns:
                if out[column].dtype == object:
                    out[column] = out[column].astype(str)
            save_frame(spark, out, RANKING_TABLE, args.run_id)
        except Exception as error:  # noqa: BLE001
            logger.error("Ranking not saved: %s", error)

    text = report.write()
    logger.info("Summary of %s\n%s", args.run_id, text)

    try:
        summary = {} if metrics.empty else {
            f"{r.nivel}:{r.modelo}:top{int(round(100 * r.top))}": r.captura
            for r in metrics[metrics["modelo"] != "tasa_base"].groupby(["nivel", "modelo", "top"])["captura"]
            .mean().reset_index().itertuples()
        }
        run = pd.DataFrame([{
            "run_id": args.run_id, "creado_ts": pd.Timestamp.now(tz="UTC").tz_localize(None),
            "dataset_version": f"historia_hasta_{last_year}", "etiqueta": "ct_anio:kva_h_imprevistos",
            "n_features": len(features_of("todo", block_columns(grid))),
            "modelos_json": json.dumps({"xgboost": cfg["xgboost"], "bloques": cfg["bloques"]}), "busqueda_json": "[]",
            "config_json": json.dumps(config, ensure_ascii=False), "resumen_json": json.dumps(summary),
            "splits_evaluados": ",".join(f"cv_{y}" for y in cfg["anios_evaluacion"]), "artefactos_uri": "",
        }])
        save_frame(spark, run, RUNS_TABLE, args.run_id)
    except Exception as error:  # noqa: BLE001
        logger.error("Run row not saved: %s", error)

    return 0


if __name__ == "__main__":
    sys.exit(main())
