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
- obras: also from contexto_ct_anio, the public works of the municipality
  (PLACSP: tendered the year before, the ones that dig, months of execution
  planned for the year) and the building of the Catastro (dwellings and m2
  built, stock and its change), the activity that brings machines near the
  network and new load.
- clima: hours with gusts of 70 km/h or more and days with 30 mm of rain or
  more per year in the cell of the CT, mean of the years before Y.
- naturaleza: also from contexto_ct_anio, the records of white storks and of
  all the birds in the cell in the five years before (GBIF) and the burnt
  areas of 30 ha or more around it (EFFIS).

With --causas (families of l2_silver.f_incidencia_causa, comma separated)
the label and the history only count the incidents of those causes: the
risk of fauna faults, of weather faults... The other features do not change.

Without --causas, when l2_silver.f_incidencia_causa exists, the job also
trains one model per family of causes (riesgo_ct.familias_causa, plus resto
with every other incident, the ones without a cause included), each with the
label and the history of its family, and adds them up: por_causa:<bloque>
(riesgo_ct.bloques_por_causa), evaluated against all the kVA·h like the rest.
The fauna of a line comes back to the same supports and the failures of the
equipment to the same old devices, which the history of all the causes
mixes up. The ranking of the next year adds what each family expects
(esperado_<familia>), which says what to do on each line.

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

# The list of columns that may never be features, the same that Gold enforces (appended: nothing
# of 03_gold shadows a module of this folder).
GOLD_JOBS = os.path.join(HERE, "..", "03_gold")
if GOLD_JOBS not in sys.path:
    sys.path.append(GOLD_JOBS)
from gold_common import FORBIDDEN_FEATURES  # noqa: E402


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
                "historia+estructura+obras", "historia+estructura+clima", "historia+estructura+naturaleza",
                "historia+estructura+arbolado", "historia+estructura+hoja", "historia+estructura+arbolado+hoja",
                "historia+estructura+corredor", "historia+estructura+arbolado+corredor", "historia+estructura+mfe",
                "historia+estructura+arbolado+mfe", "historia+estructura+avifauna", "historia+estructura+especies",
                "todo"],
    # The trees of every cell, from the public maps (None or a missing file: the block stays empty).
    "arbolado_csv": "/app/config/01_bronze/arbolado_celdas.csv",
    # The canopy density and the leaf type of every cell, from the HRL of Copernicus (block hoja).
    "hrl_csv": "/app/config/01_bronze/hrl_celdas.csv",
    # The trees in a corridor of 15 m at each side of the power lines of OpenStreetMap in the cell (block corredor).
    "corredor_csv": "/app/config/01_bronze/corredor_celdas.csv",
    # The canopy of every group of species in the cell from the Mapa Forestal de España (block mfe).
    "mfe_csv": "/app/config/01_bronze/mfe_celdas.csv",
    # The lines of each distribuidora that the Junta declared dangerous for the birds in each municipality (block
    # avifauna, experimentos/aves/lineas_rd1432.py).
    "avifauna_csv": "/app/config/01_bronze/avifauna_municipios.csv",
    # The white stork and the big birds of the 10 x 10 km square of each municipality (block especies,
    # experimentos/aves/ieet_municipios.py).
    "especies_csv": "/app/config/01_bronze/especies_municipios.csv",
    # One model per family of causes (label and history of that family only), added up: the fauna of a line
    # repeats in the same supports, the failures of equipment follow the age and the load, the weather follows
    # the exposure. resto takes every other incident, the ones without a cause included, so the sum stands
    # for all the kVA·h.
    "familias_causa": {"meteo": ["meteorologia", "desconocida", "vegetacion", "incendio", "objeto_ajeno"],
                       "fauna": ["fauna"], "equipo": ["equipo"]},
    "bloques_por_causa": ["historia", "historia+estructura", "historia+estructura+scada", "historia+estructura+clima"],
    "bloque_ranking_por_causa": "historia+estructura",
}

KEYS = ["distribuidora_id", "ct_id", "anio"]
LABEL_COLUMNS = ["y_kva_h", "y_interrupciones", "y_horas"]

LEVELS = {"ct": ["distribuidora_id", "ct_id"], "cabecera": ["distribuidora_id", "cabecera"],
          "grupo": ["distribuidora_id", "grupo_red_id"], "municipio": ["distribuidora_id", "municipio_id"]}

# n_posiciones_aguas_arriba left the block on 19.10.2026: it counts the bays of the whole history of
# dim_ct, future cuts included, and is in gold_common.FORBIDDEN_FEATURES (test_riesgo_ct_bloques.py).
STRUCTURE = ["kva", "num_abonados", "zona", "tiene_telemetria", "n_ct_grupo", "cts_cabecera", "kva_cabecera"]
PUBLIC = ["pob_municipio", "pob_crec_1a", "pob_crec_5a_anual", "densidad_hab_km2", "elevacion_municipio_m",
          "kva_municipio_por_hab", "abonados_municipio_por_hab", "celda_elev_media_m", "celda_elev_std_m",
          "celda_desnivel_m", "celda_dens_linea_aerea_km_km2", "celda_dens_apoyos_km2", "celda_dens_carreteras_km_km2",
          "celda_dens_pistas_km_km2", "celda_pct_bosque", "celda_pct_matorral", "celda_dens_edificios_km2",
          "celda_pct_cable", "celda_km2_obras"]
WORKS = ["obras_publicadas_1a", "obras_importe_1a", "obras_mov_tierras_1a", "obras_previstas_meses",
         "obras_previstas_mov_tierras_meses", "viviendas_nuevas_3a", "m2_nuevos_3a", "viviendas_parque_2a",
         "crec_viviendas_5a"]
CLIMATE = ["clima_horas_racha", "clima_dias_lluvia"]
NATURE = ["celda_ciguena_5a", "celda_aves_5a", "celda_ciguena_parte_aves_5a", "celda_incendios_5a",
          "celda_area_quemada_5a_ha"]
# The trees of the cell from the public maps (experimentos/arbolado/bajar_arbolado.py, arbolado_celdas.csv): the
# share of the cell that is tree cover and scrub (ESA WorldCover 2021, 10 m) and the canopy height (Meta and WRI,
# 1 m): the mean and the p90 of the pixels with trees and the share of the cell with a canopy of 5, 8 and 12 m or
# more, the trees that reach a medium voltage conductor. Static: the maps are of 2020 - 2021.
TREES = ["celda_pct_arbol", "celda_pct_matorral", "celda_pct_dosel", "celda_dosel_media_m", "celda_dosel_p90_m",
         "celda_pct_dosel_5m", "celda_pct_dosel_8m", "celda_pct_dosel_12m"]
# The canopy density (Tree Cover Density, 0 - 100 %) and the leaf type (Dominant Leaf Type: the share of the cell
# with broadleaved and coniferous trees and the deciduous share of the trees) of the cell, from the high resolution
# layers of Copernicus at 10 m (experimentos/arbolado/bajar_hrl.py and celdas_hrl.py, hrl_celdas.csv).
# The trees along the lines: the power lines of OpenStreetMap in the cell (power=line and minor_line), a corridor of
# 15 m at each side and the canopy height of Meta and WRI inside it (experimentos/arbolado/corredor_lineas.py,
# corredor_celdas.csv): km of line, the share of the corridor with a canopy of 5 and 8 m or more and its hectares.
CORRIDOR = ["celda_corr_km_linea", "celda_corr_pct_dosel_5m", "celda_corr_pct_dosel_8m", "celda_corr_ha_dosel_8m",
            "celda_corr_dosel_media_m"]
LEAF = ["celda_hrl_copa_media_pct", "celda_hrl_copa_30_pct", "celda_hrl_pct_frondosa", "celda_hrl_pct_conifera",
        "celda_hrl_caducifolio_de_arbol"]
# The species of the trees: the share of the cell under the canopy of deciduous broadleaves (melojo, castaño,
# quejigo, poplars), evergreen broadleaves (holm and cork oak), eucalyptus and conifers, from the Mapa Forestal de
# España 1:50.000 of MITECO (experimentos/arbolado/celdas_mfe.py, mfe_celdas.csv); what the HRL cannot tell apart.
MFE = ["celda_mfe_copa_caducifolia_pct", "celda_mfe_copa_perennifolia_pct", "celda_mfe_copa_eucalipto_pct",
       "celda_mfe_copa_conifera_pct", "celda_mfe_copa_total_pct", "celda_mfe_caducifolia_de_copa"]
# The lines of the network that the Junta de Extremadura declared dangerous for the birds (in a protection zone and
# without the measures of the RD 1432/2008; Resolución de 15.11.2021): how many in the municipality of the CT, and
# per CT of the network there.
BIRDS = ["muni_lineas_rd1432", "muni_lineas_rd1432_por_ct"]
# From the Inventario Español de Especies Terrestres (MITECO): white stork in the square of the municipality, and how
# many species of big birds (storks, raptors, big corvids) and of raptors live there.
SPECIES = ["ieet_ciguena", "ieet_aves_grandes", "ieet_rapaces"]
CAUSES_TABLE = "l2_silver.f_incidencia_causa"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--causas", default=None, help="Only the incidents of these cause families (comma separated)")
    parser.add_argument("--bloques", default=None, help="Only these blocks of riesgo_ct.bloques (comma separated)")
    parser.add_argument("--sin-causas", action="store_true", help="Without the models per family of causes")
    args, _ = parser.parse_known_args(argv)
    if not args.run_id:
        suffix = "_" + "-".join(c.strip() for c in args.causas.split(",")) if args.causas else ""
        args.run_id = ("rct" + suffix + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_"
                       + uuid.uuid4().hex[:6])
    return args


# ---------------------------------------------------------------------------
# Table CT x year
# ---------------------------------------------------------------------------

def cause_incidents(spark, causes: list) -> set:
    """Keys (distribuidora|periodo|incidencia, as Data.events) of the incidents of the cause families."""
    df = read_table(spark, CAUSES_TABLE, ["distribuidora_id", "periodo_id", "id", "causa"])
    if df is None:
        raise RuntimeError(f"{CAUSES_TABLE} is required for --causas")
    pdf = df.where(F.col("causa").isin(causes)).toPandas()
    return set(pdf["distribuidora_id"].astype(str) + "|" + pdf["periodo_id"].astype(str) + "|" + pdf["id"].astype(str))


def yearly_events(data: Data, keep: set = None) -> pd.DataFrame:
    ev = data.events[data.events["variante"] == "principal"]
    if keep is not None:
        ev = ev[ev["incidente"].isin(keep)]
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


def scada_counts(spark) -> pd.DataFrame:
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


def prepare_dim(data: Data) -> pd.DataFrame:
    """The CTs of dim_ct with the size of their feeder."""
    dim = data.dim.copy()
    dim["municipio_id"] = dim["municipio_id"].astype(str)
    per_feeder = dim.groupby(["distribuidora_id", "cabecera"]).agg(cts_cabecera=("ct_id", "nunique"),
                                                                  kva_cabecera=("kva", "sum")).reset_index()
    return dim.merge(per_feeder, on=["distribuidora_id", "cabecera"], how="left")


def history_columns(grid: pd.DataFrame) -> list:
    """The columns add_label_history writes (label, history, kVA·h per kVA, years since the last one)."""
    return LABEL_COLUMNS + [c for c in grid.columns if c.startswith(("kva_h_", "interrupciones_", "horas_", "tiepi_"))
                            and not c.startswith("horas_racha")] + ["anios_desde_ultima"]


def add_label_history(grid: pd.DataFrame, per_ct: pd.DataFrame, dim: pd.DataFrame, last_year: int) -> pd.DataFrame:
    """Label of the year and history of the CT and its network levels from the yearly events per_ct."""
    labels = per_ct.rename(columns={"kva_h": "y_kva_h", "interrupciones": "y_interrupciones", "horas": "y_horas"})
    grid = grid.merge(labels, on=KEYS, how="left")
    grid[LABEL_COLUMNS] = grid[LABEL_COLUMNS].fillna(0.0)
    grid.loc[grid["anio"] > last_year, LABEL_COLUMNS] = np.nan

    for level in LEVELS:
        hist = history(per_ct, grid, dim, level, (1, 2, 3))
        grid = grid.merge(hist, on=LEVELS[level] + ["anio"], how="left")

    # Network levels in kVA·h per installed kVA, so a big feeder does not look worse only by its size.
    for level in ("cabecera", "grupo", "municipio"):
        size = grid.groupby(LEVELS[level] + ["anio"])["kva"].transform("sum").replace(0, np.nan)
        for span in (1, 2, 3):
            grid[f"tiepi_{level}_{span}a"] = grid[f"kva_h_{level}_{span}a"] / size

    last = per_ct[per_ct["kva_h"] > 0][["distribuidora_id", "ct_id", "anio"]].rename(columns={"anio": "_ultimo"})
    since = grid[KEYS].merge(last, on=["distribuidora_id", "ct_id"], how="left")
    since = since[since["_ultimo"] < since["anio"]]
    since = since.groupby(KEYS)["_ultimo"].max().reset_index()
    grid = grid.merge(since, on=KEYS, how="left")
    grid["anios_desde_ultima"] = (grid["anio"] - grid["_ultimo"]).clip(upper=10).fillna(10.0)
    return grid.drop(columns=["_ultimo"])


def build_table(data: Data, cfg: dict, last_year: int, keep: set = None) -> pd.DataFrame:
    spark = data.spark
    dim = prepare_dim(data)

    years = list(range(int(cfg["anio_primero"]), last_year + 2))
    rows = []
    for year in years:
        lo, hi = pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year + 1, month=1, day=1)
        valid = dim[(dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi))
                    & (dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo))]
        rows.append(valid.assign(anio=year))
    grid = pd.concat(rows, ignore_index=True)
    grid = add_label_history(grid, yearly_events(data, keep), dim, last_year)

    scada = scada_counts(spark)
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
        ctx = context.select("distribuidora_id", "ct_id", "anio",
                             *[c for c in PUBLIC + WORKS + NATURE if c in context.columns]).toPandas()
        ctx["ct_id"] = ctx["ct_id"].astype(str)
        grid = grid.merge(ctx, on=["distribuidora_id", "ct_id", "anio"], how="left")

    clim = climate_features(spark, cfg)
    cells = read_table(spark, CELL_TABLE, ["distribuidora_id", "ct_id", "celda_id"])
    if cells is not None:
        cell_of = cells.toPandas()
        cell_of["ct_id"] = cell_of["ct_id"].astype(str)
        grid = grid.merge(cell_of, on=["distribuidora_id", "ct_id"], how="left")
        grid = add_trees(grid, cfg.get("arbolado_csv"))
        grid = add_trees(grid, cfg.get("hrl_csv"), LEAF, "hoja")
        grid = add_trees(grid, cfg.get("corredor_csv"), CORRIDOR, "corredor")
        grid = add_trees(grid, cfg.get("mfe_csv"), MFE, "mfe")
    if not clim.empty and cells is not None:
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

    grid = add_birds(grid, cfg.get("avifauna_csv"))
    grid = add_species(grid, cfg.get("especies_csv"))
    for column in STRUCTURE + PUBLIC + WORKS + CLIMATE + NATURE + TREES + LEAF + CORRIDOR + MFE + BIRDS + SPECIES:
        if column not in grid.columns:
            grid[column] = np.nan
    grid["tiene_telemetria"] = grid["tiene_telemetria"].astype(float)
    return grid.sort_values(KEYS, kind="stable").reset_index(drop=True)


def family_keys(spark, data: Data, families: dict) -> dict:
    """
    {family: incident keys} for every family of causes plus resto, the other
    incidents of the principal variant (without a cause included). None
    without l2_silver.f_incidencia_causa.
    """
    df = read_table(spark, CAUSES_TABLE, ["distribuidora_id", "periodo_id", "id", "causa"])
    if df is None:
        logger.warning("%s not found: no model per family of causes", CAUSES_TABLE)
        return None
    pdf = df.toPandas()
    key = pdf["distribuidora_id"].astype(str) + "|" + pdf["periodo_id"].astype(str) + "|" + pdf["id"].astype(str)
    out = {name: set(key[pdf["causa"].isin(causes)]) for name, causes in families.items()}
    every = set(data.events.loc[data.events["variante"] == "principal", "incidente"])
    out["resto"] = every - set().union(*out.values())
    return out


def family_grids(data: Data, grid: pd.DataFrame, keys: dict, last_year: int) -> dict:
    """{family: the CT x year table with the label and the history of that family}, rows as in grid."""
    dim = prepare_dim(data)
    base = grid.drop(columns=history_columns(grid))
    out = {}
    for name, keep in keys.items():
        table = add_label_history(base, yearly_events(data, keep), dim, last_year)
        out[name] = table.sort_values(KEYS, kind="stable").reset_index(drop=True)[grid.columns]
        if not out[name][KEYS].equals(grid[KEYS]):
            raise ValueError(f"The table of the family {name} does not have the rows of the main table")
    return out


def add_trees(grid: pd.DataFrame, path: str, columns: list = None, block: str = "arbolado") -> pd.DataFrame:
    """
    The columns (TREES by default) from a CSV of the cells (celda_pct_arbol is pct_arbol in the file), by the
    celda_id of the CT; NaN without file.
    """
    columns = columns or TREES
    if not path or not os.path.exists(path) or "celda_id" not in grid.columns:
        if path:
            logger.info("No cell file %s: the block %s stays empty", path, block)
        return grid
    trees = pd.read_csv(path)
    wanted = [c[len("celda_"):] for c in columns]
    trees = trees[["celda_id"] + [c for c in wanted if c in trees.columns]].rename(
        columns={c: f"celda_{c}" for c in wanted})
    logger.info("Block %s: %s cells from %s", block, len(trees), path)
    return grid.merge(trees, on="celda_id", how="left")


def plain(name) -> str:
    """A municipality name without accents, case or spaces at the ends, to join the names of two sources."""
    import unicodedata

    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode()
    return " ".join(text.lower().split())


def add_birds(grid: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    The block avifauna: the dangerous lines of the distribuidora in the municipality of each CT, by distribuidora
    and municipio_nombre (0 where the municipality has none; NaN without the file or the names).
    """
    if not path or not os.path.exists(path) or "municipio_nombre" not in grid.columns:
        if path:
            logger.info("No file %s or no municipalities: the block avifauna stays empty", path)
        return grid
    lines = pd.read_csv(path)
    lines["_muni"] = lines["municipio"].map(plain)
    grid = grid.copy()
    grid["_muni"] = grid["municipio_nombre"].map(plain)
    grid = grid.merge(lines[["distribuidora_id", "_muni", "lineas_rd1432"]].rename(
        columns={"lineas_rd1432": "muni_lineas_rd1432"}), on=["distribuidora_id", "_muni"], how="left")
    grid["muni_lineas_rd1432"] = grid["muni_lineas_rd1432"].fillna(0.0)
    per_ct = grid.groupby(["distribuidora_id", "_muni", "anio"])["ct_id"].transform("nunique")
    grid["muni_lineas_rd1432_por_ct"] = grid["muni_lineas_rd1432"] / per_ct
    found = int((lines.merge(grid[["distribuidora_id", "_muni"]].drop_duplicates(), on=["distribuidora_id", "_muni"])
                 ["lineas_rd1432"]).sum())
    logger.info("Block avifauna: %s of the %s dangerous lines in the municipalities of the CT", found,
                int(lines["lineas_rd1432"].sum()))
    return grid.drop(columns="_muni")


def add_species(grid: pd.DataFrame, path: str) -> pd.DataFrame:
    """The block especies by the municipality of each CT (NaN without the file or the names)."""
    if not path or not os.path.exists(path) or "municipio_nombre" not in grid.columns:
        if path:
            logger.info("No file %s or no municipalities: the block especies stays empty", path)
        return grid
    species = pd.read_csv(path)
    species["_muni"] = species["municipio"].map(plain)
    grid = grid.copy()
    grid["_muni"] = grid["municipio_nombre"].map(plain)
    columns = [c for c in SPECIES if c in species.columns]
    grid = grid.merge(species.drop_duplicates("_muni")[["_muni"] + columns], on="_muni", how="left")
    logger.info("Block especies: %s of %s CT-years with their square", int(grid[columns[0]].notna().sum()), len(grid))
    return grid.drop(columns="_muni")


def block_columns(grid: pd.DataFrame) -> dict:
    hist = [c for c in grid.columns if c.startswith(("kva_h_", "interrupciones_", "horas_", "tiepi_"))
            and not c.startswith(("horas_racha",))] + ["anios_desde_ultima"]
    scada = [c for c in grid.columns if c.startswith(("microcortes_", "cortes_scada_", "episodios_scada_"))]
    blocks = {"historia": hist, "estructura": STRUCTURE, "scada": scada, "publicos": PUBLIC, "obras": WORKS,
              "clima": CLIMATE, "naturaleza": NATURE, "arbolado": TREES,
              "hoja": LEAF, "corredor": CORRIDOR, "mfe": MFE, "avifauna": BIRDS,
              "especies": SPECIES}
    # The blocks found by prefix (horas_ would take horas_hasta_proximo_evento) never take a forbidden column.
    return {name: [c for c in columns if c not in FORBIDDEN_FEATURES] for name, columns in blocks.items()}


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


def family_sum(families: dict, year: int, features: list, params: dict, seed: int, n_rows: int) -> np.ndarray:
    """Sum over the families of causes of the kVA·h each model expects in year, trained on the years before."""
    total = np.zeros(n_rows)
    for table in families.values():
        train = table[(table["anio"] < year) & table["y_kva_h"].notna()]
        test = table[table["anio"] == year]
        if train["y_kva_h"].sum() <= 0:
            continue
        _, pred = fit_predict(train, test, features, params, seed)
        total += pred
    return total


def evaluate(grid: pd.DataFrame, cfg: dict, seed: int, families: dict = None) -> tuple:
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

        for block in (cfg.get("bloques_por_causa") or []) if families else []:
            name = f"por_causa:{block}"
            test[f"s_{name}"] = family_sum(families, year, features_of(block, blocks), params, seed, len(test))
            names.append(name)

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


def rank_next_year(grid: pd.DataFrame, cfg: dict, seed: int, last_year: int, families: dict = None) -> tuple:
    blocks = block_columns(grid)
    features = features_of("todo", blocks)
    params = dict(cfg["xgboost"])
    train = grid[grid["y_kva_h"].notna()]
    test = grid[grid["anio"] == last_year + 1].copy()
    if train.empty or test.empty:
        return pd.DataFrame(), pd.DataFrame()
    _, pred = fit_predict(train, test, features, params, seed)
    test["kva_h_esperado"] = pred
    test["puesto"] = test["kva_h_esperado"].rank(ascending=False, method="first").astype(int)

    # What each family of causes expects: where to look at the supports and the nests, where at the equipment.
    expected = []
    if families:
        family_features = features_of(cfg.get("bloque_ranking_por_causa", "historia+estructura"), blocks)
        for name, table in families.items():
            column = f"esperado_{name}"
            fam_train = table[table["y_kva_h"].notna()]
            fam_test = table[table["anio"] == last_year + 1]
            test[column] = (fit_predict(fam_train, fam_test, family_features, params, seed)[1]
                            if fam_train["y_kva_h"].sum() > 0 else 0.0)
            expected.append(column)

    cts = test.sort_values("puesto")[["anio", "distribuidora_id", "ct_id", "municipio_nombre", "cabecera",
                                      "grupo_red_id", "kva", "num_abonados", "kva_h_ct_2a", "tiepi_cabecera_2a",
                                      "kva_h_esperado", "puesto", *expected]]
    feeders = test.groupby(["anio", "distribuidora_id", "cabecera"]).agg(
        cts=("ct_id", "nunique"), kva=("kva", "sum"), kva_h_esperado=("kva_h_esperado", "sum"),
        kva_h_2a=("kva_h_cabecera_2a", "first"), **{c: (c, "sum") for c in expected},
    ).reset_index().sort_values("kva_h_esperado", ascending=False)
    return cts, feeders


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    cfg = dict(DEFAULTS)
    cfg.update(config.get("riesgo_ct", {}))
    if args.bloques:
        cfg["bloques"] = [b.strip() for b in args.bloques.split(",") if b.strip()]
    if args.sin_causas:
        cfg["familias_causa"] = {}
    spark = get_spark(config)
    seed = int(config.get("semilla", 20261005))

    data = Data(spark, settings(config))
    last_year = int(data.events.loc[data.events["variante"] == "principal", "anio"].max())
    causes = [c.strip() for c in args.causas.split(",") if c.strip()] if args.causas else None
    keep = cause_incidents(spark, causes) if causes else None
    logger.info("Risk per CT and year %s: history up to %s%s", args.run_id, last_year,
                f", causes {causes} ({len(keep)} incidents)" if causes else "")

    grid = build_table(data, cfg, last_year, keep)
    logger.info("Table CT x year: %s rows, %s columns, years %s - %s", len(grid), grid.shape[1],
                grid["anio"].min(), grid["anio"].max())

    # The models per family of causes only make sense for the ranking of all the causes.
    families, family_sizes = None, {}
    if not causes and cfg.get("familias_causa"):
        keys = family_keys(spark, data, cfg["familias_causa"])
        if keys:
            family_sizes = {name: len(keep_f) for name, keep_f in keys.items()}
            families = family_grids(data, grid, keys, last_year)
            logger.info("Families of causes (incidents): %s", family_sizes)

    metrics, importance, scored = evaluate(grid, cfg, seed, families)
    cts, feeders = rank_next_year(grid, cfg, seed, last_year, families)

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
        order = {m: i for i, m in enumerate(["oraculo", "naif", "naif_cabecera", *cfg["bloques"],
                                             *[f"por_causa:{b}" for b in cfg.get("bloques_por_causa") or []]])}
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
        by_family = ""
        if family_sizes:
            listed = ", ".join(f"{name} ({n})" for name, n in family_sizes.items())
            by_family = (f" por_causa:<bloque> suma un modelo por familia de causas, cada uno con la etiqueta y la "
                         f"historia de su familia: {listed} incidencias.")
        report.section(
            "Captura del año siguiente",
            "Parte de los kVA·h imprevistos de cada año que cae en el top k % del ranking hecho con los años "
            f"anteriores (media de {', '.join(str(y) for y in sorted(metrics['anio'].unique()))}). naif ordena por "
            "los kVA·h del CT en los dos años anteriores y naif_cabecera por los de su cabecera; el oráculo es el "
            f"máximo posible.{by_family}\n\n" + md_table(shown)
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
        expected = [c for c in feeders.columns if c.startswith("esperado_")]
        report.section(
            f"Prioridades para {last_year + 1}",
            "Ranking del modelo con todos los años: kVA·h imprevistos esperados de cada CT y de cada cabecera "
            "(suma de sus CT)."
            + (f" esperado_<familia> es lo que espera el modelo de cada familia de causas (bloque "
               f"{cfg.get('bloque_ranking_por_causa')}): dice qué actuación toca en cada línea." if expected else "")
            + "\n\n"
            + md_table(top_feeders, ["distribuidora", "cabecera", "cts", "kva", "kva_h_2a", "kva_h_esperado",
                                     *expected])
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
