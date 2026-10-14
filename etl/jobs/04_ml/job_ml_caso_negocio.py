"""
What the system would have said every evening, and what happened: the
replay of the daily operation over past years (caso_negocio of
config_ml.json, docs/caso-de-negocio.md).

Every day of every year to replay is scored as the evening job does
(job_ml_riesgo_manana: the variante operativa of job_ml_riesgo_dia, a
logistic regression fitted on the years before the one scored, the level
from the percentile among the training days of the distribuidora), twice:
with the reanalysis as a perfect forecast and, on the days that have it,
with the real forecast of ICON-EU of the day before. The incidents are the
ones of a local cause of fact_interrupciones_mt (what a crew is sent to),
with the customers of every CT and the real duration of every CT cut. The
crews are placed with riesgo_zonas: the fixed bases that minimise the
customer-weighted arrival (the baseline, no hindsight) against the bases of
the day on the days in alert, with the minutes by road of OSRM when the
cache of the live Landing has them, and a mobilisation delay out of
working hours when the crew is on call.

It leaves:
- l3_gold.caso_negocio_dia: one row per mode, distribuidora and day with
  the level, the probability, the incidents, the customers and the
  customer-hours of the day and the bases of the crews.
- l3_gold.caso_negocio_resumen: one row per mode, distribuidora, year and
  number of crews with the coverage of the alert days and the minutes the
  crews would have gained.
- historico.json for dashboard/historial.html (in dashboard_dir and in the
  export folder), with resumen.md, dias.csv, cts.csv and resumen_anual.csv.

    spark-submit --py-files <the other jobs of 04_ml> job_ml_caso_negocio.py --config config_ml.json
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
for folder in (HERE, os.path.join(HERE, "..", "00_landing"), os.path.join(HERE, "..", "01_bronze"),
               os.path.join(HERE, "..", "02_silver")):
    if folder not in sys.path:
        sys.path.insert(0, folder)

import ml_metrics  # noqa: E402
import riesgo_zonas as rz  # noqa: E402
from job_ml_horas_riesgo import export, rolling_evaluation  # noqa: E402
from job_ml_meteo_historia import FACTS_TABLE, read_events  # noqa: E402
from job_ml_riesgo_dia import CELLS_TABLE, MAIN_LABEL, build_dataset, settings, variant_features  # noqa: E402
from job_ml_riesgo_manana import (  # noqa: E402
    DEFAULTS as RISK_DEFAULTS,
    DIM_TABLE,
    WEATHER_TABLE,
    fit_zone_model,
    gold_cell_days,
    live_filesystem,
    number,
    risk_settings,
    road_minutes,
)
from job_ml_train import RUNS_TABLE, get_spark, load_config, logger, save_frame  # noqa: E402

DEFAULTS = {
    "anios": [2022, 2023, 2024, 2025],
    "brigadas": {},
    "movilizacion_min": 30.0,
    "recorte_min": 110.0,
    "horario_laboral": [8, 17],
    "tabla_dias": "l3_gold.caso_negocio_dia",
    "tabla_resumen": "l3_gold.caso_negocio_resumen",
    "peores_dias": 10,
    "minimo_incidencias_nivel": 10,  # a distribuidora enters the tables by level with at least this many incidents in the period
}
ALERT = ("alto", "muy alto")
MODES = {"reanalisis": "tiempo del día del reanálisis (previsión perfecta)",
         "prevision": "previsión real de ICON-EU de la víspera"}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--anios", default="", help="Years to replay, comma separated (caso_negocio.anios by default)")
    parser.add_argument("--landing", default=None, help="Folder of the live Landing, for the cache of the road minutes")
    parser.add_argument("--dashboard-dir", default=None)
    args, _ = parser.parse_known_args(argv)
    if not args.run_id:
        args.run_id = "caso_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    return args


def case_settings(config: dict) -> dict:
    return {**DEFAULTS, **config.get("caso_negocio", {})}


# ---------------------------------------------------------------------------
# The day model, year by year
# ---------------------------------------------------------------------------

def day_scores(pdf: pd.DataFrame, blocks: dict, variant: dict, years: list, config: dict, cfg: dict,
               levels: dict) -> pd.DataFrame:
    """
    One row per distribuidora and day of years with the probability and the
    level of the day from a model that only saw the years before: with the
    reanalysis (p_reanalisis) and with the forecast of the day before
    (p_prevision, null on the days without it).
    """
    features = variant_features(variant, blocks)
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])].copy()
    common = [c[len("dia_"):] for c in blocks.get("dia_comun", [])]
    swap = [(f"dia_{n}", f"prev_{n}") for n in common if f"prev_{n}" in rows.columns]
    swap += [(c, f"prev_{c}") for c in features if c.startswith("ext_") and f"prev_{c}" in rows.columns]
    parts = []
    for year in years:
        before = rows[rows["anio"] < year]
        target = rows[rows["anio"] == year].copy()
        if before.empty or target.empty or before[MAIN_LABEL].sum() == 0:
            logger.warning("Year %s skipped: nothing before it or no positive day", year)
            continue
        _, _, scorers, _ = rolling_evaluation(rows, features, MAIN_LABEL, config, cfg, [year], False,
                                              names=["logistica"], references=False)
        scorer = scorers["logistica"]
        out = target[["distribuidora_id", "fecha", "anio", MAIN_LABEL]].copy()
        out["p_reanalisis"] = scorer.score(target[features].to_numpy(dtype=np.float32, na_value=np.nan))
        raw = target.copy()
        for column, source in swap:
            raw[column] = target[source].to_numpy()
        with_forecast = (raw[[s for _, s in swap]].notna().all(axis=1).to_numpy() if swap
                         else np.zeros(len(raw), dtype=bool))
        out["p_prevision"] = np.where(with_forecast, scorer.score(raw[features].to_numpy(dtype=np.float32, na_value=np.nan)),
                                      np.nan)
        train_scores = scorer.score(before[features].to_numpy(dtype=np.float32, na_value=np.nan))
        for dist in out["distribuidora_id"].unique():
            ref = np.sort(train_scores[(before["distribuidora_id"] == dist).to_numpy()])
            mask = (out["distribuidora_id"] == dist).to_numpy()
            for col in ("p_reanalisis", "p_prevision"):
                values = out.loc[mask, col].to_numpy()
                pct = np.searchsorted(ref, values, side="right") / max(len(ref), 1)
                out.loc[mask, col.replace("p_", "pct_")] = np.where(np.isnan(values), np.nan, pct)
        parts.append(out)
        logger.info("Day model of %s: fitted on %s days (%s with interruptions), %s features", year, len(before),
                    int(before[MAIN_LABEL].sum()), len(features))
    if not parts:
        return pd.DataFrame()
    days = pd.concat(parts, ignore_index=True)
    for col in ("pct_reanalisis", "pct_prevision"):
        days[col.replace("pct_", "nivel_")] = [rz.level_of(p, levels) if pd.notna(p) else None for p in days[col]]
    return days


# ---------------------------------------------------------------------------
# The network, the zones and the incidents
# ---------------------------------------------------------------------------

def network(spark) -> pd.DataFrame:
    dim = spark.table(DIM_TABLE)
    wanted = [c for c in ("distribuidora_id", "ct_id", "grupo_red_id", "municipio_nombre", "potencia_kva",
                          "num_abonados", "potencia_tiepi_kva", "tipo_zona_codigo", "en_estudio") if c in dim.columns]
    return rz.zone_table(dim.select(*wanted).toPandas(), spark.table(CELLS_TABLE).toPandas())


def ct_cuts(spark, events: pd.DataFrame, ct: pd.DataFrame) -> pd.DataFrame:
    """
    Every CT cut of the incidents of a local cause, one row per interruption
    of a CT (a CT cut twice in the same incident counts twice in the
    customer-hours), with its duration (duracion_s, or the winsorised one),
    the cause of its incident and the CT.
    """
    facts = spark.table(FACTS_TABLE).where(F.col("variante") == "principal")
    extra = [c for c in ("duracion_s", "duracion_winsor_s") if c in facts.columns]
    cuts = facts.select(F.col("distribuidora_id").cast("long").alias("distribuidora_id"), "periodo_id", "incidencia_id",
                        "ct_id", "inicio_ts", *extra).toPandas()
    cuts["ct_id"] = cuts["ct_id"].astype(str)
    cuts["inicio_ts"] = pd.to_datetime(cuts["inicio_ts"])
    causes = (events[["distribuidora_id", "periodo_id", "incidencia_id", "causa"]]
              .drop_duplicates(["distribuidora_id", "periodo_id", "incidencia_id"]))
    cuts = cuts.merge(causes, on=["distribuidora_id", "periodo_id", "incidencia_id"], how="left")
    cuts = cuts[~cuts["causa"].fillna("").isin(rz.LOCAL_EXCLUDED)]
    cuts = cuts.merge(ct[["distribuidora_id", "ct_id", "municipio", "zona", "num_abonados", "potencia_kva"]],
                      on=["distribuidora_id", "ct_id"])
    seconds = cuts["duracion_s"] if "duracion_s" in cuts.columns else cuts.get("duracion_winsor_s")
    cuts["dur_min"] = pd.to_numeric(seconds, errors="coerce") / 60.0
    cuts["fecha"] = cuts["inicio_ts"].dt.floor("D")
    return cuts.reset_index(drop=True)


def incidents_of(cuts: pd.DataFrame) -> pd.DataFrame:
    keys = ["distribuidora_id", "periodo_id", "incidencia_id"]
    once = cuts.drop_duplicates(keys + ["ct_id"])
    inc = once.groupby(keys).agg(inicio_ts=("inicio_ts", "min"), causa=("causa", "first"), n_ct=("ct_id", "nunique"),
                                 clientes=("num_abonados", "sum"), kva=("potencia_kva", "sum"),
                                 municipio=("municipio", lambda s: s.value_counts().index[0]),
                                 zona=("zona", lambda s: s.value_counts().index[0])).reset_index()
    inc = inc.merge(cuts.groupby(keys)["dur_min"].max().rename("duracion_max_min").reset_index(), on=keys, how="left")
    inc["fecha"] = inc["inicio_ts"].dt.floor("D")
    inc["anio"] = inc["fecha"].dt.year
    return inc


def zone_scores(spark, pdf: pd.DataFrame, ct: pd.DataFrame, ev: pd.DataFrame, variant: dict, years: list,
                config: dict, cfg: dict) -> pd.DataFrame:
    """One row per zone and day of years with the score of a zone model fitted on the years before."""
    zones = rz.zones_of(ct)
    starts = rz.local_starts(ev, ct)
    weather = rz.zone_weather(ct, gold_cell_days(spark))
    day_features = [c for c in pdf.columns if c.startswith(("cal_", "hist_", "aviso_", "dia_", "ext_", "ayer_"))
                    and c != "hist_horas_desde_ultima"]
    last = f"{max(years)}-12-31"
    table = rz.zone_days(zones, starts, weather, pdf, day_features, variant["desde"], last)
    table["anio"] = table["fecha"].dt.year
    features = rz.zone_features(zones, day_features)
    parts = []
    for year in years:
        before = table[table["anio"] < year]
        target = table[table["anio"] == year].copy()
        if before.empty or target.empty or before["y"].sum() == 0:
            continue
        scorer, _, _ = fit_zone_model(before, features, config, cfg)
        target["p_zona"] = scorer.score(target[features].to_numpy(dtype=np.float32, na_value=np.nan))
        parts.append(target[["distribuidora_id", "zona", "fecha", "anio", "y", "p_zona"]])
        logger.info("Zone model of %s: %s zone days before it (%s with incidents)", year, len(before), int(before["y"].sum()))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["distribuidora_id", "zona", "fecha", "anio", "y", "p_zona"])


def gust_profiles(spark, ct: pd.DataFrame, years: list) -> pd.DataFrame:
    """(distribuidora_id, fecha, h, racha): the strongest gust of the cells of every distribuidora, hour by hour."""
    cells = spark.createDataFrame(ct[["distribuidora_id", "celda_id"]].drop_duplicates())
    weather = spark.table(WEATHER_TABLE).select("celda_id", "hora", "met_racha_1h")
    weather = weather.where(F.year("hora").isin([int(y) for y in years]))
    prof = (weather.join(cells, "celda_id").groupBy("distribuidora_id", "hora").agg(F.max("met_racha_1h").alias("racha"))
            .toPandas())
    prof["hora"] = pd.to_datetime(prof["hora"])
    prof["fecha"] = prof["hora"].dt.floor("D")
    prof["h"] = prof["hora"].dt.hour
    return prof[["distribuidora_id", "fecha", "h", "racha"]]


# ---------------------------------------------------------------------------
# The replay
# ---------------------------------------------------------------------------

def crews_of(ccfg: dict, dist: int) -> list:
    chosen = ccfg.get("brigadas", {}).get(str(int(dist)))
    if chosen is None:
        return [1, 2]
    return [int(n) for n in (chosen if isinstance(chosen, (list, tuple)) else [chosen])]


def minutes_table(dist: int, roads: dict, munis: pd.DataFrame, speed: float, factor: float) -> pd.DataFrame:
    """Minutes between the municipalities of a distribuidora: by road when known, straight line otherwise."""
    m = munis[munis["distribuidora_id"] == dist].dropna(subset=["lat", "lon"]).reset_index(drop=True)
    pts = m[["lat", "lon"]].to_numpy(dtype=float)
    straight = pd.DataFrame(rz.haversine_km(pts, pts) * factor / speed * 60.0, index=m["municipio"], columns=m["municipio"])
    road = roads.get(dist)
    if road is None:
        return straight
    by_road = road.reindex(index=straight.index, columns=straight.columns)
    return by_road.where(by_road.notna(), straight)


def replay(days: pd.DataFrame, zone_days: pd.DataFrame, ct: pd.DataFrame, cuts: pd.DataFrame, inc: pd.DataFrame,
           profiles: pd.DataFrame, roads: dict, events: pd.DataFrame, ccfg: dict, zcfg: dict, names: dict) -> tuple:
    """(day_table, ct_table): the replay of every mode, distribuidora and day."""
    speed, factor = float(zcfg["velocidad_kmh"]), float(zcfg["factor_carretera"])
    mobilisation = float(ccfg["movilizacion_min"])
    cap = float(ccfg["recorte_min"])
    work_from, work_to = [int(h) for h in ccfg["horario_laboral"]]
    munis = ct.groupby(["distribuidora_id", "municipio"]).agg(lat=("ct_lat_municipio", "mean"), lon=("ct_lon_municipio", "mean"),
                                                             n_ct=("ct_id", "count"), abonados=("num_abonados", "sum")).reset_index()
    dists = sorted(int(d) for d in days["distribuidora_id"].unique())
    minutes = {d: minutes_table(d, roads, munis, speed, factor) for d in dists}
    static = {}
    for dist in dists:
        m = munis[munis["distribuidora_id"] == dist].copy()
        m["probabilidad"] = 1.0
        m["clientes_esperados"] = m["abonados"].astype(float)
        for n in crews_of(ccfg, dist):
            plan = rz.crew_plan(m, n, speed, factor, None, roads.get(dist))
            static[(dist, n)] = [b["municipio"] for b in plan.get("bases", [])]
    logger.info("Fixed bases (customer-weighted p-median): %s", static)
    shares = {}
    for year in sorted(days["anio"].unique()):
        shares[int(year)] = rz.ct_shares(ct, events[events["inicio_ts"] < pd.Timestamp(f"{int(year)}-01-01")])

    def arrival(dist, bases, municipality):
        table = minutes[dist]
        if municipality not in table.columns:
            return np.nan
        values = [table.loc[b, municipality] for b in bases if b in table.index]
        return float(min(values)) if values else np.nan

    day_rows, ct_rows = [], []
    zone_by_day = {k: g for k, g in zone_days.groupby(["distribuidora_id", "fecha"])} if len(zone_days) else {}
    prof_by_day = {k: g for k, g in profiles.groupby(["distribuidora_id", "fecha"])} if len(profiles) else {}
    inc_by_day = {k: g for k, g in inc.groupby(["distribuidora_id", "fecha"])}
    cuts_by_inc = {k: g for k, g in cuts.groupby(["distribuidora_id", "periodo_id", "incidencia_id"])}
    for mode in MODES:
        pcol, lvl = f"p_{mode}", f"nivel_{mode}"
        for r in days.itertuples():
            dist, day, year = int(r.distribuidora_id), r.fecha, int(r.anio)
            level = getattr(r, lvl)
            if level is None or pd.isna(getattr(r, pcol)):
                continue
            alert = level in ALERT
            prof = prof_by_day.get((dist, day))
            window = (rz.risk_window(pd.DataFrame({"hora": prof["h"].to_numpy(), "racha_max": prof["racha"].to_numpy()}),
                                     float(zcfg["franja_parte_pico"]), float(zcfg["franja_calma_kmh"]))
                      if prof is not None and len(prof) else {"inicio": None, "fin": None})
            zone_p, muni_risk = None, None
            if alert and (dist, day) in zone_by_day:
                z = zone_by_day[(dist, day)].copy()
                z["probabilidad"] = rz.rescale_to_day(z["p_zona"].to_numpy(), float(getattr(r, pcol)))
                muni_risk = rz.municipality_risk(ct[ct["distribuidora_id"] == dist], z, shares[year])
                zone_p = z.sort_values("probabilidad", ascending=False)
            bases = {}
            for n in crews_of(ccfg, dist):
                bases[n] = static[(dist, n)]
                if muni_risk is not None:
                    plan = rz.crew_plan(muni_risk, n, speed, factor, None, roads.get(dist))
                    if plan.get("bases"):
                        bases[n] = [b["municipio"] for b in plan["bases"]]
            today = inc_by_day.get((dist, day))
            n_inc = len(today) if today is not None else 0
            hours, hours_cut = 0.0, 0.0
            if today is not None:
                for i in today.itertuples():
                    c = cuts_by_inc.get((dist, i.periodo_id, i.incidencia_id))
                    if c is not None:
                        hours += float((c["num_abonados"] * c["dur_min"]).sum() / 60.0)
                        hours_cut += float((c["num_abonados"] * c["dur_min"].clip(upper=cap)).sum() / 60.0)
            day_rows.append({"modo": mode, "distribuidora_id": dist, "distribuidora": names.get(dist, str(dist)), "fecha": day.date(),
                             "anio": year, "probabilidad": float(getattr(r, pcol)), "percentil": float(getattr(r, pcol.replace("p_", "pct_"))),
                             "nivel": level, "alerta": bool(alert), "franja_inicio": window.get("inicio"), "franja_fin": window.get("fin"),
                             "bases": "; ".join(bases[max(bases)]) if bases else "", "incidencias": int(n_inc),
                             "clientes_afectados": float(today["clientes"].sum()) if today is not None else 0.0,
                             "clientes_hora": hours, "clientes_hora_recortadas": hours_cut,
                             "corte_max_min": float(today["duracion_max_min"].max()) if today is not None else 0.0})
            if today is None:
                continue
            for i in today.itertuples():
                in_window = window.get("inicio") is not None and window["inicio"] <= i.inicio_ts.hour <= window["fin"]
                on_call = not (i.inicio_ts.dayofweek < 5 and work_from <= i.inicio_ts.hour < work_to)
                rank = None
                if zone_p is not None and i.zona in set(zone_p["zona"]):
                    rank = zone_p["zona"].tolist().index(i.zona) + 1
                c = cuts_by_inc.get((dist, i.periodo_id, i.incidencia_id))
                if c is None:
                    continue
                for n in crews_of(ccfg, dist):
                    t_static = arrival(dist, static[(dist, n)], i.municipio)
                    t_plan = arrival(dist, bases[n], i.municipio)
                    base_total = t_static + (mobilisation if on_call else 0.0)
                    plan_total = t_plan + (mobilisation if (on_call and not (alert and in_window)) else 0.0)
                    for k in c.itertuples():
                        dur = float(k.dur_min) if pd.notna(k.dur_min) else 0.0
                        needs = bool(np.isfinite(base_total) and dur > base_total)
                        travel = float(np.clip(t_static - t_plan, -dur, dur)) if needs and np.isfinite(t_plan) else 0.0
                        total = float(np.clip(base_total - plan_total, -dur, dur)) if needs and np.isfinite(plan_total) else 0.0
                        ct_rows.append({"modo": mode, "brigadas": n, "distribuidora_id": dist, "distribuidora": names.get(dist, str(dist)),
                                        "fecha": day.date(), "anio": year, "incidencia": f"{i.periodo_id}-{i.incidencia_id}",
                                        "inicio_ts": i.inicio_ts, "causa": i.causa, "municipio": i.municipio, "zona": i.zona,
                                        "ct_id": k.ct_id, "clientes": float(k.num_abonados), "kva": float(k.potencia_kva), "dur_min": dur,
                                        "alerta": bool(alert), "nivel": level, "en_franja": bool(in_window), "fuera_horario": bool(on_call),
                                        "puesto_zona": rank, "llegada_fija_min": t_static, "llegada_plan_min": t_plan,
                                        "llegada_fija_total_min": base_total, "llegada_plan_total_min": plan_total,
                                        "espero_brigada": needs, "min_ganados_viaje": travel, "min_ganados_total": total,
                                        "clientes_min_ganados": total * float(k.num_abonados), "kva_min_ganados": total * float(k.potencia_kva)})
    return pd.DataFrame(day_rows), pd.DataFrame(ct_rows)


def summarise(day_table: pd.DataFrame, ct_table: pd.DataFrame, installed: dict, cap: float) -> pd.DataFrame:
    """One row per mode, distribuidora, number of crews and year."""
    rows = []
    if day_table.empty:
        return pd.DataFrame()
    for (mode, dist, year), g in day_table.groupby(["modo", "distribuidora_id", "anio"]):
        cc_all = ct_table[(ct_table["modo"] == mode) & (ct_table["distribuidora_id"] == dist) & (ct_table["anio"] == year)] if len(ct_table) else ct_table
        for n in sorted(cc_all["brigadas"].unique()) if len(cc_all) else [None]:
            cc = cc_all[cc_all["brigadas"] == n] if n is not None else cc_all
            capped = cc["dur_min"].clip(upper=cap) if len(cc) else pd.Series(dtype=float)
            incs = cc.drop_duplicates("incidencia") if len(cc) else cc
            on_alert = incs[incs["alerta"]] if len(incs) else incs
            waited = cc[cc["espero_brigada"]] if len(cc) else cc
            kva = float(installed.get(dist, np.nan))
            rows.append({
                "modo": mode, "distribuidora_id": int(dist), "distribuidora": g["distribuidora"].iloc[0], "brigadas": n, "anio": int(year),
                "dias": int(len(g)), "dias_alerta": int(g["alerta"].sum()), "dias_con_incidencia": int((g["incidencias"] > 0).sum()),
                "dias_alerta_con_incidencia": int(((g["incidencias"] > 0) & g["alerta"]).sum()),
                "incidencias": int(len(incs)), "incidencias_en_alerta": int(len(on_alert)),
                "clientes_hora": float((cc["clientes"] * cc["dur_min"]).sum() / 60.0) if len(cc) else 0.0,
                "clientes_hora_en_alerta": float((cc.loc[cc["alerta"], "clientes"] * cc.loc[cc["alerta"], "dur_min"]).sum() / 60.0) if len(cc) else 0.0,
                "clientes_hora_recortadas": float((cc["clientes"] * capped).sum() / 60.0) if len(cc) else 0.0,
                "clientes_hora_recortadas_en_alerta": float((cc.loc[cc["alerta"], "clientes"] * capped[cc["alerta"]]).sum() / 60.0) if len(cc) else 0.0,
                "cortes_ct": int(len(cc)), "cortes_ct_esperaron_brigada": int(len(waited)),
                "clientes_hora_esperaron_brigada": float((waited["clientes"] * waited["dur_min"]).sum() / 60.0) if len(waited) else 0.0,
                "llegada_fija_media_min": float(incs["llegada_fija_min"].mean()) if len(incs) else np.nan,
                "llegada_plan_media_min_en_alerta": float(on_alert["llegada_plan_min"].mean()) if len(on_alert) else np.nan,
                "clientes_hora_ganadas": float(cc["clientes_min_ganados"].sum() / 60.0) if len(cc) else 0.0,
                "tiepi_min": float((cc["kva"] * cc["dur_min"]).sum() / kva) if len(cc) and kva > 0 else np.nan,
                "tiepi_min_ganados": float(cc["kva_min_ganados"].sum() / kva) if len(cc) and kva > 0 else np.nan,
                "horas_franja_alerta": float((g.loc[g["alerta"], "franja_fin"] - g.loc[g["alerta"], "franja_inicio"] + 1).fillna(0).sum()),
                "zona_top3_en_alerta": float((on_alert["puesto_zona"] <= 3).mean()) if len(on_alert) and on_alert["puesto_zona"].notna().any() else np.nan,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

def history_payload(day_table: pd.DataFrame, levels: dict, names: dict) -> dict:
    payload = {"generado": rz.now_text(), "modos": {k: v for k, v in MODES.items()}, "niveles": levels, "distribuidoras": []}
    for dist in sorted(day_table["distribuidora_id"].unique()):
        block = {"id": int(dist), "nombre": names.get(int(dist), str(dist)), "dias": {}}
        for mode in MODES:
            x = day_table[(day_table["modo"] == mode) & (day_table["distribuidora_id"] == dist)].sort_values("fecha")
            block["dias"][mode] = [{"f": str(r.fecha), "n": r.nivel, "p": round(float(r.probabilidad), 4), "i": int(r.incidencias),
                                    "ch": round(float(r.clientes_hora)), "chr": round(float(r.clientes_hora_recortadas)),
                                    "c": int(r.clientes_afectados), "m": round(float(r.corte_max_min)),
                                    "b": r.bases if r.alerta else None} for r in x.itertuples()]
        payload["distribuidoras"].append(block)
    return payload


def level_table(d: pd.DataFrame) -> pd.DataFrame:
    """Days, incidents and customer-hours (capped and raw) by the level of the evening before, as shown in the report."""
    by = d.groupby("nivel").agg(dias=("fecha", "count"), con_averia=("incidencias", lambda s: int((s > 0).sum())),
                                incidencias=("incidencias", "sum"), horas_cliente=("clientes_hora_recortadas", "sum"),
                                sin_recortar=("clientes_hora", "sum")).reindex(["bajo", "medio", "alto", "muy alto"]).dropna(how="all")
    by["parte de días"] = (by["dias"] / by["dias"].sum()).round(3)
    by["tasa de avería"] = (by["con_averia"] / by["dias"]).round(3)
    by["parte de horas-cliente"] = (by["horas_cliente"] / by["horas_cliente"].sum()).round(2) if by["horas_cliente"].sum() > 0 else 0.0
    by["parte sin recortar"] = (by["sin_recortar"] / by["sin_recortar"].sum()).round(2) if by["sin_recortar"].sum() > 0 else 0.0
    return by.reset_index().rename(columns={"nivel": "nivel previsto", "con_averia": "días con avería",
                                            "horas_cliente": "horas-cliente (recortadas)"}).drop(columns=["sin_recortar"])


def report_text(summary: pd.DataFrame, day_table: pd.DataFrame, ct_table: pd.DataFrame, ccfg: dict, run_id: str) -> str:
    parts = [f"# Lo que habría dicho el sistema: {run_id}",
             "Cada día se puntúa con el modelo de cada tarde entrenado solo con los años anteriores; las averías son las "
             "incidencias de causa local con los clientes y la duración real de cada corte de CT. Las bases fijas son la "
             "p-mediana por clientes; las del día, la del riesgo de mañana en los días en alerta (alto o muy alto). "
             f"Movilización de {number(float(ccfg['movilizacion_min']), 0)} min fuera del horario laboral cuando la brigada está "
             f"de retén; cortes recortados a {number(float(ccfg['recorte_min']), 0)} min en las columnas recortadas."]
    if summary.empty:
        return "\n\n".join(parts + ["Sin días que puntuar."])
    for mode, text in MODES.items():
        s = summary[summary["modo"] == mode]
        if s.empty:
            continue
        top = s[s["brigadas"] == s.groupby("distribuidora_id")["brigadas"].transform("max")]
        shown = pd.DataFrame({
            "distribuidora": top["distribuidora"], "año": top["anio"], "días en alerta": top["dias_alerta"],
            "incidencias": top["incidencias"], "en alerta": top["incidencias_en_alerta"],
            "horas-cliente (recortadas)": top["clientes_hora_recortadas"].round(0),
            "parte en alerta": (top["clientes_hora_recortadas_en_alerta"] / top["clientes_hora_recortadas"].where(top["clientes_hora_recortadas"] > 0)).round(2),
            "parte sin recortar": (top["clientes_hora_en_alerta"] / top["clientes_hora"].where(top["clientes_hora"] > 0)).round(2),
            "TIEPI local (min)": top["tiepi_min"].round(1),
        })
        parts.append(f"## {text}\n\n" + ml_metrics.markdown_table(shown, 2))
        crews = pd.DataFrame({
            "distribuidora": s["distribuidora"], "brigadas": s["brigadas"], "año": s["anio"], "cortes de CT": s["cortes_ct"],
            "esperaron a la brigada": s["cortes_ct_esperaron_brigada"], "llegada fija (min)": s["llegada_fija_media_min"].round(0),
            "llegada con las bases del día, en alerta (min)": s["llegada_plan_media_min_en_alerta"].round(0),
            "horas-cliente ganadas": s["clientes_hora_ganadas"].round(0), "TIEPI ganado (min)": s["tiepi_min_ganados"].round(2),
        })
        parts.append("### Brigadas\n\n" + ml_metrics.markdown_table(crews, 2))
        # By level, the distribuidoras with enough incidents together: the levels are percentiles of each distribuidora,
        # so the days of a network with one incident in years would only add zeros to every level.
        d = day_table[day_table["modo"] == mode]
        counts = d.groupby("distribuidora")["incidencias"].sum()
        enough = sorted(counts[counts >= int(ccfg["minimo_incidencias_nivel"])].index)
        d = d[d["distribuidora"].isin(enough)]
        by = level_table(d)
        parts.append(f"### Por nivel previsto ({', '.join(enough) or 'ninguna distribuidora'} con al menos "
                     f"{int(ccfg['minimo_incidencias_nivel'])} incidencias en el periodo; {len(d)} días de distribuidora)\n\n"
                     + ml_metrics.markdown_table(by, 3))
        if len(enough) > 1:
            per = pd.concat([level_table(d[d["distribuidora"] == name]).assign(distribuidora=name) for name in enough])
            per = per[["distribuidora"] + [c for c in per.columns if c != "distribuidora"]]
            parts.append("### Por nivel previsto y distribuidora\n\n" + ml_metrics.markdown_table(per, 3))
        worst = d[d["incidencias"] > 0].sort_values("clientes_hora_recortadas", ascending=False).head(int(ccfg["peores_dias"]))
        if len(worst):
            w = pd.DataFrame({"día": [f"{pd.Timestamp(f):%d.%m.%Y}" for f in worst["fecha"]], "distribuidora": worst["distribuidora"],
                              "nivel previsto": worst["nivel"], "probabilidad": worst["probabilidad"].round(3),
                              "incidencias": worst["incidencias"], "clientes": worst["clientes_afectados"].round(0),
                              "horas-cliente (recortadas)": worst["clientes_hora_recortadas"].round(0), "sin recortar": worst["clientes_hora"].round(0),
                              "corte más largo (min)": worst["corte_max_min"].round(0), "bases del día": worst["bases"]})
            parts.append(f"### Los {len(worst)} peores días\n\n" + ml_metrics.markdown_table(w, 3))
    return "\n\n".join(parts)


def write_history(payload: dict, config: dict, rcfg: dict, folder_id: str, dashboard_dir: str = None):
    targets = []
    folder = config.get("exportar_dir")
    if folder:
        targets.append(os.path.join(folder, folder_id, "historico.json"))
    dashboard = dashboard_dir or rcfg.get("dashboard_dir")
    if dashboard and os.path.isdir(dashboard):
        targets.append(os.path.join(dashboard, "historico.json"))
    elif dashboard:
        logger.warning("Dashboard folder %s not found: historico.json only in the export folder", dashboard)
    for target in targets:
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as file:
                json.dump(rz.clean(payload), file, ensure_ascii=False, separators=(",", ":"))
            logger.info("historico.json written in %s", target)
        except OSError as error:
            logger.error("historico.json not written in %s: %s", target, error)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    rcfg = risk_settings(config)
    zcfg = {**RISK_DEFAULTS["riesgo_zonas"], **rcfg.get("riesgo_zonas", {})}
    ccfg = case_settings(config)
    cfg = settings(config)
    years = [int(y) for y in (args.anios.split(",") if args.anios else ccfg["anios"]) if str(y).strip()]
    spark = get_spark(config)
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    names = {int(k): str(v) for k, v in rcfg.get("nombres", {}).items()}

    variant = next((v for v in cfg["variantes"] if v["nombre"] == rcfg["variante"]), None)
    if variant is None:
        raise ValueError(f"Variant {rcfg['variante']} not in meteo_dia.variantes")
    pdf, blocks, _ = build_dataset(spark, config, cfg)
    days = day_scores(pdf, blocks, variant, years, config, cfg, rcfg["niveles"])
    if days.empty:
        raise ValueError(f"No day of {years} could be scored")

    ct = network(spark)
    events, _ = read_events(spark, cfg)
    cuts = ct_cuts(spark, events, ct)
    cuts = cuts[cuts["fecha"].dt.year.isin(years)]
    inc = incidents_of(cuts)
    logger.info("Incidents of a local cause in %s: %s (%s CT cuts)", years, len(inc), len(cuts))
    zone_days = zone_scores(spark, pdf, ct, events, variant, years, config, cfg)
    profiles = gust_profiles(spark, ct, years)
    landing = args.landing or rcfg["landing"]
    fs = live_filesystem(landing)
    munis = ct.groupby(["distribuidora_id", "municipio"]).agg(lat=("ct_lat_municipio", "mean"), lon=("ct_lon_municipio", "mean")).reset_index()
    roads = {}
    for dist in sorted(ct["distribuidora_id"].unique()):
        found = road_minutes(fs, zcfg, munis[munis["distribuidora_id"] == dist], int(dist))
        if found is not None:
            roads[int(dist)] = found
    installed = (ct.groupby("distribuidora_id")["potencia_tiepi_kva"].sum() if "potencia_tiepi_kva" in ct.columns
                 else ct.groupby("distribuidora_id")["potencia_kva"].sum()).to_dict()

    day_table, ct_table = replay(days, zone_days, ct, cuts, inc, profiles, roads, events, ccfg, zcfg, names)
    summary = summarise(day_table, ct_table, installed, float(ccfg["recorte_min"]))
    for frame in (day_table, summary):
        frame["run_id"] = args.run_id
        frame["creado_ts"] = created
    stored = day_table.copy()
    stored["fecha"] = pd.to_datetime(stored["fecha"])
    for column in ("franja_inicio", "franja_fin"):
        stored[column] = pd.to_numeric(stored[column], errors="coerce").astype("float64")
    save_frame(spark, stored, ccfg["tabla_dias"], args.run_id)
    if not summary.empty:
        save_frame(spark, summary.assign(brigadas=summary["brigadas"].astype("float64")), ccfg["tabla_resumen"], args.run_id)
    folder_id = "".join(c if c.isalnum() or c in "_.-" else "_" for c in args.run_id)
    report = report_text(summary, day_table, ct_table, ccfg, args.run_id)
    export(config, folder_id, report, {"dias": day_table, "cts": ct_table, "resumen_anual": summary})
    write_history(history_payload(day_table, rcfg["niveles"], names), config, rcfg, folder_id, args.dashboard_dir)
    run = pd.DataFrame([{
        "run_id": args.run_id, "creado_ts": created, "dataset_version": f"caso_negocio_{min(years)}_{max(years)}",
        "etiqueta": f"caso_negocio:{MAIN_LABEL}", "n_features": 0, "modelos_json": json.dumps({"dia": "logistica", "zona": "logistica"}),
        "busqueda_json": "{}", "config_json": json.dumps({"caso_negocio": ccfg, "anios": years}, ensure_ascii=False, default=str),
        "resumen_json": json.dumps({"dias": int(len(day_table)), "incidencias": int(len(inc)),
                                    "alertas": int(day_table["alerta"].sum()) if len(day_table) else 0}),
        "splits_evaluados": "", "artefactos_uri": "",
    }])
    save_frame(spark, run, RUNS_TABLE, args.run_id)
    logger.info("Business case replay %s done: %s day rows, %s CT rows", args.run_id, len(day_table), len(ct_table))
    return 0


if __name__ == "__main__":
    sys.exit(main())
