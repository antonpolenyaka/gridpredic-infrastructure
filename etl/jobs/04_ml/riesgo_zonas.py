"""
Where, when, why and how much: the risk of tomorrow below the distribuidora
(job_ml_riesgo_manana, riesgo_manana.riesgo_zonas of config_ml.json). No Spark here:
the job reads the tables and passes pandas frames, so the functions can be
tested on small synthetic data.

- Zones. The unit is the network group of TedisNet (grupo_red_id of dim_ct,
  the line the CT hangs from): 70 lines in the three distribuidoras, with a
  median of 15 - 20 CT each, and 60 % of the incidents touch a single one.
  A zone-day model (zone_days, fit_zone_model) learns which lines fail on
  which days from their size, their history of incidents, the exposure of
  their cells and the weather of the day of their own cells on top of the
  features of the distribuidora. Measured on 2022 - 2025, the size of the
  line and its history tell the lines apart about as well as anything the
  weather adds at 10 km: the failing line is among the five of most risk of
  its distribuidora in one of four incidents (two of five would be chance
  among 20 - 40 lines), and the failing quarter of the territory is among
  the two of most risk in 62 % of the incidents (79 % on the windy days).
  So the zone probabilities split the risk of the day over the network by
  exposure and local weather; they do not say which line will break.
- The split keeps the probability of the day. The zone scores are rescaled
  (rescale_to_day) so that the probability that some zone fails equals the
  daily probability of the distribuidora, the one that is validated.
- Municipalities and CTs inherit the risk of their lines (municipality_risk,
  ct_risk): a CT has the probability of its line times the share of the
  incidents of the line that cut it (shrunk towards the mean of the line).
- When: the hours of the day with the forecast gust near its peak
  (risk_window). Three of five incidents of weather start within three
  hours of their cell's strongest gust of the day, more on the windy
  days (lag_stats, computed again every evening).
- Why: the mix of causes of the incidents of days like tomorrow (cause_mix),
  by the strongest forecast gust: with gusts of 70 km/h or more, half of
  the incidents are weather; below 40 km/h, equipment, animals and unknown
  causes dominate.
- How much: expected incidents, CTs, customers and kVA out of service
  (impact), from what the incidents of each line usually cut.
- Crews: the bases that minimise the expected arrival time to the risk of
  tomorrow (crew_plan), a p-median over the municipalities of the CTs with
  the minutes by road of OSRM over OpenStreetMap when the job has them
  (osrm_table) and straight-line distances times a road factor otherwise;
  and the cascade rule
  (cascade_rule): after a first incident, another one follows within three
  hours in 11 % of the cases, 30 % when the first one was weather.
"""

import json
import logging
import math
import os
import urllib.request
from datetime import datetime, timezone

import numpy as np
import pandas as pd

LOCAL_EXCLUDED = ("aguas_arriba", "apagon", "programada", "maniobra", "cliente_bt", "red_bt")
CAUSE_FAMILIES = {"vegetacion": "meteorologia", "incendio": "meteorologia", "objeto_ajeno": "terceros",
                  "agente_externo": "terceros", "sin_causa": "desconocida", "red_bt": "red_bt"}
CAUSE_NAMES = {"meteorologia": "meteorología y vegetación", "equipo": "equipos", "fauna": "fauna",
               "desconocida": "causa desconocida", "terceros": "terceros", "aguas_arriba": "aguas arriba",
               "apagon": "apagón", "programada": "programada", "maniobra": "maniobra", "red_bt": "red de BT",
               "cliente_bt": "cliente"}
GUST_BUCKETS = [0.0, 40.0, 55.0, 70.0, 1000.0]
GUST_LABELS = ["menos de 40 km/h", "40 - 55 km/h", "55 - 70 km/h", "70 km/h o más"]
HISTORY_WINDOWS = (30, 365, 1095)
ZONE_FEATURES_SIZE = ["z_n_ct", "z_kva", "z_abonados", "z_rural"]
ZONE_FEATURES_HISTORY = [f"z_hist_{w}d" for w in HISTORY_WINDOWS] + ["z_hist_dias_desde"]
ZONE_FEATURES_WEATHER = ["z_racha_max", "z_clima_max", "z_racha_media", "z_clima_media", "z_lluvia_max",
                         "z_lluvia_media", "z_temp_max"]
EARTH_KM = 6371.0


# ---------------------------------------------------------------------------
# Zones
# ---------------------------------------------------------------------------

def zone_table(dim: pd.DataFrame, cells: pd.DataFrame) -> pd.DataFrame:
    """
    One row per CT with its zone (grupo_red_id), municipality, coordinates
    (of the municipality, map_ct_celda), cell, power and customers. CTs
    without zone or cell are left out.
    """
    ct = dim.copy()
    ct["ct_id"] = ct["ct_id"].astype(str)
    ct["distribuidora_id"] = ct["distribuidora_id"].astype("int64")
    c = cells.copy()
    c["ct_id"] = c["ct_id"].astype(str)
    c["distribuidora_id"] = c["distribuidora_id"].astype("int64")
    keep = [k for k in ("celda_id", "ct_lat_municipio", "ct_lon_municipio", "codigo_ine") if k in c.columns]
    ct = ct.merge(c[["distribuidora_id", "ct_id"] + keep], on=["distribuidora_id", "ct_id"], how="left")
    ct = ct[ct["grupo_red_id"].notna() & ct["celda_id"].notna()].copy()
    ct["zona"] = ct["grupo_red_id"].astype("int64").astype(str)
    for column in ("potencia_kva", "num_abonados"):
        ct[column] = pd.to_numeric(ct.get(column), errors="coerce").fillna(0.0)
    # z_rural is the share of CT in rural dispersa (code 4 of the zone of the municipality); the CT of rural
    # concentrada (3), most of the CT of the three networks, do not count as rural here.
    ct["rural"] = (pd.to_numeric(ct.get("tipo_zona_codigo"), errors="coerce") == 4).astype(float)
    ct["municipio"] = ct["municipio_nombre"].fillna("").astype(str)
    return ct.reset_index(drop=True)


def zones_of(ct: pd.DataFrame) -> pd.DataFrame:
    """(distribuidora_id, zona, z_n_ct, z_kva, z_abonados, z_rural, lat, lon, municipios, nombre)."""
    out = ct.groupby(["distribuidora_id", "zona"]).agg(
        z_n_ct=("ct_id", "count"), z_kva=("potencia_kva", "sum"), z_abonados=("num_abonados", "sum"),
        z_rural=("rural", "mean"), lat=("ct_lat_municipio", "mean"), lon=("ct_lon_municipio", "mean"),
    ).reset_index()
    names = ct.groupby(["distribuidora_id", "zona"])["municipio"].agg(
        lambda s: [m for m, _ in s[s != ""].value_counts().items()])
    out["municipios"] = [names.get((d, z), []) for d, z in zip(out["distribuidora_id"], out["zona"])]
    out["nombre"] = [zone_name(ct, d, z) for d, z in zip(out["distribuidora_id"], out["zona"])]
    return out


def zone_name(ct: pd.DataFrame, dist, zone: str) -> str:
    """The line named by its most frequent municipalities (the network group of TedisNet has only an id)."""
    part = ct[(ct["distribuidora_id"] == dist) & (ct["zona"] == zone)]
    names = [m for m, _ in part["municipio"][part["municipio"] != ""].value_counts().head(2).items()]
    return " / ".join(names) if names else f"línea {zone}"


def local_starts(events: pd.DataFrame, ct: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (incident, zone) of the local causes (what a crew is sent
    to): distribuidora_id, zona, inicio_ts, conocido_ts, fecha, causa,
    periodo_id, incidencia_id. The zone of each CT is the one of today.
    """
    ev = events.copy()
    ev["ct_id"] = ev["ct_id"].astype(str)
    ev["distribuidora_id"] = ev["distribuidora_id"].astype("int64")
    ev = ev.merge(ct[["distribuidora_id", "ct_id", "zona"]], on=["distribuidora_id", "ct_id"])
    ev = ev[~ev["causa"].fillna("").isin(LOCAL_EXCLUDED)]
    ev["inicio_ts"] = pd.to_datetime(ev["inicio_ts"])
    ev["conocido_ts"] = pd.to_datetime(ev.get("conocido_ts", ev["inicio_ts"]))
    keys = ["distribuidora_id", "periodo_id", "incidencia_id", "zona"]
    starts = ev.sort_values("inicio_ts").drop_duplicates(keys).copy()
    starts["fecha"] = starts["inicio_ts"].dt.floor("D")
    return starts.reset_index(drop=True)


def zone_history(grid: pd.DataFrame, starts: pd.DataFrame) -> pd.DataFrame:
    """z_hist_<n>d (incidents of the zone known before the day) and z_hist_dias_desde, on a (zona, fecha) grid."""
    grid = grid.sort_values(["distribuidora_id", "zona", "fecha"]).reset_index(drop=True)
    for w in HISTORY_WINDOWS:
        grid[f"z_hist_{w}d"] = 0.0
    grid["z_hist_dias_desde"] = 3650.0
    known = starts.dropna(subset=["conocido_ts"])
    for key, idx in grid.groupby(["distribuidora_id", "zona"]).indices.items():
        k = np.sort(known[(known["distribuidora_id"] == key[0]) & (known["zona"] == key[1])]["conocido_ts"]
                    .to_numpy().astype("datetime64[s]").astype(np.int64))
        t = grid.loc[idx, "fecha"].to_numpy().astype("datetime64[s]").astype(np.int64)
        upto = np.searchsorted(k, t, side="right")
        for w in HISTORY_WINDOWS:
            since = np.searchsorted(k, t - w * 86400, side="right")
            grid.loc[idx, f"z_hist_{w}d"] = (upto - since).astype(float)
        if len(k):
            last = k[np.clip(upto - 1, 0, len(k) - 1)]
            days = np.where(upto > 0, (t - last) / 86400.0, 3650.0)
            grid.loc[idx, "z_hist_dias_desde"] = np.minimum(days, 3650.0)
    return grid


def zone_weather(ct: pd.DataFrame, cell_days: pd.DataFrame) -> pd.DataFrame:
    """
    (distribuidora_id, zona, fecha, z_racha_max, z_clima_max, z_racha_media,
    z_clima_media, z_lluvia_max, z_lluvia_media, z_temp_max) from the daily
    weather of the cells (celda_id, fecha, racha_max, clima_max, lluvia,
    temp_max): the worst cell and the mean of the cells weighted by their CT.
    """
    weights = ct.groupby(["distribuidora_id", "zona", "celda_id"]).size().rename("n").reset_index()
    j = weights.merge(cell_days, on="celda_id")
    for c in ("racha_max", "clima_max", "lluvia"):
        j[f"w_{c}"] = j[c] * j["n"]
        j[f"n_{c}"] = j["n"].where(j[c].notna(), 0.0)
    keys = ["distribuidora_id", "zona", "fecha"]
    out = j.groupby(keys).agg(z_racha_max=("racha_max", "max"), z_clima_max=("clima_max", "max"),
                              z_lluvia_max=("lluvia", "max"), z_temp_max=("temp_max", "max"),
                              **{f"w_{c}": (f"w_{c}", "sum") for c in ("racha_max", "clima_max", "lluvia")},
                              **{f"n_{c}": (f"n_{c}", "sum") for c in ("racha_max", "clima_max", "lluvia")}).reset_index()
    for c, name in (("racha_max", "z_racha_media"), ("clima_max", "z_clima_media"), ("lluvia", "z_lluvia_media")):
        out[name] = out[f"w_{c}"] / out[f"n_{c}"].where(out[f"n_{c}"] > 0)
    return out[keys + ZONE_FEATURES_WEATHER]


def zone_days(zones: pd.DataFrame, starts: pd.DataFrame, weather: pd.DataFrame, days: pd.DataFrame,
              day_features: list, first: str, last: str) -> pd.DataFrame:
    """
    The training table of the zone model: one row per zone and day from
    first to last with the size of the zone, its history, the weather of its
    cells, the features of the distribuidora of the day (days: the daily
    table of job_ml_riesgo_dia, columns day_features) and y (an incident
    of a local cause starts in the zone that day).
    """
    span = pd.DataFrame({"fecha": pd.date_range(first, last, freq="D")})
    grid = zones[["distribuidora_id", "zona"] + ZONE_FEATURES_SIZE].merge(span, how="cross")
    grid = grid.merge(weather, on=["distribuidora_id", "zona", "fecha"], how="left")
    grid = zone_history(grid, starts)
    labels = starts.groupby(["distribuidora_id", "zona", "fecha"]).size().rename("n_inc").reset_index()
    grid = grid.merge(labels, on=["distribuidora_id", "zona", "fecha"], how="left")
    grid["y"] = (grid["n_inc"].fillna(0) > 0).astype("int8")
    grid = grid.drop(columns="n_inc")
    grid = grid.merge(days[["distribuidora_id", "fecha"] + day_features], on=["distribuidora_id", "fecha"], how="left")
    for dist in sorted(zones["distribuidora_id"].unique()):
        grid[f"dist_{dist}"] = (grid["distribuidora_id"] == dist).astype(float)
    return grid


def zone_features(zones: pd.DataFrame, day_features: list) -> list:
    dists = [f"dist_{d}" for d in sorted(zones["distribuidora_id"].unique())]
    return dists + ZONE_FEATURES_SIZE + ZONE_FEATURES_HISTORY + ZONE_FEATURES_WEATHER + list(day_features)


def rescale_to_day(scores, p_day: float) -> np.ndarray:
    """
    The zone probabilities moved on the odds scale by one factor so that the
    probability that at least one zone fails equals p_day. The relative
    order of the zones is the one of the zone model; the level is the one of
    the model of the day.
    """
    s = np.clip(np.asarray(scores, dtype=np.float64), 1e-6, 1 - 1e-6)
    if len(s) == 0 or not (0 < p_day < 1):
        return s
    logit = np.log(s / (1 - s))

    def any_fail(shift):
        p = 1 / (1 + np.exp(-(logit + shift)))
        return 1 - np.prod(1 - p)

    lo, hi = -20.0, 20.0
    for _ in range(80):
        mid = (lo + hi) / 2
        if any_fail(mid) < p_day:
            lo = mid
        else:
            hi = mid
    return 1 / (1 + np.exp(-(logit + (lo + hi) / 2)))


def level_of(percentile: float, levels: dict) -> str:
    for name, threshold in sorted(levels.items(), key=lambda item: -float(item[1])):
        if percentile >= float(threshold):
            return name
    return "bajo"


# ---------------------------------------------------------------------------
# Municipalities, CTs and impact
# ---------------------------------------------------------------------------

def ct_shares(ct: pd.DataFrame, events: pd.DataFrame, prior: float = 0.5, weight: float = 5.0) -> pd.Series:
    """
    The share of the incidents of its zone that cut each CT, shrunk towards
    the mean of the zone (prior with the weight of weight incidents). Indexed
    by (distribuidora_id, ct_id). A line that breaks near its head cuts every
    CT; a branch, only the CTs below it.
    """
    ev = events.copy()
    ev["ct_id"] = ev["ct_id"].astype(str)
    ev["distribuidora_id"] = ev["distribuidora_id"].astype("int64")
    ev = ev[~ev["causa"].fillna("").isin(LOCAL_EXCLUDED)]
    ev = ev.merge(ct[["distribuidora_id", "ct_id", "zona"]], on=["distribuidora_id", "ct_id"])
    keys = ["distribuidora_id", "periodo_id", "incidencia_id"]
    per_zone = ev.drop_duplicates(keys + ["zona"]).groupby(["distribuidora_id", "zona"]).size().rename("n_zona")
    per_ct = ev.drop_duplicates(keys + ["ct_id"]).groupby(["distribuidora_id", "ct_id"]).size().rename("n_ct")
    table = ct[["distribuidora_id", "ct_id", "zona"]].merge(per_ct.reset_index(), on=["distribuidora_id", "ct_id"], how="left")
    table = table.merge(per_zone.reset_index(), on=["distribuidora_id", "zona"], how="left")
    n_ct, n_zone = table["n_ct"].fillna(0.0), table["n_zona"].fillna(0.0)
    mean_zone = table.groupby(["distribuidora_id", "zona"])["n_ct"].transform(lambda s: s.fillna(0).mean())
    zone_prior = (mean_zone / n_zone.where(n_zone > 0)).fillna(prior).clip(0.05, 1.0)
    share = (n_ct + weight * zone_prior) / (n_zone + weight)
    return pd.Series(share.clip(0.02, 1.0).to_numpy(), index=pd.MultiIndex.from_frame(table[["distribuidora_id", "ct_id"]]))


def municipality_risk(ct: pd.DataFrame, zone_p: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    """
    (distribuidora_id, municipio, codigo_ine, lat, lon, n_ct, abonados, kva,
    probabilidad, zonas): the probability that some CT of the municipality
    goes out, 1 - prod over its CTs of (1 - p_zone * share_ct), grouping
    the CTs of the same zone (they go out together: the zone probability
    times the largest share).
    """
    table = ct.merge(zone_p[["distribuidora_id", "zona", "probabilidad"]], on=["distribuidora_id", "zona"], how="left")
    table["share"] = shares.reindex(pd.MultiIndex.from_frame(table[["distribuidora_id", "ct_id"]])).to_numpy()
    table["p_ct"] = table["probabilidad"].fillna(0.0) * table["share"].fillna(0.5)
    rows = []
    for (dist, muni), part in table.groupby(["distribuidora_id", "municipio"]):
        by_zone = part.groupby("zona")["p_ct"].max()
        p = 1 - float(np.prod(1 - by_zone.clip(0, 1).to_numpy()))
        rows.append({"distribuidora_id": dist, "municipio": muni, "codigo_ine": part["codigo_ine"].iloc[0] if "codigo_ine" in part else None,
                     "lat": float(part["ct_lat_municipio"].mean()), "lon": float(part["ct_lon_municipio"].mean()),
                     "n_ct": int(len(part)), "abonados": float(part["num_abonados"].sum()), "kva": float(part["potencia_kva"].sum()),
                     "probabilidad": p, "zonas": sorted(part["zona"].unique().tolist()),
                     "clientes_esperados": float((part["p_ct"] * part["num_abonados"]).sum())})
    return pd.DataFrame(rows)


def ct_risk(ct: pd.DataFrame, zone_p: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    table = ct.merge(zone_p[["distribuidora_id", "zona", "probabilidad"]], on=["distribuidora_id", "zona"], how="left")
    table["share"] = shares.reindex(pd.MultiIndex.from_frame(table[["distribuidora_id", "ct_id"]])).to_numpy()
    table["probabilidad"] = table["probabilidad"].fillna(0.0) * table["share"].fillna(0.5)
    return table[["distribuidora_id", "ct_id", "zona", "municipio", "num_abonados", "potencia_kva", "probabilidad"]]


def impact_stats(ct: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """
    Per zone, what its incidents usually cut: median CTs, customers and kVA
    affected and median duration in hours (duracion_winsor_s when present).
    Zones with fewer than three incidents take the medians of their
    distribuidora.
    """
    ev = events.copy()
    ev["ct_id"] = ev["ct_id"].astype(str)
    ev["distribuidora_id"] = ev["distribuidora_id"].astype("int64")
    ev = ev[~ev["causa"].fillna("").isin(LOCAL_EXCLUDED)]
    ev = ev.merge(ct[["distribuidora_id", "ct_id", "zona", "num_abonados", "potencia_kva"]], on=["distribuidora_id", "ct_id"])
    if "duracion_winsor_s" in ev.columns:
        ev["horas"] = pd.to_numeric(ev["duracion_winsor_s"], errors="coerce") / 3600.0
    elif "fin_ts" in ev.columns:
        ev["horas"] = (pd.to_datetime(ev["fin_ts"]) - pd.to_datetime(ev["inicio_ts"])) / pd.Timedelta(hours=1)
    else:
        ev["horas"] = np.nan
    keys = ["distribuidora_id", "zona", "periodo_id", "incidencia_id"]
    per = ev.groupby(keys).agg(cts=("ct_id", "nunique"), abonados=("num_abonados", "sum"), kva=("potencia_kva", "sum"),
                               horas=("horas", "median")).reset_index()
    zone_stats = per.groupby(["distribuidora_id", "zona"]).agg(n=("cts", "size"), cts=("cts", "median"),
                                                               abonados=("abonados", "median"), kva=("kva", "median"),
                                                               horas=("horas", "median")).reset_index()
    dist_stats = per.groupby("distribuidora_id").agg(cts=("cts", "median"), abonados=("abonados", "median"),
                                                     kva=("kva", "median"), horas=("horas", "median"))
    out = ct.groupby(["distribuidora_id", "zona"]).size().rename("z_n_ct").reset_index()
    out = out.merge(zone_stats, on=["distribuidora_id", "zona"], how="left")
    few = out["n"].fillna(0) < 3
    for column in ("cts", "abonados", "kva", "horas"):
        fallback = out["distribuidora_id"].map(dist_stats[column]) if len(dist_stats) else np.nan
        out[column] = out[column].where(~few, fallback)
        out[column] = out[column].fillna(fallback)
    out["cts"] = out["cts"].fillna(out["z_n_ct"] * 0.5).clip(upper=out["z_n_ct"])
    out["horas"] = out["horas"].fillna(2.0)
    return out[["distribuidora_id", "zona", "n", "cts", "abonados", "kva", "horas"]]


def impact(zone_p: pd.DataFrame, stats: pd.DataFrame, installed_kva: float) -> dict:
    """Expected incidents, CTs, customers, kVA and minutes of TIEPI of a distribuidora from its zone probabilities."""
    needed = ["cts", "abonados", "kva", "horas"]
    if all(c in zone_p.columns for c in needed):
        table = zone_p
    else:
        table = zone_p.drop(columns=[c for c in needed if c in zone_p.columns]).merge(
            stats, on=["distribuidora_id", "zona"], how="left")
    p = table["probabilidad"].fillna(0.0)
    kva_h = float((p * table["kva"].fillna(0.0) * table["horas"].fillna(2.0)).sum())
    return {"incidencias_esperadas": float(p.sum()), "ct_esperados": float((p * table["cts"].fillna(0.0)).sum()),
            "clientes_esperados": float((p * table["abonados"].fillna(0.0)).sum()),
            "kva_esperados": float((p * table["kva"].fillna(0.0)).sum()), "kva_h_esperados": kva_h,
            "minutos_tiepi_esperados": 60.0 * kva_h / installed_kva if installed_kva > 0 else None}


# ---------------------------------------------------------------------------
# When and why
# ---------------------------------------------------------------------------

def risk_window(profile: pd.DataFrame, share: float = 0.8, calm_kmh: float = 40.0) -> dict:
    """
    The hours of the day whose forecast gust (racha_max) reaches share of
    the peak of the day, widened by one hour on each side; none when the
    peak is below calm_kmh. profile: hora (0 - 23), racha_max.
    """
    if profile is None or profile.empty or profile["racha_max"].isna().all():
        return {"inicio": None, "fin": None, "pico": None, "racha_pico": None, "texto": "sin perfil horario"}
    peak = float(profile["racha_max"].max())
    hour_peak = int(profile.loc[profile["racha_max"].idxmax(), "hora"])
    if peak < calm_kmh:
        return {"inicio": None, "fin": None, "pico": hour_peak, "racha_pico": peak,
                "texto": f"día de poco viento (racha máxima prevista de {peak:.0f} km/h): sin franja de viento"}
    hours = profile.loc[profile["racha_max"] >= share * peak, "hora"].astype(int)
    start, end = max(0, int(hours.min()) - 1), min(23, int(hours.max()) + 1)
    return {"inicio": start, "fin": end, "pico": hour_peak, "racha_pico": peak,
            "texto": f"de {start}:00 a {end + 1}:00, con el pico de racha ({peak:.0f} km/h) a las {hour_peak}:00"}


def lag_stats(starts: pd.DataFrame, cell_days: pd.DataFrame, ct: pd.DataFrame, windy_kmh: float = 60.0) -> dict:
    """
    How far from the strongest gust of the day the incidents start, from the
    history: the share of the incidents of weather within three and six hours
    of the peak of their cell, and the same on the days with a peak of
    windy_kmh or more. cell_days: celda_id, fecha, racha_max, hora_pico (the
    hour of the strongest gust).
    """
    if starts.empty or cell_days.empty or "hora_pico" not in cell_days.columns:
        return {}
    ev = starts.merge(ct.drop_duplicates(["distribuidora_id", "zona"])[["distribuidora_id", "zona", "celda_id"]],
                      on=["distribuidora_id", "zona"], how="left")
    peak = cell_days[["celda_id", "fecha", "hora_pico", "racha_max"]].rename(columns={"racha_max": "racha_pico"})
    ev = ev.merge(peak, on=["celda_id", "fecha"], how="left").dropna(subset=["hora_pico"])
    ev["lag"] = (pd.to_datetime(ev["inicio_ts"]) - pd.to_datetime(ev["hora_pico"])).dt.total_seconds() / 3600.0
    out = {}
    for name, part in (("meteorologia", ev[ev["causa"].isin(["meteorologia", "vegetacion"])]),
                       ("dias_ventosos", ev[ev["racha_pico"] >= windy_kmh]), ("todas", ev)):
        if len(part):
            out[name] = {"n": int(len(part)), "en_3h": float((part["lag"].abs() <= 3).mean()),
                         "en_6h": float((part["lag"].abs() <= 6).mean()), "mediana_h": float(part["lag"].median())}
    return out


def cause_mix(starts: pd.DataFrame, ct: pd.DataFrame, cell_days: pd.DataFrame) -> pd.DataFrame:
    """
    (bucket, familia, parte, n): the mix of cause families of the incidents
    by the strongest gust of the day in the cell of the incident (the
    reanalysis). Only the local causes, as local_starts leaves them (what a crew
    is sent to): the families of LOCAL_EXCLUDED never appear.
    """
    ev = starts.merge(ct.drop_duplicates(["distribuidora_id", "zona"])[["distribuidora_id", "zona", "celda_id"]],
                      on=["distribuidora_id", "zona"], how="left")
    ev = ev.merge(cell_days[["celda_id", "fecha", "racha_max"]], on=["celda_id", "fecha"], how="left").dropna(subset=["racha_max"])
    ev["bucket"] = pd.cut(ev["racha_max"], GUST_BUCKETS, labels=GUST_LABELS, right=False)
    ev["familia"] = ev["causa"].fillna("desconocida").map(lambda c: CAUSE_FAMILIES.get(c, c))
    counts = ev.groupby(["bucket", "familia"], observed=True).size().rename("n").reset_index()
    counts["parte"] = counts["n"] / counts.groupby("bucket", observed=True)["n"].transform("sum")
    return counts.sort_values(["bucket", "parte"], ascending=[True, False]).reset_index(drop=True)


def gust_bucket(gust: float) -> str:
    if gust is None or not np.isfinite(gust):
        return GUST_LABELS[0]
    return GUST_LABELS[int(np.searchsorted(GUST_BUCKETS[1:], gust, side="right"))]


def cascade_rule(starts: pd.DataFrame, hours: float = 3.0) -> dict:
    """After an incident of a distribuidora, the share of the cases with another one within hours."""
    out = {"horas": hours}
    if starts.empty:
        return out
    one = starts.sort_values("inicio_ts").drop_duplicates(["distribuidora_id", "periodo_id", "incidencia_id"])
    rows = []
    for _, g in one.groupby("distribuidora_id"):
        t = g["inicio_ts"].to_numpy()
        for i in range(len(t)):
            later = t[i + 1:]
            rows.append({"causa": g["causa"].iloc[i], "otra": bool(((later - t[i]) <= np.timedelta64(int(hours * 3600), "s")).any())})
    table = pd.DataFrame(rows)
    out["todas"] = float(table["otra"].mean())
    weather = table[table["causa"].isin(["meteorologia", "vegetacion"])]
    out["meteorologia"] = float(weather["otra"].mean()) if len(weather) else None
    out["n"] = int(len(table))
    return out


# ---------------------------------------------------------------------------
# Crews
# ---------------------------------------------------------------------------

def haversine_km(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    la1, lo1 = np.radians(a[:, 0])[:, None], np.radians(a[:, 1])[:, None]
    la2, lo2 = np.radians(b[:, 0])[None, :], np.radians(b[:, 1])[None, :]
    h = np.sin((la2 - la1) / 2) ** 2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(h))


def osrm_table(points: list, api: str = "https://router.project-osrm.org", timeout: float = 60.0) -> np.ndarray:
    """
    Minutes by road between points [(lat, lon)], from the table service of
    OSRM (driving profile over OpenStreetMap); NaN where there is no route.
    One request per distribuidora and day, cached by the job.
    """
    coords = ";".join(f"{lon:.5f},{lat:.5f}" for lat, lon in points)
    url = f"{api.rstrip('/')}/table/v1/driving/{coords}?annotations=duration"
    request = urllib.request.Request(url, headers={"User-Agent": "GridPredic-TFM/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as answer:
        payload = json.loads(answer.read())
    if payload.get("code") != "Ok" or "durations" not in payload:
        raise RuntimeError(f"OSRM answered {str(payload)[:120]}")
    seconds = np.array([[np.nan if v is None else float(v) for v in row] for row in payload["durations"]], dtype=float)
    if seconds.shape != (len(points), len(points)):
        raise RuntimeError(f"OSRM gave a table of {seconds.shape} for {len(points)} points")
    return seconds / 60.0


def crew_plan(municipalities: pd.DataFrame, crews: int, speed_kmh: float = 60.0, road_factor: float = 1.3,
              fixed: list = None, road_minutes: pd.DataFrame = None) -> dict:
    """
    The crews bases that minimise the expected arrival time to the risk of
    tomorrow: a greedy p-median over the municipalities of the CTs (lat,
    lon), weighting each one by its expected customers out of service
    (clientes_esperados, or its probability). The minutes between
    municipalities are the ones by road of road_minutes (a frame indexed
    by municipio in rows and columns, osrm_table) when it covers them, and
    straight lines times road_factor at speed_kmh otherwise. fixed:
    municipalities of the bases the distribuidora already has, to compare.
    """
    m = municipalities.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    if m.empty or crews <= 0:
        return {}
    pts = m[["lat", "lon"]].to_numpy(dtype=float)
    weights = m["clientes_esperados"].to_numpy(dtype=float) if "clientes_esperados" in m else m["probabilidad"].to_numpy(dtype=float)
    weights = np.where(np.isfinite(weights), weights, 0.0)
    if weights.sum() <= 0:
        weights = m["n_ct"].to_numpy(dtype=float)
    minutes = haversine_km(pts, pts) * road_factor / speed_kmh * 60.0
    source = f"línea recta por {road_factor:g} a {speed_kmh:g} km/h".replace(".", ",")
    if road_minutes is not None and len(road_minutes):
        by_road = road_minutes.reindex(index=m["municipio"], columns=m["municipio"]).to_numpy(dtype=float)
        known = np.isfinite(by_road)
        if known.mean() >= 0.9:
            minutes = np.where(known, by_road, minutes)
            source = "carretera (OSRM sobre OpenStreetMap)"
    chosen = []
    for _ in range(min(crews, len(m))):
        best = None
        for j in range(len(m)):
            if j in chosen:
                continue
            cost = float((minutes[:, chosen + [j]].min(axis=1) * weights).sum() / weights.sum())
            if best is None or cost < best[0]:
                best = (cost, j)
        chosen.append(best[1])
    arrival = minutes[:, chosen]
    nearest = arrival.argmin(axis=1)
    bases = []
    for k, j in enumerate(chosen):
        mine = nearest == k
        order = np.argsort(-weights * mine)
        covered = [str(m.loc[i, "municipio"]) for i in order if mine[i]]
        bases.append({"municipio": str(m.loc[j, "municipio"]), "lat": float(pts[j, 0]), "lon": float(pts[j, 1]),
                      "cubre_municipios": int(mine.sum()), "cubre_ct": int(m.loc[mine, "n_ct"].sum()),
                      "municipios": covered, "parte_riesgo": float(weights[mine].sum() / weights.sum()),
                      "llegada_media_min": float((arrival[mine, k] * weights[mine]).sum() / max(weights[mine].sum(), 1e-9)),
                      "llegada_max_min": float(arrival[mine, k].max())})
    best_arrival = arrival.min(axis=1)
    plan = {"n": len(chosen), "velocidad_kmh": speed_kmh, "factor_carretera": road_factor, "tiempos": source, "bases": bases,
            "llegada_media_min": float((best_arrival * weights).sum() / weights.sum()),
            "riesgo_30min": float(weights[best_arrival <= 30].sum() / weights.sum()),
            "riesgo_45min": float(weights[best_arrival <= 45].sum() / weights.sum()),
            "llegada_max_min": float(best_arrival.max())}
    if fixed:
        idx = [i for i, name in enumerate(m["municipio"]) if name in set(fixed)]
        if idx:
            arr = minutes[:, idx].min(axis=1)
            plan["fijas"] = {"municipios": [str(m.loc[i, "municipio"]) for i in idx],
                             "llegada_media_min": float((arr * weights).sum() / weights.sum()),
                             "riesgo_30min": float(weights[arr <= 30].sum() / weights.sum())}
    return plan


def crew_curve(municipalities: pd.DataFrame, up_to: int = 4, **kwargs) -> list:
    """What each extra crew buys: [{n, llegada_media_min, riesgo_30min, bases}] for 1 to up_to crews."""
    out = []
    for n in range(1, max(1, up_to) + 1):
        plan = crew_plan(municipalities, n, **kwargs)
        if not plan or plan["n"] < n:
            break
        out.append({"n": n, "llegada_media_min": plan["llegada_media_min"], "riesgo_30min": plan["riesgo_30min"],
                    "llegada_max_min": plan["llegada_max_min"], "bases": [b["municipio"] for b in plan["bases"]]})
    return out


def positioning(window: dict, plan: dict, lead_min: int = 60) -> str:
    """When the crews should be at their bases: an hour before the window of the day, if there is one."""
    if not plan or not plan.get("bases"):
        return ""
    if window and window.get("inicio") is not None:
        hour = max(0, int(window["inicio"]) - max(0, int(round(lead_min / 60.0))))
        return (f"brigadas en sus bases a las {hour}:00 (una hora antes de la franja de más riesgo, "
                f"de {int(window['inicio'])}:00 a {int(window['fin']) + 1}:00)")
    # Not "guardia normal": a warning of AEMET can reinforce the guard of a day without wind (guardia of the payload).
    return "sin franja de viento; si hay que elegir, las bases propuestas"


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def clean(value):
    """JSON friendly: numpy numbers to Python, NaN to None, timestamps to text."""
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray, pd.Series)):
        return [clean(v) for v in list(value)]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat(timespec="minutes") if isinstance(value, datetime) and value.tzinfo else str(value)[:16]
    return value


def write_json_atomic(path: str, payload, **dump_args) -> None:
    """
    json.dump to a temporary file in the same folder and os.replace over the
    target, so the dashboard (or a copy) never reads a half written file. If
    the replace is refused (a file held open on Windows), the file is written
    in place as before and the refusal is logged.
    """
    folder = os.path.dirname(os.path.abspath(path))
    temporary = os.path.join(folder, f".{os.path.basename(path)}.{os.getpid()}.tmp")
    try:
        with open(temporary, "w", encoding="utf-8") as file:
            json.dump(payload, file, **dump_args)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    except OSError as error:
        logging.getLogger(__name__).warning("%s not replaced atomically (%s): written in place", path, error)
        try:
            os.remove(temporary)
        except OSError:
            pass
        with open(path, "w", encoding="utf-8") as file:
            json.dump(payload, file, **dump_args)


def write_json(payload: dict, path: str):
    write_json_atomic(path, clean(payload), ensure_ascii=False, indent=1)


def now_text() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


def shift_sheets(payload: dict) -> dict:
    """
    The tables of the shift sheet (parte de guardia) from the payload of the
    dashboard: one frame per sheet, in the order they are written.
    """
    dists = payload.get("distribuidoras", [])
    summary, lines, munis, cts, crews, curve = [], [], [], [], [], []
    for d in dists:
        imp, fr, plan = d.get("impacto") or {}, d.get("franja") or {}, d.get("brigadas") or {}
        guard = d.get("guardia") or {}
        summary.append({
            "distribuidora": d.get("nombre"), "nivel": d.get("nivel"), "probabilidad": d.get("probabilidad"),
            "guardia": ("reforzar: " + str(guard.get("motivo", ""))) if guard.get("reforzar") else "normal",
            "veces la tasa base": d.get("veces_tasa_base"), "averías esperadas": d.get("esperadas"),
            "CT esperados sin luz": imp.get("ct_esperados"), "clientes esperados sin luz": imp.get("clientes_esperados"),
            "kVA esperados": imp.get("kva_esperados"), "franja de más riesgo": fr.get("texto"),
            "hora pico": fr.get("pico"), "racha pico (km/h)": fr.get("racha_pico"),
            "posicionamiento": plan.get("posicionamiento"),
            "bases": "; ".join(b.get("municipio", "") for b in plan.get("bases", [])),
            "llegada media (min)": plan.get("llegada_media_min"), "motivos": d.get("motivos"),
            "causas (días como mañana)": ", ".join(f"{c['familia']} {round(100 * c['parte'])} %" for c in d.get("causas", [])),
            "escenarios de ICON-EU EPS": (d.get("escenarios") or {}).get("texto"),
            "AEMET por municipios": (d.get("aemet") or {}).get("texto"),
        })
        for z in sorted(d.get("zonas", []), key=lambda z: -(z.get("probabilidad") or 0)):
            lines.append({"distribuidora": d.get("nombre"), "línea": z.get("nombre"), "id línea": z.get("zona"),
                          "nivel": z.get("nivel"), "probabilidad": z.get("probabilidad"), "parte del riesgo": z.get("parte"),
                          "CT": z.get("n_ct"), "clientes": z.get("abonados"), "kVA": z.get("kva"),
                          "clientes esperados sin luz": z.get("clientes_esperados"), "racha prevista (km/h)": z.get("racha_max"),
                          "lluvia prevista (mm)": z.get("lluvia"), "incidencias último año": z.get("hist_365d"),
                          "días desde la última": (None if (z.get("hist_dias_desde") or 0) >= 3650 else z.get("hist_dias_desde")),
                          "municipios": ", ".join(z.get("municipios", []))})
        for m in d.get("municipios", []):
            munis.append({"distribuidora": d.get("nombre"), "municipio": m.get("nombre"), "código INE": m.get("codigo_ine"),
                          "nivel": m.get("nivel"), "probabilidad": m.get("probabilidad"), "CT": m.get("n_ct"),
                          "clientes": m.get("abonados"), "clientes esperados sin luz": m.get("clientes_esperados"),
                          "racha prevista (km/h)": m.get("racha_max"), "líneas": ", ".join(m.get("zonas", []))})
        for c in d.get("cts", []):
            cts.append({"distribuidora": d.get("nombre"), "CT": c.get("ct_id"), "municipio": c.get("municipio"),
                        "línea": c.get("zona"), "clientes": c.get("abonados"), "kVA": c.get("kva"),
                        "probabilidad": c.get("probabilidad")})
        for b in plan.get("bases", []):
            crews.append({"distribuidora": d.get("nombre"), "base": b.get("municipio"), "cubre municipios": b.get("cubre_municipios"),
                          "cubre CT": b.get("cubre_ct"), "parte del riesgo": b.get("parte_riesgo"),
                          "llegada media (min)": b.get("llegada_media_min"), "llegada máxima (min)": b.get("llegada_max_min"),
                          "municipios que cubre": ", ".join(b.get("municipios", [])), "tiempos": plan.get("tiempos")})
        for c in plan.get("curva", []):
            curve.append({"distribuidora": d.get("nombre"), "brigadas": c.get("n"), "llegada media (min)": c.get("llegada_media_min"),
                          "parte del riesgo a menos de 30 min": c.get("riesgo_30min"), "llegada máxima (min)": c.get("llegada_max_min"),
                          "bases": ", ".join(c.get("bases", []))})
    rules = payload.get("reglas") or {}
    notes = [{"nota": n} for n in (payload.get("notas") or [])]
    if rules.get("texto"):
        notes.insert(0, {"nota": rules["texto"]})
    notes.insert(0, {"nota": f"Riesgo de mañana {payload.get('fecha_texto') or payload.get('fecha', '')}, generado el "
                             f"{payload.get('generado', '')} (ejecución {payload.get('run_id', '')})"})
    return {"Resumen": pd.DataFrame(summary), "Lineas": pd.DataFrame(lines), "Municipios": pd.DataFrame(munis),
            "CT": pd.DataFrame(cts), "Brigadas": pd.DataFrame(crews), "Brigadas por numero": pd.DataFrame(curve),
            "Notas": pd.DataFrame(notes)}


def write_shift_sheet(payload: dict, path: str) -> bool:
    """parte_guardia.xlsx: the tables of shift_sheets with bold headers, frozen first row and percentages."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
        from openpyxl.utils import get_column_letter
    except ImportError:
        return False
    book = Workbook()
    book.remove(book.active)
    percent = {"probabilidad", "parte del riesgo", "parte del riesgo a menos de 30 min"}
    for name, frame in shift_sheets(payload).items():
        sheet = book.create_sheet(name[:31])
        if frame.empty:
            sheet.append(["sin datos"])
            continue
        columns = list(frame.columns)
        sheet.append(columns)
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        for row in frame.itertuples(index=False):
            sheet.append([clean(v) for v in row])
        for j, column in enumerate(columns, 1):
            width = max([len(str(column))] + [len(str(v)) for v in frame[column].head(200).tolist()] + [8])
            sheet.column_dimensions[get_column_letter(j)].width = min(60, width + 2)
            if column in percent:
                for i in range(2, len(frame) + 2):
                    sheet.cell(row=i, column=j).number_format = "0.0%"
        sheet.freeze_panes = "A2"
    temporary = os.path.join(os.path.dirname(os.path.abspath(path)), f".{os.path.basename(path)}.{os.getpid()}.tmp")
    book.save(temporary)
    try:
        os.replace(temporary, path)
    except OSError as error:
        logging.getLogger(__name__).warning("%s not replaced atomically (%s): written in place", path, error)
        os.remove(temporary)
        book.save(path)
    return True
