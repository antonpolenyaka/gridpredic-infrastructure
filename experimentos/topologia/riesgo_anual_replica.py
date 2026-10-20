"""Réplica reducida del riesgo anual (job_ml_riesgo_ct.py, bloques historia e historia+estructura) sobre la
exportación del 13.10.2026, para medir dos cosas:

1. Cuánto pesa n_posiciones_aguas_arriba de dim_ct, que cuenta las posiciones de toda la ventana del SCADA
   (2021 - 2026) y que el docstring de job_gold_dim_ct.py declara "never a feature": la misma variable contada
   solo con las parejas conocidas antes del 1 de enero de cada año (primer_conocido_ts), y sin ella.
2. Cuánto gana el modelo a ordenar solo por tamaño (kVA instalados del CT o de la cabecera).

Mismas reglas que el job: variante principal 2015 - 2025 sin el 28.04.2025, kVA de TIEPI, rejilla de CT vigentes
desde 2018, historia de 1, 2 y 3 años del CT, la cabecera, el grupo y el municipio (y kVA*h por kVA de los tres
niveles), anios_desde_ultima, XGBoost Tweedie con los parámetros del job y la semilla 20261005, origen móvil
2021 - 2025 y captura del 10 % por CT y por cabecera (suma de las puntuaciones de sus CT). n_ct_grupo no está en la
exportación y se cuenta aquí como los CT de dim_ct del mismo grupo.

Uso: python riesgo_anual_replica.py <carpeta zonas_20261013_1041>
"""
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
ev = pd.read_parquet(BASE / "interrupciones.parquet")
SEED = 20261005
PARAMS = {"n_estimators": 300, "max_depth": 4, "learning_rate": 0.05, "subsample": 0.8, "colsample_bytree": 0.8,
          "min_child_weight": 5, "objective": "reg:tweedie", "tweedie_variance_power": 1.3}
KEYS = ["distribuidora_id", "ct_id", "anio"]
LEVELS = {"ct": ["distribuidora_id", "ct_id"], "cabecera": ["distribuidora_id", "cabecera"],
          "grupo": ["distribuidora_id", "grupo_red_id"], "municipio": ["distribuidora_id", "municipio_id"]}

# ---- dim como Data._dim + _feeders
pdf = aa[["trafo_elemento_id", "elemento_corte_id", "elemento_corte_nombre", "n_cortes"]].copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
top = pdf.drop_duplicates("trafo_elemento_id").rename(columns={"nombre": "cabecera"})
d = dim.drop(columns=["cabecera"]).merge(top[["trafo_elemento_id", "cabecera"]], on="trafo_elemento_id", how="left")
d["cabecera"] = d["cabecera"].fillna("sin_cabecera")
d["ct_id"] = d["ct_id"].astype(str)
kva = pd.to_numeric(d["potencia_tiepi_kva"], errors="coerce")
d["kva"] = kva.where(kva > 0, pd.to_numeric(d["potencia_kva"], errors="coerce")).fillna(0.0)
d["zona"] = pd.to_numeric(d["tipo_zona_codigo"], errors="coerce").fillna(3).astype(int)
d["num_abonados"] = pd.to_numeric(d["num_abonados"], errors="coerce").fillna(0)
d["grupo_red_id"] = d["grupo_red_id"].map(lambda x: "sin_grupo" if pd.isna(x) else str(int(x)))
d["municipio_id"] = d["municipio_id"].astype(str)
d["vd"] = pd.to_datetime(d["vigente_desde"], errors="coerce")
d["vh"] = pd.to_datetime(d["vigente_hasta_excl"], errors="coerce")
d["tiene_telemetria"] = d["tiene_telemetria"].astype(float)
g_n = d[d.grupo_red_id != "sin_grupo"].groupby(["distribuidora_id", "grupo_red_id"]).ct_id.nunique().rename("n_ct_grupo")
d = d.merge(g_n.reset_index(), on=["distribuidora_id", "grupo_red_id"], how="left")
per_feeder = d.groupby(["distribuidora_id", "cabecera"]).agg(cts_cabecera=("ct_id", "nunique"),
                                                             kva_cabecera=("kva", "sum")).reset_index()
d = d.merge(per_feeder, on=["distribuidora_id", "cabecera"], how="left")

# ---- eventos por CT y año
ev["ct_id"] = ev["ct_id"].astype(str)
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev["anio"] = ev.inicio_ts.dt.year
ev = ev[(ev.anio >= 2015) & (ev.anio <= 2025)]
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(d[["distribuidora_id", "ct_id", "kva"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev["kva"].fillna(0.0) * ev["dur_h"]
per_ct = ev.groupby(["distribuidora_id", "ct_id", "anio"]).agg(kva_h=("kva_h", "sum"), interrupciones=("ct_id", "size"),
                                                               horas=("dur_h", "sum")).reset_index()

# ---- rejilla 2018 - 2025
rows = []
for y in range(2018, 2026):
    lo, hi = pd.Timestamp(year=y, month=1, day=1), pd.Timestamp(year=y + 1, month=1, day=1)
    rows.append(d[(d.vd.isna() | (d.vd < hi)) & (d.vh.isna() | (d.vh > lo))].assign(anio=y))
grid = pd.concat(rows, ignore_index=True)
lab = per_ct.rename(columns={"kva_h": "y_kva_h", "interrupciones": "y_int", "horas": "y_horas"})
grid = grid.merge(lab, on=KEYS, how="left").fillna({"y_kva_h": 0.0, "y_int": 0.0, "y_horas": 0.0})


def history(level, spans=(1, 2, 3)):
    keys = LEVELS[level]
    e = per_ct if level == "ct" else per_ct.merge(d[["distribuidora_id", "ct_id"] + [k for k in keys if k not in
                                                   ("distribuidora_id", "ct_id")]], on=["distribuidora_id", "ct_id"], how="left")
    by = e.groupby(keys + ["anio"])[["kva_h", "interrupciones", "horas"]].sum().reset_index()
    out = grid[keys + ["anio"]].drop_duplicates()
    for span in spans:
        parts = [by.assign(anio=by.anio + k) for k in range(1, span + 1)]
        w = pd.concat(parts).groupby(keys + ["anio"])[["kva_h", "interrupciones", "horas"]].sum()
        w.columns = [f"{c}_{level}_{span}a" for c in w.columns]
        out = out.merge(w.reset_index(), on=keys + ["anio"], how="left")
    return out.fillna(0.0)


for level in LEVELS:
    grid = grid.merge(history(level), on=LEVELS[level] + ["anio"], how="left")
for level in ("cabecera", "grupo", "municipio"):
    size = grid.groupby(LEVELS[level] + ["anio"])["kva"].transform("sum").replace(0, np.nan)
    for span in (1, 2, 3):
        grid[f"tiepi_{level}_{span}a"] = grid[f"kva_h_{level}_{span}a"] / size
last = per_ct[per_ct.kva_h > 0][["distribuidora_id", "ct_id", "anio"]].rename(columns={"anio": "_u"})
since = grid[KEYS].merge(last, on=["distribuidora_id", "ct_id"], how="left")
since = since[since._u < since.anio].groupby(KEYS)._u.max().reset_index()
grid = grid.merge(since, on=KEYS, how="left")
grid["anios_desde_ultima"] = (grid.anio - grid._u).clip(upper=10).fillna(10.0)
grid = grid.drop(columns=["_u"])

# ---- n_posiciones: toda la ventana (como hoy) y solo lo conocido antes de cada año
grid["n_pos_toda"] = grid["n_posiciones_aguas_arriba"].astype(float)
asof = []
tids = d[["distribuidora_id", "ct_id", "trafo_elemento_id"]]
for y in range(2018, 2026):
    known = aa[aa.primer_conocido_ts < pd.Timestamp(year=y, month=1, day=1)]
    c = known.groupby("trafo_elemento_id").posicion_id.nunique()
    asof.append(tids.assign(anio=y, n_pos_antes=tids.trafo_elemento_id.map(c).fillna(0.0).where(
        tids.trafo_elemento_id.notna())))
grid = grid.merge(pd.concat(asof)[KEYS + ["n_pos_antes"]], on=KEYS, how="left")

HIST = [c for c in grid.columns if c.startswith(("kva_h_", "interrupciones_", "horas_", "tiepi_"))] + ["anios_desde_ultima"]
BASE_STRUCT = ["kva", "num_abonados", "zona", "tiene_telemetria", "n_ct_grupo", "cts_cabecera", "kva_cabecera"]
VARIANTES = {
    "historia": HIST,
    "historia+estructura (n_posiciones de toda la ventana, como hoy)": HIST + BASE_STRUCT + ["n_pos_toda"],
    "historia+estructura (n_posiciones conocidas antes del año)": HIST + BASE_STRUCT + ["n_pos_antes"],
    "historia+estructura (sin n_posiciones)": HIST + BASE_STRUCT,
}


def capture(y, s, frac=0.10):
    k = max(1, int(math.ceil(frac * len(y))))
    o = np.lexsort((np.arange(len(s)), -np.asarray(s, dtype=float)))
    return float(np.sum(y[o[:k]])) / float(np.sum(y))


res = []
for y in range(2021, 2026):
    train = grid[grid.anio < y]
    test = grid[grid.anio == y].copy()
    test["s_naif"] = test["kva_h_ct_2a"]
    test["s_tamaño"] = test["kva"]
    names = ["naif", "tamaño"]
    for name, feats in VARIANTES.items():
        m = XGBRegressor(**PARAMS, random_state=SEED, n_jobs=4, tree_method="hist")
        m.fit(train[feats].astype(float).to_numpy(), train["y_kva_h"].to_numpy())
        test["s_" + name] = m.predict(test[feats].astype(float).to_numpy())
        names.append(name)
    test["s_oráculo"] = test["y_kva_h"]
    names.append("oráculo")
    fe = test.groupby(["distribuidora_id", "cabecera"])[["y_kva_h"] + ["s_" + n for n in names]].sum().reset_index()
    fh = test.groupby(["distribuidora_id", "cabecera"]).agg(h=("kva_h_cabecera_2a", "first"), k=("kva", "sum")).reset_index()
    fe = fe.merge(fh, on=["distribuidora_id", "cabecera"])
    fe["s_naif"] = fe["h"]
    fe["s_tamaño"] = fe["k"]
    li = test.groupby(["distribuidora_id", "grupo_red_id"])[["y_kva_h"] + ["s_" + n for n in names]].sum().reset_index()
    lh = test.groupby(["distribuidora_id", "grupo_red_id"]).agg(h=("kva_h_grupo_2a", "first"), k=("kva", "sum")).reset_index()
    li = li.merge(lh, on=["distribuidora_id", "grupo_red_id"])
    li["s_naif"] = li["h"]
    li["s_tamaño"] = li["k"]
    for n in names:
        res.append({"anio": y, "modelo": n, "ct": capture(test.y_kva_h.to_numpy(), test["s_" + n].to_numpy()),
                    "cabecera": capture(fe.y_kva_h.to_numpy(), fe["s_" + n].to_numpy()), "n_cab": len(fe),
                    "linea": capture(li.y_kva_h.to_numpy(), li["s_" + n].to_numpy()), "n_lin": len(li)})
r = pd.DataFrame(res)
tab = r.groupby("modelo", sort=False)[["ct", "cabecera", "linea"]].mean()
sin21 = r[r.anio > 2021].groupby("modelo", sort=False)[["ct", "cabecera", "linea"]].mean()
tab = tab.join(sin21, rsuffix=" (2022 - 2025)")
pd.set_option("display.width", 220)
print(tab.map(lambda x: f"{x:.1%}").to_string())
print("\npor año, cabecera:")
print(r.pivot(index="modelo", columns="anio", values="cabecera").loc[tab.index].map(lambda x: f"{x:.0%}").to_string())
print("\npor año, CT:")
print(r.pivot(index="modelo", columns="anio", values="ct").loc[tab.index].map(lambda x: f"{x:.0%}").to_string())
print("\npor año, línea:")
print(r.pivot(index="modelo", columns="anio", values="linea").loc[tab.index].map(lambda x: f"{x:.0%}").to_string())
print("\nentidades de cabecera por año:", r.groupby("anio").n_cab.first().to_dict(), "líneas:", r.groupby("anio").n_lin.first().to_dict())


# ---- robustez: cinco semillas para las variantes de estructura (media y rango de la media 2021 - 2025)
print("\ncinco semillas (media 2021 - 2025; entre corchetes, mínimo y máximo entre semillas):")
rob = []
for seed in (20261005, 1, 2, 3, 4):
    for y in range(2021, 2026):
        train = grid[grid.anio < y]
        test = grid[grid.anio == y].copy()
        for name in list(VARIANTES)[1:]:
            feats = VARIANTES[name]
            m = XGBRegressor(**PARAMS, random_state=seed, n_jobs=4, tree_method="hist")
            m.fit(train[feats].astype(float).to_numpy(), train["y_kva_h"].to_numpy())
            test["s"] = m.predict(test[feats].astype(float).to_numpy())
            fe = test.groupby(["distribuidora_id", "cabecera"])[["y_kva_h", "s"]].sum()
            rob.append({"semilla": seed, "anio": y, "modelo": name,
                        "ct": capture(test.y_kva_h.to_numpy(), test.s.to_numpy()),
                        "cabecera": capture(fe.y_kva_h.to_numpy(), fe.s.to_numpy())})
rb = pd.DataFrame(rob).groupby(["modelo", "semilla"])[["ct", "cabecera"]].mean().reset_index()
for name, g in rb.groupby("modelo", sort=False):
    print(f"  {name}: CT {g.ct.mean():.1%} [{g.ct.min():.1%} - {g.ct.max():.1%}], "
          f"cabecera {g.cabecera.mean():.1%} [{g.cabecera.min():.1%} - {g.cabecera.max():.1%}]")
