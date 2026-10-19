"""Qué son las 48 cabeceras de 5.8.1 y cuánto pesan las que no son una salida de línea.

Repite sobre la exportación de Gold del 13.10.2026 lo que hacen job_analisis_red._feeders (cabecera =
nombre del elemento que más veces cortó al trafo, sin_cabecera si no hay ninguno) y section_where
(universo = pares distribuidora, cabecera de dim_ct; kVA*h de la variante principal de 2015 - 2025 sin el
28.04.2025; top 10 % = ceil(0,1 * entidades)).

Uso: python cabeceras_48.py <carpeta zonas_20261013_1041>
"""
import math
import sys
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
ev = pd.read_parquet(BASE / "interrupciones.parquet")
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}

# -- cabecera como en _feeders
pdf = aa[["trafo_elemento_id", "elemento_corte_id", "elemento_corte_nombre", "n_cortes"]].copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
top = pdf.drop_duplicates("trafo_elemento_id").rename(columns={"nombre": "cabecera_job"})
d = dim.merge(top[["trafo_elemento_id", "cabecera_job"]], on="trafo_elemento_id", how="left")
d["cabecera_job"] = d["cabecera_job"].fillna("sin_cabecera")
d["red"] = d.distribuidora_id.map(NOMBRE)
d["ct_id"] = d["ct_id"].astype(str)
kva = pd.to_numeric(d["potencia_tiepi_kva"], errors="coerce")
d["kva"] = kva.where(kva > 0, pd.to_numeric(d["potencia_kva"], errors="coerce")).fillna(0.0)

u = d[["distribuidora_id", "cabecera_job"]].drop_duplicates()
sin = u[u.cabecera_job == "sin_cabecera"]
reales = u[u.cabecera_job != "sin_cabecera"]
dobles = reales.groupby("cabecera_job").distribuidora_id.nunique()
dobles = dobles[dobles > 1]
print(f"entidades (distribuidora, cabecera): {len(u)} = {reales.cabecera_job.nunique()} elementos distintos "
      f"+ {len(reales) - reales.cabecera_job.nunique()} repetidos en dos redes + {len(sin)} sin_cabecera")
sc = d[d.cabecera_job == "sin_cabecera"]
print("CT en sin_cabecera por red: " + ", ".join(
    f"{r} {n} (en estudio {int(e)}, con trafo {int(t)})" for r, (n, e, t) in
    sc.groupby("red").agg(n=("ct_id", "size"), e=("en_estudio", "sum"),
                          t=("trafo_elemento_id", lambda s: s.notna().sum())).iterrows()))
for c in dobles.index:
    g = d[d.cabecera_job == c].groupby("red").ct_id.size().to_dict()
    print(f"  cabecera repetida en dos redes: CT por red {g}")

# -- kVA*h de la variante principal 2015 - 2025 sin el 28.04.2025
ev = ev.copy()
ev["ct_id"] = ev["ct_id"].astype(str)
ev["anio"] = ev.inicio_ts.dt.year
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
excl = first.dt.strftime("%Y-%m-%d") == "2025-04-28"
print("clasificaciones en la exportacion:", ev.clasificacion_id.value_counts().to_dict(),
      "factor FA_CLIEN:", int((ev.factor_id == "FA_CLIEN").sum()), "duracion <= 180 s:", int((ev.duracion_s <= 180).sum()))
ev = ev[(ev.anio >= 2015) & (ev.anio <= 2025) & ~excl]
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(d[["distribuidora_id", "ct_id", "kva", "cabecera_job"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva"] = ev["kva"].fillna(0.0)
ev["kva_h"] = ev["kva"] * ev["dur_h"]
print(f"interrupciones 2015 - 2025 sin el apagon: {len(ev):,}; sin CT en dim_ct: {int(ev.cabecera_job.isna().sum())}")

def concentra(universo, ev, etiqueta):
    g = ev.groupby(["distribuidora_id", "cabecera_job"]).kva_h.sum().reset_index()
    v = universo.merge(g, on=["distribuidora_id", "cabecera_job"], how="left").fillna({"kva_h": 0.0})
    v = v.sort_values("kva_h", ascending=False).reset_index(drop=True)
    k = max(1, math.ceil(0.1 * len(v)))
    share = v.head(k).kva_h.sum() / v.kva_h.sum()
    con = int((v.kva_h > 0).sum())
    puestos = v.index[v.cabecera_job == "sin_cabecera"].tolist()
    part_sin = v.loc[v.cabecera_job == "sin_cabecera", "kva_h"].sum() / v.kva_h.sum()
    print(f"{etiqueta}: entidades {len(v)} (con kVA*h {con}), top {k} -> {share:.1%}; "
          f"puestos de sin_cabecera {[p + 1 for p in puestos]}, su parte {part_sin:.1%}")
    return v

v = concentra(u, ev, "como el job")
concentra(reales, ev[ev.cabecera_job != "sin_cabecera"], "solo cabeceras reales")
# parte de los kVA*h por CT en estudio / fuera
ev2 = ev.drop(columns=["en_estudio"]).merge(d[["distribuidora_id", "ct_id", "en_estudio"]],
                                            on=["distribuidora_id", "ct_id"], how="left")
print("kVA*h de CT en sin_cabecera:", f"{ev.loc[ev.cabecera_job == 'sin_cabecera', 'kva_h'].sum() / ev.kva_h.sum():.1%}",
      "| de CT fuera del estudio:", f"{ev2.loc[~ev2.en_estudio.fillna(False).astype(bool), 'kva_h'].sum() / ev2.kva_h.sum():.1%}")
sc_ev = ev2[ev2.cabecera_job == "sin_cabecera"]
print("CT sin cabecera con alguna interrupcion 2015 - 2025:", sc_ev.ct_id.nunique(), "de", len(sc))
# cts_cabecera que ve el modelo anual para los CT sin cabecera
per = d.groupby(["distribuidora_id", "cabecera_job"]).agg(cts=("ct_id", "nunique"), kva=("kva", "sum")).reset_index()
x = per[per.cabecera_job == "sin_cabecera"]
print("cts_cabecera y kva_cabecera de los CT sin cabecera (modelo anual):",
      [(NOMBRE[r.distribuidora_id], int(r.cts), round(r.kva)) for r in x.itertuples()])
print("cts_cabecera de las cabeceras reales: mediana", per[per.cabecera_job != "sin_cabecera"].cts.median(),
      "max", per.cts.max())
