"""Concentracion y persistencia por cabecera aprendida frente a unidades que no salen de los cortes.

La cabecera es el elemento que más veces cortó a cada trafo en 2021 - 2026 (grupo definido por los cortes
mismos); la línea (grupo_red_id) sale de la jerarquía funcional de TedisNet y el municipio de Calser. Se repite
la sección 5 de job_analisis_red (pareto 2015 - 2025 y persistencia 2023 - 2025 con los dos años anteriores)
sobre la exportación del 13.10.2026, con las mismas reglas que cabeceras_48.py.

Uso: python concentracion_unidades.py <carpeta zonas_20261013_1041>
"""
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
ev = pd.read_parquet(BASE / "interrupciones.parquet")

pdf = aa[["trafo_elemento_id", "elemento_corte_id", "elemento_corte_nombre", "n_cortes"]].copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
top = pdf.drop_duplicates("trafo_elemento_id").rename(columns={"nombre": "cabecera"})
d = dim.drop(columns=["cabecera"]).merge(top[["trafo_elemento_id", "cabecera"]], on="trafo_elemento_id", how="left")
d["cabecera"] = d["cabecera"].fillna("sin_cabecera")
d["grupo_red_id"] = d["grupo_red_id"].map(lambda x: "sin_grupo" if pd.isna(x) else str(int(x)))
d["ct_id"] = d["ct_id"].astype(str)
kva = pd.to_numeric(d["potencia_tiepi_kva"], errors="coerce")
d["kva"] = kva.where(kva > 0, pd.to_numeric(d["potencia_kva"], errors="coerce")).fillna(0.0)

ev["ct_id"] = ev["ct_id"].astype(str)
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev["anio"] = ev.inicio_ts.dt.year
ev = ev[(ev.anio >= 2015) & (ev.anio <= 2025)]
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(d[["distribuidora_id", "ct_id", "kva", "cabecera", "grupo_red_id", "municipio_id"]],
              on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev["kva"].fillna(0.0) * ev["dur_h"]

ENT = {"ct": ["distribuidora_id", "ct_id"], "linea (grupo_red)": ["distribuidora_id", "grupo_red_id"],
       "cabecera": ["distribuidora_id", "cabecera"], "municipio": ["distribuidora_id", "municipio_id"]}


def values(keys, years, universe):
    g = ev[ev.anio.isin(years)].groupby(keys).kva_h.sum().reset_index()
    return universe.merge(g, on=keys, how="left").fillna({"kva_h": 0.0})


def top_share(v, f):
    o = v.sort_values(ascending=False)
    k = max(1, math.ceil(f * len(o)))
    return o.head(k).sum() / o.sum(), k


rows = []
for name, keys in ENT.items():
    for solo_reales in (False, True):
        uni = d[keys].drop_duplicates()
        if solo_reales:
            if name == "cabecera":
                uni = uni[uni.cabecera != "sin_cabecera"]
            elif name.startswith("linea"):
                uni = uni[uni.grupo_red_id != "sin_grupo"]
            else:
                continue
        v = values(keys, range(2015, 2026), uni)
        s10, k10 = top_share(v.kva_h, 0.10)
        pers = []
        for y in (2023, 2024, 2025):
            past = values(keys, [y - 2, y - 1], uni).rename(columns={"kva_h": "prev"})
            now = values(keys, [y], uni)
            b = past.merge(now, on=keys)
            r = b.sort_values("prev", ascending=False)
            k = max(1, math.ceil(0.10 * len(r)))
            pers.append(r.head(k).kva_h.sum() / b.kva_h.sum())
        rows.append({"unidad": name + (" (sin grupos de relleno)" if solo_reales else ""), "entidades": len(uni),
                     "top10": k10, "concentracion_10": f"{s10:.1%}", "persistencia_10": f"{np.mean(pers):.1%}"})
print(pd.DataFrame(rows).to_string(index=False))
# relación línea - cabecera: cuántas cabeceras por línea y líneas por cabecera (CT del estudio)
e = d[d.en_estudio & (d.cabecera != "sin_cabecera") & (d.grupo_red_id != "sin_grupo")]
lpc = e.groupby(["distribuidora_id", "cabecera"]).grupo_red_id.nunique()
cpl = e.groupby(["distribuidora_id", "grupo_red_id"]).cabecera.nunique()
dom = e.groupby(["distribuidora_id", "grupo_red_id"]).cabecera.agg(lambda s: s.value_counts(normalize=True).iloc[0])
print(f"CT del estudio con linea y cabecera: {len(e)}; lineas por cabecera: mediana {lpc.median():.0f}, max {lpc.max()}; "
      f"cabeceras por linea: mediana {cpl.median():.0f}, max {cpl.max()}; parte de los CT de la linea en su cabecera "
      f"mayoritaria: mediana {dom.median():.0%}, lineas con una sola cabecera {int((cpl == 1).sum())} de {len(cpl)}")

# -- cuanta red hay en el top: parte de los CT y de los kVA instalados de las entidades del top 10 %
print("\nTamano del top 10 % (2015 - 2025) y de la persistencia (media 2023 - 2025):")
size = d.groupby(["distribuidora_id", "cabecera"]).agg(cts=("ct_id", "nunique"), kva=("kva", "sum")).reset_index()
print("CT por cabecera (48 entidades): mediana", size.cts.median(), "p90", size.cts.quantile(0.9), "max", size.cts.max(),
      "| cabeceras con 5 CT o menos:", int((size.cts <= 5).sum()))
for name, keys in [("cabecera", ENT["cabecera"]), ("linea (grupo_red)", ENT["linea (grupo_red)"]),
                   ("municipio", ENT["municipio"]), ("ct", ENT["ct"])]:
    uni = d[keys].drop_duplicates()
    sz = d.groupby(keys).agg(cts=("ct_id", "nunique"), kva=("kva", "sum")).reset_index()
    v = values(keys, range(2015, 2026), uni).merge(sz, on=keys)
    v = v.sort_values("kva_h", ascending=False)
    k = max(1, math.ceil(0.10 * len(v)))
    t = v.head(k)
    out = (f"  {name}: top {k} -> {t.kva_h.sum() / v.kva_h.sum():.1%} de los kVA*h con el {t.cts.sum() / v.cts.sum():.1%} "
           f"de los CT y el {t.kva.sum() / v.kva.sum():.1%} de los kVA")
    caps, ctp, kvp = [], [], []
    for y in (2023, 2024, 2025):
        past = values(keys, [y - 2, y - 1], uni).rename(columns={"kva_h": "prev"})
        now = values(keys, [y], uni)
        b = past.merge(now, on=keys).merge(sz, on=keys).sort_values("prev", ascending=False)
        kk = max(1, math.ceil(0.10 * len(b)))
        tt = b.head(kk)
        caps.append(tt.kva_h.sum() / b.kva_h.sum())
        ctp.append(tt.cts.sum() / b.cts.sum())
        kvp.append(tt.kva.sum() / b.kva.sum())
    out += f"; persistencia {np.mean(caps):.1%} con el {np.mean(ctp):.1%} de los CT y el {np.mean(kvp):.1%} de los kVA"
    print(out)
