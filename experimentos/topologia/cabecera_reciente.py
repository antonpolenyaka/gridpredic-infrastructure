"""Sensibilidad de las cifras por cabecera a la definición: la de hoy (argmax de cortes de toda la ventana) frente a
una cabecera reciente (argmax solo entre las parejas que han cortado al trafo en los dos últimos años de la ventana;
si no hay ninguna, la de hoy). n_cortes sigue siendo el total de la pareja (la exportación no trae los cortes por
año), así que es una aproximación. Concentración 2015 - 2025, tamaño del top y lista naif frente a tamaño en
2021 - 2025 y 2022 - 2025, con las reglas de cabeceras_48.py y tamano_frente_historia.py.

Uso: python cabecera_reciente.py <carpeta zonas_20261013_1041>
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
FIN = aa.ultimo_corte_ts.max()

pdf = aa.copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
hoy = pdf.drop_duplicates("trafo_elemento_id").set_index("trafo_elemento_id")["nombre"]
rec = pdf[pdf.ultimo_corte_ts >= FIN - pd.DateOffset(years=2)].drop_duplicates("trafo_elemento_id")
rec = rec.set_index("trafo_elemento_id")["nombre"]
reciente = hoy.copy()
reciente.loc[rec.index] = rec
print(f"trafos con cabecera: {len(hoy)}; la cabecera reciente cambia en {(reciente != hoy).sum()} ({(reciente != hoy).mean():.1%})")

kva = pd.to_numeric(dim["potencia_tiepi_kva"], errors="coerce")
dim = dim.assign(kva=kva.where(kva > 0, pd.to_numeric(dim["potencia_kva"], errors="coerce")).fillna(0.0),
                 ct_id=dim.ct_id.astype(str), vd=pd.to_datetime(dim.vigente_desde, errors="coerce"),
                 vh=pd.to_datetime(dim.vigente_hasta_excl, errors="coerce"))
ev["ct_id"] = ev["ct_id"].astype(str)
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev["anio"] = ev.inicio_ts.dt.year
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(dim[["distribuidora_id", "ct_id", "kva"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev["kva"].fillna(0.0) * ev["dur_h"]
per_ct = ev.groupby(["distribuidora_id", "ct_id", "anio"]).kva_h.sum().rename("kva_h").reset_index()


def capture(y, s, k):
    o = np.lexsort((np.arange(len(s)), -np.asarray(s, dtype=float)))
    return float(y[o[:k]].sum() / y.sum())


for etiqueta, cab in (("hoy", hoy), ("reciente", reciente)):
    d = dim.copy()
    d["cabecera"] = d.trafo_elemento_id.map(cab).fillna("sin_cabecera")
    sz = d.groupby(["distribuidora_id", "cabecera"]).agg(cts=("ct_id", "nunique"), kva=("kva", "sum")).reset_index()
    tot = per_ct[(per_ct.anio >= 2015) & (per_ct.anio <= 2025)].merge(d[["distribuidora_id", "ct_id", "cabecera"]],
                                                                       on=["distribuidora_id", "ct_id"])
    v = sz.merge(tot.groupby(["distribuidora_id", "cabecera"]).kva_h.sum().reset_index(), how="left").fillna({"kva_h": 0})
    v = v.sort_values("kva_h", ascending=False)
    k = math.ceil(0.1 * len(v))
    t = v.head(k)
    out = (f"{etiqueta}: {len(v)} cabeceras, mayor {sz.cts.max()} CT; top {k} -> {t.kva_h.sum() / v.kva_h.sum():.1%} "
           f"de los kVA*h con el {t.kva.sum() / v.kva.sum():.1%} de los kVA")
    res = {"naif": [], "tamano": [], "maximo": []}
    for y in range(2021, 2026):
        lo, hi = pd.Timestamp(year=y, month=1, day=1), pd.Timestamp(year=y + 1, month=1, day=1)
        g = d[(d.vd.isna() | (d.vd < hi)) & (d.vh.isna() | (d.vh > lo))]
        lab = g.merge(per_ct[per_ct.anio == y], on=["distribuidora_id", "ct_id"], how="left").fillna({"kva_h": 0})
        prev = g.merge(per_ct[per_ct.anio.isin([y - 2, y - 1])].groupby(["distribuidora_id", "ct_id"]).kva_h.sum()
                       .rename("p").reset_index(), on=["distribuidora_id", "ct_id"], how="left").fillna({"p": 0})
        tt = lab.assign(p=prev.p.values).groupby(["distribuidora_id", "cabecera"]).agg(y=("kva_h", "sum"), p=("p", "sum"),
                                                                                       k=("kva", "sum"))
        kk = math.ceil(0.1 * len(tt))
        res["naif"].append(capture(tt.y.values, tt.p.values, kk))
        res["tamano"].append(capture(tt.y.values, tt.k.values, kk))
        res["maximo"].append(capture(tt.y.values, tt.y.values, kk))
    out += "; captura del 10 % 2021 - 2025 (2022 - 2025): " + ", ".join(
        f"{n} {np.mean(x):.1%} ({np.mean(x[1:]):.1%})" for n, x in res.items())
    print(out)
