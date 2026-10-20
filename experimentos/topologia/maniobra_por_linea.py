"""Tiempo de maniobra de 2023 - 2025 (sección 6 de job_analisis_red: en una incidencia con varios CT, los que vuelven
más de 5 minutos antes que el último) por cabecera y por línea de TedisNet: cuánto se reparte el de la cabecera
mayor entre sus líneas. Sin nombres en la salida.

Uso: python maniobra_por_linea.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
ev = pd.read_parquet(BASE / "interrupciones.parquet")
pdf = aa.copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
cab = pdf.drop_duplicates("trafo_elemento_id").set_index("trafo_elemento_id")["nombre"]
d = dim.assign(cabecera=dim.trafo_elemento_id.map(cab).fillna("sin_cabecera"), ct_id=dim.ct_id.astype(str))
kva = pd.to_numeric(d["potencia_tiepi_kva"], errors="coerce")
d["kva"] = kva.where(kva > 0, pd.to_numeric(d["potencia_kva"], errors="coerce")).fillna(0.0)
d["linea"] = d.grupo_red_id.map(lambda x: "sin_grupo" if pd.isna(x) else str(int(x)))
ev["ct_id"] = ev.ct_id.astype(str)
ev["inc"] = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby("inc")["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev = ev[ev.inicio_ts.dt.year.isin([2023, 2024, 2025])]
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(d[["distribuidora_id", "ct_id", "kva", "cabecera", "linea"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev.kva.fillna(0) * ev.dur_h
last = ev.groupby("inc").fin_ts.transform("max")
n = ev.groupby("inc").ct_id.transform("nunique")
ev["man"] = (n > 1) & (ev.fin_ts < last - pd.Timedelta(minutes=5))
ev["kva_h_man"] = ev.kva_h.where(ev.man, 0.0)
pc = ev.groupby(["distribuidora_id", "cabecera"]).agg(m=("kva_h_man", "sum"), lineas=("linea", "nunique"),
                                                      inc=("inc", "nunique")).sort_values("m", ascending=False)
top = pc.index[0]
print(f"cabeceras con tiempo de maniobra: {int((pc.m > 0).sum())}; la primera reune el {pc.m.iloc[0] / pc.m.sum():.1%} "
      f"del de las tres redes, con {pc.lineas.iloc[0]} lineas y {pc.inc.iloc[0]} incidencias")
sub = ev[(ev.distribuidora_id == top[0]) & (ev.cabecera == top[1])]
pl = sub.groupby("linea").kva_h_man.sum().sort_values(ascending=False)
print(f"dentro de ella: lineas con maniobra {int((pl > 0).sum())}; la primera linea tiene el {pl.iloc[0] / pl.sum():.1%}, "
      f"las tres primeras el {pl.head(3).sum() / pl.sum():.1%}")
pli = ev.groupby(["distribuidora_id", "linea"]).kva_h_man.sum().sort_values(ascending=False)
print(f"por linea (todas): {int((pli > 0).sum())} lineas con maniobra; la primera {pli.iloc[0] / pli.sum():.1%}; "
      f"la primera cabecera equivale a {pc.m.iloc[0] / pli.iloc[0]:.1f} veces la primera linea")

# la cabecera mayor por número de CT (la primera del riesgo de 2026 y del telemando en docs/network-analysis.md)
size = d.groupby(["distribuidora_id", "cabecera"]).ct_id.nunique().sort_values(ascending=False)
g = size.index[0]
pos = list(pc.index).index(g) + 1 if g in pc.index else None
sub = ev[(ev.distribuidora_id == g[0]) & (ev.cabecera == g[1])]
pl = sub.groupby("linea").kva_h_man.sum().sort_values(ascending=False)
print(f"cabecera mayor ({size.iloc[0]} CT): puesto {pos} por kVA*h de maniobra, {pc.loc[g, 'm'] / pc.m.sum():.1%} del total, "
      f"{pc.loc[g, 'inc']} incidencias en {pc.loc[g, 'lineas']} lineas; con maniobra {int((pl > 0).sum())} lineas; "
      f"la primera tiene el {pl.iloc[0] / pl.sum():.1%} y las tres primeras el {pl.head(3).sum() / pl.sum():.1%}")
