"""Las cabeceras más grandes: cuántos CT y cuántas líneas de TedisNet abarcan, cuántos trafos corta el elemento en
total y qué parte de los cortes de sus trafos explica. Sin nombres de instalaciones en la salida.

Uso: python cabeceras_grandes.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}
pdf = aa.copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
top = pdf.drop_duplicates("trafo_elemento_id")
tot = aa.groupby("trafo_elemento_id").n_cortes.sum()
top = top.assign(parte=top.n_cortes.values / top.trafo_elemento_id.map(tot).values)
d = dim.drop(columns=["cabecera"]).merge(top[["trafo_elemento_id", "elemento_corte_id", "parte"]],
                                         on="trafo_elemento_id", how="left")
d = d[d.elemento_corte_id.notna()]
cortados = aa.groupby("elemento_corte_id").trafo_elemento_id.nunique()
n_lineas_red = dim.groupby("distribuidora_id").grupo_red_id.nunique()
n_ct_red = dim.groupby("distribuidora_id").ct_id.nunique()
g = d.groupby(["distribuidora_id", "elemento_corte_id"]).agg(
    cts=("ct_id", "nunique"), lineas=("grupo_red_id", "nunique"), parte_mediana=("parte", "median")).reset_index()
g["trafos_que_corta"] = g.elemento_corte_id.map(cortados)
g = g.sort_values("cts", ascending=False).reset_index(drop=True)
g["red"] = g.distribuidora_id.map(NOMBRE)
g["parte_ct_red"] = g.cts / g.distribuidora_id.map(n_ct_red)
g["lineas_red"] = g.distribuidora_id.map(n_lineas_red)
pd.set_option("display.width", 200)
print(g.head(8)[["red", "cts", "parte_ct_red", "lineas", "lineas_red", "trafos_que_corta", "parte_mediana"]]
      .to_string(index=False, formatters={"parte_ct_red": "{:.0%}".format, "parte_mediana": "{:.0%}".format}))
print("cabeceras con un solo CT:", int((g.cts == 1).sum()), "| con 5 o menos:", int((g.cts <= 5).sum()), "de", len(g))
