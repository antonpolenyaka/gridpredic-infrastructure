"""Parte de los kVA*h imprevistos de cada año (2021 - 2023) que se lleva la cabecera mayor (la de más CT).
La carga es la de tamano_frente_historia.py, con las mismas reglas que cabeceras_48.py: cabecera de _feeders,
variante principal sin el 28.04.2025, kVA de TIEPI y, cada año, los CT de dim_ct vigentes ese año (como
build_table).

Uso: python parte_cabecera_mayor.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

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
d["vd"] = pd.to_datetime(d["vigente_desde"], errors="coerce")
d["vh"] = pd.to_datetime(d["vigente_hasta_excl"], errors="coerce")

ev["ct_id"] = ev["ct_id"].astype(str)
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev["anio"] = ev.inicio_ts.dt.year
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(d[["distribuidora_id", "ct_id", "kva"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev["kva"].fillna(0.0) * ev["dur_h"]
per_ct = ev.groupby(["distribuidora_id", "ct_id", "anio"]).kva_h.sum().rename("kva_h").reset_index()


def vigentes(y):
    lo, hi = pd.Timestamp(year=y, month=1, day=1), pd.Timestamp(year=y + 1, month=1, day=1)
    return d[(d.vd.isna() | (d.vd < hi)) & (d.vh.isna() | (d.vh > lo))]
for y in [2021, 2022, 2023]:
    g = vigentes(y)
    lab = g.merge(per_ct[per_ct.anio == y][["distribuidora_id", "ct_id", "kva_h"]], on=["distribuidora_id", "ct_id"], how="left").fillna({"kva_h": 0.0})
    t = lab.groupby(["distribuidora_id", "cabecera"]).kva_h.sum().sort_values(ascending=False)
    n = lab.groupby(["distribuidora_id", "cabecera"]).ct_id.nunique()
    print(y, "mayor por kVA*h:", f"{t.iloc[0]/t.sum():.1%}", "CT de esa cabecera", n[t.index[0]], "| la de mas CT:", f"{t[n.idxmax()]/t.sum():.1%}", n.max())
