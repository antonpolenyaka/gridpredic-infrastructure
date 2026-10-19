"""Cuánto de la captura por cabecera es tamaño: lista naif (kVA*h de los dos años anteriores) frente a ordenar
solo por los kVA instalados o por el número de CT, y frente al máximo posible, en 2021 - 2025 (los años que
evalúa job_ml_riesgo_ct.py) y en 2023 - 2025 (la persistencia de job_analisis_red.py).

Mismas reglas que cabeceras_48.py: cabecera de _feeders, variante principal sin el 28.04.2025, kVA de TIEPI.
Universo de cada año: los CT de dim_ct vigentes ese año (como build_table) agrupados por (distribuidora, nivel).

Uso: python tamano_frente_historia.py <carpeta zonas_20261013_1041> [parte del top, 0.10 por defecto]
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


def capture(y, s, k, rng):
    caps = []
    for _ in range(30):  # empates al azar
        o = np.lexsort((rng.random(len(s)), -s))
        caps.append(y[o[:k]].sum() / y.sum())
    return float(np.mean(caps))


rng = np.random.default_rng(0)
FRAC = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10
print(f"top {FRAC:.0%}")
for nivel, keys in [("cabecera", ["distribuidora_id", "cabecera"]), ("línea", ["distribuidora_id", "grupo_red_id"]),
                    ("ct", ["distribuidora_id", "ct_id"])]:
    for anios in ([2021, 2022, 2023, 2024, 2025], [2023, 2024, 2025]):
        res = {"naif": [], "kva": [], "n_ct": [], "maximo": [], "ent": []}
        for y in anios:
            g = vigentes(y)
            lab = g.merge(per_ct[per_ct.anio == y][["distribuidora_id", "ct_id", "kva_h"]], on=["distribuidora_id", "ct_id"],
                          how="left").fillna({"kva_h": 0.0})
            prev = g.merge(per_ct[per_ct.anio.isin([y - 2, y - 1])].groupby(["distribuidora_id", "ct_id"]).kva_h.sum()
                           .rename("prev").reset_index(), on=["distribuidora_id", "ct_id"], how="left").fillna({"prev": 0.0})
            t = lab.assign(prev=prev["prev"].values).groupby(keys).agg(y=("kva_h", "sum"), prev=("prev", "sum"),
                                                                       kva=("kva", "sum"), n_ct=("ct_id", "nunique"))
            k = max(1, math.ceil(FRAC * len(t)))
            yy = t.y.to_numpy()
            res["naif"].append(capture(yy, t.prev.to_numpy(), k, rng))
            res["kva"].append(capture(yy, t.kva.to_numpy(), k, rng))
            res["n_ct"].append(capture(yy, t.n_ct.to_numpy().astype(float), k, rng))
            res["maximo"].append(capture(yy, yy, k, rng))
            res["ent"].append(len(t))
        print(f"{nivel:9s} {anios[0]} - {anios[-1]} (entidades {min(res['ent'])} - {max(res['ent'])}), top: "
              f"naif {np.mean(res['naif']):.1%}, solo kVA instalados {np.mean(res['kva']):.1%}, "
              f"solo número de CT {np.mean(res['n_ct']):.1%}, máximo {np.mean(res['maximo']):.1%}; "
              f"por año kVA {[f'{x:.0%}' for x in res['kva']]} naif {[f'{x:.0%}' for x in res['naif']]}")


# Por CT hay muchos empates de kVA: el job (capture de job_ml_riesgo_ct.py) los deshace por el orden de las filas,
# que es el de la clave (distribuidora_id, ct_id). Aquí, el tamaño solo con ese desempate.
caps = []
for y in range(2021, 2026):
    g = vigentes(y).sort_values(["distribuidora_id", "ct_id"])
    lab = g.merge(per_ct[per_ct.anio == y][["distribuidora_id", "ct_id", "kva_h"]], on=["distribuidora_id", "ct_id"],
                  how="left").fillna({"kva_h": 0.0})
    yy, ss = lab.kva_h.to_numpy(), lab.kva.to_numpy()
    k = max(1, math.ceil(FRAC * len(yy)))
    o = np.lexsort((np.arange(len(ss)), -ss))
    caps.append(float(yy[o[:k]].sum() / yy.sum()))
print(f"ct, solo kVA con el desempate del job (orden de la clave), 2021 - 2025: {np.mean(caps):.1%}")
