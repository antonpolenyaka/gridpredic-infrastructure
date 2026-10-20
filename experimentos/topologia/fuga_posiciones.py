"""Fuga en el riesgo anual: n_posiciones_aguas_arriba de dim_ct cuenta las posiciones de toda la ventana del SCADA.

job_gold_dim_ct.py (docstring) dice que esa columna "describes the dimensión and is never a feature", pero
job_ml_riesgo_ct.py la mete en STRUCTURE, y la evaluación por origen móvil puntúa 2021 - 2025 con ella.
Aquí se compara, para cada CT y año Y, la columna tal cual (toda la historia) con la misma cuenta hecha solo con
las parejas conocidas antes del 1 de enero de Y (primer_conocido_ts), frente a la etiqueta del año: kVA*h de la
variante principal del CT en Y, sin el 28.04.2025.

Uso: python fuga_posiciones.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(sys.argv[1])
dim = pd.read_parquet(BASE / "dim_ct.parquet")
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
ev = pd.read_parquet(BASE / "interrupciones.parquet")

dim["ct_id"] = dim["ct_id"].astype(str)
kva = pd.to_numeric(dim["potencia_tiepi_kva"], errors="coerce")
dim["kva"] = kva.where(kva > 0, pd.to_numeric(dim["potencia_kva"], errors="coerce")).fillna(0.0)

ev["ct_id"] = ev["ct_id"].astype(str)
inc = ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|" + ev["incidencia_id"].astype(str)
first = ev.groupby(inc)["inicio_ts"].transform("min")
ev = ev[first.dt.strftime("%Y-%m-%d") != "2025-04-28"].copy()
ev["anio"] = ev.inicio_ts.dt.year
ev["dur_h"] = ev.duracion_s.clip(lower=0, upper=168 * 3600) / 3600.0
ev = ev.merge(dim[["distribuidora_id", "ct_id", "kva"]], on=["distribuidora_id", "ct_id"], how="left")
ev["kva_h"] = ev["kva"].fillna(0.0) * ev["dur_h"]
lab = ev.groupby(["distribuidora_id", "ct_id", "anio"]).kva_h.sum()

# CT con trafo; el n_posiciones de la exportación tiene que ser el de toda la historia
d = dim[dim.trafo_elemento_id.notna()].copy()
d["trafo_elemento_id"] = d["trafo_elemento_id"].astype("int64")
tot = aa.groupby("trafo_elemento_id").posicion_id.nunique()
chk = d.trafo_elemento_id.map(tot).fillna(0).astype(int)
print(f"n_posiciones de dim_ct igual a la cuenta de toda la historia: {(chk == d.n_posiciones_aguas_arriba).mean():.1%} "
      f"de {len(d)} CT con trafo")
print("primer_conocido_ts: de", aa.primer_conocido_ts.min(), "a", aa.primer_conocido_ts.max())


def capture(y, s, frac=0.10):
    order = np.argsort(-s, kind="stable")
    k = max(1, int(np.ceil(frac * len(y))))
    return y[order[:k]].sum() / y.sum()


rows = []
rng = np.random.default_rng(0)
for Y in range(2021, 2026):
    known = aa[aa.primer_conocido_ts < pd.Timestamp(year=Y, month=1, day=1)]
    asof = d.trafo_elemento_id.map(known.groupby("trafo_elemento_id").posicion_id.nunique()).fillna(0).to_numpy()
    nueva = d.trafo_elemento_id.map(
        aa[aa.primer_conocido_ts.dt.year == Y].groupby("trafo_elemento_id").posicion_id.nunique()).fillna(0).to_numpy()
    static = d.n_posiciones_aguas_arriba.to_numpy(dtype=float)
    y = pd.MultiIndex.from_frame(d[["distribuidora_id", "ct_id"]].assign(anio=Y)).map(lambda k: lab.get(k, 0.0))
    y = np.asarray(y, dtype=float)
    # desempates al azar, repetidos, para que los empates (muchos ceros) no decidan la captura
    def cap(s):
        return np.mean([capture(y, s + rng.uniform(0, 1e-6, len(s))) for _ in range(50)])
    rows.append({
        "anio": Y, "ct": len(d), "ct_con_kvah": int((y > 0).sum()),
        "spearman_toda": pd.Series(static).corr(pd.Series(y), method="spearman"),
        "spearman_antes": pd.Series(asof).corr(pd.Series(y), method="spearman"),
        "captura10_toda": cap(static), "captura10_antes": cap(asof), "captura10_azar": 0.10,
        "ct_con_posicion_nueva_en_Y": int((nueva > 0).sum()),
        "kvah_Y_en_CT_con_posicion_nueva": y[nueva > 0].sum() / y.sum(),
        "media_pos_toda": static.mean(), "media_pos_antes": asof.mean(),
    })
r = pd.DataFrame(rows)
pd.set_option("display.width", 200)
fmt = r.copy()
for c in ("spearman_toda", "spearman_antes"):
    fmt[c] = fmt[c].map(lambda x: f"{x:.3f}")
for c in ("captura10_toda", "captura10_antes", "kvah_Y_en_CT_con_posicion_nueva"):
    fmt[c] = fmt[c].map(lambda x: f"{x:.1%}")
for c in ("media_pos_toda", "media_pos_antes"):
    fmt[c] = fmt[c].map(lambda x: f"{x:.1f}")
print(fmt.to_string(index=False))
m = r[r.anio >= 2022]
print(f"media 2022 - 2025: spearman {m.spearman_toda.mean():.3f} (toda) frente a {m.spearman_antes.mean():.3f} (antes de Y); "
      f"captura del 10 % {m.captura10_toda.mean():.1%} frente a {m.captura10_antes.mean():.1%}")
