"""
Control de grafo_riesgo.py: cuánto de la mejora de "modelo + vecinos por kVA"
se debe solo a reponderar por los kVA del propio CT. Se comparan con el
modelo historia+estructura: el modelo solo, modelo + kVA del CT (media de
percentiles), modelo + vecinos del grafo por kVA, y modelo + vecinos con los
vecinos barajados dentro de la distribuidora (mismo kVA propio, vecinos al
azar). Bootstrap por CT de la diferencia de captura al 10 %.

Uso: python grafo_riesgo_control.py <carpeta de la exportación> <puntuaciones.csv> [salida/]
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd
from scipy.stats import rankdata

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from grafo_lib import PUNT, incidence, load  # noqa: E402
from grafo_riesgo import YEARS, capture  # noqa: E402


def main():
    dim, aa, it = load()
    p = pd.read_csv(PUNT, dtype={"ct_id": str})
    kva = dim[["distribuidora_id", "ct_id", "potencia_kva"]].drop_duplicates(["distribuidora_id", "ct_id"])
    p = p.merge(kva, on=["distribuidora_id", "ct_id"], how="left")
    p["potencia_kva"] = p.potencia_kva.fillna(p.potencia_kva.median()).clip(lower=1)
    t2c = aa[["trafo_elemento_id", "distribuidora_id", "ct_id"]].drop_duplicates()
    frames = []
    rng = np.random.default_rng(0)
    for Y in YEARS:
        B, trafos, _ = incidence(aa, cutoff=f"{Y}-01-01", weight="binary")
        g = pd.DataFrame({"trafo_elemento_id": trafos}).merge(t2c, on="trafo_elemento_id", how="left")
        py = p[p.anio == Y].reset_index(drop=True)
        g = g.merge(py[["distribuidora_id", "ct_id", "s_naif", "potencia_kva"]], on=["distribuidora_id", "ct_id"], how="left")
        naif_kva = (g.s_naif.fillna(0) / g.potencia_kva.fillna(g.potencia_kva.median())).values
        A = (B > 0).astype(float)
        W = A @ A.T
        np.fill_diagonal(W, 0)
        g["vec"] = (W @ naif_kva) / np.maximum(W.sum(1), 1e-12)
        # neighbours shuffled inside each distribuidora: same weights, random identities
        perm = np.arange(len(g))
        for dist in g.distribuidora_id.unique():
            idx = np.where(g.distribuidora_id.values == dist)[0]
            perm[idx] = rng.permutation(idx)
        g["vec_azar"] = (W @ naif_kva[perm]) / np.maximum(W.sum(1), 1e-12)
        py = py.merge(g[["distribuidora_id", "ct_id", "vec", "vec_azar"]], on=["distribuidora_id", "ct_id"], how="left")
        for c in ("vec", "vec_azar"):
            py[c] = (py[c] * py.potencia_kva).fillna((py[c] * py.potencia_kva).median())
        n = len(py)
        rm = rankdata(py["s_historia+estructura"]) / n
        py["modelo"] = py["s_historia+estructura"]
        py["kva_propio"] = py.potencia_kva
        py["modelo+kva_propio"] = (rm + rankdata(py.potencia_kva) / n) / 2
        py["modelo+vecinos_kva"] = (rm + rankdata(py.vec) / n) / 2
        py["modelo+vecinos_barajados_kva"] = (rm + rankdata(py.vec_azar) / n) / 2
        frames.append(py)
    d = pd.concat(frames, ignore_index=True)
    cols = ["modelo", "kva_propio", "modelo+kva_propio", "modelo+vecinos_kva", "modelo+vecinos_barajados_kva", "s_naif"]
    for c in cols:
        vals = {q: [capture(g.y_kva_h.values, g[c].values, q) for _, g in d.groupby("anio")] for q in (0.05, 0.1, 0.2)}
        print(f"{c:30s} top5 {np.mean(vals[0.05]):.3f} top10 {np.mean(vals[0.1]):.3f} "
              f"({', '.join(f'{v:.3f}' for v in vals[0.1])}) top20 {np.mean(vals[0.2]):.3f}")
    # bootstrap by CT (same CTs resampled in every year) of top10 difference vs the model
    cts = d[["distribuidora_id", "ct_id"]].drop_duplicates().reset_index(drop=True)
    cts["cid"] = np.arange(len(cts))
    d = d.merge(cts, on=["distribuidora_id", "ct_id"])
    for c in ("modelo+vecinos_kva", "modelo+kva_propio"):
        diffs = []
        for b in range(300):
            take = np.random.default_rng(b).integers(0, len(cts), len(cts))
            w = np.bincount(take, minlength=len(cts))
            vals = []
            for _, g in d.groupby("anio"):
                rep = np.repeat(np.arange(len(g)), w[g.cid.values])
                gg = g.iloc[rep]
                vals.append(capture(gg.y_kva_h.values, gg[c].values, 0.1, seeds=3) -
                            capture(gg.y_kva_h.values, gg["modelo"].values, 0.1, seeds=3))
            diffs.append(np.mean(vals))
        diffs = np.array(diffs)
        print(f"{c} - modelo (top 10 %): {diffs.mean():+.3f} [{np.percentile(diffs, 2.5):+.3f}, "
              f"{np.percentile(diffs, 97.5):+.3f}], P(>0) = {(diffs > 0).mean():.2f}")


if __name__ == "__main__":
    main()
