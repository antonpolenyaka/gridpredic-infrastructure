"""
Aportan los vecinos en el embedding algo al riesgo anual por CT? (cálculo
orientativo sobre la salida del repositorio rct_todas_20261012_0744/
puntuaciones.csv: por CT y año, la etiqueta y_kva_h y las puntuaciones de
cada modelo, evaluadas por origen móvil).

Para cada año Y de 2022 a 2025 el grafo de co-corte solo lleva las aristas
conocidas antes de Y (binarias). Puntuaciones nuevas, todas con información
anterior a Y:
  - vec_<embedding>_k<k>: media de s_naif (kVA.h del CT en los dos años
    anteriores, del repo) de sus k CT más parecidos por coseno, en kVA.h o por
    kVA instalado multiplicado por los kVA del CT (_kva).
  - vec_grafo: lo mismo con los CT que comparten algún elemento aguas
    arriba, ponderados por cuántos comparten (la agregación de vecinos de un
    salto, sin embedding).
  - grado: cuántos CT comparten algún elemento con él.
Combinaciones: media de percentiles con el modelo historia+estructura y con
naif. Métrica del repo: parte de los kVA.h del año en el top 5, 10 y 20 % de
CT (empates al azar, media de 20 semillas).

Uso: python grafo_riesgo.py <carpeta de la exportación> <puntuaciones.csv> [salida/]
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd
from scipy.stats import rankdata

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from grafo_lib import OUT, PUNT, embeddings, incidence, knn_cosine, load  # noqa: E402

YEARS = [2022, 2023, 2024, 2025]
TOPS = (0.05, 0.10, 0.20)


def capture(y, s, q, seeds=20):
    out = []
    k = int(round(q * len(y)))
    for seed in range(seeds):
        noise = np.random.default_rng(seed).random(len(s)) * 1e-9 * (np.nanstd(s) + 1e-12)
        top = np.argsort(-(s + noise))[:k]
        out.append(y[top].sum() / y.sum())
    return np.mean(out)


def main():
    dim, aa, it = load()
    p = pd.read_csv(PUNT, dtype={"ct_id": str})
    kva = dim[["distribuidora_id", "ct_id", "potencia_kva"]].drop_duplicates(["distribuidora_id", "ct_id"])
    p = p.merge(kva, on=["distribuidora_id", "ct_id"], how="left")
    p["potencia_kva"] = p.potencia_kva.fillna(p.potencia_kva.median()).clip(lower=1)
    t2c = aa[["trafo_elemento_id", "distribuidora_id", "ct_id"]].drop_duplicates()
    rows, frames = [], []
    for Y in YEARS:
        B, trafos, _ = incidence(aa, cutoff=f"{Y}-01-01", weight="binary")
        E = embeddings(B)
        g = pd.DataFrame({"trafo_elemento_id": trafos}).merge(t2c, on="trafo_elemento_id", how="left")
        py = p[p.anio == Y].reset_index(drop=True)
        g = g.merge(py[["distribuidora_id", "ct_id", "s_naif", "potencia_kva"]], on=["distribuidora_id", "ct_id"], how="left")
        naif = g.s_naif.fillna(0).values
        naif_kva = naif / g.potencia_kva.fillna(g.potencia_kva.median()).values
        new = {}
        for name, X in E.items():
            for k in (10, 30):
                nn, _ = knn_cosine(X, k)
                new[f"vec_{name}_k{k}"] = naif[nn].mean(1)
                new[f"vec_{name}_k{k}_kva"] = naif_kva[nn].mean(1)
        A = (B > 0).astype(float)
        W = A @ A.T
        np.fill_diagonal(W, 0)
        deg = (W > 0).sum(1)
        new["vec_grafo"] = (W @ naif) / np.maximum(W.sum(1), 1e-12)
        new["vec_grafo_kva"] = (W @ naif_kva) / np.maximum(W.sum(1), 1e-12)
        new["grado"] = deg.astype(float)
        for c, v in new.items():
            g[c] = v
        py = py.merge(g.drop(columns=["trafo_elemento_id", "s_naif", "potencia_kva"]), on=["distribuidora_id", "ct_id"],
                      how="left")
        for c in new:
            if c.endswith("_kva"):
                py[c] = py[c] * py.potencia_kva
            py[c] = py[c].fillna(py[c].median())
        n = len(py)
        r = {c: rankdata(py[c]) / n for c in ["s_naif", "s_historia+estructura"] + list(new)}
        for c in new:
            if c.startswith("vec_"):
                py[f"modelo+{c}"] = (r["s_historia+estructura"] + r[c]) / 2
                py[f"naif+{c}"] = (r["s_naif"] + r[c]) / 2
        frames.append(py)
    d = pd.concat(frames, ignore_index=True)
    cols = ["s_oraculo", "s_naif", "s_naif_cabecera", "s_historia", "s_historia+estructura", "s_todo"] + \
           [c for c in d.columns if c.startswith(("vec_", "grado", "modelo+", "naif+"))]
    for c in cols:
        r = dict(puntuacion=c)
        for q in TOPS:
            vals = [capture(g.y_kva_h.values, g[c].values, q) for _, g in d.groupby("anio")]
            r[f"top{int(q * 100)}"] = np.mean(vals)
            if q == 0.10:
                r.update({f"top10_{y}": v for y, v in zip(YEARS, vals)})
        rows.append(r)
    res = pd.DataFrame(rows)
    pd.set_option("display.width", 250)
    print("Captura de kVA.h imprevistos en el top k % de CT (media de 2022 - 2025)")
    show = res[res.puntuacion.str.match(r"^(s_|grado|vec_grafo|vec_incidencia_tfidf_k30|vec_lsa_k30|vec_netmf_k30|"
                                        r"vec_espectral_k30|vec_incidencia_tfidf_k10|vec_netmf_k10|modelo\+vec_grafo|"
                                        r"modelo\+vec_incidencia_tfidf_k30|modelo\+vec_netmf_k30|naif\+vec_grafo|"
                                        r"naif\+vec_incidencia_tfidf_k30|naif\+vec_netmf_k30)")]
    print(show.to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    os.makedirs(OUT, exist_ok=True)
    res.to_csv(os.path.join(OUT, "grafo_riesgo_captura.csv"), index=False)
    best = res[res.puntuacion.str.startswith("modelo+")].sort_values("top10", ascending=False).head(5)
    print("\nMejores combinaciones con el modelo (top 10 %):")
    print(best[["puntuacion", "top5", "top10", "top20"]].to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    print("\nSpearman entre grado y vec_grafo / s_naif_cabecera (todas las filas):")
    print(d[["grado", "vec_grafo", "vec_netmf_k30", "s_naif_cabecera", "s_naif", "y_kva_h"]].corr(method="spearman")
          .round(3).to_string())


if __name__ == "__main__":
    main()
