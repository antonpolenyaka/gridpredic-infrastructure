"""
E0. TransE del cuaderno (celda cell-29) con una sola relación.

En el cuaderno los triples salen de todo el edge_index de PubMed, que guarda
cada cita en los dos sentidos, con la relación 0. Si (h, r, t) y (t, r, h)
están los dos, TransE pide h + r = t y t + r = h a la vez, lo que empuja r a
cero: lo que queda es un embedding de proximidad (|h - t| pequeño para los
vecinos). Se comprueba con el grafo de co-corte de Calser anterior a 2025
(no dirigido) y, como contraste, con una relación dirigida entre dos tipos de
nodo (CT -> elemento de map_aguas_arriba anterior a 2025). Mismos parámetros
que el cuaderno salvo la dimensión (32 en lugar de 256, por memoria).

Uso: python embedding_una_relacion.py <carpeta de la exportación de Gold> [salida/]
"""
import os
import sys
import time

import numpy as np
from scipy.stats import spearmanr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import cargar, grafo  # noqa: E402
from kge_lib import entrenar_transe  # noqa: E402


def main():
    t0 = time.time()
    dim, it, inc, aa = cargar()
    n = len(dim)
    W, _, _ = grafo(inc, n, "2016-01-01", "2025-01-01")
    a, b = np.where(np.triu(W > 0, 1))
    tr1 = np.r_[np.c_[a, np.zeros_like(a), b], np.c_[b, np.zeros_like(b), a]].astype(np.int64)
    E, R = entrenar_transe(tr1, n, 1, d=32, epochs=100, k=1, modo="cuaderno", seed=0)
    ratio_u = np.linalg.norm(R[0]) / np.linalg.norm(E, axis=1).mean()
    red = dim.distribuidora_id.values
    rng = np.random.default_rng(0)
    i, j = rng.integers(0, n, 20000), rng.integers(0, n, 20000)
    m = (red[i] == red[j]) & (i != j)
    rho = spearmanr(-np.linalg.norm(E[i[m]] + R[0] - E[j[m]], axis=1), -np.linalg.norm(E[i[m]] - E[j[m]], axis=1))[0]
    print(f"Grafo de co-corte no dirigido (< 2025, {len(tr1):,} triples, cada arista en los dos sentidos): "
          f"|r| / |e| medio = {ratio_u:.3f}; Spearman entre -|h + r - t| y -|h - t| en 20.000 pares = {rho:.3f} "
          f"({time.time() - t0:.0f} s)", flush=True)
    e = aa[aa.primer_conocido_ts < "2025-01-01"]
    elems = {x: n + k for k, x in enumerate(np.sort(e.elemento_corte_id.unique()))}
    tr2 = np.c_[e.node.values, np.zeros(len(e), int), [elems[x] for x in e.elemento_corte_id.values]].astype(np.int64)
    E2, R2 = entrenar_transe(tr2, n + len(elems), 1, d=32, epochs=100, k=1, modo="cuaderno", seed=0)
    ratio_d = np.linalg.norm(R2[0]) / np.linalg.norm(E2, axis=1).mean()
    print(f"Relacion dirigida CT -> elemento de map_aguas_arriba (< 2025, {len(tr2):,} triples): "
          f"|r| / |e| medio = {ratio_d:.3f} ({time.time() - t0:.0f} s)", flush=True)


if __name__ == "__main__":
    main()
