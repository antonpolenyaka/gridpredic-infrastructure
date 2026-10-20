"""
Intervalo de la diferencia de incidencias contenidas en una zona entre las líneas de TedisNet y las
comunidades de Louvain sobre la similitud de Jaccard (variante C de comunidades.py), emparejado por
incidencia (bootstrap de 5.000 remuestras). Reutiliza las funciones de comunidades.py.

Uso: python comunidades_bootstrap.py <carpeta zonas_20261013_1041> [salida.txt]
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comunidades as c  # noqa: E402


def main():
    base = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else None
    d, aa, cs, it = c.cargar(base)
    for t in (d, aa, cs, it):
        t["k"] = c.ct_key(t.distribuidora_id, t.ct_id)
    elig = d[d.en_estudio & d.trafo_elemento_id.notna() & d.grupo_red_id.notna() & d.cabecera_id.notna()]
    keep = set(elig.k)
    aa_prev = aa[aa.primer_conocido_ts < c.T0]
    cs_prev = cs[cs.inicio_ts < c.T0]
    e_c = c.jaccard_topk(aa_prev, keep)
    e_a = c.coocurrencia(cs_prev, keep)
    e_b = c.coocurrencia(cs_prev[~cs_prev.es_microcorte], keep)
    import itertools
    nodes = sorted(keep.intersection(*[set(itertools.chain.from_iterable(e.keys())) for e in (e_a, e_b, e_c)]))
    nset = set(nodes)
    fut = it[(it.inicio_ts >= c.T0) & (it.inicio_ts.dt.normalize() != c.APAGON)
             & (it.clasificacion_id == "CL_IMPRE") & ~it.causa.isin(c.LOCAL_EXCLUDED) & it.k.isin(nset)]
    fut = fut.assign(inc=fut.distribuidora_id.astype(str) + "|" + fut.periodo_id.astype(str) + "|"
                     + fut.incidencia_id.astype(str))
    incid = [sorted(set(g.k)) for _, g in fut.groupby("inc") if g.k.nunique() >= 2]
    sel = elig.set_index("k")
    lin = {n: int(sel.loc[n, "grupo_red_id"]) for n in nodes}
    sub = {k: v for k, v in e_c.items() if k[0] in nset and k[1] in nset}
    lou = c.particion_louvain(sub, nodes, 1.0)
    a = np.array([len({lin[x] for x in cts}) == 1 for cts in incid], dtype=float)
    b = np.array([len({lou[x] for x in cts}) == 1 for cts in incid], dtype=float)
    rng = np.random.default_rng(42)
    idx = rng.integers(0, len(a), size=(5000, len(a)))
    diff = b[idx].mean(axis=1) - a[idx].mean(axis=1)
    lo, hi = np.percentile(diff, [2.5, 97.5])
    # Incidencias pequeñas (2 - 30 CT), las que deciden qué línea falla
    small = np.array([len(cts) <= 30 for cts in incid])
    txt = (f"Incidencias con 2 CT o mas: {len(incid)}; contenidas en una linea {100 * a.mean():.1f} %, en una "
           f"comunidad (Jaccard topK 10, Louvain res. 1,0; {len(set(lou.values()))} zonas) {100 * b.mean():.1f} %\n"
           f"Diferencia comunidad - linea: {100 * (b.mean() - a.mean()):+.1f} puntos, intervalo del 95 % "
           f"[{100 * lo:+.1f}, {100 * hi:+.1f}]\n"
           f"Solo incidencias de 2 a 30 CT ({int(small.sum())}): linea {100 * a[small].mean():.1f} %, "
           f"comunidad {100 * b[small].mean():.1f} %\n"
           f"Solo en una de las dos: linea si y comunidad no {int(((a == 1) & (b == 0)).sum())}, "
           f"comunidad si y linea no {int(((a == 0) & (b == 1)).sum())}")
    print(txt)
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(txt + "\n")


if __name__ == "__main__":
    main()
