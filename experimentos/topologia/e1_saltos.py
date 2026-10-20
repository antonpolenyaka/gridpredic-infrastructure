"""
E1. Vecinos a k saltos en el grafo de co-corte resueltos por conjuntos (memoria, 4.4.4): un salto de CT es el patrón
(c:CT)-[:AGUAS_ARRIBA]->(:ElementoCorte)<-[:AGUAS_ARRIBA]-(c2:CT). Es el experimento de amigos de amigos de la
asignatura hecho con nuestro grafo, con dos caminos de pandas y scipy:

- joins por nivel con DISTINCT y anti-join de los visitados (como hierarchy_closure en Spark): los CT alcanzados;
- la cadena de joins sin DISTINCT (enumeración de caminos), contada con la matriz de incidencia CT x elemento sin
  materializarla: las filas que tendría que mover un join relacional;
- todos los CT a un salto: autojoin con DISTINCT frente al producto de la matriz de incidencia por su traspuesta.

Con todas las aristas de map_aguas_arriba y con las fuertes (n_cortes de 2 o más y al menos el 20 % de los cortes
del CT).

    python e1_saltos.py <carpeta de la exportación> [salida/]
"""

import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import grafo_gp as gg  # noqa: E402

K = 4
N_ORIGENES = 20
SEMILLA = 7
LINES = []


def say(text=""):
    print(text)
    LINES.append(text)


def reloj(f, *a, rep=3):
    tiempos, out = [], None
    for _ in range(rep):
        t0 = time.perf_counter()
        out = f(*a)
        tiempos.append(time.perf_counter() - t0)
    return out, statistics.median(tiempos)


def bfs_pandas(aa2, s, k):
    """CT alcanzados acumulados a 1..k saltos desde s, sin repetir los visitados."""
    visit = {s}
    front = pd.DataFrame({"ct": [s]})
    acum = []
    for _ in range(k):
        els = front.merge(aa2, on="ct")[["el"]].drop_duplicates()
        nxt = els.merge(aa2, on="el")[["ct"]].drop_duplicates()
        nxt = nxt[~nxt.ct.isin(visit)]
        visit |= set(nxt.ct)
        front = nxt
        acum.append(len(visit) - 1)
    return acum


def caminos(B, idx, s, k):
    """Filas de la cadena de joins sin DISTINCT: número de paseos CT-E-CT de cada longitud."""
    w = np.zeros(B.shape[0])
    w[idx[s]] = 1.0
    out = []
    for _ in range(k):
        w = B @ (B.T @ w)
        out.append(float(w.sum()))
    return out


def todos_a_un_salto(aa2, B):
    res = {}
    codes = pd.DataFrame({"ct": pd.factorize(aa2.ct)[0].astype("int32"), "el": pd.factorize(aa2.el)[0].astype("int32")})
    t0 = time.perf_counter()
    m = codes.merge(codes, on="el")
    filas_join = len(m)
    m = m[m.ct_x != m.ct_y][["ct_x", "ct_y"]].drop_duplicates()
    res["pandas (autojoin y DISTINCT)"] = (len(m), time.perf_counter() - t0)
    del m
    t0 = time.perf_counter()
    M = (B @ B.T).tocsr()
    M.setdiag(0)
    M.eliminate_zeros()
    res["scipy (B por B traspuesta)"] = (int(M.nnz), time.perf_counter() - t0)
    return res, filas_join


def main():
    t = gg.cargar(sys.argv[1])
    aa = t["map_aguas_arriba"]
    tot = aa.groupby("ct").n_cortes.transform("sum")
    aa["fuerte"] = (aa.n_cortes >= 2) & (aa.n_cortes / tot >= 0.2)
    say(f"aristas AGUAS_ARRIBA: {len(aa):,} (fuertes: {int(aa.fuerte.sum()):,}, n_cortes >= 2 y al menos el 20 % "
        f"de los cortes del CT)")
    grado_el = aa.groupby("el").ct.nunique()
    say(f"suma de grado^2 de los elementos (filas del autojoin por elemento): {int((grado_el**2).sum()):,}")

    for nombre, sub in [("todas las aristas", aa), ("aristas fuertes", aa[aa.fuerte])]:
        cts = sorted(sub.ct.unique())
        els = sorted(sub.el.unique())
        B = gg.incidencia_matrix(sub, cts, els)
        idx = {c: i for i, c in enumerate(cts)}
        aa2 = sub[["ct", "el"]].drop_duplicates()
        origenes = list(np.random.default_rng(SEMILLA).choice(cts, size=N_ORIGENES, replace=False))

        say(f"\n===== {nombre}: {len(cts):,} CT, {len(els)} elementos, {len(aa2):,} aristas")
        say(f"Un origen (mediana de {N_ORIGENES} CT al azar): CT alcanzados acumulados por salto y tiempo")
        alcanzados, tiempos, walks = [], [], []
        for s in origenes:
            a, tt = reloj(bfs_pandas, aa2, s, K)
            alcanzados.append(a)
            tiempos.append(tt)
            walks.append(caminos(B, idx, s, K))
        say(f"  saltos de CT: {list(range(1, K + 1))}")
        say(f"  CT alcanzados (mediana): {[int(x) for x in np.median(np.array(alcanzados), axis=0)]} de {len(cts) - 1}")
        say("  filas de la cadena de joins sin DISTINCT (mediana): "
            + str([f"{x:,.0f}" for x in np.median(walks, axis=0)]))
        say(f"  pandas DISTINCT: {1000 * statistics.median(tiempos):.1f} ms por origen (K = {K})")

        s = origenes[0]
        prevision = caminos(B, idx, s, K)
        front = pd.DataFrame({"ct": [s]})
        for k in range(1, K + 1):
            if prevision[k - 1] > 20_000_000:
                say(f"  pandas sin DISTINCT, primer origen, profundidad {k}: no se materializa, serían "
                    f"{prevision[k - 1]:,.0f} filas")
                break
            t0 = time.perf_counter()
            front = front.merge(aa2, on="ct")[["el"]].merge(aa2, on="el")[["ct"]]
            say(f"  pandas sin DISTINCT, primer origen, profundidad {k}: {len(front):,} filas en "
                f"{1000 * (time.perf_counter() - t0):.0f} ms")

        say("Todos los CT a un salto (vecinos a dos aristas de cada CT):")
        r, filas = todos_a_un_salto(aa2, B)
        for m, (n, dt) in r.items():
            say(f"  {m}: {n:,} pares en {1000 * dt:.0f} ms")
        say(f"  filas intermedias del autojoin antes del DISTINCT: {filas:,}")
        say(f"  densidad de la proyección CT - CT: {r['scipy (B por B traspuesta)'][0] / (len(cts) * (len(cts) - 1)):.1%}")

    if len(sys.argv) > 2:
        Path(sys.argv[2]).mkdir(parents=True, exist_ok=True)
        (Path(sys.argv[2]) / "e1_saltos.txt").write_text("\n".join(LINES) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
