"""
Comunidades del grafo de cortes (Louvain, como en la diapositiva 42 del Lab 2 de Neo4j) frente a las
líneas de TedisNet (grupo_red_id), la cabecera argmax y el municipio, como unidad para repartir el riesgo
del día (memoria 5.7.4, riesgo_zonas.py).

Sin servidor: networkx (louvain_communities; no es una dependencia del repositorio, hay que instalarlo aparte) y pandas sobre la exportación de Gold zonas_20261013_1041.

Fuera del tiempo: el grafo solo usa lo conocido antes de T0 = 01.01.2023 y se evalúa con las incidencias
imprevistas de causa local de T0 en adelante (sin el 28.04.2025), las que haría salir a una brigada.

Variantes de grafo CT - CT:
  A. co-corte SCADA: peso = eventos de corte (Off) anteriores a T0 que cortan a los dos CT (todos)
  B. lo mismo solo con cortes sostenidos (no microcorte)
  C. similitud de nodos (Jaccard sobre los elementos de corte conocidos antes de T0, topK = 10, como
     gds.nodeSimilarity por defecto) y Louvain sobre ese grafo (la cadena de la diapositiva 43 + 42)

Métricas por partición (solo los CT con trafo, línea, cabecera y alguna arista antes de T0):
  - zonas, CT por zona (mediana, máximo)
  - % de incidencias con 2 CT o más contenidas en una sola zona
  - pares: precisión (pares de la misma zona que caen juntos en alguna incidencia futura), recall (pares
    que caen juntos y están en la misma zona) y F1

Uso: python comunidades.py <carpeta zonas_20261013_1041> [salida.txt]
"""
import itertools
import sys
import time
from collections import Counter, defaultdict

try:
    import networkx as nx
except ImportError:  # networkx no es una dependencia del repositorio
    raise SystemExit("comunidades.py necesita networkx (pip install networkx) para el algoritmo de Louvain")
import numpy as np
import pandas as pd
from networkx.algorithms.community import louvain_communities

LOCAL_EXCLUDED = ("aguas_arriba", "apagon", "programada", "maniobra", "cliente_bt", "red_bt")  # riesgo_zonas.py l. 54
T0 = pd.Timestamp("2023-01-01")
APAGON = pd.Timestamp("2025-04-28")
SEED = 42


def cargar(base):
    d = pd.read_parquet(f"{base}/dim_ct.parquet")
    aa = pd.read_parquet(f"{base}/map_aguas_arriba.parquet")
    cs = pd.read_parquet(f"{base}/cortes_scada.parquet",
                         columns=["evento_off_id", "distribuidora_id", "ct_id", "inicio_ts", "es_microcorte"])
    it = pd.read_parquet(f"{base}/interrupciones.parquet")
    return d, aa, cs, it


def ct_key(dist, ct):
    return pd.Series(dist).astype(str).values + ":" + pd.Series(ct).astype(str).values


def coocurrencia(cs, keep):
    """{(a, b): n eventos que cortan a los dos}, a < b."""
    w = Counter()
    for _, g in cs.groupby("evento_off_id"):
        cts = sorted(set(g["k"]) & keep)
        if len(cts) < 2:
            continue
        for a, b in itertools.combinations(cts, 2):
            w[(a, b)] += 1
    return w


def jaccard_topk(aa, keep, k=10):
    """Similitud de Jaccard entre CT por sus elementos de corte (bipartito CT -> elemento), topK por CT."""
    nb = aa.groupby("k")["elemento_corte_id"].apply(set).to_dict()
    nodes = sorted(n for n in nb if n in keep)
    # índice invertido para no comparar pares sin vecinos comunes
    inv = defaultdict(set)
    for n in nodes:
        for e in nb[n]:
            inv[e].add(n)
    edges = {}
    for n in nodes:
        cand = set().union(*(inv[e] for e in nb[n])) - {n}
        sims = sorted(((len(nb[n] & nb[m]) / len(nb[n] | nb[m]), m) for m in cand), reverse=True)[:k]
        for s, m in sims:
            key = (min(n, m), max(n, m))
            edges[key] = max(edges.get(key, 0.0), s)
    return edges


def particion_louvain(edges, nodes, resolution):
    G = nx.Graph()
    G.add_nodes_from(nodes)
    for (a, b), w in edges.items():
        G.add_edge(a, b, weight=float(w))
    comms = louvain_communities(G, weight="weight", resolution=resolution, seed=SEED)
    return {n: i for i, c in enumerate(comms) for n in c}


def evaluar(zona: dict, incid: list, nodes: list) -> dict:
    sizes = Counter(zona[n] for n in nodes)
    cont = np.mean([len({zona[c] for c in cts}) == 1 for cts in incid])
    # pares que caen juntos en alguna incidencia futura
    cofail = set()
    for cts in incid:
        for a, b in itertools.combinations(sorted(cts), 2):
            cofail.add((a, b))
    same = 0
    tp = 0
    by_zone = defaultdict(list)
    for n in nodes:
        by_zone[zona[n]].append(n)
    for members in by_zone.values():
        members = sorted(members)
        same += len(members) * (len(members) - 1) // 2
        for a, b in itertools.combinations(members, 2):
            if (a, b) in cofail:
                tp += 1
    prec = tp / same if same else float("nan")
    rec = tp / len(cofail) if cofail else float("nan")
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return {"zonas": len(sizes), "ct_mediana": float(np.median(list(sizes.values()))),
            "ct_max": max(sizes.values()), "contenidas_%": 100 * cont,
            "precision_%": 100 * prec, "recall_%": 100 * rec, "F1_%": 100 * f1, "pares_misma_zona": same}


def resolucion_para(edges, nodes, objetivo, lo=0.05, hi=20.0):
    """Busca la resolución de Louvain que da un número de comunidades cercano al objetivo."""
    best = None
    for _ in range(25):
        mid = (lo * hi) ** 0.5
        n = len(set(particion_louvain(edges, nodes, mid).values()))
        if best is None or abs(n - objetivo) < abs(best[1] - objetivo):
            best = (mid, n)
        if n < objetivo:
            lo = mid
        elif n > objetivo:
            hi = mid
        else:
            break
    return best


def main():
    base = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else None
    t0 = time.perf_counter()
    d, aa, cs, it = cargar(base)
    d["k"] = ct_key(d.distribuidora_id, d.ct_id)
    aa["k"] = ct_key(aa.distribuidora_id, aa.ct_id)
    cs["k"] = ct_key(cs.distribuidora_id, cs.ct_id)
    it["k"] = ct_key(it.distribuidora_id, it.ct_id)

    elig = d[d.en_estudio & d.trafo_elemento_id.notna() & d.grupo_red_id.notna() & d.cabecera_id.notna()]
    cs_prev = cs[cs.inicio_ts < T0]
    aa_prev = aa[aa.primer_conocido_ts < T0]
    keep_all = set(elig.k)

    variantes = {
        "A co-corte (todos los Off)": coocurrencia(cs_prev, keep_all),
        "B co-corte (sostenidos)": coocurrencia(cs_prev[~cs_prev.es_microcorte], keep_all),
        "C similitud Jaccard topK 10": jaccard_topk(aa_prev, keep_all),
    }
    # Universo comun: CT elegibles con alguna arista en las tres variantes
    con_arista = [set(itertools.chain.from_iterable(e.keys())) for e in variantes.values()]
    nodes = sorted(keep_all.intersection(*con_arista))
    nset = set(nodes)

    # Incidencias futuras de causa local con 2 CT o más del universo
    fut = it[(it.inicio_ts >= T0) & (it.inicio_ts.dt.normalize() != APAGON)
             & (it.clasificacion_id == "CL_IMPRE") & ~it.causa.isin(LOCAL_EXCLUDED) & it.k.isin(nset)]
    fut = fut.assign(inc=fut.distribuidora_id.astype(str) + "|" + fut.periodo_id.astype(str) + "|"
                     + fut.incidencia_id.astype(str))
    incid = [sorted(set(g.k)) for _, g in fut.groupby("inc") if g.k.nunique() >= 2]
    n_inc_total = fut.inc.nunique()

    lines = []
    p = lines.append
    p(f"Universo: {len(nodes)} CT (de {len(elig)} elegibles con trafo, linea y cabecera); T0 = {T0.date()}")
    for name, e in variantes.items():
        sub = {k: v for k, v in e.items() if k[0] in nset and k[1] in nset}
        dens = 100 * len(sub) / (len(nodes) * (len(nodes) - 1) / 2)
        p(f"  {name}: {len(sub)} aristas ({dens:.1f} % de los pares)")
    p(f"Incidencias de causa local desde T0 en el universo: {n_inc_total}; con 2 CT o mas: {len(incid)} "
      f"(mediana {int(np.median([len(c) for c in incid]))} CT, maximo {max(len(c) for c in incid)})")
    p("")

    ref = {}
    sel = elig.set_index("k")
    ref["Linea (grupo_red_id)"] = {n: f"g{int(sel.loc[n, 'grupo_red_id'])}" for n in nodes}
    ref["Cabecera argmax (historico completo)"] = {n: f"c{int(sel.loc[n, 'cabecera_id'])}" for n in nodes}
    ref["Municipio"] = {n: f"m{sel.loc[n, 'municipio_id']}" for n in nodes}
    ref["Distribuidora"] = {n: f"d{sel.loc[n, 'distribuidora_id']}" for n in nodes}
    n_lineas = len(set(ref["Linea (grupo_red_id)"].values()))
    n_cab = len(set(ref["Cabecera argmax (historico completo)"].values()))

    rows = []
    for name, z in ref.items():
        rows.append({"particion": name, **evaluar(z, incid, nodes)})
    parts = {}
    for name, e in variantes.items():
        sub = {k: v for k, v in e.items() if k[0] in nset and k[1] in nset}
        for etiqueta, res in (("res. 1,0", 1.0),):
            z = particion_louvain(sub, nodes, res)
            parts[(name, etiqueta)] = z
            rows.append({"particion": f"Louvain {name}, {etiqueta}", **evaluar(z, incid, nodes)})
        for objetivo, etiqueta in ((n_lineas, "como lineas"), (n_cab, "como cabeceras")):
            r, n = resolucion_para(sub, nodes, objetivo)
            z = particion_louvain(sub, nodes, r)
            parts[(name, etiqueta)] = z
            rows.append({"particion": f"Louvain {name}, {etiqueta} (res. {r:.2f})", **evaluar(z, incid, nodes)})
    res = pd.DataFrame(rows)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 20)
    p(res.round(1).to_string(index=False))
    p("")

    # Acuerdo entre particiones (ARI) para leer que encuentran las comunidades
    from sklearn.metrics import adjusted_rand_score as ari
    lin = [ref["Linea (grupo_red_id)"][n] for n in nodes]
    cab = [ref["Cabecera argmax (historico completo)"][n] for n in nodes]
    for (name, etiqueta), z in parts.items():
        lab = [z[n] for n in nodes]
        p(f"ARI {name}, {etiqueta}: con lineas {ari(lin, lab):.2f}; con cabeceras {ari(cab, lab):.2f}")
    p(f"ARI lineas - cabeceras: {ari(lin, cab):.2f}")
    p(f"Tiempo total: {time.perf_counter() - t0:.1f} s")
    txt = "\n".join(lines)
    print(txt)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(txt + "\n")


if __name__ == "__main__":
    main()
