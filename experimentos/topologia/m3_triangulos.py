"""M3. Triangulos de co-corte y anidamiento de los conjuntos de corte del SCADA.

En una red radial con la configuración fija, cada evento de corte deja sin tensión todo lo que
cuelga de la protección que abre, así que dos eventos cortan conjuntos de trafos disjuntos o uno
dentro del otro (familia laminar). Un triángulo de co-corte (a, b, c) que ningún evento cierra
entero obliga a que dos eventos se crucen: a solo en el primero, b en los dos, c solo en el segundo.

En GraphFrames (sin ejecutar):
  pares = g.find("(ev)-[]->(a); (ev)-[]->(b)").filter("a.id < b.id")       # proyección de co-corte
  gc = GraphFrame(trafos, pares.groupBy("a.id", "b.id").count()...)
  gc.triangleCount.run()                                                    # triángulos por vértice
  cruce = g.find("(e1)-[]->(a); (e1)-[]->(b); (e2)-[]->(b); (e2)-[]->(c); !(e1)-[]->(c); !(e2)-[]->(a)")

Aquí: matriz de incidencia evento x trafo (scipy), intersección de todos los pares de eventos con
un producto disperso y clasificación de cada par que se solapa. Los triángulos de la proyección se
cuentan con el producto de matrices; los abiertos, con conjuntos de bits por trafo.

Uso: python m3_triangulos.py <carpeta zonas_20261013_1041> <carpeta de salida>
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

T0 = time.time()
BASE, SALIDA = Path(sys.argv[1]), Path(sys.argv[2])
SALIDA.mkdir(parents=True, exist_ok=True)
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}
APAGON = pd.Timestamp("2025-04-28")

lineas = []


def out(txt=""):
    print(txt)
    lineas.append(str(txt))


cs = pd.read_parquet(BASE / "cortes_scada.parquet")
cs = cs[cs.inicio_ts.dt.normalize() != APAGON]  # el apagón vino de fuera de TedisNet
cs["red"] = cs.distribuidora_id.map(NOMBRE)

ev = cs.groupby("evento_off_id").agg(ini=("inicio_ts", "min"), fin=("fin_ts", "max"), n=("elemento_id", "nunique"),
                                      redes=("red", "nunique"), micro=("es_microcorte", "mean"),
                                      abierto=("abierto", "max"))
ev = ev.sort_values("ini")
ev["i"] = np.arange(len(ev))
trafos = np.sort(cs.elemento_id.unique())
col = {t: k for k, t in enumerate(trafos)}
filas = ev.i.reindex(cs.evento_off_id).values
cols = np.array([col[t] for t in cs.elemento_id])
M = sparse.csr_matrix((np.ones(len(cs), dtype=np.int32), (filas, cols)), shape=(len(ev), len(trafos)))
s = np.asarray(M.sum(axis=1)).ravel()
out(f"eventos {len(ev):,} (sin el apagon), trafos {len(trafos):,}, filas evento-trafo {M.nnz:,}")

# ---------------------------------------------------------------- proyección y triángulos
t1 = time.time()
A = (M.T @ M).toarray().astype(np.int64)  # trafo x trafo: eventos que cortan a los dos
np.fill_diagonal(A, 0)
pares = int((A > 0).sum() // 2)
B = (A > 0).astype(np.float64)
tri_total = int(round(np.trace(B @ B @ B) / 6))
out(f"proyeccion de co-corte: {pares:,} aristas ({pares / (len(trafos) * (len(trafos) - 1) / 2):.1%} de los pares), "
    f"triangulos {tri_total:,} ({time.time() - t1:.2f} s)")
for k in (2, 5, 10, 20):
    Bk = (A >= k).astype(np.float64)
    pk = int(Bk.sum() // 2)
    tk = int(round(np.trace(Bk @ Bk @ Bk) / 6))
    out(f"  aristas con {k} eventos comunes o mas: {pk:,}; triangulos {tk:,}")

# ---------------------------------------------------------------- pares de eventos
t1 = time.time()
grandes = np.where(s >= 2)[0]
Mg = M[grandes]
solapes = sparse.triu(Mg @ Mg.T, k=1).tocoo()
i, j, inter = grandes[solapes.row], grandes[solapes.col], solapes.data
smin = np.minimum(s[i], s[j])
anidado = inter == smin
iguales = (inter == s[i]) & (inter == s[j])
ini = ev.ini.values
fin = ev.fin.values
# solape temporal: el evento posterior empieza antes de que vuelva el último trafo del anterior
primero = np.where(ini[i] <= ini[j], i, j)
segundo = np.where(ini[i] <= ini[j], j, i)
fin_primero = fin[primero]
solapado = pd.isna(fin_primero) | (ini[segundo] < fin_primero)
gap_dias = (ini[segundo] - ini[primero]) / np.timedelta64(1, "D")
out(f"pares de eventos (con 2 trafos o mas) que se solapan en algun trafo: {len(i):,} ({time.time() - t1:.2f} s)")
out(f"  anidados {anidado.mean():.1%} (identicos {iguales.mean():.1%}), cruzados {(~anidado).mean():.1%}")
ok = ~solapado
out(f"  sin solape temporal entre los dos eventos: {ok.sum():,}; cruzados {(~anidado[ok]).mean():.1%}")
tabla = pd.DataFrame({"anidado": anidado[ok], "gap": gap_dias[ok]})
tabla["tramo"] = pd.cut(tabla.gap, [-0.01, 1, 30, 365, 730, 10_000],
                        labels=["< 1 día", "1 - 30 días", "30 días - 1 año", "1 - 2 años", "> 2 años"])
res = tabla.groupby("tramo", observed=True).agg(pares=("anidado", "size"), cruzados=("anidado", lambda x: (~x).mean()))
res["cruzados"] = res.cruzados.map(lambda x: f"{x:.1%}")
out(res.to_string())

# eventos implicados en algún cruce (sin solape temporal) y su tamaño
cruz_ev = np.unique(np.concatenate([i[ok & ~anidado], j[ok & ~anidado]]))
out(f"eventos con algun cruce: {len(cruz_ev):,} de {len(grandes):,} con 2 trafos o mas; "
    f"tamaño mediano {np.median(s[cruz_ev]):.0f} frente a {np.median(s[grandes]):.0f}")
red_ev = ev.redes.values
out(f"  de ellos con trafos de dos redes: {(red_ev[cruz_ev] >= 2).sum()}")
# un cruce típico: cuánto se sale cada evento del otro
fuera = np.minimum(s[i] - inter, s[j] - inter)[ok & ~anidado]
out(f"  en los pares cruzados, trafos del evento menor que quedan fuera del mayor: mediana {np.median(fuera):.0f}, "
    f"p90 {np.percentile(fuera, 90):.0f}; parte del menor fuera: mediana "
    f"{np.median(fuera / smin[ok & ~anidado]):.1%}")

# instancias que devolvería el motivo con negaciones (a solo en e1, b en los dos, c solo en e2), en los dos
# ordenes de (e1, e2), porque find() no quita simétricos
cz = ok & ~anidado
inst = 2 * ((s[i] - inter) * inter * (s[j] - inter))[cz].astype(np.float64)
out(f"  instancias del motivo de cruce (con negaciones) que enumeraria find(): {inst.sum():.3g}")

# por red: pares de eventos de una sola red
red_unica = np.where(ev.redes.values == 1, cs.groupby("evento_off_id").red.first().reindex(ev.index).values, "mixta")
for r in ("D1", "D2", "D3"):
    sel = ok & (red_unica[i] == r) & (red_unica[j] == r)
    if sel.sum():
        out(f"  pares de eventos solo de {r}: {sel.sum():,}; cruzados {int((~anidado[sel]).sum()):,} "
            f"({(~anidado[sel]).mean():.2%})")

# control: solo los trafos que el SCADA corta desde el primer semestre de 2021 hasta 2026, para que
# un trafo que entra o sale de la observación no fabrique cruces
vida = cs.groupby("elemento_id").inicio_ts.agg(["min", "max"])
estables = vida[(vida["min"] < "2021-07-01") & (vida["max"] >= "2026-01-01")].index
ce = np.array([col[t] for t in estables])
Ms = M[:, ce]
ss = np.asarray(Ms.sum(axis=1)).ravel()
gs = np.where(ss >= 2)[0]
Os = sparse.triu(Ms[gs] @ Ms[gs].T, k=1).tocoo()
i2, j2, inter2 = gs[Os.row], gs[Os.col], Os.data
an2 = inter2 == np.minimum(ss[i2], ss[j2])
p2 = np.where(ini[i2] <= ini[j2], i2, j2)
q2 = np.where(ini[i2] <= ini[j2], j2, i2)
ok2 = ~(pd.isna(fin[p2]) | (ini[q2] < fin[p2]))
gap2 = (ini[q2] - ini[p2]) / np.timedelta64(1, "D")
t2 = pd.DataFrame({"anidado": an2[ok2], "gap": gap2[ok2]})
t2["tramo"] = pd.cut(t2.gap, [-0.01, 1, 30, 365, 730, 10_000],
                     labels=["< 1 día", "1 - 30 días", "30 días - 1 año", "1 - 2 años", "> 2 años"])
r2 = t2.groupby("tramo", observed=True).agg(pares=("anidado", "size"), cruzados=("anidado", lambda x: (~x).mean()))
r2["cruzados"] = r2.cruzados.map(lambda x: f"{x:.1%}")
out(f"control con {len(estables)} trafos presentes en toda la ventana (eventos con 2 o mas de ellos: {len(gs):,}):")
out(r2.to_string())

# ---------------------------------------------------------------- triángulos abiertos en el grafo fuerte
t1 = time.time()
K = 5
Bk = (A >= K)
bits = []
Mc = M.tocsc()
for c in range(len(trafos)):
    b = 0
    for r in Mc.indices[Mc.indptr[c]:Mc.indptr[c + 1]]:
        b |= 1 << int(r)
    bits.append(b)
vecinos = [set(np.where(Bk[a])[0]) for a in range(len(trafos))]
abiertos = cerrados = 0
for a in range(len(trafos)):
    for b in vecinos[a]:
        if b <= a:
            continue
        comunes = vecinos[a] & vecinos[b]
        ab = bits[a] & bits[b]
        for c in comunes:
            if c <= b:
                continue
            if ab & bits[c]:
                cerrados += 1
            else:
                abiertos += 1
tot = abiertos + cerrados
out(f"triangulos del grafo con aristas de {K} eventos comunes o mas: {tot:,}; ningun evento corta los tres en "
    f"{abiertos:,} ({abiertos / max(tot, 1):.2%}) ({time.time() - t1:.1f} s)")

out(f"tiempo total {time.time() - T0:.1f} s")
(SALIDA / "m3_triangulos.txt").write_text("\n".join(lineas) + "\n", encoding="utf-8")
