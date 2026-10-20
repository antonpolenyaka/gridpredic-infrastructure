"""
E0. Inventario del grafo de propiedades de GridPredic y coste de las alternativas de reificación (memoria, 4.4.4):

- Nodos por etiqueta y aristas por tipo del grafo leído de la exportación de Gold (grafo_gp.py).
- Incidencia (y CorteScada) como nodo reificado con una arista por CT, frente a materializar el co-corte como
  aristas CT - CT (una por par de CT de la misma incidencia o evento).
- Tamaño del almacén de Neo4j con los registros fijos de las diapositivas (15 bytes por nodo y 34 por relación),
  sin propiedades.

    python e0_modelo.py <carpeta de la exportación> [salida/]
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

import grafo_gp as gg  # noqa: E402

OUT = sys.argv[2] if len(sys.argv) > 2 else None
lines = []


def say(*parts):
    text = " ".join(str(p) for p in parts)
    print(text)
    lines.append(text)


t0 = time.perf_counter()
t = gg.cargar(sys.argv[1])
t_carga = time.perf_counter() - t0
nodos, n_nodos, dobles, aristas = gg.inventario(t)
n_aristas = int(aristas.sum())
say(f"carga {t_carga:.2f} s")
say(f"nodos {n_nodos:,} ({int(nodos.sum()):,} por etiqueta: {dobles} identificadores del SCADA son a la vez centro y "
    f"posición), aristas {n_aristas:,}")
say("\nnodos por etiqueta\n" + nodos.to_string())
say("\naristas por tipo\n" + aristas.to_string())

nodos_b = n_nodos * 15
rel_b = n_aristas * 34
say(f"\nalmacén de Neo4j sin propiedades: nodos {nodos_b/1e6:.2f} MB + relaciones {rel_b/1e6:.2f} MB")
for nombre, n, r in [("topología eléctrica (SystemNodes, SystemBranches)", 12307, 11449),
                     ("simulación con baja tensión (SimulaNodes, SimulaBranches)", 1146469, 1077248)]:
    say(f"  {nombre}: {n*15/1e6:.2f} MB + {r*34/1e6:.2f} MB")

# --- reificación: estrella frente a pares ---------------------------------------
it = t["interrupciones"]
dist = it.drop_duplicates(["inc", "ct"])
n_por_inc = dist.groupby("inc").ct.nunique()
pares = int((n_por_inc * (n_por_inc - 1) // 2).sum())
apagon_inc = it[it.inicio_ts.dt.normalize() == gg.APAGON].inc.unique()
n_sin = n_por_inc.drop(apagon_inc, errors="ignore")
pares_sin = int((n_sin * (n_sin - 1) // 2).sum())
say("\nIncidencias principales (2010 - 2026) con CT:", len(n_por_inc))
say(f"  aristas AFECTA (filas): {len(it):,}; pares (incidencia, CT) distintos: {len(dist):,}")
say(f"  filas repetidas de la misma incidencia y CT (multiarista): {len(it) - len(dist):,}")
say(f"  pares CT - CT si se materializa el co-corte: {pares:,}; sin el apagon del 28.04.2025: {pares_sin:,}")
codes = pd.DataFrame({"inc": pd.factorize(dist.inc)[0].astype("int32"), "ct": pd.factorize(dist.ct)[0].astype("int32")})
pp = codes.merge(codes, on="inc")
pp = pp[pp.ct_x < pp.ct_y][["ct_x", "ct_y"]].drop_duplicates()
say(f"  pares CT - CT distintos (sumando todas las incidencias): {len(pp):,}")
say(f"  incidencias del apagon: {len(apagon_inc)}, CT en ellas: {int(n_por_inc.loc[apagon_inc].sum())}")

cs = t["cortes_scada"]
n_ev = cs.groupby("evento_off_id").ct.nunique()
pares_cs = int((n_ev * (n_ev - 1) // 2).sum())
say(f"\nEventos Off del SCADA: {len(n_ev):,}; aristas DEJA_SIN_TENSION: {len(cs):,}")
say(f"  pares CT - CT si se materializa el co-corte del SCADA: {pares_cs:,}")
say(f"  evento con mas CT: {int(n_ev.max())}; mediana {int(n_ev.median())}")

# --- arista derivada CABECERA (argmax de n_cortes) --------------------------------
aa = t["map_aguas_arriba"]
top = aa.sort_values(["ct", "n_cortes", "el"], ascending=[True, False, True]).drop_duplicates("ct")
say(f"\nArista derivada CABECERA (argmax de n_cortes por CT): {len(top):,} aristas hacia "
      f"{top.el.nunique()} elementos")
empates = aa.merge(top[["ct", "n_cortes"]], on=["ct", "n_cortes"]).groupby("ct").size()
say(f"  CT con empate en el maximo de n_cortes: {int((empates > 1).sum())}")

if OUT:
    Path(OUT).mkdir(parents=True, exist_ok=True)
    (Path(OUT) / "e0_modelo.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
