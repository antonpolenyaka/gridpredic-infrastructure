"""Materializar o no, y el contexto de un CT para un asistente.

E. Materialización (caso EUDAT y "frequently bought together"): cuántas aristas CT - CT saldrían
   de proyectar las incidencias de Calser y los eventos del SCADA, frente a las filas de la relación
   con el evento como nodo (la "cesta" de la clase), y lo que tarda contestar "con quién se corta
   este CT" sin materializar.
F. Contexto para un asistente tipo GraphRAG: el subgrafo que habría que recuperar para explicar por
   que un CT está en riesgo (CT, línea, cabecera, elementos aguas arriba, celda, municipio,
   incidencias y sus causas, compañeros de corte), con su tamaño por CT.

Uso: python materializar_contexto.py <carpeta zonas_20261013_1041> <carpeta rutas_20261014> <salida>
"""
import itertools
import json
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

T0 = time.time()
BASE = Path(sys.argv[1])
RUTAS = Path(sys.argv[2])
SALIDA = Path(sys.argv[3])
SALIDA.mkdir(parents=True, exist_ok=True)
lineas = []


def out(txt=""):
    print(txt)
    lineas.append(str(txt))


dim = pd.read_parquet(BASE / "dim_ct.parquet")
mapa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
itr = pd.read_parquet(BASE / "interrupciones.parquet")
cs = pd.read_parquet(BASE / "cortes_scada.parquet")
celda = pd.read_parquet(BASE / "map_ct_celda.parquet")

LOCALES = {"equipo", "fauna", "desconocida", "meteorologia", "terceros", "vegetacion", "objeto_ajeno",
           "incendio", "agente_externo", "sin_causa"}

# ---------------------------------------------------------------- E. materialización
out("## E. Materializar la relacion CT - CT o dejar el evento como nodo")


def proyectar(grupos):
    pares = Counter()
    filas = 0
    for cts in grupos:
        cts = sorted(set(cts))
        filas += len(cts)
        for a, b in itertools.combinations(cts, 2):
            pares[(a, b)] += 1
    return filas, pares


itr["clave"] = itr.distribuidora_id.astype(str) + "_" + itr.periodo_id.astype(str) + "_" + itr.incidencia_id.astype(str)
itr["ct"] = itr.distribuidora_id.astype(str) + "_" + itr.ct_id.astype(str)
cal = itr[(itr.en_estudio) & (itr.inicio_ts >= "2016-01-01") & (itr.causa.isin(LOCALES))]
t = time.time()
filas_c, pares_c = proyectar(cal.groupby("clave").ct.apply(list))
tc = time.time() - t
n_ct_c = cal.ct.nunique()
out(f"Calser, incidencias de causa local desde 2016 en CT del estudio: {cal.clave.nunique()} incidencias, "
    f"{filas_c:,} filas incidencia - CT, {n_ct_c} CT; proyeccion CT - CT: {len(pares_c):,} aristas "
    f"({len(pares_c) / (n_ct_c * (n_ct_c - 1) / 2):.1%} de todos los pares posibles), {tc:.2f} s")
mayor = cal.groupby("clave").ct.nunique().max()
out(f"  la incidencia mas grande corta {mayor} CT y aporta ella sola {mayor * (mayor - 1) // 2:,} pares")

cs["ct"] = cs.distribuidora_id.astype(str) + "_" + cs.ct_id.astype(str)
t = time.time()
filas_s, pares_s = proyectar(cs.groupby("evento_off_id").ct.apply(list))
ts = time.time() - t
n_ct_s = cs.ct.nunique()
out(f"SCADA, eventos de corte desde 2021: {cs.evento_off_id.nunique():,} eventos, {filas_s:,} filas evento - trafo, "
    f"{n_ct_s} trafos; proyeccion trafo - trafo: {len(pares_s):,} aristas ({len(pares_s) / (n_ct_s * (n_ct_s - 1) / 2):.1%} "
    f"de los pares posibles), {ts:.2f} s")
out(f"map_aguas_arriba (relacion ya materializada trafo - elemento): {len(mapa):,} aristas")

# consulta sin materializar: los compañeros de corte de un CT leyendo la tabla con el evento como nodo
por_ct = cal.groupby("ct").clave.apply(set).to_dict()
por_inc = cal.groupby("clave").ct.apply(set).to_dict()
rng = np.random.default_rng(12)
muestra = rng.choice(list(por_ct), size=min(500, len(por_ct)), replace=False)
t = time.time()
for c in muestra:
    comp = Counter()
    for k in por_ct[c]:
        comp.update(por_inc[k] - {c})
tq = (time.time() - t) / len(muestra) * 1e3
out(f"  'con quien se corta este CT' sin materializar, desde los indices CT -> incidencias -> CT: {tq:.3f} ms por consulta")

# las rutas de OSRM: la matriz de minutos ya materializada (el isConnectedTo con distancia de EUDAT)
for f in sorted(RUTAS.glob("*.json")):
    d = json.loads(f.read_text(encoding="utf-8"))
    n = len(d["municipios"])
    mins = np.array(d["minutos"], dtype=float)
    red_rutas = {"3": "Distribuidora 1", "2": "Distribuidora 2", "1366": "Distribuidora 3"}.get(f.stem.split("_")[0], "otra red")
    out(f"rutas de la {red_rutas}: {n} municipios, matriz {n} x {n} ({n * (n - 1) // 2} pares), pedida el "
        f"{d['pedido_utc'][:10]}; minutos entre municipios mediana {np.nanmedian(mins[np.triu_indices(n, 1)]):.0f}, "
        f"max {np.nanmax(mins):.0f}")
out()

# ---------------------------------------------------------------- F. contexto de un CT
out("## F. El subgrafo que explicaria el riesgo de un CT")
est = dim[dim.en_estudio].copy()
est["ct"] = est.distribuidora_id.astype(str) + "_" + est.ct_id.astype(str)
celda["ct"] = celda.distribuidora_id.astype(str) + "_" + celda.ct_id.astype(str)
celda_de = dict(zip(celda.ct, celda.celda_id))
cts_linea = est.groupby("grupo_red_id").ct.apply(list).to_dict()
aa = mapa.copy()
aa["ct"] = aa.distribuidora_id.astype(str) + "_" + aa.ct_id.astype(str)
aa_de = aa.groupby("ct").elemento_corte_id.apply(list).to_dict()
cal_ct = cal.groupby("ct").agg(incs=("clave", lambda s: sorted(set(s))), causas=("causa", lambda s: sorted(set(s)))).to_dict("index")

t = time.time()
filas = []
for r in est.itertuples():
    nodos = {("ct", r.ct)}
    aristas = 0
    # un salto: línea, cabecera, municipio, celda, elementos aguas arriba, incidencias y sus causas
    for tipo, valor in [("linea", r.grupo_red_id), ("cabecera", r.cabecera), ("municipio", r.municipio_id),
                        ("celda", celda_de.get(r.ct))]:
        if pd.notna(valor):
            nodos.add((tipo, valor))
            aristas += 1
    for e in aa_de.get(r.ct, []):
        nodos.add(("elemento", e))
        aristas += 1
    info = cal_ct.get(r.ct, {"incs": [], "causas": []})
    for k in info["incs"]:
        nodos.add(("incidencia", k))
        aristas += 1
    for c in info["causas"]:
        nodos.add(("causa", c))
    aristas += len(info["incs"])  # incidencia -> causa
    n1, a1 = len(nodos), aristas
    # dos saltos: los CT de su línea y los compañeros de corte de sus incidencias
    vecinos = set(cts_linea.get(r.grupo_red_id, [])) - {r.ct}
    for k in info["incs"]:
        vecinos |= por_inc[k] - {r.ct}
    nodos |= {("ct", v) for v in vecinos}
    aristas += len(set(cts_linea.get(r.grupo_red_id, [])) - {r.ct}) + sum(len(por_inc[k]) - 1 for k in info["incs"])
    filas.append({"ct": r.ct, "nodos_1": n1, "aristas_1": a1, "nodos_2": len(nodos), "aristas_2": aristas,
                  "incidencias": len(info["incs"]), "elementos_aa": len(aa_de.get(r.ct, []))})
tf = time.time() - t
ctx = pd.DataFrame(filas)
# sin códigos reales de CT en la salida: solo la red y un número de orden
ctx.insert(0, "red", ctx.ct.str.split("_").str[0].map({"3": "D1", "2": "D2", "1366": "D3"}))
ctx.drop(columns="ct").to_csv(SALIDA / "contexto_ct.csv", index_label="n")
q = ctx[["nodos_1", "aristas_1", "nodos_2", "aristas_2", "incidencias", "elementos_aa"]].quantile([.5, .95, 1]).round(0)
out(q.rename(index={.5: "mediana", .95: "p95", 1: "maximo"}).to_string())
out(f"CT {len(ctx)}; CT sin ninguna incidencia de causa local desde 2016: {(ctx.incidencias == 0).sum()}; "
    f"tiempo para los {len(ctx)} CT: {tf:.2f} s")
out()
out(f"tiempo total {time.time() - T0:.1f} s")
(SALIDA / "materializar_contexto.txt").write_text("\n".join(lineas) + "\n", encoding="utf-8")
