"""
Carga común para los experimentos de predicción de enlaces y de embeddings de grafo. Lee la exportación de
Gold del 13.10.2026 (zonas_20261013_1041).

- CT del estudio (dim_ct.en_estudio), con red, grupo, cabecera, municipio,
  celda y coordenadas del municipio.
- Incidencias imprevistas de Calser desde 2016 (antes Calser mezclaba cortes
  de menos de 180 s), sin el apagón del 28.04.2025, con los CT que cortaron.
- map_aguas_arriba con el CT de cada trafo.

Distribuidoras: 3 es la Distribuidora 1, 2 la Distribuidora 2 y 1366 la 3.
"""
import os
import sys

import numpy as np
import pandas as pd

# La carpeta de la exportación de Gold (zonas_20261013_1041 en _runlogs/ml_resultados del portátil): el primer
# argumento de cada guion o la variable GRIDPREDIC_EXPORTACION. El segundo argumento es la carpeta de salida.
DIR = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("GRIDPREDIC_EXPORTACION", "")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")
RED = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
FAMILIAS = {"meteo": ["meteorologia", "desconocida", "vegetacion", "incendio", "objeto_ajeno"],
            "fauna": ["fauna"], "equipo": ["equipo"]}  # como riesgo_ct.familias_causa; el resto va a "resto"
FIN_DATOS = pd.Timestamp("2026-08-03")  # último día con etiqueta de la copia de Calser: 02.08.2026


def salida(nombre):
    """Ruta de un fichero de resultados en la carpeta de salida."""
    os.makedirs(OUT, exist_ok=True)
    return os.path.join(OUT, nombre)


def familia(causa):
    for k, v in FAMILIAS.items():
        if causa in v:
            return k
    return "resto"


def cargar():
    dim = pd.read_parquet(f"{DIR}/dim_ct.parquet")
    dim = dim[dim.en_estudio].reset_index(drop=True)
    cel = pd.read_parquet(f"{DIR}/map_ct_celda.parquet")[["distribuidora_id", "ct_id", "celda_id",
                                                          "ct_lat_municipio", "ct_lon_municipio"]]
    dim = dim.merge(cel, on=["distribuidora_id", "ct_id"], how="left")
    dim["vigente_desde"] = pd.to_datetime(dim.vigente_desde)
    dim["vigente_hasta_excl"] = pd.to_datetime(dim.vigente_hasta_excl)
    dim["kva"] = dim.potencia_tiepi_kva.fillna(0.0)
    idx = {(d, c): i for i, (d, c) in enumerate(zip(dim.distribuidora_id, dim.ct_id))}

    it = pd.read_parquet(f"{DIR}/interrupciones.parquet")
    it = it[(it.inicio_ts >= "2016-01-01") & (it.causa != "apagon") &
            (it.inicio_ts.dt.normalize() != pd.Timestamp("2025-04-28"))]
    it = it.assign(node=[idx.get((d, c), -1) for d, c in zip(it.distribuidora_id, it.ct_id)])
    it = it[it.node >= 0].copy()
    it["anio"] = it.inicio_ts.dt.year
    it["familia"] = it.causa.map(familia)
    it["kva_h"] = dim.kva.values[it.node.values] * it.duracion_s.clip(lower=0, upper=72 * 3600) / 3600.0
    key = ["distribuidora_id", "periodo_id", "incidencia_id"]
    g = it.groupby(key)
    inc = g.agg(inicio=("inicio_ts", "min"), causa=("causa", "first")).reset_index()
    inc["nodes"] = list(g.node.apply(lambda s: np.unique(s.values)).values)
    inc["n"] = inc.nodes.apply(len)
    inc["anio"] = inc.inicio.dt.year
    inc = inc.sort_values("inicio").reset_index(drop=True)

    aa = pd.read_parquet(f"{DIR}/map_aguas_arriba.parquet")
    aa = aa.assign(node=[idx.get((d, c), -1) for d, c in zip(aa.distribuidora_id, aa.ct_id)])
    aa = aa[aa.node >= 0].copy()
    return dim, it, inc, aa


def activos(dim, desde, hasta):
    """CT vigentes en todo o parte de [desde, hasta)."""
    d = dim.vigente_desde.fillna(pd.Timestamp("1900-01-01"))
    h = dim.vigente_hasta_excl.fillna(pd.Timestamp("2100-01-01"))
    return ((d < pd.Timestamp(hasta)) & (h > pd.Timestamp(desde))).values


def grafo(inc, n, desde, hasta, min_n=2):
    """W: número de incidencias que cortaron juntos a cada par; L: último año en que lo hicieron (0 si nunca)."""
    W = np.zeros((n, n), dtype=np.float32)
    L = np.zeros((n, n), dtype=np.int16)
    sel = inc[(inc.inicio >= pd.Timestamp(desde)) & (inc.inicio < pd.Timestamp(hasta)) & (inc.n >= min_n)]
    for nodes, y in zip(sel.nodes, sel.anio):
        ix = np.ix_(nodes, nodes)
        W[ix] += 1
        L[ix] = y
    np.fill_diagonal(W, 0)
    np.fill_diagonal(L, 0)
    return W, L, sel


def grafo_decay(inc, n, hasta, tau, desde="2016-01-01"):
    """Suma de exp(-edad/tau) de las incidencias que cortaron juntos a cada par, edad en años hasta 'hasta'."""
    D = np.zeros((n, n), dtype=np.float32)
    ref = pd.Timestamp(hasta)
    sel = inc[(inc.inicio >= pd.Timestamp(desde)) & (inc.inicio < ref) & (inc.n >= 2)]
    for nodes, t in zip(sel.nodes, sel.inicio):
        edad = (ref - t).days / 365.25
        D[np.ix_(nodes, nodes)] += np.exp(-edad / tau)
    np.fill_diagonal(D, 0)
    return D
