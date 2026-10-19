"""
El grafo de propiedades de GridPredic leído como tablas desde la exportación de Gold (la de zonas, sin servidor de
grafos ni networkx): cada etiqueta de nodo es un conjunto de identificadores y cada tipo de arista una tabla con sus
dos extremos, y los recorridos se resuelven con joins de pandas.

  (:CT)-[:AGUAS_ARRIBA {n_cortes, primer_corte_ts, ultimo_corte_ts, primer_conocido_ts}]->(:ElementoCorte)
  (:ElementoCorte)-[:EN_POSICION]->(:Posicion)
  (:CT)-[:EN_CENTRO]->(:Centro)-[:CUELGA_DE]->(:Grupo)-[:DE_RED]->(:Distribuidora)
  (:CT)-[:EN_CELDA]->(:Celda), (:CT)-[:EN_MUNICIPIO]->(:Municipio), (:CT)-[:EN_ZONA]->(:TipoZona)
  (:Incidencia)-[:AFECTA {inicio_ts, fin_ts, duracion_s, periodo_id}]->(:CT)        (multiarista)
  (:Incidencia)-[:POR_CAUSA]->(:Causa), (:Incidencia)-[:DESPEJADA_POR]->(:ElementoCorte)
  (:CorteScada)-[:DEJA_SIN_TENSION {inicio_ts, fin_ts, inicio_conocido_ts, es_microcorte}]->(:CT)

Centro, Posicion y ElementoCorte son elementos del SCADA: un identificador que hace dos papeles (40 son a la vez
centro y posición) es un solo nodo con dos etiquetas. La subestación no está en la exportación (haría falta
SystemElements); la posición es la celda de la subestación donde están las protecciones del elemento de corte.
"""

from pathlib import Path

import numpy as np
import pandas as pd

APAGON = pd.Timestamp("2025-04-28")
TABLES = ["dim_ct", "map_aguas_arriba", "interrupciones", "incidencias", "incidencias_ex", "map_ct_celda",
          "cortes_scada"]
NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}


def ct_key(distribuidora, ct):
    return "CT:" + pd.Series(distribuidora).astype(str).values + ":" + pd.Series(ct).astype(str).values


def incident_key(frame, column):
    return ("IN:" + frame.distribuidora_id.astype(str) + "|" + frame.periodo_id.astype(str) + "|"
            + frame[column].astype(str))


def cargar(base) -> dict:
    base = Path(base)
    t = {name: pd.read_parquet(base / f"{name}.parquet") for name in TABLES}
    t["dim_ct"] = t["dim_ct"].drop(columns=["municipio_nombre", "cabecera", "source_database"], errors="ignore")
    it = t["interrupciones"]
    it["inc"] = incident_key(it, "incidencia_id")
    it["ct"] = ct_key(it.distribuidora_id, it.ct_id)
    aa = t["map_aguas_arriba"]
    aa["ct"] = ct_key(aa.distribuidora_id, aa.ct_id)
    aa["el"] = "EC:" + aa.elemento_corte_id.astype(str)
    t["dim_ct"]["ct"] = ct_key(t["dim_ct"].distribuidora_id, t["dim_ct"].ct_id)
    t["cortes_scada"]["ct"] = ct_key(t["cortes_scada"].distribuidora_id, t["cortes_scada"].ct_id)
    t["incidencias"]["inc"] = incident_key(t["incidencias"], "id")
    t["incidencias_ex"]["inc"] = incident_key(t["incidencias_ex"], "id")
    return t


def red(distribuidora_id) -> str:
    return NETWORKS.get(int(distribuidora_id), "otra")


def norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)


def cabecera_calser(t) -> pd.DataFrame:
    """
    (inc, el): el elemento de corte del SCADA cuyo nombre coincide con el código de cabecera que Calser guarda en
    incidencias_ex para esa incidencia (misma distribuidora): la arista DESPEJADA_POR.
    """
    aa = t["map_aguas_arriba"]
    names = aa[["distribuidora_id", "el", "elemento_corte_nombre"]].drop_duplicates()
    names["n"] = norm(names.elemento_corte_nombre)
    ex = t["incidencias_ex"]
    ex = ex[ex.codigo_cabecera.notna()][["distribuidora_id", "inc", "codigo_cabecera"]].copy()
    ex["n"] = norm(ex.codigo_cabecera)
    return ex.merge(names[["distribuidora_id", "n", "el"]], on=["distribuidora_id", "n"])[["inc", "el"]].drop_duplicates()


def inventario(t) -> tuple:
    """Nodos por etiqueta, nodos distintos (un identificador del SCADA con dos papeles cuenta una vez) y aristas por tipo."""
    dim, aa, it, cs = t["dim_ct"], t["map_aguas_arriba"], t["interrupciones"], t["cortes_scada"]
    centres = set(dim.anchor_id.dropna().astype("int64"))
    positions = set(aa.posicion_id.dropna().astype("int64"))
    elements = set(aa.elemento_corte_id.astype("int64"))
    causes = it.groupby("inc").causa.first().dropna()
    with_centre = dim[dim.anchor_id.notna()]
    groups = with_centre[with_centre.grupo_red_id.notna()]
    labels = pd.Series({
        "CorteScada": cs.evento_off_id.nunique(), "CT": dim.ct.nunique(), "Centro": len(centres),
        "Incidencia": it.inc.nunique(), "ElementoCorte": len(elements), "Posicion": len(positions),
        "Municipio": dim.municipio_id.nunique(), "Grupo": groups.grupo_red_id.nunique(),
        "Celda": t["map_ct_celda"].celda_id.nunique(), "Causa": causes.nunique(),
        "TipoZona": dim.tipo_zona_estat.dropna().nunique(), "Distribuidora": dim.distribuidora_id.nunique(),
    }).sort_values(ascending=False)
    scada = centres | positions | elements
    distinct = int(labels.sum() - (len(centres) + len(positions) + len(elements) - len(scada)))
    edges = pd.Series({
        "DEJA_SIN_TENSION": len(cs), "AFECTA": len(it), "AGUAS_ARRIBA": len(aa), "EN_MUNICIPIO": len(dim),
        "EN_ZONA": int(dim.tipo_zona_estat.notna().sum()), "EN_CELDA": len(t["map_ct_celda"]),
        "EN_CENTRO": len(with_centre),
        "CUELGA_DE": len(groups[["anchor_id", "grupo_red_id"]].drop_duplicates()),
        "POR_CAUSA": len(causes), "DESPEJADA_POR": int(cabecera_calser(t).inc.isin(it.inc).sum()),
        "EN_POSICION": len(aa[["el", "posicion_id"]].drop_duplicates()),
        "DE_RED": len(groups[["grupo_red_id"]].drop_duplicates()),
    }).sort_values(ascending=False)
    return labels, distinct, len(centres & positions), edges


def incidencia_matrix(aa: pd.DataFrame, cts: list, els: list):
    """Matriz de incidencia CT x ElementoCorte (0/1), como la de las diapositivas."""
    from scipy import sparse

    ci = {c: i for i, c in enumerate(cts)}
    ei = {e: j for j, e in enumerate(els)}
    return sparse.csr_matrix((np.ones(len(aa)), (aa.ct.map(ci).to_numpy(), aa.el.map(ei).to_numpy())),
                             shape=(len(cts), len(els)))


def componentes_conexas(origen, destino) -> list:
    """Componentes conexas del grafo no dirigido con las aristas (origen[i], destino[i]), como conjuntos de nodos."""
    from scipy import sparse
    from scipy.sparse.csgraph import connected_components

    codes, nodes = pd.factorize(pd.concat([pd.Series(list(origen)), pd.Series(list(destino))], ignore_index=True))
    half = len(codes) // 2
    graph = sparse.coo_matrix((np.ones(half), (codes[:half], codes[half:])), shape=(len(nodes), len(nodes)))
    _, labels = connected_components(graph, directed=False)
    return [set(nodes[labels == k]) for k in pd.unique(labels)]
