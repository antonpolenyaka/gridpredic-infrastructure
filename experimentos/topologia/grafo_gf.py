"""Grafo de GridPredic como lo pediría GraphFrames: un DataFrame de vértices (id, tipo, ...) y otro de
aristas (src, dst, tipo, ...), en pandas y sin Spark.

Vértices: trafos y elementos de corte (ids de SystemElements, ya únicos entre sí), eventos de corte del
SCADA (evento_off_id + OFFSET_EVENTO) e incidencias de Calser (número correlativo + OFFSET_INCIDENCIA).
Hoy los ids de evento (14.986 - 36.828) no chocan con los de elementos (24 - 9.485), pero nada lo
garantiza; el desplazamiento cumple la regla del laboratorio de claves únicas en id, src y dst, y
mantiene el id entero (GraphFrames convierte los ids no enteros con monotonically_increasing_id).

Aristas:
  DEJA_SIN_TENSION  evento -> trafo        ts = inicio del Off, es_microcorte, duracion_s
  AGUAS_ARRIBA      trafo -> elemento      ts = primer_conocido_ts, n_cortes, ultimo_corte_ts
  AFECTA            incidencia -> trafo    ts = inicio de la interrupción
  DESPEJADA_POR     incidencia -> elemento codigo_cabecera de Calser casado por nombre (como en grafo_gp.py)
"""
from pathlib import Path

import pandas as pd

OFFSET_EVENTO = 10_000_000
OFFSET_INCIDENCIA = 20_000_000
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}
APAGON = pd.Timestamp("2025-04-28")


def norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)


def cargar(base) -> dict:
    base = Path(base)
    return {n: pd.read_parquet(base / f"{n}.parquet") for n in
            ["dim_ct", "map_aguas_arriba", "cortes_scada", "interrupciones", "incidencias", "incidencias_ex"]}


def construir(t: dict):
    dim, aa, cs, it, ex = t["dim_ct"], t["map_aguas_arriba"], t["cortes_scada"], t["interrupciones"], t["incidencias_ex"]

    # --- vértices
    tr = aa[["trafo_elemento_id", "distribuidora_id", "ct_id"]].drop_duplicates("trafo_elemento_id")
    v_tr = pd.DataFrame({"id": tr.trafo_elemento_id.astype("int64"), "tipo": "trafo",
                         "red": tr.distribuidora_id.map(NOMBRE).values, "ct_id": tr.ct_id.values})
    el = aa[["elemento_corte_id", "elemento_corte_nombre", "distribuidora_id"]].drop_duplicates("elemento_corte_id")
    v_el = pd.DataFrame({"id": el.elemento_corte_id.astype("int64"), "tipo": "elemento",
                         "nombre": el.elemento_corte_nombre.values})
    ev = cs.groupby("evento_off_id").inicio_ts.min().reset_index()
    v_ev = pd.DataFrame({"id": ev.evento_off_id.astype("int64") + OFFSET_EVENTO, "tipo": "evento", "ts": ev.inicio_ts})

    it = it.copy()
    it["inc"] = it.distribuidora_id.astype(str) + "|" + it.periodo_id.astype(str) + "|" + it.incidencia_id.astype(str)
    inc = it.groupby("inc").agg(ts=("inicio_ts", "min"), distribuidora_id=("distribuidora_id", "first")).reset_index()
    inc["id"] = OFFSET_INCIDENCIA + pd.RangeIndex(len(inc))
    v_in = pd.DataFrame({"id": inc.id.astype("int64"), "tipo": "incidencia", "ts": inc.ts,
                         "red": inc.distribuidora_id.map(NOMBRE).values})
    V = pd.concat([v_tr, v_el, v_ev, v_in], ignore_index=True)
    assert V.id.is_unique

    # --- aristas
    e_dst = pd.DataFrame({"src": cs.evento_off_id.astype("int64") + OFFSET_EVENTO, "dst": cs.elemento_id.astype("int64"),
                          "tipo": "DEJA_SIN_TENSION", "ts": cs.inicio_ts, "es_microcorte": cs.es_microcorte,
                          "duracion_s": cs.duracion_s})
    e_aa = pd.DataFrame({"src": aa.trafo_elemento_id.astype("int64"), "dst": aa.elemento_corte_id.astype("int64"),
                         "tipo": "AGUAS_ARRIBA", "ts": aa.primer_conocido_ts, "n_cortes": aa.n_cortes,
                         "ultimo_corte_ts": aa.ultimo_corte_ts})
    trafo_de = dim.dropna(subset=["trafo_elemento_id"])[["distribuidora_id", "ct_id", "trafo_elemento_id"]]
    af = it.merge(trafo_de, on=["distribuidora_id", "ct_id"]).merge(inc[["inc", "id"]], on="inc")
    e_af = pd.DataFrame({"src": af.id.astype("int64"), "dst": af.trafo_elemento_id.astype("int64"),
                         "tipo": "AFECTA", "ts": af.inicio_ts})
    # un elemento que corta trafos de dos redes casa con el código de cualquiera de las dos
    nombres = aa[["distribuidora_id", "elemento_corte_id", "elemento_corte_nombre"]].drop_duplicates()
    nombres = nombres.assign(n=norm(nombres.elemento_corte_nombre))[["distribuidora_id", "n", "elemento_corte_id"]]
    ex = ex[ex.codigo_cabecera.notna()].copy()
    ex["inc"] = ex.distribuidora_id.astype(str) + "|" + ex.periodo_id.astype(str) + "|" + ex.id.astype(str)
    ex["n"] = norm(ex.codigo_cabecera)
    dp = ex[["distribuidora_id", "inc", "n"]].merge(nombres, on=["distribuidora_id", "n"]).merge(
        inc[["inc", "id"]], on="inc")
    e_dp = pd.DataFrame({"src": dp.id.astype("int64"), "dst": dp.elemento_corte_id.astype("int64"),
                         "tipo": "DESPEJADA_POR"}).drop_duplicates()
    E = pd.concat([e_dst, e_aa, e_af, e_dp], ignore_index=True)
    return V, E


def nest(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Como nestAsCol de GraphFrames: cada elemento con nombre del motivo aporta sus columnas con prefijo."""
    return df.add_prefix(f"{name}.")
