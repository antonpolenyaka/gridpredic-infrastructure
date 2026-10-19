"""Tasa base de la comprobación inversa de anillos_red.py.

Para todos los eventos de corte del SCADA con algún Off de más de 180 s (sin emparejamiento dudoso), qué parte
tiene una incidencia de Calser (imprevista o programada) de la misma red a menos de 10 minutos, separando los
eventos de una sola red de los que cortan dos.

Uso: python cobertura_calser.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(sys.argv[1])
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}
cs = pd.read_parquet(BASE / "cortes_scada.parquet")
inc = pd.read_parquet(BASE / "incidencias.parquet")
cs["red"] = cs.distribuidora_id.map(NOMBRE)
inc["red"] = inc.distribuidora_id.map(NOMBRE)
largo = cs[(cs.duracion_s.fillna(0) > 180) & ~cs.emparejamiento_dudoso]
redes_evento = cs.groupby("evento_off_id").red.nunique()
ev = largo.groupby(["evento_off_id", "red"]).agg(inicio=("inicio_ts", "min"), trafos=("ct_id", "nunique")).reset_index()
ev["cruzado"] = ev.evento_off_id.map(redes_evento) >= 2
ventana = pd.Timedelta("10min")
t_inc = {r: np.sort(g.inicio_ts.values) for r, g in inc.groupby("red")}


def hay_incidencia(row):
    arr = t_inc.get(row.red)
    if arr is None or len(arr) == 0:
        return False
    i = np.searchsorted(arr, np.datetime64(row.inicio - ventana))
    return i < len(arr) and arr[i] <= np.datetime64(row.inicio + ventana)


ev["con_incidencia"] = ev.apply(hay_incidencia, axis=1)
res = ev.groupby(["red", "cruzado"]).agg(eventos=("evento_off_id", "size"), con_incidencia=("con_incidencia", "mean"),
                                         trafos_mediana=("trafos", "median")).round(3)
print(res.to_string())
grandes = ev[ev.trafos >= 20]
print("\nsolo eventos con 20 trafos o mas en la red:")
print(grandes.groupby(["red", "cruzado"]).agg(eventos=("evento_off_id", "size"),
                                               con_incidencia=("con_incidencia", "mean")).round(3).to_string())

# parte de los episodios del SCADA de cada red (filas de fact_cortes_scada) que viene de eventos cruzados
cs["cruzado"] = cs.evento_off_id.map(redes_evento) >= 2
cs["largo"] = (cs.duracion_s.fillna(0) > 180) & ~cs.emparejamiento_dudoso
print("\nparte de los episodios Off -> On de cada red que vienen de eventos que cortan las dos redes:")
tab = cs.groupby("red").apply(lambda g: pd.Series({
    "episodios": len(g), "de_eventos_cruzados": g.cruzado.mean(),
    "microcortes": int(g.es_microcorte.sum()), "microcortes_cruzados": g[g.es_microcorte].cruzado.mean(),
    "largos": int(g.largo.sum()), "largos_cruzados": g[g.largo].cruzado.mean(),
    "corte_etiqueta": int(g.es_corte_etiqueta.sum()), "etiqueta_cruzados": g[g.es_corte_etiqueta].cruzado.mean()}),
    include_groups=False)
print(tab.round(3).to_string())
