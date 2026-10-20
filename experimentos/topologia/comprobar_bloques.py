"""Comprueba dos frases sobre los eventos cruzados de las incidencias simultáneas (anillos_red.py).

- Si los 216 trafos de la Distribuidora 1 del evento de 2022 son los mismos que los del evento de 2024.
- Si los 35 trafos de la Distribuidora 2 del evento de 2024 están entre los 36 de la componente fuerte mixta (anillos_red.py).
Los eventos se buscan por el instante de Calser (sin escribir sus identificadores aquí).

Uso: python comprobar_bloques.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from grafo_gp import componentes_conexas  # noqa: E402

BASE = Path(sys.argv[1])
cs = pd.read_parquet(BASE / "cortes_scada.parquet")
mapa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}
cs["red"] = cs.distribuidora_id.map(NOMBRE)
redes = cs.groupby("evento_off_id").red.nunique()


def evento_cruzado(instante):
    t = pd.Timestamp(instante)
    cerca = cs[(cs.inicio_ts - t).abs() <= pd.Timedelta(seconds=2)]
    ids = [e for e in cerca.evento_off_id.unique() if redes[e] >= 2]
    return cs[cs.evento_off_id.isin(ids)]


e22 = evento_cruzado("2022-04-17 14:40:46")
e24 = evento_cruzado("2024-03-27 19:21:20")
e26 = evento_cruzado("2026-01-28 08:35:11")
d1_22, d1_24 = set(e22[e22.red == "D1"].elemento_id), set(e24[e24.red == "D1"].elemento_id)
d1_26 = set(e26[e26.red == "D1"].elemento_id)
print(f"D1 2022 {len(d1_22)}, D1 2024 {len(d1_24)}, comunes {len(d1_22 & d1_24)}; D1 2026 {len(d1_26)} y comunes con 2022 {len(d1_26 & d1_22)}")

mapa["red"] = mapa.distribuidora_id.map(NOMBRE)
tot = mapa.groupby("trafo_elemento_id").n_cortes.transform("sum")
fu = mapa[(mapa.n_cortes >= 2) & (mapa.n_cortes / tot >= 0.2)]

red_trafo = dict(zip("t" + mapa.trafo_elemento_id.astype(str), mapa.red))
d2_comp = set()
for c in componentes_conexas("t" + fu.trafo_elemento_id.astype(str), "e" + fu.elemento_corte_id.astype(str)):
    rr = {red_trafo[n] for n in c if n.startswith("t")}
    if len(rr) >= 2:
        d2_comp |= {int(n[1:]) for n in c if n.startswith("t") and red_trafo[n] == "D2"}
d2_24 = set(e24[e24.red == "D2"].elemento_id)
d2_26 = set(e26[e26.red == "D2"].elemento_id)
print(f"D2 2024 {len(d2_24)}, de ellos en la componente fuerte mixta {len(d2_24 & d2_comp)} de {len(d2_comp)}; "
      f"D2 2026 {len(d2_26)}, en la componente {len(d2_26 & d2_comp)}")

# los trafos D1 de los eventos cruzados frente a los D1 que comparten algún elemento con trafos D2 en map_aguas_arriba
d1_cruz = set(cs[cs.evento_off_id.isin(redes[redes >= 2].index) & (cs.red == "D1")].elemento_id)
el_por_red = mapa.groupby("elemento_corte_id").red.apply(set)
el_mixtos = {e for e, s in el_por_red.items() if {"D1", "D2"} <= s}
d1_patron = set(mapa[(mapa.red == "D1") & mapa.elemento_corte_id.isin(el_mixtos)].trafo_elemento_id)
print(f"D1 en eventos cruzados {len(d1_cruz)}, D1 del patron estatico {len(d1_patron)}, comunes {len(d1_cruz & d1_patron)}")
