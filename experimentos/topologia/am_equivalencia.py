"""aggregateMessages frente a lo que ya calcula Gold.

GraphFrames.aggregateMessages construye las tripletas (dos joins de las aristas con los vértices),
manda una columna a src o a dst, une con los vértices y agrupa por id (AggregateMessages.agg en el
código de la 0.12.3). Es el patrón gather de GAS escrito como join y groupBy, el mismo que usa
job_gold_dim_ct.py para n_posiciones_aguas_arriba.

GraphFrames (sin ejecutar):
  from graphframes.lib import AggregateMessages as AM
  aa = g.filterEdges("tipo = 'AGUAS_ARRIBA'")
  npos = aa.aggregateMessages(F.countDistinct(AM.msg).alias("n_pos"), sendToSrc=AM.edge["posicion_id"])

Aquí se emula con pandas y se compara con la columna de dim_ct de la exportación.

Uso: python am_equivalencia.py <carpeta zonas_20261013_1041>
"""
import sys
import time
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
dim = pd.read_parquet(BASE / "dim_ct.parquet")
V = pd.DataFrame({"id": pd.concat([aa.trafo_elemento_id, aa.elemento_corte_id]).unique()})
t0 = time.time()
# tripletas: arista con su vértice origen y destino (aquí solo hace falta el id)
trip = aa[["trafo_elemento_id", "elemento_corte_id", "posicion_id"]].merge(
    V, left_on="trafo_elemento_id", right_on="id").merge(V, left_on="elemento_corte_id", right_on="id",
                                                         suffixes=("_src", "_dst"))
msgs = trip[["id_src", "posicion_id"]].rename(columns={"id_src": "id", "posicion_id": "MSG"})
res = msgs.merge(V, on="id").groupby("id").MSG.nunique().rename("n_pos").reset_index()
t_am = time.time() - t0
cmp = dim.dropna(subset=["trafo_elemento_id"]).merge(res, left_on="trafo_elemento_id", right_on="id", how="left")
cmp["n_pos"] = cmp.n_pos.fillna(0)
iguales = (cmp.n_pos == cmp.n_posiciones_aguas_arriba.fillna(0)).mean()
print(f"aggregateMessages emulado: {len(res):,} trafos con mensaje ({t_am * 1000:.0f} ms); coincide con "
      f"dim_ct.n_posiciones_aguas_arriba en el {iguales:.1%} de los {len(cmp):,} CT con trafo")
print(f"CT con trafo sin ningun mensaje (no aparecen en el resultado de aggregateMessages): {(cmp.id.isna()).sum()}")
