"""Cuántas cabeceras de job_analisis_red._feeders ya no cortan a su trafo: la pareja (trafo, cabecera) se vio por
última vez hace más de uno o dos años, mientras otros elementos siguen cortando al trafo después. Complementa el
cruce de los conjuntos de corte de m3_triangulos.py con una cifra por trafo.

Uso: python cabecera_caducada.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
dim = pd.read_parquet(BASE / "dim_ct.parquet")
FIN = aa.ultimo_corte_ts.max()
print("último corte de la ventana:", FIN)
pdf = aa.copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
cab = pdf.drop_duplicates("trafo_elemento_id").set_index("trafo_elemento_id")
ultimo = aa.groupby("trafo_elemento_id").ultimo_corte_ts.max()
est = dim[dim.en_estudio & dim.trafo_elemento_id.notna()].trafo_elemento_id.astype("int64")
kva = pd.to_numeric(dim.potencia_tiepi_kva, errors="coerce").where(lambda s: s > 0,
                                                                   pd.to_numeric(dim.potencia_kva, errors="coerce"))
kva_t = pd.Series(kva.values, index=dim.trafo_elemento_id.values).groupby(level=0).sum()
n = len(cab)
for anios in (1, 2):
    corte = FIN - pd.DateOffset(years=anios)
    viejo = cab.ultimo_corte_ts < corte
    sigue = ultimo.reindex(cab.index) >= corte
    # otro elemento empieza a cortar al trafo después de la última vez de su cabecera, con 2 cortes o más
    tras = pdf.merge(cab[["ultimo_corte_ts"]].rename(columns={"ultimo_corte_ts": "fin_cab"}), left_on="trafo_elemento_id",
                     right_index=True)
    relevo = tras[(tras.primer_corte_ts > tras.fin_cab) & (tras.n_cortes >= 2)].trafo_elemento_id.unique()
    m = viejo & sigue
    m2 = m & cab.index.isin(relevo)
    print(f"cabecera sin cortar a su trafo en {'el último año' if anios == 1 else 'los últimos ' + str(anios) + ' años'} aunque el trafo "
          f"sí se corta: {int(m.sum())} de {n} trafos ({m.mean():.1%}; kVA {kva_t.reindex(cab.index[m]).sum() / kva_t.reindex(cab.index).sum():.1%}); "
          f"con otro elemento que empieza a cortarlo después (2 cortes o más): {int(m2.sum())} ({m2.mean():.1%}); "
          f"de los del estudio: {int(m[cab.index.isin(est)].sum())} de {int(cab.index.isin(est).sum())}")
# edad de las parejas (para la ventana de validez): parte de parejas y de cortes sin verse en 1 y 2 años
for anios in (1, 2):
    corte = FIN - pd.DateOffset(years=anios)
    v = aa.ultimo_corte_ts < corte
    print(f"parejas sin verse en {anios} año(s): {v.mean():.1%} de {len(aa):,}; su parte de n_cortes {aa.n_cortes[v].sum() / aa.n_cortes.sum():.1%}")
