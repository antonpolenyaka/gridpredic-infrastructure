"""La cabecera mayor del análisis de red: de dónde salen sus cortes. Cuenta, para el elemento que es la cabecera
(argmax de _feeders) de más CT, sus cortes por trafo en map_aguas_arriba, el periodo en que cortó y los eventos de
fact_cortes_scada de ese periodo que dejaron en Off a la vez a la mayoría de sus trafos: cuántos, en cuántos días,
qué parte son microcortes (Off de 180 s o menos) y su duración. Sin nombres ni identificadores en la salida.

Uso: python brote_2021.py <carpeta zonas_20261013_1041>
"""
import sys
from pathlib import Path

import pandas as pd

BASE = Path(sys.argv[1])
aa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
cs = pd.read_parquet(BASE / "cortes_scada.parquet")
pdf = aa.copy()
pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
cab = pdf.drop_duplicates("trafo_elemento_id")
mayor = cab.groupby("elemento_corte_id").size().sort_values(ascending=False)
g = mayor.index[0]
pares = aa[aa.elemento_corte_id == g]
tr = set(pares.trafo_elemento_id)
ini, fin = pares.primer_corte_ts.min(), pares.ultimo_corte_ts.max()
print(f"cabecera mayor: lo es de {mayor.iloc[0]} trafos y corta {len(tr)}; cortes por trafo: mediana "
      f"{pares.n_cortes.median():.0f}, maximo {pares.n_cortes.max()}; primer corte {ini:%d.%m.%Y}, ultimo {fin:%d.%m.%Y}")
w = cs[(cs.inicio_ts >= ini.normalize()) & (cs.inicio_ts < (fin.normalize() + pd.Timedelta(days=1)))
       & cs.elemento_id.isin(tr)]
ev = w.groupby("evento_off_id").agg(n=("elemento_id", "nunique"), micro=("es_microcorte", "mean"),
                                    etiqueta=("es_corte_etiqueta", "mean"), dur=("duracion_s", "median"),
                                    dia=("inicio_ts", lambda s: s.min().normalize()))
grandes = ev[ev.n >= 0.6 * len(tr)]
print(f"eventos de ese periodo que dejan en Off al 60 % o mas de sus trafos: {len(grandes)}, en {grandes.dia.nunique()} dias; "
      f"solo con microcortes {int((grandes.micro == 1).sum())} ({(grandes.micro == 1).mean():.0%}); "
      f"duracion mediana de sus episodios {grandes.dur.median():.0f} s; episodios que cuentan como corte (> 180 s) "
      f"{w[w.evento_off_id.isin(grandes.index)].es_corte_etiqueta.mean():.1%}")
otros = cs[cs.elemento_id.isin(tr) & (cs.inicio_ts > fin)]
print(f"despues del {fin:%d.%m.%Y} esos trafos siguen cortandose: {otros.evento_off_id.nunique():,} eventos hasta "
      f"{cs.inicio_ts.max():%d.%m.%Y}, ninguno atribuido a esa cabecera")
