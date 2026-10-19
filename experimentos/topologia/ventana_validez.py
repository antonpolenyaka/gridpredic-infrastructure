"""
E1c. Ventana de validez de la topología aprendida del SCADA.

Pregunta: para saber qué CT caerán juntos el año siguiente (pares de CT en
una misma incidencia imprevista de Calser en Y), cuanta historia del SCADA
conviene mirar? Es el parámetro de la fecha de baja de AGUAS_ARRIBA que
que se propuso (último corte más un año) y las ventanas de 30, 90 y 365 días.

Puntuación de cada par de CT: número de eventos Off del SCADA (sin
maniobras) que dejaron sin tensión a los dos en una ventana antes del
01.01.Y (30, 90, 180, 365, 730 días o toda la historia desde 2021), o la suma
con decaimiento exponencial (tau 90, 365, 730 días). Variantes con y sin
microcortes. Referencias: elementos aguas arriba compartidos de
map_aguas_arriba conocidos antes del corte (sin fecha de baja, como hoy) y el
número de co-cortes de Calser desde 2016.

Candidatos: pares de la misma red vigentes en Y. Y = 2022 - 2026 (2026 hasta
el 02.08). PR-AUC y ROC-AUC de todos los pares de Y y de los nuevos.

Uso: python ventana_validez.py <carpeta de la exportación de Gold> [salida/]
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import DIR, activos, cargar, grafo, salida  # noqa: E402
from prediccion_enlaces import fin_anio, laterales  # noqa: E402

F3 = lambda v: f"{v:.3f}"  # noqa: E731
pd.set_option("display.width", 220)


def main():
    dim, it, inc, aa = cargar()
    n = len(dim)
    idx = {(d, c): i for i, (d, c) in enumerate(zip(dim.distribuidora_id, dim.ct_id))}
    cs = pd.read_parquet(f"{DIR}/cortes_scada.parquet",
                         columns=["evento_off_id", "distribuidora_id", "ct_id", "inicio_ts", "es_maniobra",
                                  "es_microcorte"])
    cs = cs[~cs.es_maniobra.fillna(False)]
    cs = cs.assign(node=[idx.get((d, c), -1) for d, c in zip(cs.distribuidora_id, cs.ct_id)])
    cs = cs[cs.node >= 0]
    print(f"Filas del SCADA sin maniobras con CT del estudio: {len(cs):,}; eventos: {cs.evento_off_id.nunique():,}; "
          f"desde {cs.inicio_ts.min()} hasta {cs.inicio_ts.max()}; microcortes: {cs.es_microcorte.mean():.1%} de las filas")
    variants = {"con microcortes": cs, "sin microcortes": cs[~cs.es_microcorte.fillna(False)]}
    mats = {}
    for vname, d in variants.items():
        ev = d.groupby("evento_off_id").agg(t=("inicio_ts", "min")).reset_index()
        eid = {e: i for i, e in enumerate(ev.evento_off_id)}
        rows = d.evento_off_id.map(eid).values
        M = sparse.csr_matrix((np.ones(len(d)), (rows, d.node.values)), shape=(len(ev), n))
        M.data[:] = 1.0
        M.sum_duplicates()
        M.data[:] = 1.0
        mats[vname] = (M, ev.t.values)

    red = dim.distribuidora_id.values
    same_net = red[:, None] == red[None, :]
    np.fill_diagonal(same_net, False)
    out = []
    for Y in range(2022, 2027):
        cut = np.datetime64(f"{Y}-01-01")
        Wy, _, _ = grafo(inc, n, pd.Timestamp(cut), fin_anio(Y))
        Wc, _, _ = grafo(inc, n, "2016-01-01", pd.Timestamp(cut))
        act = activos(dim, pd.Timestamp(cut), fin_anio(Y))
        C = np.triu(same_net & (act[:, None] & act[None, :]), 1)
        iu = np.where(C)
        y = (Wy > 0)[iu]
        new = ~(Wc > 0)[iu]
        scores = {}
        for vname, (M, t) in mats.items():
            age = (cut - t).astype("timedelta64[D]").astype(float)
            for w in (30, 90, 180, 365, 730, None):
                sel = (age > 0) & ((age <= w) if w else True)
                Ms = M[np.where(sel)[0]]
                S = (Ms.T @ Ms).toarray()
                scores[f"SCADA {vname}, {w} dias" if w else f"SCADA {vname}, toda la historia"] = S
            for tau in (90, 365, 730):
                sel = age > 0
                wts = np.exp(-age[sel] / tau)
                Ms = M[np.where(sel)[0]]
                S = (Ms.T @ sparse.diags(wts) @ Ms).toarray()
                scores[f"SCADA {vname}, decaimiento tau {tau} dias"] = S
        L = laterales(dim, aa, pd.Timestamp(cut), n)
        scores["map_aguas_arriba sin fecha de baja (elementos compartidos)"] = L["elementos_compartidos"]
        scores["Calser, numero de co-cortes desde 2016"] = Wc
        for name, S in scores.items():
            s = S[iu]
            row = dict(anio=Y, puntuacion=name, roc_auc=roc_auc_score(y, s), pr_auc=average_precision_score(y, s),
                       tasa=y.mean())
            if y[new].sum():
                row["pr_auc_nuevos"] = average_precision_score(y[new], s[new])
                row["roc_auc_nuevos"] = roc_auc_score(y[new], s[new])
            out.append(row)
    r = pd.DataFrame(out)
    r.to_csv(salida("lp_ventana_scada.csv"), index=False)
    print("\n== Media 2022 - 2026 (todos los pares de Y y solo los nuevos para Calser)")
    print(r.groupby("puntuacion")[["roc_auc", "pr_auc", "roc_auc_nuevos", "pr_auc_nuevos"]].mean()
          .sort_values("pr_auc", ascending=False).to_string(float_format=F3))
    print("\n== PR-AUC por anio, todos los pares")
    print(r.pivot_table(index="puntuacion", columns="anio", values="pr_auc").to_string(float_format=F3))
    print("\n== PR-AUC por anio, pares nuevos")
    print(r.pivot_table(index="puntuacion", columns="anio", values="pr_auc_nuevos").to_string(float_format=F3))


if __name__ == "__main__":
    main()
