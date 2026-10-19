"""
E3. Dos patrones que cruzan fuentes (memoria, 5.3.4 y 5.7.4), resueltos con joins de pandas:

a) Incidencias imprevistas simultáneas en dos distribuidoras cuyos CT comparten un elemento aguas arriba (lo que
   simultanea_otra_red marca solo por la hora de inicio):

   MATCH (i1:Incidencia)-[:AFECTA]->(c1:CT)-[r1:AGUAS_ARRIBA]->(e:ElementoCorte)
         <-[r2:AGUAS_ARRIBA]-(c2:CT)<-[:AFECTA]-(i2:Incidencia)
   WHERE i1.distribuidora_id < i2.distribuidora_id AND i1.inicio_ts = i2.inicio_ts
     AND r1.primer_conocido_ts < i1.inicio_ts AND r2.primer_conocido_ts < i1.inicio_ts

b) Ciclo entre Calser y el SCADA: la cabecera que Calser anota en la incidencia (incidencias_ex) es un elemento
   aguas arriba de los CT que la incidencia cortó, según la topología aprendida de los cortes:

   MATCH (i:Incidencia)-[:DESPEJADA_POR]->(e:ElementoCorte), (i)-[a:AFECTA]->(c:CT)
   OPTIONAL MATCH (c)-[r:AGUAS_ARRIBA]->(e) WHERE r.primer_conocido_ts < a.inicio_ts

    python e3_simultaneas.py <carpeta de la exportación> [salida/]
"""
import sys
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

import grafo_gp as gg  # noqa: E402

warnings.filterwarnings("ignore", category=DeprecationWarning)

DESDE = pd.Timestamp("2021-01-07")
LINES = []


def say(text=""):
    print(text)
    LINES.append(text)


def main():
    t = gg.cargar(sys.argv[1])
    aa, it, inc, dim = t["map_aguas_arriba"], t["interrupciones"], t["incidencias"], t["dim_ct"]

    # ---------------- a) simultáneas ----------------
    imp = inc[(inc.clasificacion_id == "CL_IMPRE") & (inc.inicio_ts >= DESDE)].copy()
    imp["seg"] = imp.inicio_ts.dt.floor("s")
    con_ct = set(it.inc)
    for nombre, tol in [("mismo segundo", 0), ("a 5 minutos o menos", 300)]:
        t0 = time.perf_counter()
        a = imp[["inc", "distribuidora_id", "seg"]]
        if tol == 0:
            p = a.merge(a, on="seg")
        else:
            a2 = a.assign(k=1)
            p = a2.merge(a2, on="k")
            p = p[(p.seg_x - p.seg_y).abs() <= pd.Timedelta(seconds=tol)]
            p = p.rename(columns={"seg_x": "seg"})
        p = p[p.distribuidora_id_x < p.distribuidora_id_y][["inc_x", "inc_y", "seg"]].drop_duplicates(["inc_x", "inc_y"])
        p["probables"] = p.inc_x.isin(con_ct) & p.inc_y.isin(con_ct)
        prob = p[p.probables]
        # pandas: CT de cada incidencia con sus aristas, y cruce por elemento
        ctinc = it[["inc", "ct", "inicio_ts"]].drop_duplicates(["inc", "ct"])
        e1 = prob.merge(ctinc.rename(columns={"inc": "inc_x", "ct": "ct_x"}), on="inc_x")[["inc_x", "inc_y", "seg", "ct_x"]]
        e1 = e1.merge(aa[["ct", "el", "primer_conocido_ts"]].rename(columns={"ct": "ct_x"}), on="ct_x")
        e2 = prob.merge(ctinc.rename(columns={"inc": "inc_y", "ct": "ct_y"}), on="inc_y")[["inc_x", "inc_y", "ct_y"]]
        e2 = e2.merge(aa[["ct", "el", "primer_conocido_ts"]].rename(columns={"ct": "ct_y"}), on="ct_y")
        m = e1.merge(e2, on=["inc_x", "inc_y", "el"], suffixes=("_1", "_2"))
        m_hist = m[["inc_x", "inc_y", "el"]].drop_duplicates()
        m_ant = m[(m.primer_conocido_ts_1 < m.seg) & (m.primer_conocido_ts_2 < m.seg)][["inc_x", "inc_y", "el"]].drop_duplicates()
        t_pd = time.perf_counter() - t0

        hits_pd = set(zip(m_ant.inc_x, m_ant.inc_y))
        dias = prob.seg.dt.normalize()
        say(f"[a] {nombre}: {len(p)} pares de incidencias imprevistas en dos distribuidoras desde el 07.01.2021; "
              f"con CT de nivel principal en las dos: {len(prob)} (en {dias.nunique()} días)")
        say(f"    comparten un elemento aguas arriba conocido antes del inicio: {len(hits_pd)} pares "
              f"({m_ant.el.nunique()} elementos distintos); con aristas de todo el histórico: "
              f"{m_hist[['inc_x', 'inc_y']].drop_duplicates().shape[0]} pares")
        say(f"    pandas {1000 * t_pd:.0f} ms")
        if len(prob):
            sin = prob[~prob.set_index(["inc_x", "inc_y"]).index.isin(list(hits_pd))]
            say(f"    pares sin elemento común, por día: {sin.seg.dt.date.value_counts().sort_index().to_dict()}")
            con = prob[prob.set_index(["inc_x", "inc_y"]).index.isin(list(hits_pd))]
            say(f"    pares con elemento común, por día: {con.seg.dt.date.value_counts().sort_index().to_dict()}")
            # CT por incidencia en los pares
            n_ct = it.drop_duplicates(["inc", "ct"]).groupby("inc").ct.nunique()
            say(f"    CT por incidencia en los pares (mediana): {int(pd.concat([n_ct.loc[prob.inc_x], n_ct.loc[prob.inc_y]]).median())}")

    # ---------------- b) ciclo Calser - SCADA ----------------
    cab = gg.cabecera_calser(t)
    ex = t["incidencias_ex"]
    ex_inc = inc.merge(ex[["inc", "codigo_cabecera"]], on="inc", how="left")
    desde21 = ex_inc[(ex_inc.inicio_ts >= DESDE) & ex_inc.codigo_cabecera.notna()]
    say(f"\n[b] incidencias desde el 07.01.2021 con cabecera anotada en Calser: {len(desde21)}; "
          f"cuyo código casa con un elemento de corte del SCADA: {desde21.inc.isin(cab.inc).sum()}; "
          f"de esas, con interrupciones principales de CT: {desde21[desde21.inc.isin(cab.inc)].inc.isin(con_ct).sum()}")
    t0 = time.perf_counter()
    base = it[it.inicio_ts >= DESDE][["inc", "ct", "inicio_ts"]]
    base = base.groupby(["inc", "ct"], as_index=False).inicio_ts.min()
    x = base.merge(cab, on="inc")  # inc, ct, inicio, el (cabecera de Calser)
    x = x.merge(aa[["ct", "el", "primer_conocido_ts"]], on=["ct", "el"], how="left")
    x["cierra_hist"] = x.primer_conocido_ts.notna()
    x["cierra_antes"] = x.primer_conocido_ts < x.inicio_ts
    argmax = dict(zip(dim.ct, "EC:" + dim.cabecera_id.astype("Int64").astype(str)))
    x["es_argmax"] = x.ct.map(argmax) == x.el
    por_par = x.groupby(["inc", "el"]).agg(n_ct=("ct", "nunique"), n_hist=("cierra_hist", "sum"),
                                          n_antes=("cierra_antes", "sum"), n_argmax=("es_argmax", "sum"))
    por_inc = por_par.groupby(level="inc").agg(n_ct=("n_ct", "max"), n_hist=("n_hist", "max"),
                                               n_antes=("n_antes", "max"), n_argmax=("n_argmax", "max"))
    t_pd = time.perf_counter() - t0

    n = len(por_inc)
    say(f"    incidencias evaluadas: {n}")
    say(f"    la cabecera de Calser es aguas arriba de algún CT de la incidencia (arista conocida antes): "
          f"{int((por_inc.n_antes > 0).sum())} ({(por_inc.n_antes > 0).mean():.1%}); con el histórico completo: "
          f"{int((por_inc.n_hist > 0).sum())} ({(por_inc.n_hist > 0).mean():.1%})")
    say(f"    ... de todos sus CT: {int((por_inc.n_antes == por_inc.n_ct).sum())} ({(por_inc.n_antes == por_inc.n_ct).mean():.1%})")
    say(f"    parte de los CT de la incidencia que cierran el ciclo (mediana): {(por_inc.n_antes / por_inc.n_ct).median():.1%}")
    say(f"    la cabecera de Calser es la cabecera argmax de la mayoría de sus CT: "
          f"{int((por_inc.n_argmax > por_inc.n_ct / 2).sum())} ({(por_inc.n_argmax > por_inc.n_ct / 2).mean():.1%})")
    say(f"    pandas {1000 * t_pd:.0f} ms")
    # incidencias que no cierran: CT por incidencia y distribuidora
    no = por_inc[por_inc.n_hist == 0]
    say(f"    sin ciclo ni con el histórico completo: {len(no)}; por distribuidora: "
          f"{pd.Series([gg.red(i.split(':')[1].split('|')[0]) for i in no.index]).value_counts().to_dict()}")
    say(f"    con ciclo por distribuidora (arista conocida antes): "
          f"{pd.Series([gg.red(i.split(':')[1].split('|')[0]) for i in por_inc[por_inc.n_antes > 0].index]).value_counts().to_dict()}")

    if len(sys.argv) > 2:
        Path(sys.argv[2]).mkdir(parents=True, exist_ok=True)
        (Path(sys.argv[2]) / "e3_simultaneas.txt").write_text("\n".join(LINES) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
