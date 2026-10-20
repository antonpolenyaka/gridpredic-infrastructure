"""
E1. Predicción de enlaces temporal en el grafo CT - CT de co-corte de Calser,
con el protocolo que pide el laboratorio de Graph Embeddings hecho con rigor.

Grafo: dos CT del estudio unidos si una misma incidencia imprevista los cortó
(desde 2016, sin el apagón). Para cada año Y de 2019 a 2026 (2026 hasta el
02.08) el grafo de entrenamiento lleva las incidencias anteriores al 01.01.Y y
los positivos son los pares que se cortan juntos en Y.

1. Enlaces nuevos (pares que nunca se habían cortado juntos), candidatos
   filtrados: todos los pares de la misma red, vigentes en Y, que no son
   arista de entrenamiento. Referencias simples sobre el grafo de
   entrenamiento (vecinos comunes, Jaccard, Adamic-Adar, asignación de
   recursos, preferential attachment, Katz truncado), información lateral
   (elementos aguas arriba compartidos conocidos antes del corte, coseno
   TF-IDF entre los conjuntos de elementos aguas arriba de cada CT (no hay textos), mismo grupo, cabecera, municipio o celda, distancia) y azar.
   Métricas: ROC-AUC y PR-AUC sobre todos los candidatos, ROC-AUC con
   negativos muestreados como en el cuaderno (un extremo cambiado por un nodo
   al azar de las tres redes, sin filtrar), con negativos "dificiles" (misma
   cabecera) y ranking filtrado por CT (MRR, Hits@10, Hits@50).
2. Contraste: partición aleatoria de aristas (10 %, como RandomLinkSplit) y
   todos los pares de Y (repetidos incluidos) con la persistencia.
3. Deriva: probabilidad de volver a cortarse juntos según los años desde el
   último co-corte, ventanas de entrenamiento (1, 2, 3 años, todo, decaimiento
   exponencial) y separación de 1 a 3 años entre el corte y el año evaluado.

Uso: python prediccion_enlaces.py <carpeta de la exportación de Gold> [salida/]
"""
import os
import sys
import time

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import FIN_DATOS, activos, cargar, grafo, grafo_decay, salida  # noqa: E402

YEARS = list(range(2019, 2027))
SEEDS = 5
pd.set_option("display.width", 230)
pd.set_option("display.max_rows", 200)
F3 = lambda v: f"{v:.3f}"  # noqa: E731


def fin_anio(y):
    return min(pd.Timestamp(f"{y + 1}-01-01"), FIN_DATOS)


def heuristicas(A):
    A = A.astype(np.float64)
    deg = A.sum(1)
    cn = A @ A
    union = deg[:, None] + deg[None, :] - cn
    jac = np.divide(cn, union, out=np.zeros_like(cn), where=union > 0)
    w_aa = np.where(deg > 1, 1.0 / np.log(np.maximum(deg, 2.0)), 0.0)
    w_ra = np.where(deg > 0, 1.0 / np.maximum(deg, 1.0), 0.0)
    aa = (A * w_aa[None, :]) @ A
    ra = (A * w_ra[None, :]) @ A
    pa = deg[:, None] * deg[None, :]
    lam = float(np.max(np.abs(np.linalg.eigvalsh(A)))) if A.any() else 1.0
    beta = 0.5 / lam
    katz = beta * A + beta ** 2 * cn + beta ** 3 * (cn @ A)
    return {"vecinos_comunes": cn, "jaccard": jac, "adamic_adar": aa, "asignacion_recursos": ra,
            "pref_attachment": pa, "katz": katz}, deg


def laterales(dim, aa, cut, n):
    out = {}
    same = lambda v: (v[:, None] == v[None, :]) & pd.notna(v)[:, None]  # noqa: E731
    out["mismo_grupo"] = same(dim.grupo_red_id.values).astype(float)
    out["misma_cabecera"] = same(dim.cabecera.values.astype(object)).astype(float)
    out["mismo_municipio"] = same(dim.municipio_id.values.astype(object)).astype(float)
    out["misma_celda"] = same(dim.celda_id.values.astype(object)).astype(float)
    lat = np.radians(dim.ct_lat_municipio.values.astype(float))
    lon = np.radians(dim.ct_lon_municipio.values.astype(float))
    dist = np.sqrt(((lat[:, None] - lat[None, :]) * 6371) ** 2 +
                   ((lon[:, None] - lon[None, :]) * 6371 * np.cos(np.nanmean(lat))) ** 2)
    out["cercania_municipio"] = -np.nan_to_num(dist, nan=1e6)
    e = aa[aa.primer_conocido_ts < pd.Timestamp(cut)]
    if len(e):
        elems = np.sort(e.elemento_corte_id.unique())
        ei = {x: j for j, x in enumerate(elems)}
        B = np.zeros((n, len(elems)))
        B[e.node.values, [ei[x] for x in e.elemento_corte_id.values]] = 1.0
        dfe = B.sum(0)
        out["elementos_compartidos"] = B @ B.T
        out["adamic_adar_elementos"] = (B * np.where(dfe > 1, 1 / np.log(np.maximum(dfe, 2)), 0)[None, :]) @ B.T
        tf = B / np.maximum(B.sum(1, keepdims=True), 1e-12)
        X = tf * (np.log(B.shape[0] / np.maximum(dfe, 1)) + 1.0)
        X = X / np.maximum(np.linalg.norm(X, axis=1, keepdims=True), 1e-12)
        out["coseno_tfidf"] = X @ X.T
    return out


def ranking(S, C, P, queries, rng_seeds=SEEDS, ks=(10, 50)):
    """MRR y Hits@k del primer positivo para cada CT consulta; empates rotos al azar (media de semillas)."""
    rr, hits = [], {k: [] for k in ks}
    for q in queries:
        cand = np.where(C[q])[0]
        pos = P[q, cand]
        s = S[q, cand].astype(np.float64)
        scale = max(np.abs(s).max(), 1.0) * 1e-9
        r_q, h_q = [], {k: [] for k in ks}
        for seed in range(rng_seeds):
            noise = np.random.default_rng(seed * 100003 + q).random(len(s)) * scale
            t = s + noise
            best = t[pos].max()
            rank = 1 + int((t[~pos] > best).sum())
            r_q.append(1.0 / rank)
            for k in ks:
                h_q[k].append(rank <= k)
        rr.append(np.mean(r_q))
        for k in ks:
            hits[k].append(np.mean(h_q[k]))
    return np.mean(rr), {k: np.mean(v) for k, v in hits.items()}, len(queries)


def auc_muestreo_cuaderno(S, P_pairs, n, rng):
    """Negativos como negative_sampling del cuaderno: se cambia la cabeza o la cola (p = 0,5) por un nodo
    cualquiera al azar, sin comprobar que el par resultante no exista."""
    a, b = P_pairs
    m = len(a)
    mask = rng.random(m) > 0.5
    r = rng.integers(0, n, m)
    na = np.where(mask, r, a)
    nb = np.where(mask, b, r)
    s_pos = S[a, b]
    s_neg = S[na, nb]
    y = np.r_[np.ones(m), np.zeros(m)]
    return roc_auc_score(y, np.r_[s_pos, s_neg]), na, nb


def main():
    t0 = time.time()
    dim, it, inc, aa = cargar()
    n = len(dim)
    red = dim.distribuidora_id.values
    same_net = red[:, None] == red[None, :]
    np.fill_diagonal(same_net, False)
    cab = dim.cabecera.values.astype(object)
    same_cab = (cab[:, None] == cab[None, :]) & pd.notna(cab)[:, None]
    print(f"CT del estudio: {n}; incidencias imprevistas con 2+ CT desde 2016 (sin apagon): {(inc.n >= 2).sum()}")

    rows_new, rows_rank, rows_conc, rows_all = [], [], [], []
    for Y in YEARS:
        cut = pd.Timestamp(f"{Y}-01-01")
        W, L, _ = grafo(inc, n, "2016-01-01", cut)
        A = W > 0
        Wy, _, sel_y = grafo(inc, n, cut, fin_anio(Y))
        act = activos(dim, cut, fin_anio(Y))
        act2 = act[:, None] & act[None, :]
        Py = (Wy > 0) & same_net & act2
        C_new = same_net & act2 & ~A
        P_new = Py & ~A
        H, deg = heuristicas(A)
        scores = dict(H)
        scores.update(laterales(dim, aa, cut, n))
        scores["azar"] = np.random.default_rng(Y).random((n, n))
        scores["azar"] = (scores["azar"] + scores["azar"].T) / 2
        known = deg > 0
        iu = np.where(np.triu(C_new, 1))
        y = P_new[iu]
        both_known = known[iu[0]] & known[iu[1]]
        hard = same_cab[iu]
        # concentración de los positivos nuevos en pocas incidencias
        per_inc = []
        for nodes in sel_y.nodes:
            if len(nodes) >= 2:
                sub = P_new[np.ix_(nodes, nodes)]
                per_inc.append(int(np.triu(sub, 1).sum()))
        per_inc = sorted([v for v in per_inc if v > 0], reverse=True)
        rows_conc.append(dict(anio=Y, aristas_entreno=int(np.triu(A & same_net, 1).sum()),
                              densidad_entreno=np.triu(A & same_net & act2, 1).sum() / np.triu(same_net & act2, 1).sum(),
                              CT_sin_aristas=int((act & ~known).sum()), candidatos=len(y), positivos_nuevos=int(y.sum()),
                              tasa_base=y.mean(), positivos_ambos_conocidos=int((y & both_known).sum()),
                              incidencias_con_nuevos=len(per_inc),
                              parte_mayor_incidencia=(per_inc[0] / max(sum(per_inc), 1)) if per_inc else np.nan,
                              pares_repetidos=int(np.triu(Py & A, 1).sum())))
        if y.sum() == 0:
            continue
        rng = np.random.default_rng(Y)
        pos_pairs = (iu[0][y], iu[1][y])
        queries = np.where(P_new.any(1))[0]
        for name, S in scores.items():
            s = S[iu]
            row = dict(anio=Y, puntuacion=name, positivos=int(y.sum()), candidatos=len(y),
                       roc_auc=roc_auc_score(y, s), pr_auc=average_precision_score(y, s))
            row["roc_auc_cuaderno"], _, _ = auc_muestreo_cuaderno(S, pos_pairs, n, rng)
            if (y & hard).sum() and (~y & hard).sum():
                m = hard
                row["roc_auc_misma_cabecera"] = roc_auc_score(y[m], s[m])
            if (y & both_known).sum() and (~y & both_known).sum():
                m = both_known
                row["roc_auc_ambos_conocidos"] = roc_auc_score(y[m], s[m])
                row["pr_auc_ambos_conocidos"] = average_precision_score(y[m], s[m])
            rows_new.append(row)
            mrr, hits, nq = ranking(S, C_new, P_new, queries)
            rows_rank.append(dict(anio=Y, puntuacion=name, consultas=nq, mrr=mrr, hits10=hits[10], hits50=hits[50]))
        # todos los pares de Y (repetidos incluidos), candidatos: misma red y vigentes
        C_all = same_net & act2
        iu2 = np.where(np.triu(C_all, 1))
        y2 = Py[iu2]
        for name, S in {"persistencia (arista de entrenamiento)": A.astype(float),
                        "numero de co-cortes": W.astype(float), "vecinos_comunes": H["vecinos_comunes"],
                        "adamic_adar": H["adamic_adar"], "mismo_grupo": scores["mismo_grupo"],
                        "misma_cabecera": scores["misma_cabecera"]}.items():
            s = S[iu2]
            rows_all.append(dict(anio=Y, puntuacion=name, positivos=int(y2.sum()), candidatos=len(y2),
                                 roc_auc=roc_auc_score(y2, s), pr_auc=average_precision_score(y2, s),
                                 roc_auc_cuaderno=auc_muestreo_cuaderno(S, (iu2[0][y2], iu2[1][y2]), n, rng)[0]))
        print(f"  {Y}: {time.time() - t0:.1f} s")

    conc = pd.DataFrame(rows_conc)
    print("\n== Grafo de entrenamiento y positivos nuevos por anio evaluado")
    print(conc.to_string(index=False, float_format=F3))

    new = pd.DataFrame(rows_new)
    rank = pd.DataFrame(rows_rank)
    new.to_csv(salida("lp_nuevos_por_anio.csv"), index=False)
    rank.to_csv(salida("lp_ranking_por_anio.csv"), index=False)
    cols = ["roc_auc", "pr_auc", "roc_auc_cuaderno", "roc_auc_misma_cabecera", "roc_auc_ambos_conocidos",
            "pr_auc_ambos_conocidos"]
    for title, yrs in (("2019 - 2026", YEARS), ("2022 - 2026 (con informacion de map_aguas_arriba)", list(range(2022, 2027)))):
        sub = new[new.anio.isin(yrs)]
        lat = sub.groupby("puntuacion").anio.nunique()
        keep = lat[lat == sub.anio.nunique()].index
        sub = sub[sub.puntuacion.isin(keep)]
        tb = sub.groupby("puntuacion")[cols].mean()
        base = sub.groupby("anio").apply(lambda d: d.positivos.iloc[0] / d.candidatos.iloc[0], include_groups=False).mean()
        tb = tb.sort_values("pr_auc", ascending=False)
        print(f"\n== Enlaces nuevos {title}: media de los anios (tasa base media {base:.4f})")
        print(tb.to_string(float_format=F3))
        rk = rank[rank.anio.isin(yrs) & rank.puntuacion.isin(keep)]
        rk = rk.assign(m=rk.mrr * rk.consultas, h10=rk.hits10 * rk.consultas, h50=rk.hits50 * rk.consultas)
        rk = rk.groupby("puntuacion")[["consultas", "m", "h10", "h50"]].sum()
        rk["mrr"], rk["hits10"], rk["hits50"] = rk.m / rk.consultas, rk.h10 / rk.consultas, rk.h50 / rk.consultas
        print(f"\n== Ranking filtrado por CT, enlaces nuevos {title} (ponderado por consultas)")
        print(rk[["consultas", "mrr", "hits10", "hits50"]].sort_values("mrr", ascending=False).to_string(float_format=F3))
    print("\n== PR-AUC de enlaces nuevos por anio")
    print(new.pivot_table(index="puntuacion", columns="anio", values="pr_auc").to_string(float_format=F3))
    print("\n== MRR filtrado por anio")
    print(rank.pivot_table(index="puntuacion", columns="anio", values="mrr").to_string(float_format=F3))

    alld = pd.DataFrame(rows_all)
    alld.to_csv(salida("lp_todos_por_anio.csv"), index=False)
    print("\n== Todos los pares de Y (repetidos y nuevos), media 2019 - 2026")
    print(alld.groupby("puntuacion")[["positivos", "roc_auc", "pr_auc", "roc_auc_cuaderno"]].mean()
          .sort_values("pr_auc", ascending=False).to_string(float_format=F3))

    # ---- partición aleatoria de aristas, como RandomLinkSplit del laboratorio
    W_all, _, _ = grafo(inc, n, "2016-01-01", FIN_DATOS)
    act_all = activos(dim, "2016-01-01", FIN_DATOS)
    A_all = (W_all > 0) & same_net & (act_all[:, None] & act_all[None, :])
    e0, e1 = np.where(np.triu(A_all, 1))
    rows_rs = []
    for seed in range(SEEDS):
        rng = np.random.default_rng(seed)
        test = rng.random(len(e0)) < 0.10
        A_tr = A_all.copy()
        A_tr[e0[test], e1[test]] = False
        A_tr[e1[test], e0[test]] = False
        H, _ = heuristicas(A_tr)
        P = np.zeros_like(A_tr)
        P[e0[test], e1[test]] = True
        C = np.triu(same_net & ~A_tr & (act_all[:, None] & act_all[None, :]), 1)
        iu = np.where(C)
        yy = P[iu]
        for name, S in H.items():
            rows_rs.append(dict(semilla=seed, puntuacion=name, positivos=int(yy.sum()), candidatos=len(yy),
                                roc_auc=roc_auc_score(yy, S[iu]), pr_auc=average_precision_score(yy, S[iu]),
                                roc_auc_cuaderno=auc_muestreo_cuaderno(S, (e0[test], e1[test]), n, rng)[0]))
    rs = pd.DataFrame(rows_rs)
    print(f"\n== Particion aleatoria del 10 % de las {len(e0):,} aristas 2016 - 2026 (media de {SEEDS} semillas)")
    print(rs.groupby("puntuacion")[["positivos", "candidatos", "roc_auc", "pr_auc", "roc_auc_cuaderno"]].mean()
          .sort_values("pr_auc", ascending=False).to_string(float_format=F3))

    # ---- deriva: volver a cortarse juntos según la edad del último co-corte
    rows_age = []
    for Y in YEARS:
        cut = pd.Timestamp(f"{Y}-01-01")
        _, L, _ = grafo(inc, n, "2016-01-01", cut)
        Wy, _, _ = grafo(inc, n, cut, fin_anio(Y))
        act = activos(dim, cut, fin_anio(Y))
        C = np.triu(same_net & (act[:, None] & act[None, :]), 1)
        iu = np.where(C)
        age = np.where(L[iu] > 0, Y - L[iu], 0)
        yy = (Wy > 0)[iu]
        for a in np.unique(age):
            m = age == a
            rows_age.append(dict(anio=Y, edad=int(a), pares=int(m.sum()), positivos=int(yy[m].sum())))
    age = pd.DataFrame(rows_age)
    age["tramo"] = age.edad.map(lambda a: "nunca" if a == 0 else (str(a) if a <= 5 else "6 o mas"))
    t = age.groupby("tramo")[["pares", "positivos"]].sum()
    t["tasa"] = t.positivos / t.pares
    order = ["1", "2", "3", "4", "5", "6 o mas", "nunca"]
    print("\n== Probabilidad de cortarse juntos en Y segun los anios desde el ultimo co-corte (2019 - 2026 juntos)")
    print(t.reindex(order).to_string(float_format=lambda v: f"{v:.4f}"))
    t.reindex(order).to_csv(salida("lp_edad_ultimo_cocorte.csv"))

    # ---- ventanas de entrenamiento y decaimiento, todos los pares de Y
    rows_win = []
    for Y in YEARS:
        cut = pd.Timestamp(f"{Y}-01-01")
        Wy, _, _ = grafo(inc, n, cut, fin_anio(Y))
        act = activos(dim, cut, fin_anio(Y))
        C = np.triu(same_net & (act[:, None] & act[None, :]), 1)
        iu = np.where(C)
        yy = (Wy > 0)[iu]
        cands = {}
        for k in (1, 2, 3):
            Wk, _, _ = grafo(inc, n, max(pd.Timestamp("2016-01-01"), pd.Timestamp(f"{Y - k}-01-01")), cut)
            cands[f"co-corte en los {k} anios anteriores"] = (Wk > 0).astype(float)
            cands[f"numero de co-cortes, {k} anios"] = Wk
        W, _, _ = grafo(inc, n, "2016-01-01", cut)
        cands["co-corte desde 2016"] = (W > 0).astype(float)
        cands["numero de co-cortes desde 2016"] = W
        for tau in (0.5, 1.0, 2.0, 4.0):
            cands[f"decaimiento exponencial, tau {tau} anios"] = grafo_decay(inc, n, cut, tau)
        for name, S in cands.items():
            rows_win.append(dict(anio=Y, puntuacion=name, roc_auc=roc_auc_score(yy, S[iu]),
                                 pr_auc=average_precision_score(yy, S[iu]), tasa_base=yy.mean()))
    win = pd.DataFrame(rows_win)
    win.to_csv(salida("lp_ventanas_por_anio.csv"), index=False)
    print("\n== Ventana de entrenamiento: todos los pares de Y, media 2019 - 2026")
    print(win.groupby("puntuacion")[["roc_auc", "pr_auc"]].mean().sort_values("pr_auc", ascending=False)
          .to_string(float_format=F3))
    print("\n== PR-AUC por anio de las ventanas")
    print(win.pivot_table(index="puntuacion", columns="anio", values="pr_auc").to_string(float_format=F3))

    # ---- separación entre el corte y el año evaluado
    rows_gap = []
    for Y in range(2021, 2027):
        Wy, _, _ = grafo(inc, n, pd.Timestamp(f"{Y}-01-01"), fin_anio(Y))
        act = activos(dim, f"{Y}-01-01", fin_anio(Y))
        act2 = act[:, None] & act[None, :]
        for g in (1, 2, 3):
            cut = pd.Timestamp(f"{Y - g + 1}-01-01")
            W, _, _ = grafo(inc, n, "2016-01-01", cut)
            A = W > 0
            H, _ = heuristicas(A)
            C = np.triu(same_net & act2, 1)
            iu = np.where(C)
            yy = (Wy > 0)[iu]
            for name, S in (("persistencia", A.astype(float)), ("numero de co-cortes", W),
                            ("adamic_adar", H["adamic_adar"])):
                rows_gap.append(dict(anio=Y, separacion=g, puntuacion=name, roc_auc=roc_auc_score(yy, S[iu]),
                                     pr_auc=average_precision_score(yy, S[iu])))
            Cn = np.triu(same_net & act2 & ~A, 1)
            iun = np.where(Cn)
            yn = (Wy > 0)[iun]
            if yn.sum():
                for name in ("adamic_adar", "vecinos_comunes"):
                    rows_gap.append(dict(anio=Y, separacion=g, puntuacion=f"{name} (solo nuevos)",
                                         roc_auc=roc_auc_score(yn, H[name][iun]),
                                         pr_auc=average_precision_score(yn, H[name][iun])))
    gap = pd.DataFrame(rows_gap)
    gap.to_csv(salida("lp_separacion.csv"), index=False)
    print("\n== Separacion de 1, 2 o 3 anios entre el corte y el anio evaluado (Y = 2021 - 2026, media)")
    print(gap.pivot_table(index="puntuacion", columns="separacion", values=["roc_auc", "pr_auc"])
          .to_string(float_format=F3))
    print(f"\nTiempo total: {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
