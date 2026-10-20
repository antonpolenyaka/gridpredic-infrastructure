"""
E1b. Detalle de la predicción de enlaces nuevos 2022 - 2026 (prediccion_enlaces.py):

- Que son los negativos que saca el muestreo del cuaderno (un extremo
  cambiado por un nodo cualquiera, sin filtrar): de otra red, aristas de
  entrenamiento, positivos del año evaluado o negativos de verdad.
- Pares nuevos con un CT sin ninguna arista de entrenamiento (arranque en
  frio) frente a pares de dos CT ya conocidos.
- Una combinación sencilla (media de percentiles por año) de los elementos
  aguas arriba compartidos y la cercanía del municipio, y una logística con
  esas puntuaciones entrenada con los años anteriores.

Uso: python prediccion_enlaces_detalle.py <carpeta de la exportación de Gold> [salida/]
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import activos, cargar, grafo, salida  # noqa: E402
from prediccion_enlaces import fin_anio, heuristicas, laterales  # noqa: E402

F3 = lambda v: f"{v:.3f}"  # noqa: E731
pd.set_option("display.width", 220)


def pct(x):
    return rankdata(x) / len(x)


def main():
    dim, it, inc, aa = cargar()
    n = len(dim)
    red = dim.distribuidora_id.values
    same_net = red[:, None] == red[None, :]
    np.fill_diagonal(same_net, False)
    rows_neg, rows_type, feats = [], [], []
    for Y in range(2019, 2027):
        cut = pd.Timestamp(f"{Y}-01-01")
        W, _, _ = grafo(inc, n, "2016-01-01", cut)
        A = W > 0
        Wy, _, _ = grafo(inc, n, cut, fin_anio(Y))
        act = activos(dim, cut, fin_anio(Y))
        act2 = act[:, None] & act[None, :]
        Py = (Wy > 0) & same_net & act2
        P_new = Py & ~A
        C_new = same_net & act2 & ~A
        iu = np.where(np.triu(C_new, 1))
        y = P_new[iu]
        # composición de los negativos del cuaderno
        rng = np.random.default_rng(Y)
        a, b = iu[0][y], iu[1][y]
        m = len(a)
        mask = rng.random(m) > 0.5
        r = rng.integers(0, n, m)
        na, nb = np.where(mask, r, a), np.where(mask, b, r)
        cross = red[na] != red[nb]
        selfp = na == nb
        train = A[na, nb] & ~cross
        posy = Py[na, nb] & ~cross & ~train
        rows_neg.append(dict(anio=Y, negativos=m, otra_red=cross.mean(), mismo_nodo=selfp.mean(),
                             arista_entreno=train.mean(), positivo_del_anio=posy.mean(),
                             negativo_de_verdad=(~cross & ~selfp & ~train & ~posy).mean()))
        if Y < 2022:
            continue
        H, deg = heuristicas(A)
        L = laterales(dim, aa, cut, n)
        known = deg > 0
        cold = ~(known[iu[0]] & known[iu[1]])
        sc = {"elementos_compartidos": L["elementos_compartidos"][iu], "coseno_tfidf": L["coseno_tfidf"][iu],
              "cercania_municipio": L["cercania_municipio"][iu], "adamic_adar": H["adamic_adar"][iu],
              "misma_cabecera": L["misma_cabecera"][iu], "mismo_grupo": L["mismo_grupo"][iu]}
        sc["elementos + cercania (percentiles)"] = (pct(sc["elementos_compartidos"]) + pct(sc["cercania_municipio"])) / 2
        sc["elementos + cercania + adamic_adar (percentiles)"] = (
            pct(sc["elementos_compartidos"]) + pct(sc["cercania_municipio"]) + pct(sc["adamic_adar"])) / 3
        for name, s in sc.items():
            for tipo, msk in (("con un CT sin aristas", cold), ("dos CT conocidos", ~cold), ("todos", np.ones_like(cold))):
                if y[msk].sum() and (~y[msk]).sum():
                    rows_type.append(dict(anio=Y, puntuacion=name, tipo=tipo, positivos=int(y[msk].sum()),
                                          candidatos=int(msk.sum()), roc_auc=roc_auc_score(y[msk], s[msk]),
                                          pr_auc=average_precision_score(y[msk], s[msk]), tasa=y[msk].mean()))
        X = np.c_[np.log1p(sc["elementos_compartidos"]), sc["cercania_municipio"] / 100.0,
                  np.log1p(sc["adamic_adar"]), cold.astype(float), sc["misma_cabecera"]]
        feats.append((Y, X, y, cold))
    neg = pd.DataFrame(rows_neg)
    print("== Que negativos saca el muestreo del cuaderno para los enlaces nuevos de cada anio")
    print(neg.to_string(index=False, float_format=F3))
    # logística con los años anteriores (el primero que se puede puntuar es 2023)
    for Y, X, y, cold in feats:
        tr = [(Xp, yp) for Yp, Xp, yp, _ in feats if Yp < Y]
        if not tr:
            continue
        Xt = np.vstack([t[0] for t in tr])
        yt = np.concatenate([t[1] for t in tr])
        mu, sd = Xt.mean(0), Xt.std(0) + 1e-9
        lr = LogisticRegression(C=1.0, max_iter=2000).fit((Xt - mu) / sd, yt)
        s = lr.predict_proba((X - mu) / sd)[:, 1]
        for tipo, msk in (("con un CT sin aristas", cold), ("dos CT conocidos", ~cold), ("todos", np.ones_like(cold))):
            if y[msk].sum() and (~y[msk]).sum():
                rows_type.append(dict(anio=Y, puntuacion="logistica con los anios anteriores", tipo=tipo,
                                      positivos=int(y[msk].sum()), candidatos=int(msk.sum()),
                                      roc_auc=roc_auc_score(y[msk], s[msk]), pr_auc=average_precision_score(y[msk], s[msk]),
                                      tasa=y[msk].mean()))
    t = pd.DataFrame(rows_type)
    t.to_csv(salida("lp_detalle_tipos.csv"), index=False)
    for tipo in ("todos", "con un CT sin aristas", "dos CT conocidos"):
        sub = t[(t.tipo == tipo)]
        print(f"\n== Enlaces nuevos 2022 - 2026, {tipo}: media de los anios")
        g = sub.groupby("puntuacion").agg(anios=("anio", "nunique"), positivos=("positivos", "sum"),
                                         candidatos=("candidatos", "sum"), tasa=("tasa", "mean"),
                                         roc_auc=("roc_auc", "mean"), pr_auc=("pr_auc", "mean"))
        print(g.sort_values("pr_auc", ascending=False).to_string(float_format=F3))
    sub = t[(t.tipo == "todos") & (t.anio >= 2023)]
    print("\n== Enlaces nuevos 2023 - 2026 (los anios que puntua la logistica), todos: media")
    print(sub.groupby("puntuacion")[["roc_auc", "pr_auc"]].mean().sort_values("pr_auc", ascending=False)
          .to_string(float_format=F3))
    print("\n== PR-AUC por anio, todos")
    print(t[t.tipo == "todos"].pivot_table(index="puntuacion", columns="anio", values="pr_auc").to_string(float_format=F3))


if __name__ == "__main__":
    main()
