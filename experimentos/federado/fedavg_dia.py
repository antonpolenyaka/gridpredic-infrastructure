"""
Aprendizaje federado simulado entre las distribuidoras con el modelo del día (cuaderno 3w): si cada red entrenara
en su casa y solo compartiera números, ¿perderíamos algo frente a juntar los datos? Misma tabla (dia.csv.gz de
experimentos/perfil_dia/exportar_dia.py), misma variante operativa, misma evaluación por años y misma comparación
con la previsión real de la víspera que job_ml_riesgo_dia.py:

    python fedavg_dia.py <dia.csv.gz> <bloques.json> <salida/>

Para cada año se ajustan con los días anteriores:

- produccion: la logística L2 de producción sobre los días de las tres redes juntos (ml_models, C elegida en el
  25 % interno, columnas dist_* penalizadas como el resto).
- produccion_int: la misma, con el intercepto de cada red reajustado sin penalización y las pendientes fijas
  (ml_models.intercept_with_offset sobre los días de entrenamiento de cada red): la corrección por la tasa base de
  cada red.
- local: un modelo por red con sus días (sin dist_*), C elegida en su propio 25 % interno.
- fed_1ronda: los modelos locales, promedio de las pendientes ponderado por los días con avería de cada red y el
  intercepto de cada red reajustado en local con las pendientes del promedio (FedAvg de una ronda).
- fed_exacto: pendientes comunes e intercepto propio de cada red, con la penalización L2 de producción, optimizado
  con L-BFGS sumando en el servidor la pérdida y el gradiente que manda cada red en cada ronda (solo salen 33 números por
  red y ronda: las 32 pendientes y el intercepto). Es el mismo óptimo que juntar los datos con un intercepto por red sin penalizar; se
  cuenta cuántas rondas hacen falta. La C es la que se elige con los datos juntos (la de producción): en
  un federado real habría que elegirla también por rondas, así que la cifra es algo optimista.

La estandarización es común a todos (la media y la desviación de cada variable salen de sumar conteos, sumas
y sumas de cuadrados de cada red; en la variante operativa no hay huecos que imputar desde el 25.06.2018). Deja
fedavg_dia.txt con las métricas, las diferencias emparejadas y las rondas; las predicciones por día no se guardan.
Tarda menos de un minuto.
"""

import json
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from sklearn.linear_model import LogisticRegression

warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "04_ml"))
import ml_metrics  # noqa: E402
import ml_models  # noqa: E402

SEED = 20261010
LABEL = "y_dia"
DESDE = "2018-06-25"
SHARE = 0.25
GRID_C = [0.003, 0.01, 0.03, 0.1]
NAMES = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}


def load(data_path, blocks_path):
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    dist = list(blocks["distribuidora"])
    feats = []
    for block in ["calendario", "historia", "avisos", "dia_comun", "extremos"]:
        feats += blocks[block]
    feats += ["ayer_racha_max", "ayer_lluvia_max"]
    frame = pd.read_csv(data_path, parse_dates=["fecha"])
    rows = frame[frame["fecha"] >= pd.Timestamp(DESDE)].sort_values(["fecha", "distribuidora_id"]).reset_index(drop=True)
    rows["anio"] = rows["fecha"].dt.year
    common = [c[len("dia_"):] for c in blocks["dia_comun"]]
    swap = [(f"dia_{n}", f"prev_{n}") for n in common if f"prev_{n}" in rows.columns]
    swap += [(c, f"prev_{c}") for c in feats if c.startswith("ext_") and f"prev_{c}" in rows.columns]
    return rows, dist, feats, swap


def slog(x):
    return np.sign(x) * np.log1p(np.abs(x))


class Scale:
    """Estandarización común con estadísticos sumables (conteo, suma y suma de cuadrados por red)."""

    def __init__(self, parts):
        n = sum(len(p) for p in parts)
        s = sum(slog(p).sum(axis=0) for p in parts)
        q = sum((slog(p) ** 2).sum(axis=0) for p in parts)
        self.mu = s / n
        self.sd = np.sqrt(np.maximum(q / n - self.mu ** 2, 1e-12))

    def __call__(self, x):
        return (slog(x) - self.mu) / self.sd


def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-z))


def choose_c(X, y, Xv, yv):
    best, best_ap = GRID_C[0], -1.0
    for c in GRID_C:
        m = LogisticRegression(C=c, max_iter=1000, solver="lbfgs", random_state=SEED).fit(X, y)
        ap = ml_metrics.average_precision(yv, m.predict_proba(Xv)[:, 1]) if yv.sum() else -1.0
        if ap > best_ap:
            best, best_ap = c, ap
    return best


def fed_exact(Xs, ys, c):
    """min sum_d sum_i logloss(x_i w + b_d) + ||w||^2 / (2c); cada evaluación es una ronda de comunicación."""
    k, p = len(Xs), Xs[0].shape[1]
    rounds = {"n": 0}

    def f(theta):
        w, b = theta[:p], theta[p:]
        rounds["n"] += 1
        loss, grad_w, grad_b = 0.5 * w @ w / c, w / c, np.zeros(k)
        for d, (X, y) in enumerate(zip(Xs, ys)):   # lo que calcula cada red con sus días
            z = X @ w + b[d]
            loss += np.sum(np.logaddexp(0.0, z) - y * z)
            r = sigmoid(z) - y
            grad_w = grad_w + X.T @ r
            grad_b[d] = r.sum()
        return loss, np.concatenate([grad_w, grad_b])

    res = minimize(f, np.zeros(p + k), jac=True, method="L-BFGS-B", options={"maxiter": 2000, "gtol": 1e-8})
    return res.x[:p], res.x[p:], rounds["n"]


def evaluate_year(rows, dist, feats, year, swap):
    start = pd.Timestamp(f"{year}-01-01")
    before = rows[rows["fecha"] < start]
    target = rows[rows["anio"] == year].copy()
    if swap:
        scoring = target.copy()
        for column, source in swap:
            scoring[column] = target[source].to_numpy()
        keep = target[[s for col, s in swap if col.startswith("dia_")]].notna().all(axis=1).to_numpy()
    else:
        scoring, keep = target, np.ones(len(target), dtype=bool)
    # Un gemelo de previsión sin dato (pocas filas) se rellena con la mediana de los días de entrenamiento, como
    # hace el SimpleImputer de la tubería de producción; los modelos sin tubería lo necesitan explícito.
    info_na = int(scoring.loc[keep, feats].isna().any(axis=1).sum())
    scoring = scoring.copy()
    scoring[feats] = scoring[feats].fillna(before[feats].median())
    days = np.sort(before["fecha"].unique())
    cut = days[int(len(days) * (1 - SHARE))]
    fit_part, check_part = before[before["fecha"] < cut], before[before["fecha"] >= cut]
    out = target[["distribuidora_id", "fecha", "anio", LABEL]].copy()
    info = {"anio": year, "filas_con_hueco_en_la_prevision": info_na}

    # produccion: la tubería de ml_models con las columnas dist_*
    allf = dist + feats

    def data(frame):
        return frame[allf].to_numpy(np.float32, na_value=np.nan)

    inner = {"X": data(fit_part), "y": fit_part[LABEL].to_numpy(np.int8), "w": np.ones(len(fit_part)),
             "Xv": data(check_part), "yv": check_part[LABEL].to_numpy(np.int8), "wv": np.ones(len(check_part)),
             "feature_names": allf}
    best, best_ap = None, -1.0
    for c in GRID_C:
        s = ml_models.fit_model("logistica", {"C": c}, inner, 1.0, SEED, -1)
        ap = ml_metrics.average_precision(inner["yv"], s.score(inner["Xv"]))
        if best is None or ap > best_ap:
            best, best_ap = s, ap
    c_prod = best.params["C"]
    full = {**inner, "X": data(before), "y": before[LABEL].to_numpy(np.int8), "w": np.ones(len(before))}
    prod = ml_models.fit_model("logistica", {"C": c_prod}, full, 1.0, SEED, -1)
    out["p_produccion"] = prod.score(data(scoring))
    info["C_produccion"] = c_prod

    # produccion_int: intercepto de cada red sin penalizar, pendientes fijas
    eta_tr = prod.estimator.decision_function(data(before))
    eta_te = prod.estimator.decision_function(data(scoring))
    shift = np.zeros(len(out))
    for d in sorted(rows["distribuidora_id"].unique()):
        m_tr = (before["distribuidora_id"] == d).to_numpy()
        b = ml_models.intercept_with_offset(before[LABEL].to_numpy()[m_tr].astype(float), np.ones(m_tr.sum()),
                                            eta_tr[m_tr])
        shift[(out["distribuidora_id"] == d).to_numpy()] = b
        info[f"desplazamiento_red{NAMES[d][-1]}"] = round(float(b), 3)
    out["p_produccion_int"] = sigmoid(eta_te + shift)

    # Estandarización común, sin columnas dist_*
    nets = sorted(rows["distribuidora_id"].unique())

    def raw(frame):
        return frame[feats].to_numpy(np.float64)

    scale = Scale([raw(before[before["distribuidora_id"] == d]) for d in nets])
    Xd = {d: scale(raw(before[before["distribuidora_id"] == d])) for d in nets}
    yd = {d: before.loc[before["distribuidora_id"] == d, LABEL].to_numpy(np.float64) for d in nets}
    Xte = scale(raw(scoring))
    dte = out["distribuidora_id"].to_numpy()

    # local: un modelo por red, C en su propio 25 % interno
    local = {}
    for d in nets:
        fp, cp = fit_part[fit_part["distribuidora_id"] == d], check_part[check_part["distribuidora_id"] == d]
        c = choose_c(scale(raw(fp)), fp[LABEL].to_numpy(), scale(raw(cp)), cp[LABEL].to_numpy())
        m = LogisticRegression(C=c, max_iter=2000, solver="lbfgs", random_state=SEED).fit(Xd[d], yd[d])
        local[d] = m
        info[f"C_local_red{NAMES[d][-1]}"] = c
    out["p_local"] = np.nan
    for d in nets:
        mask = dte == d
        out.loc[mask, "p_local"] = local[d].predict_proba(Xte[mask])[:, 1]

    # fed_1ronda: promedio de pendientes ponderado por los días con avería, intercepto reajustado en cada red
    weights = np.array([yd[d].sum() for d in nets], dtype=float)
    w_avg = sum(wt * local[d].coef_[0] for wt, d in zip(weights, nets)) / weights.sum()
    out["p_fed_1ronda"] = np.nan
    for d in nets:
        b = ml_models.intercept_with_offset(yd[d], np.ones(len(yd[d])), Xd[d] @ w_avg)
        mask = dte == d
        out.loc[mask, "p_fed_1ronda"] = sigmoid(Xte[mask] @ w_avg + b)

    # fed_exacto: rondas de gradiente sumado en el servidor
    w_ex, b_ex, rounds = fed_exact([Xd[d] for d in nets], [yd[d] for d in nets], c_prod)
    out["p_fed_exacto"] = np.nan
    for i, d in enumerate(nets):
        mask = dte == d
        out.loc[mask, "p_fed_exacto"] = sigmoid(Xte[mask] @ w_ex + b_ex[i])
    info["rondas_fed_exacto"] = rounds
    corr = np.corrcoef(w_ex, prod.estimator.named_steps["modelo"].coef_[0][len(dist):len(dist) + len(feats)])[0, 1]
    info["corr_pendientes_fed_exacto_produccion"] = round(float(corr), 3)
    info["corr_pendientes_1ronda_exacto"] = round(float(np.corrcoef(w_avg, w_ex)[0, 1]), 3)

    # transferencia: el modelo local de la Distribuidora 1 puntúa la 2 (con su intercepto y con el de la 2)
    m2 = dte == 2
    out["p_transfer_d1"] = np.nan
    out.loc[m2, "p_transfer_d1"] = local[3].predict_proba(Xte[m2])[:, 1]
    return out[keep], info


def summarize(scored, title):
    y, year = scored[LABEL].to_numpy(), scored["anio"].to_numpy()
    cols = ["p_produccion", "p_produccion_int", "p_local", "p_fed_1ronda", "p_fed_exacto"]
    lines = [f"== {title}: {len(scored)} días de red, {int(y.sum())} con avería"]
    rows = []
    for c in cols:
        s = scored[c].to_numpy(np.float64)
        cal = ml_metrics.calibration(y, s)
        row = {"modelo": c[2:], "pr_auc": ml_metrics.average_precision(y, s), "roc_auc": ml_metrics.roc_auc(y, s),
               "top10": top_share(y, s, year, 0.1), "top20": top_share(y, s, year, 0.2), "bss": cal["bss"],
               "pendiente": cal["pendiente"], "intercepto": cal["intercepto"]}
        for d in (3, 2):
            m = (scored["distribuidora_id"] == d).to_numpy()
            row[f"pr_auc_{NAMES[d][-1]}"] = ml_metrics.average_precision(y[m], s[m])
        rows.append(row)
    lines.append(pd.DataFrame(rows).round(4).to_string(index=False))
    _, rep = ml_metrics.bootstrap_pr_auc(y, np.ones(len(y)), scored["fecha"].to_numpy(),
                                         {c[2:]: scored[c].to_numpy() for c in cols}, 500, SEED, 7)
    pairs = [(c[2:], "produccion") for c in cols[1:]] + [("fed_1ronda", "local"), ("fed_exacto", "fed_1ronda")]
    lines.append("-- Diferencias emparejadas de PR-AUC (bloques de 7 días, 500 réplicas)")
    lines.append(ml_metrics.paired_differences(rep, pairs).round(4).to_string(index=False))
    # Por red: lo que gana cada distribuidora con el federado frente a su modelo local
    for d in (3, 2):
        m = (scored["distribuidora_id"] == d).to_numpy()
        sub = scored[m]
        _, rep_d = ml_metrics.bootstrap_pr_auc(sub[LABEL].to_numpy(), np.ones(m.sum()), sub["fecha"].to_numpy(),
                                               {c[2:]: sub[c].to_numpy() for c in cols}, 500, SEED, 7)
        lines.append(f"-- {NAMES[d]} ({int(sub[LABEL].sum())} días con avería)")
        lines.append(ml_metrics.paired_differences(rep_d, [("fed_exacto", "local"), ("fed_1ronda", "local"),
                                                           ("produccion", "local")]).round(4).to_string(index=False))
    m2 = (scored["distribuidora_id"] == 2).to_numpy()
    lines.append(f"-- Transferencia a la Distribuidora 2: modelo local de la 1 "
                 f"{ml_metrics.average_precision(y[m2], scored.loc[m2, 'p_transfer_d1'].to_numpy()):.4f} frente al "
                 f"local de la 2 {ml_metrics.average_precision(y[m2], scored.loc[m2, 'p_local'].to_numpy()):.4f}")
    for d in sorted(scored["distribuidora_id"].unique()):
        m = (scored["distribuidora_id"] == d).to_numpy()
        lines.append(f"   probabilidad media {NAMES[d]}: produccion {scored.loc[m, 'p_produccion'].mean():.4f}, "
                     f"produccion_int {scored.loc[m, 'p_produccion_int'].mean():.4f}, tasa y_dia {y[m].mean():.4f}, "
                     f"tasa y_dia_meteo {scored.loc[m, 'y_dia_meteo'].mean():.4f}")
    return "\n".join(lines)


def top_share(y, s, year, share, seed=0):
    rng = np.random.default_rng(seed)
    tie = rng.random(len(y))
    hits = positives = 0
    for value in np.unique(year):
        m = year == value
        order = np.lexsort((-tie[m], -np.nan_to_num(s[m], nan=-np.inf)))
        k = int(round(m.sum() * share))
        hits += int(y[m][order[:k]].sum())
        positives += int(y[m].sum())
    return hits / positives


def main(argv):
    data_path, blocks_path, out = argv[0], argv[1], argv[2]
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    rows, dist, feats, swap = load(data_path, blocks_path)
    lines = [f"Filas {len(rows)}, {len(dist) + len(feats)} variables con dist_*; por red: "
             + ", ".join(f"{NAMES[d]} {int(g[LABEL].sum())} días con avería" for d, g in rows.groupby('distribuidora_id'))]
    for title, years, sw in (("Reanálisis 2022 - 2026", [2022, 2023, 2024, 2025, 2026], None),
                             ("Previsión real 2024 - 2026", [2024, 2025, 2026], swap)):
        parts, infos = [], []
        for year in years:
            part, info = evaluate_year(rows, dist, feats, year, sw)
            part = part.merge(rows[["distribuidora_id", "fecha", "y_dia_meteo"]], on=["distribuidora_id", "fecha"])
            parts.append(part)
            infos.append(info)
        scored = pd.concat(parts, ignore_index=True)
        lines.append(summarize(scored, title))
        lines.append(pd.DataFrame(infos).to_string(index=False))
    lines.append(f"Coste de comunicación por ronda: {len(feats) + 1} números por red; centralizar: unos "
                 f"{int(len(rows) / 3)} días x {len(feats)} variables por red ({int(len(rows) / 3) * len(feats)} números)")
    lines.append(f"Tiempo {time.time() - t0:.0f} s")
    text = "\n".join(lines)
    print(text)
    with open(os.path.join(out, "fedavg_dia.txt"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
