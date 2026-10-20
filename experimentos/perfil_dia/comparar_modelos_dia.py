"""
La logística de producción frente a los árboles en el modelo del día, con el protocolo de job_ml_riesgo_dia.py y la
tabla que deja exportar_dia.py:

    python comparar_modelos_dia.py <dia.csv.gz> <bloques.json> <salida/>

Repite sin Spark la evaluación por años y la comparación con la previsión real de la víspera, con ml_models.fit_model
y ml_metrics del repositorio sin cambiarlos. Protocolo (el de rolling_evaluation y forecast_evaluation): variante
operativa (35 variables, desde el 25.06.2018); para cada año, la configuración de cada modelo se elige con la PR-AUC
del último 25 % de los días anteriores, se reajusta con todos ellos (XGBoost con los árboles de la parada temprana)
y puntúa el año. Reanálisis: 2022 - 2026. Previsión real: 2024 - 2026, con dia_* y ext_* cambiados por sus gemelos
prev_* y solo los días con previsión. Diferencias emparejadas frente a la logística con bootstrap por bloques de 7
días (500 réplicas, semilla 20261010), como meteo_dia.bootstrap_bloque_dias.

Modelos: logistica (rejilla de C del día), xgboost (rejilla del día: profundidad 2 y 3), random_forest (las dos
configuraciones de busqueda en config_ml.json, lo que usaría --modelos random_forest), rf_hoja20 (200 árboles sin
límite de profundidad, 20 filas por hoja, raíz de p), cart (árbol podado por coste-complejidad, alfa elegido en el
25 % interno), rango (media de los percentiles de la logística y XGBoost, calculados dentro de cada año puntuado, así que usa la
distribución de puntuaciones del propio año) y rango_rf (logística y bosque).

Deja comparar_modelos_dia.txt (las tres tablas, las configuraciones elegidas y las reglas del árbol de 2026) y los
CSV de métricas y diferencias. Las predicciones por día no se guardan.
"""

import json
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd

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
GRIDS = {
    "logistica": [{"C": 0.003}, {"C": 0.01}, {"C": 0.03}, {"C": 0.1}],
    "xgboost": [{"max_depth": 2, "min_child_weight": 5, "learning_rate": 0.05, "subsample": 0.8,
                 "colsample_bytree": 0.8, "n_estimators": 1500, "early_stopping_rounds": 100, "verbose": 0},
                {"max_depth": 3, "min_child_weight": 5, "learning_rate": 0.05, "subsample": 0.8,
                 "colsample_bytree": 0.8, "n_estimators": 1500, "early_stopping_rounds": 100, "verbose": 0}],
    "random_forest": [{"n_estimators": 200, "max_depth": 14, "min_samples_leaf": 50, "max_features": "sqrt",
                       "max_samples": 0.5},
                      {"n_estimators": 200, "max_depth": 20, "min_samples_leaf": 50, "max_features": 0.15,
                       "max_samples": 0.5, "class_weight": "balanced_subsample"}],
    "rf_hoja20": [{"n_estimators": 200, "max_depth": None, "min_samples_leaf": 20, "max_features": "sqrt",
                   "max_samples": None}],
}
TWIN_PREFIXES = ("ext_",)


def load(data_path, blocks_path):
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    features = list(blocks["distribuidora"])
    for block in ["calendario", "historia", "avisos", "dia_comun", "extremos"]:
        features += blocks[block]
    features += [v for v in ["ayer_racha_max", "ayer_lluvia_max"] if v not in features]
    frame = pd.read_csv(data_path, parse_dates=["fecha"])
    rows = frame[frame["fecha"] >= pd.Timestamp(DESDE)].sort_values(["fecha", "distribuidora_id"]).reset_index(drop=True)
    rows["anio"] = rows["fecha"].dt.year
    common = [c[len("dia_"):] for c in blocks["dia_comun"]]
    swap = [(f"dia_{n}", f"prev_{n}") for n in common if f"prev_{n}" in rows.columns]
    swap += [(c, f"prev_{c}") for c in features if c.startswith(TWIN_PREFIXES) and f"prev_{c}" in rows.columns]
    return rows, features, swap


def matrix(frame, features):
    return frame[features].to_numpy(dtype=np.float32, na_value=np.nan)


def fit_cart(X, y, Xv, yv, seed):
    """CART máximo, camino de poda por coste-complejidad y alfa elegido con la PR-AUC del 25 % interno."""
    from sklearn.tree import DecisionTreeClassifier

    path = DecisionTreeClassifier(random_state=seed).cost_complexity_pruning_path(X, y)
    alphas = np.unique(path.ccp_alphas)
    alphas = alphas[alphas >= 0]
    picks = np.unique(np.quantile(alphas, np.linspace(0, 1, 40)))
    best, best_ap, tried = None, -1.0, []
    for alpha in picks:
        tree = DecisionTreeClassifier(random_state=seed, ccp_alpha=float(alpha)).fit(X, y)
        ap = ml_metrics.average_precision(yv, tree.predict_proba(Xv)[:, 1])
        tried.append((float(alpha), tree.get_n_leaves(), ap))
        if ap > best_ap:
            best, best_ap = float(alpha), ap
    return best, best_ap, tried


def rolling(rows, features, years, swap=None):
    """Evaluación por años; con swap, puntúa con la previsión real (solo los días con previsión)."""
    from sklearn.tree import DecisionTreeClassifier

    parts, chosen, trees = [], [], {}
    for year in years:
        start = pd.Timestamp(f"{year}-01-01")
        before = rows[rows["fecha"] < start]
        target = rows[rows["anio"] == year].copy()
        days = np.sort(before["fecha"].unique())
        cut = days[int(len(days) * (1 - SHARE))]
        fit_part, check_part = before[before["fecha"] < cut], before[before["fecha"] >= cut]
        inner = {"X": matrix(fit_part, features), "y": fit_part[LABEL].to_numpy(np.int8),
                 "w": np.ones(len(fit_part)), "Xv": matrix(check_part, features),
                 "yv": check_part[LABEL].to_numpy(np.int8), "wv": np.ones(len(check_part)), "feature_names": features}
        full = {"X": matrix(before, features), "y": before[LABEL].to_numpy(np.int8), "w": np.ones(len(before)),
                "Xv": inner["Xv"], "yv": inner["yv"], "wv": inner["wv"], "feature_names": features}
        if swap:
            scoring = target.copy()
            for column, source in swap:
                scoring[column] = target[source].to_numpy()
            keep = target[[s for c, s in swap if c.startswith("dia_")]].notna().all(axis=1).to_numpy()
        else:
            scoring, keep = target, np.ones(len(target), dtype=bool)
        Xs = matrix(scoring, features)
        out = target[["distribuidora_id", "fecha", "anio", LABEL]].copy()
        for name, grid in GRIDS.items():
            model = "random_forest" if name.startswith("rf_") else name
            best, best_ap = None, -1.0
            for i, params in enumerate(grid):
                scorer = ml_models.fit_model(model, params, inner, 1.0, SEED, -1)
                ap = ml_metrics.average_precision(inner["yv"], scorer.score(inner["Xv"]))
                if best is None or ap > best_ap:
                    best, best_ap, best_i = scorer, ap, i
            params = dict(best.params)
            if model == "xgboost":
                trees_n = int(best.info["n_arboles"])
                params.update(n_estimators=trees_n, early_stopping_rounds=trees_n + 1)
            final = ml_models.fit_model(model, params, full, 1.0, SEED, -1)
            out[f"p_{name}"] = final.score(Xs)
            chosen.append({"anio": year, "modelo": name, "config": best_i, "pr_auc_interna": round(best_ap, 4),
                           **({"n_arboles": int(best.info["n_arboles"])} if model == "xgboost" else {}),
                           **({"C": params["C"]} if name == "logistica" else {})})
        alpha, ap, tried = fit_cart(inner["X"], inner["y"], inner["Xv"], inner["yv"], SEED)
        tree = DecisionTreeClassifier(random_state=SEED, ccp_alpha=alpha).fit(full["X"], full["y"])
        out["p_cart"] = tree.predict_proba(Xs)[:, 1]
        trees[year] = tree
        chosen.append({"anio": year, "modelo": "cart", "alfa": alpha, "hojas": int(tree.get_n_leaves()),
                       "profundidad": int(tree.get_depth()), "pr_auc_interna": round(ap, 4)})
        parts.append(out[keep])
    scored = pd.concat(parts, ignore_index=True)
    for name, pair in {"p_rango": ("p_logistica", "p_xgboost"), "p_rango_rf": ("p_logistica", "p_rf_hoja20")}.items():
        scored[name] = scored.groupby("anio")[list(pair)].rank(pct=True).mean(axis=1)
    return scored, pd.DataFrame(chosen), trees


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


def table(scored, title):
    y = scored[LABEL].to_numpy()
    year = scored["anio"].to_numpy()
    cols = [c for c in scored.columns if c.startswith("p_")]
    rows = []
    for c in cols:
        s = scored[c].to_numpy(dtype=np.float64)
        row = {"modelo": c[2:], "pr_auc": ml_metrics.average_precision(y, s), "roc_auc": ml_metrics.roc_auc(y, s),
               "top10": top_share(y, s, year, 0.1), "top20": top_share(y, s, year, 0.2)}
        if not c.startswith("p_rango"):
            cal = ml_metrics.calibration(y, s)
            row.update(bss=cal["bss"], pendiente=cal["pendiente"], intercepto=cal["intercepto"],
                       prob_media=cal["prob_media"])
        rows.append(row)
    res = pd.DataFrame(rows)
    _, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(y)), scored["fecha"].to_numpy(),
                                              {c[2:]: scored[c].to_numpy() for c in cols}, 500, SEED, 7)
    diffs = ml_metrics.paired_differences(replicas, [(c[2:], "logistica") for c in cols if c != "p_logistica"])
    extra = ml_metrics.paired_differences(replicas, [("rango", "xgboost"), ("rf_hoja20", "random_forest"),
                                                     ("cart", "rf_hoja20")])
    text = [f"== {title}: {len(scored)} días de red, {int(y.sum())} con avería, tasa {y.mean():.4f}",
            res.round(4).to_string(index=False), "-- Diferencias emparejadas de PR-AUC (bloques de 7 días, 500 réplicas)",
            pd.concat([diffs, extra]).round(4).to_string(index=False)]
    return "\n".join(text), res, pd.concat([diffs, extra])


def main(argv):
    data_path, blocks_path, out = argv[0], argv[1], argv[2]
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    rows, features, swap = load(data_path, blocks_path)
    lines = [f"Filas {len(rows)} desde {DESDE}, {int(rows[LABEL].sum())} días con avería, {len(features)} variables",
             f"Cambio por la previsión: {swap}"]
    rean, chosen_r, _ = rolling(rows, features, [2022, 2023, 2024, 2025, 2026])
    txt, res_r, dif_r = table(rean, "Reanálisis 2022 - 2026")
    lines += [txt]
    prev, chosen_p, trees = rolling(rows, features, [2024, 2025, 2026], swap)
    txt, res_p, dif_p = table(prev, "Previsión real 2024 - 2026")
    lines += [txt]
    sub = prev[prev["anio"] <= 2025]
    txt, _, _ = table(sub, "Previsión real 2024 - 2025")
    lines += [txt]
    lines += ["-- Configuraciones elegidas (previsión real; el ajuste es el mismo que con el reanálisis)",
              chosen_p.to_string(index=False)]
    from sklearn.tree import export_text
    lines += ["-- CART del último año (ajustado con los días anteriores a 2026)",
              export_text(trees[2026], feature_names=features, show_weights=True, max_depth=6)]
    lines.append(f"Tiempo {time.time() - t0:.0f} s")
    text = "\n".join(lines)
    print(text)
    with open(os.path.join(out, "comparar_modelos_dia.txt"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")
    res_p.to_csv(os.path.join(out, "comparar_prevision.csv"), index=False)
    dif_p.to_csv(os.path.join(out, "comparar_prevision_diferencias.csv"), index=False)
    res_r.to_csv(os.path.join(out, "comparar_reanalisis.csv"), index=False)
    dif_r.to_csv(os.path.join(out, "comparar_reanalisis_diferencias.csv"), index=False)

if __name__ == "__main__":
    main(sys.argv[1:])
