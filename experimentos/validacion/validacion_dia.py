"""
Qué partición de validación conviene al modelo del día (memoria, 5.4): la evaluación por años del repositorio
frente al protocolo del laboratorio de MLlib (randomSplit 70/30 y CrossValidator de pliegues aleatorios) y otras
particiones, con la tabla que deja experimentos/perfil_dia/exportar_dia.py y sin Spark:

    python validacion_dia.py <dia.csv.gz> <bloques.json> <salida/>

Todo con la logística L2 de producción (variante operativa, 35 variables, desde el 25.06.2018) y la misma tubería de
ml_models.build_estimator (mediana con indicador, logaritmo con signo y estandarización):

1. La evaluación por años del repositorio (rolling_evaluation de job_ml_horas_riesgo.py): cada año 2022 - 2025 lo
   puntúa un modelo que solo vio los años anteriores, con la C elegida en el último 25 % de esos días.
2. El protocolo del laboratorio: randomSplit 70/30 y CrossValidator de 5 pliegues aleatorios sobre train, eligiendo
   con el área ROC (la métrica por defecto de BinaryClassificationEvaluator) o con la PR-AUC, con 50 semillas.
3. Validación cruzada aleatoria de 5 pliegues (con la C elegida dentro de cada pliegue), puntuando las mismas filas
   de 2022 - 2025 que la evaluación por años, con 20 semillas.
4. Un pliegue por año sin orden temporal (lo que daría CrossValidator con foldCol = año): cada año se puntúa con un
   modelo entrenado con todos los demás, también los posteriores.
5. Las métricas del laboratorio sobre las predicciones por años: matriz de confusión con el umbral de 0,5,
   accuracy, recall por clase, el "área ROC" con la predicción 0/1 en vez de la probabilidad y el área PR por
   trapecios de BinaryClassificationMetrics (con y sin los 1.000 tramos de BinaryClassificationEvaluator) frente a la
   average precision de scikit-learn, que es la PR-AUC del repositorio.

Deja validacion_dia.txt; las predicciones por día no se guardan. Tarda unos tres minutos.
"""

import json
import os
import sys
import time

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import KFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

SEED = 20261010
DESDE = "2018-06-25"
YEARS = [2022, 2023, 2024, 2025]
GRID_C = [0.003, 0.01, 0.03, 0.1]
SHARE = 0.25
LABEL = "y_dia"


def signed_log1p(x):
    return np.sign(x) * np.log1p(np.abs(x))


def build(c):
    return Pipeline([
        ("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
        ("log", FunctionTransformer(signed_log1p)),
        ("escalar", StandardScaler()),
        ("modelo", LogisticRegression(C=c, max_iter=1000, solver="lbfgs", random_state=SEED)),
    ])


def fit_score(train, test, features, c):
    model = build(c).fit(train[features].to_numpy(np.float32), train[LABEL].to_numpy(np.int8))
    return model.predict_proba(test[features].to_numpy(np.float32))[:, 1], model


def choose_c(fit_part, check_part, features, metric="ap"):
    best, best_value = GRID_C[0], -1.0
    for c in GRID_C:
        p, _ = fit_score(fit_part, check_part, features, c)
        y = check_part[LABEL].to_numpy()
        value = average_precision_score(y, p) if metric == "ap" else roc_auc_score(y, p)
        if value > best_value:
            best, best_value = c, value
    return best


def choose_c_kfold(rows, features, seed, metric="roc", folds=5):
    """CrossValidator: k pliegues aleatorios, media de la métrica por C, la mejor C."""
    kf = KFold(n_splits=folds, shuffle=True, random_state=seed)
    scores = {c: [] for c in GRID_C}
    for tr, va in kf.split(rows):
        a, b = rows.iloc[tr], rows.iloc[va]
        if b[LABEL].sum() == 0:
            continue
        for c in GRID_C:
            p, _ = fit_score(a, b, features, c)
            y = b[LABEL].to_numpy()
            scores[c].append(average_precision_score(y, p) if metric == "ap" else roc_auc_score(y, p))
    return max(GRID_C, key=lambda c: np.mean(scores[c]))


def rolling(rows, features):
    """La evaluación por años del repositorio (purga 0 en el modelo del día)."""
    parts, chosen = [], {}
    for year in YEARS:
        start = pd.Timestamp(f"{year}-01-01")
        before = rows[rows["fecha"] < start]
        target = rows[(rows["fecha"] >= start) & (rows["fecha"] < pd.Timestamp(f"{year + 1}-01-01"))].copy()
        days = np.sort(before["fecha"].unique())
        cut = days[int(len(days) * (1 - SHARE))]
        c = choose_c(before[before["fecha"] < cut], before[before["fecha"] >= cut], features)
        target["p"], _ = fit_score(before, target, features, c)
        chosen[year] = c
        parts.append(target)
    return pd.concat(parts), chosen


def leave_one_year_out(rows, features):
    """CrossValidator con foldCol = año: cada año con un modelo entrenado con todos los demás."""
    parts, chosen = [], {}
    pool = rows[rows["fecha"] < pd.Timestamp("2026-01-01")]
    years = sorted(pool["fecha"].dt.year.unique())
    for year in YEARS:
        train = pool[pool["fecha"].dt.year != year]
        target = pool[pool["fecha"].dt.year == year].copy()
        inner = {c: [] for c in GRID_C}
        for other in [y for y in years if y != year]:
            a, b = train[train["fecha"].dt.year != other], train[train["fecha"].dt.year == other]
            for c in GRID_C:
                p, _ = fit_score(a, b, features, c)
                inner[c].append(average_precision_score(b[LABEL], p))
        c = max(GRID_C, key=lambda k: np.mean(inner[k]))
        target["p"], _ = fit_score(train, target, features, c)
        chosen[year] = c
        parts.append(target)
    return pd.concat(parts), chosen


def random_oof(rows, features, seed, folds=5):
    """Validación cruzada aleatoria con la C elegida dentro de cada pliegue (área PR); predicción fuera de pliegue."""
    pool = rows[rows["fecha"] < pd.Timestamp("2026-01-01")].copy()
    pool["p"] = np.nan
    kf = KFold(n_splits=folds, shuffle=True, random_state=seed)
    for tr, te in kf.split(pool):
        a, b = pool.iloc[tr], pool.iloc[te]
        c = choose_c_kfold(a, features, seed + 1, metric="ap")
        pool.iloc[te, pool.columns.get_loc("p")], _ = fit_score(a, b, features, c)
    return pool


def course_split(rows, features, seed, metric):
    """randomSplit([0.7, 0.3]) y CrossValidator(numFolds=5) sobre train, como el cuaderno del laboratorio."""
    pool = rows[rows["fecha"] < pd.Timestamp("2026-01-01")]
    rng = np.random.default_rng(seed)
    mask = rng.random(len(pool)) < 0.7
    train, test = pool[mask], pool[~mask]
    c = choose_c_kfold(train, features, seed, metric=metric)
    p, _ = fit_score(train, test, features, c)
    y = test[LABEL].to_numpy()
    return average_precision_score(y, p), roc_auc_score(y, p), c, int(y.sum())


def spark_area_pr(y, s, num_bins=0):
    """
    Área bajo la curva PR de BinaryClassificationMetrics: puntos (recall, precision) por umbral (puntuaciones
    distintas, de mayor a menor), con (0, precisión del primer punto) delante, integrados por trapecios.
    num_bins > 0 junta grupos de puntuaciones seguidas como hace BinaryClassificationEvaluator (1.000 por defecto).
    """
    y = np.asarray(y, dtype=np.int64)
    s = np.asarray(s, dtype=np.float64)
    order = np.argsort(-s, kind="mergesort")
    s, y = s[order], y[order]
    distinct, start = np.unique(-s, return_index=True)
    pos = np.add.reduceat(y, start)
    neg = np.add.reduceat(1 - y, start)
    if num_bins > 0:
        grouping = len(pos) // num_bins
        if grouping >= 2:
            idx = np.arange(0, len(pos), grouping)
            pos = np.add.reduceat(pos, idx)
            neg = np.add.reduceat(neg, idx)
    tp, fp = np.cumsum(pos), np.cumsum(neg)
    recall = tp / y.sum()
    precision = tp / (tp + fp)
    r = np.concatenate([[0.0], recall])
    p = np.concatenate([[precision[0]], precision])
    return float(np.sum((r[1:] - r[:-1]) * (p[1:] + p[:-1]) / 2.0))


def block_bootstrap_diff(frame, a, b, reps=500, block_days=7, seed=0):
    """Diferencia de average precision entre dos puntuaciones de las mismas filas, por bloques de 7 días."""
    rng = np.random.default_rng(seed)
    block = ((frame["fecha"] - frame["fecha"].min()).dt.days // block_days).to_numpy()
    ids = np.unique(block)
    groups = {k: np.flatnonzero(block == k) for k in ids}
    y, sa, sb = frame[LABEL].to_numpy(), frame[a].to_numpy(), frame[b].to_numpy()
    out = []
    for _ in range(reps):
        pick = np.concatenate([groups[k] for k in rng.choice(ids, size=len(ids), replace=True)])
        if y[pick].sum() == 0:
            continue
        out.append(average_precision_score(y[pick], sa[pick]) - average_precision_score(y[pick], sb[pick]))
    out = np.array(out)
    return float(np.mean(out)), float(np.percentile(out, 2.5)), float(np.percentile(out, 97.5)), float(np.mean(out > 0))


def top_share(y, s, year, share):
    hits = positives = 0
    for value in np.unique(year):
        m = year == value
        k = int(round(m.sum() * share))
        order = np.argsort(-s[m], kind="mergesort")
        hits += int(y[m][order[:k]].sum())
        positives += int(y[m].sum())
    return hits / positives


def main(data_path, blocks_path, out):
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    features = list(blocks["distribuidora"])
    for block in ["calendario", "historia", "avisos", "dia_comun", "extremos"]:
        features += blocks[block]
    features += [v for v in ["ayer_racha_max", "ayer_lluvia_max"] if v not in features]
    frame = pd.read_csv(data_path, parse_dates=["fecha"])
    rows = frame[frame["fecha"] >= pd.Timestamp(DESDE)].sort_values(["fecha", "distribuidora_id"]).reset_index(drop=True)
    lines = [f"Filas {len(rows)} desde {DESDE}, {int(rows[LABEL].sum())} días con avería, {len(features)} variables"]

    # 1. Evaluación por años (repositorio)
    roll, chosen = rolling(rows, features)
    y, s = roll[LABEL].to_numpy(), roll["p"].to_numpy()
    ap_roll, roc_roll = average_precision_score(y, s), roc_auc_score(y, s)
    per_year = {yr: round(average_precision_score(g[LABEL], g["p"]), 4) for yr, g in roll.groupby(roll["fecha"].dt.year)}
    lines.append(f"[1] Por años 2022-2025: PR-AUC {ap_roll:.4f}, ROC {roc_roll:.4f}, tasa {y.mean():.4f}, "
                 f"positivos {int(y.sum())}, C elegidas {chosen}, PR-AUC por año {per_year}")

    # 4. Un pliegue por año (foldCol)
    loyo, chosen_l = leave_one_year_out(rows, features)
    ap_loyo = average_precision_score(loyo[LABEL], loyo["p"])
    per_year_l = {yr: round(average_precision_score(g[LABEL], g["p"]), 4) for yr, g in loyo.groupby(loyo["fecha"].dt.year)}
    lines.append(f"[4] Un pliegue por año (foldCol), 2022-2025: PR-AUC {ap_loyo:.4f}, ROC "
                 f"{roc_auc_score(loyo[LABEL], loyo['p']):.4f}, C {chosen_l}, por año {per_year_l}")

    # 3. Validación cruzada aleatoria, fuera de pliegue, mismas filas de 2022-2025
    ap_oof, ap_oof_all = [], []
    for k in range(20):
        oof = random_oof(rows, features, SEED + 100 * k)
        sub = oof[(oof["fecha"].dt.year >= 2022) & (oof["fecha"].dt.year <= 2025)]
        ap_oof.append(average_precision_score(sub[LABEL], sub["p"]))
        ap_oof_all.append(average_precision_score(oof[LABEL], oof["p"]))
        if k == 0:
            first = sub[["distribuidora_id", "fecha", LABEL, "p"]].rename(columns={"p": "p_aleatoria"})
    ap_oof = np.array(ap_oof)
    lines.append(f"[3] CV aleatoria 5 pliegues (20 semillas), filas de 2022-2025: PR-AUC media {ap_oof.mean():.4f} "
                 f"[{ap_oof.min():.4f} - {ap_oof.max():.4f}]; sobre 2018-2025 entero {np.mean(ap_oof_all):.4f}")

    # Diferencia emparejada por bloques de 7 días (semilla 0 de la CV aleatoria frente a la evaluación por años)
    paired = roll[["distribuidora_id", "fecha", LABEL, "p"]].merge(first.drop(columns=[LABEL]),
                                                                   on=["distribuidora_id", "fecha"])
    paired2 = paired.merge(loyo[["distribuidora_id", "fecha", "p"]].rename(columns={"p": "p_anio"}),
                           on=["distribuidora_id", "fecha"])
    d1 = block_bootstrap_diff(paired2, "p_aleatoria", "p", seed=1)
    d2 = block_bootstrap_diff(paired2, "p_anio", "p", seed=2)
    lines.append(f"    CV aleatoria - por años: {d1[0]:+.4f} [{d1[1]:+.4f}, {d1[2]:+.4f}], P(>0) {d1[3]:.2f}")
    lines.append(f"    Pliegue por año - por años: {d2[0]:+.4f} [{d2[1]:+.4f}, {d2[2]:+.4f}], P(>0) {d2[3]:.2f}")

    # 2. Protocolo del laboratorio (randomSplit 70/30 + CrossValidator sobre train)
    res = {m: [course_split(rows, features, SEED + k, m) for k in range(50)] for m in ("roc", "ap")}
    for m, values in res.items():
        aps = np.array([v[0] for v in values])
        rocs = np.array([v[1] for v in values])
        cs = pd.Series([v[2] for v in values]).value_counts().to_dict()
        npos = np.array([v[3] for v in values])
        lines.append(f"[2] randomSplit 70/30 + CV 5 pliegues eligiendo con {m} (50 semillas): PR-AUC test media "
                     f"{aps.mean():.4f}, p5 {np.percentile(aps, 5):.4f}, p95 {np.percentile(aps, 95):.4f}; ROC "
                     f"{rocs.mean():.4f}; C elegidas {cs}; positivos en test {npos.min()}-{npos.max()}")

    # 5. Métricas del laboratorio sobre las predicciones por años
    pred = (s >= 0.5).astype(int)
    tp, fp = int(((pred == 1) & (y == 1)).sum()), int(((pred == 1) & (y == 0)).sum())
    fn, tn = int(((pred == 0) & (y == 1)).sum()), int(((pred == 0) & (y == 0)).sum())
    acc = (tp + tn) / len(y)
    rec1 = tp / (tp + fn)
    rec0 = tn / (tn + fp)
    auc_hard = roc_auc_score(y, pred) if pred.min() != pred.max() else 0.5
    lines.append(f"[5] Umbral 0,5: matriz [[tn {tn}, fp {fp}], [fn {fn}, tp {tp}]], accuracy {acc:.4f} "
                 f"(siempre 0: {1 - y.mean():.4f}), recall clase 1 {rec1:.4f}, recall clase 0 {rec0:.4f}, "
                 f"días con p >= 0,5: {int(pred.sum())}, p máxima {s.max():.4f}")
    lines.append(f"    'Área ROC' con la predicción 0/1: {auc_hard:.4f} (= recall medio de las dos clases "
                 f"{(rec1 + rec0) / 2:.4f}); con la probabilidad: {roc_roll:.4f}")
    for thr in (0.1, 0.05):
        pr_ = (s >= thr).astype(int)
        tp_, fp_ = int(((pr_ == 1) & (y == 1)).sum()), int(((pr_ == 1) & (y == 0)).sum())
        lines.append(f"    Umbral {thr}: {int(pr_.sum())} días en alerta, precision {tp_ / max(1, tp_ + fp_):.3f}, "
                     f"recall {tp_ / y.sum():.3f}")
    ap_spark = spark_area_pr(y, s)
    ap_spark_bins = spark_area_pr(y, s, num_bins=1000)
    lines.append(f"    Área PR: average precision (repositorio) {ap_roll:.4f}; trapecios de BinaryClassificationMetrics "
                 f"{ap_spark:.4f}; con numBins=1000 {ap_spark_bins:.4f}")
    years = roll["fecha"].dt.year.to_numpy()
    rate = y.mean()
    brier = float(np.mean((s - y) ** 2))
    lines.append(f"    Brier {brier:.4f}, referencia {rate * (1 - rate):.4f}, BSS {1 - brier / (rate * (1 - rate)):.4f}; "
                 f"días con avería en el 10 % y el 20 % de días de más riesgo de cada año: "
                 f"{top_share(y, s, years, 0.1):.3f} y {top_share(y, s, years, 0.2):.3f}")

    # Tamaño y tiempo del ajuste (lo que spark.ml repartiría)
    train = rows[rows["fecha"] < pd.Timestamp("2026-01-01")]
    start = time.time()
    _, model = fit_score(train, train.head(5), features, 0.01)
    took = time.time() - start
    n_iter = int(model.named_steps["modelo"].n_iter_[0])
    lines.append(f"[6] Ajuste de la logística con 2018-2025 ({len(train)} filas x {len(features)}): {took * 1000:.0f} ms, "
                 f"{n_iter} iteraciones de L-BFGS; matriz en float64 {len(train) * len(features) * 8 / 1e6:.2f} MB")
    lines.append(f"    Muestra por CT y hora: 884.971 x 246 en float32 = {884971 * 246 * 4 / 1e9:.3f} GB")
    lines.append(f"Tiempo total {time.time() - t0:.0f} s")

    text = "\n".join(lines)
    print(text)
    with open(os.path.join(out, "validacion_dia.txt"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
