"""
Otras familias de clasificadores frente a la logística L2 de producción en el modelo del día (cuaderno 3w), con la
tabla que deja exportar_dia.py (dia.csv.gz y bloques.json) y la misma evaluación que job_ml_riesgo_dia.py: variante
operativa, cada año lo puntúa un modelo elegido en el último 25 % de los días anteriores y reajustado con todos
(rolling_evaluation), con la previsión real de ICON-EU de la víspera para 2024 - 2025 y 2024 - 2026 y con el
reanálisis para 2022 - 2025. No cambia el código de producción: importa sus módulos y añade las familias nuevas en
tiempo de ejecución sobre la misma tubería (mediana con indicador de faltante, logaritmo con signo y estandarización).

    python familias_dia.py <dia.csv.gz> <bloques.json> <config_ml.json> <salida/> [etapa] [modelos] [etiqueta]

Etapas:

- reproducir: solo la L2, con las cifras de producción (0,151 en el reanálisis, 0,188 con la previsión real).
- familias: la tabla de familias (LDA, QDA, Naive Bayes, k vecinos, SVM, perceptrón y variantes de la logística),
  con las diferencias emparejadas frente a la L2 por bloques de siete días; se pueden pasar los modelos separados por
  comas y una etiqueta para el nombre del fichero.
- curva: la curva de aprendizaje del modelo del día (25, 50, 75 y 100 % de las fechas anteriores, cinco sorteos);
  salida/familias/curva_resumen.csv sale de la etapa con los modelos tasa_base,logistica,xgboost.
- brecha: mejora de log loss dentro y fuera de muestra con la C de producción.
- coeficientes, tarde y unidades: estabilidad de los coeficientes, la C que elegiría la tarde y razones de odds en
  unidades naturales.

Aparte, con la tabla de días del caso de negocio (dias.csv de job_ml_caso_negocio):

    python familias_dia.py umbrales <dias.csv> [salida/]

El repositorio se busca en la variable GRIDPREDIC_REPO o dos carpetas por encima de este fichero. Las redes salen
siempre como Distribuidora 1, 2 y 3.
"""

import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "04_ml"))
import job_ml_horas_riesgo as hr  # noqa: E402
import job_ml_riesgo_dia as rd  # noqa: E402
import ml_metrics  # noqa: E402
import ml_models  # noqa: E402

logging.basicConfig(level=logging.WARNING)
import warnings  # noqa: E402

warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------------
# Familias nuevas sobre la tubería de ml_models
# ---------------------------------------------------------------------------

class DecisionAsProba:
    """Envuelve un clasificador sin probabilidad (SVM): predict_proba = logística de decision_function. Solo ordena."""

    def __init__(self, model):
        self.model = model

    def fit(self, X, y):
        self.model.fit(X, y)
        self.classes_ = np.array([0, 1])
        return self

    def predict_proba(self, X):
        d = self.model.decision_function(X)
        p = 1.0 / (1.0 + np.exp(-np.clip(d, -30, 30)))
        return np.column_stack([1 - p, p])

    def get_params(self, deep=True):
        return {"model": self.model}

    def set_params(self, **params):
        return self


NEW = {}


def new_family(name, probability=True):
    def register(builder):
        NEW[name] = {"build": builder, "probability": probability}
        return builder
    return register


def pipeline(model, log=True, scale=True, extra=None):
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer, StandardScaler

    steps = [("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True))]
    if log:
        steps.append(("log", FunctionTransformer(ml_models.signed_log1p)))
    if scale:
        steps.append(("escalar", StandardScaler()))
    steps += list(extra or [])
    steps.append(("modelo", model))
    return Pipeline(steps)


@new_family("lda")
def _lda(p, seed):
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    return pipeline(LinearDiscriminantAnalysis(solver="lsqr", shrinkage=p.get("shrinkage", "auto")))


for _s in (0.7, 0.9, 1.0):
    def _lda_fixed(p, seed, s=_s):
        from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
        return pipeline(LinearDiscriminantAnalysis(solver="lsqr", shrinkage=s))
    NEW[f"lda_{int(_s * 10):02d}"] = {"build": _lda_fixed, "probability": True}


@new_family("qda")
def _qda(p, seed):
    from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
    return pipeline(QuadraticDiscriminantAnalysis(reg_param=float(p["reg_param"])))


@new_family("naive_bayes")
def _nb(p, seed):
    from sklearn.naive_bayes import GaussianNB
    return pipeline(GaussianNB(var_smoothing=float(p["var_smoothing"])))


@new_family("knn")
def _knn(p, seed):
    from sklearn.neighbors import KNeighborsClassifier
    return pipeline(KNeighborsClassifier(n_neighbors=int(p["k"]), weights=p.get("weights", "uniform")))


@new_family("svm_lineal", probability=False)
def _linsvc(p, seed):
    from sklearn.svm import LinearSVC
    return pipeline(DecisionAsProba(LinearSVC(C=float(p["C"]), max_iter=50000, random_state=seed)))


@new_family("svm_rbf", probability=False)
def _rbf(p, seed):
    from sklearn.svm import SVC
    return pipeline(DecisionAsProba(SVC(kernel="rbf", C=float(p["C"]), gamma=float(p["gamma"]),
                                        class_weight=p.get("class_weight"))))


@new_family("mlp")
def _mlp(p, seed):
    from sklearn.neural_network import MLPClassifier
    return pipeline(MLPClassifier(hidden_layer_sizes=tuple(p["capas"]), alpha=float(p["alpha"]), early_stopping=True,
                                  validation_fraction=0.2, n_iter_no_change=20, max_iter=500, random_state=seed))


@new_family("mlp_lbfgs")
def _mlp_lbfgs(p, seed):
    """Perceptrón pequeño con lbfgs (el de scikit-learn para pocos datos), sin parada temprana por accuracy."""
    from sklearn.neural_network import MLPClassifier
    return pipeline(MLPClassifier(hidden_layer_sizes=tuple(p["capas"]), alpha=float(p["alpha"]), solver="lbfgs",
                                  max_iter=int(p.get("max_iter", 500)), random_state=seed))


@new_family("mlp_epocas")
def _mlp_epochs(p, seed):
    """Adam sin parada temprana: el número de épocas se elige en la validación interna como otro parámetro más."""
    from sklearn.neural_network import MLPClassifier
    return pipeline(MLPClassifier(hidden_layer_sizes=tuple(p["capas"]), alpha=float(p["alpha"]), early_stopping=False,
                                  max_iter=int(p["max_iter"]), random_state=seed))


@new_family("logistica_pares")
def _pairs(p, seed):
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import PolynomialFeatures, StandardScaler
    extra = [("pares", PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)),
             ("escalar2", StandardScaler())]
    return pipeline(LogisticRegression(C=float(p["C"]), max_iter=5000), extra=extra)


@new_family("logistica_sin_log")
def _nolog(p, seed):
    from sklearn.linear_model import LogisticRegression
    return pipeline(LogisticRegression(C=float(p["C"]), max_iter=5000), log=False)


@new_family("logistica_sin_escala")
def _noscale(p, seed):
    from sklearn.linear_model import LogisticRegression
    return pipeline(LogisticRegression(C=float(p["C"]), max_iter=20000), scale=False)


@new_family("logistica_c_amplia")
def _cwide(p, seed):
    from sklearn.linear_model import LogisticRegression
    return pipeline(LogisticRegression(C=float(p["C"]), max_iter=5000))


GRIDS = {
    "lda": [{"shrinkage": "auto"}, {"shrinkage": 0.2}, {"shrinkage": 0.5}],
    "lda_07": [{"contraccion": 0.7}], "lda_09": [{"contraccion": 0.9}], "lda_10": [{"contraccion": 1.0}],
    "qda": [{"reg_param": r} for r in (0.3, 0.6, 0.9)],
    "naive_bayes": [{"var_smoothing": v} for v in (1e-9, 1e-2, 1e-1)],
    "knn": [{"k": k} for k in (50, 100, 200, 400)],
    "svm_lineal": [{"C": c} for c in (0.001, 0.01, 0.1)],
    "svm_rbf": [{"C": c, "gamma": g, "class_weight": "balanced"} for c in (0.1, 1.0) for g in (0.003, 0.01, 0.03)],
    "mlp": [{"capas": [h], "alpha": a} for h in (8, 16) for a in (1.0, 10.0)],
    "mlp_lbfgs": [{"capas": [16], "alpha": a} for a in (1.0, 10.0, 100.0)],
    "mlp_epocas": [{"capas": [16], "alpha": a, "max_iter": e} for a in (1.0, 10.0) for e in (50, 200)],
    "logistica_pares": [{"C": c} for c in (0.0003, 0.001, 0.003, 0.01)],
    "logistica_sin_log": [{"C": c} for c in (0.003, 0.01, 0.03, 0.1)],
    "logistica_sin_escala": [{"C": c} for c in (0.003, 0.01, 0.03, 0.1)],
    "logistica_c_amplia": [{"C": c} for c in (0.0003, 0.001, 0.003, 0.01, 0.03, 0.1)],
}

_fit_model = ml_models.fit_model


def fit_model(name, params, data, rate, seed, n_jobs, use_weights=False):
    if name not in NEW:
        return _fit_model(name, params, data, rate, seed, n_jobs, use_weights)
    estimator = NEW[name]["build"](params, seed)
    start = time.time()
    estimator.fit(data["X"], data["y"])
    scorer = ml_models.EstimatorScorer(name, data["feature_names"], estimator, rate)
    scorer.params = dict(params)
    scorer.info = {"segundos_entrenamiento": round(time.time() - start, 2), "usa_peso_muestra": False}
    return scorer


ml_models.fit_model = fit_model
ml_models.TRAINED = tuple(ml_models.TRAINED) + tuple(NEW)


# ---------------------------------------------------------------------------
# Datos y evaluación
# ---------------------------------------------------------------------------

def load(path, blocks_path):
    pdf = pd.read_csv(path, parse_dates=["fecha"])
    pdf["distribuidora_id"] = pdf["distribuidora_id"].astype("int64")
    pdf["anio"] = pdf["fecha"].dt.year
    pdf["ct_key"] = pdf["distribuidora_id"]
    pdf["hora"] = pdf["fecha"]
    pdf["split"] = "train"
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    return pdf, blocks


def setup(config_path, models):
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    busqueda = {**cfg.get("busqueda", {}), **{k: v for k, v in GRIDS.items()}}
    cfg = {**cfg, "modelos": list(models), "purga_horas": float(cfg.get("purga_horas", 0)), "busqueda": busqueda}
    return config, cfg


def metrics(found, scores, shares=(0.1, 0.2), block_days=7, reps=500, seed=0, probability=None):
    y = found[rd.MAIN_LABEL].to_numpy().astype(int)
    year = found["anio"].to_numpy()
    intervals, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(found)), found["fecha"].to_numpy(), scores, reps,
                                                      seed, block_days=block_days)
    pairs = [(n, "logistica") for n in scores if n != "logistica"]
    diffs = ml_metrics.paired_differences(replicas, pairs).set_index("a") if pairs else pd.DataFrame()
    rows = []
    for _, r in intervals.iterrows():
        name = r["nombre"]
        s = scores[name]
        row = {"modelo": name, "dias": len(found), "dias_con_averia": int(y.sum()), "pr_auc": r["pr_auc"],
               "pr_auc_p2_5": r.get("p2_5", np.nan), "pr_auc_p97_5": r.get("p97_5", np.nan),
               "roc_auc": ml_metrics.roc_auc(y, s)}
        for share in shares:
            row[f"captura_{int(share * 100)}"] = rd.top_share(y, s, year, share, seed)
        if name in diffs.index:
            d = diffs.loc[name]
            row.update(dif=d["dif_p50"], dif_p2_5=d["dif_p2_5"], dif_p97_5=d["dif_p97_5"], gana=d["prob_a_mejor"])
        if probability is None or probability.get(name, True):
            cal = ml_metrics.calibration(y, np.clip(s, 1e-6, 1 - 1e-6))
            row.update(bss=cal["bss"], pendiente=cal["pendiente"], intercepto=cal["intercepto"],
                       prob_media=cal["prob_media"], prevalencia=cal["prevalencia"])
        rows.append(row)
    return pd.DataFrame(rows)


def is_probability(name):
    if name in NEW:
        return NEW[name]["probability"]
    return name in ("tasa_base", "logistica", "logistica_equilibrada", "logistica_firth", "logistica_bayes",
                    "xgboost", "random_forest")


def operativa(cfg):
    return next(v for v in cfg["variantes"] if v["nombre"] == "operativa")


def run_reanalysis(pdf, blocks, config, cfg, years, models, terms=None):
    variant = operativa(cfg)
    features = rd.variant_features(variant, blocks)
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]
    scored, search, scorers, _ = hr.rolling_evaluation(rows, features, rd.MAIN_LABEL, config, cfg, years, False,
                                                       names=models, terms=terms)
    return scored, pd.DataFrame(search), features


def run_forecast(pdf, blocks, config, cfg, years):
    variant = operativa(cfg)
    return rd.forecast_evaluation(pdf, variant, blocks, config, cfg, years, only_first=False)


def stage_reproduce(pdf, blocks, config_path, out):
    models = ["tasa_base", "logistica"]
    config, cfg = setup(config_path, models)
    t0 = time.time()
    scored, search, features = run_reanalysis(pdf, blocks, config, cfg, [2022, 2023, 2024, 2025], models)
    scores = {m: scored[f"p_{m}"].to_numpy() for m in models}
    rea = metrics(scored, scores, block_days=1)
    rea.insert(0, "banco", "reanalisis 2022-2025 (fechas)")
    fc = run_forecast(pdf, blocks, config, cfg, [2024, 2025])
    scores = {m: fc[f"p_{m}__prevision"].to_numpy() for m in models}
    fcm = metrics(fc, scores, block_days=1)
    fcm.insert(0, "banco", "prevision 2024-2025 (fechas)")
    table = pd.concat([rea, fcm])
    print(f"{len(features)} variables: {features}")
    print(table.round(4).to_string(index=False))
    print(search.round(4).to_string(index=False))
    print(f"{time.time() - t0:.1f} s")
    table.to_csv(os.path.join(out, "reproducir.csv"), index=False)
    search.to_csv(os.path.join(out, "reproducir_busqueda.csv"), index=False)


# Las familias de la tabla. XGBoost y los bosques los compara comparar_modelos_dia.py (su parada temprana con unos 20
# días con avería en la validación interna cambia de una ejecución a otra). "mlp" es el perceptrón con la parada
# temprana por defecto de scikit-learn (accuracy), que se hunde; "mlp_lbfgs" y "mlp_epocas" son los montajes justos.
FAMILIES = ["tasa_base", "logistica", "logistica_c_amplia", "logistica_firth", "logistica_bayes", "lda", "lda_10",
            "qda", "naive_bayes", "knn", "svm_lineal", "svm_rbf", "mlp_lbfgs", "mlp_epocas", "mlp", "logistica_pares",
            "logistica_sin_log", "logistica_sin_escala"]


def stage_families(pdf, blocks, config_path, out, models=None, tag=""):
    models = models or FAMILIES
    tag = f"_{tag}" if tag else ""
    config, cfg = setup(config_path, models)
    prob = {m: is_probability(m) for m in models}
    tables, searches = [], []
    t0 = time.time()
    scored, search, _ = run_reanalysis(pdf, blocks, config, cfg, [2022, 2023, 2024, 2025], models)
    search["banco"] = "reanalisis"
    searches.append(search)
    scores = {m: scored[f"p_{m}"].to_numpy() for m in models}
    t = metrics(scored, scores, probability=prob)
    t.insert(0, "banco", "reanalisis 2022-2025")
    tables.append(t)
    print(f"reanalisis {time.time() - t0:.0f} s", flush=True)
    for years in ([2024, 2025], [2024, 2025, 2026]):
        fc = run_forecast(pdf, blocks, config, cfg, years)
        scores = {m: fc[f"p_{m}__prevision"].to_numpy() for m in models}
        t = metrics(fc, scores, probability=prob)
        t.insert(0, "banco", f"prevision {years[0]}-{years[-1]}")
        tables.append(t)
        print(f"prevision {years} {time.time() - t0:.0f} s", flush=True)
    table = pd.concat(tables, ignore_index=True)
    secs = (searches[0].groupby("modelo")["segundos_entrenamiento"].sum().rename("segundos_busqueda_reanalisis")
            .reset_index())
    table = table.merge(secs, on="modelo", how="left")
    table.to_csv(os.path.join(out, f"familias{tag}.csv"), index=False)
    pd.concat(searches).to_csv(os.path.join(out, f"familias{tag}_busqueda.csv"), index=False)
    cols = ["banco", "modelo", "pr_auc", "dif", "dif_p2_5", "dif_p97_5", "gana", "captura_10", "captura_20", "roc_auc",
            "bss", "pendiente", "segundos_busqueda_reanalisis"]
    print(table[cols].round(3).to_string(index=False))


def stage_coefficients(pdf, blocks, config_path, out):
    models = ["tasa_base", "logistica"]
    config, cfg = setup(config_path, models)
    terms = []
    scored, search, features = run_reanalysis(pdf, blocks, config, cfg, [2022, 2023, 2024, 2025, 2026], models,
                                              terms=terms)
    t = pd.DataFrame(terms)
    t["variante"] = "operativa"
    t.to_csv(os.path.join(out, "coeficientes.csv"), index=False)
    stab = rd.coefficient_stability(t)
    stab.to_csv(os.path.join(out, "coeficientes_estabilidad.csv"), index=False)
    print(stab[stab["modelo"] == "logistica"].round(3).to_string(index=False))
    print(search.round(4).to_string(index=False))
    search.to_csv(os.path.join(out, "coeficientes_busqueda.csv"), index=False)


def stage_curve(pdf, blocks, config_path, out, models=None, fractions=(0.25, 0.5, 0.75, 1.0), seeds=(1, 2, 3, 4, 5),
                years=(2022, 2023, 2024, 2025)):
    """
    Curva de aprendizaje del modelo del día: cada año lo puntúa un modelo elegido y ajustado con una parte al azar
    de las fechas anteriores (todas las redes de una fecha juntas), como en 5.8.4 para las horas. PR-AUC de los
    cuatro años juntos (reanálisis) y, para ver la brecha, la PR-AUC de los días de entrenamiento.
    """
    models = models or ["tasa_base", "logistica"]
    config, cfg = setup(config_path, models)
    variant = operativa(cfg)
    features = rd.variant_features(variant, blocks)
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])].copy()
    trained = [m for m in models if m != "tasa_base"]
    results = []
    for fraction in fractions:
        for seed in (seeds if fraction < 1 else seeds[:1]):
            parts, insample = [], {m: [] for m in trained}
            for year in years:
                before = rows[rows["fecha"] < pd.Timestamp(f"{year}-01-01")]
                dates = np.sort(before["fecha"].unique())
                rng = np.random.default_rng(seed * 1000 + year)
                keep = dates if fraction >= 1 else np.sort(rng.choice(dates, int(round(len(dates) * fraction)),
                                                                       replace=False))
                subset = pd.concat([before[before["fecha"].isin(keep)], rows[rows["anio"] == year]])
                scored, _, scorers, _ = hr.rolling_evaluation(subset, features, rd.MAIN_LABEL, config, cfg, [year],
                                                              False, names=models, references=False)
                parts.append(scored)
                Xb, yb = hr.to_arrays(before[before["fecha"].isin(keep)], features, rd.MAIN_LABEL)
                for m in trained:
                    insample[m].append(ml_metrics.average_precision(yb, scorers[m].score(Xb)))
            scored = pd.concat(parts, ignore_index=True)
            y = scored[rd.MAIN_LABEL].to_numpy()
            for m in trained:
                results.append({"modelo": m, "fraccion": fraction, "semilla": seed,
                                "pr_auc": ml_metrics.average_precision(y, scored[f"p_{m}"].to_numpy()),
                                "captura_10": rd.top_share(y, scored[f"p_{m}"].to_numpy(), scored["anio"].to_numpy(),
                                                           0.1, 0),
                                "pr_auc_entrenamiento": float(np.mean(insample[m])),
                                "positivos_entrenamiento_2025": None})
            print(fraction, seed, {r["modelo"]: round(r["pr_auc"], 4) for r in results[-len(trained):]}, flush=True)
    table = pd.DataFrame(results)
    table.to_csv(os.path.join(out, "curva.csv"), index=False)
    summary = table.groupby(["modelo", "fraccion"]).agg(pr_auc=("pr_auc", "mean"), pr_auc_min=("pr_auc", "min"),
                                                        pr_auc_max=("pr_auc", "max"),
                                                        captura_10=("captura_10", "mean"),
                                                        entrenamiento=("pr_auc_entrenamiento", "mean")).reset_index()
    summary.to_csv(os.path.join(out, "curva_resumen.csv"), index=False)
    print(summary.round(4).to_string(index=False))


def operativa_rows(pdf, blocks, config_path):
    config, cfg = setup(config_path, ["tasa_base", "logistica"])
    variant = operativa(cfg)
    features = rd.variant_features(variant, blocks)
    return config, cfg, features, pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]


def log_loss_skill(y, p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    rate = float(np.mean(y))
    loss = -np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))
    base = -(rate * np.log(rate) + (1 - rate) * np.log(1 - rate))
    return 1 - loss / base


def stage_gap(pdf, blocks, config_path, out, C=0.003):
    """Mejora de log loss sobre la tasa propia, en los días de entrenamiento y en el año puntuado, con C fija."""
    _, _, features, rows = operativa_rows(pdf, blocks, config_path)
    results = []
    for fraction in (0.25, 0.5, 1.0):
        for seed in ((1, 2, 3) if fraction < 1 else (1,)):
            inside, outside = [], []
            for year in (2022, 2023, 2024, 2025):
                before = rows[rows["fecha"] < pd.Timestamp(f"{year}-01-01")]
                target = rows[rows["anio"] == year]
                dates = np.sort(before["fecha"].unique())
                rng = np.random.default_rng(seed * 1000 + year)
                keep = dates if fraction >= 1 else np.sort(rng.choice(dates, int(round(len(dates) * fraction)),
                                                                       replace=False))
                X, y = hr.to_arrays(before[before["fecha"].isin(keep)], features, rd.MAIN_LABEL)
                Xt, yt = hr.to_arrays(target, features, rd.MAIN_LABEL)
                scorer = ml_models.fit_model("logistica", {"C": C}, {"X": X, "y": y, "w": None,
                                                                     "feature_names": features}, 1.0, 0, 1)
                inside.append(log_loss_skill(y, scorer.score(X)))
                outside.append(log_loss_skill(yt, scorer.score(Xt)))
            results.append({"fraccion": fraction, "semilla": seed, "entrenamiento": np.mean(inside),
                            "anios_puntuados": np.mean(outside)})
    table = pd.DataFrame(results).groupby("fraccion")[["entrenamiento", "anios_puntuados"]].mean().reset_index()
    table.to_csv(os.path.join(out, "brecha.csv"), index=False)
    print(table.round(3).to_string(index=False))


def stage_evening(pdf, blocks, config_path, out, until="2026-06-20"):
    """La C que elegiría la tarde (días etiquetados hasta until, validación en el último 25 %), con dos rejillas."""
    config, cfg, features, rows = operativa_rows(pdf, blocks, config_path)
    rows = rows[rows["fecha"] <= pd.Timestamp(until)]
    days = np.sort(rows["fecha"].unique())
    cut = days[int(len(days) * 0.75)]
    X, y = hr.to_arrays(rows[rows["fecha"] < cut], features, rd.MAIN_LABEL)
    Xv, yv = hr.to_arrays(rows[rows["fecha"] >= cut], features, rd.MAIN_LABEL)
    inner = {"X": X, "y": y, "w": np.ones(len(y)), "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
             "feature_names": features}
    out_rows = []
    for name, grid in (("produccion", [0.003, 0.01, 0.03, 0.1]), ("abierta", [0.0001, 0.0003, 0.001, 0.003, 0.01])):
        _, search = hr.train(["logistica"], config, {**cfg, "busqueda": {"logistica": [{"C": c} for c in grid]}},
                             inner, float(y.mean()), False, references=False)
        out_rows += [{"rejilla": name, "C": s["params"]["C"], "pr_auc_validacion": s["pr_auc_valid_muestra"]}
                     for s in search if s["modelo"] == "logistica"]
    table = pd.DataFrame(out_rows)
    table.to_csv(os.path.join(out, "tarde_c.csv"), index=False)
    print(f"corte {pd.Timestamp(cut).date()}, positivos {int(y.sum())} y {int(yv.sum())}")
    print(table.round(4).to_string(index=False))


def stage_units(pdf, blocks, config_path, out):
    """Razones de odds de una variable sola entre dos valores naturales, con los coeficientes medios por año."""
    _, _, features, rows = operativa_rows(pdf, blocks, config_path)
    stability = pd.read_csv(os.path.join(out, "coeficientes_estabilidad.csv"))
    coef = stability[stability["modelo"] == "logistica"].set_index("variable")["media"]
    train = rows[rows["fecha"] < pd.Timestamp("2026-01-01")]
    cases = {"ext_racha_clima_max": [(0.74, 0.92), (0.74, 1.4), (0.74, 2.0)], "dia_lluvia_max": [(4.8, 19.2)],
             "dia_racha_max": [(46, 62), (46, 146)], "aviso_viento_nivel": [(0, 1), (0, 2)]}
    out_rows = []
    for name, pairs in cases.items():
        x = train[name].fillna(train[name].median())
        scale = float(ml_models.signed_log1p(x.to_numpy()).std())
        for a, b in pairs:
            ratio = float(np.exp(coef[name] / scale * (np.log1p(b) - np.log1p(a))))
            out_rows.append({"variable": name, "de": a, "a": b, "razon_de_odds_sola": ratio})
    table = pd.DataFrame(out_rows)
    table.to_csv(os.path.join(out, "unidades.csv"), index=False)
    print(table.round(3).to_string(index=False))


def stage_thresholds(dias_path, out):
    """Probabilidad mínima de cada nivel por red y efecto de un suelo de probabilidad en la alerta (caso de negocio)."""
    days = pd.read_csv(dias_path, parse_dates=["fecha"])
    days["red"] = days["distribuidora_id"].map({3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"})
    days = days.drop(columns=[c for c in ("distribuidora", "bases") if c in days.columns])
    levels, floors = [], []
    for mode, part in days.groupby("modo"):
        for net, g in part.groupby("red"):
            alert = g["alerta"].astype(bool)
            row = {"modo": mode, "red": net, "dias": len(g), "dias_con_incidencia": int((g["incidencias"] > 0).sum()),
                   "dias_alerta": int(alert.sum()), "alerta_con_incidencia": int((g.loc[alert, "incidencias"] > 0).sum())}
            for name in ("medio", "alto", "muy alto"):
                row[f"p_min_{name}"] = g.loc[g["nivel"] == name, "probabilidad"].min()
            levels.append(row)
            for floor in (0.04, 0.05, 0.06):
                keep = alert & (g["probabilidad"] >= floor)
                lost = alert & ~keep
                floors.append({"modo": mode, "red": net, "suelo": floor, "alerta": int(alert.sum()),
                               "alerta_con_suelo": int(keep.sum()),
                               "incidencias_quitadas": int(g.loc[lost, "incidencias"].sum()),
                               "horas_cliente_quitadas": float(g.loc[lost, "clientes_hora_recortadas"].sum())})
    pd.DataFrame(levels).to_csv(os.path.join(out, "umbrales.csv"), index=False)
    pd.DataFrame(floors).to_csv(os.path.join(out, "umbrales_suelo.csv"), index=False)
    print(pd.DataFrame(levels).round(4).to_string(index=False))
    print(pd.DataFrame(floors).round(1).to_string(index=False))


def main(argv):
    if argv and argv[0] == "umbrales":
        out = argv[2] if len(argv) > 2 else "salida"
        os.makedirs(out, exist_ok=True)
        stage_thresholds(argv[1], out)
        return 0
    data, blocks_path, config_path, out = argv[:4]
    stage = argv[4] if len(argv) > 4 else "reproducir"
    os.makedirs(out, exist_ok=True)
    pdf, blocks = load(data, blocks_path)
    if stage == "reproducir":
        stage_reproduce(pdf, blocks, config_path, out)
    elif stage == "familias":
        stage_families(pdf, blocks, config_path, out, argv[5].split(",") if len(argv) > 5 else None,
                       argv[6] if len(argv) > 6 else "")
    elif stage == "coeficientes":
        stage_coefficients(pdf, blocks, config_path, out)
    elif stage == "curva":
        stage_curve(pdf, blocks, config_path, out, argv[5].split(",") if len(argv) > 5 else None)
    elif stage == "brecha":
        stage_gap(pdf, blocks, config_path, out)
    elif stage == "tarde":
        stage_evening(pdf, blocks, config_path, out)
    elif stage == "unidades":
        stage_units(pdf, blocks, config_path, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
