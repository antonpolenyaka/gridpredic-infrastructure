"""
Models of the outage classifier and the references they are compared with.

No Spark here: the training job builds the matrices and calls these
functions, the executors unpickle the fitted models to score valid and test,
and the tests use them on small synthetic data.

Every model returns a score per row where a higher value means a higher risk.
For the trained models the score is a probability of the real population:
they are fitted on the training sample of dataset_train (every positive and
a share r of the negatives, r = 1 / peso_muestra of the negatives), so their
raw probability is corrected back with the prior correction

    p = p_s * r / (p_s * r + 1 - p_s)

which is monotonic: it changes the calibration, not the ranking, so PR-AUC,
ROC-AUC and the alerts under a budget are the same with or without it.

Models (name in config_ml.json):

- tasa_base: the same score for every row. Its PR-AUC is the share of
  positives; anything useful must be clearly above it.
- naif_historico: ranks the CTs by their known interruptions in the last
  365 days (then 90 and 30 days to break ties). It is what an operator would
  do without a model, and the reference of the model card.
- logistica: regularised logistic regression on the imputed, log scaled and
  standardised features. The interpretable reference.
- random_forest: scikit-learn random forest (it accepts missing values).
- xgboost: gradient boosting on trees, the main candidate. Early stopping on
  the PR-AUC of the validation sample.
"""

import time

import numpy as np


TRAINED = ("logistica", "random_forest", "xgboost")
REFERENCES = ("tasa_base", "naif_historico")
ALL_MODELS = REFERENCES + TRAINED

# Features of the naive reference, from the most to the least important.
NAIVE_FEATURES = ("hist_interr_365d", "hist_interr_90d", "hist_interr_30d")


# Fitted models already unpickled in this Python worker, by run. The executors
# reuse their Python workers, so a model is loaded once per worker and not
# once per batch of rows.
_LOADED = {}


def cached(key: str, loader):
    if key not in _LOADED:
        _LOADED.clear()
        _LOADED[key] = loader()

    return _LOADED[key]


def prior_correction(p, rate: float):
    """
    Probability of the full population from the probability of a model
    fitted on a sample that kept a share rate of the negatives.
    """
    p = np.asarray(p, dtype=np.float64)

    if rate >= 1.0:
        return p

    return p * rate / (p * rate + (1.0 - p))


def signed_log1p(x):
    """
    sign(x) * log(1 + |x|): counts and currents are heavy tailed, the
    logistic regression works better on this scale. A module level function
    so the fitted pipeline can be pickled and sent to the executors.
    """
    return np.sign(x) * np.log1p(np.abs(x))


class Scorer:
    """
    A fitted model ready to score a float32 matrix whose columns follow
    feature_names. score() returns float32.
    """

    def __init__(self, name: str, feature_names: list, rate: float = 1.0):
        self.name = name
        self.feature_names = list(feature_names)
        self.rate = rate
        self.params = {}
        self.info = {}

    def score(self, X) -> np.ndarray:
        raise NotImplementedError

    def single_thread(self):
        """Executors run one task per core: one thread per model there."""
        return self


class ConstantScorer(Scorer):
    def __init__(self, feature_names: list, value: float):
        super().__init__("tasa_base", feature_names)
        self.value = float(value)
        self.params = {"valor": self.value}

    def score(self, X):
        return np.full(len(X), self.value, dtype=np.float32)


class HistoricalScorer(Scorer):
    """
    hist_interr_365d, then 90d and 30d as tie breakers. The counts are small
    integers, so the weights keep the order of the first feature.
    """

    def __init__(self, feature_names: list):
        super().__init__("naif_historico", feature_names)
        self.columns = [feature_names.index(f) for f in NAIVE_FEATURES if f in feature_names]

        if not self.columns:
            raise ValueError(f"The naive reference needs one of {NAIVE_FEATURES} in the features")

        self.params = {"features": [feature_names[i] for i in self.columns]}

    def score(self, X):
        out = np.zeros(len(X), dtype=np.float64)

        for weight, column in zip((1.0, 1e-3, 1e-6), self.columns):
            out += weight * np.nan_to_num(X[:, column].astype(np.float64), nan=0.0)

        return out.astype(np.float32)


class EstimatorScorer(Scorer):
    """A scikit-learn compatible classifier with predict_proba."""

    def __init__(self, name: str, feature_names: list, estimator, rate: float):
        super().__init__(name, feature_names, rate)
        self.estimator = estimator

    def score(self, X):
        p = self.estimator.predict_proba(X)[:, 1]
        return prior_correction(p, self.rate).astype(np.float32)

    def single_thread(self):
        model = self.estimator
        final = model.steps[-1][1] if hasattr(model, "steps") else model

        if "n_jobs" in final.get_params():
            final.set_params(n_jobs=1)

        return self


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def build_estimator(name: str, params: dict, seed: int, n_jobs: int):
    if name == "logistica":
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import FunctionTransformer, StandardScaler

        # add_indicator: a missing measurement (CT without telemetry, gap of
        # the SCADA) is information by itself.
        return Pipeline([
            ("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("log", FunctionTransformer(signed_log1p)),
            ("escalar", StandardScaler()),
            ("modelo", LogisticRegression(
                C=float(params.get("C", 0.1)),
                max_iter=int(params.get("max_iter", 1000)),
                solver="lbfgs",
                random_state=seed,
            )),
        ])

    if name == "random_forest":
        from sklearn.ensemble import RandomForestClassifier

        return RandomForestClassifier(
            n_estimators=int(params.get("n_estimators", 200)),
            max_depth=params.get("max_depth", 14),
            min_samples_leaf=int(params.get("min_samples_leaf", 50)),
            max_features=params.get("max_features", "sqrt"),
            max_samples=params.get("max_samples", 0.5),
            class_weight=params.get("class_weight"),
            n_jobs=n_jobs,
            random_state=seed,
        )

    if name == "xgboost":
        from xgboost import XGBClassifier

        return XGBClassifier(
            objective="binary:logistic",
            tree_method="hist",
            eval_metric="aucpr",
            n_estimators=int(params.get("n_estimators", 1500)),
            learning_rate=float(params.get("learning_rate", 0.05)),
            max_depth=int(params.get("max_depth", 6)),
            min_child_weight=float(params.get("min_child_weight", 1)),
            subsample=float(params.get("subsample", 0.8)),
            colsample_bytree=float(params.get("colsample_bytree", 0.6)),
            reg_lambda=float(params.get("reg_lambda", 1.0)),
            max_bin=int(params.get("max_bin", 256)),
            scale_pos_weight=float(params.get("scale_pos_weight", 1.0)),
            early_stopping_rounds=int(params.get("early_stopping_rounds", 100)),
            n_jobs=n_jobs,
            random_state=seed,
        )

    raise ValueError(f"Unknown model {name}")


def fit_model(name: str, params: dict, data: dict, rate: float, seed: int, n_jobs: int,
              use_weights: bool = False) -> EstimatorScorer:
    """
    Fits one configuration. data has X, y, w (train sample) and Xv, yv, wv
    (validation sample with its weights, for early stopping).

    use_weights: fit with peso_muestra instead of correcting the prior
    afterwards. The probabilities come out of the population already, so the
    prior correction is not applied.
    """
    estimator = build_estimator(name, params, seed, n_jobs)
    weights = data["w"] if use_weights else None
    start = time.time()

    if name == "xgboost":
        estimator.fit(
            data["X"], data["y"],
            sample_weight=weights,
            eval_set=[(data["Xv"], data["yv"])],
            sample_weight_eval_set=[data["wv"]],
            verbose=int(params.get("verbose", 100)),
        )
    elif name == "logistica":
        estimator.fit(data["X"], data["y"], modelo__sample_weight=weights)
    else:
        estimator.fit(data["X"], data["y"], sample_weight=weights)

    scorer = EstimatorScorer(name, data["feature_names"], estimator, 1.0 if use_weights else rate)
    scorer.params = dict(params)
    scorer.info = {"segundos_entrenamiento": round(time.time() - start, 1), "usa_peso_muestra": use_weights}

    if name == "xgboost":
        scorer.info["mejor_iteracion"] = int(estimator.best_iteration)
        scorer.info["n_arboles"] = int(estimator.best_iteration) + 1

    return scorer


def build_reference(name: str, feature_names: list, base_rate: float) -> Scorer:
    if name == "tasa_base":
        return ConstantScorer(feature_names, base_rate)

    if name == "naif_historico":
        return HistoricalScorer(feature_names)

    raise ValueError(f"Unknown reference {name}")


# ---------------------------------------------------------------------------
# Examination
# ---------------------------------------------------------------------------

def importances(scorer: Scorer, X_sample=None) -> dict:
    """
    {metric: array per feature}. For XGBoost the mean absolute SHAP value
    (exact TreeSHAP of the library, pred_contribs) on X_sample and the gain;
    for the random forest the impurity importance; for the logistic
    regression the absolute coefficient of the standardised feature.
    """
    names = scorer.feature_names
    out = {}

    if scorer.name == "xgboost":
        import xgboost as xgb

        booster = scorer.estimator.get_booster()
        gain = booster.get_score(importance_type="gain")
        out["gain"] = np.array([gain.get(f"f{i}", 0.0) for i in range(len(names))])

        if X_sample is not None and len(X_sample):
            limit = (0, scorer.info.get("n_arboles", 0))
            contribs = booster.predict(xgb.DMatrix(X_sample), pred_contribs=True, iteration_range=limit)
            out["shap_medio_abs"] = np.abs(contribs[:, :-1]).mean(axis=0)

    elif scorer.name == "random_forest":
        out["importancia_impureza"] = scorer.estimator.feature_importances_

    elif scorer.name == "logistica":
        coef = scorer.estimator.named_steps["modelo"].coef_[0]
        # The first len(names) coefficients are the features; the rest are
        # the missing indicators added by the imputer.
        out["coef_abs"] = np.abs(coef[:len(names)])

    return out
