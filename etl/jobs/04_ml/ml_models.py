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
- xgboost_cluster: one XGBoost per group of similar CTs (fit_clusters), with
  the configuration chosen for the global model; a group with too few
  positives uses the global model. Divide and conquer: a single model over
  very different CTs dilutes what each kind of CT has to say.
- logistica_equilibrada: the logistic regression with class_weight balanced
  (Sharma et al. 2023: in small rural networks the imbalance is the main
  problem). Same ranking family, more weight on the few positives.
- poisson: a regularised Poisson regression on the same pipeline, fitted on
  the counts of the rows (data["n"], the number of incidents of the day)
  when the caller gives them and on the binary label otherwise. Its score
  is the expected number of incidents, a count instead of a probability
  (Liu et al. 2005, Guikema and Quiring 2012): a day with six incidents
  teaches more than one with one. Compared with the others on the same
  binary metrics.
- logistica_firth: the logistic regression with the bias reduction of
  Firth (1993) and the intercept of FLIC (Puhr et al. 2017), on the same
  pipeline. Nothing to tune.
- logistica_bayes: the logistic regression at the mode of the posterior
  with the weakly informative Cauchy prior of Gelman et al. (2008), on the
  same pipeline. Nothing to tune either.

The last two answer the few events of the model of the day: about 17 days
with an incident a year per network for 20 - 40 features, 3 - 6 events per
variable against the 10 - 20 of the usual rules, so the maximum likelihood
overfits and the L2 has to choose its C on a handful of positives.
"""

import time

import numpy as np
from sklearn.base import BaseEstimator, ClassifierMixin


TRAINED = ("logistica", "random_forest", "xgboost", "logistica_equilibrada", "poisson", "logistica_firth",
           "logistica_bayes")
# The models on the pipeline of imputation, log scale and standardisation, whose importance is the coefficient.
LINEAR = ("logistica", "logistica_equilibrada", "poisson", "logistica_firth", "logistica_bayes")
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


# ---------------------------------------------------------------------------
# Logistic regressions for few events
# ---------------------------------------------------------------------------

def independent_columns(X) -> np.ndarray:
    """
    Indexes of a set of linearly independent columns of X once the model has
    an intercept: QR with column pivoting on the centred columns of unit
    norm. A constant column and a copy of another one are left out. The
    missing indicators of two features that are missing on the same days are
    the same column, and without a strong penalty the information matrix of
    the likelihood would be singular.
    """
    from scipy.linalg import qr

    X = np.asarray(X, dtype=np.float64)
    centred = X - X.mean(axis=0)
    norms = np.sqrt((centred ** 2).sum(axis=0))
    raw = np.sqrt((X ** 2).sum(axis=0))
    # Relative to the size of the column: a constant column keeps a rounding residue after centring.
    varying = np.flatnonzero(norms > 1e-10 * np.maximum(raw, 1e-300))

    if varying.size == 0:
        return varying

    _, r, pivot = qr(centred[:, varying] / norms[varying], mode="economic", pivoting=True)
    diagonal = np.abs(np.diag(r))
    rank = int((diagonal > 1e-7 * diagonal[0]).sum())
    return np.sort(varying[pivot[:rank]])


def intercept_with_offset(y, w, offset, start: float = 0.0, tol: float = 1e-10, max_iter: int = 100) -> float:
    """
    The maximum likelihood intercept b of a logistic regression whose linear
    predictor is b + offset (offset fixed): the root of sum w (y - p) = 0,
    by Newton. Without positives or without negatives there is no root and
    start is returned.
    """
    from scipy.special import expit

    y, w, offset = (np.asarray(a, dtype=np.float64) for a in (y, w, offset))
    target, total = float(np.sum(w * y)), float(np.sum(w))

    if target <= 0.0 or target >= total:
        return float(start)

    b = float(start)
    for _ in range(max_iter):
        p = expit(b + offset)
        curvature = float(np.sum(w * p * (1.0 - p)))
        delta = float(np.clip((target - float(np.sum(w * p))) / max(curvature, 1e-300), -5.0, 5.0))
        b += delta
        if abs(delta) < tol:
            break

    return b


class FirthLogisticRegression(ClassifierMixin, BaseEstimator):
    """
    Logistic regression with the bias reduction of Firth (1993): the log
    likelihood gets the penalty 0.5 log |X'WX|, the Jeffreys prior. It
    removes the first order bias of the coefficients with few events and
    keeps them finite when a feature separates the classes, and it has
    nothing to tune. With a single binary feature it gives the odds ratio of
    the 2 x 2 table with half an event added to every cell.

    The penalty pulls the probabilities of a rare event towards 0.5. With
    flic (Puhr et al. 2017) the intercept is fitted again without the
    penalty, with the rest of the linear predictor as an offset, so the mean
    probability of the training rows is the observed rate; the ranking does
    not change.

    Newton-Raphson on the modified score, with a cap on the step and step
    halving on the penalised likelihood, as the logistf package of R does.
    Constant and repeated columns get coefficient 0 (independent_columns).
    """

    def __init__(self, flic: bool = True, max_iter: int = 100, tol: float = 1e-6, max_step: float = 5.0,
                 max_halvings: int = 30):
        self.flic = flic
        self.max_iter = max_iter
        self.tol = tol
        self.max_step = max_step
        self.max_halvings = max_halvings

    @staticmethod
    def _penalised(Z, y, w, beta) -> float:
        from scipy.special import expit

        eta = Z @ beta
        loglik = float(np.sum(w * (y * eta - np.logaddexp(0.0, eta))))
        p = expit(eta)
        sign, logdet = np.linalg.slogdet(Z.T @ (Z * (w * p * (1.0 - p))[:, None]))
        return loglik + 0.5 * logdet if sign > 0 else -np.inf

    def fit(self, X, y, sample_weight=None):
        from scipy.special import expit

        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64).ravel()
        w = np.ones(len(y)) if sample_weight is None else np.asarray(sample_weight, dtype=np.float64)
        self.classes_ = np.array([0, 1])
        self.n_features_in_ = X.shape[1]

        columns = independent_columns(X)
        Z = np.column_stack([np.ones(len(y)), X[:, columns]])
        rate = float(np.clip(np.average(y, weights=w), 1e-8, 1.0 - 1e-8))
        beta = np.zeros(Z.shape[1])
        beta[0] = np.log(rate / (1.0 - rate))
        current = self._penalised(Z, y, w, beta)
        self.converged_ = False

        for iteration in range(1, int(self.max_iter) + 1):
            p = expit(Z @ beta)
            weight = w * p * (1.0 - p)
            # info^-1 Z': the hat values and the Newton step come out of the same solve.
            solved = np.linalg.solve(Z.T @ (Z * weight[:, None]), Z.T)
            hat = weight * np.einsum("ij,ji->i", Z, solved)
            step = solved @ (w * (y - p) + hat * (0.5 - p))
            largest = float(np.abs(step).max())
            if largest > self.max_step:
                step *= self.max_step / largest

            candidate = self._penalised(Z, y, w, beta + step)
            for _ in range(int(self.max_halvings)):
                if candidate >= current - 1e-10 * max(1.0, abs(current)):
                    break
                step /= 2.0
                candidate = self._penalised(Z, y, w, beta + step)

            beta, current = beta + step, candidate
            if float(np.abs(step).max()) < self.tol:
                self.converged_ = True
                break

        self.n_iter_ = iteration
        coef = np.zeros(X.shape[1])
        coef[columns] = beta[1:]
        intercept = float(beta[0])
        self.intercept_firth_ = intercept

        if self.flic:
            intercept = intercept_with_offset(y, w, X @ coef, intercept)

        self.coef_ = coef[None, :]
        self.intercept_ = np.array([intercept])
        return self

    def decision_function(self, X):
        return np.asarray(X, dtype=np.float64) @ self.coef_[0] + self.intercept_[0]

    def predict_proba(self, X):
        from scipy.special import expit

        p = expit(self.decision_function(X))
        return np.column_stack([1.0 - p, p])

    def predict(self, X):
        return (self.decision_function(X) > 0).astype(int)


class CauchyLogisticRegression(ClassifierMixin, BaseEstimator):
    """
    Logistic regression at the mode of the posterior with the weakly
    informative prior of Gelman et al. (2008): independent Cauchy
    distributions centred at 0, with scale 2.5 for inputs scaled to a
    standard deviation of 0.5 and 10 for the intercept. The pipeline scales
    the features to a standard deviation of 1, so the default scale is 1.25,
    the same prior per unit of the scaled feature. Unlike the normal prior
    of the L2, the heavy tails of the Cauchy let a strong effect (the gust
    on a storm day) keep its size while the weak ones are pulled towards 0,
    and there is no C to choose on a handful of positives.

    The fit writes the Cauchy as a scale mixture of normals: every step is a
    Newton step of a ridge whose penalty per coefficient, 2 / (s^2 + b^2),
    follows the current coefficient, with step halving on the log posterior
    (the iteration of the bayesglm function of the arm package of R). The
    prior is not log concave, but with thousands of days the likelihood
    dominates and the mode is unique in practice.
    """

    def __init__(self, scale: float = 1.25, intercept_scale: float = 10.0, max_iter: int = 200, tol: float = 1e-8):
        self.scale = scale
        self.intercept_scale = intercept_scale
        self.max_iter = max_iter
        self.tol = tol

    @staticmethod
    def _log_posterior(Z, y, w, beta, scales) -> float:
        eta = Z @ beta
        return float(np.sum(w * (y * eta - np.logaddexp(0.0, eta))) - np.sum(np.log1p((beta / scales) ** 2)))

    def fit(self, X, y, sample_weight=None):
        from scipy.special import expit

        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64).ravel()
        w = np.ones(len(y)) if sample_weight is None else np.asarray(sample_weight, dtype=np.float64)
        self.classes_ = np.array([0, 1])
        self.n_features_in_ = X.shape[1]

        Z = np.column_stack([np.ones(len(y)), X])
        scales = np.r_[float(self.intercept_scale), np.full(X.shape[1], float(self.scale))]
        rate = float(np.clip(np.average(y, weights=w), 1e-8, 1.0 - 1e-8))
        beta = np.zeros(Z.shape[1])
        beta[0] = np.log(rate / (1.0 - rate))
        current = self._log_posterior(Z, y, w, beta, scales)
        self.converged_ = False

        for iteration in range(1, int(self.max_iter) + 1):
            p = expit(Z @ beta)
            penalty = 2.0 / (scales ** 2 + beta ** 2)
            gradient = Z.T @ (w * (y - p)) - penalty * beta
            step = np.linalg.solve(Z.T @ (Z * (w * p * (1.0 - p))[:, None]) + np.diag(penalty), gradient)

            candidate = self._log_posterior(Z, y, w, beta + step, scales)
            for _ in range(30):
                if candidate >= current - 1e-12 * max(1.0, abs(current)):
                    break
                step /= 2.0
                candidate = self._log_posterior(Z, y, w, beta + step, scales)

            beta, current = beta + step, candidate
            if float(np.abs(step).max()) < self.tol:
                self.converged_ = True
                break

        self.n_iter_ = iteration
        self.coef_ = beta[None, 1:]
        self.intercept_ = beta[:1].copy()
        return self

    def decision_function(self, X):
        return np.asarray(X, dtype=np.float64) @ self.coef_[0] + self.intercept_[0]

    def predict_proba(self, X):
        from scipy.special import expit

        p = expit(self.decision_function(X))
        return np.column_stack([1.0 - p, p])

    def predict(self, X):
        return (self.decision_function(X) > 0).astype(int)


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

    def __init__(self, feature_names: list, naive_features=NAIVE_FEATURES):
        super().__init__("naif_historico", feature_names)
        self.columns = [feature_names.index(f) for f in naive_features if f in feature_names]

        if not self.columns:
            raise ValueError(f"The naive reference needs one of {list(naive_features)} in the features")

        self.params = {"features": [feature_names[i] for i in self.columns]}

    def score(self, X):
        out = np.zeros(len(X), dtype=np.float64)

        for weight, column in zip((1.0, 1e-3, 1e-6), self.columns):
            out += weight * np.nan_to_num(X[:, column].astype(np.float64), nan=0.0)

        return out.astype(np.float32)


class FeatureScorer(Scorer):
    """
    One feature used as the score (missing values at the bottom). A
    reference of what an operator would watch without a model, for example
    the strongest gust forecast in the area of the distribuidora.
    """

    def __init__(self, name: str, feature_names: list, feature: str):
        super().__init__(name, feature_names)

        if feature not in feature_names:
            raise ValueError(f"The reference {name} needs the feature {feature}")

        self.column = feature_names.index(feature)
        self.params = {"feature": feature}

    def score(self, X):
        x = X[:, self.column].astype(np.float64)
        missing = np.isnan(x)
        low = np.nanmin(x) - 1.0 if not missing.all() else 0.0
        return np.where(missing, low, x).astype(np.float32)


class CountScorer(Scorer):
    """A scikit-learn compatible regressor of counts: the score is the expected number of events."""

    def __init__(self, name: str, feature_names: list, estimator):
        super().__init__(name, feature_names, 1.0)
        self.estimator = estimator

    def score(self, X):
        return np.clip(self.estimator.predict(X), 0.0, None).astype(np.float32)

    def single_thread(self):
        return self


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
    if name in LINEAR:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression, PoissonRegressor
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import FunctionTransformer, StandardScaler

        if name == "poisson":
            model = PoissonRegressor(alpha=float(params.get("alpha", 1.0)), max_iter=int(params.get("max_iter", 1000)))
        elif name == "logistica_firth":
            model = FirthLogisticRegression(flic=bool(params.get("flic", True)),
                                            max_iter=int(params.get("max_iter", 100)))
        elif name == "logistica_bayes":
            model = CauchyLogisticRegression(scale=float(params.get("escala", 1.25)),
                                             intercept_scale=float(params.get("escala_intercepto", 10.0)),
                                             max_iter=int(params.get("max_iter", 200)))
        else:
            model = LogisticRegression(
                C=float(params.get("C", 0.1)),
                max_iter=int(params.get("max_iter", 1000)),
                solver="lbfgs",
                class_weight="balanced" if name == "logistica_equilibrada" else params.get("class_weight"),
                random_state=seed,
            )
        # add_indicator: a missing measurement (CT without telemetry, gap of
        # the SCADA) is information by itself.
        return Pipeline([
            ("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("log", FunctionTransformer(signed_log1p)),
            ("escalar", StandardScaler()),
            ("modelo", model),
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
    elif name == "poisson":
        counts = data.get("n")
        target = np.asarray(counts if counts is not None else data["y"], dtype=np.float64)
        estimator.fit(data["X"], target, modelo__sample_weight=weights)
    elif name.startswith("logistica"):
        estimator.fit(data["X"], data["y"], modelo__sample_weight=weights)
    else:
        estimator.fit(data["X"], data["y"], sample_weight=weights)

    if name == "poisson":
        scorer = CountScorer(name, data["feature_names"], estimator)
    else:
        scorer = EstimatorScorer(name, data["feature_names"], estimator, 1.0 if use_weights else rate)
    scorer.params = dict(params)
    scorer.info = {"segundos_entrenamiento": round(time.time() - start, 1), "usa_peso_muestra": use_weights}

    if name == "xgboost":
        scorer.info["mejor_iteracion"] = int(estimator.best_iteration)
        scorer.info["n_arboles"] = int(estimator.best_iteration) + 1

    return scorer


def build_reference(name: str, feature_names: list, base_rate: float, naive_features=NAIVE_FEATURES) -> Scorer:
    if name == "tasa_base":
        return ConstantScorer(feature_names, base_rate)

    if name == "naif_historico":
        return HistoricalScorer(feature_names, naive_features)

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

    elif scorer.name in LINEAR:
        coef = np.asarray(scorer.estimator.named_steps["modelo"].coef_)
        coef = coef[0] if coef.ndim == 2 else coef
        # The first len(names) coefficients are the features; the rest are
        # the missing indicators added by the imputer.
        out["coef_abs"] = np.abs(coef[:len(names)])

    elif isinstance(scorer, ClusterScorer):
        # What weighs in every group: mean absolute SHAP of its submodel on
        # the rows of that group.
        groups = X_sample[:, scorer.column] if X_sample is not None and len(X_sample) else None

        for group, model in scorer.models.items():
            rows = X_sample[groups == group] if groups is not None else None
            sub = importances(model, rows)
            if "shap_medio_abs" in sub:
                out[f"shap_medio_abs_grupo_{group}"] = sub["shap_medio_abs"]

    return out


# ---------------------------------------------------------------------------
# Groups of CTs
# ---------------------------------------------------------------------------

class ClusterScorer(Scorer):
    """
    Scores every row with the submodel of its group (the value of the
    cluster column of the row) and the fallback model when its group has no
    submodel.
    """

    def __init__(self, name: str, feature_names: list, cluster_feature: str, models: dict, fallback: Scorer):
        super().__init__(name, feature_names)
        self.cluster_feature = cluster_feature
        self.column = feature_names.index(cluster_feature)
        self.models = dict(models)
        self.fallback = fallback

    def score(self, X):
        out = self.fallback.score(X)
        groups = X[:, self.column]

        for group, model in self.models.items():
            mask = groups == group
            if mask.any():
                out[mask] = model.score(X[mask])

        return out

    def single_thread(self):
        self.fallback.single_thread()
        for model in self.models.values():
            model.single_thread()
        return self


def cluster_matrix(pdf, numeric: list, log: list, categorical: list):
    """
    Matrix for k-means from one row per CT: numeric variables (log scale for
    the skewed ones, median for the missing), one hot of the categorical
    ones, every column standardised so no variable weighs more for its unit.
    """
    import pandas as pd
    from sklearn.preprocessing import StandardScaler

    parts = []

    for column in numeric:
        values = pd.to_numeric(pdf[column], errors="coerce").astype(float)
        if column in log:
            values = np.log1p(values.clip(lower=0))
        parts.append(values.fillna(values.median() if values.notna().any() else 0.0).rename(column))

    for column in categorical:
        dummies = pd.get_dummies(pdf[column].astype("Int64").astype(str), prefix=column, dtype=float)
        parts.append(dummies)

    frame = pd.concat(parts, axis=1)
    return StandardScaler().fit_transform(frame.to_numpy(dtype=float)), list(frame.columns)


def fit_clusters(X: np.ndarray, k_values: list, seed: int, min_size: int = 1) -> tuple:
    """
    k-means for every k of k_values; keeps the k with the best silhouette
    among those whose smallest group has at least min_size CTs (the best
    silhouette of all if none does). Returns (labels, k, [(k, silhouette,
    smallest group)]).
    """
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    tried = []
    best = None

    for k in k_values:
        if k < 2 or k >= len(X):
            continue

        labels = KMeans(n_clusters=k, n_init=10, random_state=seed).fit_predict(X)
        silhouette = float(silhouette_score(X, labels))
        smallest = int(np.bincount(labels).min())
        tried.append((k, silhouette, smallest))

        valid = smallest >= min_size
        key = (valid, silhouette)

        if best is None or key > best[0]:
            best = (key, labels, k)

    if best is None:
        return np.zeros(len(X), dtype=int), 1, tried

    return best[1], best[2], tried
