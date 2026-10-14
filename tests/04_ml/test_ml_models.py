"""The models of ml_models on small synthetic data, without Spark."""

import os
import sys

import numpy as np

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")
if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import ml_metrics  # noqa: E402
import ml_models  # noqa: E402


def synthetic(seed: int = 3, n: int = 3000):
    """A gust feature that drives the number of incidents of the day, a noise feature and a gap in the first one."""
    rng = np.random.default_rng(seed)
    gust = rng.gamma(4.0, 1.0, n)
    noise = rng.normal(size=n)
    counts = rng.poisson(np.exp(-4.5 + 0.7 * gust))
    X = np.column_stack([gust, noise]).astype(np.float32)
    X[rng.random(n) < 0.05, 0] = np.nan
    y = (counts > 0).astype(np.int8)
    return X, y, counts.astype(np.float64)


def data_of(X, y, counts=None, split: int = 2000):
    out = {"X": X[:split], "y": y[:split], "w": np.ones(split), "Xv": X[split:], "yv": y[split:],
           "wv": np.ones(len(y) - split), "feature_names": ["racha", "ruido"]}
    if counts is not None:
        out["n"] = counts[:split]
    return out


def test_the_poisson_model_learns_the_counts_and_ranks_the_days():
    X, y, counts = synthetic()
    data = data_of(X, y, counts)
    scorer = ml_models.fit_model("poisson", {"alpha": 1.0}, data, 1.0, 0, 1)
    assert isinstance(scorer, ml_models.CountScorer) and scorer.name == "poisson"
    score = scorer.score(data["Xv"])
    assert score.dtype == np.float32 and (score >= 0).all()
    # Its score is a number of incidents: the mean over the validation days is of the order of the observed one
    # (the generator is exponential in the gust and the pipeline takes its log, so it is not exact).
    observed = counts[2000:].mean()
    assert 0.5 * observed < score.mean() < 1.5 * observed
    assert np.corrcoef(score, counts[2000:])[0, 1] > 0.5
    # And it ranks the days with incidents like the logistic regression does.
    logistic = ml_models.fit_model("logistica", {"C": 1.0}, data, 1.0, 0, 1)
    ap_counts = ml_metrics.average_precision(data["yv"], score)
    ap_logit = ml_metrics.average_precision(data["yv"], logistic.score(data["Xv"]))
    assert ap_counts > 2 * data["yv"].mean() and abs(ap_counts - ap_logit) < 0.1
    # The gust weighs, the noise does not.
    weights = ml_models.importances(scorer)["coef_abs"]
    assert weights[0] > 5 * weights[1]
    assert scorer.info["segundos_entrenamiento"] >= 0 and scorer.params == {"alpha": 1.0}
    # Without counts it learns from the binary label.
    binary = ml_models.fit_model("poisson", {"alpha": 1.0}, data_of(X, y), 1.0, 0, 1)
    assert binary.score(data["Xv"]).max() <= 1.5


def test_the_balanced_logistic_regression_gives_more_weight_to_the_positives():
    X, y, _ = synthetic()
    data = data_of(X, y)
    plain = ml_models.fit_model("logistica", {"C": 1.0}, data, 1.0, 0, 1)
    balanced = ml_models.fit_model("logistica_equilibrada", {"C": 1.0}, data, 1.0, 0, 1)
    assert isinstance(balanced, ml_models.EstimatorScorer)
    assert balanced.estimator.named_steps["modelo"].class_weight == "balanced"
    # Same ranking family: the same days on top; higher probabilities because the positives weigh more.
    p_plain, p_balanced = plain.score(data["Xv"]), balanced.score(data["Xv"])
    assert np.corrcoef(p_plain, p_balanced)[0, 1] > 0.95
    assert p_balanced.mean() > p_plain.mean()
    assert ml_models.importances(balanced)["coef_abs"].shape == (2,)
    assert "logistica_equilibrada" in ml_models.TRAINED and "poisson" in ml_models.TRAINED
