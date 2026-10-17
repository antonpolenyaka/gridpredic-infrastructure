"""The models of ml_models on small synthetic data, without Spark."""

import os
import pickle
import sys

import numpy as np
import pytest

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


def test_firth_matches_the_closed_forms_and_flic_keeps_the_observed_rate():
    from scipy.special import expit

    # Intercept only: Firth adds half an event and half a non event, (k + 0.5) / (n + 1); FLIC gives k / n back.
    y = np.r_[np.ones(3), np.zeros(97)]
    empty = np.zeros((100, 0))
    firth = ml_models.FirthLogisticRegression(flic=False).fit(empty, y)
    assert expit(firth.intercept_[0]) == pytest.approx(3.5 / 101, rel=1e-6)
    flic = ml_models.FirthLogisticRegression(flic=True).fit(empty, y)
    assert expit(flic.intercept_[0]) == pytest.approx(0.03, rel=1e-8)

    # A single binary feature with a zero cell (separation): the odds ratio of the table with 0.5 in every cell.
    a, b, c, d = 5, 0, 20, 75
    x = np.r_[np.ones(a + b), np.zeros(c + d)][:, None]
    y = np.r_[np.ones(a), np.zeros(b), np.ones(c), np.zeros(d)]
    firth = ml_models.FirthLogisticRegression(flic=False).fit(x, y)
    assert firth.converged_
    assert firth.coef_[0, 0] == pytest.approx(np.log((a + 0.5) * (d + 0.5) / ((b + 0.5) * (c + 0.5))), abs=1e-5)
    assert firth.intercept_[0] == pytest.approx(np.log((c + 0.5) / (d + 0.5)), abs=1e-5)


def test_firth_ignores_repeated_and_constant_columns_and_flic_fixes_the_mean():
    from scipy.special import expit

    rng = np.random.default_rng(0)
    X = rng.normal(size=(3000, 6))
    y = (rng.random(3000) < expit(-3.8 + X @ np.r_[1.0, -0.7, 0.5, 0, 0, 0])).astype(float)
    plain = ml_models.FirthLogisticRegression(flic=False).fit(X, y)
    # Two missing indicators of features that are missing on the same days are the same column; an empty feature
    # is a column of zeros after the imputer and the scaler.
    wide = np.column_stack([X, X[:, 0], np.zeros(3000), np.full(3000, 3.0)])
    again = ml_models.FirthLogisticRegression(flic=False).fit(wide, y)
    assert np.allclose(again.coef_[0, -3:], 0.0)
    assert np.allclose(again.predict_proba(wide)[:, 1], plain.predict_proba(X)[:, 1], atol=1e-8)
    # The penalty pulls the rare probabilities up; FLIC brings their mean back to the rate without moving the order.
    flic = ml_models.FirthLogisticRegression(flic=True).fit(X, y)
    assert plain.predict_proba(X)[:, 1].mean() > y.mean()
    assert flic.predict_proba(X)[:, 1].mean() == pytest.approx(y.mean(), rel=1e-6)
    assert np.allclose(flic.coef_, plain.coef_)


def test_the_cauchy_prior_is_the_maximum_likelihood_when_wide_and_stays_finite_under_separation():
    from scipy.special import expit
    from sklearn.linear_model import LogisticRegression

    rng = np.random.default_rng(1)
    X = rng.normal(size=(3000, 5))
    y = (rng.random(3000) < expit(-3.5 + X @ np.r_[1.0, -0.6, 0, 0, 0])).astype(float)
    ml = LogisticRegression(penalty=None, max_iter=5000, tol=1e-12).fit(X, y)
    wide = ml_models.CauchyLogisticRegression(scale=1e6, intercept_scale=1e6).fit(X, y)
    assert np.allclose(np.r_[wide.intercept_, wide.coef_[0]], np.r_[ml.intercept_, ml.coef_[0]], atol=1e-4)
    # The default prior: the mode of the posterior, where its gradient is zero.
    model = ml_models.CauchyLogisticRegression().fit(X, y)
    beta = np.r_[model.intercept_, model.coef_[0]]
    Z = np.column_stack([np.ones(3000), X])
    scales = np.r_[10.0, np.full(5, 1.25)]
    gradient = Z.T @ (y - expit(Z @ beta)) - 2 * beta / (scales ** 2 + beta ** 2)
    assert model.converged_ and np.abs(gradient).max() < 1e-6
    assert np.abs(model.coef_[0]).sum() < np.abs(ml.coef_[0]).sum()
    # A feature that separates the classes: the maximum likelihood diverges, the prior keeps it finite.
    x = np.r_[rng.normal(-1, 0.3, 50), rng.normal(2, 0.3, 5)][:, None]
    labels = np.r_[np.zeros(50), np.ones(5)]
    separated = ml_models.CauchyLogisticRegression().fit(x, labels)
    assert separated.converged_ and 0 < separated.coef_[0, 0] < 10


@pytest.mark.parametrize("name,params", [("logistica_firth", {}), ("logistica_bayes", {}),
                                         ("logistica_bayes", {"escala": 0.5})])
def test_the_penalised_logistic_regressions_run_in_the_pipeline(name, params):
    X, y, _ = synthetic()
    data = data_of(X, y)
    scorer = ml_models.fit_model(name, params, data, 1.0, 0, 1)
    assert isinstance(scorer, ml_models.EstimatorScorer) and name in ml_models.TRAINED
    score = scorer.score(data["Xv"])
    assert score.dtype == np.float32 and ((score >= 0) & (score <= 1)).all()
    assert ml_metrics.average_precision(data["yv"], score) > 2 * data["yv"].mean()
    # The same ranking family as the L2 logistic regression.
    logistic = ml_models.fit_model("logistica", {"C": 1.0}, data, 1.0, 0, 1)
    assert np.corrcoef(score, logistic.score(data["Xv"]))[0, 1] > 0.9
    weights = ml_models.importances(scorer)["coef_abs"]
    assert weights.shape == (2,) and weights[0] > 5 * weights[1]
    again = pickle.loads(pickle.dumps(scorer.single_thread()))
    assert np.allclose(again.score(data["Xv"]), score)


@pytest.mark.parametrize("name", ["logistica", "logistica_firth", "poisson"])
def test_the_coefficients_of_the_linear_models_with_their_odds_ratios(name):
    X, y, counts = synthetic()
    scorer = ml_models.fit_model(name, {"C": 1.0} if name == "logistica" else {}, data_of(X, y, counts), 1.0, 0, 1)
    rows = ml_models.linear_terms(scorer)
    # The intercept, the two features and the indicator of the gap of the gust that the imputer adds.
    assert [r["variable"] for r in rows] == ["(intercepto)", "racha", "ruido", "racha__falta"]
    for row in rows:
        assert row["razon_por_desviacion"] == pytest.approx(np.exp(row["coeficiente"]))
    terms = {r["variable"]: r["coeficiente"] for r in rows}
    # More gust, more risk; the noise weighs little; and the same coefficients the importances report.
    assert terms["racha"] > 0.3 and abs(terms["ruido"]) < 0.2 * terms["racha"]
    assert ml_models.importances(scorer)["coef_abs"] == pytest.approx(np.abs([terms["racha"], terms["ruido"]]), rel=1e-6)
    # The models without coefficients give nothing.
    assert ml_models.linear_terms(ml_models.build_reference("tasa_base", ["racha", "ruido"], 0.1)) == []
