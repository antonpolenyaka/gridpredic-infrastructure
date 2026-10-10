"""
Tests of the metrics and models of the training step, without Spark.

    python -m pytest tests/04_ml/test_ml_metrics.py -q
"""

import os
import pickle
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ML_DIR = os.path.join(ROOT, "etl", "jobs", "04_ml")

if ML_DIR not in sys.path:
    sys.path.insert(0, ML_DIR)

import ml_metrics
import ml_models


def hours(*values):
    return [pd.Timestamp("2025-03-01") + pd.Timedelta(f"{h}h") for h in values]


def test_prior_correction_is_monotonic_and_exact():
    p = np.array([0.01, 0.2, 0.5, 0.9])
    q = ml_models.prior_correction(p, 0.02)

    assert np.all(np.diff(q) > 0)
    assert q[2] == pytest.approx(0.5 * 0.02 / (0.5 * 0.02 + 0.5))
    assert np.array_equal(ml_models.prior_correction(p, 1.0), p)


def test_episodes_split_by_ct_and_gap():
    df = pd.DataFrame({
        "ct_key": [1, 1, 1, 1, 1, 2, 2],
        "hora": hours(1, 2, 3, 5, 6, 2, 3),
        "y": [1, 1, 1, 0, 1, 1, 0],
    })

    ep = ml_metrics.episode_ids(df, "y")

    assert ep.tolist()[:3] == [0, 0, 0]
    assert ep.iloc[3] == -1
    assert ep.iloc[4] == 1
    assert ep.iloc[5] == 2
    assert ep.iloc[6] == -1


def test_budget_alerts_take_n_per_group():
    df = pd.DataFrame({
        "distribuidora_id": [1] * 6 + [2] * 3,
        "fecha": [pd.Timestamp("2025-03-01")] * 9,
        "s": [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.5, 0.5, 0.5],
    })

    alerts = ml_metrics.budget_alerts(df, "s", 2, seed=1)

    assert alerts[:6].tolist() == [True, False, True, False, False, False]
    # Ties: exactly two of the three equal scores, chosen at random.
    assert alerts[6:].sum() == 2


def test_budget_metrics_episodes_and_lead_time():
    y = np.array([1, 1, 1, 0, 1, 0])
    ep = np.array([0, 0, 0, -1, 1, -1])
    lead = np.array([3.0, 2.0, 1.0, np.nan, 2.5, np.nan])
    alerts = np.array([False, True, True, True, False, False])

    out = ml_metrics.budget_metrics(y, alerts, ep, lead)

    assert out["alertas"] == 3
    assert out["aciertos"] == 2
    assert out["precision"] == pytest.approx(2 / 3)
    assert out["recall_filas"] == pytest.approx(2 / 4)
    assert out["episodios"] == 2
    assert out["episodios_detectados"] == 1
    assert out["recall_episodios"] == pytest.approx(0.5)
    # Earliest alert of the episode: two hours before the start.
    assert out["antelacion_media_h"] == pytest.approx(2.0)


def test_evaluate_constant_score_gives_the_prevalence():
    rng = np.random.default_rng(0)
    n = 2000
    df = pd.DataFrame({
        "distribuidora_id": rng.integers(1, 3, n),
        "ct_key": rng.integers(0, 50, n),
        "hora": pd.Timestamp("2025-01-01") + pd.to_timedelta(rng.integers(0, 24 * 30, n), unit="h"),
        "y": (rng.random(n) < 0.05).astype(int),
        "zona": rng.integers(1, 3, n).astype(float),
    })
    df["p_tasa_base"] = 0.05
    df["p_bueno"] = df["y"] + rng.random(n) * 0.5

    metrics = ml_metrics.evaluate(df, ["p_tasa_base", "p_bueno"], ["y"], [10], {"zona": "zona"})
    get = metrics.set_index(["modelo", "segmento", "valor_segmento", "metrica"])["valor"]

    assert get[("tasa_base", "total", "todos", "pr_auc")] == pytest.approx(df["y"].mean())
    assert get[("bueno", "total", "todos", "pr_auc")] == pytest.approx(1.0)
    assert ("bueno", "zona", "1", "recall_episodios@10") in get.index
    # About 1.6 positives per distribuidora and day: a budget of 10 catches
    # all of them with the perfect score, a few with the constant one.
    assert get[("bueno", "total", "todos", "recall_filas@10")] == pytest.approx(1.0)
    assert get[("tasa_base", "total", "todos", "recall_filas@10")] < 0.6


def test_univariate_ap_finds_a_leaky_feature():
    rng = np.random.default_rng(1)
    y = (rng.random(5000) < 0.02).astype(int)
    X = np.column_stack([
        rng.random(5000),
        y + rng.random(5000) * 0.1,
        -(y * 2.0) + rng.random(5000) * 0.1,
        np.full(5000, np.nan),
    ]).astype(np.float32)

    ap = ml_metrics.univariate_ap(X, y)

    assert ap[0] < 0.1
    assert ap[1] == pytest.approx(1.0)
    assert ap[2] == pytest.approx(1.0)
    assert np.isnan(ap[3])


@pytest.fixture(scope="module")
def synthetic():
    rng = np.random.default_rng(2)
    names = ["hist_interr_365d", "hist_interr_90d", "hist_interr_30d", "med_intensidad_max", "ev_defecto_tierra_24h"]

    def make(n):
        X = np.column_stack([
            rng.poisson(1.0, n), rng.poisson(0.3, n), rng.poisson(0.1, n),
            np.where(rng.random(n) < 0.3, np.nan, rng.normal(100, 20, n)),
            rng.poisson(0.2, n),
        ]).astype(np.float32)
        logit = -4 + 0.8 * X[:, 0] + 1.5 * X[:, 4]
        y = (rng.random(n) < 1 / (1 + np.exp(-logit))).astype(np.int8)
        return X, y

    X, y = make(4000)
    Xv, yv = make(2000)

    return {
        "X": X, "y": y, "w": np.ones(len(y)),
        "Xv": Xv, "yv": yv, "wv": np.ones(len(yv)),
        "feature_names": names,
    }


@pytest.mark.parametrize("name,params", [
    ("logistica", {"C": 0.1}),
    ("random_forest", {"n_estimators": 20, "max_depth": 6, "min_samples_leaf": 5}),
    ("xgboost", {"n_estimators": 50, "max_depth": 3, "early_stopping_rounds": 10, "verbose": 0}),
])
def test_trained_models_rank_better_than_the_base_rate(synthetic, name, params):
    scorer = ml_models.fit_model(name, params, synthetic, rate=0.5, seed=0, n_jobs=1)
    score = scorer.score(synthetic["Xv"])

    assert score.dtype == np.float32
    assert np.all((score >= 0) & (score <= 1))

    base = synthetic["yv"].mean()
    assert ml_metrics.average_precision(synthetic["yv"], score) > 2 * base

    # The executors get the pickled scorer.
    again = pickle.loads(pickle.dumps(scorer.single_thread()))
    assert np.allclose(again.score(synthetic["Xv"]), score)

    importance = ml_models.importances(scorer, synthetic["Xv"][:200])
    assert importance
    assert all(len(v) == len(synthetic["feature_names"]) for v in importance.values())

    if name == "xgboost":
        assert scorer.info["n_arboles"] == scorer.info["mejor_iteracion"] + 1
        assert "shap_medio_abs" in importance


def test_references(synthetic):
    naive = ml_models.build_reference("naif_historico", synthetic["feature_names"], 0.01)
    score = naive.score(synthetic["Xv"])

    order = np.lexsort((synthetic["Xv"][:, 2], synthetic["Xv"][:, 1], synthetic["Xv"][:, 0]))
    assert np.all(np.diff(score[order]) >= 0)

    constant = ml_models.build_reference("tasa_base", synthetic["feature_names"], 0.01)
    assert np.all(constant.score(synthetic["Xv"]) == np.float32(0.01))

    with pytest.raises(ValueError):
        ml_models.build_reference("naif_historico", ["otra"], 0.01)


def test_fit_clusters_finds_the_groups_and_respects_min_size():
    rng = np.random.default_rng(4)
    pdf = pd.DataFrame({
        "potencia": np.r_[rng.normal(100, 5, 40), rng.normal(1000, 50, 40)],
        "zona": np.r_[np.full(40, 4), np.full(40, 1)],
        "abonados": np.r_[rng.normal(20, 2, 40), rng.normal(400, 20, 40)],
    })
    pdf.loc[5, "abonados"] = np.nan  # imputed with the median
    X, columns = ml_models.cluster_matrix(pdf, ["potencia", "abonados"], ["potencia"], ["zona"])

    assert X.shape == (80, 4)
    assert "zona_1" in columns and "zona_4" in columns
    assert not np.isnan(X).any()

    labels, k, tried = ml_models.fit_clusters(X, [2, 3, 4], seed=0, min_size=10)
    assert k == 2
    assert len(set(labels[:40])) == 1 and len(set(labels[40:])) == 1
    assert labels[0] != labels[40]
    assert [t[0] for t in tried] == [2, 3, 4]


def test_cluster_scorer_routes_rows_to_their_group(synthetic):
    names = synthetic["feature_names"] + ["ct_cluster"]
    groups = (synthetic["X"][:, 0] > 1).astype(np.float32)
    groups_v = (synthetic["Xv"][:, 0] > 1).astype(np.float32)
    data = {**synthetic, "X": np.column_stack([synthetic["X"], groups]),
            "Xv": np.column_stack([synthetic["Xv"], groups_v]), "feature_names": names}

    params = {"n_estimators": 30, "max_depth": 3, "early_stopping_rounds": 10, "verbose": 0}
    base = ml_models.fit_model("xgboost", params, data, rate=0.5, seed=0, n_jobs=1)
    sub = ml_models.fit_model("xgboost", params, {**data, "X": data["X"][groups == 1], "y": data["y"][groups == 1],
                                                  "w": data["w"][groups == 1]}, rate=0.5, seed=0, n_jobs=1)

    scorer = ml_models.ClusterScorer("xgboost_cluster", names, "ct_cluster", {1.0: sub}, base)
    score = scorer.score(data["Xv"])

    in_group = groups_v == 1
    assert np.allclose(score[in_group], sub.score(data["Xv"][in_group]))
    assert np.allclose(score[~in_group], base.score(data["Xv"][~in_group]))

    again = pickle.loads(pickle.dumps(scorer.single_thread()))
    assert np.allclose(again.score(data["Xv"]), score)

    shap = ml_models.importances(scorer, data["Xv"][:300])
    assert "shap_medio_abs_grupo_1.0" in shap
