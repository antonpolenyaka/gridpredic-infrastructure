"""
Evaluation metrics of the model card, on pandas. No Spark here: the
training job collects the scores of a split and calls evaluate().

- PR-AUC (average precision) and ROC-AUC, with weights when the rows are a
  sample (validation sample of the training job).
- Alerts under a budget: the operator can only attend N alerts per day and
  distribuidora, so the N rows (CT-hours) with the highest score of each
  (distribuidora, day) are the alerts. Precision and recall are measured on
  them, by rows and by episodes.
- Episode: consecutive positive hours of the same CT for a label. With
  y_1_3h an interruption makes about three positive hours in a row (it
  starts between one and three hours after each of them); the operator
  needs one alert per interruption, not three, so the useful recall is the
  share of episodes with at least one alert.
- Lead time: for every detected episode, horas_hasta_proximo_evento of its
  earliest alerted row, i.e. how many hours before the start the first
  alert came.
- Calibration of a probability (calibration): Brier score and its skill
  against the observed rate, log loss, and the slope and intercept of the
  logistic recalibration (Cox 1958). The ranking metrics say which days
  come first; these say whether a 20 % means one day in five.

Ties of the score are broken by a seeded random key, so a constant score
(tasa_base) gives a random choice of alerts and not the first rows.
"""

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


def average_precision(y, score, weight=None) -> float:
    y = np.asarray(y)

    if y.sum() == 0:
        return float("nan")

    return float(average_precision_score(y, score, sample_weight=weight))


def roc_auc(y, score, weight=None) -> float:
    y = np.asarray(y)

    if y.sum() == 0 or y.sum() == len(y):
        return float("nan")

    return float(roc_auc_score(y, score, sample_weight=weight))


def calibration(y, p, weight=None) -> dict:
    """
    How well the probabilities p match the outcomes y (0/1), with weights
    when the rows are a sample or a bootstrap replica:

    - brier: mean of (p - y)^2. brier_referencia: the Brier of saying the
      observed rate every day (its uncertainty term); bss = 1 - brier /
      brier_referencia, the Brier skill score. The reference knows the rate
      of the rows it is measured on, so the skill is a conservative one.
    - log_loss: mean of -log of the probability given to what happened.
    - pendiente / intercepto (Cox 1958): the logistic regression of y on
      logit(p). A slope below 1 means probabilities too extreme (the
      overfitting of a model with few events), above 1 too timid. The
      intercept is the one of y on logit(p) with the slope fixed at 1, the
      calibration in the large: below 0 the model announces more risk than
      there is on average.
    - prob_media: the mean of p, against prevalencia.

    nan when y has no positives or no negatives (and the slope when its
    regression does not converge).
    """
    y = np.asarray(y, dtype=np.float64)
    p = np.clip(np.asarray(p, dtype=np.float64), 1e-6, 1.0 - 1e-6)
    w = np.ones(len(y)) if weight is None else np.asarray(weight, dtype=np.float64)
    total = float(w.sum())
    nan = float("nan")
    out = {"brier": nan, "brier_referencia": nan, "bss": nan, "log_loss": nan, "pendiente": nan,
           "intercepto": nan, "prob_media": nan, "prevalencia": nan}

    if total <= 0:
        return out

    rate = float(np.sum(w * y) / total)
    out["prevalencia"] = rate
    out["prob_media"] = float(np.sum(w * p) / total)
    out["brier"] = float(np.sum(w * (p - y) ** 2) / total)
    out["log_loss"] = float(-np.sum(w * (y * np.log(p) + (1.0 - y) * np.log(1.0 - p))) / total)

    if rate <= 0.0 or rate >= 1.0:
        return out

    out["brier_referencia"] = rate * (1.0 - rate)
    out["bss"] = 1.0 - out["brier"] / out["brier_referencia"]
    logit = np.log(p / (1.0 - p))
    out["intercepto"] = _intercept_with_offset(y, w, logit)
    out["pendiente"] = _recalibration_slope(y, w, logit)
    return out


def _intercept_with_offset(y, w, offset) -> float:
    """The intercept a of y ~ a + offset by maximum likelihood (Newton on sum w (y - p) = 0)."""
    a = 0.0
    target = float(np.sum(w * y))
    for _ in range(100):
        p = 1.0 / (1.0 + np.exp(-(a + offset)))
        delta = float(np.clip((target - np.sum(w * p)) / max(float(np.sum(w * p * (1.0 - p))), 1e-300), -5, 5))
        a += delta
        if abs(delta) < 1e-10:
            break
    return a


def _recalibration_slope(y, w, x) -> float:
    """The slope b of y ~ a + b x by maximum likelihood (Newton with two parameters); nan if it does not converge."""
    if float(np.ptp(x)) < 1e-9:
        return float("nan")
    beta = np.zeros(2)
    X = np.column_stack([np.ones(len(x)), x])
    for _ in range(100):
        p = 1.0 / (1.0 + np.exp(-(X @ beta)))
        hessian = X.T @ (X * (w * p * (1.0 - p))[:, None])
        try:
            step = np.linalg.solve(hessian, X.T @ (w * (y - p)))
        except np.linalg.LinAlgError:
            return float("nan")
        step = np.clip(step, -5.0, 5.0)
        beta += step
        if np.abs(step).max() < 1e-10:
            return float(beta[1]) if abs(beta[1]) < 100 else float("nan")
    return float("nan")


def episode_ids(df: pd.DataFrame, label: str, ct: str = "ct_key", hour: str = "hora") -> pd.Series:
    """
    Id of the episode of every positive row of label (-1 for the negatives).
    A new episode starts when the CT changes or the previous positive hour of
    the same CT is more than one hour before.
    """
    out = pd.Series(-1, index=df.index, dtype="int64")
    pos = df.loc[df[label] == 1, [ct, hour]].sort_values([ct, hour])

    if pos.empty:
        return out

    new_ct = pos[ct].ne(pos[ct].shift())
    gap = pos[hour].diff().ne(pd.Timedelta("1h"))
    out.loc[pos.index] = (new_ct | gap).cumsum().astype("int64") - 1
    return out


def budget_alerts(df: pd.DataFrame, score: str, n: int, seed: int = 0,
                  group=("distribuidora_id", "fecha")) -> np.ndarray:
    """
    Boolean array: the row is one of the n highest scores of its group.
    Works on numpy arrays only: valid has 11 million rows and this runs once
    per model and budget.
    """
    rng = np.random.default_rng(seed)
    tiebreak = rng.random(len(df))
    codes = df.groupby(list(group), sort=False).ngroup().to_numpy()

    order = np.lexsort((tiebreak, -df[score].to_numpy(dtype=np.float64), codes))
    sorted_codes = codes[order]
    starts = np.r_[0, np.flatnonzero(np.diff(sorted_codes)) + 1]
    group_start = np.repeat(starts, np.diff(np.r_[starts, len(order)]))
    rank = np.arange(len(order)) - group_start

    alerts = np.zeros(len(df), dtype=bool)
    alerts[order] = rank < n
    return alerts


def budget_metrics(y, alerts: np.ndarray, episodes, lead=None) -> dict:
    """
    Metrics of a set of alerts. y: label (0/1), episodes: episode id per row
    (-1 for the negatives), lead: horas_hasta_proximo_evento per row.
    """
    y = np.asarray(y) == 1
    ep = np.asarray(episodes)
    n_alerts = int(alerts.sum())
    tp = int((alerts & y).sum())
    n_episodes = int(len(np.unique(ep[ep >= 0])))

    hit = alerts & (ep >= 0)
    detected = np.unique(ep[hit])

    out = {
        "alertas": n_alerts,
        "aciertos": tp,
        "precision": tp / n_alerts if n_alerts else float("nan"),
        "recall_filas": tp / int(y.sum()) if y.sum() else float("nan"),
        "episodios": n_episodes,
        "episodios_detectados": int(len(detected)),
        "recall_episodios": len(detected) / n_episodes if n_episodes else float("nan"),
        "antelacion_media_h": float("nan"),
        "antelacion_mediana_h": float("nan"),
    }

    if lead is not None and len(detected):
        first = pd.DataFrame({"ep": ep[hit], "lead": np.asarray(lead)[hit]}).groupby("ep")["lead"].max()
        out["antelacion_media_h"] = float(first.mean())
        out["antelacion_mediana_h"] = float(first.median())

    return out


def segments(df: pd.DataFrame, columns: dict) -> list:
    """
    [(segmento, valor, boolean mask)]: the whole split and every value of
    every segment column ({segment name: column}).
    """
    out = [("total", "todos", np.ones(len(df), dtype=bool))]

    for name, column in columns.items():
        if column not in df.columns:
            continue

        values = df[column]

        for value in sorted(values.dropna().unique().tolist()):
            label = str(int(value)) if isinstance(value, (float, np.floating)) and float(value).is_integer() else str(value)
            out.append((name, label, (values == value).to_numpy()))

        if values.isna().any():
            out.append((name, "nulo", values.isna().to_numpy()))

    return out


def evaluate(df: pd.DataFrame, scores: list, labels: list, budgets: list, segment_columns: dict,
             seed: int = 0, weight: str = None) -> pd.DataFrame:
    """
    Long table of metrics: one row per (modelo, etiqueta, segmento, valor,
    metrica). df needs distribuidora_id, ct_key, hora, the labels, the score
    columns p_<modelo> and, for the lead time, horas_hasta_proximo_evento.

    Budget metrics are computed once on the whole split (the budget is per
    distribuidora and day) and then restricted to every segment. With a
    weight column (sampled rows) only PR-AUC and ROC-AUC are computed: a
    budget of alerts needs every row of the day.
    """
    df = df.reset_index(drop=True)

    if "fecha" not in df.columns:
        df["fecha"] = df["hora"].dt.floor("D")

    parts = segments(df, segment_columns)
    w_all = df[weight].to_numpy() if weight else None
    episodes = {label: episode_ids(df, label).to_numpy() for label in labels}
    lead_all = df["horas_hasta_proximo_evento"].to_numpy() if "horas_hasta_proximo_evento" in df.columns else None
    rows = []

    def add(modelo, etiqueta, segmento, valor, metrica, value, n, pos):
        rows.append((modelo, etiqueta, segmento, valor, metrica,
                     float(value) if value is not None else float("nan"), int(n), int(pos)))

    for column in scores:
        modelo = column[2:] if column.startswith("p_") else column
        s_all = df[column].to_numpy(dtype=np.float64)
        alerts = {} if weight else {n: budget_alerts(df, column, n, seed) for n in budgets}

        for label in labels:
            y_all = df[label].to_numpy()

            for segmento, valor, mask in parts:
                y = y_all[mask]
                s = s_all[mask]
                w = w_all[mask] if w_all is not None else None
                n = int(mask.sum())
                pos = int(y.sum())
                prevalence = (np.average(y, weights=w) if n else float("nan"))

                ap = average_precision(y, s, w)
                add(modelo, label, segmento, valor, "prevalencia", prevalence, n, pos)
                add(modelo, label, segmento, valor, "pr_auc", ap, n, pos)
                add(modelo, label, segmento, valor, "roc_auc", roc_auc(y, s, w), n, pos)
                add(modelo, label, segmento, valor, "lift_pr_auc",
                    ap / prevalence if prevalence else float("nan"), n, pos)

                for budget, flags in alerts.items():
                    lead = lead_all[mask] if lead_all is not None else None
                    result = budget_metrics(y, flags[mask], episodes[label][mask], lead)

                    for metric, value in result.items():
                        add(modelo, label, segmento, valor, f"{metric}@{budget}", value, n, pos)

    return pd.DataFrame(rows, columns=[
        "modelo", "etiqueta", "segmento", "valor_segmento", "metrica", "valor", "n_filas", "n_positivos",
    ])


def univariate_ap(X: np.ndarray, y, weight=None) -> np.ndarray:
    """
    PR-AUC of every feature used alone as a score, in its best direction
    (missing values at the bottom). A single feature that ranks the
    positives almost perfectly is the typical symptom of leakage.
    """
    out = np.full(X.shape[1], np.nan)

    if np.asarray(y).sum() == 0:
        return out

    for j in range(X.shape[1]):
        x = X[:, j].astype(np.float64)
        missing = np.isnan(x)

        if missing.all():
            continue

        low = np.nanmin(x) - 1.0
        high = np.nanmax(x) + 1.0
        up = np.where(missing, low, x)
        down = np.where(missing, -high, -x)
        out[j] = max(average_precision(y, up, weight), average_precision(y, down, weight))

    return out


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def markdown_table(df: pd.DataFrame, digits: int = 4) -> str:
    """Plain markdown table, without extra dependencies."""
    def cell(value):
        if isinstance(value, (float, np.floating)):
            return "" if np.isnan(value) else f"{value:.{digits}f}"
        return str(value)

    header = "| " + " | ".join(str(c) for c in df.columns) + " |"
    rule = "| " + " | ".join("---" for _ in df.columns) + " |"
    rows = ["| " + " | ".join(cell(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([header, rule, *rows])


def report_markdown(metrics: pd.DataFrame, importance: pd.DataFrame, search: list, label: str,
                    budget: int, local_label: str = None, top: int = 15, profile: pd.DataFrame = None) -> str:
    """
    Short report of a run: the comparison of the models per split, PR-AUC per
    distribuidora, the local events, the features that weigh most in
    XGBoost, the features flagged for leakage and the search.
    """
    parts = []
    columns = ["pr_auc", "lift_pr_auc", "roc_auc", f"precision@{budget}", f"recall_episodios@{budget}",
               f"antelacion_media_h@{budget}"]

    for split in [s for s in ("valid", "test") if s in set(metrics["split"])]:
        total = metrics[(metrics["split"] == split) & (metrics["segmento"] == "total")]
        main = total[total["etiqueta"] == label]

        if main.empty:
            continue

        n, pos = int(main["n_filas"].iloc[0]), int(main["n_positivos"].iloc[0])
        table = main.pivot_table(index="modelo", columns="metrica", values="valor")
        table = table[[c for c in columns if c in table.columns]].sort_values("pr_auc", ascending=False)
        parts.append(f"## {split}: {label} ({n} filas, {pos} positivos)\n\n"
                     + markdown_table(table.reset_index()))

        if local_label:
            local = total[total["etiqueta"] == local_label]
            if not local.empty:
                lt = local.pivot_table(index="modelo", columns="metrica", values="valor")
                keep = [c for c in ["pr_auc", f"recall_episodios@{budget}"] if c in lt.columns]
                parts.append(f"### {split}: solo eventos locales ({local_label})\n\n"
                             + markdown_table(lt[keep].sort_values(keep[0], ascending=False).reset_index()))

        by = metrics[(metrics["split"] == split) & (metrics["etiqueta"] == label)
                     & (metrics["segmento"] == "distribuidora") & (metrics["metrica"] == "pr_auc")]
        if not by.empty:
            dt = by.pivot_table(index="modelo", columns="valor_segmento", values="valor")
            dt.columns = [f"pr_auc dist {c}" for c in dt.columns]
            parts.append(f"### {split}: PR-AUC por distribuidora\n\n" + markdown_table(dt.reset_index()))

    if profile is not None and not profile.empty:
        parts.append("## Grupos de CT (k-means)\n\n" + markdown_table(profile, digits=3))

    by_group = metrics[(metrics["split"] == "valid") & (metrics["etiqueta"] == label)
                       & (metrics["segmento"] == "grupo_ct")
                       & metrics["metrica"].isin(["pr_auc", "lift_pr_auc", f"recall_episodios@{budget}"])]
    if not by_group.empty:
        gt = by_group.pivot_table(index=["valor_segmento", "modelo"], columns="metrica", values="valor")
        counts = by_group.groupby("valor_segmento")[["n_filas", "n_positivos"]].first()
        gt = gt.reset_index().merge(counts.reset_index(), on="valor_segmento")
        gt = gt[gt["modelo"].isin(["tasa_base", "naif_historico", "xgboost", "xgboost_cluster"])]
        parts.append("### valid: por grupo de CT\n\n" + markdown_table(gt.rename(columns={"valor_segmento": "grupo"})))

    group_shap = importance[importance["metrica"].str.startswith("shap_medio_abs_grupo_")]
    if not group_shap.empty:
        lines = []
        for metric, g in group_shap.groupby("metrica"):
            names = g.sort_values("valor", ascending=False).head(5)["feature"].tolist()
            lines.append(f"| {metric.rsplit('_', 1)[-1]} | {', '.join(names)} |")
        parts.append("## Lo que más pesa en el submodelo de cada grupo (SHAP)\n\n| grupo | 5 features con más peso |\n"
                     "| --- | --- |\n" + "\n".join(lines))

    shap = importance[(importance["modelo"] == "xgboost") & (importance["metrica"] == "shap_medio_abs")]
    if not shap.empty:
        top_shap = shap.sort_values("valor", ascending=False).head(top)[["feature", "valor"]]
        parts.append(f"## Las {top} features con más peso en XGBoost (SHAP medio absoluto)\n\n"
                     + markdown_table(top_shap, digits=5))

    flagged = importance[importance["alerta_leakage"]]
    if not flagged.empty:
        parts.append("## Features con alerta de leakage (PR-AUC sola)\n\n"
                     + markdown_table(flagged[["feature", "valor"]].sort_values("valor", ascending=False)))
    else:
        parts.append("## Features con alerta de leakage\n\nNinguna supera el umbral.")

    if search:
        rows = pd.DataFrame([{
            "modelo": s["modelo"],
            "config": s["config"],
            "params": json_compact(s.get("params", {})),
            "pr_auc_valid_muestra": s.get("pr_auc_valid_muestra", float("nan")),
            "segundos": s.get("segundos_entrenamiento", ""),
        } for s in search])
        parts.append("## Búsqueda (muestra de valid)\n\n" + markdown_table(rows, digits=5))

    return "\n\n".join(parts) + "\n"


def json_compact(params: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in params.items() if k != "verbose")


# ---------------------------------------------------------------------------
# Uncertainty
# ---------------------------------------------------------------------------

def day_blocks(day, block_days: int = 1) -> np.ndarray:
    """
    The block of every row for the bootstrap: its day, or the run of
    block_days consecutive days it falls in (counted from the first day).
    A storm of two or three days stays in one block of a week, so the
    replicas do not split it.
    """
    day = np.asarray(day)

    if block_days <= 1:
        return day

    days = pd.to_datetime(pd.Series(day)).dt.normalize()
    return ((days - days.min()).dt.days // int(block_days)).to_numpy()


def bootstrap_pr_auc(y, weight, day, scores: dict, reps: int = 500, seed: int = 0, block_days: int = 1) -> tuple:
    """
    Block bootstrap of the PR-AUC by days: every replica draws the days of
    the split with replacement and gives every row the weight of its row
    times the times its day was drawn. Interruptions come in storms that last
    hours, so resampling rows would understate the uncertainty; days keep a
    storm together. With block_days > 1 the blocks are runs of that many
    consecutive days (day_blocks), for storms that last more than a day.

    scores: {name: score array}. The same replicas are used for every score,
    so the difference between two scores is paired.
    Returns (DataFrame name, pr_auc, p2_5, p50, p97_5; replicas as {name: array}).
    """
    y = np.asarray(y)
    weight = np.asarray(weight, dtype=np.float64)
    codes, day_index = np.unique(day_blocks(day, block_days), return_inverse=True)
    n_days = len(codes)
    rng = np.random.default_rng(seed)
    replicas = {name: np.empty(reps) for name in scores}

    for r in range(reps):
        counts = np.bincount(rng.integers(0, n_days, n_days), minlength=n_days)
        w = weight * counts[day_index]
        keep = w > 0

        for name, score in scores.items():
            replicas[name][r] = average_precision(y[keep], np.asarray(score)[keep], w[keep])

    rows = []
    for name, score in scores.items():
        values = replicas[name]
        rows.append({
            "nombre": name,
            "pr_auc": average_precision(y, score, weight),
            "p2_5": float(np.nanpercentile(values, 2.5)),
            "p50": float(np.nanpercentile(values, 50)),
            "p97_5": float(np.nanpercentile(values, 97.5)),
        })

    return pd.DataFrame(rows), replicas


def paired_differences(replicas: dict, pairs: list) -> pd.DataFrame:
    """
    For every (a, b): the bootstrap interval of PR-AUC(a) - PR-AUC(b) and the
    share of replicas where a beats b. An interval that crosses zero means
    the data do not tell the two apart.
    """
    rows = []

    for a, b in pairs:
        diff = replicas[a] - replicas[b]
        rows.append({
            "a": a,
            "b": b,
            "dif_p2_5": float(np.nanpercentile(diff, 2.5)),
            "dif_p50": float(np.nanpercentile(diff, 50)),
            "dif_p97_5": float(np.nanpercentile(diff, 97.5)),
            "prob_a_mejor": float(np.nanmean(diff > 0)),
        })

    return pd.DataFrame(rows)
