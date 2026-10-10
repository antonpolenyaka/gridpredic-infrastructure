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
