"""
Prueba de la humedad del suelo, la duracion del viento y el factor de racha
como variables del modelo del dia (la hipotesis de Lorenz et al., NHESS 2025:
los arboles caen mas con el suelo empapado y con el viento que dura).

Entradas:
- dias.csv del replay de job_ml_caso_negocio.py (modo reanalisis, un dia por
  distribuidora con la probabilidad de la vispera y las incidencias reales).
- la serie horaria de bajar_suelo.py.

    python experimentos/suelo_humedad/analizar.py dias.csv suelo_viento_horario.csv [salida.md]

Que hace: agrega la serie horaria a un dia por red (pico de racha de la red,
horas por encima del percentil 90 local del punto, factor de racha en el pico,
lluvia, humedad del suelo y su anomalia frente a la climatologia del dia del
año, lluvia de 7 y 30 dias), une cada dia con la probabilidad del replay y
mide si alguna variable nueva añade algo a esa probabilidad: coeficiente y
p-valor en una logistica (logit de la probabilidad + candidata), AUC y Brier
por años rodantes (entrena con los años anteriores, puntua el siguiente)
frente a la probabilidad sola, y las tablas de contraste (dias de mas viento
con suelo humedo frente a seco; incidencias por horas de viento fuerte).
"""

import sys
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

CANDIDATES = ["suelo_0_7", "suelo_7_28", "suelo_anom", "horas_p90", "factor_racha", "lluvia_7d", "lluvia_30d"]


def daily(hourly: pd.DataFrame) -> pd.DataFrame:
    h = hourly.copy()
    h["hora"] = pd.to_datetime(h["hora"])
    h["fecha"] = h["hora"].dt.strftime("%Y-%m-%d")
    h["p90"] = h.groupby(["dist", "punto"])["racha"].transform(lambda s: s.quantile(0.90))
    h["fuerte"] = (h["racha"] > h["p90"]).astype(int)
    h["factor"] = h["racha"] / h["viento"].clip(lower=1.0)
    per_point = h.groupby(["dist", "punto", "fecha"]).agg(
        racha_max=("racha", "max"), horas_p90=("fuerte", "sum"), lluvia=("lluvia", "sum"),
        suelo_0_7=("suelo_0_7", "mean"), suelo_7_28=("suelo_7_28", "mean"), temp_max=("temp", "max"),
    ).reset_index()
    peak = h.loc[h.groupby(["dist", "punto", "fecha"])["racha"].idxmax(), ["dist", "punto", "fecha", "factor"]]
    per_point = per_point.merge(peak, on=["dist", "punto", "fecha"])
    # The day of the network: the windiest point gives the gust, the hours and the factor; the rest is the mean.
    idx = per_point.groupby(["dist", "fecha"])["racha_max"].idxmax()
    windiest = per_point.loc[idx, ["dist", "fecha", "racha_max", "horas_p90", "factor"]].rename(columns={"factor": "factor_racha"})
    means = per_point.groupby(["dist", "fecha"]).agg(lluvia=("lluvia", "mean"), suelo_0_7=("suelo_0_7", "mean"),
                                                     suelo_7_28=("suelo_7_28", "mean"), temp_max=("temp_max", "max")).reset_index()
    d = windiest.merge(means, on=["dist", "fecha"]).sort_values(["dist", "fecha"]).reset_index(drop=True)
    d["doy"] = pd.to_datetime(d["fecha"]).dt.dayofyear
    # Climatology of the soil by day of the year (+-15 days, all years) and the anomaly of the day.
    clim = d.groupby(["dist", "doy"])["suelo_0_7"].mean().reset_index().rename(columns={"suelo_0_7": "clim"})
    clim["clim"] = clim.groupby("dist")["clim"].transform(lambda s: s.rolling(31, center=True, min_periods=1).mean())
    d = d.merge(clim, on=["dist", "doy"], how="left")
    d["suelo_anom"] = d["suelo_0_7"] - d["clim"]
    d["lluvia_7d"] = d.groupby("dist")["lluvia"].transform(lambda s: s.rolling(7, min_periods=7).sum().shift(1))
    d["lluvia_30d"] = d.groupby("dist")["lluvia"].transform(lambda s: s.rolling(30, min_periods=30).sum().shift(1))
    return d.drop(columns=["clim"])


def auc(y: np.ndarray, p: np.ndarray) -> float:
    order = np.argsort(p)
    ranks = np.empty(len(p))
    ranks[order] = np.arange(1, len(p) + 1)
    # Ties get the mean rank.
    s = pd.Series(p).rank().to_numpy()
    pos = y == 1
    if pos.sum() == 0 or (~pos).sum() == 0:
        return float("nan")
    return (s[pos].sum() - pos.sum() * (pos.sum() + 1) / 2) / (pos.sum() * (~pos).sum())


def fit_logit(x: np.ndarray, y: np.ndarray, iterations: int = 200, ridge: float = 1e-4):
    """Logistic regression by Newton-Raphson with a light ridge; returns the coefficients (with intercept)."""
    X = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(X.shape[1])
    for _ in range(iterations):
        p = 1.0 / (1.0 + np.exp(-X @ beta))
        gradient = X.T @ (y - p) - ridge * beta
        hessian = (X * (p * (1 - p))[:, None]).T @ X + ridge * np.eye(X.shape[1])
        step = np.linalg.solve(hessian, gradient)
        beta = beta + step
        if np.abs(step).max() < 1e-8:
            break
    p = 1.0 / (1.0 + np.exp(-X @ beta))
    cov = np.linalg.inv((X * (p * (1 - p))[:, None]).T @ X + ridge * np.eye(X.shape[1]))
    return beta, np.sqrt(np.diag(cov))


def z_to_p(z: float) -> float:
    from math import erf, sqrt
    return 2 * (1 - 0.5 * (1 + erf(abs(z) / sqrt(2))))


def fisher(a: int, b: int, c: int, d: int) -> float:
    """Two-sided Fisher exact test of the table [[a, b], [c, d]] (sum of the tables as or less likely)."""
    from math import comb
    n, row1, col1 = a + b + c + d, a + b, a + c
    total = comb(n, col1)
    probability = lambda k: comb(row1, k) * comb(n - row1, col1 - k) / total  # noqa: E731
    observed = probability(a)
    return min(1.0, sum(probability(k) for k in range(max(0, col1 - (n - row1)), min(row1, col1) + 1)
                        if probability(k) <= observed * (1 + 1e-9)))


def rolling(days: pd.DataFrame, columns: list) -> tuple:
    """Train with the years before each year from the second one, score that year; pooled AUC and Brier."""
    years = sorted(days["anio"].unique())
    scored = []
    for year in years[1:]:
        train = days[days["anio"] < year]
        test = days[days["anio"] == year]
        if train["y"].sum() < 5 or test.empty:
            continue
        beta, _ = fit_logit(train[columns].to_numpy(float), train["y"].to_numpy(float))
        X = np.column_stack([np.ones(len(test)), test[columns].to_numpy(float)])
        scored.append(pd.DataFrame({"y": test["y"].to_numpy(), "p": 1.0 / (1.0 + np.exp(-X @ beta))}))
    s = pd.concat(scored, ignore_index=True)
    return auc(s["y"].to_numpy(), s["p"].to_numpy()), float(np.mean((s["p"] - s["y"]) ** 2)), len(s)


def main(argv):
    days = pd.read_csv(argv[0])
    hourly = pd.read_csv(argv[1])
    out = argv[2] if len(argv) > 2 else None
    days = days[days["modo"] == "reanalisis"].copy()
    days["dist"] = days["distribuidora_id"].astype(int)
    d = daily(hourly)
    d["dist"] = d["dist"].astype(int)
    m = days.merge(d, on=["dist", "fecha"], how="inner")
    m = m.dropna(subset=CANDIDATES).copy()
    m["y"] = (m["incidencias"] > 0).astype(int)
    p = m["probabilidad"].clip(1e-4, 1 - 1e-4)
    m["logit_p"] = np.log(p / (1 - p))
    for c in CANDIDATES:
        m[c + "_z"] = (m[c] - m[c].mean()) / m[c].std()

    lines = [f"Días: {len(m)} ({m['fecha'].min()} - {m['fecha'].max()}), con incidencia: {int(m['y'].sum())} "
             f"({m['y'].mean():.1%}); distribuidoras {sorted(m['dist'].unique())}; años {sorted(m['anio'].unique())}", ""]
    base_auc, base_brier, n = rolling(m, ["logit_p"])
    lines += ["| variable añadida a la probabilidad de la víspera | coeficiente (por desviación típica) | p-valor | AUC rodante | Brier rodante |",
              "| --- | --- | --- | --- | --- |",
              f"| ninguna (la probabilidad sola) | - | - | {base_auc:.3f} | {base_brier:.4f} |"]
    for c in CANDIDATES:
        beta, se = fit_logit(m[["logit_p", c + "_z"]].to_numpy(float), m["y"].to_numpy(float))
        pv = z_to_p(beta[2] / se[2])
        a, b, _ = rolling(m, ["logit_p", c + "_z"])
        lines.append(f"| {c} | {beta[2]:+.3f} | {pv:.3f} | {a:.3f} | {b:.4f} |")
    beta, se = fit_logit(m[["logit_p", "horas_p90_z", "suelo_anom_z"]].assign(
        inter=m["horas_p90_z"] * m["suelo_anom_z"]).to_numpy(float), m["y"].to_numpy(float))
    a, b, _ = rolling(m.assign(inter=m["horas_p90_z"] * m["suelo_anom_z"]), ["logit_p", "horas_p90_z", "suelo_anom_z", "inter"])
    lines.append(f"| horas_p90 x suelo_anom (interacción) | {beta[4]:+.3f} | {z_to_p(beta[4] / se[4]):.3f} | {a:.3f} | {b:.4f} |")
    lines.append(f"\nAUC y Brier rodantes sobre {n} días puntuados (cada año con el modelo ajustado a los anteriores).\n")

    windy = m[m["racha_max"] >= m["racha_max"].quantile(0.95)]
    wet, dry = windy[windy["suelo_anom"] > 0], windy[windy["suelo_anom"] <= 0]
    lines += ["Los días de más viento (5 % de días con mayor racha), según el suelo esté más húmedo o más seco que lo normal en esa fecha:", "",
              "| suelo | días | con incidencia | parte |", "| --- | --- | --- | --- |",
              f"| más húmedo de lo normal | {len(wet)} | {int(wet['y'].sum())} | {wet['y'].mean():.1%} |",
              f"| más seco de lo normal | {len(dry)} | {int(dry['y'].sum())} | {dry['y'].mean():.1%} |", "",
              f"Test exacto de Fisher de esa tabla: p = {fisher(int(wet['y'].sum()), len(wet) - int(wet['y'].sum()), int(dry['y'].sum()), len(dry) - int(dry['y'].sum())):.2f}.", ""]
    bins = pd.cut(m["horas_p90"], [-1, 0, 3, 8, 24], labels=["0", "1 - 3", "4 - 8", "más de 8"])
    g = m.groupby(bins, observed=False)["y"].agg(["size", "sum", "mean"])
    lines += ["Incidencias según las horas del día con racha por encima del percentil 90 local:", "",
              "| horas de viento fuerte | días | con incidencia | parte |", "| --- | --- | --- | --- |"]
    for label, row in g.iterrows():
        lines.append(f"| {label} | {int(row['size'])} | {int(row['sum'])} | {row['mean']:.1%} |")
    text = "\n".join(lines) + "\n"
    print(text)
    if out:
        with open(out, "w", encoding="utf-8") as file:
            file.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
