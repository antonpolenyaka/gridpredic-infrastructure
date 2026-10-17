"""
Calibración y valor económico de la probabilidad de la víspera, con la tabla
de días que deja job_ml_caso_negocio (dias.csv del caso de negocio):

    python calibracion_valor.py dias.csv salida/ [--reps 1000]

Responde a tres preguntas que la PR-AUC no contesta:

1. Calibración: si un 10 % de la víspera es de verdad un día de cada diez.
   Brier y su mejora frente a decir cada día la tasa observada (BSS), log
   loss, pendiente e intercepto de la recalibración logística (Cox 1958) y el
   diagrama de fiabilidad por tramos de probabilidad y por niveles, con
   intervalos de Wilson.
2. Valor relativo (Richardson 2000; Murphy 1977): para una distribuidora que
   gasta C en reforzar la guardia un día y pierde L si la avería la pilla sin
   reforzar, cuánto de lo que ahorraría una previsión perfecta ahorra la
   nuestra, según el cociente alfa = C / L. Con las averías contadas por días
   y con la pérdida proporcional a las horas-cliente de cada día, que es lo
   que pesa en el TIEPI. Se miden las reglas de los niveles (alerta = alto o
   muy alto, solo muy alto, medio o más) y la regla probabilística (reforzar
   si la probabilidad pasa de alfa), que es la óptima si está calibrada.
3. Beneficio neto (Vickers y Elkin 2006): la misma decisión en la escala de
   los días con avería bien atendidos, frente a reforzar siempre y nunca.

Todo con y sin el temporal del 28.01.2026, que pesa mucho en las
horas-cliente, y con intervalos bootstrap por fechas y por semanas (las
réplicas toman días enteros, o bloques de siete días seguidos, de las tres
redes a la vez). Los nombres de las distribuidoras no salen nunca: la tabla
trae sus ids y aquí pasan a Distribuidora 1, 2 y 3.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "etl", "jobs", "04_ml"))
import ml_metrics  # noqa: E402

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
STORM = pd.Timestamp("2026-01-28")
ALPHAS = np.round(np.arange(0.01, 0.51, 0.01), 2)
THRESHOLDS = np.round(np.arange(0.01, 0.31, 0.01), 2)
# Las reglas de decisión de los niveles: qué días se refuerza la guardia.
RULES = {"alerta": ("alto", "muy alto"), "muy_alto": ("muy alto",), "medio_o_mas": ("medio", "alto", "muy alto")}
BINS = [0.0, 0.02, 0.04, 0.06, 0.10, 0.15, 0.25, 1.0]
LEVELS = ["bajo", "medio", "alto", "muy alto"]
# Los alfas de las tablas: una guardia reforzada que cuesta el 5, el 10 o el 20 % de lo que se pierde en un día con
# avería sin ella.
ALPHAS_SHOWN = (0.05, 0.1, 0.2)
THRESHOLDS_SHOWN = (0.05, 0.1)


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["fecha"])
    df = df.drop(columns=[c for c in ("distribuidora", "bases") if c in df.columns])
    df["red"] = df["distribuidora_id"].map(NETWORKS)
    if df["red"].isna().any():
        raise ValueError(f"Distribuidoras sin nombre de la memoria: {sorted(df.loc[df['red'].isna(), 'distribuidora_id'].unique())}")
    df["y"] = (df["incidencias"].fillna(0) > 0).astype(int)
    df["w"] = df["clientes_hora"].fillna(0.0).clip(lower=0.0)
    return df.sort_values(["modo", "fecha", "red"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Medidas sobre filas con peso (el peso de cada fila es cuántas veces sale en la réplica)
# ---------------------------------------------------------------------------

def relative_value(y, loss, act, alpha: float, rw=None) -> float:
    """
    Valor relativo de Richardson de la decisión act (1 = reforzar) para el
    cociente alpha. loss es la pérdida de cada día sin reforzar en unidades
    de L (1 en los días con avería si se cuentan días; las horas-cliente del
    día entre la media de los días con avería si se pesan). La previsión
    perfecta refuerza solo cuando la pérdida evitada pasa del coste.
    """
    rw = np.ones(len(y)) if rw is None else rw
    total = rw.sum()
    climate = min(alpha, float(np.sum(rw * loss)) / total)
    forecast = float(np.sum(rw * (act * alpha + (1 - act) * loss))) / total
    perfect = float(np.sum(rw * np.minimum(loss, alpha) * (y > 0))) / total
    if climate - perfect <= 1e-12:
        return float("nan")
    return (climate - forecast) / (climate - perfect)


def net_benefit(y, act, threshold: float, rw=None) -> float:
    """Beneficio neto de Vickers: verdaderos positivos menos falsos positivos pesados por pt / (1 - pt), por día."""
    rw = np.ones(len(y)) if rw is None else rw
    total = rw.sum()
    tp = float(np.sum(rw * act * y)) / total
    fp = float(np.sum(rw * act * (1 - y))) / total
    return tp - fp * threshold / (1 - threshold)


def losses(df: pd.DataFrame) -> dict:
    """La pérdida de cada día sin reforzar: por días (1 si hay avería) y por horas-cliente (relativas a la media)."""
    y = df["y"].to_numpy()
    w = df["w"].to_numpy()
    mean = w[y > 0].mean() if (y > 0).any() and w[y > 0].mean() > 0 else 1.0
    return {"dias": y.astype(float), "horas_cliente": np.where(y > 0, w / mean, 0.0)}


def decisions(df: pd.DataFrame) -> dict:
    return {name: df["nivel"].isin(levels).to_numpy().astype(float) for name, levels in RULES.items()}


def block_weights(df: pd.DataFrame, block_days: int, rng) -> np.ndarray:
    """El peso de cada fila en una réplica: cuántas veces sale su fecha (o su bloque de días seguidos)."""
    blocks = ml_metrics.day_blocks(df["fecha"].to_numpy(), block_days)
    codes, index = np.unique(blocks, return_inverse=True)
    counts = np.bincount(rng.integers(0, len(codes), len(codes)), minlength=len(codes))
    return counts[index].astype(float)


# ---------------------------------------------------------------------------
# Tablas
# ---------------------------------------------------------------------------

def scopes(df: pd.DataFrame) -> list:
    """Las tres redes juntas (lo que vive la guardia), la Distribuidora 1 y la 2 (la 3 casi nunca avería)."""
    return [("Las tres redes", df), ("Distribuidora 1", df[df["red"] == "Distribuidora 1"]),
            ("Distribuidora 2", df[df["red"] == "Distribuidora 2"])]


def interval(values) -> tuple:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if not len(values):
        return float("nan"), float("nan")
    return float(np.percentile(values, 2.5)), float(np.percentile(values, 97.5))


def calibration_rows(df: pd.DataFrame, label: str, reps: int, seed: int) -> list:
    rows = []
    for scope, part in scopes(df):
        if part.empty:
            continue
        y, p = part["y"].to_numpy(), part["probabilidad"].to_numpy()
        found = ml_metrics.calibration(y, p)
        row = {"muestra": label, "redes": scope, "dias": len(part), "dias_con_averia": int(y.sum()),
               "tasa": found["prevalencia"], "prob_media": found["prob_media"], "brier": found["brier"],
               "bss": found["bss"], "log_loss": found["log_loss"], "pendiente": found["pendiente"],
               "intercepto": found["intercepto"]}
        for block in (1, 7):
            rng = np.random.default_rng(seed + block)
            draws = {"bss": [], "pendiente": [], "intercepto": []}
            for _ in range(reps):
                rw = block_weights(part, block, rng)
                keep = rw > 0
                measured = ml_metrics.calibration(y[keep], p[keep], rw[keep])
                for key in draws:
                    draws[key].append(measured[key])
            for key, values in draws.items():
                low, high = interval(values)
                row[f"{key}_ic{block}d"] = f"{es(low, 3)} a {es(high, 3)}"
        rows.append(row)
    return rows


def reliability_rows(df: pd.DataFrame, label: str) -> list:
    rows = []
    for scope, part in scopes(df)[:1]:
        groups = pd.cut(part["probabilidad"], BINS, include_lowest=True)
        for interval_, g in part.groupby(groups, observed=True):
            low, high = ml_metrics_wilson(int(g["y"].sum()), len(g))
            rows.append({"muestra": label, "redes": scope,
                         "tramo": f"{es(max(float(interval_.left), 0.0))} a {es(float(interval_.right))}",
                         "dias": len(g), "dias_con_averia": int(g["y"].sum()), "prob_media": g["probabilidad"].mean(),
                         "tasa": g["y"].mean(), "tasa_ic_bajo": low, "tasa_ic_alto": high})
        for level in LEVELS:
            g = part[part["nivel"] == level]
            if g.empty:
                continue
            low, high = ml_metrics_wilson(int(g["y"].sum()), len(g))
            rows.append({"muestra": label, "redes": scope, "tramo": f"nivel {level}", "dias": len(g),
                         "dias_con_averia": int(g["y"].sum()), "prob_media": g["probabilidad"].mean(),
                         "tasa": g["y"].mean(), "tasa_ic_bajo": low, "tasa_ic_alto": high,
                         "parte_horas_cliente": g["w"].sum() / part["w"].sum() if part["w"].sum() else float("nan")})
    return rows


def ml_metrics_wilson(k: int, n: int, z: float = 1.96) -> tuple:
    """Intervalo de Wilson de una proporción k / n."""
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return max(0.0, centre - half), min(1.0, centre + half)


def value_rows(df: pd.DataFrame, label: str) -> list:
    """Valor relativo de cada regla y de la regla probabilística para cada alfa, contado por días y por horas-cliente."""
    rows = []
    y, p = df["y"].to_numpy(), df["probabilidad"].to_numpy()
    rules = decisions(df)
    for weighting, loss in losses(df).items():
        for alpha in ALPHAS:
            row = {"muestra": label, "perdida": weighting, "alfa": alpha}
            for name, act in rules.items():
                row[name] = relative_value(y, loss, act, alpha)
            row["probabilidad"] = relative_value(y, loss, (p > alpha).astype(float), alpha)
            rows.append(row)
    return rows


def benefit_rows(df: pd.DataFrame, label: str) -> list:
    rows = []
    y, p = df["y"].to_numpy(), df["probabilidad"].to_numpy()
    rules = decisions(df)
    rate = y.mean()
    for threshold in THRESHOLDS:
        row = {"muestra": label, "umbral": threshold, "modelo": net_benefit(y, (p >= threshold).astype(float), threshold),
               "reforzar_siempre": rate - (1 - rate) * threshold / (1 - threshold), "nunca": 0.0}
        for name, act in rules.items():
            row[name] = net_benefit(y, act, threshold)
        rows.append(row)
    return rows


def value_intervals(df: pd.DataFrame, label: str, reps: int, seed: int) -> list:
    """Intervalos bootstrap del valor relativo (alfas de ALPHAS_SHOWN) y del beneficio neto (umbrales de THRESHOLDS_SHOWN)."""
    y, p = df["y"].to_numpy(), df["probabilidad"].to_numpy()
    rules = {**decisions(df)}
    loss_of = losses(df)
    rows = []
    for block in (1, 7):
        rng = np.random.default_rng(seed + 10 * block)
        draws = {}
        for _ in range(reps):
            rw = block_weights(df, block, rng)
            for weighting, loss in loss_of.items():
                for alpha in ALPHAS_SHOWN:
                    for name, act in {**rules, "probabilidad": (p > alpha).astype(float)}.items():
                        draws.setdefault(("valor", weighting, alpha, name), []).append(
                            relative_value(y, loss, act, alpha, rw))
            for threshold in THRESHOLDS_SHOWN:
                for name, act in {**rules, "modelo": (p >= threshold).astype(float)}.items():
                    draws.setdefault(("beneficio_neto", "dias", threshold, name), []).append(
                        net_benefit(y, act, threshold, rw))
        for (kind, weighting, level, name), values in draws.items():
            low, high = interval(values)
            if kind == "valor":
                point = relative_value(y, loss_of[weighting], (p > level).astype(float) if name == "probabilidad"
                                       else rules[name], level)
            else:
                point = net_benefit(y, (p >= level).astype(float) if name == "modelo" else rules[name], level)
            rows.append({"muestra": label, "medida": kind, "perdida": weighting, "alfa_o_umbral": level,
                         "regla": name, "bloque_dias": block, "valor": point, "ic_bajo": low, "ic_alto": high,
                         "prob_positivo": float(np.mean(np.asarray(values)[np.isfinite(values)] > 0))
                         if np.isfinite(values).any() else float("nan")})
    return rows


# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

def figures(reliability: pd.DataFrame, value: pd.DataFrame, benefit: pd.DataFrame, out: str) -> list:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, muted, grid, accent, grey, light = "#1b2430", "#5d6878", "#e3e7ed", "#2457c5", "#9aa5b1", "#c9d6f2"
    plt.rcParams.update({"font.family": ["Liberation Sans", "DejaVu Sans"], "font.size": 8.5, "axes.labelsize": 8.5,
                         "xtick.labelsize": 8, "ytick.labelsize": 8, "axes.edgecolor": grid, "axes.labelcolor": ink,
                         "xtick.color": muted, "ytick.color": ink, "savefig.dpi": 300, "axes.unicode_minus": False})

    def comma(ax, axis="both", digits=2, percent=False):
        from matplotlib.ticker import FuncFormatter

        def fmt(v, _):
            text = f"{100 * v:.0f} %" if percent else f"{v:.{digits}f}"
            return text.replace(".", ",")
        if axis in ("x", "both"):
            ax.xaxis.set_major_formatter(FuncFormatter(fmt))
        if axis in ("y", "both"):
            ax.yaxis.set_major_formatter(FuncFormatter(fmt))

    def tidy(ax):
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.grid(color=grid, lw=0.6, zorder=0)

    paths = []
    # 1. Fiabilidad: tramos de probabilidad y niveles, con la previsión real.
    fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.9))
    part = reliability[(reliability["muestra"] == "prevision") & ~reliability["tramo"].str.startswith("nivel")]
    levels = reliability[(reliability["muestra"] == "prevision") & reliability["tramo"].str.startswith("nivel")]
    for ax, data, heading in ((axes[0], part, "Por tramos de probabilidad"), (axes[1], levels, "Por niveles")):
        top = max(0.3, float(data["tasa_ic_alto"].max()) * 1.05)
        ax.plot([0, top], [0, top], color=grey, lw=0.9, ls=(0, (3, 3)), zorder=1)
        ax.errorbar(data["prob_media"], data["tasa"], yerr=[data["tasa"] - data["tasa_ic_bajo"],
                                                              data["tasa_ic_alto"] - data["tasa"]],
                    fmt="o", color=accent, ecolor=light, elinewidth=3, capsize=0, ms=4.5, zorder=3)
        crowded = []
        for _, r in data.iterrows():
            if r["tramo"].startswith("nivel"):
                text = r["tramo"].replace("nivel ", "")
            elif r["prob_media"] < 0.04:
                # The lowest bins sit almost on top of each other: their days go in a note of the panel.
                crowded.append(f"{int(r['dias']):,}".replace(",", "."))
                continue
            else:
                text = f"{int(r['dias'])} días"
            ax.annotate(text, (r["prob_media"], r["tasa"]), xytext=(5, -3), textcoords="offset points", fontsize=6.8,
                        color=muted)
        if crowded:
            ax.text(0.03, 0.97, f"Tramos por debajo del 4 %:\n{' y '.join(crowded)} días", transform=ax.transAxes,
                    ha="left", va="top", fontsize=6.8, color=muted)
        ax.set_xlim(0, top)
        ax.set_ylim(0, top)
        ax.set_xlabel("Probabilidad media de la víspera")
        ax.set_title(heading, fontsize=8.5, color=ink, loc="left")
        comma(ax, percent=True)
        tidy(ax)
    axes[0].set_ylabel("Días con avería (intervalo del 95 %)")
    fig.text(0.01, 0.99, "Lo que dice la víspera y lo que pasó", ha="left", va="top", fontsize=10, fontweight="bold",
             color=ink)
    fig.text(0.01, 0.925, "Previsión real de ICON-EU, 20.01.2024 a 02.08.2026, las tres redes. En la diagonal, un 10 % "
             "es un día de cada diez.", ha="left", va="top", fontsize=7.5, color=muted)
    fig.subplots_adjust(left=0.1, right=0.98, top=0.78, bottom=0.17, wspace=0.28)
    path = os.path.join(out, "fiabilidad.png")
    fig.savefig(path)
    plt.close(fig)
    paths.append(path)

    # 2. Valor relativo según el cociente coste / pérdida.
    names = {"probabilidad": ("Reforzar si la probabilidad pasa de alfa", accent, "-"),
             "alerta": ("Reforzar en alerta (alto o muy alto)", ink, "-"),
             "muy_alto": ("Reforzar solo en muy alto", ink, (0, (4, 2))),
             "medio_o_mas": ("Reforzar desde medio", grey, "-")}
    fig, axes = plt.subplots(1, 2, figsize=(6.3, 3.1), sharey=True)
    for ax, weighting, heading in ((axes[0], "dias", "Pérdida igual en cada día con avería"),
                                   (axes[1], "horas_cliente", "Pérdida según las horas-cliente del día")):
        for sample, alpha_line in (("prevision", 1.0), ("prevision_sin_28_01", 0.45)):
            data = value[(value["muestra"] == sample) & (value["perdida"] == weighting)]
            for column, (label, colour, style) in names.items():
                ax.plot(data["alfa"], data[column].clip(lower=-0.5), color=colour, ls=style, lw=1.3 if sample == "prevision" else 1.0,
                        alpha=alpha_line, label=label if sample == "prevision" else None)
        ax.axhline(0, color=muted, lw=0.8)
        ax.axvspan(0.05, 0.3, color=grid, alpha=0.5, zorder=0)
        ax.set_xlim(0, 0.5)
        ax.set_ylim(-0.5, 1)
        ax.set_xlabel("Coste de reforzar / pérdida de un día con avería")
        ax.set_title(heading, fontsize=8.5, color=ink, loc="left")
        comma(ax, digits=1)
        tidy(ax)
    axes[0].set_ylabel("Valor relativo (1 = previsión perfecta)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower left", bbox_to_anchor=(0.01, 0.0), ncol=2, fontsize=7, frameon=False)
    fig.text(0.01, 0.99, "Cuánto ahorra reforzar la guardia con la víspera", ha="left", va="top", fontsize=10,
             fontweight="bold", color=ink)
    fig.text(0.01, 0.93, "Previsión real; en trazo claro, sin el temporal del 28.01.2026. Banda gris: los cocientes "
             "verosímiles, de 0,05 a 0,3.", ha="left", va="top", fontsize=7.5, color=muted)
    fig.subplots_adjust(left=0.1, right=0.98, top=0.8, bottom=0.3, wspace=0.08)
    path = os.path.join(out, "valor_relativo.png")
    fig.savefig(path)
    plt.close(fig)
    paths.append(path)

    # 3. Beneficio neto.
    fig, ax = plt.subplots(figsize=(6.3, 2.9))
    data = benefit[benefit["muestra"] == "prevision"]
    ax.plot(data["umbral"], data["modelo"], color=accent, lw=1.4, label="Reforzar si la probabilidad pasa del umbral")
    ax.plot(data["umbral"], data["alerta"], color=ink, lw=1.2, label="Reforzar en alerta")
    ax.plot(data["umbral"], data["muy_alto"], color=ink, lw=1.0, ls=(0, (4, 2)), label="Reforzar solo en muy alto")
    ax.plot(data["umbral"], data["reforzar_siempre"], color=grey, lw=1.0, label="Reforzar siempre")
    ax.axhline(0, color=muted, lw=0.8, label="No reforzar nunca")
    ax.set_ylim(-0.01, max(0.03, float(data[["modelo", "alerta", "muy_alto"]].max().max()) * 1.2))
    ax.set_xlim(0.01, 0.3)
    ax.set_xlabel("Umbral de probabilidad a partir del que compensa reforzar")
    ax.set_ylabel("Beneficio neto (días con avería por día)")
    comma(ax, axis="x", percent=True)
    comma(ax, axis="y", digits=3)
    tidy(ax)
    ax.legend(fontsize=7, frameon=False, loc="upper right")
    fig.text(0.01, 0.99, "Beneficio neto de la decisión de cada tarde", ha="left", va="top", fontsize=10,
             fontweight="bold", color=ink)
    fig.text(0.01, 0.925, "Previsión real, las tres redes. Por encima de cero y de reforzar siempre, la víspera ayuda.",
             ha="left", va="top", fontsize=7.5, color=muted)
    fig.subplots_adjust(left=0.11, right=0.98, top=0.8, bottom=0.17)
    path = os.path.join(out, "beneficio_neto.png")
    fig.savefig(path)
    plt.close(fig)
    paths.append(path)
    return paths


# ---------------------------------------------------------------------------
# Informe
# ---------------------------------------------------------------------------

def es(value, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return "-"
    return f"{value:.{digits}f}".replace(".", ",")


def table(df: pd.DataFrame, columns: list, digits: dict = None) -> str:
    digits = digits or {}
    lines = ["| " + " | ".join(columns) + " |", "|" + "|".join("---" for _ in columns) + "|"]
    for _, r in df.iterrows():
        cells = []
        for c in columns:
            v = r[c]
            cells.append(es(v, digits.get(c, 3)) if isinstance(v, (float, np.floating)) else str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def report(calibration: pd.DataFrame, reliability: pd.DataFrame, value: pd.DataFrame, benefit: pd.DataFrame,
           intervals: pd.DataFrame, reps: int) -> str:
    out = ["# Calibración y valor de la probabilidad de la víspera", "",
           f"Tabla de días del caso de negocio (dias.csv). Intervalos del 95 % con {reps} réplicas bootstrap por fechas "
           "(1d) y por bloques de siete días (7d). La muestra prevision es la previsión real de ICON-EU; reanalisis, el "
           "tiempo del día sacado del reanálisis (una previsión perfecta).", "",
           "## Calibración", "",
           table(calibration, ["muestra", "redes", "dias", "dias_con_averia", "tasa", "prob_media", "brier", "bss",
                               "bss_ic1d", "bss_ic7d", "pendiente", "pendiente_ic1d", "pendiente_ic7d", "intercepto",
                               "intercepto_ic7d"]), "",
           "## Fiabilidad por tramos y por niveles (previsión real, las tres redes)", "",
           table(reliability[reliability["muestra"] == "prevision"],
                 ["tramo", "dias", "dias_con_averia", "prob_media", "tasa", "tasa_ic_bajo", "tasa_ic_alto"]), "",
           "## Valor relativo en los cocientes de referencia", "",
           "El valor relativo es la parte del ahorro de una previsión perfecta que consigue cada regla: 1 es perfecta, "
           "0 es lo mismo que decidir sin previsión (siempre o nunca, lo que salga más barato) y por debajo de 0 sale "
           "más caro que eso.", ""]
    shown = intervals[intervals["medida"] == "valor"].copy()
    shown["ic"] = shown.apply(lambda r: f"{es(r['ic_bajo'])} a {es(r['ic_alto'])}", axis=1)
    out += [table(shown, ["muestra", "perdida", "alfa_o_umbral", "regla", "bloque_dias", "valor", "ic", "prob_positivo"],
                  {"alfa_o_umbral": 2, "valor": 3, "prob_positivo": 2}), "",
            "## Beneficio neto en los umbrales de referencia", ""]
    shown = intervals[intervals["medida"] == "beneficio_neto"].copy()
    shown["ic"] = shown.apply(lambda r: f"{es(r['ic_bajo'], 4)} a {es(r['ic_alto'], 4)}", axis=1)
    out += [table(shown, ["muestra", "alfa_o_umbral", "regla", "bloque_dias", "valor", "ic", "prob_positivo"],
                  {"alfa_o_umbral": 2, "valor": 4, "prob_positivo": 2}), ""]
    best = value[(value["muestra"] == "prevision")]
    for weighting in ("dias", "horas_cliente"):
        part = best[best["perdida"] == weighting]
        positive = part[part["probabilidad"] > 0]["alfa"]
        if len(positive):
            out.append(f"Con la pérdida por {weighting.replace('_', '-')}, la regla probabilística tiene valor positivo "
                       f"para cocientes entre {es(positive.min())} y {es(positive.max())}; su máximo es "
                       f"{es(part['probabilidad'].max())} en {es(float(part.loc[part['probabilidad'].idxmax(), 'alfa']))}.")
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dias")
    parser.add_argument("salida")
    parser.add_argument("--reps", type=int, default=1000)
    parser.add_argument("--semilla", type=int, default=0)
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)

    df = load(args.dias)
    samples = {"prevision": df[df["modo"] == "prevision"], "reanalisis": df[df["modo"] == "reanalisis"]}
    samples["prevision_sin_28_01"] = samples["prevision"][samples["prevision"]["fecha"] != STORM]
    samples = {k: v.reset_index(drop=True) for k, v in samples.items() if not v.empty}

    calibration = pd.DataFrame([r for label, part in samples.items()
                                for r in calibration_rows(part, label, args.reps, args.semilla)])
    reliability = pd.DataFrame([r for label, part in samples.items() for r in reliability_rows(part, label)])
    value = pd.DataFrame([r for label, part in samples.items() for r in value_rows(part, label)])
    benefit = pd.DataFrame([r for label, part in samples.items() for r in benefit_rows(part, label)])
    intervals = pd.DataFrame([r for label in ("prevision", "prevision_sin_28_01") if label in samples
                              for r in value_intervals(samples[label], label, args.reps, args.semilla)])

    for name, frame in (("calibracion", calibration), ("fiabilidad", reliability), ("valor_relativo", value),
                        ("beneficio_neto", benefit), ("intervalos", intervals)):
        frame.to_csv(os.path.join(args.salida, f"{name}.csv"), index=False)
    with open(os.path.join(args.salida, "resumen.md"), "w", encoding="utf-8") as file:
        file.write(report(calibration, reliability, value, benefit, intervals, args.reps))
    figures(reliability, value, benefit, args.salida)
    print(open(os.path.join(args.salida, "resumen.md"), encoding="utf-8").read())
    return 0


if __name__ == "__main__":
    sys.exit(main())
