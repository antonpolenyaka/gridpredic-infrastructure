"""
Los dos gráficos del perfil de los días con avería, pensados para leerse sin ser
estadístico: barras horizontales del valor test ordenadas, con la diferencia en
unidades reales escrita al final de cada barra, y la parte de días con avería
por tramos de cuatro variables con la misma escala. Tamaño de impresión de la
memoria (16 cm de ancho), coma decimal, sin colores de nivel de riesgo.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from perfil import wilson  # noqa: E402

INK, MUTED, GRID, ACCENT, GREY = "#1b2430", "#5d6878", "#e3e7ed", "#2457c5", "#9aa5b1"
WIDTH = 6.3  # pulgadas: 16 cm, la caja de texto de la memoria
plt.rcParams.update({"font.family": ["Liberation Sans", "DejaVu Sans"], "font.size": 8.5, "axes.labelsize": 8.5,
                     "xtick.labelsize": 8, "ytick.labelsize": 8, "axes.edgecolor": GRID, "axes.labelcolor": INK,
                     "xtick.color": MUTED, "ytick.color": INK, "savefig.dpi": 300, "axes.unicode_minus": False})


def es(value: float, digits: int = 1) -> str:
    text = f"{value:,.{digits}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def title(fig, text: str, sub: str = None) -> None:
    fig.text(0.01, 0.99, text, ha="left", va="top", fontsize=10, fontweight="bold", color=INK)
    if sub:
        fig.text(0.01, 0.955, sub, ha="left", va="top", fontsize=8, color=MUTED)


def vtest_bars(quanti, names: dict, in_model: set, units: dict, threshold: float, path: str, heading: str,
               sub: str) -> None:
    """Una barra por variable continua, de mayor a menor valor test; a la derecha, la media con y sin avería."""
    data = quanti[quanti["variable"].isin(names)].sort_values("v_test")
    height = 1.15 + 0.235 * len(data)
    fig, ax = plt.subplots(figsize=(WIDTH, height))
    ys = np.arange(len(data))
    colours = [ACCENT if v in in_model else GREY for v in data["variable"]]
    ax.barh(ys, data["v_test"], color=colours, height=0.62, zorder=2)
    for x in (-threshold, threshold):
        ax.axvline(x, color=MUTED, lw=0.8, ls=(0, (3, 3)), zorder=1)
    ax.axvline(0, color=INK, lw=0.8, zorder=3)
    for y, (_, r) in zip(ys, data.iterrows()):
        unit, digits = units.get(r["variable"], ("", 1))
        ax.text(1.02, y, f"{es(r['media_clase'], digits)} frente a {es(r['media_resto'], digits)}{unit}",
                transform=ax.get_yaxis_transform(), va="center", ha="left", fontsize=7, color=MUTED)
    ax.text(1.02, len(data) - 0.15, "Con avería frente al resto", transform=ax.get_yaxis_transform(), va="bottom",
            ha="left", fontsize=7, color=INK, fontweight="bold")
    low = min(-threshold * 2.2, data["v_test"].min() * 1.15)
    ax.set_xlim(low, data["v_test"].max() * 1.08)
    ax.set_yticks(ys, [names[v] for v in data["variable"]])
    ax.set_xlabel("Valor test (diferencia de medias en errores típicos)")
    ax.text(threshold, len(data) - 0.15, f" umbral {es(threshold, 2)} (p = 0,001)", fontsize=6.8, color=MUTED,
            va="bottom")
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="x", color=GRID, lw=0.6, zorder=0)
    handles = [plt.Rectangle((0, 0), 1, 1, color=ACCENT), plt.Rectangle((0, 0), 1, 1, color=GREY)]
    fig.legend(handles, ["En el modelo en producción", "Fuera del modelo: descartada en 5.7.6 o sin previsión"],
               loc="lower left", bbox_to_anchor=(0.01, 0.0), ncol=2, fontsize=7, frameon=False)
    title(fig, heading, sub)
    fig.subplots_adjust(left=0.33, right=0.70, top=1 - 0.78 / height, bottom=0.62 / height)
    fig.savefig(path)
    plt.close(fig)


def rate_panels(df, y: str, panels: list, base: float, path: str, heading: str, sub: str) -> list:
    """Parte de días con avería por tramo (con su intervalo de Wilson), cuatro paneles con la misma escala."""
    fig, axes = plt.subplots(2, 2, figsize=(WIDTH, 4.6), sharey=True)
    rows, top = [], 0.0
    for column, label, order in panels:
        g = df.groupby(column, observed=True)[y].agg(["sum", "size"])
        if order:
            g = g.reindex([o for o in order if o in g.index])
        for mod, r in g.iterrows():
            lo, hi = wilson(int(r["sum"]), int(r["size"]))
            rows.append({"variable": column, "panel": label, "tramo": str(mod), "dias": int(r["size"]),
                         "dias_averia": int(r["sum"]), "tasa": r["sum"] / r["size"], "ic_inf": lo, "ic_sup": hi})
            top = max(top, hi)
    for ax, (column, label, order) in zip(axes.flat, panels):
        part = [r for r in rows if r["variable"] == column]
        xs = np.arange(len(part))
        rates = np.array([r["tasa"] for r in part]) * 100
        lows = rates - np.array([r["ic_inf"] for r in part]) * 100
        highs = np.array([r["ic_sup"] for r in part]) * 100 - rates
        ax.bar(xs, rates, color=ACCENT, width=0.62)
        ax.errorbar(xs, rates, yerr=[lows, highs], fmt="none", ecolor=INK, elinewidth=0.8, capsize=2)
        ax.axhline(base * 100, color=MUTED, lw=0.8, ls=(0, (3, 3)))
        for x, r in zip(xs, part):
            ax.text(x, -0.05, r["tramo"], transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=6.8,
                    color=INK)
            ax.text(x, -0.19, es(r["dias"], 0), transform=ax.get_xaxis_transform(), ha="center", va="top",
                    fontsize=6.3, color=MUTED)
        ax.set_xticks([])
        ax.set_title(label, fontsize=8.5, loc="left", color=INK)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.set_axisbelow(True)
    axes[0, 0].text(-0.4, base * 100 + 0.4, f"todos los días: {es(base * 100, 1)} %", fontsize=6.8, color=MUTED,
                    ha="left", va="bottom")
    for ax in axes[:, 0]:
        ax.set_ylabel("Días con avería (%)")
    axes[0, 0].set_ylim(0, top * 100 * 1.08)
    title(fig, heading, sub)
    fig.subplots_adjust(left=0.08, right=0.99, top=0.85, bottom=0.12, hspace=0.72, wspace=0.12)
    fig.savefig(path)
    plt.close(fig)
    return rows
