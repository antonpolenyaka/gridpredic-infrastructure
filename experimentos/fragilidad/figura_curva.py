"""
La figura de la curva de fragilidad para la memoria (figura 9, 5.7.6), desde tramos.csv y
curva.csv de curva_fragilidad.py:

    python figura_curva.py experimentos/fragilidad experimentos/fragilidad/curva_fragilidad.png

Azul y gris, como las figuras 3 y 4, con el intervalo de Wilson del 95 % de cada barra y el
título con la conclusión, a 6,3 pulgadas de ancho, que es lo que ocupa en la página.
"""

import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

FIT, LATER, INK, MUTED, GRID, SURFACE = "#2457c5", "#9aa5b1", "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def wilson(k: np.ndarray, n: np.ndarray, z: float = 1.96) -> tuple:
    """Límites del intervalo de Wilson de k aciertos en n, en proporción."""
    p = np.where(n > 0, k / np.maximum(n, 1), 0.0)
    den = 1 + z ** 2 / n
    centre = (p + z ** 2 / (2 * n)) / den
    half = z * np.sqrt(p * (1 - p) / n + z ** 2 / (4 * n ** 2)) / den
    return centre - half, centre + half


def label(lo: float, hi: float) -> str:
    if lo == 0:
        return f"< {hi * 100:.0f}"
    if hi >= 5:
        return f"{lo * 100:.0f} o más"
    return f"{lo * 100:.0f} - {hi * 100:.0f}"


def main(folder: str, out: str):
    bins = pd.read_csv(os.path.join(folder, "tramos.csv"))
    curve = pd.read_csv(os.path.join(folder, "curva.csv"))
    x = np.arange(len(bins))
    width = 0.38
    fig, ax = plt.subplots(figsize=(6.3, 3.7), dpi=300)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    series = [("ajuste", -1, FIT, "2017 - 2021 (ajuste de la curva)"),
              ("despues", 1, LATER, "2022 - 2026 (días que la curva no vio)")]
    for key, side, color, name in series:
        n = bins[f"size_{key}"].to_numpy(dtype=float)
        k = np.round(bins[f"mean_{key}"].to_numpy() * n)
        lo, hi = wilson(k, n)
        mean = bins[f"mean_{key}"].to_numpy()
        pos = x + side * (width / 2 + 0.01)
        ax.bar(pos, 100 * mean, width, color=color, label=name, edgecolor=SURFACE, linewidth=0.8)
        ax.errorbar(pos, 100 * mean, yerr=[100 * (mean - lo), 100 * (hi - mean)], fmt="none", ecolor=INK,
                    elinewidth=0.6, capsize=1.6, capthick=0.6)
    # The isotonic curve read at the middle of every bin.
    mids = [(lo + min(hi, 2.0)) / 2 for lo, hi in zip(bins["desde"], bins["hasta"])]
    fitted = np.interp(mids, curve["ext_racha_clima_max"], curve["frag_prob"])
    ax.plot(x, 100 * fitted, color=INK, linewidth=1.4, marker="o", markersize=3, label="curva isotónica (2017 - 2021)")
    ticks = [f"{label(lo, hi)}\n{int(a + b):,}".replace(",", ".") for lo, hi, a, b in
             zip(bins["desde"], bins["hasta"], bins["size_ajuste"], bins["size_despues"])]
    ax.set_xticks(x, ticks, rotation=0, fontsize=6)
    ax.set_xlabel("Racha máxima del día, en % del percentil 98 de su celda (debajo, días del tramo)", fontsize=7.5,
                  color=INK)
    ax.set_ylabel("Días con avería (%)", fontsize=7.5, color=INK)
    ax.set_title("Por encima de 1,4 veces el percentil 98 de la racha, la avería se dispara", fontsize=9,
                 color=INK, loc="left")
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=6.5)
    ax.legend(frameon=False, fontsize=6.5, loc="upper left")
    note = ("Días de distribuidora de las tres redes con alguna interrupción computable. Bigotes: intervalo de "
            "Wilson del 95 %. Los tramos no tienen la misma anchura.")
    fig.text(0.01, 0.01, note, fontsize=5.5, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out, facecolor=SURFACE)
    print("escrito", out)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
