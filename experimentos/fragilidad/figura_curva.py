"""
La figura de la curva de fragilidad para la memoria, desde tramos.csv y
curva.csv de curva_fragilidad.py:

    python figura_curva.py experimentos/fragilidad experimentos/fragilidad/curva_fragilidad.png
"""

import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

FIT, LATER, INK, MUTED, GRID, SURFACE = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def label(lo: float, hi: float) -> str:
    if lo == 0:
        return f"< {hi * 100:.0f} %"
    if hi >= 5:
        return f">= {lo * 100:.0f} %"
    return f"{lo * 100:.0f} - {hi * 100:.0f} %"


def main(folder: str, out: str):
    bins = pd.read_csv(os.path.join(folder, "tramos.csv"))
    curve = pd.read_csv(os.path.join(folder, "curva.csv"))
    x = np.arange(len(bins))
    width = 0.38
    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.bar(x - width / 2 - 0.01, 100 * bins["mean_ajuste"], width, color=FIT, label="2017 - 2021 (ajuste de la curva)",
           edgecolor=SURFACE, linewidth=1)
    ax.bar(x + width / 2 + 0.01, 100 * bins["mean_despues"], width, color=LATER,
           label="2022 - 2026 (días que la curva no vio)", edgecolor=SURFACE, linewidth=1)
    # The isotonic curve read at the middle of every bin.
    mids = [(lo + min(hi, 2.0)) / 2 for lo, hi in zip(bins["desde"], bins["hasta"])]
    fitted = np.interp(mids, curve["ext_racha_clima_max"], curve["frag_prob"])
    ax.plot(x, 100 * fitted, color=INK, linewidth=2, marker="o", markersize=5, label="curva isotónica (2017 - 2021)")
    ticks = [f"{label(lo, hi)}\n{int(a + b)} días" for lo, hi, a, b in
             zip(bins["desde"], bins["hasta"], bins["size_ajuste"], bins["size_despues"])]
    ax.set_xticks(x, ticks, rotation=0, fontsize=7.5)
    ax.set_xlabel("Racha máxima del día frente al percentil 98 de su celda", fontsize=9, color=INK)
    ax.set_ylabel("Días con avería (%)", fontsize=9, color=INK)
    ax.set_title("Curva de fragilidad de las redes: parte de días con avería según la racha", fontsize=11,
                 color=INK, loc="left")
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.text(0.01, 0.01, "Días de distribuidora con avería de causa local. Bajo cada tramo, los días que caen en él.",
             fontsize=7, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out, facecolor=SURFACE)
    print("escrito", out)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
