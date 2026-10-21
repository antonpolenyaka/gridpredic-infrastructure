"""Figura 7 de la memoria v11: la Distribuidora 1 de 2024 a 2026 como matriz mes por día.

Un mes por fila y un día por columna, con el color del nivel que habría dado la tarde anterior con la previsión
real; anillo en los días con avería de causa local, de área proporcional a sus horas-cliente sin recortar; recuadro
fino con aviso amarillo de AEMET y grueso con naranja o rojo; borde discontinuo, día sin datos. Lee historico.json
de la copia de la demo (web/demo/data).
Uso: python3 experimentos/figuras/matriz_dias/matriz_dias.py web/demo/data/historico.json salida.png ["Distribuidora 1"]
"""
import calendar
import json
import sys
from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch, Rectangle  # noqa: E402

SRC, OUT = sys.argv[1], sys.argv[2]
NAME = sys.argv[3] if len(sys.argv) > 3 else "Distribuidora 1"
plt.rcParams.update({"font.family": "Liberation Sans", "font.size": 8.5})
COLOR = {"bajo": "#d9dee4", "medio": "#f2c230", "alto": "#d55e00", "muy alto": "#7f1d2d"}
INK, MUTED, GRID = "#1f2933", "#5d6878", "#9aa5b1"
CAP = 110

h = json.load(open(SRC, encoding="utf-8"))
dist = next(d for d in h["distribuidoras"] if d["nombre"] == NAME)
days = {date.fromisoformat(x["f"]): x for x in dist["dias"]["prevision"]}
first, last = min(days), max(days)
months = []
y, m = first.year, first.month
while (y, m) <= (last.year, last.month):
    months.append((y, m))
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def hc(x, recortar):
    if not recortar:
        return x["ch"]
    if x.get("chr") is not None:
        return x["chr"]
    return x["ch"] * CAP / x["m"] if x["m"] > CAP else x["ch"]


big = max(x["ch"] for x in days.values())
fig_w, fig_h = 16 / 2.54, 15.2 / 2.54
fig = plt.figure(figsize=(fig_w, fig_h), dpi=300)
ax = fig.add_axes([0.085, 0.13, 0.905, 0.85])
n = len(months)
for r, (y, m) in enumerate(months):
    for d in range(1, calendar.monthrange(y, m)[1] + 1):
        dt = date(y, m, d)
        x0, y0 = d - 1, n - 1 - r
        if dt < first or dt > last:
            continue
        x = days.get(dt)
        if x is None:
            ax.add_patch(Rectangle((x0 + 0.06, y0 + 0.06), 0.88, 0.88, fill=False, ec=GRID, lw=0.6, ls=(0, (2, 1.5))))
            continue
        ax.add_patch(Rectangle((x0 + 0.06, y0 + 0.06), 0.88, 0.88, fc=COLOR[x["n"]], ec="none"))
        if x.get("a"):
            lw = 1.6 if x["a"] >= 2 else 0.7
            ax.add_patch(Rectangle((x0 + 0.06, y0 + 0.06), 0.88, 0.88, fill=False, ec=INK, lw=lw))
        if x["i"] > 0:
            s = 6 + 230 * (x["ch"] / big) ** 0.5 if big else 6
            ax.scatter([x0 + 0.5], [y0 + 0.5], s=s, facecolors="none", edgecolors=INK if x["n"] in ("bajo", "medio") else "white",
                       linewidths=0.9, zorder=3)
ax.set_xlim(0, 31)
ax.set_ylim(0, n)
ax.set_xticks([d - 0.5 for d in (1, 5, 10, 15, 20, 25, 31)], ["1", "5", "10", "15", "20", "25", "31"])
MES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
ax.set_yticks([n - 1 - r + 0.5 for r in range(n)],
              [f"{MES[m - 1]} {y}" if m == 1 or r == 0 else MES[m - 1] for r, (y, m) in enumerate(months)])
for lab, (y, m) in zip(ax.get_yticklabels(), months):
    if m == 1:
        lab.set_fontweight("bold")
ax.tick_params(length=0, colors=INK, labelsize=7.5)
for s in ax.spines.values():
    s.set_visible(False)
for r, (y, m) in enumerate(months):
    if m == 1 and r:
        ax.axhline(n - r, color=GRID, lw=0.8)
ax.set_xlabel("Día del mes", color=MUTED, fontsize=8)

handles = [Patch(fc=COLOR[k], ec="none", label=k) for k in ("bajo", "medio", "alto", "muy alto")]
handles += [Line2D([], [], marker="o", ls="", mfc="none", mec=INK, ms=7, label="avería (área: horas-cliente)"),
            Patch(fc="white", ec=INK, lw=0.7, label="aviso amarillo"), Patch(fc="white", ec=INK, lw=1.6, label="naranja o rojo"),
            Patch(fc="white", ec=GRID, lw=0.6, ls="--", label="sin datos")]
fig.legend(handles=handles, loc="lower center", ncol=4, frameon=False, fontsize=7.5, bbox_to_anchor=(0.54, 0.0),
           handlelength=1.4, columnspacing=1.2)
fig.savefig(OUT)

vals = list(days.values())
alert = [x for x in vals if x["n"] in ("alto", "muy alto")]
tot = sum(x["ch"] for x in vals)
totr = sum(hc(x, True) for x in vals)
print(NAME, "días", len(vals), "alerta", len(alert), f"{len(alert) / len(vals):.1%}",
      "hc alerta sin recortar", f"{sum(x['ch'] for x in alert) / tot:.1%}",
      "recortado", f"{sum(hc(x, True) for x in alert) / totr:.1%}",
      "con avería", sum(1 for x in vals if x["i"] > 0), "desde", first, "hasta", last, "huecos",
      (last - first).days + 1 - len(vals))
