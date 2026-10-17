"""
Figura de 4.7.3: los dos planes del join de tres tablas (consulta C5-B) lado a
lado, como los árboles de proceso de Database Foundations, con las filas que
estimó el optimizador y las reales en cada nodo:

    python figura_planes.py <carpeta con los json> <png de salida>

A la izquierda, el plan sin estadísticas (fase A); a la derecha, el de después
de ANALYZE (fase C). Las cifras salen de los EXPLAIN ANALYZE guardados por
planes.py; la tabla hash de cada join va resaltada.
"""

import json
import os
import re
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

INK, MUTED, LINE, ACCENT, ACCENT_SOFT, PAPER = "#1b2430", "#5d6878", "#c3ccd8", "#2457c5", "#e3ebfa", "#f4f6f9"
WIDTH = 6.3
plt.rcParams.update({"font.family": ["Liberation Sans", "DejaVu Sans"], "font.size": 7.5, "savefig.dpi": 300,
                     "axes.unicode_minus": False})


def es(n) -> str:
    return "?" if n in (None, "?") else f"{int(n):,}".replace(",", ".")


def size(text: str) -> str:
    """'46.19MB' -> '46 MB'; '?' se queda igual."""
    m = re.match(r"([\d.]+)([kMG]?B)", text or "")
    return f"{float(m.group(1)):.0f} {m.group(2)}" if m else "?"


def read(src: str, name: str) -> dict:
    d = json.load(open(os.path.join(src, name), encoding="utf-8"))
    text = d["explain_analyze"]["filas"][0][0]
    times = [p["stats"]["elapsedTimeMillis"] / 1000 for p in d["pasadas"] if p.get("estado") == "FINISHED"]
    first = d["pasadas"][0]["stats"]
    joins = re.findall(r"InnerJoin\[criteria = \(([a-z_0-9]+) = .*?distribution = (\w+)\]\n.*?Estimates: \{rows: (\?|\d+)"
                       r" \(([^)]*)\).*?Output: (\d+) rows", text, flags=re.S)
    scans = {}
    for table, block in re.findall(r"Scan\w*\[table = lakehouse:l3_gold\.(\w+)(.*?)(?=\n\s*[└├]─|\nFragment |\Z)", text,
                                   flags=re.S):
        est = re.search(r"Estimates: \{rows: (\?|\d+) \(([^)]*)\)[^}]*\}(?:/\{rows: (\?|\d+) \(([^)]*)\))?", block)
        inp = re.search(r"Input: (\d+) rows", block)
        out = re.search(r"Output: (\d+) rows", block)
        scans[table] = {"est": (est.group(3) or est.group(1)) if est else "?",
                        "est_mb": (est.group(4) or est.group(2)) if est else "?", "leidas": inp.group(1) if inp else None,
                        "salen": out.group(1) if out else None}
    return {"joins": joins, "scans": scans, "caliente": statistics.median(times[1:]),
            "memoria": first.get("peakMemoryBytes", 0) / 1e6, "cpu": first.get("cpuTimeMillis", 0) / 1000}


def box(ax, x, y, w, h, title, lines, hash_side=False):
    face, edge = (ACCENT_SOFT, ACCENT) if hash_side else (PAPER, LINE)
    ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h, boxstyle="round,pad=0.006,rounding_size=0.012",
                                facecolor=face, edgecolor=edge, linewidth=1.1 if hash_side else 0.8, zorder=2))
    ax.text(x, y + h / 2 - 0.022, title, ha="center", va="top", fontsize=7.4, fontweight="bold",
            color=ACCENT if hash_side else INK, zorder=3)
    for k, line in enumerate(lines):
        ax.text(x, y + h / 2 - 0.058 - k * 0.034, line, ha="center", va="top", fontsize=6.9, color=INK if k == 0 else
                MUTED, zorder=3)


def edge(ax, x0, y0, x1, y1):
    ax.plot([x0, x1], [y0, y1], color=LINE, lw=0.9, zorder=1)


def tree(ax, x0, plan, after: bool):
    """Dibuja un plan en la mitad del eje que empieza en x0 (ancho 0,5)."""
    top_join, inner_join = plan["joins"][0], plan["joins"][1]
    ds, mp, mt = plan["scans"]["dataset_train"], plan["scans"]["map_ct_celda"], plan["scans"]["meteo_celda_hora"]
    w, h = 0.215, 0.16
    cx = x0 + 0.25
    box(ax, cx, 0.86, w, 0.1, "Agregación por red", ["3 filas"])
    yj = 0.66
    box(ax, cx, yj, w, h, "Join por celda y hora", [f"estimadas: {es(top_join[2])}", f"reales: {es(top_join[4])}"])
    edge(ax, cx, 0.81, cx, yj + h / 2)
    left, right = x0 + 0.125, x0 + 0.375
    meteo_est = es(mt["est"]) + (f" ({size(mt['est_mb'])})" if after else "")
    inner_est = es(inner_join[2]) + (f" ({size(inner_join[3])})" if after else "")
    meteo_lines = [f"lee {es(mt['leidas'])}, salen {es(mt['salen'])}", f"estimadas: {meteo_est}"]
    inner_lines = [f"estimadas: {inner_est}", f"reales: {es(inner_join[4])}"]
    ds_lines = [f"lee {es(ds['leidas'])}", "enero de 2025"]
    map_lines = [f"{es(mp['salen'])} CT", "tabla hash"]
    if not after:
        box(ax, left, 0.42, w, h, "Join por red y CT", inner_lines)
        box(ax, right, 0.42, w, h, "meteo_celda_hora", meteo_lines + ["tabla hash"], hash_side=True)
        edge(ax, cx, yj - h / 2, left, 0.42 + h / 2)
        edge(ax, cx, yj - h / 2, right, 0.42 + h / 2)
        box(ax, x0 + 0.073, 0.15, 0.13, 0.14, "dataset_train", ds_lines)
        box(ax, x0 + 0.222, 0.15, 0.12, 0.14, "map_ct_celda", map_lines, hash_side=True)
        edge(ax, left, 0.42 - h / 2, x0 + 0.073, 0.15 + 0.07)
        edge(ax, left, 0.42 - h / 2, x0 + 0.222, 0.15 + 0.07)
    else:
        box(ax, left, 0.42, w, h, "meteo_celda_hora", meteo_lines)
        box(ax, right, 0.42, w, h, "Join por red y CT", inner_lines + ["tabla hash"], hash_side=True)
        edge(ax, cx, yj - h / 2, left, 0.42 + h / 2)
        edge(ax, cx, yj - h / 2, right, 0.42 + h / 2)
        box(ax, x0 + 0.278, 0.15, 0.13, 0.14, "dataset_train", ds_lines)
        box(ax, x0 + 0.427, 0.15, 0.12, 0.14, "map_ct_celda", map_lines, hash_side=True)
        edge(ax, right, 0.42 - h / 2, x0 + 0.278, 0.15 + 0.07)
        edge(ax, right, 0.42 - h / 2, x0 + 0.427, 0.15 + 0.07)
    calor = f"{plan['caliente']:.1f}".replace(".", ",")
    ax.text(cx, 0.015, f"{calor} s en caliente, {plan['memoria']:.0f} MB de memoria", ha="center", va="bottom",
            fontsize=7.4, color=INK, fontweight="bold")


def main(argv) -> int:
    src, out = argv[0], argv[1]
    before, after = read(src, "A_c5b.json"), read(src, "C_c5b.json")
    fig, ax = plt.subplots(figsize=(WIDTH, 3.9))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.13)
    ax.axis("off")
    ax.axvline(0.5, ymin=0.02, ymax=0.86, color=LINE, lw=0.6, ls=(0, (2, 3)))
    ax.text(0.25, 1.0, "Sin estadísticas (antes)", ha="center", fontsize=8, color=INK, fontweight="bold")
    ax.text(0.75, 1.0, "Después de ANALYZE", ha="center", fontsize=8, color=INK, fontweight="bold")
    tree(ax, 0.0, before, after=False)
    tree(ax, 0.5, after, after=True)
    fig.text(0.01, 0.985, "Con estadísticas, Trino cambió de lado la tabla hash y el join tardó más", ha="left",
             va="top", fontsize=9.5, fontweight="bold", color=INK)
    fig.text(0.01, 0.935, "Join de dataset_train, map_ct_celda y meteo_celda_hora para enero de 2025. En azul, la tabla "
             "hash de cada join, que Trino replica; en cada nodo, las filas que estimó el optimizador y las reales.",
             ha="left", va="top", fontsize=7, color=MUTED, wrap=True)
    fig.subplots_adjust(left=0.01, right=0.99, top=0.9, bottom=0.01)
    fig.savefig(out)
    plt.close(fig)
    print(json.dumps({"antes": {k: before[k] for k in ("caliente", "memoria", "cpu")},
                      "despues": {k: after[k] for k in ("caliente", "memoria", "cpu")}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
