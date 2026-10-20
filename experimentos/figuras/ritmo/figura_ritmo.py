"""
La figura del ritmo del proyecto para la memoria (2.3): commits de trabajo, PR mezcladas y versiones acumulados por
día desde que existe el repositorio, con los hitos del máster desde mayo. Así se ve que el negocio y el análisis de
las fuentes van antes del primer commit y que el grueso del código llega con la carga completa del 10.10.2026.

    python experimentos/figuras/ritmo/figura_ritmo.py [salida.png] [--ref origin/main] [--hasta 2026-10-20]

Lee la historia de git del propio repositorio (commits de main sin los de merge y los merges de las PR) y las fechas
de las versiones del CHANGELOG. No necesita red. Deja también ritmo_dia.csv junto a la figura.
"""

import argparse
import csv
import os
import re
import subprocess
from collections import Counter
from datetime import date, timedelta

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
COMMITS, PRS, VERSIONS = "#2a78d6", "#52514e", "#9c9a94"
INK, MUTED, GRID, SURFACE, BAND = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb", "#f1f0ec"

# Hitos del máster y del proyecto (memoria 2.3). El M7 es la entrega.
# Hitos del máster y del proyecto (memoria 2.3), con la altura de su etiqueta (alta o baja) para que no se pisen.
MILESTONES = [
    ("M1", date(2026, 5, 11), "alta"),
    ("M2", date(2026, 6, 22), "alta"),
    ("Pitch", date(2026, 7, 3), "baja"),
    ("M6", date(2026, 9, 13), "alta"),
    ("Carga completa", date(2026, 10, 10), "alta"),
    ("Primera tarde", date(2026, 10, 13), "baja"),
    ("M7", date(2026, 10, 26), "baja"),
]
# La primera ronda de tutorías (negocio, técnica y prototipo), con una sola etiqueta.
TUTORIALS = [date(2026, 7, 6), date(2026, 7, 13), date(2026, 7, 17)]
START = date(2026, 5, 1)


def git(*args) -> list:
    out = subprocess.run(["git", "-C", ROOT, *args], check=True, capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if line.strip()]


def daily(ref: str, until: date) -> dict:
    """{día: (commits, PR mezcladas, versiones)} del repositorio hasta el día dado."""
    commits = Counter(date.fromisoformat(d) for d in git("log", ref, "--no-merges", "--format=%ad",
                                                         "--date=format:%Y-%m-%d"))
    prs = Counter()
    for line in git("log", ref, "--merges", "--format=%ad|%s", "--date=format:%Y-%m-%d"):
        day, subject = line.split("|", 1)
        if subject.startswith("Merge pull request #"):
            prs[date.fromisoformat(day)] += 1
    versions = Counter()
    with open(os.path.join(ROOT, "CHANGELOG.md"), encoding="utf-8") as handle:
        for line in handle:
            m = re.match(r"^## \[(\d+\.\d+\.\d+)\] - (\d{2})\.(\d{2})\.(\d{4})", line)
            if m:
                versions[date(int(m.group(4)), int(m.group(3)), int(m.group(2)))] += 1
    first = min(commits)
    out, day = {}, first
    while day <= until:
        out[day] = (commits[day], prs[day], versions[day])
        day += timedelta(days=1)
    return out


def main():
    parser = argparse.ArgumentParser(description="Figura del ritmo del proyecto (memoria 2.3)")
    parser.add_argument("salida", nargs="?", default=os.path.join(os.path.dirname(__file__), "ritmo_proyecto.png"))
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--hasta", default=date.today().isoformat())
    args = parser.parse_args()
    until = date.fromisoformat(args.hasta)
    rows = daily(args.ref, until)
    days = sorted(rows)
    acc, totals = [], [0, 0, 0]
    for d in days:
        totals = [t + v for t, v in zip(totals, rows[d])]
        acc.append(tuple(totals))
    with open(os.path.join(os.path.dirname(os.path.abspath(args.salida)), "ritmo_dia.csv"), "w", newline="",
              encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fecha", "commits_dia", "commits_acum", "pr_dia", "pr_acum", "versiones_dia",
                         "versiones_acum"])
        for d, (c, p, v), (ca, pa, va) in zip(days, (rows[d] for d in days), acc):
            writer.writerow([d.strftime("%d.%m.%Y"), c, ca, p, pa, v, va])

    fig, (top, bottom) = plt.subplots(2, 1, figsize=(9, 5.2), dpi=200, sharex=True,
                                      gridspec_kw={"height_ratios": [2.2, 1]})
    fig.patch.set_facecolor(SURFACE)
    first = days[0]
    for ax in (top, bottom):
        ax.set_facecolor(SURFACE)
        ax.axvspan(START, first, color=BAND, zorder=0)
        for _, day, _ in MILESTONES:
            ax.axvline(day, color=GRID if day <= until else VERSIONS, linewidth=1,
                       linestyle="-" if day <= until else (0, (3, 3)), zorder=1)
        for day in TUTORIALS:
            ax.axvline(day, color=GRID, linewidth=0.8, linestyle=(0, (1, 2)), zorder=1)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=MUTED, labelsize=8)

    top.step(days, [a[0] for a in acc], where="post", color=COMMITS, linewidth=2)
    top.set_ylabel("Commits", fontsize=9, color=INK)
    top.text(days[-1], acc[-1][0], f"  {acc[-1][0]}", color=COMMITS, fontsize=8, va="center")
    bottom.step(days, [a[1] for a in acc], where="post", color=PRS, linewidth=1.8, label="PR mezcladas")
    bottom.step(days, [a[2] for a in acc], where="post", color=VERSIONS, linewidth=1.8, linestyle=(0, (4, 2)),
                label="Versiones")
    bottom.text(days[-1], acc[-1][1], f"  {acc[-1][1]}", color=PRS, fontsize=8, va="center")
    bottom.text(days[-1], acc[-1][2], f"  {acc[-1][2]}", color=VERSIONS, fontsize=8, va="center")
    bottom.set_ylabel("PR y versiones", fontsize=9, color=INK)
    bottom.legend(frameon=False, fontsize=8, loc="upper left")

    ymax = max(a[0] for a in acc)
    top.set_ylim(0, ymax * 1.18)
    for name, day, level in MILESTONES:
        top.text(day, ymax * (1.11 if level == "alta" else 1.03), name, fontsize=7.5,
                 color=INK if day <= until else MUTED, ha="center", va="bottom")
    top.text(TUTORIALS[1], ymax * 1.11, "Tutorías", fontsize=7.5, color=INK, ha="center", va="bottom")
    top.text(START + (first - START) / 2, ymax * 0.45,
             "Negocio (M1, M2 y el Pitch), tutorías y análisis\nde TedisNet y Calser en local, fuera de Git",
             fontsize=8, color=MUTED, ha="center", va="center")
    top.set_title(f"Ritmo del proyecto: commits, PR y versiones acumulados (a {until.strftime('%d.%m.%Y')})",
                  fontsize=11, color=INK, loc="left")
    bottom.set_xlim(START, max(until, MILESTONES[-1][1]) + timedelta(days=6))
    bottom.xaxis.set_major_locator(mdates.MonthLocator())
    bottom.xaxis.set_major_formatter(mdates.DateFormatter("%m.%Y"))
    note = ("Commits de trabajo de main, sin los de merge; PR mezcladas en main; versiones del CHANGELOG. "
            "Líneas de puntos: la primera ronda de tutorías; discontinua: la entrega del M7.")
    fig.text(0.01, 0.01, note, fontsize=7, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(args.salida, facecolor=SURFACE)
    print("escrito", args.salida, "| commits", acc[-1][0], "| PR", acc[-1][1], "| versiones", acc[-1][2])


if __name__ == "__main__":
    main()
