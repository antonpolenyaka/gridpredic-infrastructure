"""
Falsas derivas de la parte de días en alerta (memoria 5.7.7 y 6.1): si cada 10 días miramos la parte de días en
alerta (nivel alto o muy alto) de los últimos 90 días de una red, ¿cuántas ventanas del histórico, en el que no hay
ninguna deriva, saldrían de la banda?

Dos referencias: la parte fija de días en alerta de 2024 - 2026 con la previsión real (12,5 %, el 10 % de referencia de la
escala) y la parte de esos mismos días del calendario en los años que la ventana no toca (la estación manda en la parte
de días en alerta).
La banda es la binomial del 99 % (o del 95 %) con los días de la ventana y la parte de referencia; una ventana
"da deriva" si su parte queda fuera.

Lee historico.json, el día a día de 2022 - 2026 que escribe job_ml_caso_negocio.py (modo reanálisis, las tres redes),
sin Spark:

    python deriva_alerta.py web/demo/data/historico.json salida/deriva_alerta.txt

Escribe un resumen por ventana (90 días y banda del 99 %; 60 días y banda del 95 %) y los máximos y mínimos de la
parte de días en alerta por mes del calendario.
"""

import json
import sys

import numpy as np
import pandas as pd
from scipy.stats import binom

ALERTA = {"alto", "muy alto"}
PASO = 10
REF_FIJA = 0.125  # la parte de días en alerta de 2024 - 2026 con la previsión real (memoria 5.7.7)
INICIO_VENTANAS = "2024-01-01"


def cargar(path):
    d = json.load(open(path, encoding="utf-8"))
    rows = []
    for dist in d["distribuidoras"]:
        for x in dist["dias"]["reanalisis"]:
            rows.append({"red": dist["nombre"], "fecha": pd.Timestamp(x["f"]), "alerta": x["n"] in ALERTA})
    return pd.DataFrame(rows).sort_values(["red", "fecha"]).reset_index(drop=True)


def ventanas(t, dias, nivel):
    """Una fila por ventana de `dias` días que acaba cada PASO días desde INICIO_VENTANAS."""
    out = []
    for red, g in t.groupby("red"):
        g = g.set_index("fecha")["alerta"]
        fijo = REF_FIJA
        fines = pd.date_range(pd.Timestamp(INICIO_VENTANAS) + pd.Timedelta(days=dias - 1), g.index.max(), freq=f"{PASO}D")
        for fin in fines:
            v = g[fin - pd.Timedelta(days=dias - 1) : fin]
            if len(v) < dias:
                continue
            k, n = int(v.sum()), len(v)
            # misma estación: los mismos meses del calendario en los otros años del histórico
            meses = set(zip(v.index.month, v.index.day))
            otros = g[(g.index.year != fin.year) & [(m, d) in meses for m, d in zip(g.index.month, g.index.day)]]
            if fin.month < v.index[0].month:  # la ventana cruza el año: quitar también el año de arranque
                otros = otros[otros.index.year != v.index[0].year]
            ref_mes = otros.mean() if len(otros) else np.nan
            out.append({"red": red, "fin": fin.date(), "n": n, "alerta": k / n, "ref_fija": fijo, "ref_mes": ref_mes,
                        "fuera_fija": fuera(k, n, fijo, nivel), "fuera_mes": fuera(k, n, ref_mes, nivel)})
    return pd.DataFrame(out)


def fuera(k, n, p, nivel):
    if not (p > 0):
        return k > 0
    lo, hi = binom.interval(nivel, n, p)
    return bool(k < lo or k > hi)


def main():
    t = cargar(sys.argv[1])
    out = sys.argv[2] if len(sys.argv) > 2 else None
    lines = [f"{len(t)} días de red de {t.fecha.min().date()} a {t.fecha.max().date()}, {t.alerta.mean():.1%} en alerta "
             f"(" + ", ".join(f"{r} {v:.1%}" for r, v in t.groupby('red').alerta.mean().items()) + ")"]
    mes = t.assign(mes=t.fecha.dt.month).groupby("mes").alerta.mean()
    lines.append("Parte de días en alerta por mes del calendario (tres redes, 2022 - 2026): " +
                 ", ".join(f"{m:02d} {v:.0%}" for m, v in mes.items()))
    for dias, nivel in ((90, 0.99), (60, 0.95)):
        w = ventanas(t, dias, nivel)
        lines.append(f"Ventanas de {dias} días cada {PASO} días desde {INICIO_VENTANAS}, banda binomial del {nivel:.0%}: "
                     f"{len(w)} ventanas ({len(w) // 3} por red); parte en alerta de {w.alerta.min():.0%} a {w.alerta.max():.0%}; "
                     f"fuera de la banda contra la parte fija ({REF_FIJA:.1%}): {w.fuera_fija.mean():.0%} de las ventanas; "
                     f"contra los mismos meses de los otros años: {w.fuera_mes.mean():.0%}")
        for red, g in w.groupby("red"):
            lines.append(f"  {red}: fija {g.fuera_fija.mean():.0%}, mismos meses {g.fuera_mes.mean():.0%} ({len(g)} ventanas)")
    text = "\n".join(lines)
    print(text)
    if out:
        open(out, "w", encoding="utf-8").write(text + "\n")


if __name__ == "__main__":
    main()
