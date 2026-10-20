"""
Junta los JSON que deja escalado.py (uno por número de núcleos y parte de los meses) y ajusta los modelos de escala
de la carga de cálculo, sin Spark:

    python ajustar_escalado.py <carpeta con escalado_*.json> [salida/]

- Con los núcleos (toda la tabla): la ley de Amdahl, T(n) = T(1) ((1 - p) + p / n), y la ley universal de
  escalabilidad de Gunther, C(n) = n / (1 + s (n - 1) + k n (n - 1)), las dos por mínimos cuadrados lineales sobre
  n T(n) / T(1) - 1. Se queda la USL si k supera dos errores típicos; si no, Amdahl.
- Con los datos (8 núcleos, la cuarta parte, la mitad y todos los meses): el exponente b de T = a filas^b. Por
  debajo de 0,9, la estimación lineal del coste de Gold con más distribuidoras es una cota superior; entre 0,9 y 1,1
  se sostiene; por encima de 1,1 hay que corregirla.

Marca los puntos con más de un 10 % entre las pasadas que cuentan, que hay que repetir. Los JSON de antes del
20.10.2026 solo calentaban el recuento: en ellos la primera pasada del cálculo no cuenta (salía un 4 - 30 % más lenta
que la menor de las otras dos) y su sobrecoste queda en la columna primera_pasada_calculo. Deja escalado_nucleos.csv y
escalado_nucleos_ajuste.txt (escalado.csv es la medida del 17.10.2026 y no se toca).
"""

import glob
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))


def ols(X, y):
    """Coeficientes y errores típicos de y = X b sin intercepto."""
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    residual = y - X @ beta
    dof = max(len(y) - X.shape[1], 1)
    cov = (residual @ residual / dof) * np.linalg.inv(X.T @ X)
    return beta, np.sqrt(np.diag(cov))


def counted(run: dict, load: str) -> list:
    """Tiempos de las pasadas que cuentan: todas si la ejecución calentó esa carga; si no, todas menos la primera."""
    times = [t for t, _ in run.get(load) or []]
    if load in (run.get("calentamiento") or ["recorrido"]) or len(times) < 3:
        return times
    return times[1:]


def first_pass(run: dict):
    """Lo que tardó de más la primera pasada del cálculo sin calentar, frente a la menor de las otras."""
    times = [t for t, _ in run.get("calculo") or []]
    if "calculo" in (run.get("calentamiento") or []) or len(times) < 3 or min(times[1:]) <= 0:
        return None
    return round(times[0] / min(times[1:]) - 1, 3)


def main(argv) -> int:
    folder = argv[0]
    out = argv[1] if len(argv) > 1 else os.path.join(HERE, "salida")
    runs = []
    for path in sorted(glob.glob(os.path.join(folder, "escalado_*.json"))):
        with open(path, encoding="utf-8") as handle:
            runs.append(json.load(handle))
    if not runs:
        raise SystemExit(f"No hay escalado_*.json en {folder}")
    for run in runs:
        for load in ("recorrido", "calculo"):
            times = counted(run, load)
            if times:
                run[f"{load}_s"] = min(times)
                run[f"{load}_dispersion"] = round(max(times) / min(times) - 1, 3) if min(times) > 0 else None
        run["primera_pasada_calculo"] = first_pass(run)
    lines = []
    cores = sorted((r for r in runs if r.get("meses", 1.0) >= 1), key=lambda r: r["nucleos"])
    base = next((r for r in cores if r["nucleos"] == 1), None)
    rows = ["nucleos,meses,filas,recorrido_s,calculo_s,dispersion_calculo,primera_pasada_calculo,speedup_recorrido,"
            "speedup_calculo"]
    for r in sorted(runs, key=lambda r: (r.get("meses", 1.0), r["nucleos"])):
        sr = base["recorrido_s"] / r["recorrido_s"] if base and r.get("meses", 1.0) >= 1 else float("nan")
        sc = base["calculo_s"] / r["calculo_s"] if base and r.get("meses", 1.0) >= 1 else float("nan")
        first = r.get("primera_pasada_calculo")
        rows.append(f"{r['nucleos']},{r.get('meses', 1.0):g},{r.get('filas', '')},{r['recorrido_s']},{r['calculo_s']},"
                    f"{r.get('calculo_dispersion', '')},{'' if first is None else first},{sr:.2f},{sc:.2f}")
        if (r.get("calculo_dispersion") or 0) > 0.10:
            lines.append(f"REPETIR: {r['nucleos']} núcleos y {r.get('meses', 1.0):g} de los meses, "
                         f"{100 * r['calculo_dispersion']:.0f} % entre las pasadas que cuentan")
    firsts = [r["primera_pasada_calculo"] for r in runs if r.get("primera_pasada_calculo") is not None]
    if firsts:
        lines.append(f"Primera pasada del cálculo sin calentar (no cuenta): entre un {100 * min(firsts):.0f} y un "
                     f"{100 * max(firsts):.0f} % más lenta que la menor de las otras.")
    if base and len(cores) >= 3:
        n = np.array([r["nucleos"] for r in cores], dtype=float)
        t = np.array([r["calculo_s"] for r in cores], dtype=float)
        y = n * t / base["calculo_s"] - 1
        (serial,), _ = ols((n - 1)[:, None], y)
        p = 1 - serial
        (sigma, kappa), (se_sigma, se_kappa) = ols(np.column_stack([n - 1, n * (n - 1)]), y)
        use_usl = kappa > 2 * se_kappa
        lines.append(f"Núcleos medidos: {', '.join(f'{int(v)}' for v in n)}; cálculo {', '.join(f'{v:g}' for v in t)} s.")
        lines.append(f"Amdahl: parte paralela p = {100 * p:.1f} %, techo {1 / (1 - p):.1f} veces." if p < 1
                     else "Amdahl: p >= 1, sin techo con estos puntos.")
        lines.append(f"USL: s = {sigma:.4f} (error {se_sigma:.4f}), k = {kappa:.5f} (error {se_kappa:.5f}); "
                     + (f"máximo en {np.sqrt((1 - sigma) / kappa):.1f} núcleos." if kappa > 0 else "sin máximo."))
        lines.append(f"Modelo elegido: {'USL (k supera dos errores típicos)' if use_usl else 'Amdahl (k no supera dos errores típicos)'}.")
    data = sorted((r for r in runs if r["nucleos"] == 8 and r.get("filas")), key=lambda r: r["filas"])
    if len(data) >= 2:
        rows_read = np.array([r["filas"] for r in data], dtype=float)
        t = np.array([r["calculo_s"] for r in data], dtype=float)
        b, a = np.polyfit(np.log(rows_read), np.log(t), 1)
        verdict = ("la estimación lineal de Gold es una cota superior" if b < 0.9 else
                   "la estimación lineal de Gold se sostiene" if b <= 1.1 else "hay que corregir la estimación lineal")
        lines.append(f"Datos (8 núcleos): {', '.join(f'{int(v):,}'.replace(',', '.') for v in rows_read)} filas; "
                     f"exponente b = {b:.2f}: {verdict}.")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "escalado_nucleos.csv"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(rows) + "\n")
    with open(os.path.join(out, "escalado_nucleos_ajuste.txt"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print("\n".join(rows + [""] + lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
