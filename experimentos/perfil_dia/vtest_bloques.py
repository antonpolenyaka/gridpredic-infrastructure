"""
El valor test del perfil de los días con avería (memoria, 5.3.5) corregido por la dependencia entre redes del mismo
día y entre días seguidos, con la tabla que deja exportar_dia.py:

    python vtest_bloques.py <dia.csv.gz> <salida/> [--replicas 2000] [--semillas 20261018 20261019]

catdes supone que los días con avería son una muestra al azar de los días de red. Aquí las tres redes comparten el
tiempo de cada fecha y un temporal dura varios días. Para cada variable continua de correr_perfil.py se calcula el
error de la diferencia de medias (clase frente al resto) con tres bootstraps sobre los mismos días: días de red
sueltos (lo que supone catdes), fechas enteras con las tres redes juntas y bloques de siete fechas seguidas (lo que
usa job_ml_riesgo_dia.py desde la 0.16.0). El cociente entre el error con bloques y el de días sueltos dice cuánto
hay que encoger el valor test. Se repite con dos semillas para ver si la lista de variables que pasan el umbral de
3,29 cambia con el sorteo.

Deja vtest_bloques_<semilla>.csv y vtest_bloques.md. Con 2.000 réplicas tarda unos minutos.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from correr_perfil import NAMES, THRESHOLD, prepare  # noqa: E402
from perfil import vtest_numeric  # noqa: E402


def block_index(df: pd.DataFrame) -> tuple:
    """Las filas de cada fecha y las de cada bloque de siete fechas seguidas."""
    dates = df["fecha"].to_numpy()
    unique = np.sort(np.unique(dates))
    position = np.searchsorted(unique, dates)
    by_date = [np.flatnonzero(position == i) for i in range(len(unique))]
    week = np.arange(len(unique)) // 7
    by_week = [np.concatenate([by_date[i] for i in np.flatnonzero(week == w)]) for w in range(week.max() + 1)]
    return by_date, by_week


def corrected(df: pd.DataFrame, numeric: list, replicas: int, seed: int) -> pd.DataFrame:
    y = df["y_dia"].to_numpy().astype(int)
    by_date, by_week = block_index(df)
    rng = np.random.default_rng(seed)
    X = {c: df[c].to_numpy(dtype=float) for c in numeric}

    def resample(kind):
        if kind == "dias":
            return rng.integers(0, len(df), len(df))
        groups = by_date if kind == "fechas" else by_week
        return np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))])

    draws = {kind: {c: [] for c in numeric} for kind in ("dias", "fechas", "semanas")}
    for kind in draws:
        for _ in range(replicas):
            index = resample(kind)
            yy = y[index]
            for c in numeric:
                x = X[c][index]
                ok = ~np.isnan(x)
                draws[kind][c].append(x[ok & (yy == 1)].mean() - x[ok & (yy == 0)].mean())
    rows = []
    for c in numeric:
        v = vtest_numeric(df[c], df["y_dia"])["v_test"]
        se = {kind: np.nanstd(draws[kind][c], ddof=1) for kind in draws}
        rows.append({"variable": c, "nombre": NAMES[c], "v_test": v, "error_dias": se["dias"],
                     "error_fechas": se["fechas"], "error_semanas": se["semanas"],
                     "factor_fechas": se["fechas"] / se["dias"], "factor_semanas": se["semanas"] / se["dias"],
                     "v_test_fechas": v * se["dias"] / se["fechas"], "v_test_semanas": v * se["dias"] / se["semanas"]})
    return pd.DataFrame(rows).sort_values("v_test", key=np.abs, ascending=False).reset_index(drop=True)


def main(argv) -> int:
    parser = argparse.ArgumentParser(description="Valor test del perfil con bloques de fechas y de semanas")
    parser.add_argument("datos")
    parser.add_argument("salida")
    parser.add_argument("--replicas", type=int, default=2000)
    parser.add_argument("--semillas", type=int, nargs="+", default=[20261018, 20261019])
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)
    df = prepare(args.datos)
    numeric = [c for c in NAMES if c in df.columns]
    lines = ["# Valor test del perfil con bloques", "",
             f"{len(df)} días de red del 25.06.2018 al 31.12.2025, {int(df['y_dia'].sum())} con avería; "
             f"{args.replicas} réplicas por bootstrap. Umbral {THRESHOLD}.", ""]
    lists = {}
    for seed in args.semillas:
        table = corrected(df, numeric, args.replicas, seed)
        table.round(4).to_csv(os.path.join(args.salida, f"vtest_bloques_{seed}.csv"), index=False)
        passing = set(table.loc[table["v_test"].abs() >= THRESHOLD, "variable"])
        weekly = set(table.loc[table["v_test_semanas"].abs() >= THRESHOLD, "variable"])
        lists[seed] = weekly
        lines += [f"## Semilla {seed}", "",
                  f"Pasan el umbral con catdes {len(passing)} variables; con bloques de siete días {len(weekly)}. "
                  f"Con las semanas el error es de {table['factor_semanas'].min():.2f} a "
                  f"{table['factor_semanas'].max():.2f} veces el de días sueltos. Dejan de pasar con las semanas: "
                  f"{', '.join(NAMES[v] for v in sorted(passing - weekly)) or 'ninguna'}.", "",
                  "| Variable | v-test | factor fechas | factor semanas | v-test semanas |", "| --- | --- | --- | --- | --- |"]
        lines += [f"| {r.nombre} | {r.v_test:.2f} | {r.factor_fechas:.2f} | {r.factor_semanas:.2f} | "
                  f"{r.v_test_semanas:.2f} |" for r in table.itertuples()]
        lines.append("")
    same = len({frozenset(v) for v in lists.values()}) == 1
    lines.append(f"La lista con bloques de siete días {'es la misma' if same else 'cambia'} con las "
                 f"{len(lists)} semillas.")
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.salida, "vtest_bloques.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
