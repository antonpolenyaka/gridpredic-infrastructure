"""
Perfil de los días por causa de la avería (memoria, 5.8.1; docs/causas-incidencias.md): catdes con "día de red con
alguna incidencia de la causa" como clase, sobre la tabla de días y tiempo de job_causas_incidencias.py:

    python perfil_causas.py <causas_dias_meteo.csv> <salida/> [--replicas 1000] [--semilla 20261019]

Para cada causa local y cada variable del tiempo del día (racha, lluvia máxima y media, mínima y máxima, CAPE y días
secos antes), el valor test de catdes y el mismo valor corregido con un bootstrap por bloques de siete fechas
seguidas, como vtest_bloques.py: el cociente entre el error con bloques y el de días sueltos. Las modalidades de la
estación y de la red, con su valor test hipergeométrico.

Regla para citar una cifra: solo causas con 40 días o más y variables con valor test de 3,29 o más también con
bloques. Deja perfil_causas.csv, perfil_causas_modalidades.csv y perfil_causas.md.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from perfil import catdes, vtest_numeric  # noqa: E402

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
WEATHER = {"racha_max_kmh": "Racha máxima (km/h)", "lluvia_max_mm": "Lluvia del día, peor celda (mm)",
           "lluvia_media_mm": "Lluvia del día, media de la red (mm)", "tmin_c": "Temperatura mínima (°C)",
           "tmax_c": "Temperatura máxima (°C)", "cape_max_jkg": "CAPE máximo (J/kg)",
           "dias_secos_antes": "Días secos antes"}
CAUSES = ["meteorologia", "equipo", "desconocida", "fauna", "aguas_arriba"]
THRESHOLD, MIN_DAYS = 3.29, 40


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["dia"])
    month = df["dia"].dt.month
    df["estacion"] = np.select([month.isin([12, 1, 2]), month.isin([3, 4, 5]), month.isin([6, 7, 8])],
                               ["invierno", "primavera", "verano"], "otoño")
    df["red"] = df["distribuidora_id"].map(NETWORKS)
    if df["red"].isna().any():
        raise ValueError("Hay distribuidoras sin nombre de la memoria")
    return df.drop(columns=["distribuidora_id"])


def main(argv) -> int:
    parser = argparse.ArgumentParser(description="Perfil de los días por causa con bloques de siete días")
    parser.add_argument("datos")
    parser.add_argument("salida")
    parser.add_argument("--replicas", type=int, default=1000)
    parser.add_argument("--semilla", type=int, default=20261019)
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)
    df = load(args.datos)
    dates = np.sort(df["dia"].unique())
    week = np.searchsorted(dates, df["dia"].to_numpy()) // 7
    weeks = [np.flatnonzero(week == w) for w in np.unique(week)]
    rng = np.random.default_rng(args.semilla)
    X = {c: df[c].to_numpy(dtype=float) for c in WEATHER}

    def diff(x, y):
        ok = ~np.isnan(x)
        return x[ok & (y == 1)].mean() - x[ok & (y == 0)].mean()

    rows, modalities = [], []
    for cause in CAUSES:
        y = (df[cause] > 0).astype(int).to_numpy()
        draws = {"dias": {c: [] for c in WEATHER}, "semanas": {c: [] for c in WEATHER}}
        for _ in range(args.replicas):
            loose = rng.integers(0, len(df), len(df))
            blocks = np.concatenate([weeks[k] for k in rng.integers(0, len(weeks), len(weeks))])
            for c in WEATHER:
                draws["dias"][c].append(diff(X[c][loose], y[loose]))
                draws["semanas"][c].append(diff(X[c][blocks], y[blocks]))
        for c in WEATHER:
            found = vtest_numeric(df[c], pd.Series(y))
            factor = np.nanstd(draws["semanas"][c], ddof=1) / np.nanstd(draws["dias"][c], ddof=1)
            rows.append({"causa": cause, "dias_con_causa": int(y.sum()), "variable": c, "nombre": WEATHER[c],
                         "v_test": found["v_test"], "factor_semanas": factor,
                         "v_test_semanas": found["v_test"] / factor, "media_clase": found["media_clase"],
                         "media_resto": found["media_resto"]})
        _, _, mods = catdes(df.assign(y=y), "y", ["racha_max_kmh"], ["estacion", "red"])
        modalities.append(mods.assign(causa=cause))
    table = pd.DataFrame(rows)
    table["se_cita"] = ((table["dias_con_causa"] >= MIN_DAYS) & (table["v_test"].abs() >= THRESHOLD)
                        & (table["v_test_semanas"].abs() >= THRESHOLD))
    table.round(4).to_csv(os.path.join(args.salida, "perfil_causas.csv"), index=False)
    mods = pd.concat(modalities, ignore_index=True)
    mods.round(4).to_csv(os.path.join(args.salida, "perfil_causas_modalidades.csv"), index=False)

    lines = ["# Perfil de los días por causa", "",
             f"{len(df)} días de red; {args.replicas} réplicas por bloques de siete fechas (semilla {args.semilla}). "
             f"Se citan las variables de causas con {MIN_DAYS} días o más y valor test de {THRESHOLD} o más también "
             "con bloques.", ""]
    for cause, part in table.groupby("causa", sort=False):
        cited = part[part["se_cita"]].sort_values("v_test_semanas", key=np.abs, ascending=False)
        text = ", ".join(f"{r.nombre} ({r.v_test:.1f}; con bloques {r.v_test_semanas:.1f})" for r in cited.itertuples())
        lines.append(f"- {cause} ({int(part['dias_con_causa'].iloc[0])} días): {text or 'ninguna variable'}.")
    crossed = table[(table["v_test"].abs() >= THRESHOLD) & (table["v_test_semanas"].abs() < THRESHOLD)]
    lines += ["", f"Pasan el umbral con catdes y dejan de pasarlo con bloques: {len(crossed)}.", "",
              "| Causa | Variable | v-test | factor | v-test con bloques | media con la causa | media sin ella |",
              "| --- | --- | --- | --- | --- | --- | --- |"]
    lines += [f"| {r.causa} | {r.nombre} | {r.v_test:.2f} | {r.factor_semanas:.2f} | {r.v_test_semanas:.2f} | "
              f"{r.media_clase:.2f} | {r.media_resto:.2f} |" for r in table.itertuples()]
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.salida, "perfil_causas.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
