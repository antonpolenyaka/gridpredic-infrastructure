"""
Análisis de componentes principales de las 16 variables meteorológicas de la variante operativa del modelo del día
(memoria, 5.3.5), con la tabla que deja exportar_dia.py:

    python acp_dia.py <dia.csv.gz> <bloques.json> <config_ml.json> <salida/> [--sin-evaluacion]

1. ACP normado de las 16 variables (las diez del día, las cuatro de extremos y la racha y la lluvia de ayer) sobre
   los días de red desde el 25.06.2018 sin huecos: valores propios, inercia acumulada, correlaciones de cada
   variable con las cuatro primeras componentes y las cargas rotadas con varimax en cuatro y cinco dimensiones. Lo
   mismo en la escala del modelo (logaritmo con signo antes de estandarizar).
2. Evaluación por años con la de producción (rolling_evaluation de job_ml_horas_riesgo.py, la rejilla de C de la
   logística y la previsión real de la víspera, como familias_dia.py): la logística de producción frente a la misma
   con las 16 variables sustituidas por sus 3, 4, 5 o 7 primeras componentes. Las componentes se ajustan dentro de
   cada entrenamiento, sobre la tubería de producción (mediana, logaritmo con signo y estandarización), así que no
   ven el año que puntúan. Es una evaluación por años, no una propuesta de cambio: la producción necesita la
   gemela de previsión de cada variable y su unidad física para los motivos.

Deja acp_inercia.csv, acp_correlaciones.csv, acp_varimax_4.csv, acp_varimax_5.csv, acp_evaluacion.csv y acp_dia.md.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import familias_dia as fd  # noqa: E402  (añade las familias nuevas a ml_models y trae la evaluación)

ml_models = fd.ml_models
rd = fd.rd

METEO = ["dia_racha_max", "dia_racha_media_max", "dia_viento_max", "dia_viento_media_max", "dia_lluvia_max",
         "dia_lluvia_media", "dia_lluvia_hora_max", "dia_temp_max", "dia_temp_media_min", "dia_humedad_media",
         "ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku", "ext_dias_calor", "ayer_racha_max",
         "ayer_lluvia_max"]
SINCE = "2018-06-25"
COMPONENTS = (3, 4, 5, 7)
METEO_INDEX = []  # posiciones de las 16 en la lista de variables de la variante, se fijan antes de evaluar


def eigen(Z: np.ndarray) -> tuple:
    R = np.corrcoef(Z, rowvar=False)
    values, vectors = np.linalg.eigh(R)
    order = np.argsort(values)[::-1]
    return values[order], vectors[:, order]


def varimax(L: np.ndarray, gamma: float = 1.0, steps: int = 100, tol: float = 1e-6) -> np.ndarray:
    p, k = L.shape
    rotation, total = np.eye(k), 0.0
    for _ in range(steps):
        rotated = L @ rotation
        u, s, vt = np.linalg.svd(L.T @ (rotated ** 3 - (gamma / p) * rotated @ np.diag(np.diag(rotated.T @ rotated))))
        rotation = u @ vt
        if s.sum() < total * (1 + tol):
            break
        total = s.sum()
    rotated = L @ rotation
    return rotated * np.where(rotated.sum(axis=0) < 0, -1.0, 1.0)  # el signo de un factor es arbitrario


def describe(df: pd.DataFrame) -> tuple:
    X = df[METEO].dropna()
    tables = {}
    rows = []
    for scale in ("estandarizada", "logaritmo con signo"):
        A = X.to_numpy(dtype=float)
        if scale != "estandarizada":
            A = np.sign(A) * np.log1p(np.abs(A))
        Z = (A - A.mean(0)) / A.std(0)
        values, vectors = eigen(Z)
        cumulative = np.cumsum(values) / values.sum()
        rows += [{"escala": scale, "componente": j + 1, "valor_propio": v, "inercia": v / values.sum(),
                  "inercia_acumulada": c} for j, (v, c) in enumerate(zip(values, cumulative))]
        if scale == "estandarizada":
            loadings = vectors * np.sqrt(values)
            tables["correlaciones"] = pd.DataFrame(loadings[:, :4], index=METEO,
                                                   columns=[f"CP{j}" for j in range(1, 5)])
            for k in (4, 5):
                tables[f"varimax_{k}"] = pd.DataFrame(varimax(loadings[:, :k]), index=METEO,
                                                      columns=[f"F{j}" for j in range(1, k + 1)])
    return pd.DataFrame(rows), tables, len(X), int(df.loc[X.index, "y_dia"].sum())


def register(k: int) -> str:
    name = f"logistica_acp{k}"

    def build(p, seed, k=k):
        from sklearn.compose import ColumnTransformer
        from sklearn.decomposition import PCA
        from sklearn.linear_model import LogisticRegression

        reduce = ColumnTransformer([("acp", PCA(n_components=k), list(METEO_INDEX))], remainder="passthrough")
        return fd.pipeline(LogisticRegression(C=float(p["C"]), max_iter=5000), extra=[("componentes", reduce)])

    fd.NEW[name] = {"build": build, "probability": True}
    fd.GRIDS[name] = [{"C": c} for c in (0.003, 0.01, 0.03, 0.1)]
    ml_models.TRAINED = tuple(ml_models.TRAINED) + (name,)
    return name


def evaluate(pdf, blocks, config_path) -> pd.DataFrame:
    names = [register(k) for k in COMPONENTS]
    models = ["tasa_base", "logistica"] + names
    config, cfg = fd.setup(config_path, models)
    features = rd.variant_features(fd.operativa(cfg), blocks)
    METEO_INDEX[:] = [features.index(c) for c in METEO]
    tables = []
    scored, _, _ = fd.run_reanalysis(pdf, blocks, config, cfg, [2022, 2023, 2024, 2025, 2026], models)
    t = fd.metrics(scored, {m: scored[f"p_{m}"].to_numpy() for m in models})
    tables.append(t.assign(banco="reanálisis 2022 - 2026"))
    for years in ([2024, 2025], [2024, 2025, 2026]):
        found = fd.run_forecast(pdf, blocks, config, cfg, years)
        t = fd.metrics(found, {m: found[f"p_{m}__prevision"].to_numpy() for m in models})
        tables.append(t.assign(banco=f"previsión real {years[0]} - {years[-1]}"))
    table = pd.concat(tables, ignore_index=True)
    return table[["banco"] + [c for c in table.columns if c != "banco"]]


def main(argv) -> int:
    parser = argparse.ArgumentParser(description="ACP de las variables meteorológicas del modelo del día")
    parser.add_argument("datos")
    parser.add_argument("bloques")
    parser.add_argument("config")
    parser.add_argument("salida")
    parser.add_argument("--sin-evaluacion", action="store_true", help="solo el ACP, sin la evaluación por años")
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)
    pdf, blocks = fd.load(args.datos, args.bloques)
    inertia, tables, n, positives = describe(pdf[pdf["fecha"] >= SINCE])
    inertia.to_csv(os.path.join(args.salida, "acp_inercia.csv"), index=False)
    for name, table in tables.items():
        table.round(4).to_csv(os.path.join(args.salida, f"acp_{name}.csv"))
    std = inertia[inertia["escala"] == "estandarizada"]
    kaiser = int((std["valor_propio"] > 1).sum())
    ninety = int(std.loc[std["inercia_acumulada"] >= 0.9, "componente"].min())
    lines = ["# Componentes principales de las variables meteorológicas del modelo del día", "",
             f"{n} días de red desde el 25.06.2018 con las 16 variables, {positives} con avería. Valores propios "
             f"{', '.join(f'{v:.2f}' for v in std['valor_propio'].head(5))}. Por encima de 1: {kaiser} componentes "
             f"con el {100 * std['inercia_acumulada'].iloc[kaiser - 1]:.1f} % de la inercia; el 90 % se pasa con "
             f"{ninety}.", "", "Cargas rotadas con varimax en cuatro dimensiones (las de 0,3 o más):", "",
             "| Variable | F1 | F2 | F3 | F4 |", "| --- | --- | --- | --- | --- |"]
    lines += [f"| {name} | " + " | ".join(f"{v:.2f}" if abs(v) >= 0.3 else "" for v in row) + " |"
              for name, row in tables["varimax_4"].iterrows()]
    if not args.sin_evaluacion:
        found = evaluate(pdf, blocks, args.config)
        found.to_csv(os.path.join(args.salida, "acp_evaluacion.csv"), index=False)
        lines += ["", "Evaluación por años con la de producción (PR-AUC, captura del 10 % y diferencia emparejada frente "
                  "a la logística por bloques de siete días):", "",
                  "| Banco | Modelo | PR-AUC | Captura 10 % | Diferencia | Intervalo |", "| --- | --- | --- | --- | --- | --- |"]
        for r in found.itertuples():
            gap = "" if pd.isna(r.dif) else f"{r.dif:+.3f}"
            ci = "" if pd.isna(r.dif) else f"{r.dif_p2_5:+.3f} a {r.dif_p97_5:+.3f}"
            lines.append(f"| {r.banco} | {r.modelo} | {r.pr_auc:.3f} | {r.captura_10:.3f} | {gap} | {ci} |")
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.salida, "acp_dia.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
