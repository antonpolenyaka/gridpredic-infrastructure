"""
La L2 de la logística del modelo del día leída como prior gaussiana y su curva de validación entera con la previsión
real (memoria, 5.5 y 5.7.1; docs/investigacion-continua.md 3t). Misma evaluación que job_ml_riesgo_dia.py
(forecast_evaluation: variante operativa, cada año lo puntúa un modelo elegido en el último 25 % de los días anteriores
y reajustado con todos, con la previsión real de ICON-EU de la víspera) sobre la tabla que deja
experimentos/perfil_dia/exportar_dia.py (dia.csv.gz y bloques.json, que no se suben a git), como
experimentos/perfil_dia/codificacion.py, sin Spark:

    python curva_c.py <dia.csv.gz> <bloques.json> <config_ml.json> <salida/>

scikit-learn minimiza 0,5 ||b||^2 + C por la suma de la pérdida logística, que es la moda de la posterior con una prior
normal N(0, C) sobre cada coeficiente de la variable estandarizada (el intercepto no se penaliza): la desviación de la
prior es la raíz de C. Tres partes:

1. La C que elige la validación interna de cada año de 2022 a 2026, con la rejilla de producción (la de
   meteo_dia.busqueda o la de job_ml_riesgo_dia.py, {0,003; 0,01; 0,03; 0,1}) y con la rejilla ampliada por abajo con
   0,0003 y 0,001: la PR-AUC interna de cada valor, el tamaño de los coeficientes con la C elegida y la desviación de la
   prior (c_elegida.csv).
2. La curva con C fijas de 0,0003 a 1, con la previsión real de 2024 - 2025 y de 2024 - 2026: PR-AUC, días con avería
   en el 10 y el 20 % de días de más riesgo de cada año, pendiente e intercepto de calibración, BSS y la diferencia
   emparejada de PR-AUC frente a la rejilla de producción (bootstrap de 500 réplicas por bloques de siete días, semilla
   0), en curva_c.csv.
3. La rejilla ampliada en las mismas ventanas y con la misma diferencia, también en curva_c.csv.

Deja c_elegida.csv, curva_c.csv y curva_c.md. Tarda alrededor de medio minuto. El repositorio se busca en la variable
GRIDPREDIC_REPO o dos carpetas por encima de este fichero.
"""

import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "04_ml"))
sys.path.insert(0, os.path.join(REPO, "experimentos", "perfil_dia"))
import codificacion as cod  # noqa: E402
import job_ml_riesgo_dia as rd  # noqa: E402
import ml_metrics  # noqa: E402
from job_ml_horas_riesgo import rolling_evaluation  # noqa: E402

logging.basicConfig(level=logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)

EXTRA = [0.0003, 0.001]  # lo que añade la rejilla ampliada por abajo
FIXED = [0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 1.0]
WINDOWS = ([2024, 2025], [2024, 2025, 2026])
FOLDS = (2022, 2023, 2024, 2025, 2026)
REPS, SEED, BLOCK_DAYS = 500, 0, 7


def with_grid(cfg: dict, values: list) -> dict:
    """cfg con la rejilla de C de la logística cambiada."""
    return {**cfg, "busqueda": {**cfg["busqueda"], "logistica": [{"C": c} for c in values]}}


def capture(df: pd.DataFrame, score: str, frac: float) -> float:
    """Parte de los días con avería que caen en la fracción frac de días de más riesgo de cada año."""
    out = []
    for _, g in df.groupby("anio"):
        k = int(np.ceil(frac * len(g)))
        top = g.sort_values(score, ascending=False).head(k)
        out.append((top[rd.MAIN_LABEL].sum(), g[rd.MAIN_LABEL].sum()))
    a = np.array(out)
    return float(a[:, 0].sum() / a[:, 1].sum())


def chosen_c(pdf, blocks, config, cfg, variant, grids: dict) -> pd.DataFrame:
    """La C elegida en cada pliegue, la PR-AUC interna de cada valor y los coeficientes con la C elegida."""
    features = rd.variant_features(variant, blocks)
    blank = rd.blank_features(variant, features)
    rows = pdf[pdf["fecha"] >= pd.Timestamp(variant["desde"])]
    out = []
    for grid_name, values in grids.items():
        run = with_grid(cfg, values)
        for year in FOLDS:
            _, search, scorers, _ = rolling_evaluation(rows, features, rd.MAIN_LABEL, config, run, [year], False,
                                                       names=run["modelos"], blank=blank)
            scorer = scorers["logistica"]
            c = float(scorer.params["C"])
            coef = np.abs(np.asarray(scorer.estimator.named_steps["modelo"].coef_).ravel())
            for r in search:
                if r.get("modelo") != "logistica":
                    continue
                value = float(r["params"]["C"])
                out.append({"rejilla": grid_name, "anio": year, "C": value,
                            "pr_auc_interna": r["pr_auc_valid_muestra"], "elegida": value == c,
                            "coef_abs_media": coef.mean() if value == c else np.nan,
                            "coef_abs_max": coef.max() if value == c else np.nan,
                            "desviacion_prior": np.sqrt(value), "variables": len(features)})
    return pd.DataFrame(out)


def curve(pdf, blocks, config, cfg, variant, configurations: dict) -> pd.DataFrame:
    """PR-AUC, captura, calibración y diferencia frente a la rejilla de producción de cada configuración."""
    rows = []
    for years in WINDOWS:
        scores, found = {}, None
        for name, values in configurations.items():
            part = rd.forecast_evaluation(pdf, variant, blocks, config, with_grid(cfg, values), years,
                                          only_first=False)
            if found is None:
                found = part
            assert len(part) == len(found) and (part["fecha"].to_numpy() == found["fecha"].to_numpy()).all()
            scores[name] = part["p_logistica__prevision"].to_numpy()
        y = found[rd.MAIN_LABEL].to_numpy()
        _, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(y)), found["fecha"].to_numpy(), scores, REPS, SEED,
                                                  block_days=BLOCK_DAYS)
        diffs = ml_metrics.paired_differences(replicas, [(n, "produccion") for n in scores if n != "produccion"])
        diffs = diffs.set_index("a")
        for name, s in scores.items():
            c = ml_metrics.calibration(y, s)
            tmp = found[["anio", rd.MAIN_LABEL]].assign(s=s)
            d = diffs.loc[name] if name in diffs.index else None
            values = configurations[name]
            rows.append({"anios": f"{years[0]}-{years[-1]}", "configuracion": name,
                         "C": values[0] if len(values) == 1 else np.nan,
                         "rejilla": " ".join(f"{v:g}" for v in values), "dias": len(y),
                         "dias_con_averia": int(y.sum()), "pr_auc": ml_metrics.average_precision(y, s),
                         "captura_10": capture(tmp, "s", 0.1), "captura_20": capture(tmp, "s", 0.2),
                         "pendiente": c["pendiente"], "intercepto": c["intercepto"], "bss": c["bss"],
                         "dif_frente_produccion": np.nan if d is None else d["dif_p50"],
                         "dif_p2_5": np.nan if d is None else d["dif_p2_5"],
                         "dif_p97_5": np.nan if d is None else d["dif_p97_5"],
                         "prob_mejor": np.nan if d is None else d["prob_a_mejor"]})
    return pd.DataFrame(rows)


def md(frame: pd.DataFrame, digits: int = 4) -> str:
    shown = frame.copy()
    for col in shown.columns:
        if shown[col].dtype.kind == "f":
            shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else f"{v:.{digits}f}")
    lines = ["| " + " | ".join(shown.columns) + " |", "| " + " | ".join("---" for _ in shown.columns) + " |"]
    lines += ["| " + " | ".join(str(v) for v in r) + " |" for r in shown.itertuples(index=False)]
    return "\n".join(lines)


def summary(chosen: pd.DataFrame, table: pd.DataFrame, production: list, seconds: float) -> str:
    lines = ["# La L2 del modelo del día como prior y su curva de validación", "",
             f"Variante operativa con {int(chosen['variables'].iloc[0])} variables; rejilla de producción "
             f"{' '.join(f'{c:g}' for c in production)}; bootstrap de {REPS} réplicas por bloques de {BLOCK_DAYS} días "
             f"(semilla {SEED}). Tiempo: {seconds:.0f} s.", "", "## 1. La C que elige la validación interna", ""]
    picks = chosen[chosen["elegida"]].pivot(index="rejilla", columns="anio", values="C").reset_index()
    picks.columns = [str(c) for c in picks.columns]
    lines += [md(picks), ""]
    prod = chosen[chosen["rejilla"] == "produccion"]
    falls = all(np.all(np.diff(g.sort_values("C")["pr_auc_interna"].to_numpy()) < 0) for _, g in prod.groupby("anio"))
    sel = prod[prod["elegida"]]
    lines += [f"Con la rejilla de producción, la PR-AUC interna baja al subir C en todos los años: "
              f"{'sí' if falls else 'no'}. Con la C elegida, los coeficientes valen de media de "
              f"{sel['coef_abs_media'].min():.3f} a {sel['coef_abs_media'].max():.3f} en valor absoluto y "
              f"{sel['coef_abs_max'].max():.3f} como mucho; la desviación de la prior es "
              f"{', '.join(sorted({f'{v:.3f}' for v in sel['desviacion_prior']}))}.", "",
              "## 2. Curva con C fijas y rejillas (previsión real de la víspera)", ""]
    cols = ["configuracion", "rejilla", "pr_auc", "captura_10", "captura_20", "pendiente", "intercepto", "bss",
            "dif_frente_produccion", "dif_p2_5", "dif_p97_5", "prob_mejor"]
    for window, part in table.groupby("anios", sort=False):
        fixed = part[part["C"].notna()]
        best = fixed.loc[fixed["pr_auc"].idxmax()]
        lines += [f"### {window}: {int(part['dias'].iloc[0])} días, {int(part['dias_con_averia'].iloc[0])} con avería",
                  "", md(part[cols]), "",
                  f"Máximo con C fija en C = {best['C']:g} (PR-AUC {best['pr_auc']:.4f}, pendiente "
                  f"{best['pendiente']:.2f}); con C fijas de 0.01 a 1, PR-AUC de "
                  f"{fixed[fixed['C'] >= 0.01]['pr_auc'].min():.4f} a {fixed[fixed['C'] >= 0.01]['pr_auc'].max():.4f} "
                  f"y pendiente de {fixed[fixed['C'] >= 0.01]['pendiente'].max():.2f} a "
                  f"{fixed[fixed['C'] >= 0.01]['pendiente'].min():.2f}.", ""]
    return "\n".join(lines)


def main(argv) -> int:
    data, blocks_path, config_path, out = argv[:4]
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    cfg = {**cfg, "modelos": ["tasa_base", "logistica"], "purga_horas": float(cfg.get("purga_horas", 0))}
    pdf, blocks = cod.load(data, blocks_path)
    variant = next(v for v in cfg["variantes"] if v["nombre"] == "operativa")
    production = [float(p["C"]) for p in cfg["busqueda"]["logistica"]]
    wide = sorted(set(EXTRA + production))

    chosen = chosen_c(pdf, blocks, config, cfg, variant, {"produccion": production, "ampliada": wide})
    configurations = {"produccion": production, "ampliada": wide, **{f"fija_{c:g}": [c] for c in FIXED}}
    table = curve(pdf, blocks, config, cfg, variant, configurations)

    chosen.to_csv(os.path.join(out, "c_elegida.csv"), index=False, float_format="%.4f")
    table.to_csv(os.path.join(out, "curva_c.csv"), index=False, float_format="%.4f")
    text = summary(chosen, table, production, time.time() - t0)
    with open(os.path.join(out, "curva_c.md"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
