"""
Cómo entran la estación y los avisos en el modelo del día: la variante
operativa tal cual frente a la misma sin la estación, con la estación en
indicadores y con los avisos en indicadores (amarillo; naranja o rojo) en vez
del nivel 0 - 3. Para la memoria (5.3.3 y 5.7.6).

Usa la misma evaluación que job_ml_riesgo_dia.py (forecast_evaluation: cada año
lo puntúa un modelo que solo ha visto los días anteriores, con la previsión real
de ICON-EU de la víspera) sobre la tabla exportada con exportar_dia.py, así que
no necesita Spark:

    python codificacion.py dia.csv.gz bloques.json etl/config/04_ml/config_ml.json salida/

Deja codificacion.csv (PR-AUC de cada variante y diferencia emparejada frente a
la operativa con su intervalo del 95 %, bootstrap por fechas) para 2024 - 2025 y
para 2024 - 2026.
"""

import json
import logging
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "etl", "jobs", "04_ml"))
import job_ml_riesgo_dia as rd  # noqa: E402
import ml_metrics  # noqa: E402

logging.basicConfig(level=logging.WARNING)

SEASONS = {"est_primavera": 2.0, "est_verano": 3.0, "est_otono": 4.0}
WARNINGS = ["aviso_max_nivel", "aviso_viento_nivel", "aviso_lluvia_nivel", "aviso_tormenta_nivel", "aviso_nieve_nivel"]


def load(path: str, blocks_path: str) -> tuple:
    pdf = pd.read_csv(path, parse_dates=["fecha"])
    pdf["distribuidora_id"] = pdf["distribuidora_id"].astype("int64")
    pdf["anio"] = pdf["fecha"].dt.year
    pdf["ct_key"] = pdf["distribuidora_id"]
    pdf["hora"] = pdf["fecha"]
    pdf["split"] = "train"
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    # Indicadores de estación (invierno de referencia) y de avisos (sin aviso de referencia); nulo antes del archivo.
    for name, value in SEASONS.items():
        pdf[name] = (pdf["estacion"] == value).astype(float)
    for name in WARNINGS:
        level = pdf[name]
        pdf[f"{name}_amarillo"] = np.where(level.isna(), np.nan, (level == 1).astype(float))
        pdf[f"{name}_naranja_rojo"] = np.where(level.isna(), np.nan, (level >= 2).astype(float))
    calendar = [c for c in blocks["calendario"] if c != "estacion"]
    blocks["calendario_sin_estacion"] = calendar
    blocks["calendario_estacion_ind"] = calendar + list(SEASONS)
    blocks["avisos_ind"] = [f"{n}_{s}" for n in WARNINGS for s in ("amarillo", "naranja_rojo")]
    return pdf, blocks


def variants(base: dict) -> list:
    swap = lambda old, new: [new if b == old else b for b in base["bloques"]]  # noqa: E731
    return [
        base,
        {**base, "nombre": "sin_estacion", "bloques": swap("calendario", "calendario_sin_estacion")},
        {**base, "nombre": "estacion_indicadores", "bloques": swap("calendario", "calendario_estacion_ind")},
        {**base, "nombre": "avisos_indicadores", "bloques": swap("avisos", "avisos_ind")},
    ]


def main(argv) -> int:
    data, blocks_path, config_path, out = argv[:4]
    os.makedirs(out, exist_ok=True)
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    cfg = {**cfg, "modelos": ["tasa_base", "logistica"], "purga_horas": float(cfg.get("purga_horas", 0))}
    pdf, blocks = load(data, blocks_path)
    base = next(v for v in cfg["variantes"] if v["nombre"] == "operativa")
    rows = []
    for years in ([2024, 2025], [2024, 2025, 2026]):
        scores, found = {}, None
        for variant in variants(base):
            part = rd.forecast_evaluation(pdf, variant, blocks, config, cfg, years, only_first=False)
            if found is None:
                found = part
            assert len(part) == len(found) and (part["fecha"].to_numpy() == found["fecha"].to_numpy()).all()
            scores[variant["nombre"]] = part["p_logistica__prevision"].to_numpy()
        y = found[rd.MAIN_LABEL].to_numpy()
        intervals, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(found)), found["fecha"].to_numpy(), scores,
                                                          int(cfg["bootstrap_reps"]), int(config.get("semilla", 0)))
        diffs = ml_metrics.paired_differences(replicas, [(n, "operativa") for n in scores if n != "operativa"])
        diffs = diffs.set_index("a")
        for _, r in intervals.iterrows():
            d = diffs.loc[r["nombre"]] if r["nombre"] in diffs.index else None
            rows.append({"anios": f"{years[0]}-{years[-1]}", "variante": r["nombre"], "dias": len(found),
                         "dias_con_averia": int(y.sum()), "pr_auc": r["pr_auc"],
                         "dif_frente_operativa": None if d is None else d["dif_p50"],
                         "dif_p2_5": None if d is None else d["dif_p2_5"],
                         "dif_p97_5": None if d is None else d["dif_p97_5"],
                         "prob_mejor": None if d is None else d["prob_a_mejor"]})
    table = pd.DataFrame(rows)
    table.to_csv(os.path.join(out, "codificacion.csv"), index=False)
    print(table.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
