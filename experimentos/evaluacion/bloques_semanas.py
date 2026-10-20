"""
Cuánto cambian las diferencias emparejadas de PR-AUC de las pruebas del modelo del día (memoria, 5.3.3 y 5.7.6) si el
bootstrap sortea bloques de siete días seguidos en lugar de fechas sueltas. Misma evaluación que job_ml_riesgo_dia.py
(forecast_evaluation, previsión real de la víspera) sobre la tabla que deja experimentos/perfil_dia/exportar_dia.py,
como experimentos/perfil_dia/codificacion.py, sin Spark:

    python bloques_semanas.py <dia.csv.gz> <bloques.json> <config_ml.json> <salida/>

Las variantes son las tres de codificacion.py (sin la estación, la estación en indicadores, los avisos en
indicadores) y las de las pruebas de octubre que la tabla exportada permite repetir (fragilidad, extensión del viento
fuerte, interacciones, régimen, NAO y pendiente por red); las que no tienen sus columnas en la tabla se saltan con un
aviso. Deja bloques_semanas.csv con cada diferencia por fechas (bloque 1) y por semanas (bloque 7) y la razón de
anchuras de los dos intervalos y de qué lado del cero queda cada uno, y bloques_semanas.md con el resumen. Tarda alrededor de un minuto.
"""

import json
import logging
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "04_ml"))
sys.path.insert(0, os.path.join(REPO, "experimentos", "perfil_dia"))
import codificacion as cod  # noqa: E402
import job_ml_riesgo_dia as rd  # noqa: E402
import ml_metrics  # noqa: E402

logging.basicConfig(level=logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)

OCTOBER = ("operativa_fragilidad", "operativa_extension", "operativa_interacciones", "operativa_regimen",
           "operativa_nao", "operativa_pendiente_red")


def side(lo: float, hi: float) -> str:
    """Dónde queda el intervalo de la diferencia: encima del cero, debajo o incluyéndolo."""
    return "encima" if lo > 0 else "debajo" if hi < 0 else "incluye_0"


def main(argv) -> int:
    data, blocks_path, config_path, out = argv[:4]
    os.makedirs(out, exist_ok=True)
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    cfg = {**cfg, "modelos": ["tasa_base", "logistica"], "purga_horas": float(cfg.get("purga_horas", 0))}
    pdf, blocks = cod.load(data, blocks_path)
    by_name = {v["nombre"]: v for v in cfg["variantes"]}
    base = by_name["operativa"]
    wanted = cod.variants(base) + [by_name[n] for n in OCTOBER if n in by_name]
    rows = []
    for years in ([2024, 2025], [2024, 2025, 2026]):
        scores, found = {}, None
        for variant in wanted:
            try:
                part = rd.forecast_evaluation(pdf, variant, blocks, config, cfg, years, only_first=False)
            except (KeyError, ValueError) as exc:  # una variante sin sus columnas en la tabla exportada
                print(f"Sin {variant['nombre']}: {type(exc).__name__}")
                continue
            if found is None:
                found = part
            if len(part) != len(found) or not (part["fecha"].to_numpy() == found["fecha"].to_numpy()).all():
                print(f"Sin {variant['nombre']}: puntúa otros días")
                continue
            scores[variant["nombre"]] = part["p_logistica__prevision"].to_numpy()
        y = found[rd.MAIN_LABEL].to_numpy()
        by_block = {}
        for block in (1, 7):
            intervals, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(found)), found["fecha"].to_numpy(),
                                                              scores, int(cfg["bootstrap_reps"]),
                                                              int(config.get("semilla", 0)), block_days=block)
            diffs = ml_metrics.paired_differences(replicas, [(n, "operativa") for n in scores if n != "operativa"])
            by_block[block] = (intervals.set_index("nombre"), diffs.set_index("a"))
        for name in scores:
            row = {"anios": f"{years[0]}-{years[-1]}", "variante": name, "dias": len(found),
                   "dias_con_averia": int(y.sum()), "pr_auc": by_block[1][0].loc[name, "pr_auc"]}
            for block, (intervals, diffs) in by_block.items():
                row[f"pr_p2_5_b{block}"] = intervals.loc[name, "p2_5"]
                row[f"pr_p97_5_b{block}"] = intervals.loc[name, "p97_5"]
                if name in diffs.index:
                    row[f"dif_b{block}"] = diffs.loc[name, "dif_p50"]
                    row[f"dif_p2_5_b{block}"] = diffs.loc[name, "dif_p2_5"]
                    row[f"dif_p97_5_b{block}"] = diffs.loc[name, "dif_p97_5"]
                    row[f"gana_b{block}"] = diffs.loc[name, "prob_a_mejor"]
            if name != "operativa":
                row["razon_anchura"] = ((row["dif_p97_5_b7"] - row["dif_p2_5_b7"])
                                        / (row["dif_p97_5_b1"] - row["dif_p2_5_b1"]))
                row["lado_b1"] = side(row["dif_p2_5_b1"], row["dif_p97_5_b1"])
                row["lado_b7"] = side(row["dif_p2_5_b7"], row["dif_p97_5_b7"])
                row["decision_cambia"] = row["lado_b1"] != row["lado_b7"]
            rows.append(row)
    table = pd.DataFrame(rows)
    table.to_csv(os.path.join(out, "bloques_semanas.csv"), index=False)
    widths = table["razon_anchura"].dropna()
    flips = int(table["decision_cambia"].dropna().astype(bool).sum())
    lines = ["# Diferencias de las pruebas del día por fechas y por semanas", "",
             f"{int(widths.size)} diferencias frente a la operativa ({table['variante'].nunique() - 1} variantes en "
             f"2024 - 2025 y 2024 - 2026). Con bloques de siete días el intervalo mide entre {widths.min():.2f} y "
             f"{widths.max():.2f} veces lo que mide por fechas (mediana {widths.median():.2f}). Cambian de clase (intervalo "
             f"encima del cero, debajo o incluyéndolo) {flips}" + (": " + ", ".join(
                 f"{r.variante} en {r.anios} ({r.lado_b1} por fechas, {r.lado_b7} por semanas)"
                 for r in table[table["decision_cambia"].eq(True)].itertuples()) if flips else "")
             + ".", ""]
    shown = table[["anios", "variante", "pr_auc", "dif_b1", "dif_p2_5_b1", "dif_p97_5_b1", "dif_p2_5_b7",
                   "dif_p97_5_b7", "razon_anchura"]].round(3)
    lines += ["| " + " | ".join(shown.columns) + " |", "| " + " | ".join("---" for _ in shown.columns) + " |"]
    lines += ["| " + " | ".join("" if pd.isna(v) else str(v) for v in r) + " |" for r in shown.itertuples(index=False)]
    text = "\n".join(lines) + "\n"
    with open(os.path.join(out, "bloques_semanas.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
