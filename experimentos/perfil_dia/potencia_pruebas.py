"""
Qué diferencia de PR-AUC podía ver la regla de entrada de las pruebas de octubre (memoria 5.7.6 y 5.9).

Repite la evaluación del modelo del día (job_ml_riesgo_dia.forecast_evaluation: cada año lo puntúa un modelo que
solo ha visto los días anteriores, con la previsión real de la víspera) para la variante operativa y las variantes
que se probaron frente a ella, con el bootstrap de 500 réplicas por fechas sueltas (bloque de 1 día) y por bloques
de 7 días, y saca para cada una la diferencia emparejada con su intervalo del 95 %. De la anchura de ese intervalo
sale la diferencia mínima detectable con un 80 % de potencia y un 5 % de dos colas: 2,8 desviaciones de la
diferencia entre réplicas, con la desviación estimada como la anchura del intervalo del 95 % entre 3,92. La mediana
de esa cifra entre las variantes es la que cita la memoria (por fechas, como se midió la tabla de 5.7.6: unas 0,021
con 58 días con avería y 0,018 con 84).

Sin Spark, sobre la tabla exportada con exportar_dia.py, como codificacion.py:

    python potencia_pruebas.py <repo> <dia.csv.gz> <bloques.json> <carpeta de salida>

Escribe potencia_pruebas.csv (una fila por años, bloque y variante) y potencia_pruebas.txt (el resumen).
"""

import json
import logging
import os
import sys

import numpy as np
import pandas as pd

repo, data, blocks_path, out = sys.argv[1:5]
sys.path.insert(0, os.path.join(repo, "etl", "jobs", "04_ml"))
sys.path.insert(0, os.path.join(repo, "experimentos", "perfil_dia"))
logging.basicConfig(level=logging.ERROR)
import job_ml_riesgo_dia as rd  # noqa: E402
import ml_metrics  # noqa: E402
import codificacion as cod  # noqa: E402

os.makedirs(out, exist_ok=True)
config = json.load(open(os.path.join(repo, "etl", "config", "04_ml", "config_ml.json"), encoding="utf-8"))
cfg = rd.settings(config)
cfg = {**cfg, "modelos": ["tasa_base", "logistica"], "purga_horas": float(cfg.get("purga_horas", 0))}
pdf, blocks = cod.load(data, blocks_path)
byname = {v["nombre"]: v for v in cfg["variantes"]}
base = byname["operativa"]
wanted = [base] + [v for v in cod.variants(base)[1:]]
for name in ("operativa_fragilidad", "operativa_extension", "operativa_interacciones", "operativa_regimen",
             "operativa_nao", "operativa_pendiente_red"):
    if name in byname:
        wanted.append(byname[name])
rows = []
for years in ([2024, 2025], [2024, 2025, 2026]):
    scores, found = {}, None
    for variant in wanted:
        try:
            part = rd.forecast_evaluation(pdf, variant, blocks, config, cfg, years, only_first=False)
        except Exception as exc:  # una variante sin sus columnas en la tabla exportada
            print("sin", variant["nombre"], type(exc).__name__, str(exc)[:120])
            continue
        if found is None:
            found = part
        if len(part) != len(found) or not (part["fecha"].to_numpy() == found["fecha"].to_numpy()).all():
            print("otras filas", variant["nombre"])
            continue
        scores[variant["nombre"]] = part["p_logistica__prevision"].to_numpy()
    y = found[rd.MAIN_LABEL].to_numpy()
    for block in (1, 7):
        intervals, replicas = ml_metrics.bootstrap_pr_auc(y, np.ones(len(found)), found["fecha"].to_numpy(), scores,
                                                          500, int(config.get("semilla", 0)), block_days=block)
        diffs = ml_metrics.paired_differences(replicas, [(n, "operativa") for n in scores if n != "operativa"])
        diffs = diffs.set_index("a")
        for _, r in intervals.iterrows():
            d = diffs.loc[r["nombre"]] if r["nombre"] in diffs.index else None
            rows.append({"anios": f"{years[0]}-{years[-1]}", "bloque_dias": block, "variante": r["nombre"],
                         "dias": len(found), "dias_con_averia": int(y.sum()), "pr_auc": r["pr_auc"],
                         "pr_p2_5": r["p2_5"], "pr_p97_5": r["p97_5"],
                         "dif": None if d is None else d["dif_p50"], "dif_p2_5": None if d is None else d["dif_p2_5"],
                         "dif_p97_5": None if d is None else d["dif_p97_5"],
                         "gana": None if d is None else d["prob_a_mejor"]})
table = pd.DataFrame(rows)
table["minima_detectable"] = 2.8 * (table["dif_p97_5"] - table["dif_p2_5"]) / 3.92
table.to_csv(os.path.join(out, "potencia_pruebas.csv"), index=False)

lines = []
pd.set_option("display.width", 220)
lines.append(table.round(4).to_string(index=False))
lines.append("")
lines.append("Diferencia mínima detectable (80 % de potencia, 5 % de dos colas) por años y bloque: mediana entre las "
             "variantes, mínimo y máximo")
res = table.dropna(subset=["minima_detectable"]).groupby(["anios", "bloque_dias", "dias_con_averia"])["minima_detectable"]
for (anios, block, averias), s in res:
    lines.append(f"  {anios}, bloques de {block} día(s), {averias} días con avería: mediana {s.median():.3f}, "
                 f"de {s.min():.3f} a {s.max():.3f} (de {len(s)} variantes)")
lines.append("")
lines.append("Diferencias medidas (por fechas) frente a la operativa, con su intervalo del 95 %:")
for _, r in table[(table["bloque_dias"] == 1) & table["dif"].notna()].iterrows():
    lines.append(f"  {r['anios']} {r['variante']}: {r['dif']:+.3f} [{r['dif_p2_5']:+.3f}, {r['dif_p97_5']:+.3f}]")
text = "\n".join(lines)
print(text)
open(os.path.join(out, "potencia_pruebas.txt"), "w", encoding="utf-8").write(text + "\n")
