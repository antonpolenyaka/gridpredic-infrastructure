"""
Exporta la tabla del modelo del día (una fila por red y día, con sus etiquetas y
todas las variables candidatas) para el perfil de los días con avería de la
memoria (5.3.5). La tabla no se guarda en Gold: la construye build_dataset de
job_ml_riesgo_dia.py en cada ejecución, así que aquí se llama igual que en
experimentos/fragilidad/curva_fragilidad.py. Se lanza dentro de spark-master:

    spark-submit --py-files <los de job_ml_riesgo_dia> exportar_dia.py /app/config/04_ml/config_ml.json /tmp/perfil_dia [hasta]

Deja dia.csv.gz (CSV para no depender de pyarrow fuera del contenedor),
bloques.json (variables de cada bloque) y resumen.csv (días y días con avería por
año y red). El fichero fila a fila no se sube a git: el perfil y los gráficos los
saca correr_perfil.py.
"""

import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/app/jobs/04_ml")
import job_ml_riesgo_dia as rd  # noqa: E402


def main(argv):
    config_path, out = argv[0], argv[1]
    os.makedirs(out, exist_ok=True)
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    # Hasta agosto de 2026, como la curva de fragilidad: el perfil usa hasta 2025 y 2026 queda para comprobar.
    cfg["hasta"] = argv[2] if len(argv) > 2 else "2026-08-03"
    spark = SparkSession.builder.appName("perfil_dia").enableHiveSupport().getOrCreate()
    pdf, blocks, warnings_first = rd.build_dataset(spark, config, cfg)
    keys = ["distribuidora_id", "fecha", "anio"] + rd.LABELS + ["n_dia"]
    features = sorted({name for names in blocks.values() for name in names} - set(keys))
    columns = [c for c in keys + features if c in pdf.columns]
    pdf[columns].to_csv(os.path.join(out, "dia.csv.gz"), index=False)
    with open(os.path.join(out, "bloques.json"), "w", encoding="utf-8") as handle:
        json.dump({"bloques": blocks, "avisos_desde": str(warnings_first) if warnings_first is not None else None,
                   "hasta": cfg["hasta"], "dias_excluidos": cfg.get("dias_excluidos", [])},
                  handle, ensure_ascii=False, indent=1)
    summary = rd.describe(pdf)
    summary.to_csv(os.path.join(out, "resumen.csv"), index=False)
    print(f"Filas {len(pdf)}, columnas {len(columns)}")
    print(summary.to_string())
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
