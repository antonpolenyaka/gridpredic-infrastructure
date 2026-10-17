"""
La curva de fragilidad de las redes (cuaderno 3o), para la memoria: la parte
de días con avería según la racha frente al p98 de la celda, por tramos y con
la curva isotónica que usa el bloque fragilidad de job_ml_riesgo_dia.py,
ajustada con los días anteriores a fragilidad_hasta y comprobada con los de
después. Se lanza dentro de spark-master:

    spark-submit --py-files <los de job_ml_riesgo_dia> curva_fragilidad.py /app/config/04_ml/config_ml.json /tmp/fragilidad

Deja tramos.csv (tramo, días y tasa antes y después del corte) y curva.csv
(la curva isotónica en una rejilla de rachas).
"""

import json
import os
import sys

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

sys.path.insert(0, "/app/jobs/04_ml")
import job_ml_riesgo_dia as rd  # noqa: E402

EDGES = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.4, 1.7, 5.0]


def main(argv):
    config_path, out = argv[0], argv[1]
    os.makedirs(out, exist_ok=True)
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    cfg["hasta"] = "2026-08-03"
    spark = SparkSession.builder.appName("curva_fragilidad").enableHiveSupport().getOrCreate()
    pdf, _, _ = rd.build_dataset(spark, config, cfg)
    until = pd.Timestamp(cfg.get("fragilidad_hasta", "2022-01-01"))
    days = pdf.dropna(subset=["ext_racha_clima_max", rd.MAIN_LABEL]).copy()
    days["tramo"] = pd.cut(days["ext_racha_clima_max"], EDGES, right=False)
    days["periodo"] = np.where(days["fecha"] < until, "ajuste", "despues")
    table = (days.groupby(["tramo", "periodo"], observed=True)[rd.MAIN_LABEL].agg(["size", "mean"]).unstack("periodo"))
    table.columns = [f"{a}_{b}" for a, b in table.columns]
    table = table.reset_index()
    table["desde"] = [i.left for i in table["tramo"]]
    table["hasta"] = [i.right for i in table["tramo"]]
    table.drop(columns="tramo").to_csv(os.path.join(out, "tramos.csv"), index=False)
    grid = pd.DataFrame({"ext_racha_clima_max": np.round(np.arange(0.3, 2.01, 0.01), 2),
                         "fecha": until - pd.Timedelta(days=1)})
    fit = days[days["fecha"] < until][["fecha", "ext_racha_clima_max", rd.MAIN_LABEL]]
    both = pd.concat([fit, grid], ignore_index=True)
    rd.add_fragility(both, {}, until)
    both.iloc[len(fit):][["ext_racha_clima_max", "frag_prob"]].to_csv(os.path.join(out, "curva.csv"), index=False)
    print(table.to_string())
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
