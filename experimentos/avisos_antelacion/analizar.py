"""
Con cuanta antelacion llegan los avisos de AEMET que usamos: para cada aviso
(amarillo o mas, no cancelado) en vigor en alguna hora de un dia D en las
celdas de la red, las horas entre su emision y las 00:00 de D, y que parte
de los avisos de cada dia ya estaba emitida a las 20:00 de la vispera (lo que
usa el sistema), a las 12:00 de la vispera y dos dias antes. Lee
l2_silver.f_aviso_meteo y l3_gold.map_ct_celda en el lakehouse.

    spark-submit experimentos/avisos_antelacion/analizar.py [--desde 2018-06-20] [--salida /tmp/ml_resultados/avisos_antelacion]
"""

import argparse
import os
import sys

import pandas as pd


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde", default="2018-06-20")
    parser.add_argument("--salida", default="/tmp/ml_resultados/avisos_antelacion")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("avisos-antelacion").getOrCreate()
    cells = spark.table("l3_gold.map_ct_celda").where(F.col("celda_id").isNotNull()).select("distribuidora_id", "celda_id").distinct()
    w = spark.table("l2_silver.f_aviso_meteo").join(cells, "celda_id")
    w = w.where((F.col("nivel_num") >= 1) & (F.coalesce(F.col("tipo_mensaje"), F.lit("Alert")) != "Cancel") & (F.col("enviado_ts") >= F.lit(args.desde)))
    pdf = w.select("distribuidora_id", "celda_id", "fenomeno_codigo", "nivel_num", "enviado_ts", "inicio_ts", "fin_ts").toPandas()
    for c in ("enviado_ts", "inicio_ts", "fin_ts"):
        pdf[c] = pd.to_datetime(pdf[c])
    # One row per (distribuidora, day, phenomenon, level, sent): the days D the warning covers.
    rows = []
    for r in pdf.drop_duplicates(["distribuidora_id", "fenomeno_codigo", "nivel_num", "enviado_ts", "inicio_ts", "fin_ts"]).itertuples(index=False):
        day = r.inicio_ts.floor("D")
        while day < r.fin_ts:
            rows.append({"distribuidora_id": r.distribuidora_id, "fecha": day, "fenomeno": r.fenomeno_codigo, "nivel": int(r.nivel_num),
                         "horas_antes": (day - r.enviado_ts).total_seconds() / 3600.0})
            day += pd.Timedelta(days=1)
    d = pd.DataFrame(rows)
    # The first message of every (distribuidora, day, phenomenon): when the day was first warned.
    first = d.sort_values("horas_antes", ascending=False).groupby(["distribuidora_id", "fecha", "fenomeno"], as_index=False).first()
    lines = [f"Avisos de AEMET desde {args.desde} en las celdas de la red: {len(d)} aviso-días, {len(first)} (distribuidora, día, fenómeno).", "",
             "| fenómeno | días con aviso | emitido antes de las 20:00 de la víspera | antes de las 12:00 de la víspera | dos días antes o más | mediana de horas de antelación (primer mensaje) |",
             "| --- | --- | --- | --- | --- | --- |"]
    for phen, g in [("todos", first)] + list(first.groupby("fenomeno")):
        lines.append(f"| {phen} | {len(g)} | {(g['horas_antes'] >= 4).mean():.0%} | {(g['horas_antes'] >= 12).mean():.0%} | "
                     f"{(g['horas_antes'] >= 24).mean():.0%} | {g['horas_antes'].median():.0f} h |")
    text = "\n".join(lines) + "\n"
    print(text)
    os.makedirs(args.salida, exist_ok=True)
    with open(os.path.join(args.salida, "resumen.md"), "w", encoding="utf-8") as file:
        file.write(text)
    d.to_csv(os.path.join(args.salida, "aviso_dias.csv"), index=False)
    first.to_csv(os.path.join(args.salida, "primer_aviso.csv"), index=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
