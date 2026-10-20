"""
Cómo escala la lectura de Gold con los núcleos (speed-up, ley de Amdahl y ley universal de escalabilidad) y con los
datos, para la memoria (4.7.3 y 6.1). Solo lee dataset_train, la tabla más grande de Gold. Dos cargas: una que casi
solo recorre la tabla (recuento por mes) y otra que calcula medias y desviaciones de 30 features por CT, con más
cálculo por fila. Se lanza dentro de spark-master una vez por número de núcleos (lanzar_escalado.ps1 hace la serie):

    spark-submit --conf spark.cores.max=<n> --conf spark.executor.memory=6g escalado.py <n> <salida.json>
        [--pasadas 3] [--meses 1.0]

Una pasada de cada carga calienta la caché del sistema operativo y el plan de las agregaciones y no cuenta; luego
--pasadas pasadas de cada carga, de las que se guardan todas, la menor y la dispersión (máximo entre mínimo). Hasta el
20.10.2026 solo se calentaba el recuento, y la primera medida del cálculo salía entre un 4 y un 30 % más lenta que la
menor de las otras dos. Con --meses por debajo de 1 lee solo esa parte de los meses de la tabla (los primeros, por
fecha_mes), para medir cómo crece el tiempo con los datos. ajustar_escalado.py junta los JSON y ajusta los modelos.
"""

import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "l3_gold.dataset_train"
KEYS = {"distribuidora_id", "ct_id", "hora", "split", "fecha_mes", "en_muestra_train"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Escalado de la lectura de dataset_train con los núcleos y los datos")
    parser.add_argument("nucleos", type=int)
    parser.add_argument("salida")
    parser.add_argument("--pasadas", type=int, default=3)
    parser.add_argument("--meses", type=float, default=1.0, help="parte de los meses que se leen (0 - 1]")
    args = parser.parse_args(argv)
    spark = SparkSession.builder.appName(f"escalado_{args.nucleos}_{args.meses:g}").enableHiveSupport().getOrCreate()
    df = spark.table(TABLE)
    months = sorted(r[0] for r in df.select("fecha_mes").distinct().collect())
    kept = months
    if args.meses < 1:
        kept = months[:max(1, round(len(months) * args.meses))]
        df = df.where(F.col("fecha_mes").isin(kept))
    numeric = [f.name for f in df.schema.fields
               if f.dataType.typeName() in ("double", "float") and f.name not in KEYS][:30]

    def light():
        return df.groupBy("fecha_mes").count().collect()

    def heavy():
        aggs = [F.avg(c).alias(f"m_{i}") for i, c in enumerate(numeric)]
        aggs += [F.stddev(c).alias(f"s_{i}") for i, c in enumerate(numeric)]
        return df.groupBy("distribuidora_id", "ct_id").agg(*aggs).collect()

    def timed(fn):
        start = time.time()
        rows = fn()
        return round(time.time() - start, 2), len(rows)

    warm = light()  # calienta la caché y da las filas leídas
    heavy()  # y el plan de las 60 agregaciones, que en la primera pasada medida costaba de más
    result = {"nucleos": args.nucleos, "meses": args.meses, "meses_leidos": len(kept), "meses_tabla": len(months),
              "filas": int(sum(r["count"] for r in warm)), "columnas": numeric, "calentamiento": ["recorrido", "calculo"],
              "recorrido": [], "calculo": []}
    for _ in range(args.pasadas):
        result["recorrido"].append(timed(light))
        result["calculo"].append(timed(heavy))
    for load in ("recorrido", "calculo"):
        times = [t for t, _ in result[load]]
        result[f"{load}_s"] = min(times)
        result[f"{load}_dispersion"] = round(max(times) / min(times) - 1, 3) if min(times) > 0 else None
    with open(args.salida, "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=1)
    print(f"ESCALADO nucleos={args.nucleos} meses={args.meses:g} filas={result['filas']} "
          f"recorrido={result['recorrido_s']} s calculo={result['calculo_s']} s "
          f"dispersion_calculo={result['calculo_dispersion']}")
    spark.stop()


if __name__ == "__main__":
    main()
