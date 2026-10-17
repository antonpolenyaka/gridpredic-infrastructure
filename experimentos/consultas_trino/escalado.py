"""
Cómo escala la lectura de Gold con los núcleos (speed-up y ley de Amdahl), para
la memoria (4.7.3 y 6.1). Solo lee dataset_train, la tabla más grande de Gold.
Dos cargas: una que casi solo recorre la tabla (recuento por mes) y otra que
calcula medias y desviaciones de 30 features por CT, con más cálculo por fila.
Se lanza dentro de spark-master una vez por número de núcleos:

    spark-submit --conf spark.cores.max=<n> --conf spark.executor.memory=6g escalado.py <n> /tmp/gp_planes/escalado_<n>.json

La primera pasada calienta la caché del sistema operativo y no cuenta; luego
dos pasadas de cada carga, de las que se guarda la menor.
"""

import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "l3_gold.dataset_train"
KEYS = {"distribuidora_id", "ct_id", "hora", "split", "fecha_mes", "en_muestra_train"}


def main(argv):
    cores, out = int(argv[0]), argv[1]
    spark = SparkSession.builder.appName(f"escalado_{cores}").enableHiveSupport().getOrCreate()
    df = spark.table(TABLE)
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

    timed(light)  # calienta la caché
    result = {"nucleos": cores, "columnas": numeric, "recorrido": [], "calculo": []}
    for _ in range(2):
        result["recorrido"].append(timed(light))
        result["calculo"].append(timed(heavy))
    result["recorrido_s"] = min(t for t, _ in result["recorrido"])
    result["calculo_s"] = min(t for t, _ in result["calculo"])
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=1)
    print(f"ESCALADO nucleos={cores} recorrido={result['recorrido_s']} s calculo={result['calculo_s']} s")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
