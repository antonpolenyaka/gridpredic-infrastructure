"""
Cuántas aplicaciones tiene en marcha el master de Spark, según su API (/json/, campo activeapps). Ve también lo que
lanza Airflow, que no aparece al contar procesos spark-submit en spark-master. Se ejecuta dentro de spark-master:

    python3 apps_spark.py [http://localhost:8080]

Imprime el número (o -1 si el master no responde).
"""

import json
import sys
import urllib.request


def main(argv) -> int:
    url = (argv[0] if argv else "http://localhost:8080").rstrip("/") + "/json/"
    try:
        with urllib.request.urlopen(url, timeout=20) as reply:
            print(len(json.loads(reply.read().decode("utf-8")).get("activeapps", [])))
    except (OSError, ValueError):
        print(-1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
