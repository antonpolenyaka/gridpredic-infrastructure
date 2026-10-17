"""
Planes y tiempos de las consultas de la memoria (4.7.3) en Trino, para comparar
cada consulta antes y después de cada cambio: la misma consulta con el filtro de
la columna de partición, las estadísticas de ANALYZE, otro reparto de los joins
y la compactación y el orden Z de dataset_train (compactar_dataset.py).

Habla con Trino por su API REST, sin dependencias, desde un contenedor de la red
de compose (spark-master):

    python3 planes.py --fase A --consultas c1a,c2a --salida /tmp/gp_planes [--sesion join_distribution_type=PARTITIONED]
    python3 planes.py --fase C --analyze --salida /tmp/gp_planes

De cada consulta: una pasada (fría si Trino se acaba de reiniciar), cinco más
(caliente), EXPLAIN (TYPE IO, FORMAT JSON), EXPLAIN (TYPE DISTRIBUTED), EXPLAIN
ANALYZE y, si la tiene, la estimación de filas de su filtro (SHOW STATS). De cada
pasada guarda lo que mide el propio servidor (/v1/query/<id>): tiempo total y de
planificación, CPU, splits, filas y bytes leídos y memoria máxima, también por
operador. Deja un JSON por consulta y fase en la carpeta de salida; los resume
resumir_planes.py.

Solo lee, salvo --analyze, que escribe las estadísticas extendidas de Trino
(número de valores distintos y tamaño por columna) junto al log de cada tabla
sin tocar sus datos.
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request

TRINO = os.environ.get("TRINO_URL", "http://trino:8080")
USER = "gp_planes"
T = "lakehouse.l3_gold"
MES = "hora >= TIMESTAMP '2025-01-01 00:00:00' AND hora < TIMESTAMP '2025-02-01 00:00:00'"
MES_D = MES.replace("hora", "d.hora")

# distribuidora_id: 3 es la Distribuidora 1, 2 la Distribuidora 2 y 1366 la Distribuidora 3.
JOIN = (f"SELECT /* gp_{{n}} */ d.distribuidora_id, count(*) AS horas_ct, "
        f"count_if(m.met_racha_clima_1h > 1) AS horas_racha_sobre_p98, sum(d.y_1_3h) AS positivos, "
        f"sum(CASE WHEN m.met_racha_clima_1h > 1 THEN d.y_1_3h ELSE 0 END) AS positivos_con_racha "
        f"FROM {T}.dataset_train d "
        f"JOIN {T}.map_ct_celda c ON c.distribuidora_id = d.distribuidora_id AND c.ct_id = d.ct_id "
        f"JOIN {T}.meteo_celda_hora m ON m.celda_id = c.celda_id AND m.hora = d.hora "
        f"WHERE {MES_D}{{extra}} GROUP BY d.distribuidora_id ORDER BY d.distribuidora_id")

CONSULTAS = {
    # Recuento por conjunto: recorre la tabla entera, solo con la columna de partición split.
    "c1a": f"SELECT /* gp_c1a */ split, count(*) AS filas FROM {T}.dataset_train GROUP BY split ORDER BY split",
    # La misma respuesta desde dataset_versions, que la guarda al construir cada versión del dataset.
    "c1c": f"SELECT /* gp_c1c */ dataset_version, filas_json FROM {T}.dataset_versions ORDER BY creado_ts DESC LIMIT 1",
    # Un mes de la Distribuidora 1: como se midió el 16.10.2026 (solo hora) y con la partición fecha_mes.
    "c2a": f"SELECT /* gp_c2a */ count(*) AS filas, sum(y_1_3h) AS positivos FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 3 AND {MES}",
    "c2b": f"SELECT /* gp_c2b */ count(*) AS filas, sum(y_1_3h) AS positivos FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 3 AND {MES} AND fecha_mes = DATE '2025-01-01'",
    # Lo mismo para la Distribuidora 3, la red pequeña, donde ordenar los ficheros por red debería notarse más.
    "c2c": f"SELECT /* gp_c2c */ count(*) AS filas, sum(y_1_3h) AS positivos FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 1366 AND {MES}",
    "c2d": f"SELECT /* gp_c2d */ count(*) AS filas, sum(y_1_3h) AS positivos FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 1366 AND {MES} AND fecha_mes = DATE '2025-01-01'",
    # Racha máxima por celda: toda la tabla (dos columnas), un mes por hora y un mes con la partición anio.
    "c3a": f"SELECT /* gp_c3a */ celda_id, max(met_racha_clima_1h) AS racha_max FROM {T}.meteo_celda_hora "
           f"GROUP BY celda_id ORDER BY 2 DESC LIMIT 5",
    "c3b": f"SELECT /* gp_c3b */ celda_id, max(met_racha_clima_1h) AS racha_max FROM {T}.meteo_celda_hora "
           f"WHERE {MES} GROUP BY celda_id ORDER BY 2 DESC LIMIT 5",
    "c3c": f"SELECT /* gp_c3c */ celda_id, max(met_racha_clima_1h) AS racha_max FROM {T}.meteo_celda_hora "
           f"WHERE {MES} AND anio = 2025 GROUP BY celda_id ORDER BY 2 DESC LIMIT 5",
    # El riesgo de la última tarde: el coste fijo de planificar y leer metadatos.
    "c4a": f"SELECT /* gp_c4a */ * FROM {T}.riesgo_dia_siguiente ORDER BY 1 DESC LIMIT 3",
    # El join del entrenamiento (dataset_train con la meteorología de su celda) para enero de 2025.
    "c5a": JOIN.format(n="c5a", extra=""),
    "c5b": JOIN.format(n="c5b", extra=" AND d.fecha_mes = DATE '2025-01-01' AND m.anio = 2025"),
    # Con un filtro selectivo en la tabla pequeña: el filtrado dinámico manda sus claves a la lectura de la grande.
    "c5c": JOIN.format(n="c5c", extra=" AND d.fecha_mes = DATE '2025-01-01' AND m.anio = 2025 AND c.distribuidora_id = 1366"),
    # Ficheros y bytes de cada partición, con las columnas ocultas del conector de Delta.
    "ficheros_dataset": f"SELECT /* gp_fd */ split, fecha_mes, count(*) AS ficheros, sum(tam) AS bytes FROM "
                        f"(SELECT DISTINCT split, fecha_mes, \"$path\" AS p, \"$file_size\" AS tam FROM {T}.dataset_train) "
                        f"GROUP BY split, fecha_mes ORDER BY fecha_mes, split",
    "ficheros_meteo": f"SELECT /* gp_fm */ anio, count(*) AS ficheros, sum(tam) AS bytes FROM "
                      f"(SELECT DISTINCT anio, \"$path\" AS p, \"$file_size\" AS tam FROM {T}.meteo_celda_hora) "
                      f"GROUP BY anio ORDER BY anio",
}

# Consultas de inventario: una sola pasada, sin planes.
INVENTARIO = {"c1c", "ficheros_dataset", "ficheros_meteo"}

# La estimación de filas del filtro de cada consulta, para compararla con las reales de EXPLAIN ANALYZE.
ESTIMACION = {
    "c2a": f"SHOW STATS FOR (SELECT distribuidora_id, hora, y_1_3h FROM {T}.dataset_train WHERE distribuidora_id = 3 AND {MES})",
    "c2b": f"SHOW STATS FOR (SELECT distribuidora_id, hora, y_1_3h FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 3 AND {MES} AND fecha_mes = DATE '2025-01-01')",
    "c2c": f"SHOW STATS FOR (SELECT distribuidora_id, hora, y_1_3h FROM {T}.dataset_train WHERE distribuidora_id = 1366 AND {MES})",
    "c2d": f"SHOW STATS FOR (SELECT distribuidora_id, hora, y_1_3h FROM {T}.dataset_train "
           f"WHERE distribuidora_id = 1366 AND {MES} AND fecha_mes = DATE '2025-01-01')",
}

ANALYZE = [
    f"SHOW STATS FOR {T}.map_ct_celda",
    f"SHOW STATS FOR {T}.meteo_celda_hora",
    f"SHOW STATS FOR {T}.dataset_train",
    f"ANALYZE {T}.map_ct_celda",
    f"ANALYZE {T}.meteo_celda_hora WITH (columns = ARRAY['celda_id', 'hora', 'met_racha_clima_1h'])",
    f"ANALYZE {T}.dataset_train WITH (columns = ARRAY['distribuidora_id', 'ct_id', 'hora', 'y_1_3h'])",
    f"SHOW STATS FOR {T}.map_ct_celda",
    f"SHOW STATS FOR {T}.meteo_celda_hora",
    f"SHOW STATS FOR {T}.dataset_train",
]

QUERY_STATS = ["elapsedTime", "queuedTime", "analysisTime", "planningTime", "planningCpuTime", "executionTime",
               "totalCpuTime", "totalScheduledTime", "totalBlockedTime", "physicalInputDataSize",
               "physicalInputPositions", "physicalInputReadTime", "processedInputDataSize", "processedInputPositions",
               "rawInputDataSize", "rawInputPositions", "internalNetworkInputDataSize", "outputDataSize",
               "outputPositions", "peakUserMemoryReservation", "peakTotalMemoryReservation", "totalDrivers",
               "completedDrivers", "spilledDataSize"]
OPERATOR_STATS = ["stageId", "pipelineId", "operatorId", "planNodeId", "operatorType", "totalDrivers",
                  "inputPositions", "inputDataSize", "physicalInputDataSize", "physicalInputPositions",
                  "rawInputDataSize", "rawInputPositions", "outputPositions", "outputDataSize",
                  "dynamicFilterSplitsProcessed", "peakUserMemoryReservation"]


def request(method: str, url: str, body: str = None, headers: dict = None) -> dict:
    data = body.encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=900) as resp:
        return json.loads(resp.read().decode("utf-8"))


def operators(node, found: dict) -> None:
    """Every operator summary of a QueryInfo, whatever the nesting of the stages."""
    if isinstance(node, dict):
        if "operatorType" in node and "planNodeId" in node:
            key = (node.get("stageId"), node.get("pipelineId"), node.get("operatorId"))
            found.setdefault(key, {}).update({k: node[k] for k in OPERATOR_STATS if node.get(k) is not None})
        for value in node.values():
            operators(value, found)
    elif isinstance(node, list):
        for value in node:
            operators(value, found)


def query_info(query_id: str) -> dict:
    try:
        info = request("GET", f"{TRINO}/v1/query/{query_id}", None, {"X-Trino-User": USER})
    except (urllib.error.URLError, ValueError) as error:
        return {"error": str(error)[:300]}
    stats = info.get("queryStats", {})
    found = {}
    operators(info.get("outputStage"), found)
    operators(stats.get("operatorSummaries"), found)
    return {**{k: stats.get(k) for k in QUERY_STATS if k in stats}, "operadores": list(found.values())}


def run(sql: str, session: list = (), info: bool = True) -> dict:
    headers = {"X-Trino-User": USER, "X-Trino-Source": "gp_planes", "X-Trino-Catalog": "lakehouse",
               "X-Trino-Schema": "l3_gold", "Content-Type": "text/plain"}
    if session:
        headers["X-Trino-Session"] = ",".join(session)
    start = time.time()
    try:
        reply = request("POST", f"{TRINO}/v1/statement", sql, headers)
    except urllib.error.URLError as error:
        return {"sql": sql, "error": {"message": str(error)[:300]}}
    rows, columns, last = [], None, reply
    while True:
        if columns is None and reply.get("columns"):
            columns = [c["name"] for c in reply["columns"]]
        rows.extend(reply.get("data") or [])
        last = reply
        if reply.get("error") or not reply.get("nextUri"):
            break
        reply = request("GET", reply["nextUri"], None, {"X-Trino-User": USER})
    out = {"sql": sql, "id": last.get("id"), "estado": last.get("stats", {}).get("state"),
           "reloj_s": round(time.time() - start, 3), "stats": last.get("stats", {}), "columnas": columns,
           "n_filas": len(rows), "filas": rows[:60]}
    if last.get("error"):
        error = last["error"]
        out["error"] = {k: error.get(k) for k in ("message", "errorName", "errorType")}
    elif info and out["id"]:
        out["servidor"] = query_info(out["id"])
    return out


def measure(name: str, session: list, repeat: int) -> dict:
    sql = CONSULTAS[name]
    result = {"consulta": name, "sesion": list(session), "sql": sql, "inicio": time.strftime("%Y-%m-%d %H:%M:%S")}
    if name in INVENTARIO:
        result["pasadas"] = [run(sql, session)]
        return result
    result["pasadas"] = [run(sql, session) for _ in range(1 + repeat)]
    result["explain_io"] = run("EXPLAIN (TYPE IO, FORMAT JSON) " + sql, session, info=False)
    result["explain_distribuido"] = run("EXPLAIN (TYPE DISTRIBUTED) " + sql, session, info=False)
    result["explain_analyze"] = run("EXPLAIN ANALYZE " + sql, session)
    if name in ESTIMACION:
        result["estimacion"] = run(ESTIMACION[name], session, info=False)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Planes y tiempos de las consultas de 4.7.3 en Trino")
    parser.add_argument("--fase", required=True)
    parser.add_argument("--consultas", default="")
    parser.add_argument("--salida", default="/tmp/gp_planes")
    parser.add_argument("--sesion", action="append", default=[], help="clave=valor de una propiedad de sesión")
    parser.add_argument("--repeticiones", type=int, default=5)
    parser.add_argument("--analyze", action="store_true", help="SHOW STATS, ANALYZE y SHOW STATS de las tablas")
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)
    tag = args.fase + ("_" + "_".join(s.replace("=", "-") for s in args.sesion) if args.sesion else "")
    failed = 0
    if args.analyze:
        steps = [run(sql) for sql in ANALYZE]
        failed += sum(1 for s in steps if "error" in s)
        with open(os.path.join(args.salida, f"{tag}_analyze.json"), "w", encoding="utf-8") as handle:
            json.dump(steps, handle, ensure_ascii=False, indent=1)
        for step in steps:
            print(step["sql"][:90], "->", step.get("error", {}).get("message", "ok")[:160], f"{step.get('reloj_s')} s")
    for name in [c for c in args.consultas.split(",") if c]:
        result = measure(name, args.sesion, args.repeticiones)
        with open(os.path.join(args.salida, f"{tag}_{name}.json"), "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=1)
        times = [p.get("stats", {}).get("elapsedTimeMillis") for p in result["pasadas"]]
        errors = [p["error"]["message"][:160] for p in result["pasadas"] if "error" in p]
        failed += len(errors)
        print(f"{tag} {name}: ms {times}" + (f" ERROR {errors[0]}" if errors else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
