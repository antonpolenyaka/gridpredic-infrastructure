"""
Resume las medidas de planes.py y compactar_dataset.py para la memoria (4.7.3):

    python resumir_planes.py <carpeta con los json> <carpeta de salida>

Lee los {fase}[_sesion]_{consulta}.json que deja planes.py (una pasada en frío
y cinco en caliente, EXPLAIN ANALYZE y la estimación de SHOW STATS) y
compactacion.json, y escribe:

- planes_resumen.csv: por fase y consulta, el tiempo en frío y la mediana en
  caliente (tiempo del servidor), los splits, las filas y los MB que leyó el
  motor, las filas que pasaron el filtro, la estimación de filas de la lectura
  principal frente a las reales y el reparto de cada join.
- ficheros_particion.csv: ficheros y MB de cada partición de dataset_train
  antes y después de compactar.
- compactacion_resumen.json: ficheros, tamaño y duración de la compactación y
  del orden Z.

Fases: A, línea base; F, otro reparto del join o sin filtrado dinámico; C,
después de ANALYZE; DE, después de compactar dataset_train por particiones y
ordenar 2025 por (distribuidora_id, ct_id).
"""

import csv
import glob
import json
import os
import re
import statistics
import sys

PHASES = ("A", "F", "C", "DE")
SCAN = re.compile(r"(?:ScanFilterProject|ScanFilter|TableScan|ScanProject)\[table = lakehouse:([\w.]+)")
INPUT = re.compile(r"Input: ([\d.]+) rows \(([\d.]+)([kMG]?B)\), Filtered: ([\d.]+)%, Physical input: ([\d.]+)([kMG]?B)"
                   r".*?Splits: (\d+)")
ESTIMATES = re.compile(r"Estimates: (\{rows: [^}]*\})(?:/(\{rows: [^}]*\}))?")
ROWS = re.compile(r"rows: (\?|\d+)")
OUTPUT = re.compile(r"Output: (\d+) rows")
JOIN = re.compile(r"(InnerJoin|LeftJoin|SemiJoin)\[criteria = ([^,\]]+(?: AND [^,\]]+)*), distribution = (\w+)\]")
UNITS = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9}


def megas(value: str, unit: str) -> float:
    return float(value) * UNITS[unit] / 1e6


def plan_nodes(text: str) -> tuple:
    """Lecturas y joins de un EXPLAIN ANALYZE: tabla, filas leídas, filtradas, MB físicos, splits, estimaciones y
    filas de salida de cada lectura; reparto, estimación y salida de cada join."""
    lines = text.splitlines()
    scans, joins = [], []
    for k, line in enumerate(lines):
        m = SCAN.search(line)
        if m:
            block = "\n".join(lines[k:k + 40])
            nxt = re.search(r"\n\s*[└├]─|\nFragment ", block[len(line):])
            block = block[:len(line) + nxt.start()] if nxt else block
            est = ESTIMATES.search(block)
            first = ROWS.search(est.group(1)).group(1) if est else "?"
            second = ROWS.search(est.group(2)).group(1) if est and est.group(2) else first
            out = OUTPUT.search(block)
            inp = INPUT.search(block.replace("\n", " "))
            scans.append({"tabla": m.group(1).split(".")[-1], "est_entrada": first, "est_salida": second,
                          "salida": int(out.group(1)) if out else None,
                          "filas_leidas": int(float(inp.group(1))) if inp else None,
                          "filtrado_pct": float(inp.group(4)) if inp else None,
                          "mb_fisicos": round(megas(inp.group(5), inp.group(6)), 2) if inp else None,
                          "splits": int(inp.group(7)) if inp else None})
        j = JOIN.search(line)
        if j:
            block = "\n".join(lines[k:k + 6])
            est = ESTIMATES.search(block)
            out = OUTPUT.search(block)
            joins.append({"criterio": j.group(2), "reparto": j.group(3),
                          "est": ROWS.search(est.group(1)).group(1) if est else "?",
                          "salida": int(out.group(1)) if out else None})
    return scans, joins


def summarize(path: str) -> dict:
    d = json.load(open(path, encoding="utf-8"))
    name = os.path.basename(path)[:-5]
    phase = name.split("_")[0]
    passes = [p for p in d.get("pasadas", []) if p.get("estado") == "FINISHED"]
    times = [p["stats"]["elapsedTimeMillis"] / 1000 for p in passes]
    first = passes[0]["stats"] if passes else {}
    row = {"fase": phase, "variante": ";".join(d.get("sesion") or []) or "-", "consulta": d["consulta"],
           "frio_s": round(times[0], 2) if times else None,
           "caliente_mediana_s": round(statistics.median(times[1:]), 2) if len(times) > 1 else None,
           "caliente_min_s": round(min(times[1:]), 2) if len(times) > 1 else None,
           "planificacion_frio_s": round((first.get("planningTimeMillis", 0) + first.get("analysisTimeMillis", 0))
                                         / 1000, 2) if first else None,
           "splits": first.get("totalSplits"), "filas_leidas": first.get("processedRows"),
           "mb_leidos": round(first.get("physicalInputBytes", 0) / 1e6, 2) if first else None,
           "cpu_s": round(first.get("cpuTimeMillis", 0) / 1000, 2) if first else None,
           "memoria_pico_mb": round(first.get("peakMemoryBytes", 0) / 1e6, 2) if first else None}
    analyze = (d.get("explain_analyze") or {}).get("filas") or []
    if analyze:
        scans, joins = plan_nodes(analyze[0][0])
        main = max(scans, key=lambda s: s["filas_leidas"] or 0) if scans else None
        if main:
            row.update({"tabla_principal": main["tabla"], "est_filas_lectura": main["est_entrada"],
                        "est_filas_filtro": main["est_salida"], "filas_utiles": main["salida"],
                        "splits_principal": main["splits"], "mb_principal": main["mb_fisicos"],
                        "filtrado_pct": main["filtrado_pct"]})
        row["lecturas"] = " | ".join(f"{s['tabla']}: {s['filas_leidas']} leídas, {s['salida']} útiles, "
                                     f"{s['splits']} splits, {s['mb_fisicos']} MB" for s in scans)
        row["joins"] = " | ".join(f"{j['reparto']} est={j['est']} real={j['salida']}" for j in joins)
    return row


def main(argv) -> int:
    src, out = argv[0], argv[1]
    os.makedirs(out, exist_ok=True)
    rows = []
    for path in sorted(glob.glob(os.path.join(src, "*.json"))):
        name = os.path.basename(path)
        if not name.startswith(tuple(p + "_" for p in PHASES)) or "ficheros" in name or "analyze" in name:
            continue
        rows.append(summarize(path))
    order = {p: k for k, p in enumerate(PHASES)}
    rows.sort(key=lambda r: (r["consulta"], order[r["fase"]], r["variante"]))
    fields = ["consulta", "fase", "variante", "frio_s", "caliente_mediana_s", "caliente_min_s", "planificacion_frio_s",
              "splits", "filas_leidas", "mb_leidos", "cpu_s", "memoria_pico_mb", "tabla_principal",
              "est_filas_lectura", "est_filas_filtro", "filas_utiles", "splits_principal", "mb_principal",
              "filtrado_pct", "joins", "lecturas"]
    with open(os.path.join(out, "planes_resumen.csv"), "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    comp = os.path.join(src, "compactacion.json")
    if os.path.exists(comp):
        # Ficheros de cada partición antes y después, de las métricas de cada OPTIMIZE (el recuento por "$path" de
        # planes.py guarda solo las 60 primeras filas de la respuesta).
        c = json.load(open(comp, encoding="utf-8"))
        zorder = {(z["split"], z["fecha_mes"]): z for z in c.get("orden_z", []) if not z.get("error")}
        with open(os.path.join(out, "ficheros_particion.csv"), "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["split", "fecha_mes", "filas", "ficheros_antes", "mb_antes", "ficheros_compactada",
                             "mb_compactada", "segundos_compactar", "ficheros_orden_z", "mb_orden_z", "segundos_orden_z"])
            for p in c.get("compactacion", []):
                m = p.get("metricas") or {}
                z = zorder.get((p["split"], p["fecha_mes"]))
                zm = (z or {}).get("metricas") or {}
                writer.writerow([p["split"], p["fecha_mes"], p.get("filas"), m.get("numFilesRemoved"),
                                 round(m["filesRemoved"]["totalSize"] / 1e6, 1) if m else None, m.get("numFilesAdded"),
                                 round(m["filesAdded"]["totalSize"] / 1e6, 1) if m else None, p.get("segundos"),
                                 zm.get("numFilesAdded"),
                                 round(zm["filesAdded"]["totalSize"] / 1e6, 1) if zm else None,
                                 (z or {}).get("segundos")])

    if os.path.exists(comp):
        parts = c.get("compactacion", [])
        zparts = c.get("orden_z", [])
        summary = {"ficheros_antes": c["antes"]["numFiles"], "gb_antes": round(c["antes"]["sizeInBytes"] / 1e9, 2),
                   "ficheros_compactada": c["despues_compactar"]["numFiles"],
                   "gb_compactada": round(c["despues_compactar"]["sizeInBytes"] / 1e9, 2),
                   "min_compactar": round(c["segundos_compactar"] / 60, 1),
                   "particiones": len(parts), "errores": sum(1 for p in parts if p.get("error")),
                   "ficheros_orden_z": c["despues_orden_z"]["numFiles"],
                   "gb_orden_z": round(c["despues_orden_z"]["sizeInBytes"] / 1e9, 2),
                   "min_orden_z": round(c["segundos_orden_z"] / 60, 1), "particiones_orden_z": len(zparts),
                   "columnas_con_estadisticas": (parts[0]["metricas"] or {}).get("numTableColumnsWithStats") if parts
                   else None,
                   "columnas_tabla": (parts[0]["metricas"] or {}).get("numTableColumns") if parts else None}
        with open(os.path.join(out, "compactacion_resumen.json"), "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=1)
        print(json.dumps(summary, ensure_ascii=False))
    for r in rows:
        print(f"{r['consulta']:4} {r['fase']:3} {r['variante'][:40]:40} frío {r['frio_s']!s:>6} caliente {r['caliente_mediana_s']!s:>5}"
              f" splits {r.get('splits')!s:>4} leídas {r.get('filas_leidas')!s:>9} MB {r.get('mb_leidos')!s:>7}"
              f" est {r.get('est_filas_filtro', '')!s:>8} útiles {r.get('filas_utiles', '')!s:>8} {r.get('joins', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
