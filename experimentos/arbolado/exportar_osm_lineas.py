import json, sys
sys.path.insert(0, "/app/jobs/00_landing")
from job_landing_meteo_openmeteo import filesystem
fs = filesystem()
files = sorted(fs.glob("s3://datalake/00_landing/publicos/osm/*.json"))
lines, poles = [], 0
for path in files:
    cell = path.rsplit("/", 1)[1][:-5]
    with fs.open(path, "r") as f:
        d = json.load(f)
    for e in d.get("elements", []):
        if e.get("type") == "way" and e.get("tags", {}).get("power") in ("line", "minor_line") and e.get("geometry"):
            lines.append({"id": e["id"], "celda_id": cell, "power": e["tags"].get("power"), "voltage": e["tags"].get("voltage"),
                          "coords": [[p["lon"], p["lat"]] for p in e["geometry"]]})
        elif e.get("type") == "node":
            poles += 1
json.dump(lines, open("/tmp/osm_lineas.json", "w"))
print(len(files), "celdas", len(lines), "lineas", poles, "nodos")
