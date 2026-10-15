"""
Landing of the free public data that describe where the network is, saved as
the APIs return them:

- INE, Padron continuo (operation DPOP): population of every municipality
  of the provinces of the study, by year since 1996 (one table per province,
  config ine.tablas_por_provincia). With tip=M every series carries the INE
  code of its municipality, so the match with Calser is by code, not by name.
  s3://datalake/00_landing/publicos/ine/padron_<tabla>.json
- Wikidata (SPARQL endpoint): surface (km2) and altitude of every
  municipality of those provinces, by the INE code (property P772).
  .../wikidata/municipios.json
- Open-Meteo Elevation API (Copernicus DEM, 90 m): a grid of
  puntos_por_lado x puntos_por_lado points inside every weather cell and the
  centre of every municipality, to describe the relief (mean, spread, top).
  .../elevacion/puntos.json
- OpenStreetMap through the Overpass API: power lines and cables, poles and
  towers, transformers and substations, roads and tracks, forest, scrub,
  construction sites and the number of buildings inside every weather cell.
  One file per cell (.../osm/<celda_id>.json); a cell already there is not
  asked again, so a run that stops is continued by launching it again. The
  endpoints of the configuration are tried in order.
- Plataforma de Contratacion del Sector Publico (PLACSP), open data of the
  tenders of the contracting profiles: the works contracts (contract type 3
  or CPV 45) of the provinces of the study, the public works that dig and
  drive machines near the network. The yearly file of the whole country
  (about 1,5 GB zipped, ATOM in CODICE) is downloaded to a temporary file
  and only the entries of the territory (NUTS ES43 or a postal code of the
  provinces) are kept, as they come. One file per year
  (.../obras/placsp_<anio>.json.gz) with the entries, the namespaces of the
  feed and the entries deleted from the syndication.
- Catastro, INSPIRE Buildings: the buildings of every municipality of the
  study with their year of construction, use, dwellings and surface, the
  zip of the Catastro as it comes (.../catastro/<codigo_ine>.zip). The
  Catastro numbers the municipalities with its own codes, so the match with
  the INE code is by name inside the province (.../catastro/municipios.json).
- Population projections 2024-2039 of the Instituto de Estadistica de
  Extremadura by groups of municipalities (AME), and the AME of every
  municipality: the IEEX publishes them in formats the Spark image does not
  read (Excel 97 inside 7z, a PDF map), so extract_ieex_proyecciones.py turns
  them once into two CSV files of the configuration, which this source copies
  to Landing (.../proyecciones/).
- GBIF (Global Biodiversity Information Facility), occurrence search API,
  free and without key: the records of the white stork (Ciconia ciconia) and
  of all the birds inside every weather cell, by year and month (only the
  counts, limit=0 with facets). Stork nests on poles are the first cause of
  the faults of the own overhead lines that the operators name; the share of
  storks among the bird records of a cell corrects the effort of the
  observers. One file per cell (.../aves/<celda_id>.json).
- EFFIS (European Forest Fire Information System, Copernicus), burnt areas
  of 30 ha or more mapped from MODIS, through its open WFS: the polygons of
  the study area with the date, the municipality and the area
  (.../incendios/effis_area_quemada.gml). The fires lead to cuts asked by the
  emergency services and to damaged insulators. The axis order of the WFS
  versions differs, so the request tries the variants of the configuration
  and keeps the first one whose polygons fall in the study area.
- GECOS (MITECO), the official TIEPI and NIEPI of the sector by zone type
  and cause for Spain, the communities and the provinces, 2003 onwards: the
  reference the CNMC incentive compares every distributor with.
  extract_gecos_indices.py asks the public pages once a year and leaves one
  CSV next to the configuration, which this source copies to Landing
  (.../calidad_sector/).
- AEMET OpenData, archive of the warnings of adverse weather (CAP messages,
  from 18.06.2018, CC BY 4.0): wind, rain, thunderstorms, snow and heat by
  warning zone, with the level (yellow, orange, red), the time it was issued
  and the hours it covers. It is the forecast the operators already watch
  and a forecast made in advance, not the reanalysis. It needs the free key
  of opendata.aemet.es in AEMET_API_KEY. One tar per piece of days, as AEMET
  gives it (.../avisos/<desde>_<hasta>.tar).
- NASA FIRMS, the active fires seen by MODIS (from 2000) and VIIRS (from
  2012 on Suomi NPP and 2018 on NOAA-20) in the box of the study, by pieces of
  at most 5 days, the archive of standard processing and the near real time
  for the last months. It needs the free MAP_KEY of FIRMS in the environment
  (it goes in the URL, so the URL is never written to the log). One CSV per
  sensor and piece (.../focos/<sensor>/<desde>.csv), as FIRMS gives it.
- Copernicus EWDS (Early Warning Data Store of CEMS), fire danger indices
  of the reanalysis (cems-fire-historical-v1, CC BY 4.0): the Fire Weather
  Index of the Canadian system and its parts (FFMC, DMC, DC, ISI, BUI) every
  day on the grid of 0.25 degrees in the box of the study, one request per
  year, from the consolidated dataset while it covers the month and from the
  intermediate one after. It needs the free Personal Access Token of the
  EWDS in COPERNICUS_EWDS_KEY (and the URL of the API in COPERNICUS_EWDS_URL,
  the one of the configuration by default), sent in a header, never written
  to the log, and the licence of the dataset accepted on its page. The
  requests wait in the queue of the EWDS; several go at once. One NetCDF per
  piece (.../fwi/<anio>_<conjunto>_<mes>-<mes>.nc) with the request next to
  it (.json).

The places are the municipalities of data/reference_data/municipios.xlsx and
their cells, the same as the weather (job_landing_meteo_openmeteo).

Exit codes: 0 everything landed; 4 some source (or some OSM cell) could not be
downloaded: the rest is in Landing and Bronze can run, and launching again
asks only what is missing.

    docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py
    docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes osm
    docker compose exec spark-master python3 /app/jobs/00_landing/job_landing_datos_publicos.py --fuentes obras,catastro
"""

import argparse
import gzip
import http.client
import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))

if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_landing_meteo_openmeteo import filesystem, municipality_cells, read_reference  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("publicos")

CONFIG_CANDIDATES = [
    "/app/config/01_bronze/config_bronze_publicos.json",
    "/opt/airflow/config/01_bronze/config_bronze_publicos.json",
    os.path.join(HERE, "..", "..", "config", "01_bronze", "config_bronze_publicos.json"),
]

SOURCES = ["ine", "wikidata", "elevacion", "osm", "obras", "catastro", "proyecciones", "aves", "incendios", "avisos",
           "focos", "fwi", "calidad_sector"]
PARTIAL_EXIT = 4


def load_config(path: str = None) -> dict:
    for candidate in [path] if path else CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                return json.load(file)

    raise FileNotFoundError(f"config_bronze_publicos.json not found in {CONFIG_CANDIDATES}")


def fetch(url: str, data: bytes = None, headers: dict = None, retries: int = 4, wait_s: float = 30,
          timeout: float = 300, label: str = None) -> bytes:
    """
    GET (or POST with data) with retries on 429, 5xx and network errors. With
    label, the messages name the request by it instead of by its URL (a URL
    that carries a key).
    """
    headers = {"User-Agent": "GridPredic-TFM/1.0", **(headers or {})}
    shown = label or url[:80]

    for attempt in range(1, retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "replace")[:200]
            if error.code in (429, 500, 502, 503, 504) and attempt < retries:
                logger.warning("HTTP %s from %s (%s), waiting %.0f s, attempt %s of %s",
                               error.code, shown, body, wait_s * attempt, attempt, retries)
                time.sleep(wait_s * attempt)
                continue
            raise RuntimeError(f"HTTP {error.code} for {label or url[:120]}: {body}") from error
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt < retries:
                logger.warning("%s for %s, waiting %.0f s, attempt %s of %s", error, shown, wait_s * attempt,
                               attempt, retries)
                time.sleep(wait_s * attempt)
                continue
            if label:
                raise RuntimeError(f"{type(error).__name__} for {label}") from None
            raise

    raise RuntimeError(f"No answer for {label or url[:120]}")


def save_json(fs, path: str, payload):
    with fs.open(path, "w") as file:
        json.dump(payload, file, ensure_ascii=False)


def stamp(source: str, **extra) -> dict:
    return {"fuente": source, "descargado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), **extra}


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

def provinces_of(places: list) -> list:
    return sorted({p["codigo_ine"][:2] for p in places if p.get("codigo_ine")})


def land_ine(fs, cfg: dict, landing: str, provinces: list, force: bool) -> int:
    ine = cfg["ine"]
    done = 0

    for province in provinces:
        table = ine["tablas_por_provincia"].get(province)
        if table is None:
            logger.warning("No INE table configured for province %s", province)
            continue

        target = f"{landing}/ine/padron_{table}.json"
        if fs.exists(target) and not force:
            logger.info("INE table %s already in Landing", table)
            continue

        url = ine["url"].format(tabla=table, anios=int(ine.get("anios", 30)))
        series = json.loads(fetch(url, retries=int(cfg["reintentos"]), wait_s=float(cfg["espera_reintento_s"])))
        if not series:
            raise RuntimeError(f"Empty answer of the INE for table {table}")

        save_json(fs, target, {"_peticion": stamp("ine", tabla=table, provincia=province, url=url), "series": series})
        done += 1
        logger.info("INE table %s (province %s): %s series", table, province, len(series))
        time.sleep(float(cfg["pausa_s"]))

    return done


def wikidata_query(provinces: list) -> str:
    starts = " || ".join(f'STRSTARTS(?ine, "{p}")' for p in provinces)
    return (
        "SELECT ?item ?ine ?itemLabel ?area ?elev WHERE {\n"
        "  ?item wdt:P772 ?ine .\n"
        f"  FILTER(STRLEN(?ine) = 5 && ({starts}))\n"
        "  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area . }\n"
        "  OPTIONAL { ?item wdt:P2044 ?elev . }\n"
        '  SERVICE wikibase:label { bd:serviceParam wikibase:language "es". }\n'
        "}"
    )


def land_wikidata(fs, cfg: dict, landing: str, provinces: list, force: bool) -> int:
    target = f"{landing}/wikidata/municipios.json"
    if fs.exists(target) and not force:
        logger.info("Wikidata already in Landing")
        return 0

    wd = cfg["wikidata"]
    query = wikidata_query(provinces)
    # POST: the endpoint accepts the query in the body, which avoids the limits of long URLs.
    payload = json.loads(fetch(
        wd["url"], data=urllib.parse.urlencode({"query": query, "format": "json"}).encode("utf-8"),
        headers={"Accept": "application/sparql-results+json", "User-Agent": wd["agente"],
                 "Content-Type": "application/x-www-form-urlencoded"},
        retries=int(cfg["reintentos"]), wait_s=float(cfg["espera_reintento_s"])))
    rows = payload.get("results", {}).get("bindings", [])
    if not rows:
        raise RuntimeError("Empty answer of Wikidata")

    payload["_peticion"] = stamp("wikidata", provincias=provinces, consulta=query)
    save_json(fs, target, payload)
    logger.info("Wikidata: %s municipalities", len(rows))
    return 1


def grid_points(cells: dict, size: float, per_side: int) -> list:
    """per_side x per_side points evenly spread inside every cell (centre lat, lon, side size in degrees)."""
    points = []
    for cell, (lat, lon) in sorted(cells.items()):
        for i in range(per_side):
            for j in range(per_side):
                points.append({
                    "tipo": "celda", "clave": cell, "punto": i * per_side + j,
                    "latitud": round(lat + ((i + 0.5) / per_side - 0.5) * size, 5),
                    "longitud": round(lon + ((j + 0.5) / per_side - 0.5) * size, 5),
                })
    return points


def land_elevation(fs, cfg: dict, landing: str, places: list, cells: dict, force: bool) -> int:
    target = f"{landing}/elevacion/puntos.json"
    if fs.exists(target) and not force:
        logger.info("Elevation already in Landing")
        return 0

    el = cfg["elevacion"]
    points = grid_points(cells, float(cfg["celda_grados"]), int(el["puntos_por_lado"]))
    points += [{"tipo": "municipio", "clave": p["codigo_ine"], "punto": 0, "latitud": p["latitud"],
                "longitud": p["longitud"]} for p in places]
    size = int(el["max_por_peticion"])

    for start in range(0, len(points), size):
        batch = points[start:start + size]
        query = urllib.parse.urlencode({
            "latitude": ",".join(f"{p['latitud']:.5f}" for p in batch),
            "longitude": ",".join(f"{p['longitud']:.5f}" for p in batch),
        }, safe=",")
        answer = json.loads(fetch(f"{el['url']}?{query}", retries=int(cfg["reintentos"]),
                                  wait_s=float(cfg["espera_reintento_s"])))
        values = answer.get("elevation", [])
        if len(values) != len(batch):
            raise RuntimeError(f"Elevation API returned {len(values)} values for {len(batch)} points")
        for point, value in zip(batch, values):
            point["elevacion_m"] = value
        time.sleep(float(cfg["pausa_s"]))

    save_json(fs, target, {"_peticion": stamp("open-meteo-elevation", url=el["url"]), "puntos": points})
    logger.info("Elevation: %s points (%s cells, %s municipalities)", len(points), len(cells), len(places))
    return 1


def cell_bbox(lat: float, lon: float, size: float) -> tuple:
    half = size / 2.0
    return (round(lat - half, 5), round(lon - half, 5), round(lat + half, 5), round(lon + half, 5))


def overpass_query(bbox: tuple, timeout: int) -> str:
    south, west, north, east = bbox
    return (
        f"[out:json][timeout:{timeout}][bbox:{south},{west},{north},{east}];\n"
        "(\n"
        '  way["power"~"^(line|minor_line|cable)$"];\n'
        '  way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|track)$"];\n'
        '  way["landuse"~"^(forest|construction)$"];\n'
        '  way["natural"~"^(wood|scrub)$"];\n'
        '  relation["landuse"="forest"];\n'
        '  relation["natural"~"^(wood|scrub)$"];\n'
        ");\n"
        "out tags geom;\n"
        "(\n"
        '  node["power"~"^(pole|tower|transformer|substation|switch)$"];\n'
        '  way["power"="substation"];\n'
        ");\n"
        "out tags center;\n"
        'way["building"];\n'
        "out count;\n"
    )


def overpass_parts(bbox: tuple, timeout: int) -> dict:
    """The query of a cell in three lighter parts: the busy servers time out less with each one alone."""
    south, west, north, east = bbox
    head = f"[out:json][timeout:{timeout}][bbox:{south},{west},{north},{east}];\n"
    return {
        "red": head + (
            'way["power"~"^(line|minor_line|cable)$"];\nout tags geom;\n'
            '(\n  node["power"~"^(pole|tower|transformer|substation|switch)$"];\n  way["power"="substation"];\n);\n'
            'out tags center;\nway["building"];\nout count;\n'
        ),
        "viario": head + (
            'way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|track)$"];\n'
            "out tags geom;\n"
        ),
        "cubierta": head + (
            '(\n  way["landuse"~"^(forest|construction)$"];\n  way["natural"~"^(wood|scrub)$"];\n'
            '  relation["landuse"="forest"];\n  relation["natural"~"^(wood|scrub)$"];\n);\nout tags geom;\n'
        ),
    }


def overpass_call(cfg: dict, query: str, cell: str, part: str):
    """(answer, endpoint) of the first endpoint that answers the query, or (None, None)."""
    osm = cfg["osm"]
    for endpoint in osm["endpoints"]:
        try:
            raw = fetch(endpoint, data=urllib.parse.urlencode({"data": query}).encode("utf-8"),
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        retries=2, wait_s=float(cfg["espera_reintento_s"]), timeout=float(osm["timeout_s"]) + 60)
            answer = json.loads(raw)
            remark = str(answer.get("remark", ""))
            if "runtime error" in remark.lower():
                raise RuntimeError(remark[:200])
            return answer, endpoint
        except Exception as error:  # noqa: BLE001
            logger.warning("Overpass %s failed for cell %s (%s): %s", endpoint, cell, part, str(error)[:160])
    return None, None


def land_osm(fs, cfg: dict, landing: str, cells: dict, force: bool, limit: int = None) -> tuple:
    osm = cfg["osm"]
    size = float(cfg["celda_grados"])
    done = failed = 0

    for cell, (lat, lon) in sorted(cells.items()):
        target = f"{landing}/osm/{cell}.json"
        if fs.exists(target) and not force:
            continue
        if limit is not None and done >= limit:
            break

        bbox = cell_bbox(lat, lon, size)
        elements, used, base = [], {}, None
        for part, query in overpass_parts(bbox, int(osm["timeout_s"])).items():
            answer, endpoint = overpass_call(cfg, query, cell, part)
            if answer is None:
                elements = None
                break
            elements += answer.get("elements", [])
            used[part] = endpoint
            base = base or answer.get("osm3s")
            time.sleep(float(cfg["pausa_s"]))

        if elements is None:
            failed += 1
            logger.error("Cell %s not downloaded from any Overpass endpoint", cell)
            continue

        payload = {"elements": elements, "osm3s": base or {}}
        payload["_peticion"] = stamp("overpass", celda_id=cell, bbox=list(bbox),
                                     endpoint=",".join(sorted(set(used.values()))), partes=used)
        save_json(fs, target, payload)
        done += 1
        logger.info("OSM cell %s: %s elements (%s now, %s failed)", cell, len(elements), done, failed)
        time.sleep(float(osm.get("pausa_s", 10)))

    return done, failed


# PLACSP --------------------------------------------------------------------

ENTRY_OPEN, ENTRY_CLOSE = "<entry>", "</entry>"
NAMESPACE = re.compile(r'xmlns:([\w.-]+)="([^"]+)"')
DELETED = re.compile(r"<(?:[\w-]+:)?deleted-entry\b([^>]*)>(.*?)</(?:[\w-]+:)?deleted-entry>", re.S)
ATTRIBUTE = re.compile(r'([\w:-]+)="([^"]*)"')


def placsp_marks(cfg: dict) -> list:
    """Plain strings every entry of the territory contains: found with str.find, much faster than a regex."""
    return [f">{n}" for n in cfg["nuts"]] + [f"PostalZone>{p}" for p in cfg["codigos_postales"]]


def placsp_patterns(cfg: dict) -> tuple:
    """(territory, works) regular expressions over the raw text of an entry, whatever the namespace prefixes."""
    nuts = "|".join(re.escape(n) for n in cfg["nuts"])
    postal = "|".join(re.escape(p) for p in cfg["codigos_postales"])
    territory = re.compile(
        rf"<(?:[\w-]+:)?CountrySubentityCode\b[^>]*>\s*(?:{nuts})\d*\s*<|<(?:[\w-]+:)?PostalZone>\s*(?:{postal})\d{{3}}\s*<")
    types = "|".join(re.escape(t) for t in cfg["tipos_contrato"])
    cpv = "|".join(re.escape(c) for c in cfg["cpv_prefijos"])
    works = re.compile(
        rf"<(?:[\w-]+:)?TypeCode\b[^>]*ContractCode[^>]*>\s*(?:{types})\s*<"
        rf"|<(?:[\w-]+:)?ItemClassificationCode\b[^>]*>\s*(?:{cpv})\d*\s*<")
    return territory, works


def filter_feed(text: str, territory, works, marks: list = None) -> tuple:
    """
    (kept entries, deleted entries, entries read) of one ATOM file of PLACSP.
    Only the entries with a mark of the territory are cut out and tested, so
    the 97 % of the country is skipped by a search over the text, not parsed.
    With marks, the candidates come from plain string searches and the regular
    expressions only run on them.
    """
    starts = set()
    if marks:
        for mark in marks:
            position = text.find(mark)
            while position >= 0:
                start = text.rfind(ENTRY_OPEN, 0, position)
                if start >= 0:
                    starts.add(start)
                position = text.find(mark, position + len(mark))
    else:
        for match in territory.finditer(text):
            start = text.rfind(ENTRY_OPEN, 0, match.start())
            if start >= 0:
                starts.add(start)

    kept = []
    for start in sorted(starts):
        end = text.find(ENTRY_CLOSE, start)
        if end < 0:
            continue
        entry = text[start:end + len(ENTRY_CLOSE)]
        if territory.search(entry) and works.search(entry):
            kept.append(entry)

    deleted = []
    position = text.find("deleted-entry")
    while position >= 0:
        match = DELETED.match(text, text.rfind("<", 0, position))
        if match is None:
            position = text.find("deleted-entry", position + 13)
            continue
        attrs = dict(ATTRIBUTE.findall(match.group(1)))
        kind = re.search(r'type="([^"]*)"', match.group(2))
        deleted.append({"ref": attrs.get("ref"), "when": attrs.get("when"), "tipo": kind.group(1) if kind else None})
        position = text.find("deleted-entry", match.end())

    return kept, deleted, text.count(ENTRY_OPEN)


def download(url: str, target: str, retries: int = 3, wait_s: float = 30, chunk: int = 1 << 20) -> int:
    """Streams a big file to disk, with retries from the start (the server ignores Range)."""
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "GridPredic-TFM/1.0"})
            total, next_log = 0, 250 << 20
            with urllib.request.urlopen(request, timeout=300) as response, open(target, "wb") as out:
                while True:
                    block = response.read(chunk)
                    if not block:
                        break
                    out.write(block)
                    total += len(block)
                    if total >= next_log:
                        logger.info("  %s: %.0f MB", url.rsplit("/", 1)[-1], total / 1e6)
                        next_log += 250 << 20
            if not zipfile.is_zipfile(target):
                raise RuntimeError(f"{url} is not a zip ({total} bytes)")
            return total
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException, RuntimeError,
                OSError) as error:
            if attempt >= retries:
                raise
            logger.warning("Download of %s failed (%s), waiting %.0f s, attempt %s of %s", url[-60:], str(error)[:120],
                           wait_s * attempt, attempt, retries)
            time.sleep(wait_s * attempt)

    raise RuntimeError(f"No answer for {url}")


def placsp_members(task: tuple) -> tuple:
    """(kept, deleted, namespaces, entries read, files) of some ATOM files of a PLACSP zip; runs in a worker."""
    path, names, cfg = task
    territory, works = placsp_patterns(cfg)
    marks = placsp_marks(cfg)
    kept, deleted, namespaces, read = [], [], {}, 0
    with zipfile.ZipFile(path) as archive:
        for name in names:
            with archive.open(name) as member:
                text = member.read().decode("utf-8", "replace")
            namespaces.update(NAMESPACE.findall(text[:5000]))
            entries, gone, n = filter_feed(text, territory, works, marks)
            kept += entries
            deleted += gone
            read += n
    return kept, deleted, namespaces, read, len(names)


def placsp_archive(path: str, cfg: dict, processes: int = 1) -> dict:
    """
    The entries of the territory of a PLACSP zip already on disk. With more
    than one process every worker reads a contiguous block of the ATOM files,
    so the entries keep the order of the files.
    """
    with zipfile.ZipFile(path) as archive:
        names = sorted(n for n in archive.namelist() if n.endswith(".atom"))
    processes = max(1, min(int(processes or 1), len(names) or 1))
    size = -(-len(names) // processes) if names else 0
    tasks = [(path, names[i:i + size], cfg) for i in range(0, len(names), size)] if names else []
    if processes > 1 and len(tasks) > 1:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=processes) as pool:
            parts = list(pool.map(placsp_members, tasks))
    else:
        parts = [placsp_members(task) for task in tasks]

    kept, deleted, namespaces, read, files = [], [], {}, 0, 0
    for entries, gone, found, n, count in parts:
        kept += entries
        deleted += gone
        namespaces.update(found)
        read += n
        files += count
    return {"_namespaces": namespaces, "entradas": kept, "borradas": deleted,
            "_resumen": {"ficheros": files, "entradas_leidas": read, "entradas_guardadas": len(kept)}}


def placsp_years(cfg: dict, wanted: str = None) -> list:
    """Years to land: the ones of the configuration, or the list of --anios ("2025,2024" or "2021-2023") in its order."""
    if not wanted:
        return list(range(int(cfg["anio_desde"]), int(cfg["anio_hasta"]) + 1))
    years = []
    for part in wanted.split(","):
        part = part.strip()
        if "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
            years += list(range(a, b + 1) if a <= b else range(a, b - 1, -1))
        elif part:
            years.append(int(part))
    return years


def land_obras(fs, cfg: dict, landing: str, force: bool, years: str = None, processes: int = 1) -> tuple:
    ob = cfg["obras"]
    done = failed = 0

    for year in placsp_years(ob, years):
        target = f"{landing}/obras/placsp_{year}.json.gz"
        if fs.exists(target) and not force:
            logger.info("PLACSP %s already in Landing", year)
            continue

        url = ob["url_anual"].format(anio=year)
        started = time.time()
        try:
            with tempfile.TemporaryDirectory(prefix="placsp_") as folder:
                path = os.path.join(folder, f"placsp_{year}.zip")
                size = download(url, path, int(ob.get("reintentos_descarga", 3)), float(cfg["espera_reintento_s"]))
                payload = placsp_archive(path, ob, processes)
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("PLACSP %s not landed: %s", year, str(error)[:300])
            continue

        summary = payload.pop("_resumen")
        payload["_peticion"] = stamp("placsp", anio=year, url=url, bytes=size, **summary)
        with fs.open(target, "wb") as file:
            file.write(gzip.compress(json.dumps(payload, ensure_ascii=False).encode("utf-8")))
        done += 1
        logger.info("PLACSP %s: %s works entries of the territory out of %s (%s files, %.0f MB, %.0f s)", year,
                    summary["entradas_guardadas"], summary["entradas_leidas"], summary["ficheros"], size / 1e6,
                    time.time() - started)

    return done, failed


# Catastro ------------------------------------------------------------------

CATASTRO_ENTRY = re.compile(r"<entry>(.*?)</entry>", re.S)
CATASTRO_TITLE = re.compile(r"<title[^>]*>\s*(\d{5})-(.*?)\s+buildings\s*</title>", re.S)
CATASTRO_LINK = re.compile(r'<link\b[^>]*rel="enclosure"[^>]*href="([^"]+)"')
ARTICLE = re.compile(r"^(.*?)\s*(?:,|\()\s*(el|la|los|las|l')\s*\)?\s*$", re.I)


def decode_feed(raw: bytes) -> str:
    """The feeds of the Catastro say UTF-8 but some come in Latin-1 (the Ñ)."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def plain_name(name: str) -> str:
    """Name to match municipalities: no accents, case, spaces or punctuation; "Torno (El)" as "El Torno"."""
    text = str(name or "").strip()
    moved = ARTICLE.match(text)
    if moved:
        text = f"{moved.group(2)} {moved.group(1)}"
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z]", "", text)


def catastro_index(text: str) -> list:
    """Municipalities of a province feed: Catastro code, name and the url of the zip."""
    out = []
    for entry in CATASTRO_ENTRY.findall(text):
        title, link = CATASTRO_TITLE.search(entry), CATASTRO_LINK.search(entry)
        if title and link:
            out.append({"codigo_catastro": title.group(1), "nombre_catastro": title.group(2).strip(),
                        "url": link.group(1)})
    return out


def land_catastro(fs, cfg: dict, landing: str, places: list, provinces: list, force: bool) -> tuple:
    cat = cfg["catastro"]
    index = {}
    for province in provinces:
        url = cat["url_provincia"].format(provincia=province)
        text = decode_feed(fetch(url, retries=int(cfg["reintentos"]), wait_s=float(cfg["espera_reintento_s"])))
        for item in catastro_index(text):
            index.setdefault((item["codigo_catastro"][:2], plain_name(item["nombre_catastro"])), item)
        time.sleep(float(cat.get("pausa_s", 2)))
    if not index:
        raise RuntimeError("Empty feeds of the Catastro")

    done = failed = unmatched = 0
    manifest, seen = [], set()
    for place in sorted(places, key=lambda p: p["codigo_ine"]):
        code = place["codigo_ine"]
        if code in seen:
            continue
        seen.add(code)
        item = index.get((code[:2], plain_name(place["municipio"])))
        if item is None:
            unmatched += 1
            logger.warning("Municipality %s %s not found by name in the feed of the Catastro", code, place["municipio"])
            continue

        target = f"{landing}/catastro/{code}.zip"
        if not fs.exists(target) or force:
            try:
                raw = fetch(urllib.parse.quote(item["url"], safe=":/"), retries=int(cfg["reintentos"]),
                            wait_s=float(cfg["espera_reintento_s"]), timeout=600)
                if not raw.startswith(b"PK"):
                    raise RuntimeError(f"not a zip ({len(raw)} bytes)")
            except Exception as error:  # noqa: BLE001
                failed += 1
                logger.error("Catastro %s %s not downloaded: %s", code, place["municipio"], str(error)[:200])
                continue
            with fs.open(target, "wb") as file:
                file.write(raw)
            done += 1
            logger.info("Catastro %s %s (%s): %.1f MB", code, place["municipio"], item["codigo_catastro"],
                        len(raw) / 1e6)
            time.sleep(float(cat.get("pausa_s", 2)))
        manifest.append({"codigo_ine": code, "municipio": place["municipio"], **item, "fichero": target})

    save_json(fs, f"{landing}/catastro/municipios.json",
              {"_peticion": stamp("catastro-inspire-buildings", provincias=provinces), "municipios": manifest})
    logger.info("Catastro: %s municipalities in Landing (%s now), %s failed, %s without a match by name",
                len(manifest), done, failed, unmatched)
    return done, failed


# IEEX projections -----------------------------------------------------------

def land_config_files(fs, cfg: dict, landing: str, source: str, label: str) -> int:
    """Copies the CSV files of a source prepared next to the configuration (IEEX, GECOS) to Landing."""
    block = cfg[source]
    folders = [os.path.dirname(c) for c in CONFIG_CANDIDATES]
    done = 0
    for name in block["ficheros"]:
        path = next((os.path.join(f, name) for f in folders if os.path.exists(os.path.join(f, name))), None)
        if path is None:
            raise FileNotFoundError(f"{name} not found next to config_bronze_publicos.json")
        with open(path, "rb") as file:
            raw = file.read()
        with fs.open(f"{landing}/{source}/{name}", "wb") as file:
            file.write(raw)
        done += 1
    save_json(fs, f"{landing}/{source}/peticion.json", stamp(label, origen=block.get("origen"), ficheros=block["ficheros"]))
    logger.info("%s: %s files in Landing", label, done)
    return done


def land_projections(fs, cfg: dict, landing: str) -> int:
    return land_config_files(fs, cfg, landing, "proyecciones", "ieex-proyecciones")


def land_sector_quality(fs, cfg: dict, landing: str) -> int:
    return land_config_files(fs, cfg, landing, "calidad_sector", "gecos-calidad-sector")


# GBIF ----------------------------------------------------------------------

def gbif_params(taxon: dict, lat: float, lon: float, half: float, cfg: dict) -> dict:
    """Query of the occurrence search API for the counts of a taxon inside a cell, by year and month."""
    return {
        **taxon,
        "decimalLatitude": f"{lat - half:.4f},{lat + half:.4f}",
        "decimalLongitude": f"{lon - half:.4f},{lon + half:.4f}",
        "hasCoordinate": "true",
        "occurrenceStatus": "PRESENT",
        "year": f"{cfg['anio_desde']},{cfg['anio_hasta']}",
        "limit": "0",
        "facet": ["year", "month"],
        "facetLimit": str(cfg.get("facet_limite", 60)),
    }


def land_birds(fs, cfg: dict, landing: str, cells: dict, force: bool) -> tuple:
    birds = cfg["aves"]
    half = float(cfg["celda_grados"]) / 2.0
    done = failed = 0

    for cell, (lat, lon) in sorted(cells.items()):
        target = f"{landing}/aves/{cell}.json"
        if fs.exists(target) and not force:
            continue

        answers = {}
        try:
            for name, taxon in birds["taxones"].items():
                query = urllib.parse.urlencode(gbif_params(taxon, lat, lon, half, birds), doseq=True, safe=",")
                answers[name] = json.loads(fetch(f"{birds['api']}?{query}").decode("utf-8"))
                time.sleep(float(birds.get("pausa_s", 1)))
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("GBIF cell %s not downloaded: %s", cell, str(error)[:200])
            continue

        payload = {"taxones": answers, "_peticion": stamp("gbif", celda_id=cell, latitud=lat, longitud=lon,
                                                          bbox=[lat - half, lon - half, lat + half, lon + half])}
        save_json(fs, target, payload)
        done += 1
        logger.info("GBIF cell %s: %s", cell, {k: v.get("count") for k, v in answers.items()})

    return done, failed


# EFFIS ---------------------------------------------------------------------

COORDINATE_PAIRS = re.compile(r"<(?:[\w-]+:)?(?:posList|coordinates|pos)\b[^>]*>([^<]+)<")


def first_points(text: str, limit: int = 20) -> list:
    """Some (a, b) pairs of the first geometries of a GML answer, whatever the axis order."""
    points = []
    for match in COORDINATE_PAIRS.finditer(text):
        numbers = [float(x) for x in re.split(r"[\s,]+", match.group(1).strip()) if x]
        points += list(zip(numbers[0::2], numbers[1::2]))[:3]
        if len(points) >= limit:
            break
    return points


def in_area(points: list, box: tuple) -> bool:
    """True when most points fall in box (min lon, min lat, max lon, max lat) in either axis order."""
    if not points:
        return False
    lon0, lat0, lon1, lat1 = box
    inside = sum(1 for a, b in points if (lon0 <= a <= lon1 and lat0 <= b <= lat1)
                 or (lat0 <= a <= lat1 and lon0 <= b <= lon1))
    return inside >= 0.5 * len(points)


def effis_requests(fires: dict, box: tuple) -> list:
    """The variants of the GetFeature request, from the configuration."""
    lon0, lat0, lon1, lat1 = box
    values = {"lon0": f"{lon0:.4f}", "lat0": f"{lat0:.4f}", "lon1": f"{lon1:.4f}", "lat1": f"{lat1:.4f}"}
    out = []
    for variant in fires["variantes"]:
        params = {key: str(value).format(**values) for key, value in variant.items()}
        params.setdefault("service", "WFS")
        params.setdefault("request", "GetFeature")
        out.append(f"{fires['wfs']}?{urllib.parse.urlencode(params, safe=',:')}")
    return out


def land_fires(fs, cfg: dict, landing: str, cells: dict, force: bool) -> tuple:
    fires = cfg["incendios"]
    target = f"{landing}/incendios/effis_area_quemada.gml"
    if fs.exists(target) and not force:
        return 0, 0

    margin = float(fires.get("margen_grados", 0.2)) + float(cfg["celda_grados"]) / 2.0
    lats = [c[0] for c in cells.values()]
    lons = [c[1] for c in cells.values()]
    box = (min(lons) - margin, min(lats) - margin, max(lons) + margin, max(lats) + margin)

    for url in effis_requests(fires, box):
        try:
            raw = fetch(url, timeout=float(fires.get("timeout_s", 600)))
        except Exception as error:  # noqa: BLE001
            logger.warning("EFFIS request failed (%s): %s", url[:160], str(error)[:200])
            continue
        text = raw.decode("utf-8", "replace")
        features = len(re.findall(r"<(?:[\w-]+:)?featureMember\b", text))
        if "ExceptionReport" in text[:3000] or "ServiceException" in text[:3000]:
            logger.warning("EFFIS refused %s: %s", url[:160], text[:300])
            continue
        if not features or not in_area(first_points(text), box):
            logger.warning("EFFIS answer without polygons in the area (%s features): %s", features, url[:160])
            continue
        with fs.open(target, "wb") as file:
            file.write(raw)
        save_json(fs, f"{landing}/incendios/peticion.json",
                  stamp("effis-wfs", url=url, bbox=list(box), poligonos=features, bytes=len(raw)))
        logger.info("EFFIS: %s burnt areas (%s bytes)", features, len(raw))
        return 1, 0

    return 0, 1


# AEMET ----------------------------------------------------------------------

def warning_chunks(first: str, last: str, days: int) -> list:
    """[(start, end)] dates covering [first, last] in pieces of days."""
    from datetime import date, timedelta

    lo, hi = date.fromisoformat(first), date.fromisoformat(last)
    out = []
    while lo <= hi:
        end = min(hi, lo + timedelta(days=days - 1))
        out.append((lo, end))
        lo = end + timedelta(days=1)
    return out


def aemet_url(warnings: dict, lo, hi) -> str:
    """URL of the archive of warnings between two dates (AEMET wants UTC times with the UTC suffix)."""
    return warnings["api"].format(ini=f"{lo.isoformat()}T00:00:00UTC", fin=f"{hi.isoformat()}T23:59:59UTC")


def land_warnings(fs, cfg: dict, landing: str, cells: dict, force: bool) -> tuple:
    """
    Archive of the warnings of adverse weather of AEMET (CAP, from
    18.06.2018, CC BY 4.0) by pieces of dias_por_peticion days (AEMET refuses
    more than 2 days per request and limits the requests per minute, hence
    pausa_s), saved as AEMET gives them (a tar with the CAP messages) in
    avisos/<desde>_<hasta>.tar.
    AEMET OpenData answers first with a JSON that points to the data. Needs the
    free key of opendata.aemet.es in the environment variable clave_env.
    """
    warnings = cfg["avisos"]
    key = os.environ.get(warnings.get("clave_env", "AEMET_API_KEY"), "").strip()
    if not key:
        logger.error("No %s in the environment: the warnings of AEMET need the free key of opendata.aemet.es",
                     warnings.get("clave_env", "AEMET_API_KEY"))
        return 0, 1

    # The cells of the study, so Bronze keeps only the warning zones around them.
    save_json(fs, f"{landing}/celdas.json",
              [{"celda_id": c, "latitud": lat, "longitud": lon} for c, (lat, lon) in sorted(cells.items())])
    headers = {"api_key": key, "Accept": "application/json"}
    done = failed = 0
    for lo, hi in warning_chunks(warnings["desde"], warnings["hasta"], int(warnings.get("dias_por_peticion", 31))):
        target = f"{landing}/avisos/{lo:%Y%m%d}_{hi:%Y%m%d}.tar"
        empty = f"{landing}/avisos/{lo:%Y%m%d}_{hi:%Y%m%d}.vacio.json"
        if (fs.exists(target) or fs.exists(empty)) and not force:
            continue
        try:
            answer = json.loads(fetch(aemet_url(warnings, lo, hi), headers=headers,
                                      timeout=float(warnings.get("timeout_s", 300))).decode("utf-8", "replace"))
            if int(answer.get("estado", 0)) == 404:
                save_json(fs, empty, stamp("aemet-avisos", desde=str(lo), hasta=str(hi), respuesta=answer))
                logger.info("AEMET %s - %s: no warnings", lo, hi)
                continue
            if int(answer.get("estado", 0)) != 200 or not answer.get("datos"):
                raise RuntimeError(f"unexpected answer {str(answer)[:200]}")
            raw = fetch(answer["datos"], headers={"api_key": key}, timeout=float(warnings.get("timeout_s", 300)))
        except Exception as error:  # noqa: BLE001
            failed += 1
            logger.error("AEMET %s - %s not downloaded: %s", lo, hi, str(error)[:200])
            time.sleep(float(warnings.get("pausa_s", 3)))
            continue
        with fs.open(target, "wb") as file:
            file.write(raw)
        done += 1
        logger.info("AEMET %s - %s: %s bytes", lo, hi, len(raw))
        time.sleep(float(warnings.get("pausa_s", 3)))

    return done, failed


# NASA FIRMS -----------------------------------------------------------------

def firms_plan(availability: str, sensors: list, first: str, last: str, days: int) -> list:
    """
    [(source, start date, number of days)]: the pieces of every sensor between
    first and last, from the archive of standard processing (<sensor>_SP)
    while it covers them and from the near real time (<sensor>_NRT) after.
    availability is the CSV of data_availability (data_id,min_date,max_date).
    """
    from datetime import date, timedelta

    ranges = {}
    for line in availability.strip().splitlines()[1:]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3 and parts[1] and parts[2]:
            ranges[parts[0]] = (date.fromisoformat(parts[1]), date.fromisoformat(parts[2]))

    plan = []
    lo, hi = date.fromisoformat(first), date.fromisoformat(last)
    for sensor in sensors:
        day = lo
        while day <= hi:
            end = min(hi, day + timedelta(days=days - 1))
            for kind in ("SP", "NRT"):
                span = ranges.get(f"{sensor}_{kind}")
                if span and span[0] <= day and end <= span[1]:
                    plan.append((f"{sensor}_{kind}", day, (end - day).days + 1))
                    break
            else:
                # A piece across the end of the archive: each day from the source that has it.
                for k in range((end - day).days + 1):
                    one = day + timedelta(days=k)
                    for kind in ("SP", "NRT"):
                        span = ranges.get(f"{sensor}_{kind}")
                        if span and span[0] <= one <= span[1]:
                            plan.append((f"{sensor}_{kind}", one, 1))
                            break
            day = end + timedelta(days=1)
    return plan


def land_hotspots(fs, cfg: dict, landing: str, cells: dict, force: bool) -> tuple:
    """Active fires of FIRMS in the box of the study, one CSV per sensor and piece of days."""
    hotspots = cfg["focos"]
    key = os.environ.get(hotspots.get("clave_env", "MAP_KEY"), "").strip()
    if not key:
        logger.error("No %s in the environment: FIRMS needs its free MAP_KEY", hotspots.get("clave_env", "MAP_KEY"))
        return 0, 1

    margin = float(hotspots.get("margen_grados", 0.3)) + float(cfg["celda_grados"]) / 2.0
    lats = [c[0] for c in cells.values()]
    lons = [c[1] for c in cells.values()]
    area = f"{min(lons) - margin:.2f},{min(lats) - margin:.2f},{max(lons) + margin:.2f},{max(lats) + margin:.2f}"

    def get(url: str, label: str) -> str:
        # The key goes in the URL: the messages name the request by its label.
        try:
            return fetch(url, retries=int(hotspots.get("reintentos", 4)), wait_s=30,
                         timeout=float(hotspots.get("timeout_s", 180)), label=label).decode("utf-8", "replace")
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(str(error).replace(key, "<MAP_KEY>")[:200]) from None

    availability = get(hotspots["disponibilidad"].format(clave=key), "FIRMS data_availability")
    save_json(fs, f"{landing}/focos/disponibilidad.json", stamp("firms", csv=availability, area=area))
    plan = firms_plan(availability, hotspots["sensores"], hotspots["desde"], hotspots["hasta"],
                      int(hotspots.get("dias_por_peticion", 5)))
    done = failed = 0
    for source, day, days in plan:
        target = f"{landing}/focos/{source}/{day:%Y%m%d}_{days}d.csv"
        if fs.exists(target) and not force:
            continue
        url = hotspots["api"].format(clave=key, fuente=source, area=area, dias=days, fecha=day.isoformat())
        try:
            text = get(url, f"FIRMS {source} {day} ({days} days)")
        except RuntimeError as error:
            failed += 1
            logger.error("FIRMS %s %s (%s days) not downloaded: %s", source, day, days, error)
            continue
        if not text.startswith(("latitude", "country_id")):
            failed += 1
            logger.error("FIRMS %s %s: unexpected answer %s", source, day, text[:120].replace(key, "<MAP_KEY>"))
            continue
        with fs.open(target, "w") as file:
            file.write(text)
        done += 1
        time.sleep(float(hotspots.get("pausa_s", 1)))

    logger.info("FIRMS: %s pieces downloaded now, %s missing, area %s", done, failed, area)
    return done, failed


# Copernicus EWDS ---------------------------------------------------------------

def fwi_plan(constraints: list, first: str, last: str, product: str = "reanalysis", data_format: str = "netcdf",
             grid: str = None) -> list:
    """
    [(dataset_type, year, [months])]: the months between first and last by
    year, from the consolidated dataset while it has every day of the month
    and from the intermediate one after. constraints is the constraints.json
    of the dataset (the valid combinations of the request).
    """
    from calendar import monthrange
    from datetime import date

    days = {}
    for entry in constraints:
        if product not in entry.get("product_type", [product]) or data_format not in entry.get("data_format",
                                                                                                [data_format]):
            continue
        if grid and grid not in entry.get("grid", [grid]):
            continue
        for kind in entry.get("dataset_type", []):
            seen = days.setdefault(kind, set())
            for year in entry.get("year", []):
                for month in entry.get("month", []):
                    seen.update((int(year), int(month), int(day)) for day in entry.get("day", []))

    lo, hi = date.fromisoformat(first), date.fromisoformat(last)
    plan = {}
    year, month = lo.year, lo.month
    while (year, month) <= (hi.year, hi.month):
        start = lo.day if (year, month) == (lo.year, lo.month) else 1
        end = hi.day if (year, month) == (hi.year, hi.month) else monthrange(year, month)[1]
        needed = {(year, month, day) for day in range(start, end + 1)}
        for kind in ("consolidated_dataset", "intermediate_dataset"):
            if needed <= days.get(kind, set()):
                plan.setdefault((year, kind), []).append(month)
                break
        else:
            logger.warning("EWDS has no complete data for %s-%02d yet", year, month)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return [(kind, year, months) for (year, kind), months in sorted(plan.items())]


def fwi_area(cells: dict, margin: float, size: float) -> list:
    """[north, west, south, east] of the cells of the study with the margin (the order of the EWDS)."""
    lats = [c[0] for c in cells.values()]
    lons = [c[1] for c in cells.values()]
    half = size / 2.0 + margin
    return [round(max(lats) + half, 2), round(min(lons) - half, 2), round(min(lats) - half, 2),
            round(max(lons) + half, 2)]


def fwi_request(fwi: dict, kind: str, year: int, months: list, area: list) -> dict:
    """Request of one piece; the days that a month does not have are ignored by the EWDS."""
    return {
        "product_type": fwi.get("producto", "reanalysis"),
        "variable": list(fwi["variables"]),
        "dataset_type": kind,
        "system_version": [fwi.get("version", "4_1")],
        "year": [str(year)],
        "month": [f"{m:02d}" for m in months],
        "day": [f"{d:02d}" for d in range(1, 32)],
        "grid": fwi.get("rejilla", "0.25/0.25"),
        "data_format": fwi.get("formato", "netcdf"),
        "area": area,
    }


def fwi_piece(kind: str, year: int, months: list) -> str:
    return f"{year}_{kind.split('_')[0]}_{months[0]:02d}-{months[-1]:02d}"


def land_fwi(fs, cfg: dict, landing: str, cells: dict, force: bool) -> tuple:
    """
    Fire danger indices of CEMS (Fire Weather Index and its parts) from the
    EWDS, one NetCDF per year and dataset type. A request goes to the queue
    of the EWDS and is downloaded when it is ready; peticiones_simultaneas
    wait at once and espera_max_s bounds the whole run (what is left is asked
    in the next one).
    """
    fwi = cfg["fwi"]
    key = os.environ.get(fwi.get("clave_env", "COPERNICUS_EWDS_KEY"), "").strip()
    if not key:
        logger.error("No %s in the environment: the EWDS needs the free Personal Access Token of Copernicus",
                     fwi.get("clave_env", "COPERNICUS_EWDS_KEY"))
        return 0, 1
    api = (os.environ.get(fwi.get("url_env", "COPERNICUS_EWDS_URL"), "").strip() or fwi["api"]).rstrip("/")
    dataset = fwi["conjunto"]
    timeout = float(fwi.get("timeout_s", 300))
    # The token goes in a header; the object store of the files does not need it and never gets it.
    auth = {"PRIVATE-TOKEN": key, "Accept": "application/json"}

    constraints = json.loads(fetch(f"{api}/catalogue/v1/collections/{dataset}/constraints.json", timeout=timeout,
                                   label="EWDS constraints").decode("utf-8"))
    plan = fwi_plan(constraints, fwi["desde"], fwi["hasta"], fwi.get("producto", "reanalysis"),
                    fwi.get("formato", "netcdf"), fwi.get("rejilla"))
    area = fwi_area(cells, float(fwi.get("margen_grados", 0.3)), float(cfg["celda_grados"]))
    queue = []
    for kind, year, months in plan:
        name = fwi_piece(kind, year, months)
        target = f"{landing}/fwi/{name}.nc"
        if fs.exists(target) and not force:
            continue
        queue.append((name, target, fwi_request(fwi, kind, year, months, area)))
    logger.info("EWDS %s: %s pieces in the plan, %s to ask, area %s", dataset, len(plan), len(queue), area)

    done = failed = 0
    running = {}
    deadline = time.time() + float(fwi.get("espera_max_s", 4 * 3600))
    while (queue or running) and time.time() < deadline:
        while queue and len(running) < int(fwi.get("peticiones_simultaneas", 2)):
            name, target, request = queue.pop(0)
            try:
                answer = json.loads(fetch(f"{api}/retrieve/v1/processes/{dataset}/execution",
                                          data=json.dumps({"inputs": request}).encode("utf-8"),
                                          headers={**auth, "Content-Type": "application/json"}, timeout=timeout,
                                          label=f"EWDS {name}").decode("utf-8"))
            except Exception as error:  # noqa: BLE001
                failed += 1
                hint = " (accept the licence of the dataset on its page)" if "licence" in str(error) else ""
                logger.error("EWDS %s not submitted: %s%s", name, str(error)[:300], hint)
                continue
            monitor = [link["href"] for link in answer.get("links", []) if link.get("rel") == "monitor"]
            job = monitor[0] if monitor else f"{api}/retrieve/v1/jobs/{answer['jobID']}"
            running[name] = (job, target, request)
            logger.info("EWDS %s submitted: %s", name, answer.get("status"))

        time.sleep(float(fwi.get("pausa_s", 20)))
        for name in list(running):
            job, target, request = running[name]
            try:
                status = json.loads(fetch(job, headers=auth, timeout=timeout,
                                          label=f"EWDS job {name}").decode("utf-8")).get("status")
            except Exception as error:  # noqa: BLE001
                logger.warning("EWDS %s: no status (%s)", name, str(error)[:200])
                continue
            if status in ("accepted", "running"):
                continue
            del running[name]
            try:
                results = json.loads(fetch(f"{job}/results", headers=auth, timeout=timeout,
                                           label=f"EWDS results {name}").decode("utf-8"))
                if status != "successful":
                    raise RuntimeError(f"state {status}: {str(results)[:300]}")
                asset = results["asset"]["value"]
                raw = fetch(urllib.parse.urljoin(job, asset["href"]), timeout=timeout, label=f"EWDS file {name}")
            except Exception as error:  # noqa: BLE001
                failed += 1
                logger.error("EWDS %s not downloaded: %s", name, str(error)[:300])
                continue
            if not raw.startswith((b"\x89HDF", b"CDF")):
                failed += 1
                logger.error("EWDS %s: the answer is not a NetCDF (%s)", name, raw[:40])
                continue
            with fs.open(target, "wb") as file:
                file.write(raw)
            save_json(fs, target[:-3] + ".json", stamp("cems-fwi", conjunto=dataset, peticion=request,
                                                       bytes=len(raw)))
            done += 1
            logger.info("EWDS %s: %s bytes", name, len(raw))

    if queue or running:
        failed += len(queue) + len(running)
        logger.error("EWDS: %s pieces not finished in espera_max_s; launch again to ask them",
                     len(queue) + len(running))
    return done, failed


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--fuentes", default=",".join(SOURCES), help="Comma separated subset of " + ",".join(SOURCES))
    parser.add_argument("--forzar", action="store_true", help="Download again what is already in Landing")
    parser.add_argument("--max-celdas", type=int, default=None, help="Stop OSM after this many cells")
    parser.add_argument("--anios", default=None, help='Years of PLACSP, in this order ("2025,2024" or "2021-2023")')
    parser.add_argument("--procesos", type=int, default=1, help="Processes to read every PLACSP zip")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)["publicos"]
    sources = [s.strip() for s in args.fuentes.split(",") if s.strip()]
    unknown = set(sources) - set(SOURCES)
    if unknown:
        raise SystemExit(f"Unknown sources: {sorted(unknown)}")

    fs = filesystem()
    landing = cfg["landing"].rstrip("/")
    places = municipality_cells(read_reference(fs, cfg["referencia_municipios"]), float(cfg["celda_grados"]))
    cells = {}
    for place in places:
        cells.setdefault(place["celda_id"], (place["celda_latitud"], place["celda_longitud"]))
    provinces = provinces_of(places)
    logger.info("%s municipalities in %s cells, provinces %s; sources %s", len(places), len(cells), provinces, sources)

    # A source that fails does not stop the others: Bronze loads what is in Landing.
    missing = []
    steps = {
        "ine": lambda: land_ine(fs, cfg, landing, provinces, args.forzar),
        "wikidata": lambda: land_wikidata(fs, cfg, landing, provinces, args.forzar),
        "elevacion": lambda: land_elevation(fs, cfg, landing, places, cells, args.forzar),
        "osm": lambda: land_osm(fs, cfg, landing, cells, args.forzar, args.max_celdas),
        "obras": lambda: land_obras(fs, cfg, landing, args.forzar, args.anios, args.procesos),
        "catastro": lambda: land_catastro(fs, cfg, landing, places, provinces, args.forzar),
        "proyecciones": lambda: land_projections(fs, cfg, landing),
        "aves": lambda: land_birds(fs, cfg, landing, cells, args.forzar),
        "incendios": lambda: land_fires(fs, cfg, landing, cells, args.forzar),
        "avisos": lambda: land_warnings(fs, cfg, landing, cells, args.forzar),
        "focos": lambda: land_hotspots(fs, cfg, landing, cells, args.forzar),
        "fwi": lambda: land_fwi(fs, cfg, landing, cells, args.forzar),
        "calidad_sector": lambda: land_sector_quality(fs, cfg, landing),
    }
    for source in SOURCES:
        if source not in sources:
            continue
        try:
            result = steps[source]()
            if source in ("osm", "obras", "catastro", "aves", "incendios", "avisos", "focos", "fwi") and result[1]:
                logger.error("%s pieces of %s are missing: launch again to ask only those", result[1], source)
                missing.append(source)
        except Exception as error:  # noqa: BLE001
            logger.error("Source %s not landed: %s", source, str(error)[:300])
            missing.append(source)

    if missing:
        logger.error("Sources incomplete: %s. Launch again to ask only what is missing", missing)
        return PARTIAL_EXIT

    logger.info("Landing of the public data complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
