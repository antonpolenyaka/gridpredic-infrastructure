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

SOURCES = ["ine", "wikidata", "elevacion", "osm", "obras", "catastro", "proyecciones"]
PARTIAL_EXIT = 4


def load_config(path: str = None) -> dict:
    for candidate in [path] if path else CONFIG_CANDIDATES:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                return json.load(file)

    raise FileNotFoundError(f"config_bronze_publicos.json not found in {CONFIG_CANDIDATES}")


def fetch(url: str, data: bytes = None, headers: dict = None, retries: int = 4, wait_s: float = 30,
          timeout: float = 300) -> bytes:
    """GET (or POST with data) with retries on 429, 5xx and network errors."""
    headers = {"User-Agent": "GridPredict-TFM/1.0", **(headers or {})}

    for attempt in range(1, retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "replace")[:200]
            if error.code in (429, 500, 502, 503, 504) and attempt < retries:
                logger.warning("HTTP %s from %s (%s), waiting %.0f s, attempt %s of %s",
                               error.code, url[:80], body, wait_s * attempt, attempt, retries)
                time.sleep(wait_s * attempt)
                continue
            raise RuntimeError(f"HTTP {error.code} for {url[:120]}: {body}") from error
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt < retries:
                logger.warning("%s for %s, waiting %.0f s, attempt %s of %s", error, url[:80], wait_s * attempt,
                               attempt, retries)
                time.sleep(wait_s * attempt)
                continue
            raise

    raise RuntimeError(f"No answer for {url[:120]}")


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
            request = urllib.request.Request(url, headers={"User-Agent": "GridPredict-TFM/1.0"})
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

def land_projections(fs, cfg: dict, landing: str) -> int:
    """Copies the CSV files of the IEEX projections from the configuration folder to Landing."""
    pr = cfg["proyecciones"]
    folders = [os.path.dirname(c) for c in CONFIG_CANDIDATES]
    done = 0
    for name in pr["ficheros"]:
        source = next((os.path.join(f, name) for f in folders if os.path.exists(os.path.join(f, name))), None)
        if source is None:
            raise FileNotFoundError(f"{name} not found next to config_bronze_publicos.json")
        with open(source, "rb") as file:
            raw = file.read()
        with fs.open(f"{landing}/proyecciones/{name}", "wb") as file:
            file.write(raw)
        done += 1
    save_json(fs, f"{landing}/proyecciones/peticion.json",
              stamp("ieex-proyecciones", origen=pr.get("origen"), ficheros=pr["ficheros"]))
    logger.info("IEEX projections: %s files in Landing", done)
    return done


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
    }
    for source in SOURCES:
        if source not in sources:
            continue
        try:
            result = steps[source]()
            if source in ("osm", "obras", "catastro") and result[1]:
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
