"""
Reference tables of the population projections 2024-2039 of the Instituto de
Estadistica de Extremadura (IEEX) by Agrupacion Municipal Estadistica (AME,
groups of municipalities based on the mancomunidades; the three capitals go
alone), the only official projection below the province.

The IEEX publishes them as Excel 97 files inside 7z archives and the list of
municipalities of every AME only in a PDF map, formats the Spark image does
not read. This script runs once on a computer with xlrd, py7zr and pdfplumber
(pip install xlrd py7zr pdfplumber) and writes two small CSV files next to the
configuration of the public data, which job_landing_datos_publicos.py copies
to Landing (source proyecciones):

- ieex_proyecciones_ame.csv: AME, year and population (both sexes) of every
  AME from 2024 to 2039.
- ieex_ame_municipios.csv: INE code, name and AME of every municipality of
  Badajoz and Caceres. The names of the map are matched with the Padron of
  the INE; the two names the map shortens are fixed by hand (ALIASES).

The projections are published every few years: run it again when the IEEX
publishes new ones.

    python etl/jobs/00_landing/extract_ieex_proyecciones.py --salida etl/config/01_bronze
"""

import argparse
import csv
import io
import json
import os
import re
import tempfile
import unicodedata
import urllib.request

BASE = "https://www.juntaex.es/documents/77055/37614982/"
ARCHIVES = ["prov_ba.7z", "prov_cc.7z"]
MAP = "mapa.pdf"
INE = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{tabla}?nult=1&tip=M"
INE_TABLES = {"06": 2859, "10": 2863}
# Names of the map that are not the INE name.
ALIASES = {("10", "higuera"): "10097", ("10", "collado"): "10065"}
CAPITALS = {"0615": "06015", "0616": "06083", "1015": "10037"}


def get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "GridPredict-TFM/1.0"})
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read()


def tidy(name: str) -> str:
    """Names of the IEEX with plain hyphens and single spaces ("LAS VILLUERCAS – LOS IBORES –   LA JARA")."""
    text = re.sub(r"\s*[\u2013\u2014-]\s*", " - ", str(name or ""))
    return re.sub(r"\s+", " ", text).strip()


def plain(name: str) -> str:
    text = str(name or "").strip()
    moved = re.match(r"^(.*?)\s*[,(]\s*(el|la|los|las)\s*\)?\s*$", text, re.I)
    if moved:
        text = f"{moved.group(2)} {moved.group(1)}"
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z]", "", text)


def projections(raw_archives: dict) -> list:
    """Rows (ame, name, year, population) of the 'Ambos sexos' line of every AME workbook."""
    import py7zr
    import xlrd

    rows = []
    for raw in raw_archives.values():
        with tempfile.TemporaryDirectory() as folder:
            with py7zr.SevenZipFile(io.BytesIO(raw), "r") as archive:
                archive.extractall(folder)
            for root, _, files in os.walk(folder):
                for name in sorted(files):
                    if not name.lower().endswith(".xls"):
                        continue
                    code = os.path.splitext(name)[0]
                    sheet = xlrd.open_workbook(os.path.join(root, name)).sheet_by_index(0)
                    title = tidy(str(sheet.cell_value(0, 0)).split(":", 1)[-1])
                    header = next(r for r in range(sheet.nrows) if sheet.cell_value(r, 0) == "Población")
                    years = [int(float(v)) for v in sheet.row_values(header)[1:] if str(v).strip()]
                    total = next(r for r in range(header + 1, sheet.nrows) if sheet.cell_value(r, 0) == "Ambos sexos")
                    for year, value in zip(years, sheet.row_values(total)[1:]):
                        rows.append({"ame_codigo": code, "ame_nombre": title, "anio": year,
                                     "poblacion": round(float(value), 1)})
    return rows


def memberships(raw_map: bytes, ine_names: dict) -> list:
    """Municipalities of every AME from the second page of the map (five columns, heading 'NNNN:NAME')."""
    import pdfplumber

    with pdfplumber.open(io.BytesIO(raw_map)) as pdf:
        words = pdf.pages[1].extract_words(keep_blank_chars=True, x_tolerance=1.5, y_tolerance=2)
    rows, names = [], {}
    for x in sorted({round(w["x0"]) for w in words}):
        code, heading = None, False
        for word in sorted((w for w in words if round(w["x0"]) == x), key=lambda w: w["top"]):
            text = word["text"].strip()
            match = re.match(r"^(\d{4}):\s*(.*)$", text)
            if match:
                code, heading = match.group(1), True
                names[code] = match.group(2).strip()
                continue
            if heading and text == text.upper() and re.search(r"[A-ZÁÉÍÓÚÑ]", text):
                names[code] = f"{names[code]} {text}".strip()
                continue
            heading = False
            if code:
                rows.append((code, text))

    out, missing = [], []
    for code, name in rows:
        key = (code[:2], plain(name))
        ine = ALIASES.get(key) or ine_names.get(key)
        if ine is None:
            missing.append(name)
            continue
        out.append({"codigo_ine": ine, "municipio": name, "ame_codigo": code, "ame_nombre": tidy(names[code])})
    for ame, ine in CAPITALS.items():
        if not any(r["codigo_ine"] == ine for r in out):
            out.append({"codigo_ine": ine, "municipio": names.get(ame, ame).title(), "ame_codigo": ame,
                        "ame_nombre": tidy(names.get(ame, ame))})
    if missing:
        raise RuntimeError(f"Municipalities of the map without INE code: {missing}")
    return sorted(out, key=lambda r: r["codigo_ine"])


def ine_codes() -> dict:
    names = {}
    for province, table in INE_TABLES.items():
        for series in json.loads(get(INE.format(tabla=table))):
            meta = {m.get("FK_Variable"): m for m in series.get("MetaData", [])}
            town = meta.get(19)
            if town:
                names[(province, plain(town["Nombre"]))] = str(town["Codigo"]).zfill(5)
    return names


def write(path: str, rows: list):
    with open(path, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{path}: {len(rows)} rows")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", default=os.path.join("etl", "config", "01_bronze"))
    args = parser.parse_args()

    archives = {name: get(BASE + name) for name in ARCHIVES}
    write(os.path.join(args.salida, "ieex_proyecciones_ame.csv"), projections(archives))
    write(os.path.join(args.salida, "ieex_ame_municipios.csv"), memberships(get(BASE + MAP), ine_codes()))


if __name__ == "__main__":
    main()
