"""
Reference table of the official quality-of-supply indices of the Spanish
distribution sector: TIEPI and NIEPI by zone type (urban, semi-urban, rural
concentrated, rural dispersed and total), with the split by cause that the
regulation uses (scheduled transport and distribution; unplanned generation,
transport, third parties, force majeure and own), for Spain, every autonomous
community and every province, for every year the Ministry publishes
(2003 onwards). It is the sector reference the CNMC incentive compares every
distributor with, and the benchmark of the three distributors of the study.

Source: GECOS, Gestion de la calidad de servicio, public data of the
Ministerio para la Transicion Ecologica y el Reto Demografico
(https://energia.serviciosmin.gob.es/Gecos/DatosPublicos/CalidadServicio),
offered for free reuse. The pages build their tables with small GET requests
that return HTML fragments, which this script asks for and parses; nothing
else of the site is touched.

The script runs once a year, when the Ministry publishes a new year, and
writes one small CSV next to the configuration of the public data, which
job_landing_datos_publicos.py copies to Landing (source calidad_sector) and
job_bronze_datos_publicos.py loads in l1_bronze.gecos_indice_zonal:

- gecos_indices_zonales.csv: ambito (espana, comunidad or provincia),
  nombre, anio, zona (U, S, RC, RD, TOTAL; the pages write Total and TOTAL),
  indice (tiepi, tiepie, niepi, niepie: the "e" variants
  are the indices with the exceptions of the Orden ITC/3519/2009, only
  relevant for 2009-2013), potencia_mva, suministros, the seven components
  and the value of the index (hours for TIEPI, interruptions for NIEPI).

    python etl/jobs/00_landing/extract_gecos_indices.py --salida etl/config/01_bronze [--pausa 1.5]
"""

import argparse
import csv
import re
import sys
import time
import urllib.request
from html.parser import HTMLParser

BASE = "https://energia.serviciosmin.gob.es/Gecos/DatosPublicos"
NATIONAL = BASE + "/DescargaDatosIndices?Tipo={tipo}"
AGGREGATED = BASE + "/ConsultaIndicesAgregados?Tipo=anual&Anyo={anio}&Agr={agr}"
KINDS = ["tiepi", "tiepie", "niepi", "niepie"]
COMPONENTS = ["potencia_mva", "suministros", "prog_transporte", "prog_distribucion", "prog_total", "imp_generacion",
              "imp_transporte", "imp_terceros", "imp_fuerza_mayor", "imp_propias", "imp_total", "valor"]
COLUMNS = ["ambito", "nombre", "anio", "zona", "indice", *COMPONENTS]


class Bodies(HTMLParser):
    """Every <tbody> of a fragment as a list of rows, every row a list of cell texts."""

    def __init__(self):
        super().__init__()
        self.tables, self.rows, self.row, self.cell, self.in_cell, self.in_body = [], None, None, "", False, False

    def handle_starttag(self, tag, attrs):
        if tag == "tbody":
            self.in_body, self.rows = True, []
        elif self.in_body and tag == "tr":
            self.row = []
        elif self.in_body and tag == "td":
            self.in_cell, self.cell = True, ""

    def handle_endtag(self, tag):
        if tag == "td" and self.in_cell:
            self.row.append(re.sub(r"\s+", " ", self.cell).strip())
            self.in_cell = False
        elif tag == "tr" and self.row is not None:
            if self.row:
                self.rows.append(self.row)
            self.row = None
        elif tag == "tbody" and self.in_body:
            self.tables.append(self.rows)
            self.in_body = False

    def handle_data(self, data):
        if self.in_cell:
            self.cell += data


def number(text: str):
    """'50.028,850' -> 50028.85; an empty cell -> None."""
    text = text.strip()
    return float(text.replace(".", "").replace(",", ".")) if text else None


def fetch(url: str, retries: int = 3, wait_s: float = 20.0) -> str:
    for attempt in range(retries):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "GridPredict-TFM/1.0"})
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.read().decode("utf-8")
        except Exception as error:  # noqa: BLE001
            print(f"reintento {attempt + 1} de {url}: {error}", file=sys.stderr)
            time.sleep(wait_s)
    raise RuntimeError("GECOS no responde: " + url)


def parse_national(html: str, kind: str) -> list:
    """The table of all the years of Spain: anio, zona and the twelve numbers."""
    parser = Bodies()
    parser.feed(html)
    out = []
    for rows in parser.tables:
        for row in rows:
            if len(row) != 2 + len(COMPONENTS):
                continue
            out.append({"ambito": "espana", "nombre": "España", "anio": int(row[0]), "zona": row[1].upper(), "indice": kind,
                        **{c: number(v) for c, v in zip(COMPONENTS, row[2:])}})
    return out


def parse_aggregated(html: str, year: int, scope: str) -> list:
    """The yearly table by community or province: one tab for TIEPI and one for NIEPI."""
    parser = Bodies()
    parser.feed(html)
    kinds = re.findall(r'<li><a href="#tabs-(\w+)">', html)
    out = []
    for kind, rows in zip(kinds, parser.tables):
        for row in rows:
            if len(row) != 2 + len(COMPONENTS):
                continue
            out.append({"ambito": scope, "nombre": row[0], "anio": year, "zona": row[1].upper(), "indice": kind,
                        **{c: number(v) for c, v in zip(COMPONENTS, row[2:])}})
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--salida", required=True, help="carpeta del CSV (la de config_bronze_publicos.json)")
    parser.add_argument("--pausa", type=float, default=1.5)
    parser.add_argument("--sin-provincias", action="store_true", help="solo España y comunidades")
    args = parser.parse_args(argv)

    records = []
    for kind in KINDS:
        records += parse_national(fetch(NATIONAL.format(tipo=kind)), kind)
        time.sleep(args.pausa)
    years = sorted({r["anio"] for r in records if r["indice"] == "tiepi"})
    print(f"España: {len(records)} filas, años {years[0]} - {years[-1]}", file=sys.stderr)

    scopes = [("ca", "comunidad")] + ([] if args.sin_provincias else [("prov", "provincia")])
    for agr, scope in scopes:
        for year in years:
            rows = parse_aggregated(fetch(AGGREGATED.format(anio=year, agr=agr)), year, scope)
            records += rows
            print(f"{scope} {year}: {len(rows)} filas", file=sys.stderr)
            time.sleep(args.pausa)

    path = f"{args.salida.rstrip('/')}/gecos_indices_zonales.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        for r in records:
            writer.writerow({c: ("" if r.get(c) is None else r[c]) for c in COLUMNS})
    print(f"{len(records)} filas en {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
