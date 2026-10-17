"""
Las líneas de alta tensión de las tres redes que la Junta de Extremadura
declaró peligrosas para la avifauna (en zona de protección y sin las medidas
del RD 1432/2008): anexo de la Resolución de 15.11.2021 de la Dirección
General de Sostenibilidad (DOE 227, 25.11.2021):

    http://doe.juntaex.es/pdfs/doe/2021/2270o/21063520.pdf

Lee el texto del PDF (pdftotext -layout) y deja cuántas líneas peligrosas
tiene cada distribuidora en cada término municipal:

    pdftotext -layout 21063520.pdf lineas.txt
    python lineas_rd1432.py lineas.txt etl\\config\\01_bronze\\avifauna_municipios.csv
"""

import json
import os
import re
import sys

import pandas as pd

# Titular en el anexo -> distribuidora_id de TedisNet (Distribuidora 1, 2 y 3). La razón social de cada titular no
# se versiona: va en titulares.json, junto a este script, como {"razón social del anexo": distribuidora_id}.
TITULARES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "titulares.json")


def owners_and_pattern():
    with open(TITULARES, encoding="utf-8") as handle:
        owners = {k: int(v) for k, v in json.load(handle).items()}
    row = re.compile(r"(?P<owner>" + "|".join(re.escape(o).replace(r",\ SL", r",?\s+SL") for o in owners) + r")"
                     r"\s+(?P<anio>(19|20)\d\d)\s+(?P<kv>[\d.,]+)\s+(?P<cat>\d)\s+(?P<muni>.+?)(\s{2,}.*)?$")
    return owners, row


def municipality(name: str) -> str:
    """'Granja, La' pasa a 'La Granja', como se escriben los municipios de los CT."""
    name = name.strip()
    if ", " in name:
        base, article = name.rsplit(", ", 1)
        if article.lower() in ("la", "el", "los", "las"):
            return f"{article} {base}"
    return name


def main(argv):
    owners, row_pattern = owners_and_pattern()
    rows = []
    for line in open(argv[0], encoding="utf-8"):
        match = row_pattern.search(line.rstrip("\n"))
        if not match:
            continue
        owner = next(o for o in owners if match.group("owner").replace(" ", "")[:12] == o.replace(" ", "")[:12])
        rows.append({"distribuidora_id": owners[owner], "municipio": municipality(match.group("muni")),
                     "anio_linea": int(match.group("anio")), "kv": float(match.group("kv").replace(",", "."))})
    lines = pd.DataFrame(rows)
    out = (lines.groupby(["distribuidora_id", "municipio"])
           .agg(lineas_rd1432=("anio_linea", "size"), anio_linea_media=("anio_linea", "mean")).reset_index())
    out.round(1).to_csv(argv[1], index=False)
    print(len(lines), "lineas;", out.groupby("distribuidora_id")["lineas_rd1432"].sum().to_dict())


if __name__ == "__main__":
    main(sys.argv[1:])
