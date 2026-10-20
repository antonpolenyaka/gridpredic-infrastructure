"""
Extracto fechado de las listas de precios de nube que usa coste_nube.py (precios_nube.csv), sacado de las listas
públicas descargadas:

    python extraer_precios.py <oci_products_eur.json> <carpeta con los JSON de AWS> [precios_nube.csv]

- OCI: la API pública de la calculadora de costes de Oracle en euros,
  https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/?currencyCode=EUR (lastUpdated 2026-10-08), el
  precio de pago por uso (el último tramo cuando hay tramo gratuito).
- AWS: los ficheros de oferta de la región Europe (Spain), eu-south-2, en dólares,
  https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<servicio>/current/eu-south-2/index.json para
  AmazonMWAA, AmazonS3, ElasticMapReduce y AmazonAthena (publicados entre el 11.09 y el 18.10.2026), el primer tramo
  bajo demanda.

Las listas completas no se versionan (pesan 1,4 MB y cambian): el CSV guarda solo las 21 líneas que se usan, con la
fecha de cada lista, para que el cálculo se repita sin descargar nada.
"""

import csv
import json
import os
import sys

OCI = {
    "B97384": "Compute - Standard - E5 - OCPU (OCPU por hora)",
    "B97385": "Compute - Standard - E5 - Memory (GB por hora)",
    "B91961": "Block Volume - Storage (GB al mes)",
    "B91962": "Block Volume - Performance Units (VPU por GB al mes)",
    "B91628": "Object Storage - Storage (GB al mes)",
    "B95701": "Autonomous AI Lakehouse - ECPU (ECPU por hora)",
    "B95754": "Autonomous AI Database Storage (GB al mes)",
    "B110500": "Streaming with Apache Kafka (OCPU por hora)",
    "B92992": "GoldenGate (OCPU por hora)",
    "B99060": "Database with PostgreSQL - X86 (OCPU por hora)",
    "B99062": "Database Optimized Storage (GB al mes)",
    "B92598": "Data Integration - Workspace Usage (hora)",
    "B88325": "FastConnect 1 Gbps (puerto por hora)",
}
AWS = {
    ("AmazonMWAA", "EUS2-Airflow-MicroEnvironment"): "MWAA, entorno micro (hora)",
    ("AmazonMWAA", "EUS2-Airflow-SmallEnvironment"): "MWAA, entorno pequeño (hora)",
    ("AmazonMWAA", "EUS2-MWAA-Serverless-AWSManaged"): "MWAA Serverless (hora de tarea)",
    ("AmazonS3", "EUS2-TimedStorage-ByteHrs"): "S3 Standard (GB al mes)",
    ("AmazonS3", "EUS2-Requests-Tier1"): "S3 PUT (petición)",
    ("ElasticMapReduce", "EUS2-EMR-SERVERLESS-vCPUHours"): "EMR Serverless (vCPU por hora)",
    ("ElasticMapReduce", "EUS2-EMR-SERVERLESS-MemoryGBHours"): "EMR Serverless (GB por hora)",
    ("AmazonAthena", "EUS2-DataScannedInTB"): "Athena (TB leído)",
}


def oci_rows(path):
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    rows = []
    for item in data["items"]:
        if item["partNumber"] not in OCI:
            continue
        for loc in item.get("currencyCodeLocalizations", []):
            if loc["currencyCode"] == "EUR":
                paid = [p for p in loc["prices"] if p["model"] == "PAY_AS_YOU_GO"]
                rows.append({"proveedor": "OCI", "servicio": item["serviceCategory"], "clave": item["partNumber"],
                             "descripcion": OCI[item["partNumber"]], "moneda": "EUR", "precio": paid[-1]["value"],
                             "lista": data["lastUpdated"][:10]})
    return rows


def aws_rows(folder):
    rows = []
    for (service, usage), text in AWS.items():
        with open(os.path.join(folder, f"{service}.json"), encoding="utf-8") as handle:
            data = json.load(handle)
        price = None
        for sku, product in data["products"].items():
            if product.get("attributes", {}).get("usagetype") != usage:
                continue
            for term in data["terms"]["OnDemand"].get(sku, {}).values():
                for dim in term["priceDimensions"].values():
                    if dim.get("beginRange") in ("0", None) and price is None:
                        price = float(dim["pricePerUnit"]["USD"])
        if price is None:
            raise KeyError(usage)
        rows.append({"proveedor": "AWS eu-south-2", "servicio": service, "clave": usage, "descripcion": text,
                     "moneda": "USD", "precio": price, "lista": data["publicationDate"][:10]})
    return rows


def main(argv) -> int:
    out = argv[2] if len(argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "precios_nube.csv")
    rows = oci_rows(argv[0]) + aws_rows(argv[1])
    if len(rows) != len(OCI) + len(AWS):
        raise ValueError(f"faltan precios: {len(rows)} de {len(OCI) + len(AWS)}")
    with open(out, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} precios en {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
