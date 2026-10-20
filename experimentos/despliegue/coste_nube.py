"""
Orden de magnitud del coste mensual de GridPredic en la nube para una distribuidora tipo y lo que paga la partida de
nube del plan financiero (memoria, 3.7 y 3.9), con el extracto fechado de precios de precios_nube.csv (OCI en euros,
08.10.2026; AWS Europe (Spain) en dólares, 11.09 a 18.10.2026; lo saca extraer_precios.py):

    python coste_nube.py [precios_nube.csv] [salida/]

Volúmenes y recursos del repositorio (docs/cifras.md, etl/config, docs/operacion-diaria.md) y de la memoria (3.9):

- B1, IaaS dedicado: el compose sin SQL Server en una máquina virtual E5, el lago en Object Storage.
- B2, PaaS dedicado en OCI: Data Flow para la tarde, el mes nuevo y el CDC, Kafka gestionado, PostgreSQL gestionado
  y Autonomous AI Lakehouse, en una versión baja y otra alta.
- A, SaaS multicliente: una plataforma común sobre IaaS más lo que añade cada cliente.
- La partida de nube del plan (7.200, 9.600 y 12.000 € en los años 1 a 3) frente a lo que paga con la instalación
  en el cliente: los entornos de demo y el plan comercial de la previsión.

Los supuestos que no salen de una lista (horas de Data Flow, tamaño de Kafka y de PostgreSQL, demos por año, el
plan de Open-Meteo de 99$ al mes de su anuncio del 12.06.2023) están marcados en el código. Deja coste_nube.txt.
"""

import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
H = 730  # horas por mes (8.760 / 12)
LINES = []


def say(text=""):
    print(text)
    LINES.append(text)


def num(value, digits=0):
    """Número con punto de miles y coma decimal."""
    return f"{value:,.{digits}f}".replace(",", "x").replace(".", ",").replace("x", ".")


def euros(value, digits=0):
    return f"{value:,.{digits}f}".replace(",", "x").replace(".", ",").replace("x", ".") + " €"


def day(text):
    """dd.mm.aaaa como aaaammdd, para ordenar."""
    return "".join(reversed(text.split(".")))


def load(path):
    with open(path, encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    dates = {}
    for r in rows:
        dates.setdefault(r["proveedor"], set()).add(".".join(reversed(r["lista"].split("-"))))
    return {r["clave"]: float(r["precio"]) for r in rows}, {k: " a ".join(sorted({min(v, key=day), max(v, key=day)}, key=day))
                                                           for k, v in dates.items()}


def main(argv) -> int:
    prices_path = argv[0] if argv else os.path.join(HERE, "precios_nube.csv")
    out = argv[1] if len(argv) > 1 else os.path.join(HERE, "salida")
    P, dates = load(prices_path)
    E5_OCPU, E5_GB = P["B97384"], P["B97385"]
    BV_GB, BV_VPU, OBJ_GB = P["B91961"], P["B91962"], P["B91628"]
    ADB_ECPU, ADB_GB = P["B95701"], P["B95754"]
    KAFKA_FEE, GG_OCPU = P["B110500"], P["B92992"]
    PG_OCPU, PG_GB = P["B99060"], P["B99062"]
    DI_WS, FC_1G = P["B92598"], P["B88325"]
    usd = E5_OCPU / 0.03  # la lista de Oracle en euros es la de dólares por un cambio fijo (0,03 $ el OCPU-h E5)

    say(f"Listas: OCI {dates['OCI']} (EUR), AWS eu-south-2 {dates['AWS eu-south-2']} (USD). Cambio implícito de la "
        f"lista de Oracle: {f'{usd:.3f}'.replace('.', ',')} € por dólar.")

    def vm(ocpu, gb, hours=H):
        return (ocpu * E5_OCPU + gb * E5_GB) * hours

    def block(gb, vpu=10):
        return gb * (BV_GB + vpu * BV_VPU)

    def obj(gb):
        return gb * OBJ_GB

    lake_gb = 110  # 108,8 GiB para las tres redes (docs/cifras.md): cota alta para una distribuidora tipo
    rows = []
    for name, ocpu, gb in (("B1 IaaS ajustada (2 OCPU, 16 GB)", 2, 16), ("B1 IaaS holgada (4 OCPU, 32 GB)", 4, 32)):
        rows.append((name, {"VM 24/7": vm(ocpu, gb), "disco 200 GB": block(200), "objetos 110 GB": obj(lake_gb),
                            "copias del disco (100 GB a precio de objetos)": obj(100), "VPN sitio a sitio": 0.0,
                            "salida de menos de 10 TB": 0.0}))
    evening = vm(3, 24, hours=0.25 * 30)  # supuesto: driver de 1 OCPU y 8 GB más ejecutor de 2 y 16, 15 min al día
    monthly = vm(5, 40, hours=3)          # supuesto: Silver y Gold de un mes nuevo, 3 h con dos ejecutores
    cdc_batch = vm(3, 24, hours=10)       # supuesto: CDC a Bronze por lotes, 4 pasadas de 5 min al día
    cdc_stream = vm(3, 24)                # CDC a Bronze en continuo, 24/7
    kafka_starter = KAFKA_FEE * H + vm(1, 8) + block(50)  # supuesto: un broker de 1 OCPU y 8 GB
    small_vm = vm(1, 8)                   # Airflow, Kafka Connect (Debezium) y nginx
    pg = PG_OCPU * H + 50 * PG_GB         # supuesto: 1 OCPU y 50 GB
    adb_60h = 2 * ADB_ECPU * 60 + 1024 * ADB_GB  # 2 ECPU mínimo, 60 h al mes, 1 TB (el incremento de la FAQ)
    adb_247 = 2 * ADB_ECPU * H + 1024 * ADB_GB
    rows.append(("B2 PaaS baja (CDC por lotes, Kafka de un broker, ADB 60 h)", {
        "objetos 110 GB": obj(lake_gb), "Data Flow tarde": evening, "Data Flow mes": monthly,
        "Data Flow CDC por lotes": cdc_batch, "Kafka gestionado un broker": kafka_starter, "VM pequeña": small_vm,
        "PostgreSQL gestionado": pg, "ADB 60 h": adb_60h}))
    rows.append(("B2 PaaS alta (CDC continuo, Kafka de tres brokers, ADB 24/7)", {
        "objetos 110 GB": obj(lake_gb), "Data Flow tarde": evening, "Data Flow mes": monthly,
        "Data Flow CDC continuo": cdc_stream, "Kafka gestionado de tres brokers": 3 * kafka_starter,
        "VM pequeña": small_vm, "PostgreSQL gestionado": pg, "ADB 24/7": adb_247}))
    say("\n## Coste por distribuidora y mes")
    for name, parts in rows:
        say(f"\n{name}: {euros(sum(parts.values()))} al mes")
        say("   " + "; ".join(f"{k} {euros(v, 1)}" for k, v in parts.items()))
    say(f"\nGoldenGate de 1 OCPU 24/7: {euros(GG_OCPU * H)} al mes; Data Integration 24/7: {euros(DI_WS * H)}; "
        f"FastConnect de 1 Gbps: {euros(FC_1G * H)}")

    shared = 2 * vm(8, 64) + block(400)
    per_client = obj(lake_gb) + evening + monthly + cdc_batch
    say(f"\nA SaaS sobre IaaS: plataforma común {euros(shared)} al mes más {euros(per_client, 1)} por cliente y mes")
    for n in (3, 8, 15):
        say(f"   con {n} clientes: {euros((shared + n * per_client) / n)} por cliente y mes")

    say("\n## La partida de nube del plan")
    client_months = {1: 27000 / 1500, 2: 102000 / 1500, 3: 199500 / 1500}  # licencias del plan / 1.500 € al mes
    budget = {1: 7200, 2: 9600, 3: 12000}
    for name, parts in rows:
        total = sum(parts.values())
        say(f"{name}, con los meses-cliente del plan ({', '.join(f'{v:.0f}' for v in client_months.values())}): "
            + "; ".join(f"año {a} {euros(m * total)}" for a, m in client_months.items()))
    demo_vm = vm(4, 32) + block(500)       # supuesto: SQL Server con las copias del cliente, 500 GB de disco
    open_meteo = 99 * usd                  # supuesto: plan profesional de 99$ al mes (anuncio del 12.06.2023)
    new_clients = {1: 3, 2: 6, 3: 8}       # clientes nuevos del plan; entre una y dos demos de dos meses por cliente
    forecast_months = {1: 9, 2: 12, 3: 12}  # la previsión se paga desde la primera firma (mes 4 del año 1)
    say(f"Con la instalación en el cliente la nube paga las demos (VM de demo {euros(demo_vm)} al mes, dos meses cada "
        f"una) y el plan de la previsión ({euros(open_meteo)} al mes):")
    for a in (1, 2, 3):
        low = new_clients[a] * 2 * demo_vm + forecast_months[a] * open_meteo
        high = 2 * new_clients[a] * 2 * demo_vm + forecast_months[a] * open_meteo
        say(f"   año {a}: {euros(low)} - {euros(high)} frente a {euros(budget[a])} de la partida "
            f"({100 * high / budget[a]:.0f} % como mucho)")

    mwaa_micro, mwaa_small = P["EUS2-Airflow-MicroEnvironment"], P["EUS2-Airflow-SmallEnvironment"]
    emr = 4 * P["EUS2-EMR-SERVERLESS-vCPUHours"] + 16 * P["EUS2-EMR-SERVERLESS-MemoryGBHours"]
    say("\n## AWS Europe (Spain), en dólares")
    say(f"MWAA micro {num(mwaa_micro, 2)}$/h ({num(mwaa_micro * H, 1)}$ al mes); pequeño {num(mwaa_small, 2)}$/h "
        f"({num(mwaa_small * H, 1)}$); MWAA Serverless {num(P['EUS2-MWAA-Serverless-AWSManaged'], 3)}$ por hora de tarea")
    say(f"S3 {num(P['EUS2-TimedStorage-ByteHrs'], 3)}$ el GB al mes ({num(P['EUS2-TimedStorage-ByteHrs'] * lake_gb, 2)}$ "
        f"los 110 GB); PUT {num(P['EUS2-Requests-Tier1'] * 1000, 4)}$ por 1.000; Athena "
        f"{num(P['EUS2-DataScannedInTB'], 2)}$ por TB")
    say(f"EMR Serverless, la tarde (4 vCPU, 16 GB, 15 min al día): {num(emr * 0.25 * 30, 2)}$ al mes; en continuo: "
        f"{num(emr * H, 0)}$")
    say(f"\nLicencia: {euros(18000 / 12)} por cliente y mes")

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "coste_nube.txt"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(LINES) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
