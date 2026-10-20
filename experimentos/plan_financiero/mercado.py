# -*- coding: utf-8 -*-
"""Mercado de GridPredic con la tarifa por tamaño de red (3.3, 3.4.1 y 3.9.1 de la memoria).

Lee la retribución de 2023 de cada distribuidora de la resolución de la CNMC publicada en el
BOE (BOE-A-2026-19941, el texto del PDF sacado con `pdftotext -layout`) y, de un fichero
privado que no se versiona, qué códigos de esa resolución son la Distribuidora 1 y la 2. Con la
retribución por CT de esas dos redes estima los CT de las demás, les aplica la tarifa de
plan_financiero.py y escribe en salida/ solo agregados (sumas redondeadas, recuentos y una
mediana redondeada) que no identifican a ninguna distribuidora.

Uso:
    python mercado.py                      # usa privado/mercado.json
    python mercado.py ruta/a/mercado.json

privado/mercado.json (no se versiona; la carpeta está en .gitignore):
    {"boe_txt": "ruta al texto del BOE",
     "codigos": {"Distribuidora 1": "R1-...", "Distribuidora 2": "R1-..."}}

Salidas: salida/mercado.txt y salida/mercado.json.
"""

import importlib.util
import json
import os
import re
import statistics
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
PRIVADO = os.path.join(AQUI, "privado", "mercado.json")
SALIDA = os.path.join(AQUI, "salida")

spec = importlib.util.spec_from_file_location("plan_financiero", os.path.join(AQUI, "plan_financiero.py"))
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)

GRANDES = 5          # las cinco grandes, fuera del mercado
UN_MILLON = 1e6      # las 92 "de más de un millón"
INCENTIVO = 0.04     # el incentivo de calidad llega al 4 % de la retribución (Circular 8/2025)
REGLA_RAPIDA = 0.0075


def leer_boe(texto):
    """{código R1: retribución en euros} de los anexos I y II de la resolución."""
    L = texto.split("\n")
    i1 = next(i for i, linea in enumerate(L) if linea.strip() == "ANEXO I")
    i2 = next(i for i, linea in enumerate(L) if linea.strip() == "ANEXO II")
    i3 = next(i for i, linea in enumerate(L) if linea.strip() == "ANEXO III")
    out = {}
    for linea in L[i1:i2]:
        m = re.match(r"\s*(R1-\d{3})\s+(.*)", linea)
        if m:
            out[m.group(1)] = int(re.findall(r"-?[\d.]+", m.group(2))[-1].replace(".", ""))
    for linea in L[i2:i3]:
        m = re.match(r"\s*(R1-\d{3})\s+.*?([\d.]+)\s*$", linea)
        if m:
            out.setdefault(m.group(1), int(m.group(2).replace(".", "")))
    return out


def calcular(retribuciones, codigos):
    redes = pf.REDES_CALIBRACION
    r1, r2 = (retribuciones[codigos[n]] for n in ("Distribuidora 1", "Distribuidora 2"))
    c1, c2 = redes["Distribuidora 1"]["ct"], redes["Distribuidora 2"]["ct"]
    retrib_por_ct = (r1 / c1 + r2 / c2) / 2
    senales_por_ct = ((redes["Distribuidora 1"]["senales"] + redes["Distribuidora 2"]["senales"]) / (c1 + c2))
    resto = sorted(retribuciones.values(), reverse=True)[GRANDES:]
    mas_de_un_millon = [x for x in resto if x >= UN_MILLON]

    def lic(x):
        ct = x / retrib_por_ct
        return pf.licencia(ct, ct * senales_por_ct)

    def suma(xs, f):
        return round(sum(f(x) for x in xs), -2)
    cts89 = [x / retrib_por_ct for x in resto[:89]]
    return {
        "distribuidoras_sin_las_grandes": len(resto),
        "con_mas_de_un_millon": len(mas_de_un_millon),
        "tarifa_320": suma(resto, lic),
        "tarifa_mas_de_un_millon": suma(mas_de_un_millon, lic),
        "tarifa_89_mayores": suma(resto[:89], lic),
        "tarifa_52_mayores": suma(resto[:52], lic),
        "regla_320": suma(resto, lambda x: max(6000.0, REGLA_RAPIDA * x)),
        "regla_89_mayores": suma(resto[:89], lambda x: max(6000.0, REGLA_RAPIDA * x)),
        "de_las_89_con_150_ct_o_mas": sum(1 for c in cts89 if c >= 150),
        "de_las_89_con_100_ct_o_mas": sum(1 for c in cts89 if c >= 100),
        "mediana_ct_89": round(statistics.median(cts89)),
        "incentivo_4_pct_mediana_mas_de_un_millon": round(INCENTIVO * statistics.median(mas_de_un_millon), -2),
    }


def texto(c):
    m = pf.fmt
    return "\n".join([
        "Mercado con la tarifa por tamaño de red (CT estimados con la retribución por CT de las Distribuidoras 1 y 2)",
        "",
        f"Distribuidoras con retribución publicada, sin las cinco grandes: {c['distribuidoras_sin_las_grandes']}; "
        f"de más de un millón de euros: {c['con_mas_de_un_millon']}",
        f"Licencias al año con la tarifa: las {c['distribuidoras_sin_las_grandes']}, {m(c['tarifa_320'])} euros; "
        f"las de más de un millón, {m(c['tarifa_mas_de_un_millon'])}; las 89 mayores, {m(c['tarifa_89_mayores'])}; "
        f"las 52 mayores, {m(c['tarifa_52_mayores'])}",
        f"Con la regla rápida (0,75 % de la retribución, mínimo de 6.000 euros): las "
        f"{c['distribuidoras_sin_las_grandes']}, {m(c['regla_320'])}; las 89 mayores, {m(c['regla_89_mayores'])}",
        f"De las 89 mayores: {c['de_las_89_con_150_ct_o_mas']} con 150 CT o más, {c['de_las_89_con_100_ct_o_mas']} con "
        f"100 CT o más; mediana de {c['mediana_ct_89']} CT",
        f"El 4 % de la retribución mediana de las de más de un millón: unos {m(c['incentivo_4_pct_mediana_mas_de_un_millon'])} "
        "euros al año",
    ]) + "\n"


def main():
    ruta = sys.argv[1] if len(sys.argv) > 1 else PRIVADO
    if not os.path.exists(ruta):
        sys.exit(f"Falta {ruta}: es privado y no se versiona (ver la cabecera de este guion).")
    with open(ruta, encoding="utf-8") as f:
        conf = json.load(f)
    with open(conf["boe_txt"], encoding="utf-8") as f:
        retribuciones = leer_boe(f.read())
    c = calcular(retribuciones, conf["codigos"])
    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, "mercado.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(c, f, ensure_ascii=False, indent=1, sort_keys=True)
        f.write("\n")
    with open(os.path.join(SALIDA, "mercado.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write(texto(c))
    print(texto(c))


if __name__ == "__main__":
    main()
