"""
Descarga de la serie horaria de ERA5-Land / Open-Meteo (archivo, sin clave)
para unos puntos de cada red: racha y viento a 10 m, lluvia, humedad del
suelo a 0-7 y 7-28 cm y temperatura. Es la entrada de analizar.py.

    python experimentos/suelo_humedad/bajar_suelo.py salida.csv [2021-10-01 2026-08-05]

Los puntos son cabeceras y zonas con CT de cada red (3 = EOSA, 2 = Pitarch);
con cinco puntos la descarga son unos 50 MB y un par de minutos.
"""

import csv
import json
import sys
import time
import urllib.request

POINTS = {
    "3": [("Plasencia", 40.0312, -6.0884), ("Hervas", 40.2708, -5.8672), ("Aldea del Cano", 39.2887, -6.3181)],
    "2": [("Casar de Caceres", 39.5611, -6.4194), ("Alcantara", 39.7189, -6.8837)],
}
VARIABLES = "wind_gusts_10m,wind_speed_10m,precipitation,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,temperature_2m"
URL = ("https://archive-api.open-meteo.com/v1/archive?latitude=%.4f&longitude=%.4f&start_date=%s&end_date=%s"
       "&hourly=" + VARIABLES + "&timezone=Europe%%2FMadrid&wind_speed_unit=kmh")


def ask(url: str, retries: int = 4, wait_s: float = 20.0) -> dict:
    for attempt in range(retries):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "GridPredict-TFM/1.0"})
            with urllib.request.urlopen(request, timeout=120) as response:
                return json.loads(response.read())
        except Exception as error:  # noqa: BLE001
            print("reintento", attempt + 1, error, file=sys.stderr)
            time.sleep(wait_s)
    raise RuntimeError("Open-Meteo no responde: " + url)


def main(argv):
    out = argv[0]
    first = argv[1] if len(argv) > 1 else "2021-10-01"
    last = argv[2] if len(argv) > 2 else "2026-08-05"
    rows = []
    for dist, points in POINTS.items():
        for name, lat, lon in points:
            hourly = ask(URL % (lat, lon, first, last))["hourly"]
            for i, stamp in enumerate(hourly["time"]):
                rows.append({"dist": dist, "punto": name, "hora": stamp, "racha": hourly["wind_gusts_10m"][i],
                             "viento": hourly["wind_speed_10m"][i], "lluvia": hourly["precipitation"][i],
                             "suelo_0_7": hourly["soil_moisture_0_to_7cm"][i], "suelo_7_28": hourly["soil_moisture_7_to_28cm"][i],
                             "temp": hourly["temperature_2m"][i]})
            print(name, len(hourly["time"]), "horas", file=sys.stderr)
            time.sleep(5)
    with open(out, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print("filas", len(rows), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
