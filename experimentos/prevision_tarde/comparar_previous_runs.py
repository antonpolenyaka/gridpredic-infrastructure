"""La previsión que guardó cada tarde (Landing vivo/<día>/prevision/<celda>.json, la pasada de ICON-EU de las
20:30 de la víspera) frente a lo que da Previous Runs como previous_day1 (lo previsto unas 24 h antes de cada hora),
que es lo que usa la evaluación. Para cada día guardado y celda: racha máxima del día y lluvia del día (horas GMT).
Solo lectura de MinIO; pide Previous Runs a Open-Meteo (una petición por celda y día). Se ejecuta en el contenedor
de Airflow: docker compose exec airflow python /app/../experimentos/prevision_tarde/comparar_previous_runs.py salida.csv
(o copiándolo antes al contenedor). Resultado del 21.10.2026 en la memoria, 5.4: 344 celdas y días del 14 al 21.10.2026."""
import csv
import json
import statistics as st
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, "/app/jobs/00_landing")
from job_landing_meteo_openmeteo import filesystem  # noqa: E402

OUT = sys.argv[1]
ROOT = "datalake/00_landing/vivo"
fs = filesystem()
days = sorted(p.rsplit("/", 1)[-1] for p in fs.ls(ROOT) if p.rsplit("/", 1)[-1].isdigit())
rows = []
for day in days:
    folder = f"{ROOT}/{day}/prevision"
    if not fs.exists(folder):
        continue
    date = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    for path in sorted(fs.ls(folder)):
        if not path.endswith(".json"):
            continue
        with fs.open(path) as f:
            p = json.load(f)
        req = p.get("_peticion", {})
        hourly = p["hourly"]
        idx = [i for i, t in enumerate(hourly["time"])
               if datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d") == date]
        if not idx:
            continue
        g = [hourly["wind_gusts_10m"][i] for i in idx if hourly["wind_gusts_10m"][i] is not None]
        r = [hourly["precipitation"][i] for i in idx if hourly["precipitation"][i] is not None]
        lat, lon = p["latitude"], p["longitude"]
        url = ("https://previous-runs-api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f"
               "&hourly=wind_gusts_10m_previous_day1,precipitation_previous_day1&models=icon_eu"
               "&start_date=%s&end_date=%s&timezone=GMT&wind_speed_unit=kmh" % (lat, lon, date, date))
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                q = json.loads(resp.read().decode("utf-8"))["hourly"]
        except Exception as e:  # noqa: BLE001
            print("error", day, path, str(e)[:120])
            continue
        g1 = [v for v in q["wind_gusts_10m_previous_day1"] if v is not None]
        r1 = [v for v in q["precipitation_previous_day1"] if v is not None]
        if not g or not g1:
            continue
        rows.append({"dia": date, "celda": req.get("celda_id", path.rsplit("/", 1)[-1][:-5]), "modelo": req.get("modelo"),
                     "pedido_utc": req.get("pedido_utc"), "racha_tarde": max(g), "racha_prev1": max(g1),
                     "lluvia_tarde": round(sum(r), 2), "lluvia_prev1": round(sum(r1), 2)})
        time.sleep(0.3)
with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
d = [x["racha_tarde"] - x["racha_prev1"] for x in rows]
dr = [x["lluvia_tarde"] - x["lluvia_prev1"] for x in rows]
print("dias", sorted({x["dia"] for x in rows}), "celdas-dia", len(rows))
print("racha maxima: diferencia media %.2f km/h, mediana %.2f, media absoluta %.2f, p90 absoluta %.2f, a 5 km/h o menos %.1f %%"
      % (st.mean(d), st.median(d), st.mean(abs(x) for x in d), sorted(abs(x) for x in d)[int(0.9 * len(d))],
         100 * sum(abs(x) <= 5 for x in d) / len(d)))
print("lluvia del dia: diferencia media %.2f mm, media absoluta %.2f, iguales a 1 mm %.1f %%"
      % (st.mean(dr), st.mean(abs(x) for x in dr), 100 * sum(abs(x) <= 1 for x in dr) / len(dr)))
try:
    print("correlacion racha %.3f, lluvia %.3f" % (st.correlation([x["racha_tarde"] for x in rows], [x["racha_prev1"] for x in rows]),
                                                   st.correlation([x["lluvia_tarde"] for x in rows], [x["lluvia_prev1"] for x in rows])))
except Exception as e:  # noqa: BLE001
    print("correlacion", e)
