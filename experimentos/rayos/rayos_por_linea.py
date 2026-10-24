"""
La regla de los rayos medida por línea frente a la red entera (memoria, 5.7.4; docs/causas-incidencias.md 8 y 10). La
columna "Próx. 3 h" del cuadro de mando multiplica por 15 el riesgo de las líneas (grupos de red) con algún CT en un
municipio a 10 km o menos de un rayo de la última hora (near_network de etl/jobs/00_landing/job_landing_rayos.py y
RULE.rayos de dashboard/index.html). El 15 sale del experimento de rayos del 12.10.2026: hubo rayos a menos de 10 km de
la red de la distribuidora en la hora anterior al 17 % de las 46 incidencias de agosto de 2024 a noviembre de 2025,
frente al 1,1 % de las ventanas de control (causas-incidencias.md 8). Las ventanas de control no están en la
exportación, así que la tasa base no se recalcula aquí.

Con las incidencias de Calser de la exportación de Gold en la misma ventana, los rayos de aquel experimento y la hora de
inicio de Calser, que es local, pasada a UTC con el horario de Madrid:

1. Escala de red, con los puntos de los municipios de los CT de cada distribuidora, como en vivo: cuántas incidencias
   tuvieron algún rayo a 10 km o menos en la hora anterior, con el intervalo de Wilson al 95 %.
2. Escala de línea, en esas incidencias: cuántas líneas de su red habría marcado la regla, si alguna línea de la
   incidencia estaba entre ellas y la razón entre ese acierto y el que se tendría marcando al azar el mismo número de
   líneas de la misma red (probabilidad hipergeométrica de que alguna línea afectada caiga entre las marcadas). Con
   una razón cerca de 1 la regla no distingue la línea.

    python rayos_por_linea.py <carpeta de la exportación de Gold> <flashes.csv> <salida/>

La exportación de Gold es zonas_20261013_1041 (_runlogs/ml_resultados del portátil; lee dim_ct.parquet,
map_ct_celda.parquet e interrupciones.parquet) y flashes.csv son los 19.318 flashes del Lightning Imager de MTG del
experimento de rayos (_runlogs/ml_resultados/rayos_20261012_2046). Ninguno de los dos se sube a git: la exportación
lleva nombres de municipios y cabeceras, y los rayos ocupan 4,4 MB. Deja rayos_por_linea.txt y rayos_por_linea.csv, con
una fila por incidencia con rayos cerca de la red numerada por orden de inicio y solo recuentos de líneas: sin
identificadores, fechas ni nombres. Tarda un par de segundos.
"""

import os
import sys
from math import comb

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "perfil_dia"))
from perfil import wilson  # noqa: E402

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
RADIUS_KM = 10.0
HOUR = np.timedelta64(1, "h")  # la hora anterior al inicio (pd.Timedelta avisa de una unidad obsoleta con numpy 2.5)
WINDOW = ("2024-08-01", "2025-12-01")  # agosto de 2024 a noviembre de 2025, la muestra del experimento


def haversine_km(lat1, lon1, lat2, lon2, r: float = 6371.0):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    h = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(h))


def load(folder: str, flashes_path: str):
    dim = pd.read_parquet(os.path.join(folder, "dim_ct.parquet"), columns=["distribuidora_id", "ct_id", "grupo_red_id"])
    cts = pd.read_parquet(os.path.join(folder, "map_ct_celda.parquet"),
                          columns=["distribuidora_id", "ct_id", "codigo_ine", "ct_lat_municipio", "ct_lon_municipio"])
    cts = cts.merge(dim, on=["distribuidora_id", "ct_id"], how="left").dropna(subset=["grupo_red_id"])
    rows = pd.read_parquet(os.path.join(folder, "interrupciones.parquet"),
                           columns=["distribuidora_id", "periodo_id", "incidencia_id", "ct_id", "inicio_ts"])
    flashes = pd.read_csv(flashes_path, usecols=["ts_utc", "lat", "lon"])
    flashes["ts"] = pd.to_datetime(flashes["ts_utc"], format="ISO8601", utc=True).dt.tz_localize(None)
    return cts, rows, flashes


def measure(cts: pd.DataFrame, rows: pd.DataFrame, flashes: pd.DataFrame) -> pd.DataFrame:
    key = ["distribuidora_id", "periodo_id", "incidencia_id"]
    inc = rows.groupby(key).agg(inicio=("inicio_ts", "min")).reset_index()
    inc = inc[(inc["inicio"] >= WINDOW[0]) & (inc["inicio"] < WINDOW[1])].sort_values("inicio").reset_index(drop=True)
    inc["inicio_utc"] = (inc["inicio"].dt.tz_localize("Europe/Madrid", ambiguous=True, nonexistent="shift_forward")
                         .dt.tz_convert("UTC").dt.tz_localize(None))
    # Cada municipio, su punto y las líneas con algún CT en él (lo que riesgo.json guarda en municipios[].zonas)
    places = (cts.groupby(["distribuidora_id", "codigo_ine"])
              .agg(lat=("ct_lat_municipio", "first"), lon=("ct_lon_municipio", "first"),
                   lineas=("grupo_red_id", lambda s: sorted(set(s)))).reset_index())
    lines_of = cts.groupby("distribuidora_id")["grupo_red_id"].nunique()
    line_of_ct = dict(zip(zip(cts["distribuidora_id"], cts["ct_id"]), cts["grupo_red_id"]))
    out = []
    for order, r in enumerate(inc.itertuples(index=False), start=1):
        t0 = r.inicio_utc
        recent = flashes[(flashes["ts"] >= t0 - HOUR) & (flashes["ts"] < t0)]
        mine = places[places["distribuidora_id"] == r.distribuidora_id]
        flagged, near = set(), False
        if len(recent) and len(mine):
            d = haversine_km(recent["lat"].to_numpy()[:, None], recent["lon"].to_numpy()[:, None],
                             mine["lat"].to_numpy()[None, :], mine["lon"].to_numpy()[None, :])
            hit = (d <= RADIUS_KM).any(axis=0)
            near = bool(hit.any())
            for lines in mine.loc[hit, "lineas"]:
                flagged.update(lines)
        own = rows[(rows["distribuidora_id"] == r.distribuidora_id) & (rows["periodo_id"] == r.periodo_id)
                   & (rows["incidencia_id"] == r.incidencia_id)]["ct_id"]
        affected = {line_of_ct[(r.distribuidora_id, ct)] for ct in own if (r.distribuidora_id, ct) in line_of_ct}
        out.append({"orden": order, "red": NETWORKS.get(r.distribuidora_id, str(r.distribuidora_id)),
                    "rayos_cerca_red": near, "lineas_red": int(lines_of.get(r.distribuidora_id, 0)),
                    "lineas_marcadas": len(flagged), "lineas_afectadas": len(affected),
                    "afectada_marcada": bool(affected & flagged)})
    return pd.DataFrame(out)


def report(table: pd.DataFrame) -> str:
    n = len(table)
    near = table[table["rayos_cerca_red"]]
    k = len(near)
    lo, hi = wilson(k, n)
    by_network = table.groupby("red").size().to_dict()
    lines = [f"Incidencias de Calser de agosto de 2024 a noviembre de 2025 en la exportación: {n} "
             f"({', '.join(f'{name}: {count}' for name, count in sorted(by_network.items()))}).", "",
             f"1. Escala de red: algún rayo a {RADIUS_KM:g} km o menos de los municipios de su red en la hora anterior "
             f"en {k} de {n} ({100 * k / n:.1f} %; Wilson al 95 %, de {100 * lo:.1f} a {100 * hi:.1f} %)."]
    if k:
        hits = int(near["afectada_marcada"].sum())
        hit_rate = hits / k
        share = float((near["lineas_marcadas"] / near["lineas_red"]).mean())
        # the same question by chance: marking as many lines at random, would some affected line be among them?
        chance = float(near.apply(lambda r: 1.0 - comb(int(r.lineas_red - r.lineas_afectadas), int(r.lineas_marcadas))
                                  / comb(int(r.lineas_red), int(r.lineas_marcadas)), axis=1).mean())
        lo2, hi2 = wilson(hits, k)
        whole = near[near["lineas_marcadas"] == near["lineas_red"]]
        lines += [f"2. Escala de línea, en esas {k}: la regla habría marcado de media {near['lineas_marcadas'].mean():.1f} "
                  f"de las {near['lineas_red'].mean():.1f} líneas de su red ({100 * share:.0f} %), y alguna línea de la "
                  f"incidencia estaba entre las marcadas en {hits} de {k} ({100 * hit_rate:.0f} %; Wilson al 95 %, de "
                  f"{100 * lo2:.0f} a {100 * hi2:.0f} %). Marcando al azar el mismo número de líneas de la misma red, alguna "
                  f"afectada habría caído entre las marcadas en el {100 * chance:.0f} % de los casos: razón {hit_rate / chance:.2f}. Incidencias con todas las líneas de su red marcadas: "
                  f"{len(whole)} ({'; '.join(f'{r.red}: {r.lineas_red} de {r.lineas_red}' for r in whole.itertuples())}).",
                  "",
                  near.drop(columns="rayos_cerca_red").to_string(index=False)]
    return "\n".join(lines)


def main(argv) -> int:
    folder, flashes_path, out = argv[:3]
    os.makedirs(out, exist_ok=True)
    cts, rows, flashes = load(folder, flashes_path)
    table = measure(cts, rows, flashes)
    table[table["rayos_cerca_red"]].drop(columns="rayos_cerca_red").to_csv(
        os.path.join(out, "rayos_por_linea.csv"), index=False)
    text = report(table)
    with open(os.path.join(out, "rayos_por_linea.txt"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
