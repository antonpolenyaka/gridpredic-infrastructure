"""
Perfil de los días con avería del modelo del día (memoria, 5.3.5), con la tabla
que deja exportar_dia.py:

    python correr_perfil.py dia.csv.gz salida/

Días de red del 25.06.2018 (desde el archivo de avisos, como la variante
operativa) al 31.12.2025: 2026 fue el año de la prueba única y queda fuera. El
tiempo del día es el del reanálisis, que es con el que aprende el modelo. Deja
las tablas de catdes en CSV (las tres redes juntas y la Distribuidora 1 y la 2
por separado), la estabilidad entre 2018 - 2021 y 2022 - 2025 y los dos
gráficos de la memoria.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graficos import rate_panels, vtest_bars  # noqa: E402
from perfil import catdes, stability  # noqa: E402

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
SINCE, UNTIL, EXCLUDED = "2018-06-25", "2026-01-01", ["2025-04-28"]
THRESHOLD = 3.29  # p = 0,001 de dos colas; con unas 50 pruebas es también el umbral de Bonferroni del 5 %

NAMES = {
    "dia_racha_max": "Racha máxima, peor celda (km/h)",
    "dia_racha_media_max": "Racha máxima, media de los CT (km/h)",
    "dia_viento_max": "Viento medio máximo, peor celda (km/h)",
    "dia_viento_media_max": "Viento medio máximo, media de los CT (km/h)",
    "dia_lluvia_max": "Lluvia del día, peor celda (mm)",
    "dia_lluvia_media": "Lluvia del día, media de los CT (mm)",
    "dia_lluvia_hora_max": "Lluvia en la peor hora (mm)",
    "dia_temp_max": "Temperatura máxima (°C)",
    "dia_temp_media_min": "Temperatura mínima, media (°C)",
    "dia_humedad_media": "Humedad media (%)",
    "ext_racha_clima_max": "Racha frente al p98 de su celda",
    "ext_racha_clima_media": "Racha frente al p98, media de los CT",
    "ext_racha_ku": "Exceso de racha sobre el p98",
    "ext_dias_calor": "Días seguidos de calor",
    "hist_incidencias_7d": "Incidencias de los 7 días anteriores",
    "hist_incidencias_30d": "Incidencias de los 30 días anteriores",
    "hist_incidencias_365d": "Incidencias del año anterior",
    "hist_dias_desde_ultima": "Días desde la última incidencia",
    "ayer_racha_max": "Racha máxima de ayer (km/h)",
    "ayer_lluvia_max": "Lluvia de ayer (mm)",
    "dia_presion_min": "Presión mínima (hPa)",
    "dia_presion_caida_24h": "Variación de presión en 24 h (hPa)",
    "ext_fraccion_p98_max": "Parte de la red sobre su p98",
    "hie_horas": "Horas con riesgo de hielo",
    "seq_3m": "Balance hídrico de 3 meses",
    "fen_ndvi_anom": "Anomalía del verdor (NDVI)",
    "nao_30d": "Índice NAO de 30 días",
}
UNITS = {
    "dia_racha_max": (" km/h", 0), "dia_racha_media_max": (" km/h", 0), "dia_viento_max": (" km/h", 0),
    "dia_viento_media_max": (" km/h", 0), "dia_lluvia_max": (" mm", 1), "dia_lluvia_media": (" mm", 1),
    "dia_lluvia_hora_max": (" mm", 1), "dia_temp_max": (" °C", 1), "dia_temp_media_min": (" °C", 1),
    "dia_humedad_media": (" %", 0), "ext_racha_clima_max": ("", 2), "ext_racha_clima_media": ("", 2),
    "ext_racha_ku": ("", 2), "ext_dias_calor": ("", 1), "hist_incidencias_7d": ("", 2),
    "hist_incidencias_30d": ("", 1), "hist_incidencias_365d": ("", 0), "hist_dias_desde_ultima": (" días", 0),
    "ayer_racha_max": (" km/h", 0), "ayer_lluvia_max": (" mm", 1), "dia_presion_min": (" hPa", 0),
    "dia_presion_caida_24h": (" hPa", 1), "ext_fraccion_p98_max": ("", 2), "hie_horas": ("", 2),
    "seq_3m": ("", 2), "fen_ndvi_anom": ("", 3), "nao_30d": ("", 2),
}
# La variante operativa: dia_comun, extremos, historia, la racha y la lluvia de ayer (config_ml.json).
# En el gráfico, sin las medias de los CT, que repiten lo que dice la peor celda (las tablas las llevan todas).
CHART = [n for n in NAMES if n not in ("dia_racha_media_max", "dia_viento_media_max", "dia_lluvia_media",
                                       "ext_racha_clima_media")]
IN_MODEL = {"dia_racha_max", "dia_racha_media_max", "dia_viento_max", "dia_viento_media_max", "dia_lluvia_max",
            "dia_lluvia_media", "dia_lluvia_hora_max", "dia_temp_max", "dia_temp_media_min", "dia_humedad_media",
            "ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku", "ext_dias_calor", "hist_incidencias_7d",
            "hist_incidencias_30d", "hist_incidencias_365d", "hist_dias_desde_ultima", "ayer_racha_max",
            "ayer_lluvia_max"}
LEVEL = {0.0: "sin aviso", 1.0: "amarillo", 2.0: "naranja o rojo", 3.0: "naranja o rojo"}
WARNINGS = {"aviso_max_nivel": "Aviso de AEMET (cualquiera)", "aviso_viento_nivel": "Aviso de viento",
            "aviso_lluvia_nivel": "Aviso de lluvia", "aviso_tormenta_nivel": "Aviso de tormenta",
            "aviso_nieve_nivel": "Aviso de nieve"}
DAYS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def prepare(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["fecha"])
    df = df[(df["fecha"] >= SINCE) & (df["fecha"] < UNTIL) & ~df["fecha"].isin(pd.to_datetime(EXCLUDED))]
    df = df[df["y_dia"].notna()].reset_index(drop=True)
    df["red"] = df["distribuidora_id"].map(NETWORKS)
    month = df["fecha"].dt.month
    df["estacion_cat"] = np.select([month.isin([12, 1, 2]), month.isin([3, 4, 5]), month.isin([6, 7, 8])],
                                   ["invierno", "primavera", "verano"], "otoño")
    df["dia_semana"] = df["fecha"].dt.dayofweek.map(dict(enumerate(DAYS)))
    df["fin_de_semana"] = np.where(df["fecha"].dt.dayofweek >= 5, "fin de semana", "laborable")
    df["festivo"] = np.where(df["cal_festivo"] > 0, "festivo", "no festivo")
    for column in WARNINGS:
        df[column + "_cat"] = df[column].map(LEVEL)
    weather = df[["aviso_viento_nivel", "aviso_lluvia_nivel", "aviso_tormenta_nivel"]].max(axis=1, skipna=False)
    df["aviso_vlt_cat"] = weather.map(LEVEL)
    df["tramo_racha_p98"] = pd.cut(df["ext_racha_clima_max"], [0, 0.6, 0.8, 1.0, 1.2, 1.4, 10], right=False,
                                   labels=["< 0,6", "0,6 - 0,8", "0,8 - 1", "1 - 1,2", "1,2 - 1,4", "1,4 o más"])
    df["tramo_lluvia"] = pd.cut(df["dia_lluvia_max"], [-0.001, 0.05, 1, 5, 15, 1e9],
                                labels=["sin lluvia", "hasta 1", "1 - 5", "5 - 15", "más de 15"])
    df["tramo_historia"] = pd.cut(df["hist_incidencias_30d"], [-0.5, 0.5, 1.5, 3.5, 1e9],
                                  labels=["ninguna", "una", "dos o tres", "cuatro o más"])
    return df


def main(argv) -> int:
    path, out = argv[0], argv[1]
    os.makedirs(out, exist_ok=True)
    df = prepare(path)
    numeric = [c for c in NAMES if c in df.columns]
    categorical = (["red", "estacion_cat", "dia_semana", "fin_de_semana", "festivo", "aviso_vlt_cat"]
                   + [w + "_cat" for w in WARNINGS])
    base = float(df["y_dia"].mean())
    print(f"{len(df)} días de red, {int(df['y_dia'].sum())} con avería ({base:.2%})")
    quanti, chi, mods = catdes(df, "y_dia", numeric, categorical)
    quanti.to_csv(os.path.join(out, "continuas_tres_redes.csv"), index=False)
    chi.to_csv(os.path.join(out, "chi2_tres_redes.csv"), index=False)
    mods.to_csv(os.path.join(out, "modalidades_tres_redes.csv"), index=False)
    for nid in (3, 2):
        part = df[df["distribuidora_id"] == nid]
        q, _, m = catdes(part, "y_dia", numeric, [c for c in categorical if c != "red"])
        tag = NETWORKS[nid].lower().replace(" ", "_")
        q.to_csv(os.path.join(out, f"continuas_{tag}.csv"), index=False)
        m.to_csv(os.path.join(out, f"modalidades_{tag}.csv"), index=False)
    both, rho, same = stability(df, "y_dia", numeric, pd.Timestamp("2022-01-01"))
    both.to_csv(os.path.join(out, "estabilidad.csv"))
    print(f"Estabilidad 2018 - 2021 frente a 2022 - 2025: Spearman {rho:.2f}, mismo signo en el {same:.0%}")
    corr = df[numeric].corr(method="spearman")
    corr.to_csv(os.path.join(out, "correlaciones.csv"))
    heading = "La lluvia y el viento del propio día son lo que más separa los días con avería"
    days = f"{len(df):,}".replace(",", ".")
    vtest_bars(quanti, {n: NAMES[n] for n in CHART}, IN_MODEL, UNITS, THRESHOLD, os.path.join(out, "perfil_vtest.png"),
               heading, f"Las tres redes del 25.06.2018 al 31.12.2025: {days} días de red, {int(df['y_dia'].sum())} "
               "con avería. El tiempo del día es el del reanálisis.")
    panels = [("tramo_racha_p98", "Racha frente al p98 de su celda", None),
              ("aviso_vlt_cat", "Aviso de viento, lluvia o tormenta emitido la víspera",
               ["sin aviso", "amarillo", "naranja o rojo"]),
              ("tramo_lluvia", "Lluvia del día en la peor celda (mm)", None),
              ("estacion_cat", "Estación", ["invierno", "primavera", "verano", "otoño"])]
    rows = rate_panels(df, "y_dia", panels, base, os.path.join(out, "perfil_tramos.png"),
                       "La avería sube con el viento, los avisos y la lluvia; con la estación, apenas cambia",
                       "Parte de los días del tramo con alguna avería, con su intervalo de Wilson del 95 %; bajo "
                       "cada tramo, cuántos días tiene.")
    pd.DataFrame(rows).to_csv(os.path.join(out, "tramos.csv"), index=False)
    pd.set_option("display.width", 200)
    print(quanti[["variable", "v_test", "media_clase", "media_resto", "d", "auc"]].round(3).to_string(index=False))
    print(chi.round(4).to_string(index=False))
    print(mods[mods["p_valor"] < 0.001][["variable", "modalidad", "dias", "dias_averia", "cla_mod", "lift", "v_test"]]
          .round(2).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
