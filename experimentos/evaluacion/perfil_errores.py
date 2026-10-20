"""
Perfil de los errores del modelo del día (memoria, 5.7.2 y 5.7.3): catdes con el error como clase, sobre la
repetición del caso de negocio, el tiempo del reanálisis de la tabla del día y las causas de cada día:

    python perfil_errores.py <dias.csv> <dia.csv.gz> <causas_dias_meteo.csv> <salida/>

dias.csv es la tabla de días de job_ml_caso_negocio (caso_v10_20261014_1543 o una repetición posterior), dia.csv.gz
la de experimentos/perfil_dia/exportar_dia.py y causas_dias_meteo.csv la de job_causas_incidencias.py
(cau_20261012T025409_4681c9). Solo las Distribuidoras 1 y 2, como el caso de negocio. Dos preguntas, con la
previsión real de la víspera y con el reanálisis:

Los días con avería son los del caso de negocio, que ya cuenta solo las incidencias de causa local (sin las de
LOCAL_EXCLUDED de riesgo_zonas.py: aguas arriba, apagón, programadas, maniobras y baja tensión), así que no hace
falta filtrar otra vez por causa.

1. Fallos: días con avería de causa local en nivel bajo frente a los días con avería en alerta (alto o muy alto).
   La memoria dice que son averías de equipos o de causa desconocida que llegan con buen tiempo; la frase se queda si
   al menos dos tercios de los fallos son solo de equipos o desconocida y ninguno tenía aviso de viento o tormenta.
2. Falsas alarmas: días en alerta sin avería frente a los días en alerta con avería, y si el temporal previsto llegó
   (racha del reanálisis frente a la prevista).

Deja perfil_errores.md y las tablas de catdes en CSV. No guarda filas por día.
"""

import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "perfil_dia"))
from perfil import catdes  # noqa: E402

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2"}
LOCAL = ["equipo", "meteorologia", "vegetacion", "fauna", "terceros", "desconocida"]
WEATHER = ["dia_racha_max", "dia_lluvia_max", "ext_racha_clima_max", "dia_humedad_media", "dia_temp_max",
           "dia_temp_media_min", "ayer_racha_max", "hist_incidencias_365d"]
FORECAST = ["prev_racha_max", "prev_ext_racha_clima_max", "prev_lluvia_max"]


def load(cases_path, day_path, causes_path):
    cases = pd.read_csv(cases_path, parse_dates=["fecha"])
    cases = cases.drop(columns=[c for c in ("distribuidora", "bases") if c in cases.columns])
    day = pd.read_csv(day_path, parse_dates=["fecha"], usecols=["distribuidora_id", "fecha"] + WEATHER + FORECAST)
    causes = pd.read_csv(causes_path, parse_dates=["dia"]).rename(columns={"dia": "fecha"})
    causes["causa_dominante"] = causes[LOCAL].idxmax(axis=1).where(causes[LOCAL].sum(axis=1) > 0, "ninguna")
    for c in LOCAL:
        causes[f"hay_{c}"] = (causes[c] > 0).astype(int)
    machine = (causes["equipo"] + causes["desconocida"]) > 0
    causes["equipo_o_desconocida"] = machine.astype(int)
    causes["solo_equipo_o_desconocida"] = (machine & (causes[["meteorologia", "vegetacion", "fauna", "terceros"]]
                                                      .sum(axis=1) == 0)).astype(int)
    keep = ["distribuidora_id", "fecha", "causa_dominante", "equipo_o_desconocida", "solo_equipo_o_desconocida"]
    df = (cases.merge(day, on=["distribuidora_id", "fecha"], how="left")
          .merge(causes[keep + [f"hay_{c}" for c in LOCAL]], on=["distribuidora_id", "fecha"], how="left"))
    df = df[df["distribuidora_id"].isin(NETWORKS)].copy()
    df["averia"] = (df["incidencias"] > 0).astype(int)
    month = df["fecha"].dt.month
    df["estacion"] = np.select([month.isin([12, 1, 2]), month.isin([3, 4, 5]), month.isin([6, 7, 8])],
                               ["invierno", "primavera", "verano"], "otoño")
    df["aviso_viento_tormenta"] = np.where(df[["aviso_viento_nivel", "aviso_tormenta_nivel"]].max(axis=1) > 0,
                                           "con aviso", "sin aviso")
    df["racha_llego"] = np.where(df["dia_racha_max"] >= 0.85 * df["prev_racha_max"], "llegó", "no llegó")
    return df


def misses(d):
    """Días con avería en nivel bajo (fallo = 1) frente a los días con avería en alerta (fallo = 0)."""
    hit = d[d["averia"] == 1].copy()
    hit = hit[(hit["nivel"] == "bajo") | hit["alerta"].astype(bool)]
    hit["fallo"] = (hit["nivel"] == "bajo").astype(int)
    cats = (["causa_dominante", "estacion", "aviso_viento_tormenta"] + [f"hay_{c}" for c in LOCAL]
            + ["equipo_o_desconocida", "solo_equipo_o_desconocida"])
    quanti, _, mods = catdes(hit, "fallo", WEATHER, cats)
    failed = hit[hit["fallo"] == 1]
    summary = {"fallos": len(failed), "aciertos": int((hit["fallo"] == 0).sum()),
               "fallos_con_aviso": int((failed["aviso_viento_tormenta"] == "con aviso").sum()),
               "fallos_solo_equipo_o_desconocida": int(failed["solo_equipo_o_desconocida"].sum()),
               "racha_fallos": failed["dia_racha_max"].mean(),
               "racha_aciertos": hit.loc[hit["fallo"] == 0, "dia_racha_max"].mean()}
    return quanti, mods[mods["modalidad"].astype(str) != "0"], summary


def false_alarms(d, forecast):
    alert = d[d["alerta"].astype(bool)].copy()
    alert["falsa"] = (alert["averia"] == 0).astype(int)
    quanti, _, mods = catdes(alert, "falsa", WEATHER + (FORECAST if forecast else []),
                             ["estacion", "aviso_viento_tormenta"] + (["racha_llego"] if forecast else []))
    summary = {"dias_alerta": len(alert), "falsas": int(alert["falsa"].sum())}
    if forecast:
        ratio = alert["dia_racha_max"] / alert["prev_racha_max"]
        summary.update(razon_mediana_falsas=ratio[alert["falsa"] == 1].median(),
                       razon_mediana_aciertos=ratio[alert["falsa"] == 0].median(),
                       razon_media_falsas=ratio[alert["falsa"] == 1].mean(),
                       razon_media_aciertos=ratio[alert["falsa"] == 0].mean())
        alert["razon"] = ratio
        extra, _, _ = catdes(alert, "falsa", ["razon"], [])
        summary["razon_v_test"] = float(extra["v_test"].iloc[0])
    return quanti, mods, summary


def md(frame, columns):
    shown = frame[columns].copy()
    for c in shown.columns:
        if shown[c].dtype.kind == "f":
            shown[c] = shown[c].map(lambda v: f"{v:.2f}")
    return "\n".join(["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
                     + ["| " + " | ".join(str(v) for v in r) + " |" for r in shown.itertuples(index=False)])


def main(argv) -> int:
    cases_path, day_path, causes_path, out = argv[:4]
    os.makedirs(out, exist_ok=True)
    df = load(cases_path, day_path, causes_path)
    lines = ["# Perfil de los errores del modelo del día", "",
             "Distribuidoras 1 y 2. Fallo: día con avería de causa local en nivel bajo; acierto: día con avería en "
             "alerta. Falsa alarma: día en alerta sin avería.", ""]
    for mode in ("prevision", "reanalisis"):
        d = df[df["modo"] == mode]
        q, m, s = misses(d)
        q.to_csv(os.path.join(out, f"fallos_continuas_{mode}.csv"), index=False)
        m.to_csv(os.path.join(out, f"fallos_modalidades_{mode}.csv"), index=False)
        qa, ma, sa = false_alarms(d, mode == "prevision")
        qa.to_csv(os.path.join(out, f"falsas_continuas_{mode}.csv"), index=False)
        ma.to_csv(os.path.join(out, f"falsas_modalidades_{mode}.csv"), index=False)
        keep = s["fallos_solo_equipo_o_desconocida"] >= 2 * s["fallos"] / 3 and s["fallos_con_aviso"] == 0
        lines += [f"## {'Previsión real de la víspera' if mode == 'prevision' else 'Reanálisis'}", "",
                  f"{len(d)} días de red, {int(d['averia'].sum())} con avería, {int(d['alerta'].sum())} en alerta.", "",
                  f"Fallos: {s['fallos']} días con avería en nivel bajo frente a {s['aciertos']} en alerta; "
                  f"{s['fallos_con_aviso']} con aviso de viento o tormenta, {s['fallos_solo_equipo_o_desconocida']} "
                  f"solo de equipos o de causa desconocida; racha media {s['racha_fallos']:.0f} km/h frente a "
                  f"{s['racha_aciertos']:.0f}. La frase de la memoria {'se sostiene' if keep else 'no se sostiene'}.",
                  "", md(q, ["variable", "v_test", "media_clase", "media_resto", "auc"]), "",
                  md(m, ["variable", "modalidad", "dias", "dias_averia", "cla_mod", "mod_cla", "v_test"]), "",
                  f"Falsas alarmas: {sa['falsas']} de {sa['dias_alerta']} días en alerta."]
        if mode == "prevision":
            lines.append(f"Racha medida frente a la prevista (mediana): {sa['razon_mediana_falsas']:.2f} en las falsas "
                         f"alarmas y {sa['razon_mediana_aciertos']:.2f} en los aciertos (medias "
                         f"{sa['razon_media_falsas']:.2f} y {sa['razon_media_aciertos']:.2f}; valor test "
                         f"{sa['razon_v_test']:.2f}).")
        lines += ["", md(qa, ["variable", "v_test", "media_clase", "media_resto", "auc"]), "",
                  md(ma, ["variable", "modalidad", "dias", "dias_averia", "cla_mod", "mod_cla", "v_test"]), ""]
    text = "\n".join(lines)
    with open(os.path.join(out, "perfil_errores.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
