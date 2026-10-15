"""
Prueba de un umbral de alerta por estación: el sistema marca como alerta los
días por encima del percentil 90 de la probabilidad de los días de
entrenamiento de cada distribuidora (el mismo umbral todo el año). Como la
probabilidad media es menor en verano, en verano hay menos alertas (un 8 % de
los días frente a un 18 % en invierno) aunque las tormentas y los incendios
traen averías. ¿Cubriría más un umbral que reparta las alertas por estación o
por mes (los k días de más riesgo de cada estación, con la misma parte de días
en alerta en total)?

    python experimentos/umbral_estacion/analizar.py dias.csv

Entrada: dias.csv del replay de job_ml_caso_negocio.py. Salida: por modo, la
parte de días en alerta y la cobertura de horas-cliente, incidencias y días
con incidencia de la alerta actual, del mismo número de días elegidos por año,
por año y estación (abril a septiembre frente al resto) y por año y mes.
"""

import sys

import pandas as pd


def coverage(x: pd.DataFrame, mask: pd.Series) -> tuple:
    hours, incidents, days = x["clientes_hora_recortadas"].sum(), x["incidencias"].sum(), (x["incidencias"] > 0).sum()
    return (round(float(mask.mean()), 3), round(float(x.loc[mask, "clientes_hora_recortadas"].sum() / hours), 3),
            round(float(x.loc[mask, "incidencias"].sum() / incidents), 3), round(float((x.loc[mask, "incidencias"] > 0).sum() / days), 3))


def top_by(x: pd.DataFrame, keys: list, share: float) -> pd.Series:
    mask = pd.Series(False, index=x.index)
    for _, s in x.groupby(keys):
        n = int(round(share * len(s)))
        mask.loc[s.sort_values("probabilidad", ascending=False).index[:n]] = True
    return mask


def main(argv):
    d = pd.read_csv(argv[0])
    d = d[d["distribuidora_id"].isin([2, 3])].copy()
    d["mes"] = pd.to_datetime(d["fecha"]).dt.month
    d["verano"] = d["mes"].between(4, 9)
    print("| modo | regla | parte de días en alerta | horas-cliente cubiertas | incidencias cubiertas | días con incidencia cubiertos |")
    print("| --- | --- | --- | --- | --- | --- |")
    for mode, x in d.groupby("modo"):
        share = float(x["alerta"].mean())
        rules = [("alerta actual (umbral fijo)", x["alerta"].astype(bool)), ("mismos días, los de más riesgo de cada año", top_by(x, ["distribuidora_id", "anio"], share)),
                 ("mismos días, por año y estación", top_by(x, ["distribuidora_id", "anio", "verano"], share)),
                 ("mismos días, por año y mes", top_by(x, ["distribuidora_id", "anio", "mes"], share))]
        for name, mask in rules:
            print(f"| {mode} | {name} | " + " | ".join(str(v) for v in coverage(x, mask)) + " |")
    for season, s in d[d["modo"] == "prevision"].groupby("verano"):
        print(f"\n{'verano' if season else 'invierno'} (previsión): {len(s)} días, {s['alerta'].mean():.1%} en alerta, "
              f"{(s['incidencias'] > 0).mean():.1%} con incidencia, probabilidad media {s['probabilidad'].mean():.1%}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
