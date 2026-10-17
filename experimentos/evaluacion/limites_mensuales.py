"""
Los meses por encima de los límites mensuales de Extremadura frente a los días
de alerta de la víspera (prueba E8 de la segunda pasada de investigación):

    python limites_mensuales.py limites_mensuales_extremadura.csv dias.csv salida/

El primer fichero lo deja job_analisis_red.py (sección 2, art. 9.6 de la Ley
2/2002 con el Decreto-ley 1/2018: TIEPI mensual de 0,88 h y NIEPI de 0,74 en
zona rural concentrada, 1,32 h y 1,10 en rural dispersa); el segundo es la
tabla de días del caso de negocio. Para cada distribuidora y mes: si alguna de
sus zonas pasó el límite, cuántos días de alerta (alto o muy alto) tuvo y qué
parte de sus horas-cliente cayó en ellos. La pregunta es si los meses que
obligan a dar explicaciones a la Junta son los que la víspera había marcado.
Los nombres de las distribuidoras no salen: los ids pasan a Distribuidora 1, 2
y 3.
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from calibracion_valor import NETWORKS, table  # noqa: E402


def months_of(limits: pd.DataFrame) -> pd.DataFrame:
    """Un mes por distribuidora: supera si alguna de sus zonas con límite lo pasó."""
    limits = limits.copy()
    limits["red"] = limits["distribuidora_id"].map(NETWORKS)
    limits["supera"] = limits["supera"].astype(str).str.lower().isin(["true", "1"])
    return limits.groupby(["red", "anio", "mes"], as_index=False).agg(
        supera=("supera", "any"), tiepi_max_h=("tiepi_h", "max"), niepi_max=("niepi", "max"))


def alert_months(days: pd.DataFrame) -> pd.DataFrame:
    days = days.copy()
    days["red"] = days["distribuidora_id"].map(NETWORKS)
    days["anio"], days["mes"] = days["fecha"].dt.year, days["fecha"].dt.month
    days["en_alerta"] = days["nivel"].isin(["alto", "muy alto"])
    days["horas_cliente_alerta"] = np.where(days["en_alerta"], days["clientes_hora"].fillna(0.0), 0.0)
    return days.groupby(["modo", "red", "anio", "mes"], as_index=False).agg(
        dias=("fecha", "size"), dias_alerta=("en_alerta", "sum"), dias_con_averia=("incidencias", lambda s: int((s > 0).sum())),
        horas_cliente=("clientes_hora", "sum"), horas_cliente_alerta=("horas_cliente_alerta", "sum"))


def summary(joined: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (mode, flag), g in joined.groupby(["modo", "supera"]):
        weight = g["horas_cliente"].sum()
        rows.append({"modo": mode, "meses": "por encima del límite" if flag else "por debajo", "n_meses": len(g),
                     "con_algun_dia_de_alerta": float((g["dias_alerta"] > 0).mean()),
                     "dias_alerta_por_mes": float(g["dias_alerta"].mean()),
                     "parte_horas_cliente_en_alerta": float(g["horas_cliente_alerta"].sum() / weight) if weight else np.nan})
    return pd.DataFrame(rows)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("limites")
    parser.add_argument("dias")
    parser.add_argument("salida")
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)

    days = pd.read_csv(args.dias, parse_dates=["fecha"])
    days = days.drop(columns=[c for c in ("distribuidora", "bases") if c in days.columns])
    joined = alert_months(days).merge(months_of(pd.read_csv(args.limites)), on=["red", "anio", "mes"], how="inner")
    joined.to_csv(os.path.join(args.salida, "meses.csv"), index=False)
    summary_table = summary(joined)
    summary_table.to_csv(os.path.join(args.salida, "resumen.csv"), index=False)
    over = joined[joined["supera"]].sort_values(["modo", "red", "anio", "mes"])
    lines = ["# Meses por encima de los límites mensuales de Extremadura y días de alerta", "",
             table(summary_table, list(summary_table.columns)), "", "## Meses por encima", "",
             table(over, ["modo", "red", "anio", "mes", "tiepi_max_h", "niepi_max", "dias_alerta", "dias_con_averia"])]
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.salida, "resumen.md"), "w", encoding="utf-8") as file:
        file.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
