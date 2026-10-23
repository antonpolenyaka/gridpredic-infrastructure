"""
Colinealidad de la variante operativa del modelo del día y lo que hace con los motivos del parte de guardia.

1. Signo de cada coeficiente en la evaluación por años (2022 - 2026, cada año con un modelo ajustado con los
   anteriores), con la tubería de producción (mediana con indicador, logaritmo con signo, estandarizar, L2) y la C
   que elige la tarde: la de más PR-AUC en el último 25 % de los días de entrenamiento, dentro de la rejilla de
   producción (0,003 a 0,1; en estos datos sale siempre 0,003). Es una aproximación de coeficientes_estabilidad.csv
   de job_ml_riesgo_dia (0.16.0).
2. Con el modelo ajustado con todos los días hasta el 31.12.2025, la suma de los términos de cada grupo de GROUPS
   (los grupos de job_ml_riesgo_manana.py) en los días que el modelo pone en el 10 % de más riesgo: cuántas veces el
   multiplicador que se enseña de "viento" o "clima" queda inflado porque el otro grupo del mismo factor resta.
3. Número de condición y VIF de las 16 variables meteorológicas en la escala del modelo.

Uso: python coeficientes_motivos.py <dia.csv.gz> <carpeta de salida>

dia.csv.gz es la tabla de días de red que deja exportar_dia.py. Escribe coeficientes_motivos.txt (lo que imprime),
coeficientes_anios.csv y coeficientes_estabilidad_aprox.csv en la carpeta de salida.
"""

import math
import os
import sys
import warnings

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

warnings.filterwarnings("ignore")

OUT = sys.argv[2] if len(sys.argv) > 2 else "salida"
os.makedirs(OUT, exist_ok=True)
_log = open(os.path.join(OUT, "coeficientes_motivos.txt"), "w", encoding="utf-8")


def print(*args, **kwargs):  # noqa: A001
    import builtins
    builtins.print(*args, **kwargs)
    builtins.print(*args, **{k: v for k, v in kwargs.items() if k != "file"}, file=_log)


df = pd.read_csv(sys.argv[1], parse_dates=["fecha"])
df = df[(df["fecha"] >= "2018-06-25") & (df["fecha"] != "2025-04-28") & df["y_dia"].notna()].copy()
df["anio"] = df["fecha"].dt.year
DIST = ["dist_1366", "dist_2", "dist_3"]
CAL = ["cal_dia_anio_cos", "cal_dia_anio_sin", "cal_dia_semana_cos", "cal_dia_semana_sin", "cal_festivo",
       "cal_fin_de_semana", "estacion"]
HIST = ["hist_incidencias_7d", "hist_incidencias_30d", "hist_incidencias_365d", "hist_dias_desde_ultima"]
AVISOS = ["aviso_max_nivel", "aviso_viento_nivel", "aviso_lluvia_nivel", "aviso_tormenta_nivel", "aviso_nieve_nivel"]
DIA = ["dia_racha_max", "dia_racha_media_max", "dia_viento_max", "dia_viento_media_max", "dia_lluvia_max",
       "dia_lluvia_media", "dia_lluvia_hora_max", "dia_temp_max", "dia_temp_media_min", "dia_humedad_media"]
EXT = ["ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku", "ext_dias_calor"]
AYER = ["ayer_racha_max", "ayer_lluvia_max"]
FEATURES = DIST + CAL + HIST + AVISOS + DIA + EXT + AYER
METEO = DIA + EXT + AYER
GROUPS = {
    "viento": ["dia_racha_max", "dia_racha_media_max", "dia_viento_max", "dia_viento_media_max"],
    "clima": ["ext_racha_clima_max", "ext_racha_clima_media", "ext_racha_ku"],
    "lluvia": ["dia_lluvia_max", "dia_lluvia_media", "dia_lluvia_hora_max"],
    "calor": ["dia_temp_max", "ext_dias_calor"], "frio": ["dia_temp_media_min"], "humedad": ["dia_humedad_media"],
    "aviso": AVISOS, "ayer": AYER, "historia": HIST,
    "semana": ["cal_dia_semana_sin", "cal_dia_semana_cos"], "fin_de_semana": ["cal_fin_de_semana"],
    "festivo": ["cal_festivo"], "epoca": ["cal_dia_anio_sin", "cal_dia_anio_cos", "estacion"],
}
print(f"{len(FEATURES)} variables; {len(df)} días de red, {int(df.y_dia.sum())} con avería")


def slog(x):
    return np.sign(x) * np.log1p(np.abs(x))


def pipe(C):
    return Pipeline([("imputar", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
                     ("log", FunctionTransformer(slog)), ("escalar", StandardScaler()),
                     ("modelo", LogisticRegression(C=C, max_iter=2000, solver="lbfgs"))])


def fit_like_evening(tr):
    days = np.sort(tr["fecha"].unique())
    cut = days[int(len(days) * 0.75)]
    a, b = tr[tr["fecha"] < cut], tr[tr["fecha"] >= cut]
    best = max((average_precision_score(b.y_dia, pipe(C).fit(a[FEATURES], a.y_dia).predict_proba(b[FEATURES])[:, 1]), C)
               for C in (0.003, 0.01, 0.03, 0.1))
    return pipe(best[1]).fit(tr[FEATURES], tr.y_dia), best[1]


rows = []
for year in range(2022, 2027):
    tr = df[df["anio"] < year]
    model, C = fit_like_evening(tr)
    coef = model.named_steps["modelo"].coef_[0][:len(FEATURES)]
    rows += [{"anio": year, "C": C, "variable": f, "coef": c} for f, c in zip(FEATURES, coef)]
coefs = pd.DataFrame(rows)
print("C elegida por año:", coefs.groupby("anio")["C"].first().to_dict())
stab = coefs.groupby("variable")["coef"].agg(media="mean", minimo="min", maximo="max")
stab["signo_estable"] = coefs.assign(s=np.sign(coefs.coef)).groupby("variable")["s"].nunique().eq(1)
stab = stab.reindex(FEATURES)
pd.set_option("display.width", 200)
print(stab.round(3).to_string())
print(f"Signo estable en los cinco años: {int(stab.signo_estable.sum())} de {len(stab)}; "
      f"en las 16 meteorológicas: {int(stab.loc[METEO, 'signo_estable'].sum())} de 16")

# 2. Motivos con el modelo de todos los días hasta 2025
full = df[df["anio"] <= 2025]
model, C = fit_like_evening(full)
coef = model.named_steps["modelo"].coef_[0]
print(f"\nModelo con todos los días hasta 2025 (C = {C}): coeficientes por grupo (escala estandarizada)")
for g, fs in GROUPS.items():
    print(f"  {g:14s}", ", ".join(f"{f}={coef[FEATURES.index(f)]:+.3f}" for f in fs))
imp = model.named_steps["imputar"]
z = model.named_steps["escalar"].transform(model.named_steps["log"].transform(imp.transform(full[FEATURES])))
terms = (z * coef)[:, :len(FEATURES)]
prob = model.predict_proba(full[FEATURES])[:, 1]
top = prob >= np.quantile(prob, 0.90)
tot = pd.DataFrame({g: terms[:, [FEATURES.index(f) for f in fs]].sum(axis=1) for g, fs in GROUPS.items()})
t = tot[top]
shown = lambda g: t[g] > math.log(1.05)  # noqa: E731
for a, b in (("viento", "clima"), ("calor", "frio"), ("calor", "humedad")):
    both = t[a] + t[b]
    for g, other in ((a, b), (b, a)):
        sel = shown(g) & (t[other] < 0)
        infl = np.exp(t[g][sel]) / np.exp(both[sel]).clip(lower=1e-9)
        print(f"  {g} se enseña y {other} resta: {int(sel.sum())} de {int(top.sum())} días del 10 % de más riesgo; "
              f"multiplicador enseñado / conjunto: mediana {np.median(infl) if len(infl) else float('nan'):.2f}, "
              f"máximo {infl.max() if len(infl) else float('nan'):.2f}")
first = t.apply(lambda r: r.idxmax(), axis=1)
print("  Motivo primero en los días del 10 %:", first.value_counts().to_dict())

# 3. Número de condición y VIF de las 16 meteorológicas (escala del modelo)
X = full[METEO].dropna()
Z = (slog(X) - slog(X).mean()) / slog(X).std(ddof=0)
R = np.corrcoef(Z.values, rowvar=False)
lam = np.linalg.eigvalsh(R)
vif = np.diag(np.linalg.inv(R))
print(f"\nNúmero de condición de la matriz de correlaciones de las 16 meteorológicas: {math.sqrt(lam.max() / lam.min()):.1f}")
print("VIF:", ", ".join(f"{f}={v:.1f}" for f, v in sorted(zip(METEO, vif), key=lambda p: -p[1])))
coefs.to_csv(os.path.join(OUT, "coeficientes_anios.csv"), index=False)
stab.round(4).to_csv(os.path.join(OUT, "coeficientes_estabilidad_aprox.csv"))
_log.close()
