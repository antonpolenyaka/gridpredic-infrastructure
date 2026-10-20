"""
La serie diaria de averías sin Spark (memoria, 5.2, 5.7.3 y 5.7.5), con la tabla que deja exportar_dia.py y la copia
congelada del caso de negocio que se publica con la demo (web/demo/data/historico.json):

    python serie_dia.py <dia.csv.gz> <bloques.json> <salida/> [--historico ruta] [--config ruta]

Cinco preguntas:

1. Autocorrelación de la etiqueta diaria de cada red (y_dia) a 1, 7 y 365 días, contraste de Ljung y Box (1978) a
   7 y 14 retardos con la función de experimentos/evaluacion/calibracion_valor.py, la probabilidad de avería tras un
   día con avería y la estacionalidad por mes (chi cuadrado).
2. Referencias sin meteorología, puntuadas año a año con los años anteriores y sobre los mismos días que puntúa la
   previsión real de la víspera: tasa base, climatología mensual y de más o menos 15 días del año, avería de ayer y
   una logística autorregresiva de siete retardos. Avería de ayer y la autorregresiva usan la etiqueta de D - 1, que a
   las 20:30 de la víspera no siempre se conoce: son cotas optimistas.
3. El hueco de etiquetas del 14.08 al 31.10.2025: la PR-AUC de producción (forecast_evaluation de job_ml_riesgo_dia.py,
   la de 0,188) con esos días como días sin avería o fuera del ajuste y de la puntuación.
4. Cuánto cambia la alerta de dos tardes antes a la víspera, con logísticas orientativas (C = 0,1, sin búsqueda) de
   las variantes operativa y operativa_48h.
5. Calendarios fijos (fines de semana de invierno) y climatológicos frente a la alerta, con la misma parte de días.

Si statsmodels está instalado (no es una dependencia del repositorio), añade el ARMA(1,1) a las referencias y la
STL y el contraste ADF de la serie mensual; sin él, esas filas no salen. Deja serie_dia.md y un CSV por pregunta.
Las redes salen como Distribuidora 1, 2 y 3. Tarda menos de cinco minutos.
"""

import argparse
import json
import logging
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.preprocessing import StandardScaler

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "04_ml"))
sys.path.insert(0, os.path.join(REPO, "experimentos", "evaluacion"))
import job_ml_riesgo_dia as rd  # noqa: E402
from calibracion_valor import ljung_box  # noqa: E402

logging.basicConfig(level=logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

NETWORKS = {3: "Distribuidora 1", 2: "Distribuidora 2", 1366: "Distribuidora 3"}
SINCE = "2018-06-25"  # el primer día del archivo de avisos, como la variante operativa
BLACKOUT = "2025-04-28"
GAP = ("2025-08-14", "2025-10-31")
STORM = "2026-01-28"
WINDOWS = {"2024 - 2025": [2024, 2025], "2024 - 2026": [2024, 2025, 2026]}

try:
    import statsmodels  # noqa: F401
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False


def load(data_path, blocks_path):
    df = pd.read_csv(data_path, parse_dates=["fecha"])
    df["red"] = df["distribuidora_id"].map(NETWORKS)
    df["anio"] = df["fecha"].dt.year
    with open(blocks_path, encoding="utf-8") as handle:
        blocks = json.load(handle)["bloques"]
    return df.sort_values(["fecha", "red"]).reset_index(drop=True), blocks


def acf(x, lags):
    """Autocorrelación con el denominador de toda la serie, la misma de ljung_box."""
    x = np.asarray(x, dtype=float) - np.mean(x)
    total = float((x ** 2).sum())
    return {k: float((x[k:] * x[:-k]).sum() / total) for k in lags}


# ---------------------------------------------------------------------------
# 1. Autocorrelación y estacionalidad
# ---------------------------------------------------------------------------

def autocorrelation(df):
    wide = df.pivot(index="fecha", columns="red", values="y_dia").sort_index()
    counts = df.pivot(index="fecha", columns="red", values="n_dia").sort_index()
    wide["Las tres redes"] = (counts.sum(axis=1) > 0).astype(int)
    rows = []
    for name in ["Distribuidora 1", "Distribuidora 2", "Distribuidora 3", "Las tres redes"]:
        x = wide[name].to_numpy(dtype=float)
        r = acf(x, (1, 2, 7, 365))
        lb = ljung_box(x, (7, 14))
        prev = wide[name].shift(1)
        after_one, after_zero = wide[name][prev == 1].mean(), wide[name][prev == 0].mean()
        episodes = int((wide[name].diff() == 1).sum())
        rows.append({"redes": name, "dias": len(x), "dias_con_averia": int(x.sum()), "tasa": x.mean(),
                     "r1": r[1], "r2": r[2], "r7": r[7], "r365": r[365], "banda_95": 1.96 / np.sqrt(len(x)),
                     "q7": lb["q7"], "p7": lb["p7"], "q14": lb["q14"], "p14": lb["p14"],
                     "p_averia_tras_averia": after_one, "p_averia_tras_dia_sin": after_zero,
                     "cociente": after_one / after_zero if after_zero else np.nan,
                     "racha_media_dias": x.sum() / max(episodes, 1)})
    table = pd.DataFrame(rows)
    chi2, p_month, _, _ = stats.chi2_contingency(pd.crosstab(df["fecha"].dt.month, df["y_dia"]))
    extra = {"chi2_mes": chi2, "p_mes": p_month}
    if HAS_STATSMODELS:
        from statsmodels.tsa.seasonal import STL
        from statsmodels.tsa.stattools import adfuller

        monthly = wide[["Distribuidora 1", "Distribuidora 2", "Distribuidora 3"]].resample("MS").sum().sum(axis=1)
        days = wide["Las tres redes"].resample("MS").size()
        monthly = monthly[days >= 25].astype(float)
        stl = STL(monthly, period=12, robust=True).fit()
        extra.update(stl_estacional=stl.seasonal.var() / monthly.var(), stl_tendencia=stl.trend.var() / monthly.var(),
                     stl_resto=stl.resid.var() / monthly.var(), adf_p=adfuller(monthly.to_numpy(), autolag="AIC")[1])
    return table, extra


# ---------------------------------------------------------------------------
# 2. Referencias sin meteorología
# ---------------------------------------------------------------------------

def forecast_days(df, blocks):
    """Los días que puntúa la previsión real: todas las variables del día con su gemela prev_ (forecast_evaluation)."""
    twins = [f"prev_{c[len('dia_'):]}" for c in blocks["dia_comun"] if f"prev_{c[len('dia_'):]}" in df.columns]
    keep = df[twins].notna().all(axis=1) & (df["fecha"] >= SINCE) & (df["fecha"] != BLACKOUT)
    return df[keep]


def references(df, blocks):
    series = df[df["fecha"] != BLACKOUT].pivot(index="fecha", columns="red", values="y_dia").sort_index().asfreq("D")
    target = forecast_days(df, blocks)
    rows = []
    for label, years in WINDOWS.items():
        frame = target[target["anio"].isin(years)].reset_index(drop=True)
        scores = {name: np.full(len(frame), np.nan) for name in
                  ["tasa_base", "clima_mensual", "clima_15_dias", "averia_ayer", "logistica_autorregresiva"]
                  + (["arma_1_1"] if HAS_STATSMODELS else [])}
        for (red, year), part in frame.groupby(["red", "anio"]):
            idx = part.index
            past = df[(df["red"] == red) & (df["anio"] < year) & (df["fecha"] != BLACKOUT)]
            scores["tasa_base"][idx] = past["y_dia"].mean()
            scores["clima_mensual"][idx] = part["fecha"].dt.month.map(past.groupby(past["fecha"].dt.month)["y_dia"]
                                                                      .mean()).to_numpy()
            doy, yp = past["fecha"].dt.dayofyear.to_numpy(), past["y_dia"].to_numpy()
            values = []
            for day in part["fecha"].dt.dayofyear:
                dist = np.minimum(np.abs(doy - day), 365 - np.abs(doy - day))
                values.append(yp[dist <= 15].mean())
            scores["clima_15_dias"][idx] = values
            s = series[red].astype(float)
            scores["averia_ayer"][idx] = s.shift(1).reindex(part["fecha"]).fillna(0).to_numpy()
            X = pd.DataFrame({f"r{k}": s.shift(k) for k in range(1, 8)})
            X["h30"] = s.shift(1).rolling(30, min_periods=1).sum()
            X["h365"] = s.shift(1).rolling(365, min_periods=30).sum()
            X["sin"] = np.sin(2 * np.pi * s.index.dayofyear.values / 365.25)
            X["cos"] = np.cos(2 * np.pi * s.index.dayofyear.values / 365.25)
            X = X.fillna(0)
            fit = (s.index.year < year) & s.notna().to_numpy()
            model = LogisticRegression(C=0.1, max_iter=2000).fit(X[fit], s[fit])
            scores["logistica_autorregresiva"][idx] = model.predict_proba(X.loc[part["fecha"]])[:, 1]
            if HAS_STATSMODELS:
                from statsmodels.tsa.arima.model import ARIMA

                arma = ARIMA(s[s.index.year < year], order=(1, 0, 1), trend="c").fit()
                full = s[s.index.year <= year]
                pred = arma.apply(full).get_prediction(start=full.index[full.index.year == year][0],
                                                       end=full.index[-1]).predicted_mean
                scores["arma_1_1"][idx] = pred.reindex(part["fecha"]).to_numpy()
        y, year_of = frame["y_dia"].to_numpy(), frame["anio"].to_numpy()
        for name, s in scores.items():
            ap = average_precision_score(y, s)
            rows.append({"anios": label, "referencia": name, "dias": len(frame), "dias_con_averia": int(y.sum()),
                         "pr_auc": ap, "veces_tasa": ap / y.mean(), "captura_10": rd.top_share(y, s, year_of, 0.1, 0),
                         "captura_20": rd.top_share(y, s, year_of, 0.2, 0)})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 3. El hueco de 2025, con la evaluación de producción
# ---------------------------------------------------------------------------

def gap(df, blocks, config_path):
    with open(config_path, encoding="utf-8") as handle:
        config = json.load(handle)
    cfg = rd.settings(config)
    cfg = {**cfg, "modelos": ["tasa_base", "logistica"], "purga_horas": float(cfg.get("purga_horas", 0))}
    variant = next(v for v in cfg["variantes"] if v["nombre"] == "operativa")
    pdf = df.copy()
    pdf["ct_key"], pdf["hora"], pdf["split"] = pdf["distribuidora_id"], pdf["fecha"], "train"
    pdf = pdf[pdf["fecha"] != BLACKOUT].reset_index(drop=True)
    in_gap = (pdf["fecha"] >= GAP[0]) & (pdf["fecha"] <= GAP[1])
    rows = []
    for label, years in WINDOWS.items():
        for treated, part in (("como días sin avería", pdf), ("fuera del ajuste y de la puntuación",
                                                               pdf[~in_gap].reset_index(drop=True))):
            found = rd.forecast_evaluation(part, variant, blocks, config, cfg, years, only_first=False)
            y, s = found[rd.MAIN_LABEL].to_numpy(), found["p_logistica__prevision"].to_numpy()
            ap = average_precision_score(y, s)
            rows.append({"anios": label, "hueco": treated, "dias": len(found), "dias_con_averia": int(y.sum()),
                         "tasa": y.mean(), "pr_auc": ap, "veces_tasa": ap / y.mean(),
                         "captura_10": rd.top_share(y, s, found["anio"].to_numpy(), 0.1, 0),
                         "captura_20": rd.top_share(y, s, found["anio"].to_numpy(), 0.2, 0)})
    summary = {"dias_hueco": int(in_gap.sum()), "averias_hueco": int(pdf.loc[in_gap, "y_dia"].sum())}
    return pd.DataFrame(rows), summary


# ---------------------------------------------------------------------------
# 4. De dos tardes antes a la víspera
# ---------------------------------------------------------------------------

def signed_log(X, median=None):
    median = X.median() if median is None else median
    X = X.fillna(median)
    return np.sign(X) * np.log1p(np.abs(X)), median


def revision(df, blocks):
    df = df[df["fecha"] != BLACKOUT].reset_index(drop=True)
    v24 = (blocks["distribuidora"] + blocks["calendario"] + blocks["historia"] + blocks["avisos"]
           + blocks["dia_comun"] + blocks["extremos"] + ["ayer_racha_max", "ayer_lluvia_max"])
    t24 = {c: "prev_" + c[4:] for c in blocks["dia_comun"]} | {c: "prev_" + c for c in blocks["extremos"]}
    v48 = (blocks["distribuidora"] + blocks["calendario"] + blocks["historia"] + blocks["avisos_48h"]
           + blocks["dia_comun_48h"] + blocks["extremos_racha"] + ["hace2_racha_max", "hace2_lluvia_max"])
    t48 = {c: "prev2_" + c[4:] for c in blocks["dia_comun_48h"]} | {c: "prev2_" + c for c in blocks["extremos_racha"]}

    def run(features, twins, years):
        base = df[df["fecha"] >= SINCE]
        parts = []
        for year in years:
            train = base[base["anio"] < year]
            test = base[(base["anio"] == year) & base["prev_racha_max"].notna() & base["prev2_racha_max"].notna()]
            Xtr, median = signed_log(train[features])
            scaler = StandardScaler().fit(Xtr)
            model = LogisticRegression(C=0.1, max_iter=5000).fit(scaler.transform(Xtr), train["y_dia"])
            fitted = pd.Series(model.predict_proba(scaler.transform(Xtr))[:, 1], index=train.index)
            cut = fitted.groupby(train["red"]).quantile(0.90)  # alerta: por encima del p90 de su red
            raw = test[features].copy()
            for column, source in twins.items():
                raw[column] = test[source]
            p = model.predict_proba(scaler.transform(signed_log(raw, median)[0]))[:, 1]
            parts.append(pd.DataFrame({"fecha": test["fecha"].values, "red": test["red"].values, "anio": year,
                                       "y": test["y_dia"].values, "p": p,
                                       "alerta": p >= test["red"].map(cut).values}))
        return pd.concat(parts, ignore_index=True)

    rows = []
    for label, years in WINDOWS.items():
        m = run(v24, t24, years).merge(run(v48, t48, years), on=["fecha", "red", "anio", "y"], suffixes=("_24", "_48"))
        row = {"anios": label, "dias": len(m), "dias_con_averia": int(m["y"].sum())}
        for h in ("24", "48"):
            ap = average_precision_score(m["y"], m[f"p_{h}"])
            row[f"pr_auc_{h}h"] = ap
            row[f"veces_tasa_{h}h"] = ap / m["y"].mean()
            row[f"parte_alerta_{h}h"] = m[f"alerta_{h}"].mean()
            row[f"recoge_{h}h"] = m.loc[m[f"alerta_{h}"], "y"].sum() / m["y"].sum()
        both = m["alerta_48"] & m["alerta_24"]
        row.update(alerta_48h=int(m["alerta_48"].sum()), alerta_24h=int(m["alerta_24"].sum()), en_las_dos=int(both.sum()),
                   siguen_en_alerta=both.sum() / m["alerta_48"].sum(), ya_en_alerta=both.sum() / m["alerta_24"].sum(),
                   spearman=m["p_24"].rank().corr(m["p_48"].rank()),
                   tasa_las_dos=m.loc[both, "y"].mean(),
                   tasa_solo_48h=m.loc[m["alerta_48"] & ~m["alerta_24"], "y"].mean(),
                   tasa_solo_24h=m.loc[~m["alerta_48"] & m["alerta_24"], "y"].mean())
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 5. Calendarios frente a la alerta
# ---------------------------------------------------------------------------

def calendars(history_path, dia):
    with open(history_path, encoding="utf-8") as handle:
        history = json.load(handle)
    rows = [{"red": d["nombre"], "fecha": r["f"], "nivel": r["n"], "p": r["p"], "inc": r["i"], "ch": r["ch"],
             "chr": r["chr"]} for d in history["distribuidoras"] for r in d["dias"]["prevision"]]
    df = pd.DataFrame(rows)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df[df["red"].isin(["Distribuidora 1", "Distribuidora 2"])].reset_index(drop=True)
    df["anio"] = df["fecha"].dt.year
    years = df.groupby("red")["fecha"].nunique().mean() / 365.25
    dia = dia[["red", "fecha", "anio", "y_dia"]]
    for column in ("clima15", "climm"):
        values = pd.Series(np.nan, index=df.index)
        for (red, year), part in df.groupby(["red", "anio"]):
            past = dia[(dia["red"] == red) & (dia["anio"] < year)]
            if column == "climm":
                rate = past.groupby(past["fecha"].dt.month)["y_dia"].mean()
                values[part.index] = part["fecha"].dt.month.map(rate).to_numpy()
            else:
                doy, yp = past["fecha"].dt.dayofyear.to_numpy(), past["y_dia"].to_numpy()
                values[part.index] = [yp[np.minimum(np.abs(doy - d), 365 - np.abs(doy - d)) <= 15].mean()
                                      for d in part["fecha"].dt.dayofyear]
        df[column] = values

    def top(column, share):
        rng = np.random.default_rng(0)
        out = pd.Series(False, index=df.index)
        for _, part in df.groupby(["red", "anio"]):
            k = int(round(share * len(part)))
            order = (part[column].to_numpy() + rng.random(len(part)) * 1e-9).argsort()[::-1][:k]
            out.loc[part.index[order]] = True
        return out

    alert = df["nivel"].isin(["alto", "muy alto"])
    winter_weekend = df["fecha"].dt.month.isin([11, 12, 1, 2, 3]) & (df["fecha"].dt.dayofweek >= 5)
    share = alert.mean()
    rules = {"GridPredic en alerta": alert, "Fines de semana de noviembre a marzo": winter_weekend,
             "Todos los fines de semana": df["fecha"].dt.dayofweek >= 5,
             "Clima de 15 días, la parte de días de la alerta": top("clima15", share),
             "Clima mensual, la parte de días de la alerta": top("climm", share),
             "GridPredic, la misma parte de días por red y año": top("p", share)}
    storm = df["fecha"] == STORM
    table = []
    for name, mask in rules.items():
        rest = df[~storm]
        table.append({"regla": name, "dias_anio_red": mask.sum() / 2 / years, "parte_dias": mask.mean(),
                      "parte_incidencias": df.loc[mask, "inc"].sum() / df["inc"].sum(),
                      "parte_horas_cliente": df.loc[mask, "chr"].sum() / df["chr"].sum(),
                      "parte_horas_cliente_sin_recortar": df.loc[mask, "ch"].sum() / df["ch"].sum(),
                      "parte_horas_cliente_sin_temporal": rest.loc[mask[~storm], "chr"].sum() / rest["chr"].sum()})
    weekend = df["fecha"].dt.dayofweek >= 5
    hit = df["inc"] > 0
    contingency = [[int((hit & weekend).sum()), int((~hit & weekend).sum())],
                   [int((hit & ~weekend).sum()), int((~hit & ~weekend).sum())]]
    extra = {"dias": len(df), "desde": str(df["fecha"].min().date()), "hasta": str(df["fecha"].max().date()),
             "fin_de_semana": f"{contingency[0][0]} de {sum(contingency[0])}",
             "laborable": f"{contingency[1][0]} de {sum(contingency[1])}",
             "fisher_p": stats.fisher_exact(contingency)[1]}
    return pd.DataFrame(table), extra


def md_table(frame, digits=3):
    frame = frame.copy()
    for column in frame.columns:
        if frame[column].dtype.kind == "f":
            frame[column] = frame[column].map(lambda v: "" if pd.isna(v) else f"{v:.{digits}f}")
    head = "| " + " | ".join(frame.columns) + " |"
    sep = "| " + " | ".join("---" for _ in frame.columns) + " |"
    body = ["| " + " | ".join(str(v) for v in row) + " |" for row in frame.itertuples(index=False)]
    return "\n".join([head, sep] + body)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("datos")
    parser.add_argument("bloques")
    parser.add_argument("salida")
    parser.add_argument("--historico", default=os.path.join(REPO, "web", "demo", "data", "historico.json"))
    parser.add_argument("--config", default=os.path.join(REPO, "etl", "config", "04_ml", "config_ml.json"))
    args = parser.parse_args(argv)
    os.makedirs(args.salida, exist_ok=True)
    t0 = time.time()
    df, blocks = load(args.datos, args.bloques)

    acf_table, acf_extra = autocorrelation(df)
    refs = references(df, blocks)
    gap_table, gap_extra = gap(df, blocks, args.config)
    rev = revision(df, blocks)
    cal, cal_extra = calendars(args.historico, df)
    for name, table in (("autocorrelacion", acf_table), ("referencias", refs), ("hueco", gap_table),
                        ("revision_48h", rev), ("calendarios", cal)):
        table.to_csv(os.path.join(args.salida, f"{name}.csv"), index=False)

    lines = ["# La serie diaria de averías", "",
             f"Tabla del modelo del día: {len(df)} días de red, del {df['fecha'].min():%d.%m.%Y} al "
             f"{df['fecha'].max():%d.%m.%Y}. statsmodels {'instalado' if HAS_STATSMODELS else 'no instalado'}.", "",
             "## 1. Autocorrelación de la etiqueta", "", md_table(acf_table), "",
             f"Chi cuadrado del mes (tres redes apiladas): {acf_extra['chi2_mes']:.1f}, p = {acf_extra['p_mes']:.2f}."]
    if HAS_STATSMODELS:
        lines.append(f"STL de la serie mensual: estacional {acf_extra['stl_estacional']:.2f}, tendencia "
                     f"{acf_extra['stl_tendencia']:.2f} y resto {acf_extra['stl_resto']:.2f} de la varianza; ADF "
                     f"p = {acf_extra['adf_p']:.3f}.")
    lines += ["", "## 2. Referencias sin meteorología (días con previsión real de la víspera)", "", md_table(refs), "",
              "## 3. El hueco de 2025 con la evaluación de producción", "",
              f"{gap_extra['dias_hueco']} días de red en el hueco, {gap_extra['averias_hueco']} con avería.", "",
              md_table(gap_table), "", "## 4. De dos tardes antes a la víspera (logísticas orientativas)", "",
              md_table(rev), "", "## 5. Calendarios frente a la alerta (Distribuidoras 1 y 2)", "",
              f"{cal_extra['dias']} días de red del {cal_extra['desde']} al {cal_extra['hasta']}; días con incidencia "
              f"en fin de semana {cal_extra['fin_de_semana']} y en laborable {cal_extra['laborable']} "
              f"(Fisher p = {cal_extra['fisher_p']:.3f}).", "", md_table(cal), "",
              f"Tiempo: {time.time() - t0:.0f} s."]
    text = "\n".join(lines) + "\n"
    with open(os.path.join(args.salida, "serie_dia.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)


if __name__ == "__main__":
    main(sys.argv[1:])
