"""
Dos comprobaciones de las reglas de causas de Silver (etl/jobs/02_silver/causas_incidencia.py) para 5.8.1 de la memoria
y docs/causas-incidencias.md 2, sobre la tabla de incidencias que deja job_causas_incidencias.py (incidencias_causa.csv
de cau_20261012T025409_4681c9, la misma que lee comparar_reglas_tfidf.py), sin Spark:

    python kappa_ambiguas.py <incidencias_causa.csv> <salida/>

1. El acuerdo de la causa leída del texto con el código oficial de Extremadura, corregido por azar. La causa cuadra si es
   de la familia compatible con el código (CODIGO_ESPERADO de job_causas_incidencias.py, con fits_code de
   comparar_reglas_tfidf.py), en las incidencias desde 2016 con código y causa leída del texto, como el job. Un código
   admite varias causas, así que la kappa es la ponderada con pesos 0 y 1: el acuerdo por azar es la suma, para cada
   código y cada par de causa y origen compatibles, del producto de sus frecuencias, como si el código y el par fueran
   independientes. Intervalo del 95 % con 2.000 réplicas bootstrap de las incidencias, y la misma cuenta sin las
   programadas.
2. Las palabras de doble sentido de las reglas, contadas en los partes reales: "temporal" (borrasca o algo
   provisional), "aparato electrico" (tormenta o aparato del abonado), los nombres de borrasca fuera de su semana, y las
   cifras y cadenas cortas sin límite de palabra ("132" y "220" pegados a otras cifras, "i de" dentro de otra palabra o
   suelto, "riada" y "dana" dentro de otra palabra). Se reaplican los patrones del módulo a lo que lee cada regla: los
   textos para la meteorología y, para el origen y la subcausa, también las dos instalaciones. Una causa depende de una
   coincidencia ambigua si todas las palabras meteorológicas de su parte son ambiguas; una cifra o cadena corta cambia
   algo si las incidencias en que aparece tienen el origen, la subcausa o la causa que su patrón podría dar. Aparte,
   cuántas incidencias con la subcausa de interconexión cuyo patrón lleva "i de" la deben solo a esas letras.

Deja kappa.csv, ambiguas.csv y kappa_ambiguas.md. Solo escribe recuentos y proporciones: ni textos de los partes, ni
nombres de instalaciones o de empresas, ni identificadores de incidencias. Tarda unos quince segundos.
"""

import os
import re
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "02_silver"))
import causas_incidencia as ci  # noqa: E402
from comparar_reglas_tfidf import fits_code  # noqa: E402

FROM_YEAR = 2016
REPS, SEED = 2000, 7
# Nombre normalizado de cada borrasca y su semana, con margen
STORMS = {"gloria": ("2020-01-17", "2020-01-26"), "filomena": ("2021-01-05", "2021-01-14"),
          "barbara": ("2020-10-17", "2020-10-25"), "kristin": ("2026-01-24", "2026-02-02")}
DOUBLE = ["temporal", "aparato electrico"] + list(STORMS)
# "temporal" con sentido de provisional
PROVISIONAL = re.compile(r"(forma|manera|modo) temporal|temporalmente|provisional|puente temporal|alimentacion temporal|"
                         r"temporal (del|de la|de los|para)|grupo electrogeno")


# ---------------------------------------------------------------------------
# 1. Acuerdo con el código, corregido por azar
# ---------------------------------------------------------------------------

def agreement(d: pd.DataFrame) -> tuple:
    """(acuerdo observado, acuerdo por azar, kappa ponderada 0/1)."""
    po = float(np.mean([fits_code(a, c, o) for a, c, o in zip(d["causa"], d["casuistica_nombre"], d["origen"])]))
    codes = d["casuistica_nombre"].value_counts(normalize=True)
    pairs = d.groupby(["causa", "origen"]).size() / len(d)
    pe = float(sum(codes[c] * pairs[(a, o)] for c in codes.index for (a, o) in pairs.index if fits_code(a, c, o)))
    return po, pe, (po - pe) / (1 - pe)


def kappa_table(df: pd.DataFrame) -> tuple:
    d = df[(df["anio"] >= FROM_YEAR) & df["casuistica_nombre"].notna() & (df["fuente_causa"] == "texto")].copy()
    rows = []
    for label, part in (("con código y causa leída del texto", d),
                        ("las mismas sin las programadas", d[d["causa"] != "programada"])):
        po, pe, k = agreement(part)
        rng = np.random.default_rng(SEED)
        reps = [agreement(part.sample(len(part), replace=True, random_state=int(rng.integers(1_000_000_000))))[2]
                for _ in range(REPS)]
        lo, hi = np.percentile(reps, [2.5, 97.5])
        rows.append({"poblacion": f"desde {FROM_YEAR}, {label}", "incidencias": len(part), "observado": po,
                     "azar": pe, "kappa": k, "kappa_p2_5": lo, "kappa_p97_5": hi})
    d["cuadra"] = [fits_code(a, c, o) for a, c, o in zip(d["causa"], d["casuistica_nombre"], d["origen"])]
    by_code = (d.groupby("casuistica_nombre").agg(incidencias=("cuadra", "size"), cuadra=("cuadra", "mean"))
               .sort_values("incidencias", ascending=False).reset_index().rename(columns={"casuistica_nombre": "codigo"}))
    return pd.DataFrame(rows), by_code


# ---------------------------------------------------------------------------
# 2. Palabras de doble sentido
# ---------------------------------------------------------------------------

def texts(df: pd.DataFrame) -> pd.DataFrame:
    """Lo que lee cada regla: norm (los textos, sin la instalación de origen del final) y full (con las dos)."""
    whole = [ci.normalize(t) for t in df["texto_causa"]]
    origin = [ci.normalize(t) for t in df["instalacion_origen"]]
    out = pd.DataFrame({"fecha": df["inicio_ts"].dt.strftime("%Y-%m-%d"), "anio": df["anio"],
                        "nivel": df["nivel_afectacion"], "causa": df["causa"], "subcausa": df["subcausa"],
                        "origen": df["origen"]})
    out["norm"] = [w[: -len(o)].strip() if o and w.endswith(o) else w for w, o in zip(whole, origin)]
    out["full"] = [" ".join(x for x in (w, ci.normalize(c)) if x) for w, c in zip(whole, df["instalacion_cabecera"])]
    return out


def meteo_words(t: pd.DataFrame) -> pd.DataFrame:
    """Una fila por parte con alguna palabra de METEO: sus palabras, las ambiguas y si la causa solo tiene ambiguas."""
    rows = []
    for r in t.itertuples(index=False):
        tokens = {m.group(0) for m in ci.METEO.finditer(r.norm)}
        if not tokens:
            continue
        ambiguous, clear = set(), set()
        for tok in tokens:
            if tok == "temporal":
                (ambiguous if PROVISIONAL.search(r.norm) else clear).add(tok)
            elif tok == "aparato electrico":
                ambiguous.add(tok)
            elif tok in STORMS:
                lo, hi = STORMS[tok]
                (clear if lo <= r.fecha <= hi else ambiguous).add(tok)
            else:
                clear.add(tok)
        rows.append({"fecha": r.fecha, "anio": r.anio, "nivel": r.nivel, "causa": r.causa, "tokens": tokens,
                     "ambiguos": ambiguous, "solo_ambiguo": bool(ambiguous) and not clear})
    return pd.DataFrame(rows)


def ambiguous_tables(t: pd.DataFrame) -> tuple:
    meteo = meteo_words(t)
    since = meteo[meteo["anio"] >= FROM_YEAR]
    totals = []
    for label, part in (("todos los partes", meteo), (f"desde {FROM_YEAR}", since),
                        (f"desde {FROM_YEAR}, nivel CT", since[since["nivel"] == "ct"])):
        only = part[part["solo_ambiguo"]]
        totals.append({"grupo": "meteorología", "patron": label, "partes": len(part),
                       "desde_2016": np.nan, "causa_meteorologia": int((part["causa"] == "meteorologia").sum()),
                       "dependen_solo_de_ambigua": int((only["causa"] == "meteorologia").sum()),
                       "cualquier_causa_solo_ambigua": len(only)})
    for tok in DOUBLE:
        has = meteo.loc[meteo["tokens"].map(lambda s, w=tok: w in s).astype(bool)]
        amb = has.loc[has["ambiguos"].map(lambda s, w=tok: w in s).astype(bool)]
        days = pd.to_datetime(has["fecha"])
        totals.append({"grupo": "palabra de doble sentido", "patron": tok, "partes": len(has),
                       "desde_2016": int((has["anio"] >= FROM_YEAR).sum()), "con_sentido_ambiguo": len(amb),
                       "con_otra_palabra_clara": int(sum(bool(s - a - {tok}) for s, a in zip(has["tokens"],
                                                                                            has["ambiguos"]))),
                       "dependen_solo_de_ambigua": int(amb["solo_ambiguo"].sum()),
                       "primera_fecha": days.min().strftime("%d.%m.%Y") if len(has) else "",
                       "ultima_fecha": days.max().strftime("%d.%m.%Y") if len(has) else ""})
    # Cifras y cadenas cortas sin límite de palabra, sobre lo que leen el origen y la subcausa: cuántos partes desde
    # 2016 tienen el resultado que el patrón podría dar
    upstream = [name for name, _ in ci.AGUAS_ARRIBA_SUB[1:]]  # la subcausa de "i de" y la de la red de transporte
    company = ci.AGUAS_ARRIBA_SUB[1][1].pattern.split("|")[0]
    meteo_cause = lambda h: h["causa"] == "meteorologia"  # noqa: E731
    interconnection = lambda h: h["subcausa"] == upstream[0]  # noqa: E731
    checks = {
        "132 pegado a otras cifras": (r"[0-9]132|132[0-9]", "origen subestación",
                                      lambda h: h["origen"] == "subestacion"),
        "220 pegado a otras cifras": (r"[0-9]220|220[0-9]", "subcausa red de transporte u origen subestación",
                                      lambda h: (h["subcausa"] == upstream[1]) | (h["origen"] == "subestacion")),
        "i de dentro de otra palabra": (r"[a-z]i de\b", "subcausa de interconexión", interconnection),
        "i de suelto, sin el nombre de la empresa vecina detrás": (r"(?<![a-z])i de\b(?!.*" + company + ")",
                                                                   "subcausa de interconexión", interconnection),
        "riada dentro de otra palabra": (r"[a-z]riada", "causa meteorología", meteo_cause),
        "dana dentro de otra palabra": (r"[a-z]dana|dana[a-z]", "causa meteorología", meteo_cause),
    }
    for label, (pattern, effect, has_effect) in checks.items():
        rx = re.compile(pattern)
        hit = t[[bool(rx.search(x)) for x in t["full"]]]
        hit16 = hit[hit["anio"] >= FROM_YEAR]
        totals.append({"grupo": "cifra o cadena corta", "patron": label, "partes": len(hit),
                       "desde_2016": len(hit16), "podria_dar": effect,
                       "con_ese_resultado_desde_2016": int(has_effect(hit16).sum())})
    # La subcausa de interconexión cuyo patrón lleva "i de": cuántas la deben solo a "i de"
    name, rx = ci.AGUAS_ARRIBA_SUB[1]
    without = re.compile("|".join(p for p in rx.pattern.split("|") if not p.startswith("i de")))
    sub = t[t["subcausa"] == name]
    only_ide = sub[[not without.search(x) for x in sub["full"]]]
    totals.append({"grupo": "subcausa de interconexión del patrón con i de", "patron": "la dan solo las letras i de",
                   "partes": len(sub), "desde_2016": int((sub["anio"] >= FROM_YEAR).sum()),
                   "dependen_solo_de_ambigua": len(only_ide)})
    columns = ["grupo", "patron", "partes", "desde_2016", "causa_meteorologia", "con_sentido_ambiguo",
               "con_otra_palabra_clara", "dependen_solo_de_ambigua", "cualquier_causa_solo_ambigua", "primera_fecha",
               "ultima_fecha", "podria_dar", "con_ese_resultado_desde_2016"]
    return pd.DataFrame(totals)[columns]


# ---------------------------------------------------------------------------

def md(frame: pd.DataFrame, formats: dict = None) -> str:
    """Tabla en markdown; formats da el formato de las columnas con decimales y el resto de números va sin ellos."""
    formats = formats or {}
    shown = frame.copy()
    for col in shown.columns:
        if shown[col].dtype.kind in "fi":
            spec = formats.get(col, ".0f")
            shown[col] = shown[col].map(lambda v, s=spec: "" if pd.isna(v) else format(v, s))
    shown = shown.fillna("")
    lines = ["| " + " | ".join(shown.columns) + " |", "| " + " | ".join("---" for _ in shown.columns) + " |"]
    lines += ["| " + " | ".join(str(v) for v in r) + " |" for r in shown.itertuples(index=False)]
    return "\n".join(lines)


def main(argv) -> int:
    path, out = argv[:2]
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    df = pd.read_csv(path, parse_dates=["inicio_ts"], low_memory=False)
    df["anio"] = df["inicio_ts"].dt.year

    kappa, by_code = kappa_table(df)
    words = ambiguous_tables(texts(df))
    kappa.to_csv(os.path.join(out, "kappa.csv"), index=False, float_format="%.4f")
    words.to_csv(os.path.join(out, "ambiguas.csv"), index=False, float_format="%.0f")

    main_row = kappa.iloc[0]
    meteo_all = words[(words["grupo"] == "meteorología")]
    lines = ["# Las reglas de causas: acuerdo con el código corregido por azar y palabras de doble sentido", "",
             f"{len(df)} partes de Calser; tiempo {time.time() - t0:.0f} s.", "",
             "## 1. Acuerdo con el código oficial, corregido por azar", "",
             md(kappa.assign(observado=kappa["observado"] * 100, azar=kappa["azar"] * 100)
                .rename(columns={"observado": "observado_%", "azar": "azar_%"}),
                {"observado_%": ".1f", "azar_%": ".1f", "kappa": ".3f", "kappa_p2_5": ".3f", "kappa_p97_5": ".3f"}),
             "",
             f"Las reglas cuadran con el código en el {100 * main_row['observado']:.1f} % de las "
             f"{int(main_row['incidencias'])} incidencias; si el código y la causa se repartieran por azar, cuadrarían "
             f"en el {100 * main_row['azar']:.1f} %. Kappa {main_row['kappa']:.3f} (del {main_row['kappa_p2_5']:.3f} al "
             f"{main_row['kappa_p97_5']:.3f} con {REPS} réplicas).", "", "Por código:", "",
             md(by_code.assign(cuadra=by_code["cuadra"] * 100).rename(columns={"cuadra": "cuadra_%"}),
                {"cuadra_%": ".1f"}), "",
             "## 2. Palabras de doble sentido de las reglas", "",
             f"Partes con alguna palabra meteorológica en el texto: {int(meteo_all.iloc[0]['partes'])} "
             f"({int(meteo_all.iloc[1]['partes'])} desde {FROM_YEAR}). Causas que dependen solo de una coincidencia "
             f"ambigua: {int(meteo_all['cualquier_causa_solo_ambigua'].max())}; meteorología que depende solo de una: "
             f"{int(meteo_all['dependen_solo_de_ambigua'].max())}.", ""]
    for group, part in words.groupby("grupo", sort=False):
        part = part.dropna(axis=1, how="all").drop(columns="grupo")
        lines += [f"### {group[0].upper() + group[1:]}", "", md(part), ""]
    device = words[words["patron"] == "aparato electrico"].iloc[0]
    lines += [f"\"aparato electrico\" cuenta siempre como ambigua: {int(device['con_otra_palabra_clara'])} de los "
              f"{int(device['partes'])} partes que la llevan tienen otra palabra del tiempo. Los nombres de borrasca son "
              f"claros dentro de su semana, con margen, y \"temporal\" lo es salvo con sentido de provisional."]
    text = "\n".join(lines)
    with open(os.path.join(out, "kappa_ambiguas.md"), "w", encoding="utf-8") as handle:
        handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
