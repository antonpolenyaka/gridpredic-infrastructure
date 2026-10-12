"""
Causes of the Calser incidents of the three distribuidoras: what happened,
how often, how much TIEPI every cause brings, when and where it happens and
how it moves with the weather. It is the basis to decide which data could
anticipate every cause and which of them we already have.

Reads l2_silver.f_incidencia_causa (job_silver_f_incidencia_causa: every
incident with its cause, subcause, component and origin, read from its codes
and all its texts) and, for the TIEPI weight and the weather, the Gold tables
of the network analysis through job_analisis_red.Data, so the TIEPI figures
are the same as in that analysis (Calser rules, the blackout of 28.04.2025
apart).

Sections (each one independent, as in job_analisis_red):

1. What Calser keeps of every incident, by period and distribuidora: level,
   duration, planned, official code, origin installation, and where the
   cause comes from (text, official code, factor or nothing). Before 2016
   Calser also imported the cuts of less than 180 s and the low voltage
   incidents, and most texts only name the switch that tripped.
2. Every cause and subcause: incidents by period, CT incidents since 2016,
   TIEPI (kVA.h of the regulated TIEPI, 2016 onwards), CTs, duration and how
   often the text also mentions the weather.
3. Where it starts (origen): upstream network, substation, 45 kV line,
   medium voltage line, CT, private installation, low voltage.
4. Official code (casuistica) against the cause read from the text, since
   2016: how far the code can be trusted.
5. When: month, hour and working day of every cause.
6. Repeated places: the installations of origin with several incidents.
7. Weather by cause (the years with weather in Gold): probability of a day
   with an incident of every cause with strong gust, heavy rain, frost, heat,
   the first rain after a dry spell and, from 2021, high CAPE (the unstable
   air of a thunderstorm), against the other days.
8. Public data against every cause: the yearly probability that a CT has an
   unplanned incident of a cause, by thirds (or zero against more) of the
   public data of its cell that should explain it: storks (GBIF) for fauna,
   forest (OpenStreetMap) for vegetation, burnt areas (EFFIS) for fires,
   relief and overhead lines for weather, works and density for third
   parties.

Outputs in exportar_dir/<run_id>/ (resumen.md and causas_*.csv, with the
whole classified list in incidencias_causa.csv) and l3_gold.analisis_causas
(the table of section 2 with the run_id).

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 4g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py,\\
/app/jobs/04_ml/job_analisis_red.py \\
        /app/jobs/04_ml/job_causas_incidencias.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import os
import re
import sys
import traceback
import unicodedata
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))

if HERE not in sys.path:
    sys.path.insert(0, HERE)

import job_analisis_red as red  # noqa: E402
from calendario import working_days  # noqa: E402
from job_ml_train import GOLD, get_spark, load_config, logger, save_frame  # noqa: E402

CAUSES_TABLE = "l2_silver.f_incidencia_causa"
CONTEXT_TABLE = f"{GOLD}.contexto_ct_anio"
WEATHER_TABLE = f"{GOLD}.meteo_celda_hora"
CELL_TABLE = f"{GOLD}.map_ct_celda"
SUMMARY_TABLE = f"{GOLD}.analisis_causas"

CAUSAS = [
    "programada", "aguas_arriba", "apagon", "meteorologia", "fauna", "vegetacion", "incendio", "terceros",
    "objeto_ajeno", "agente_externo", "equipo", "red_bt", "cliente_bt", "sobrecarga", "maniobra", "desconocida",
    "sin_causa",
]

# What the official code should be for every cause read from the text.
CODIGO_ESPERADO = {
    "atmosferica": {"meteorologia"},
    "fuerza_mayor": {"meteorologia", "incendio", "apagon"},
    "agente_externo": {"fauna", "vegetacion", "objeto_ajeno", "agente_externo", "terceros"},
    "accion_terceros": {"terceros", "vegetacion", "incendio", "objeto_ajeno"},
    "interna": {"equipo", "programada", "maniobra", "sobrecarga", "red_bt"},
    "instalacion_particular": {"equipo", "fauna", "vegetacion", "terceros", "meteorologia", "objeto_ajeno"},
    "otra_distribuidora": {"aguas_arriba"},
    "transporte": {"aguas_arriba", "apagon"},
    "desconocida": {"desconocida", "sin_causa", "apagon"},
}

DEFAULTS = {
    "anio_corte": 2016,
    "rachas_kmh": [60, 80],
    "lluvias_mm": [10, 30],
    "helada_c": 0.0,
    "calor_c": 38.0,
    "lluvia_tras_seca_mm": 5.0,
    "dias_secos_min": 20,
    "min_repeticiones": 2,
    "max_filas_md": 40,
    "cape_jkg": 1000.0,
    "min_ct_anios": 50,
    # (cause, public variable of contexto_ct_anio) pairs of section 8.
    "publicos": [
        ["fauna", "celda_ciguena_parte_aves_5a"], ["fauna", "celda_ciguena_5a"], ["fauna", "celda_dens_apoyos_km2"],
        ["vegetacion", "celda_pct_bosque"], ["vegetacion", "celda_pct_matorral"],
        ["incendio", "celda_incendios_5a"], ["incendio", "celda_area_quemada_5a_ha"],
        ["meteorologia", "celda_elev_media_m"], ["meteorologia", "celda_desnivel_m"],
        ["meteorologia", "celda_dens_linea_aerea_km_km2"], ["desconocida", "celda_dens_linea_aerea_km_km2"],
        ["desconocida", "celda_ciguena_parte_aves_5a"], ["terceros", "obras_mov_tierras_1a"],
        ["terceros", "densidad_hab_km2"], ["equipo", "celda_dens_linea_aerea_km_km2"],
        ["todas", "celda_dens_linea_aerea_km_km2"], ["todas", "celda_pct_bosque"],
    ],
}

MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]


# ---------------------------------------------------------------------------
# Arguments and data
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--secciones", default=None,
                        help="Comma separated subset: inventario,causas,origen,codigo,cuando,repetidas,meteo,publicos")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "cau_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def settings(config: dict) -> dict:
    cfg = dict(DEFAULTS)
    cfg.update(config.get("causas_incidencias", {}))
    return cfg


def plain(value) -> str:
    """Upper case without accents and with single spaces, to group installation names."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    text = unicodedata.normalize("NFKD", str(value).upper())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^A-Z0-9]+", " ", text).strip()


class Causes:
    """The classified incidents in pandas, with their TIEPI when Gold has them."""

    def __init__(self, spark, config: dict, cfg: dict):
        self.spark = spark
        self.cfg = cfg
        self.red_cfg = red.settings(config)
        self.inc = self._incidents()
        self.data = self._network()
        self._add_tiepi()

    def _incidents(self) -> pd.DataFrame:
        df = red.read_table(self.spark, CAUSES_TABLE)

        if df is None:
            raise RuntimeError(f"{CAUSES_TABLE} is required (run job_silver_f_incidencia_causa)")

        pdf = df.toPandas()
        pdf["inicio_ts"] = pd.to_datetime(pdf["inicio_ts"])
        pdf["anio"] = pdf["inicio_ts"].dt.year
        cut = int(self.cfg["anio_corte"])
        pdf["periodo"] = np.where(pdf["anio"] < cut, f"hasta {cut - 1}", f"desde {cut}")
        pdf["incidente"] = (pdf["distribuidora_id"].astype(str) + "|" + pdf["periodo_id"].astype(str) + "|"
                            + pdf["id"].astype(str))
        for column in ("n_int_ct", "n_interrupciones", "n_int_mas_180s", "n_elementos"):
            if column in pdf.columns:
                pdf[column] = pd.to_numeric(pdf[column], errors="coerce").fillna(0)
        pdf["dur_h"] = pd.to_numeric(pdf.get("dur_int_max_s"), errors="coerce") / 3600.0
        pdf["es_ct"] = pdf["nivel_afectacion"] == "ct"
        pdf["mas_180s"] = pdf["n_int_mas_180s"] > 0
        pdf["causa"] = pdf["causa"].fillna("sin_causa")
        pdf["subcausa"] = pdf["subcausa"].fillna("sin_dato")
        return pdf

    def _network(self):
        try:
            return red.Data(self.spark, self.red_cfg)
        except Exception as error:  # noqa: BLE001
            logger.warning("Gold of the network analysis not available, no TIEPI weights: %s", error)
            return None

    def _add_tiepi(self):
        self.inc["kva_h_tiepi"] = np.nan
        self.inc["kva_h_imprevista"] = np.nan
        self.inc["en_gold"] = False

        if self.data is None or self.data.events_all.empty:
            return

        ev = self.data.events_all[self.data.events_all["variante"] == "todas"]
        ev = ev[ev["incidente"].notna()].assign(
            kva_h_tiepi=lambda d: d["kva_h"].where(d["computa_tiepi"], 0.0),
            kva_h_imprevista=lambda d: d["kva_h"].where(d["computa_imprevista"], 0.0),
        )
        g = ev.groupby("incidente").agg(
            kva_h_tiepi=("kva_h_tiepi", "sum"), kva_h_imprevista=("kva_h_imprevista", "sum"),
            excluido=("excluido", "max"),
        ).reset_index()
        merged = self.inc.drop(columns=["kva_h_tiepi", "kva_h_imprevista", "en_gold"]).merge(
            g, on="incidente", how="left")
        merged["en_gold"] = merged["kva_h_tiepi"].notna()
        merged["excluido"] = merged["excluido"].astype("boolean").fillna(False).astype(bool)
        self.inc = merged

    def years_with_tiepi(self) -> tuple:
        return int(self.red_cfg["anio_desde"]), int(self.red_cfg["anio_hasta"])


def pct(value, digits: int = 1) -> str:
    return red.pct(value, digits)


def share_table(pdf: pd.DataFrame, columns: list) -> pd.DataFrame:
    out = pdf.copy()
    for column in columns:
        out[column] = out[column].map(pct)
    return out


# ---------------------------------------------------------------------------
# 1. What Calser keeps
# ---------------------------------------------------------------------------

def section_inventory(c: Causes, report: red.Report, state: dict):
    inc = c.inc
    g = inc.groupby(["periodo", "source_database"]).agg(
        incidencias=("id", "size"),
        nivel_ct=("es_ct", "mean"),
        mas_180s=("mas_180s", "mean"),
        programadas=("clasificacion_id", lambda s: float((s == "CL_PROGR").mean())),
        codigo_no_35=("casuistica", lambda s: float((s.notna() & (s != 35)).mean())),
        con_origen=("instalacion_origen", lambda s: float(s.notna().mean())),
        causa_texto=("fuente_causa", lambda s: float((s == "texto").mean())),
        sin_causa=("causa", lambda s: float((s == "sin_causa").mean())),
    ).reset_index()
    report.csv("causas_inventario.csv", g)

    sources = pd.crosstab(inc["periodo"], inc["fuente_causa"], normalize="index").reset_index()
    report.csv("causas_fuente.csv", sources)

    shown = share_table(g, ["nivel_ct", "mas_180s", "programadas", "codigo_no_35", "con_origen", "causa_texto",
                            "sin_causa"])
    cut = int(c.cfg["anio_corte"])
    report.section(
        "1. Qué guarda Calser de cada incidencia",
        f"Incidencias de las tres distribuidoras: {len(inc)} ({inc['anio'].min()} - {inc['anio'].max()}). "
        f"nivel_ct es la parte con alguna interrupción de CT completo, mas_180s la parte con alguna de más de 180 s, "
        f"codigo_no_35 la parte con código de causa distinto del 35 (desconocida, el valor por defecto), con_origen "
        f"la parte con instalación de origen y causa_texto la parte cuya causa sale del texto.\n\n"
        + red.md_table(shown)
        + f"\nHasta {cut - 1} Calser importaba también los cortes de menos de 180 s y las incidencias de baja tensión, "
        "y la mayoría de textos solo dice qué interruptor disparó. Desde " + str(cut) + " solo hay incidencias de CT "
        "de más de 180 s, casi todas con una causa escrita.\n"
    )
    state["inventario"] = g


# ---------------------------------------------------------------------------
# 2. Every cause
# ---------------------------------------------------------------------------

def cause_summary(c: Causes, by: list) -> pd.DataFrame:
    inc = c.inc
    cut = int(c.cfg["anio_corte"])
    old, new = inc["anio"] < cut, inc["anio"] >= cut
    lo, hi = c.years_with_tiepi()
    tiepi = inc["en_gold"] & ~inc["excluido"] & inc["anio"].between(max(lo, cut), hi)

    rows = []
    for key, g in inc.groupby(by, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        recent = g[g["anio"] >= cut]
        rows.append({
            **dict(zip(by, key)),
            "incidencias": len(g),
            f"hasta_{cut - 1}": int(old[g.index].sum()),
            f"desde_{cut}": int(new[g.index].sum()),
            f"ct_desde_{cut}": int((recent["es_ct"]).sum()),
            "kva_h_tiepi": float(g.loc[tiepi[g.index], "kva_h_tiepi"].sum()),
            "cts_medio": float(recent.loc[recent["es_ct"], "n_int_ct"].mean()) if recent["es_ct"].any() else np.nan,
            "dur_mediana_h": float(recent.loc[recent["es_ct"], "dur_h"].median()) if recent["es_ct"].any() else np.nan,
            "con_meteo_en_texto": float(g["meteo_en_texto"].mean()),
        })

    out = pd.DataFrame(rows)
    total = out["kva_h_tiepi"].sum()
    out["parte_tiepi"] = out["kva_h_tiepi"] / total if total else np.nan
    out["parte_incidencias_recientes"] = out[f"desde_{cut}"] / out[f"desde_{cut}"].sum()
    order = {name: i for i, name in enumerate(CAUSAS)}
    out["_o"] = out[by[0]].map(order).fillna(99) if by[0] == "causa" else 0
    return out.sort_values(["_o", "kva_h_tiepi"], ascending=[True, False]).drop(columns="_o")


def section_causes(c: Causes, report: red.Report, state: dict):
    cut = int(c.cfg["anio_corte"])
    by_cause = cause_summary(c, ["causa"])
    by_sub = cause_summary(c, ["causa", "subcausa"])
    report.csv("causas_resumen.csv", by_cause)
    report.csv("causas_subcausas.csv", by_sub)

    recent = c.inc[c.inc["anio"] >= cut]
    by_dist = pd.crosstab(recent["causa"], recent["source_database"]).reset_index()
    report.csv("causas_distribuidora.csv", by_dist)

    columns = ["incidencias", f"hasta_{cut - 1}", f"desde_{cut}", f"ct_desde_{cut}", "parte_incidencias_recientes",
               "parte_tiepi", "cts_medio", "dur_mediana_h", "con_meteo_en_texto"]
    shown = share_table(by_cause, ["parte_incidencias_recientes", "parte_tiepi", "con_meteo_en_texto"])
    sub_shown = share_table(by_sub[by_sub[f"desde_{cut}"] + by_sub[f"hasta_{cut - 1}"] > 0],
                            ["parte_incidencias_recientes", "parte_tiepi", "con_meteo_en_texto"])
    lo, hi = c.years_with_tiepi()
    report.section(
        "2. Todas las causas",
        f"parte_tiepi es la parte de los kVA·h del TIEPI regulado de {max(lo, cut)} - {hi} (reglas de Calser, sin el "
        "apagón); cts_medio y dur_mediana_h se calculan con las incidencias de CT desde " + str(cut) + ".\n\n"
        + red.md_table(shown, ["causa"] + columns, max_rows=40)
        + "\nSubcausas:\n\n"
        + red.md_table(sub_shown, ["causa", "subcausa", f"hasta_{cut - 1}", f"desde_{cut}", f"ct_desde_{cut}",
                                   "parte_tiepi", "cts_medio", "dur_mediana_h"], max_rows=int(c.cfg["max_filas_md"]) * 2)
        + "\nDesde " + str(cut) + " por distribuidora:\n\n" + red.md_table(by_dist, max_rows=40)
    )
    state["resumen"] = by_cause


# ---------------------------------------------------------------------------
# 3. Origin
# ---------------------------------------------------------------------------

def section_origin(c: Causes, report: red.Report, state: dict):
    cut = int(c.cfg["anio_corte"])
    recent = c.inc[c.inc["anio"] >= cut]
    table = pd.crosstab(recent["causa"], recent["origen"], margins=True, margins_name="total").reset_index()
    everything = pd.crosstab(c.inc["origen"], c.inc["periodo"], margins=True, margins_name="total").reset_index()
    report.csv("causas_origen.csv", table)
    component = recent[recent["componente"].notna()]
    parts = component.groupby(["componente", "origen"]).size().rename("incidencias").reset_index()
    parts = parts.sort_values("incidencias", ascending=False)
    report.csv("causas_componente.csv", parts)
    report.section(
        "3. Dónde empieza: origen de cada causa",
        f"Desde {cut}:\n\n" + red.md_table(table, max_rows=40)
        + "\nTodas las incidencias por origen y periodo:\n\n" + red.md_table(everything, max_rows=20)
        + "\nComponentes que fallan (desde " + str(cut) + "):\n\n" + red.md_table(parts, max_rows=30)
    )


# ---------------------------------------------------------------------------
# 4. Official code against the text
# ---------------------------------------------------------------------------

def section_code(c: Causes, report: red.Report, state: dict):
    cut = int(c.cfg["anio_corte"])
    recent = c.inc[(c.inc["anio"] >= cut) & c.inc["casuistica_nombre"].notna()].copy()

    if recent.empty:
        report.section("4. Código oficial frente al texto", "_Sin código de causa de Extremadura._")
        return

    from_text = recent[recent["fuente_causa"] == "texto"].copy()
    from_text["coincide"] = [
        causa in CODIGO_ESPERADO.get(code, set()) or (code == "instalacion_particular" and origen == "instalacion_particular")
        for causa, code, origen in zip(from_text["causa"], from_text["casuistica_nombre"], from_text["origen"])
    ]
    table = pd.crosstab(recent["casuistica_nombre"], recent["causa"]).reset_index()
    report.csv("causas_codigo_texto.csv", table)
    agree = from_text.groupby("casuistica_nombre").agg(
        incidencias=("id", "size"), coincide=("coincide", "mean")).reset_index()
    report.csv("causas_codigo_coincidencia.csv", agree)
    by_year = from_text.groupby("anio")["coincide"].mean().reset_index()
    total = float(from_text["coincide"].mean()) if len(from_text) else np.nan
    state["codigo_coincide"] = total

    report.section(
        "4. Código oficial frente al texto",
        f"Incidencias desde {cut} con código de Extremadura: {len(recent)}. Entre las que tienen la causa escrita, el "
        f"código cuadra con el texto en el {pct(total)} (por ejemplo, un nido de cigüeñas con código 33 agente "
        "externo cuadra; con código 34 interna, no).\n\n"
        + red.md_table(share_table(agree, ["coincide"]))
        + "\nPor año:\n\n" + red.md_table(share_table(by_year, ["coincide"]))
        + "\nCódigo (filas) frente a causa leída (columnas):\n\n" + red.md_table(table, max_rows=20)
    )


# ---------------------------------------------------------------------------
# 5. When
# ---------------------------------------------------------------------------

def working_day(inc: pd.DataFrame) -> pd.Series:
    """
    Monday to Friday and not a holiday of Extremadura, from the start of the
    incident. The tipo de día of Calser is not used: since 2016 it marks as
    working day 85 incidents that started on a Saturday or a Sunday.
    """
    return pd.Series(working_days(inc["inicio_ts"]), index=inc.index)


def section_when(c: Causes, report: red.Report, state: dict):
    cut = int(c.cfg["anio_corte"])
    inc = c.inc.copy()
    inc["mes"] = inc["inicio_ts"].dt.month
    inc["hora"] = inc["inicio_ts"].dt.hour
    inc["laborable"] = working_day(inc)
    recent = inc[(inc["anio"] >= cut) & inc["es_ct"]]

    month = pd.crosstab(recent["causa"], recent["mes"]).reindex(columns=range(1, 13), fill_value=0)
    month.columns = MONTHS
    month_share = month.div(month.sum(axis=1), axis=0)
    month_out = month.reset_index()
    month_all = pd.crosstab(inc["causa"], inc["mes"]).reindex(columns=range(1, 13), fill_value=0)
    month_all.columns = MONTHS
    report.csv("causas_mes.csv", month_out)
    report.csv("causas_mes_todas.csv", month_all.reset_index())

    hour = pd.crosstab(recent["causa"], recent["hora"]).reindex(columns=range(24), fill_value=0)
    report.csv("causas_hora.csv", hour.reset_index())

    def peak(row):
        top = row.sort_values(ascending=False).head(3)
        return ", ".join(f"{m} {pct(v, 0)}" for m, v in top.items() if v > 0)

    summary = pd.DataFrame({
        "incidencias": month.sum(axis=1),
        "meses_pico": month_share.apply(peak, axis=1),
        "de_8_a_18": (hour.loc[:, 8:17].sum(axis=1) / hour.sum(axis=1)),
        "noche_0_6": (hour.loc[:, 0:5].sum(axis=1) / hour.sum(axis=1)),
        "laborable": recent.groupby("causa")["laborable"].mean(),
    }).reset_index()
    summary = summary[summary["incidencias"] > 0]
    report.csv("causas_cuando.csv", summary)
    report.section(
        "5. Cuándo pasa cada causa",
        f"Incidencias de CT desde {cut}. meses_pico son los tres meses con más incidencias de la causa (parte de "
        "sus incidencias); de_8_a_18 la parte que empieza en horario de trabajo, noche_0_6 de 0 a 6 h y laborable "
        "la parte en día laborable (de lunes a viernes sin los festivos de Extremadura; el tipo de día de Calser "
        "marca como laborables muchas incidencias de fin de semana).\n\n"
        + red.md_table(share_table(summary, ["de_8_a_18", "noche_0_6", "laborable"]), max_rows=30)
    )
    state["cuando"] = summary


# ---------------------------------------------------------------------------
# 6. Repeated places
# ---------------------------------------------------------------------------

def section_repeated(c: Causes, report: red.Report, state: dict):
    inc = c.inc[c.inc["instalacion_origen"].notna() & (c.inc["causa"] != "programada")].copy()

    if inc.empty:
        report.section("6. Lugares que se repiten", "_Sin instalación de origen._")
        return

    inc["lugar"] = inc["instalacion_origen"].map(plain)
    minimum = int(c.cfg["min_repeticiones"])
    by_place = inc.groupby(["source_database", "lugar"]).agg(
        incidencias=("id", "size"),
        causas=("causa", lambda s: ", ".join(f"{k} {v}" for k, v in s.value_counts().items())),
        primera=("inicio_ts", "min"), ultima=("inicio_ts", "max"),
        kva_h_tiepi=("kva_h_tiepi", "sum"),
    ).reset_index()
    by_place = by_place[by_place["incidencias"] >= minimum].sort_values(["incidencias", "kva_h_tiepi"],
                                                                         ascending=False)
    by_place["primera"] = by_place["primera"].dt.strftime("%d.%m.%Y")
    by_place["ultima"] = by_place["ultima"].dt.strftime("%d.%m.%Y")
    report.csv("causas_lugares_repetidos.csv", by_place)

    same = inc.groupby(["source_database", "lugar", "causa"]).size().rename("incidencias").reset_index()
    same = same[same["incidencias"] >= minimum].sort_values("incidencias", ascending=False)
    report.csv("causas_misma_causa_mismo_lugar.csv", same)
    state["repetidos"] = by_place
    report.section(
        "6. Lugares que se repiten",
        f"Instalaciones de origen (desde que Calser las guarda) con {minimum} o más incidencias imprevistas. Una "
        "misma salida que falla una y otra vez es donde más rinde una actuación.\n\n"
        + red.md_table(by_place, ["source_database", "lugar", "incidencias", "causas", "primera", "ultima"],
                       max_rows=int(c.cfg["max_filas_md"]))
        + "\nLa misma causa en el mismo lugar:\n\n" + red.md_table(same, max_rows=25)
    )


# ---------------------------------------------------------------------------
# 7. Weather by cause
# ---------------------------------------------------------------------------

def daily_weather(spark) -> pd.DataFrame:
    """Worst cell of every distribuidora and day: strongest gust, most rain, coldest and hottest hour."""
    weather = red.read_table(spark, WEATHER_TABLE, ["celda_id", "hora", "met_racha_1h", "met_lluvia_1h", "met_temp_1h",
                                                     "met_cape_1h"])
    cells = red.read_table(spark, CELL_TABLE, ["distribuidora_id", "ct_id", "celda_id"])

    if weather is None or cells is None:
        return pd.DataFrame()

    temp = F.col("met_temp_1h") if "met_temp_1h" in weather.columns else F.lit(None).cast("double")
    cape = F.col("met_cape_1h") if "met_cape_1h" in weather.columns else F.lit(None).cast("double")
    per_cell = weather.groupBy("celda_id", F.to_date("hora").alias("dia")).agg(
        F.max("met_racha_1h").alias("racha"), F.sum("met_lluvia_1h").alias("lluvia"),
        F.min(temp).alias("tmin"), F.max(temp).alias("tmax"), F.max(cape).alias("cape"),
    )
    pairs = cells.where(F.col("celda_id").isNotNull()).select("distribuidora_id", "celda_id").distinct()
    daily = per_cell.join(pairs, "celda_id").groupBy("distribuidora_id", "dia").agg(
        F.max("racha").alias("racha_max_kmh"), F.max("lluvia").alias("lluvia_max_mm"),
        F.avg("lluvia").alias("lluvia_media_mm"), F.min("tmin").alias("tmin_c"), F.max("tmax").alias("tmax_c"),
        F.max("cape").alias("cape_max_jkg"),
    )
    pdf = daily.toPandas()
    pdf["dia"] = pd.to_datetime(pdf["dia"])
    return pdf.sort_values(["distribuidora_id", "dia"]).reset_index(drop=True)


def add_dry_spell(days: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Days without rain before every day, and the first rain after a dry spell."""
    out = []
    for _, g in days.groupby("distribuidora_id"):
        g = g.sort_values("dia").copy()
        dry = 0
        streak = []
        for rain in g["lluvia_media_mm"].fillna(0.0):
            streak.append(dry)
            dry = dry + 1 if rain < 1.0 else 0
        g["dias_secos_antes"] = streak
        out.append(g)
    days = pd.concat(out, ignore_index=True) if out else days
    days["lluvia_tras_seca"] = (days["lluvia_media_mm"] >= float(cfg["lluvia_tras_seca_mm"])) & (
        days["dias_secos_antes"] >= int(cfg["dias_secos_min"]))
    return days


def weather_conditions(cfg: dict) -> list:
    out = []
    for gust in cfg["rachas_kmh"]:
        out.append((f"racha >= {gust} km/h", lambda d, g=float(gust): d["racha_max_kmh"] >= g))
    for rain in cfg["lluvias_mm"]:
        out.append((f"lluvia >= {rain} mm", lambda d, r=float(rain): d["lluvia_max_mm"] >= r))
    out.append((f"helada (min <= {cfg['helada_c']} C)", lambda d: d["tmin_c"] <= float(cfg["helada_c"])))
    out.append((f"calor (max >= {cfg['calor_c']} C)", lambda d: d["tmax_c"] >= float(cfg["calor_c"])))
    out.append((f"primera lluvia tras {cfg['dias_secos_min']} dias secos", lambda d: d["lluvia_tras_seca"]))
    # CAPE only exists from 2021: the days without it are left out of that condition (NaN, not False).
    out.append((f"CAPE >= {int(cfg['cape_jkg'])} J/kg", lambda d: (d["cape_max_jkg"] >= float(cfg["cape_jkg"])).where(
        d["cape_max_jkg"].notna())))
    return out


def section_weather(c: Causes, report: red.Report, state: dict):
    days = daily_weather(c.spark)

    if days.empty:
        report.section("7. Meteorología por causa", "_Sin meteorología en Gold._")
        return

    days = add_dry_spell(days, c.cfg)
    inc = c.inc[(c.inc["clasificacion_id"] != "CL_PROGR") & c.inc["es_ct"] & ~c.inc["causa"].isin(["apagon"])].copy()
    inc["dia"] = inc["inicio_ts"].dt.normalize()
    first, last = days["dia"].min(), min(days["dia"].max(), inc["dia"].max())
    days = days[(days["dia"] >= first) & (days["dia"] <= last)]
    inc = inc[(inc["dia"] >= first) & (inc["dia"] <= last)]

    if inc.empty:
        report.section("7. Meteorología por causa", "_Sin incidencias en los años con meteorología._")
        return

    counts = inc.groupby(["distribuidora_id", "dia", "causa"]).size().unstack("causa", fill_value=0)
    days = days.merge(counts.reset_index(), on=["distribuidora_id", "dia"], how="left")
    causes = [name for name in CAUSAS if name in counts.columns]
    days[causes] = days[causes].fillna(0)
    days["todas"] = days[causes].sum(axis=1)
    report.csv("causas_dias_meteo.csv", days)

    rows = []
    for label, condition in weather_conditions(c.cfg):
        raw = condition(days)
        known = raw.notna()
        flag = raw.astype("boolean").fillna(False).astype(bool)
        sub = days[known]
        flag = flag[known]
        if sub.empty:
            continue
        for name in causes + ["todas"]:
            hit = sub[name] > 0
            p_on = float(hit[flag].mean()) if flag.any() else np.nan
            p_off = float(hit[~flag].mean()) if (~flag).any() else np.nan
            rows.append({
                "condicion": label, "causa": name, "dias_condicion": int(flag.sum()),
                "parte_dias": float(flag.mean()), "dias_con_incidencia": int(hit.sum()),
                "dias_con_incidencia_en_condicion": int((hit & flag).sum()),
                "p_con": p_on, "p_sin": p_off, "riesgo_relativo": p_on / p_off if p_off else np.nan,
                "parte_incidencias_en_condicion": float(sub.loc[flag, name].sum() / sub[name].sum())
                if sub[name].sum() else np.nan,
            })
    table = pd.DataFrame(rows)
    report.csv("causas_meteo.csv", table)
    state["meteo"] = table

    shown = table[table["dias_con_incidencia"] >= 3].copy()
    for column in ("parte_dias", "p_con", "p_sin", "parte_incidencias_en_condicion"):
        shown[column] = shown[column].map(lambda v: pct(v, 2))
    report.section(
        "7. Meteorología por causa",
        f"Días de cada distribuidora entre el {first:%d.%m.%Y} y el {last:%d.%m.%Y} con el tiempo de su peor celda, "
        "e incidencias imprevistas de CT por causa (sin el apagón). p_con es la probabilidad de un día con alguna "
        "incidencia de la causa cuando se da la condición, p_sin cuando no, y parte_incidencias_en_condicion la "
        "parte de las incidencias de la causa que caen en esos días. Solo causas con tres o más días.\n\n"
        + red.md_table(shown, ["condicion", "causa", "dias_condicion", "parte_dias", "dias_con_incidencia",
                               "p_con", "p_sin", "riesgo_relativo", "parte_incidencias_en_condicion"], max_rows=120)
    )


# ---------------------------------------------------------------------------
# 8. Public data against every cause
# ---------------------------------------------------------------------------

def ct_years_with_causes(c: Causes) -> pd.DataFrame:
    """One row per CT and year with a flag per cause: did the CT have an unplanned incident of it."""
    ev = c.data.events[(c.data.events["variante"] == "todas") & c.data.events["computa_imprevista"]]
    ev = ev.merge(c.inc[["incidente", "causa"]], on="incidente", how="left")
    ev["causa"] = ev["causa"].fillna("sin_causa")
    hits = ev.groupby(["distribuidora_id", "ct_id", "anio", "causa"]).size().unstack("causa", fill_value=0) > 0
    hits["todas"] = hits.any(axis=1)
    return hits.reset_index()


def bands_of(values: pd.Series) -> pd.Series:
    """Thirds of a variable, or zero against more when most values are zero."""
    if (values > 0).mean() < 0.4:
        return pd.Series(np.where(values > 0, "mas_de_0", "0"), index=values.index)
    try:
        return pd.qcut(values, 3, labels=["tercio_bajo", "tercio_medio", "tercio_alto"], duplicates="drop").astype(str)
    except ValueError:
        return pd.Series(np.where(values > values.median(), "sobre_mediana", "bajo_mediana"), index=values.index)


def section_public(c: Causes, report: red.Report, state: dict):
    if c.data is None:
        report.section("8. Datos públicos frente a cada causa", "_Sin Gold de la red._")
        return

    pairs = [tuple(p) for p in c.cfg["publicos"]]
    variables = sorted({v for _, v in pairs})
    context = red.read_table(c.spark, CONTEXT_TABLE, ["distribuidora_id", "ct_id", "anio", *variables])

    if context is None:
        report.section("8. Datos públicos frente a cada causa", "_Sin l3_gold.contexto_ct_anio._")
        return

    ctx = context.toPandas()
    ctx["ct_id"] = ctx["ct_id"].astype(str)
    lo, hi = c.years_with_tiepi()
    ctx = ctx[ctx["anio"].between(max(lo, int(c.cfg["anio_corte"])), hi)]
    hits = ct_years_with_causes(c)
    hits["ct_id"] = hits["ct_id"].astype(str)
    grid = ctx.merge(hits, on=["distribuidora_id", "ct_id", "anio"], how="left")

    rows = []
    for cause, variable in pairs:
        if variable not in grid.columns:
            continue
        flag = (grid[cause].astype("boolean").fillna(False).astype(bool) if cause in grid.columns
                else pd.Series(False, index=grid.index))
        values = pd.to_numeric(grid[variable], errors="coerce")
        known = values.notna()
        if known.sum() < int(c.cfg["min_ct_anios"]) or flag[known].sum() == 0:
            rows.append({"causa": cause, "variable": variable, "banda": "sin_datos", "ct_anios": int(known.sum()),
                         "ct_anios_con_incidencia": int(flag[known].sum()), "p_anual": np.nan, "razon": np.nan})
            continue
        bands = bands_of(values[known])
        g = pd.DataFrame({"banda": bands, "hit": flag[known], "valor": values[known]}).groupby("banda").agg(
            ct_anios=("hit", "size"), ct_anios_con_incidencia=("hit", "sum"), p_anual=("hit", "mean"),
            valor_medio=("valor", "mean")).reset_index()
        base = g.loc[g["banda"].isin(["tercio_bajo", "0", "bajo_mediana"]), "p_anual"]
        base = float(base.iloc[0]) if len(base) else np.nan
        for _, r in g.iterrows():
            rows.append({"causa": cause, "variable": variable, "banda": r["banda"], "ct_anios": int(r["ct_anios"]),
                         "ct_anios_con_incidencia": int(r["ct_anios_con_incidencia"]), "valor_medio": r["valor_medio"],
                         "p_anual": r["p_anual"], "razon": r["p_anual"] / base if base else np.nan})
    table = pd.DataFrame(rows)
    report.csv("causas_datos_publicos.csv", table)
    state["publicos"] = table
    shown = table.copy()
    shown["p_anual"] = shown["p_anual"].map(lambda v: pct(v, 2))
    report.section(
        "8. Datos públicos frente a cada causa",
        f"CT y año ({max(lo, int(c.cfg['anio_corte']))} - {hi}): p_anual es la probabilidad de que el CT sufra en el "
        "año alguna incidencia imprevista de la causa, por tercios de la variable pública de su celda (o cero "
        "frente a más de cero cuando casi todos son cero); razon, frente al tercio bajo. Las variables son las de "
        "l3_gold.contexto_ct_anio, todas conocidas antes de empezar el año.\n\n"
        + red.md_table(shown, ["causa", "variable", "banda", "ct_anios", "ct_anios_con_incidencia", "valor_medio",
                               "p_anual", "razon"], max_rows=120)
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

SECTIONS = [
    ("inventario", section_inventory),
    ("causas", section_causes),
    ("origen", section_origin),
    ("codigo", section_code),
    ("cuando", section_when),
    ("repetidas", section_repeated),
    ("meteo", section_weather),
    ("publicos", section_public),
]


def export_list(c: Causes, report: red.Report):
    columns = ["source_database", "periodo_id", "id", "inicio_ts", "periodo", "clasificacion_id", "factor_id",
               "tipo_equipo_id", "casuistica", "casuistica_nombre", "nivel_afectacion", "n_int_ct", "dur_h", "causa",
               "subcausa", "componente", "origen", "regla", "fuente_causa", "meteo_en_texto", "maniobra_reparacion",
               "instalacion_origen", "instalacion_cabecera", "metodos_recogida", "kva_h_tiepi", "texto_causa"]
    present = [name for name in columns if name in c.inc.columns]
    report.csv("incidencias_causa.csv", c.inc[present].sort_values("inicio_ts"))


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    cfg = settings(config)
    spark = get_spark(config)
    wanted = set(args.secciones.split(",")) if args.secciones else None

    started = datetime.now(timezone.utc)
    c = Causes(spark, config, cfg)
    logger.info("Causes %s: %s incidents", args.run_id, len(c.inc))

    report = red.Report(config.get("exportar_dir"), args.run_id)
    report.parts[0] = f"# Causas de las incidencias {args.run_id}\n"
    state = {}
    failed = []

    for name, function in SECTIONS:
        if wanted and name not in wanted:
            continue
        try:
            function(c, report, state)
        except Exception as error:  # noqa: BLE001
            failed.append(name)
            logger.error("Section %s failed: %s\n%s", name, error, traceback.format_exc())
            report.section(f"Error en {name}", f"`{type(error).__name__}: {error}`")

    export_list(c, report)

    summary = state.get("resumen")
    if summary is not None and not summary.empty:
        try:
            out = summary.copy()
            out.insert(0, "run_id", args.run_id)
            save_frame(spark, out, SUMMARY_TABLE, args.run_id)
        except Exception as error:  # noqa: BLE001
            logger.error("Table %s not saved: %s", SUMMARY_TABLE, error)

    text = report.write()
    logger.info("Summary of %s\n%s", args.run_id, text)
    logger.info("Causes finished in %.0f s; failed sections: %s",
                (datetime.now(timezone.utc) - started).total_seconds(), failed or "none")
    return 1 if failed and len(failed) == len([s for s in SECTIONS if not wanted or s[0] in wanted]) else 0


if __name__ == "__main__":
    sys.exit(main())
