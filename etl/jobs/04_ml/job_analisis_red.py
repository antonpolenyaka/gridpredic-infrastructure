"""
Network analysis on Gold: where the TIEPI of every distribuidora comes from,
which part of it could be avoided, what it costs and which transformers are
running out of capacity. It answers the questions an operator asks before
spending money, with the history of Calser, TedisNet and the weather.

Sections (each one is independent: if a table is missing or a section fails,
the error goes to the log and the report and the rest goes on):

1. TIEPI and NIEPI by year, distribuidora, classification and factor, with
   the Calser rules (CT level, more than 180 s, incident required for the
   regulated indices, FA_CLIEN out of TIEPI). Each factor is tagged with the
   use the regulation makes of it: the quality incentive of the CNMC counts
   the planned interruptions and the unplanned ones of transport and of the
   distribuidora itself (propias), not those of third parties, force
   majeure or generation (tiepi_anual.csv).
2. Limits of RD 1955/2000: zonal TIEPI, NIEPI and percentile 80 by province
   and zone type (art. 106), municipalities above the percentile 80 limit
   and for how many years in a row, and the CTs whose customers exceed the
   individual limits of art. 104, with the bill discounts of art. 105
   (limites_zonales.csv, municipios_p80.csv, calidad_individual.csv).
3. Causes: equipment type of the incident (tipo_equipo_id), cause families
   read from the free text of the incident (description, observations and
   resolution) with the patterns of analisis_red.causas, and the most
   frequent words, so the patterns can be checked (causas_*.csv).
4. When: month, hour of the day and weekday of the unplanned incidents by
   factor family (cuando.csv).
5. Where, and how stable it is: concentration of the TIEPI in a few CTs,
   network groups, feeders (the upstream switch that cuts each CT most
   often) and municipalities (pareto.csv), and how much of the TIEPI of a
   year falls in the top k % of the ranking of the two previous years
   (persistencia.csv).
6. Staged restoration: within an incident, the CTs restored before the last
   ones were brought back by switching, not by repair. Their time is the
   share of TIEPI that remote controlled switches could leave below the
   3 minutes that count, an upper bound by feeder and year, with the cost of
   the switch from the reference unit values (reposicion*.csv).
7. Weather: daily strongest gust and rain of every distribuidora against its
   incidents, the probability of an incident and the TIEPI by gust band
   (dias_meteo.csv, dosis_respuesta.csv).
8. Transformer load from the SCADA series (LOCAL scope of agg_medida_hora):
   the % load tag where it exists, the active and reactive power otherwise,
   hourly percentiles by year, hours above 80 % and 100 % of the rated
   power, the trend and the year the trend reaches 100 %, and the hours of
   voltage outside +-7 % (carga_trafo*.csv, tension_ct.csv).
9. Age of the CTs (fecha_pes of Calser) against the rate of interruptions of
   their own equipment (edad_ct.csv).
10. Money: the quality incentive of CNMC Circular 8/2025 estimated with the
    TIEPI of the last three years, the value of one minute of TIEPI, the
    energy not supplied valued at the VoLL, and the value of the measures
    of the previous sections (economia.csv).
11. Public works and building (l3_gold.obras_municipio_anio, from PLACSP and
    the Catastro): the unplanned incidents of third parties, of machines and
    works (read from the text) and the own ones (the control) by
    distribuidora, municipality and year against the public works in
    execution that dig the street, as rates per 100 CT-years and as rate
    ratios of a Poisson model between towns and within every town, with an
    interval that resamples municipalities; the same with the works tendered
    before the year (a warning the distribuidora could have). And the
    dwellings built in the last five years against the kVA installed in
    every municipality, the new capacity the same pace would ask for
    (obras_terceros_*.csv, crecimiento_municipios.csv).

Outputs in exportar_dir/<run_id>/ (resumen.md and the CSV files) and the
ranking tables l3_gold.analisis_red_* with the run_id, for the notebooks.

    docker compose exec spark-master /opt/spark/bin/spark-submit --driver-memory 6g \\
        --py-files /app/jobs/04_ml/ml_models.py,/app/jobs/04_ml/ml_metrics.py,/app/jobs/04_ml/job_ml_train.py \\
        /app/jobs/04_ml/job_analisis_red.py --config /app/config/04_ml/config_ml.json
"""

import argparse
import math
import json
import os
import re
import sys
import traceback
import unicodedata
import uuid
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))

if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_ml_train import GOLD, get_spark, load_config, logger, save_frame  # noqa: E402
import riesgo_zonas as rz  # noqa: E402


FACT_TABLE = f"{GOLD}.fact_interrupciones_mt"
DIM_TABLE = f"{GOLD}.dim_ct"
UPSTREAM_TABLE = f"{GOLD}.map_aguas_arriba"
AGG_TABLE = f"{GOLD}.agg_medida_hora"
TAG_MAP_TABLE = f"{GOLD}.map_tag_ct"
WEATHER_TABLE = f"{GOLD}.meteo_celda_hora"
CELL_TABLE = f"{GOLD}.map_ct_celda"
INCIDENTS_TABLE = "l2_silver.f_incidencia"
SILVER_CT_TABLE = "l2_silver.d_ct"
WORKS_TABLE = f"{GOLD}.obras_municipio_anio"
CONTEXT_TABLE = f"{GOLD}.contexto_ct_anio"
PROJECTION_TABLE = "l2_silver.f_proyeccion_ame"
AME_TABLE = "l2_silver.d_ame_municipio"
SECTOR_TABLE = "l1_bronze.gecos_indice_zonal"

RANKING_TABLE = f"{GOLD}.analisis_red_prioridades"
LOAD_TABLE = f"{GOLD}.analisis_red_carga_trafo"
TIEPI_TABLE = f"{GOLD}.analisis_red_tiepi"

FACTOR_NAMES = {
    "FA_PROPI": "propias",
    "FA_TERCE": "terceros",
    "FA_FUMAY": "fuerza_mayor",
    "FA_TRANS": "transporte",
    "FA_GENER": "generacion",
    "FA_DISTR": "distribucion",
    "FA_CLIEN": "cliente",
}

ZONE_NAMES = {1: "urbana", 2: "semiurbana", 3: "rural_concentrada", 4: "rural_dispersa"}

# Defaults of the configuration section analisis_red (config_ml.json can
# override any of them).
DEFAULTS = {
    "anio_desde": 2015,
    "anio_hasta": 2025,
    "anios_persistencia": [2023, 2024, 2025],
    "anios_incentivo": [2023, 2024, 2025],
    "fracciones_top": [0.05, 0.1, 0.2],
    "duracion_max_h": 168,
    "margen_maniobra_min": 5,
    # RD 1955/2000 art. 106 (unplanned only): TIEPI, percentile 80 of TIEPI
    # and NIEPI by zone type. Art. 104: hours and interruptions per customer
    # in low voltage.
    "limites_zonales": {
        "1": [1.5, 2.5, 3], "2": [3.5, 5, 5], "3": [6, 10, 8], "4": [9, 15, 12],
    },
    "limites_individuales_bt": {"1": [5, 10], "2": [9, 13], "3": [14, 16], "4": [19, 22]},
    # Extremadura: monthly TIEPI (h) and NIEPI by zone type of art. 9.6 of Ley 2/2002, as written by the
    # Decreto-ley 1/2018 (DOE 227 of 22.11.2018, BOE-A-2019-1793). Two consecutive months above them oblige the
    # distribuidora to present measures. Same scope as art. 106 (unplanned interruptions), an assumption: the
    # table comes from a consolidated text, not from the DOE.
    "limites_mensuales_extremadura": {"3": [0.88, 0.74], "4": [1.32, 1.10]},
    # Art. 105: average billed power of a customer, price of the kWh (access
    # tolls and charges of 2.0TD in 2026) and annual bill for the 10 % cap.
    "cliente_potencia_kw": 4.3,
    "precio_kwh_eur": 0.0358,
    "factura_anual_eur": 600.0,
    # CNMC Circular 8/2025: TIEPI of the sector by zone type for 2026 - 2028,
    # +-4 % of the remuneration, bonus at -0,7 and penalty at +0,5.
    "tiepi_sector_h": {"1": 0.4888, "2": 0.6, "3": 1.0648, "4": 1.7393},
    # Official indices of the sector (GECOS, MITECO) to compare with: the
    # places and the number of last published years that are averaged.
    "sector_ambitos": [["provincia", "Cáceres"], ["provincia", "Badajoz"], ["comunidad", "Extremadura"], ["espana", "España"]],
    "sector_anios_media": 3,
    "incentivo_pct": 0.04,
    "incentivo_tope_bonificacion": -0.7,
    "incentivo_tope_penalizacion": 0.5,
    # Remuneration of 2023 (BOE-A-2026-19941) by source database.
    "retribucion_eur": {
        "Calser_EOSA": 11994208,
        "Calser_Pitarch": 10833762,
        "Calser_ValleSantaAna": 208986,
    },
    # Energy not supplied: utilisation of the installed kVA and power factor,
    # valued at the VoLL (CEPA for ACER 2018, domestic; DGPEM 2025).
    "factor_utilizacion": 0.25,
    "factor_potencia": 0.9,
    "voll_eur_kwh": [7.88, 22.879],
    # Reference unit values (Orden IET/2660/2015): remote controlled
    # load break switch TI-187A and its yearly O&M.
    "coste_interruptor_telemando_eur": 11174,
    "om_interruptor_telemando_eur": 61,
    "vida_util_anios": 30,
    "tasa_descuento": 0.0558,
    "rachas_kmh": [0, 40, 60, 80, 100],
    "lluvias_mm": [0, 1, 10, 30],
    "umbral_racha_alerta_kmh": 70,
    "umbral_carga": [0.8, 1.0],
    "carga_max_plausible": 1.6,
    "min_horas_carga": 1000,
    "tokens_min": 8,
    "tokens_top": 300,
    "causas": [
        ["apagon", "apagon|cero general|cero en toda|cero nacional|incidente del sistema|28 de abril|28 04 2025"],
        ["meteorologia", "tormenta|rayo|descarga atmosf|viento|vendaval|temporal|lluvia|nieve|nevada|hielo|"
                         "helada|granizo|borrasca|tempestad|inundac|riada|dana|meteorolog|metereolog|climatolog"],
        ["incendio", "incendio|fuego|quema"],
        ["fauna", "pajar|\\bave\\b|\\baves\\b|cigu|nido|animal|roedor|\\brata|raton|serpiente|culebra|"
                  "\\bgato|gardun|fauna|cuervo|paloma|buitre|aguila|lagart|cabra|vaca"],
        ["vegetacion", "arbol|\\brama|poda|vegetac|arbust|eucalipt|\\bpino|chopo|encina|alcornoque|olivo|maleza"],
        ["terceros", "excavad|retroexcav|maquina|\\bobra|camion|vehicul|tractor|\\bcoche|accidente|choque|chocad|"
                     "golpe|\\bgrua|tercero|vandal|\\brobo|hurto|sustrac|cosechad|ajena|particular|abonado"],
        ["red_superior", "subestacion|\\bstr\\b|\\bset\\b|transporte|red electrica|\\bree\\b|iberdrola|endesa|"
                         "naturgy|aguas arriba|alta tension|alimentacion|cero de tension|alimentador|interconexion"],
        ["sobrecarga", "sobrecarga|sobreintensidad|exceso de carga|sobrecalent|calentamiento"],
        ["desconocida", "desconoc|sin causa|no se encuentra|no localiz|causa no|investigando|sin determinar"],
        ["equipo", "transformador|\\btrafo|celda|fusible|autovalvula|pararrayo|seccionador|aislador|aislamiento|"
                   "empalme|terminal|botella|\\bcable|conductor|puente|grapa|cuadro|\\bposte|apoyo|cruceta|bornas"],
        ["averia_generica", "averia|rotura|defecto|cortocircuito|derivacion|\\bfalta\\b"],
        ["actuacion_proteccion", "bloqueo|disparo|reenganche|reconectador|interruptor|proteccion|\\bsalta"],
        ["trabajos", "maniobra|descargo|trabajo|mantenimiento|revision|programad|reforma|sustitucion|mejora"],
    ],
    # Days of an external event that would hide everything else (the blackout of
    # the Iberian peninsula of 28.04.2025): out of every section except the
    # first one, where they are shown apart.
    "dias_excluidos": {"2025-04-28": "apagon peninsular"},
    # 3 year mean of the incentive TIEPI the CNMC used for Q2019 (Orden
    # TED/749/2022), to check ours.
    "tiepi_oficial_media3": {"2015-2017": {"Calser_EOSA": 0.67, "Calser_Pitarch": 1.04,
                                           "Calser_ValleSantaAna": 3.28}},
}

STOPWORDS = set(
    "de la el en y a los las del se por con para un una al lo que sin su sus es no ha han le "
    "se ct cts linea lineas tension baja media alta hora horas dia dias".split()
)


# ---------------------------------------------------------------------------
# Arguments, configuration, helpers
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--secciones", default=None,
                        help="Comma separated subset: tiepi,sector,limites,causas,cuando,donde,reposicion,meteo,carga,"
                             "edad,economia,obras")
    parser.add_argument("--dashboard-dir", default=None, help="folder of dashboard/data to leave sector.json in")
    args, _ = parser.parse_known_args(argv)

    if not args.run_id:
        args.run_id = "red_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

    return args


def settings(config: dict) -> dict:
    cfg = dict(DEFAULTS)
    cfg.update(config.get("analisis_red", {}))
    # The names the outputs show for each distribuidora (the dashboard uses the
    # same ones, from riesgo_manana.nombres); without them, the source database.
    if "nombres" not in cfg:
        cfg["nombres"] = dict(config.get("riesgo_manana", {}).get("nombres", {}))
    return cfg


def table_exists(spark, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # noqa: BLE001
        return False


def read_table(spark, name: str, columns: list = None):
    """The table with the columns of the list it has (missing ones are left out), or None."""
    if not table_exists(spark, name):
        logger.warning("Table %s not found", name)
        return None

    df = spark.table(name)

    if columns:
        df = df.select(*[c for c in columns if c in df.columns])

    return df


def normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""

    text = unicodedata.normalize("NFKD", str(value).lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def province_of(municipio_id) -> str:
    if municipio_id is None or (isinstance(municipio_id, float) and math.isnan(municipio_id)):
        return "??"

    text = re.sub(r"\D", "", str(municipio_id))
    return text.zfill(5)[:2] if text else "??"


def share(part: float, total: float) -> float:
    return float(part) / float(total) if total else float("nan")


def fmt(value, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "-"

    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}".replace(",", ".")

    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct(value, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"

    return fmt(100.0 * value, digits) + " %"


def md_table(pdf: pd.DataFrame, columns: list = None, max_rows: int = 30) -> str:
    if pdf is None or pdf.empty:
        return "_Sin datos._\n"

    data = pdf[columns] if columns else pdf
    data = data.head(max_rows)
    header = "| " + " | ".join(str(c) for c in data.columns) + " |"
    sep = "| " + " | ".join("---" for _ in data.columns) + " |"
    rows = []

    # Float columns whose values are all whole numbers (years, counts) are shown as integers.
    whole = set()
    for column in data.columns:
        values = pd.to_numeric(data[column], errors="coerce") if data[column].dtype.kind == "f" else None
        if values is not None and values.notna().any() and (values.dropna() % 1 == 0).all():
            whole.add(column)

    for _, row in data.iterrows():
        cells = []
        for column, value in zip(data.columns, row.tolist()):
            year_like = "anio" in str(column) and not str(column).startswith("anios")
            if isinstance(value, float) and column in whole and not math.isnan(value):
                cells.append(str(int(value)) if year_like else fmt(int(value)))
            elif isinstance(value, float):
                cells.append(fmt(value, 3 if abs(value) < 10 else 1))
            elif isinstance(value, (int, np.integer)) and not year_like:
                cells.append(fmt(int(value)))
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")

    return "\n".join([header, sep, *rows]) + "\n"


class Report:
    """Markdown sections and CSV files of the run."""

    def __init__(self, folder: str, run_id: str):
        self.folder = os.path.join(folder, run_id) if folder else None
        self.run_id = run_id
        self.parts = [f"# Analisis de red {run_id}\n"]

        if self.folder:
            os.makedirs(self.folder, exist_ok=True)

    def section(self, title: str, text: str):
        self.parts.append(f"\n## {title}\n\n{text.strip()}\n")

    def csv(self, name: str, pdf: pd.DataFrame):
        if self.folder and pdf is not None:
            pdf.to_csv(os.path.join(self.folder, name), index=False)

    def json(self, name: str, payload: dict):
        if self.folder and payload is not None:
            with open(os.path.join(self.folder, name), "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=1)

    def write(self) -> str:
        text = "\n".join(self.parts)

        if self.folder:
            with open(os.path.join(self.folder, "resumen.md"), "w", encoding="utf-8") as file:
                file.write(text)

        return text


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

EVENT_COLUMNS = [
    "variante", "distribuidora_id", "source_database", "ct_id", "inicio_ts", "fin_ts", "duracion_s",
    "periodo_id", "incidencia_id", "clasificacion_id", "factor_id", "tipo_evento_familia",
    "n_cts_incidencia", "es_origen_scada",
]

DIM_COLUMNS = [
    "distribuidora_id", "ct_id", "municipio_id", "municipio_nombre", "potencia_tiepi_kva", "potencia_kva",
    "potencia_nominal_kva", "num_abonados", "tipo_zona_codigo", "grupo_red_id", "n_ct_grupo",
    "trafo_elemento_id", "anchor_id", "tiene_telemetria", "observable", "vigente_desde", "vigente_hasta_excl",
    "n_trafos_ct", "en_estudio",
]


class Data:
    """The small tables of the analysis, in pandas."""

    def __init__(self, spark, cfg: dict):
        self.spark = spark
        self.cfg = cfg
        self.dim = self._dim()
        self.events_all = self._events()
        self.events = self.events_all[~self.events_all["excluido"]]
        # sources: {distribuidora_id: Calser database} (the keys of the remuneration
        # and of the official indices); databases: {distribuidora_id: name shown}.
        self.sources = self._databases()
        names = {str(k): str(v) for k, v in (cfg.get("nombres") or {}).items()}
        self.databases = {d: names.get(str(int(d)), db) for d, db in self.sources.items()}

    def _dim(self) -> pd.DataFrame:
        df = read_table(self.spark, DIM_TABLE, DIM_COLUMNS)

        if df is None:
            raise RuntimeError(f"{DIM_TABLE} is required")

        dim = df.toPandas()

        for column in DIM_COLUMNS:
            if column not in dim.columns:
                dim[column] = np.nan

        kva = pd.to_numeric(dim["potencia_tiepi_kva"], errors="coerce")
        kva = kva.where(kva > 0, pd.to_numeric(dim["potencia_kva"], errors="coerce"))
        dim["kva"] = kva.fillna(0.0).astype(float)
        dim["zona"] = pd.to_numeric(dim["tipo_zona_codigo"], errors="coerce").fillna(3).astype(int)
        dim["provincia"] = dim["municipio_id"].map(province_of)
        dim["num_abonados"] = pd.to_numeric(dim["num_abonados"], errors="coerce").fillna(0)
        dim["vigente_desde"] = pd.to_datetime(dim["vigente_desde"], errors="coerce")
        dim["vigente_hasta_excl"] = pd.to_datetime(dim["vigente_hasta_excl"], errors="coerce")
        dim["ct_id"] = dim["ct_id"].astype(str)

        upstream = self._feeders(dim)
        dim = dim.merge(upstream, on=["trafo_elemento_id"], how="left")
        dim["cabecera"] = dim["cabecera"].fillna("sin_cabecera")
        dim["grupo_red_id"] = dim["grupo_red_id"].fillna("sin_grupo").astype(str)
        return dim

    def _feeders(self, dim: pd.DataFrame) -> pd.DataFrame:
        """For every transformer, the upstream switch that cut it most often (its feeder)."""
        df = read_table(self.spark, UPSTREAM_TABLE,
                        ["trafo_elemento_id", "elemento_corte_id", "elemento_corte_nombre", "n_cortes"])

        if df is None:
            return pd.DataFrame({"trafo_elemento_id": dim["trafo_elemento_id"].unique(), "cabecera": "sin_cabecera"})

        pdf = df.toPandas()

        if pdf.empty:
            return pd.DataFrame({"trafo_elemento_id": [], "cabecera": []})

        pdf["nombre"] = pdf["elemento_corte_nombre"].fillna(pdf["elemento_corte_id"].astype(str))
        pdf = pdf.sort_values(["trafo_elemento_id", "n_cortes", "nombre"], ascending=[True, False, True])
        top = pdf.drop_duplicates("trafo_elemento_id")
        return top.rename(columns={"nombre": "cabecera"})[["trafo_elemento_id", "cabecera"]]

    def _events(self) -> pd.DataFrame:
        df = read_table(self.spark, FACT_TABLE, EVENT_COLUMNS)

        if df is None:
            raise RuntimeError(f"{FACT_TABLE} is required")

        ev = df.where(F.col("variante").isin("principal", "todas")).toPandas()

        for column in EVENT_COLUMNS:
            if column not in ev.columns:
                ev[column] = np.nan

        ev["ct_id"] = ev["ct_id"].astype(str)
        ev["inicio_ts"] = pd.to_datetime(ev["inicio_ts"])
        ev["fin_ts"] = pd.to_datetime(ev["fin_ts"])
        cap = float(self.cfg["duracion_max_h"]) * 3600.0
        ev["dur_h"] = pd.to_numeric(ev["duracion_s"], errors="coerce").clip(lower=0, upper=cap) / 3600.0
        ev["anio"] = ev["inicio_ts"].dt.year
        ev = ev[(ev["anio"] >= int(self.cfg["anio_desde"])) & (ev["anio"] <= int(self.cfg["anio_hasta"]))]

        incidents = self._incidents()
        if incidents is not None and not incidents.empty:
            ev = ev.merge(incidents, on=["distribuidora_id", "periodo_id", "incidencia_id"], how="left")
        else:
            for column in ("tipo_equipo_id", "texto"):
                ev[column] = np.nan

        attrs = self.dim[["distribuidora_id", "ct_id", "kva", "zona", "provincia", "municipio_id",
                          "municipio_nombre", "grupo_red_id", "cabecera", "num_abonados", "tiene_telemetria"]]
        ev = ev.merge(attrs, on=["distribuidora_id", "ct_id"], how="left")
        ev["kva"] = ev["kva"].fillna(0.0)
        ev["kva_h"] = ev["kva"] * ev["dur_h"]

        ev["factor"] = ev["factor_id"].map(FACTOR_NAMES).fillna("sin_factor")
        has_incident = ev["incidencia_id"].notna() & ev["clasificacion_id"].notna()
        planned = ev["clasificacion_id"] == "CL_PROGR"
        unplanned = ev["clasificacion_id"] == "CL_IMPRE"
        client = ev["factor_id"] == "FA_CLIEN"

        ev["grupo"] = np.select(
            [~has_incident, client, planned, unplanned],
            ["sin_incidencia", "cliente", "programada", "imprevista"],
            default="otra",
        )
        # Regulated TIEPI (Calser): incident required, FA_CLIEN out.
        ev["computa_tiepi"] = has_incident & ~client & (planned | unplanned)
        # Quality incentive of the CNMC: planned of transport and distribution,
        # unplanned of transport and propias.
        ev["computa_incentivo"] = has_incident & (
            (planned & ev["factor_id"].isin(["FA_TRANS", "FA_DISTR"]))
            | (unplanned & ev["factor_id"].isin(["FA_TRANS", "FA_PROPI"]))
        )
        # Zonal and individual quality of RD 1955/2000: unplanned only.
        ev["computa_imprevista"] = has_incident & unplanned & ~client
        ev["incidente"] = (
            ev["distribuidora_id"].astype(str) + "|" + ev["periodo_id"].astype(str) + "|"
            + ev["incidencia_id"].astype(str)
        ).where(has_incident, None)

        # An event belongs to an excluded day when its incident (or the event
        # itself, without incident) starts that day.
        first = ev.groupby("incidente")["inicio_ts"].transform("min")
        day = first.fillna(ev["inicio_ts"]).dt.strftime("%Y-%m-%d")
        ev["excluido"] = day.isin(set(self.cfg.get("dias_excluidos", {})))
        return ev

    def _incidents(self):
        df = read_table(self.spark, INCIDENTS_TABLE,
                        ["distribuidora_id", "periodo_id", "id", "descripcion", "observaciones", "resolucion",
                         "tipo_equipo_id"])

        if df is None:
            return None

        pdf = df.toPandas().rename(columns={"id": "incidencia_id"})

        for column in ("descripcion", "observaciones", "resolucion", "tipo_equipo_id"):
            if column not in pdf.columns:
                pdf[column] = None

        pdf["texto"] = (
            pdf["descripcion"].map(normalize_text) + " " + pdf["observaciones"].map(normalize_text) + " "
            + pdf["resolucion"].map(normalize_text)
        ).str.strip()
        pdf = pdf.drop_duplicates(["distribuidora_id", "periodo_id", "incidencia_id"])
        return pdf[["distribuidora_id", "periodo_id", "incidencia_id", "tipo_equipo_id", "texto"]]

    def _databases(self) -> dict:
        pairs = self.events_all[["distribuidora_id", "source_database"]].dropna().drop_duplicates()
        return dict(zip(pairs["distribuidora_id"], pairs["source_database"]))

    def installed(self, year: int, by: list) -> pd.DataFrame:
        """Installed kVA (TIEPI power) and customers of the CTs valid in the year, by the columns of by."""
        lo, hi = pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year + 1, month=1, day=1)
        dim = self.dim
        valid = (dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi)) & (
            dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo)
        )
        out = dim[valid].groupby(by, dropna=False).agg(
            p_total_kva=("kva", "sum"), abonados=("num_abonados", "sum"), n_cts=("ct_id", "count")
        )
        return out.reset_index()


def years_of(data: Data) -> list:
    return list(range(int(data.cfg["anio_desde"]), int(data.cfg["anio_hasta"]) + 1))


# ---------------------------------------------------------------------------
# 1. TIEPI by year, classification and factor
# ---------------------------------------------------------------------------

def niepi_numerator(events: pd.DataFrame, by: list) -> pd.DataFrame:
    """kVA of the CTs affected, every CT once per incident (Calser), every event once without incident."""
    fallback = pd.Series("ev|" + events.index.astype(str), index=events.index)
    key = events["incidente"].fillna(fallback)
    unique = events.assign(_k=key).drop_duplicates(["_k", "distribuidora_id", "ct_id"])
    return unique.groupby(by, dropna=False)["kva"].sum().rename("kva_niepi").reset_index()


def excluded_days_table(data: Data) -> pd.DataFrame:
    ev = data.events_all[(data.events_all["variante"] == "todas") & data.events_all["excluido"]]
    if ev.empty:
        return pd.DataFrame()
    rows = []
    for (dist, year), g in ev.groupby(["distribuidora_id", "anio"]):
        p_total = data.installed(int(year), ["distribuidora_id"])
        p_total = p_total.loc[p_total["distribuidora_id"] == dist, "p_total_kva"].sum()
        rows.append({
            "distribuidora": data.databases.get(dist, dist), "anio": int(year),
            "dias": ", ".join(sorted(g["inicio_ts"].dt.strftime("%d.%m.%Y").unique())),
            "cts": g["ct_id"].nunique(), "tiepi_h": share(g.loc[g["computa_tiepi"], "kva_h"].sum(), p_total),
            "tiepi_sin_incidencia_h": share(g.loc[g["grupo"] == "sin_incidencia", "kva_h"].sum(), p_total),
            "factores": ", ".join(sorted(g.loc[g["computa_tiepi"], "factor"].unique())),
        })
    return pd.DataFrame(rows)


def no_incident_bands(data: Data) -> pd.DataFrame:
    ev = data.events[(data.events["variante"] == "todas") & (data.events["grupo"] == "sin_incidencia")]
    if ev.empty:
        return pd.DataFrame()
    bands = pd.cut(ev["dur_h"], [0, 1, 4, 24, 72, 1e9], right=True, labels=["<=1h", "1-4h", "4-24h", "24-72h", ">72h"])
    g = ev.groupby(bands, observed=False).agg(registros=("ct_id", "size"), kva_h=("kva_h", "sum")).reset_index()
    g = g.rename(columns={"dur_h": "duracion"})
    g["parte_kva_h"] = g["kva_h"] / g["kva_h"].sum()
    return g


def section_tiepi(data: Data, report: Report, state: dict):
    ev = data.events[data.events["variante"] == "todas"]
    rows, zones = [], []

    for year in years_of(data):
        sub = ev[ev["anio"] == year]
        if sub.empty:
            continue

        inst = data.installed(year, ["distribuidora_id"])
        g = sub.groupby(["distribuidora_id", "grupo", "factor"], dropna=False).agg(
            n_incidencias=("incidente", "nunique"),
            n_interrupciones=("ct_id", "size"),
            kva_h=("kva_h", "sum"),
            dur_p50_min=("dur_h", lambda s: 60.0 * s.median()),
            dur_p90_min=("dur_h", lambda s: 60.0 * s.quantile(0.9)),
        ).reset_index()
        g = g.merge(niepi_numerator(sub, ["distribuidora_id", "grupo", "factor"]),
                    on=["distribuidora_id", "grupo", "factor"], how="left")
        g = g.merge(inst, on="distribuidora_id", how="left")
        g["anio"] = year
        g["tiepi_h"] = g["kva_h"] / g["p_total_kva"]
        g["niepi"] = g["kva_niepi"] / g["p_total_kva"]
        rows.append(g)

        # Incentive TIEPI by zone type, for the money section.
        inst_z = data.installed(year, ["distribuidora_id", "zona"])
        inc = sub[sub["computa_incentivo"]].groupby(["distribuidora_id", "zona"])["kva_h"].sum().reset_index()
        imp = sub[sub["computa_imprevista"]].groupby(["distribuidora_id", "zona"])["kva_h"].sum()
        imp = imp.rename("kva_h_imprevista").reset_index()
        z = inst_z.merge(inc, on=["distribuidora_id", "zona"], how="left").merge(
            imp, on=["distribuidora_id", "zona"], how="left")
        z["anio"] = year
        z["kva_h"] = z["kva_h"].fillna(0.0)
        z["kva_h_imprevista"] = z["kva_h_imprevista"].fillna(0.0)
        z["tiepi_incentivo_h"] = z["kva_h"] / z["p_total_kva"]
        z["tiepi_imprevista_h"] = z["kva_h_imprevista"] / z["p_total_kva"]
        zones.append(z)

    if not rows:
        report.section("1. TIEPI por año", "_Sin interrupciones en los años pedidos._")
        return

    table = pd.concat(rows, ignore_index=True)
    rules = {
        ("programada", "transporte"): True, ("programada", "distribucion"): True,
        ("imprevista", "transporte"): True, ("imprevista", "propias"): True,
    }
    table["computa_incentivo"] = [rules.get((g, f), False) for g, f in zip(table["grupo"], table["factor"])]
    table["computa_tiepi"] = table["grupo"].isin(["programada", "imprevista"])
    table["source_database"] = table["distribuidora_id"].map(data.sources)
    table["distribuidora"] = table["distribuidora_id"].map(data.databases)
    report.csv("tiepi_anual.csv", table)
    by_zone = pd.concat(zones, ignore_index=True)
    report.csv("tiepi_incentivo_zona.csv", by_zone)
    state["tiepi_zona"] = by_zone

    summary = []
    for (dist, year), g in table.groupby(["distribuidora_id", "anio"]):
        total = g.loc[g["computa_tiepi"], "tiepi_h"].sum()
        summary.append({
            "distribuidora": data.databases.get(dist, dist),
            "anio": int(year),
            "tiepi_h": total,
            "tiepi_imprevista_h": g.loc[g["grupo"] == "imprevista", "tiepi_h"].sum(),
            "tiepi_programada_h": g.loc[g["grupo"] == "programada", "tiepi_h"].sum(),
            "tiepi_incentivo_h": g.loc[g["computa_incentivo"], "tiepi_h"].sum(),
            "propias": share(g.loc[(g["grupo"] == "imprevista") & (g["factor"] == "propias"), "tiepi_h"].sum(), total),
            "terceros": share(g.loc[(g["grupo"] == "imprevista") & (g["factor"] == "terceros"), "tiepi_h"].sum(), total),
            "fuerza_mayor": share(g.loc[(g["grupo"] == "imprevista") & (g["factor"] == "fuerza_mayor"), "tiepi_h"].sum(),
                                  total),
            "transporte": share(g.loc[g["factor"] == "transporte", "tiepi_h"].sum(), total),
            "sin_incidencia_h": g.loc[g["grupo"] == "sin_incidencia", "tiepi_h"].sum(),
            "niepi": g.loc[g["computa_tiepi"], "niepi"].sum(),
            "incidencias": int(g.loc[g["computa_tiepi"], "n_incidencias"].sum()),
        })

    summary = pd.DataFrame(summary).sort_values(["distribuidora", "anio"])
    for column in ("tiepi_h", "tiepi_imprevista_h", "tiepi_incentivo_h"):
        summary[column + "_media3"] = summary.groupby("distribuidora")[column].transform(
            lambda s: s.rolling(3, min_periods=3).mean())
    report.csv("tiepi_resumen.csv", summary)
    state["tiepi_resumen"] = summary

    shown = summary.copy()
    for column in ("propias", "terceros", "fuerza_mayor", "transporte"):
        shown[column] = shown[column].map(pct)

    bands = no_incident_bands(data)
    excluded = excluded_days_table(data)
    report.csv("tiepi_dias_excluidos.csv", excluded)
    report.csv("sin_incidencia_duracion.csv", bands)
    check = []
    label_of = {src: data.databases.get(d, src) for d, src in data.sources.items()}
    for window, values in data.cfg.get("tiepi_oficial_media3", {}).items():
        lo, hi = [int(x) for x in window.split("-")]
        for db, official in values.items():
            label = label_of.get(db, db)
            ours = summary[(summary["distribuidora"] == label) & summary["anio"].between(lo, hi)]
            if len(ours) == hi - lo + 1:
                check.append({"distribuidora": label, "anios": window, "nuestro_h": ours["tiepi_incentivo_h"].mean(),
                              "cnmc_h": float(official)})
    check = pd.DataFrame(check)
    if not check.empty:
        check["diferencia"] = (check["nuestro_h"] / check["cnmc_h"] - 1.0).map(pct)
    report.csv("tiepi_validacion_cnmc.csv", check)

    totals = table[table["computa_tiepi"]].groupby(["grupo", "factor"])["kva_h"].sum()
    all_kvah = totals.sum()
    lines = [f"- {g} / {f}: {pct(share(v, all_kvah))} de los kVA·h que cuentan para el TIEPI"
             for (g, f), v in totals.sort_values(ascending=False).items()]
    no_incident = table.loc[table["grupo"] == "sin_incidencia", "kva_h"].sum()

    report.section(
        "1. TIEPI por año, clasificación y factor",
        "TIEPI con las reglas de Calser (nivel CT, más de 180 s, con incidencia y sin el factor cliente). "
        "La columna incentivo es la parte que cuenta para el incentivo de calidad de la CNMC (programadas, "
        "imprevistas de transporte y propias). media3 es la media móvil de tres años, la que usa el incentivo.\n\n"
        + md_table(shown, ["distribuidora", "anio", "tiepi_h", "tiepi_imprevista_h", "tiepi_incentivo_h",
                           "tiepi_incentivo_h_media3", "propias", "terceros", "fuerza_mayor", "transporte",
                           "niepi", "incidencias"], max_rows=60)
        + "\nReparto de todos los años:\n\n" + "\n".join(lines)
        + f"\n\nInterrupciones de CT sin incidencia (no cuentan en Calser): {fmt(no_incident, 0)} kVA·h, "
        f"{pct(share(no_incident, all_kvah + no_incident))} del total. Por duración:\n\n"
        + md_table(bands.assign(parte_kva_h=bands["parte_kva_h"].map(pct)) if not bands.empty else bands)
        + "\nDías excluidos del resto del análisis (analisis_red.dias_excluidos), con su TIEPI aparte:\n\n"
        + md_table(excluded)
        + "\nComprobación con el TIEPI medio de tres años que usó la CNMC para el incentivo de 2019 (Orden "
        "TED/749/2022):\n\n" + md_table(check)
    )


# ---------------------------------------------------------------------------
# 1b. The sector: the official TIEPI and NIEPI by zone type (GECOS, MITECO)
# ---------------------------------------------------------------------------

ZONE_CODES = {"U": 1, "S": 2, "RC": 3, "RD": 4}


def sector_frame(spark, cfg: dict) -> pd.DataFrame:
    """The official indices of the sector from Bronze or, without the table, from the CSV next to the configuration."""
    df = read_table(spark, SECTOR_TABLE)
    if df is not None:
        pdf = df.toPandas()
    else:
        candidates = [os.path.join(HERE, "..", "..", "config", "01_bronze", "gecos_indices_zonales.csv"),
                      "/app/config/01_bronze/gecos_indices_zonales.csv", "/opt/airflow/config/01_bronze/gecos_indices_zonales.csv"]
        path = next((c for c in candidates if os.path.exists(c)), None)
        if path is None:
            return pd.DataFrame()
        pdf = pd.read_csv(path)
    if pdf.empty:
        return pdf
    for column in ("potencia_mva", "suministros", "prog_total", "imp_transporte", "imp_terceros", "imp_fuerza_mayor",
                   "imp_propias", "imp_total", "valor"):
        if column in pdf.columns:
            pdf[column] = pd.to_numeric(pdf[column], errors="coerce")
    pdf["anio"] = pd.to_numeric(pdf["anio"], errors="coerce").astype("Int64")
    pdf["zona_codigo"] = pdf["zona"].map(ZONE_CODES)
    pdf["tiepi_incentivo_h"] = pdf["prog_total"].fillna(0.0) + pdf["imp_transporte"].fillna(0.0) + pdf["imp_propias"].fillna(0.0)
    return pdf


def clean(value):
    """A JSON-friendly number (None when missing)."""
    try:
        return None if value is None or pd.isna(value) else round(float(value), 4)
    except (TypeError, ValueError):
        return None


def sector_payload(data: Data, official: pd.DataFrame, ours_last: pd.DataFrame, ours_same: pd.DataFrame,
                   series: pd.DataFrame, scopes: list, years_ours: list, years_sector: list) -> dict:
    """sector.json for negocio.html: our TIEPI by zone next to the official one, and the yearly series of the sector."""
    distributors = []
    for dist, g in ours_last.groupby("distribuidora_id"):
        same = ours_same[ours_same["distribuidora_id"] == dist].set_index("zona")
        weights = g.set_index("zona")["p_total_kva"] / g["p_total_kva"].sum()
        zones = []
        for _, z in g.iterrows():
            zone = int(z["zona"])
            sector = {}
            for _, o in official[official["zona_codigo"] == zone].iterrows():
                sector[o["ambito"]] = {"imprevista": clean(o["tiepi_imprevista_h"]), "incentivo": clean(o["tiepi_incentivo_h"]),
                                       "fuerza_mayor": clean(o["fuerza_mayor_h"])}
            zones.append({"zona": ZONE_NAMES.get(zone, str(zone)), "abonados": int(z["abonados"]) if pd.notna(z["abonados"]) else None,
                          "peso": clean(weights.get(zone)), "imprevista": clean(z["tiepi_imprevista_h"]),
                          "incentivo": clean(z["tiepi_incentivo_h"]),
                          "imprevista_mismos_anios": clean(same["tiepi_imprevista_h"].get(zone)),
                          "incentivo_mismos_anios": clean(same["tiepi_incentivo_h"].get(zone)), "sector": sector})
        mix = {"nuestra": clean((g.set_index("zona")["tiepi_imprevista_h"] * weights).sum())}
        for _, name in scopes:
            o = official[official["ambito"] == name].set_index("zona_codigo")["tiepi_imprevista_h"]
            if not o.empty:
                mix[name] = clean(sum(weights.get(z, 0.0) * o.get(int(z), np.nan) for z in weights.index))
        distributors.append({"id": int(dist), "nombre": data.databases.get(dist, str(dist)), "zonas": zones, "mezcla": mix})
    yearly = {}
    for (name, zone), g in series.groupby(["nombre", "zona"]):
        yearly.setdefault(name, {})[zone] = [
            {"anio": int(r["anio"]), "tiepi": clean(r["valor"]), "imprevista": clean(r["imp_total"]),
             "fuerza_mayor": clean(r["imp_fuerza_mayor"]), "propias": clean(r["imp_propias"]), "incentivo": clean(r["tiepi_incentivo_h"])}
            for _, r in g.sort_values("anio").iterrows()]
    return {"generado": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC"),
            "fuente": "GECOS (MITECO), índices zonales oficiales de calidad de suministro",
            "anios_sector": years_sector, "anios_nuestros": [min(years_ours), max(years_ours)],
            "ambitos": [name for _, name in scopes], "distribuidoras": distributors, "serie": yearly}


def section_sector(data: Data, report: Report, state: dict):
    """Our TIEPI by zone type next to the official one of the province, the community and Spain."""
    zones = state.get("tiepi_zona")
    sector = sector_frame(data.spark, data.cfg)
    if zones is None or zones.empty or sector.empty:
        report.section("1b. Frente al sector", "_Sin TIEPI por zona o sin los índices oficiales (l1_bronze.gecos_indice_zonal)._")
        return
    tiepi = sector[(sector["indice"] == "tiepi") & sector["zona_codigo"].notna()].copy()
    last_year = int(tiepi["anio"].max())
    window = int(data.cfg.get("sector_anios_media", 3))
    first_year = last_year - window + 1
    scopes = [tuple(a) for a in data.cfg.get("sector_ambitos", [])]
    recent = tiepi[tiepi["anio"].between(first_year, last_year)]

    # The sector in the last published years, by place and zone.
    official = []
    for scope, name in scopes:
        g = recent[(recent["ambito"] == scope) & (recent["nombre"] == name)]
        if g.empty:
            continue
        agg = g.groupby("zona_codigo").agg(tiepi_h=("valor", "mean"), tiepi_imprevista_h=("imp_total", "mean"),
                                          tiepi_incentivo_h=("tiepi_incentivo_h", "mean"),
                                          fuerza_mayor_h=("imp_fuerza_mayor", "mean"), anios=("anio", "nunique")).reset_index()
        agg["ambito"] = name
        official.append(agg)
    if not official:
        report.section("1b. Frente al sector", "_Los ámbitos de sector_ambitos no están en los índices oficiales._")
        return
    official = pd.concat(official, ignore_index=True)

    # Ours in the same years (when we have them) and in our last three years.
    years_incentive = [int(y) for y in data.cfg["anios_incentivo"]]
    ours_same = zones[zones["anio"].between(first_year, last_year)].groupby(["distribuidora_id", "zona"]).agg(
        tiepi_imprevista_h=("tiepi_imprevista_h", "mean"), tiepi_incentivo_h=("tiepi_incentivo_h", "mean"),
        p_total_kva=("p_total_kva", "last"), abonados=("abonados", "last"), anios=("anio", "nunique")).reset_index()
    ours_last = zones[zones["anio"].isin(years_incentive)].groupby(["distribuidora_id", "zona"]).agg(
        tiepi_imprevista_h=("tiepi_imprevista_h", "mean"), tiepi_incentivo_h=("tiepi_incentivo_h", "mean"),
        p_total_kva=("p_total_kva", "last"), abonados=("abonados", "last"), anios=("anio", "nunique")).reset_index()

    rows = []
    for dist, g in ours_last.groupby("distribuidora_id"):
        db = data.databases.get(dist, str(dist))
        same = ours_same[ours_same["distribuidora_id"] == dist].set_index("zona")
        for _, z in g.iterrows():
            zone = int(z["zona"])
            row = {"distribuidora": db, "zona": ZONE_NAMES.get(zone, zone), "p_total_kva": z["p_total_kva"],
                   "abonados": z["abonados"], "anios_nuestros": f"{min(years_incentive)}-{max(years_incentive)}",
                   "imprevista_nuestra_h": z["tiepi_imprevista_h"], "incentivo_nuestro_h": z["tiepi_incentivo_h"],
                   "anios_sector": f"{first_year}-{last_year}",
                   "imprevista_nuestra_mismos_anios_h": same["tiepi_imprevista_h"].get(zone, np.nan),
                   "incentivo_nuestro_mismos_anios_h": same["tiepi_incentivo_h"].get(zone, np.nan)}
            for _, o in official[official["zona_codigo"] == zone].iterrows():
                key = o["ambito"].lower().replace("á", "a").replace("ñ", "n").replace(" ", "_")
                row[f"imprevista_{key}_h"] = o["tiepi_imprevista_h"]
                row[f"incentivo_{key}_h"] = o["tiepi_incentivo_h"]
                row[f"fuerza_mayor_{key}_h"] = o["fuerza_mayor_h"]
            rows.append(row)
    table = pd.DataFrame(rows)
    report.csv("tiepi_sector.csv", table)
    state["tiepi_sector"] = {"tabla": table, "oficial": official, "anios": [first_year, last_year]}

    # The whole series of the sector for the document: Spain and the places, every year, TIEPI and its force majeure.
    all_zones = sector[sector["indice"] == "tiepi"]
    series = all_zones[all_zones["ambito"].isin([a for a, _ in scopes]) & all_zones["nombre"].isin([n for _, n in scopes])]
    report.csv("sector_series.csv", series[["ambito", "nombre", "anio", "zona", "valor", "prog_total", "imp_total",
                                            "imp_propias", "imp_terceros", "imp_fuerza_mayor", "tiepi_incentivo_h"]])

    # Weighted by our mix of zones: what the sector would have with our network.
    lines = []
    for dist, g in ours_last.groupby("distribuidora_id"):
        db = data.databases.get(dist, str(dist))
        weights = g.set_index("zona")["p_total_kva"] / g["p_total_kva"].sum()
        ours = float((g.set_index("zona")["tiepi_imprevista_h"] * weights).sum())
        parts = []
        for scope, name in scopes:
            o = official[official["ambito"] == name].set_index("zona_codigo")["tiepi_imprevista_h"]
            if o.empty:
                continue
            mixed = float(sum(weights.get(z, 0.0) * o.get(int(z), np.nan) for z in weights.index))
            parts.append(f"{name} {fmt(mixed, 2)} h")
        lines.append(f"- {db}: TIEPI imprevisto de {min(years_incentive)}-{max(years_incentive)} {fmt(ours, 2)} h al año; "
                     f"el sector con la misma mezcla de zonas ({first_year}-{last_year}): " + ", ".join(parts))

    state["sector_json"] = sector_payload(data, official, ours_last, ours_same, series, scopes, years_incentive,
                                          [first_year, last_year])
    report.json("sector.json", state["sector_json"])

    shown = table.copy()
    for column in shown.columns:
        if column.endswith("_h"):
            shown[column] = shown[column].map(lambda v: fmt(v, 2) if pd.notna(v) else "-")
    columns = ["distribuidora", "zona", "abonados", "imprevista_nuestra_h", "incentivo_nuestro_h"]
    columns += [c for c in shown.columns if c.startswith("imprevista_") and c not in columns and "mismos" not in c]
    report.section(
        "1b. Frente al sector (índices oficiales de GECOS, MITECO)",
        f"TIEPI imprevisto por tipo de zona: el nuestro de {min(years_incentive)}-{max(years_incentive)} frente a la media "
        f"oficial de {first_year}-{last_year} (los últimos años publicados) de la provincia, la comunidad y España. Las columnas "
        "incentivo son la parte que cuenta para el incentivo de la CNMC (programadas, transporte y propias); el fichero "
        "tiepi_sector.csv lleva también la fuerza mayor del sector y nuestro TIEPI en los mismos años.\n\n"
        + md_table(shown, columns) + "\n" + "\n".join(lines)
    )


# ---------------------------------------------------------------------------
# 2. Limits of RD 1955/2000
# ---------------------------------------------------------------------------

def percentile80(values) -> float:
    values = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    return float(np.percentile(values, 80)) if values else float("nan")


def monthly_zone_quality(events: pd.DataFrame, installed: pd.DataFrame, year: int, limits: dict,
                         last_month: int = 12) -> list:
    """
    TIEPI and NIEPI of every month of year (up to last_month) by
    distribuidora, province and zone type, against the monthly limits
    ({zone: [TIEPI h, NIEPI]}) of the zones that have one. The installed
    power is the one of the year, as in the annual figures.
    """
    rows = []
    if installed.empty:
        return rows
    months = events["inicio_ts"].dt.month
    for (dist, prov, zone), inst in installed.groupby(["distribuidora_id", "provincia", "zona"]):
        limit = limits.get(str(int(zone))) if pd.notna(zone) else None
        p_total = float(inst["p_total_kva"].sum())
        if not limit or p_total <= 0:
            continue
        inside = (events["distribuidora_id"] == dist) & events["municipio_id"].isin(set(inst["municipio_id"]))
        for month in range(1, int(last_month) + 1):
            part = events[inside & (months == month)]
            niepi = float(niepi_numerator(part, ["distribuidora_id"])["kva_niepi"].sum()) if len(part) else 0.0
            tiepi = float(part["kva_h"].sum()) / p_total
            rows.append({"distribuidora_id": dist, "provincia": prov, "zona": int(zone), "anio": int(year),
                         "mes": month, "tiepi_h": tiepi, "limite_tiepi_h": float(limit[0]),
                         "niepi": niepi / p_total, "limite_niepi": float(limit[1]),
                         "supera": bool(tiepi > float(limit[0]) or niepi / p_total > float(limit[1]))})
    return rows


def consecutive_months(monthly: pd.DataFrame) -> pd.DataFrame:
    """The longest run of consecutive months above a monthly limit, by distribuidora, province and zone."""
    rows = []
    for key, g in monthly.sort_values(["anio", "mes"]).groupby(["distribuidora_id", "provincia", "zona"]):
        best = run = 0
        for flag in g["supera"]:
            run = run + 1 if flag else 0
            best = max(best, run)
        rows.append({"distribuidora_id": key[0], "provincia": key[1], "zona": key[2], "meses": len(g),
                     "meses_supera": int(g["supera"].sum()), "racha_max_meses": best})
    return pd.DataFrame(rows)


def section_limits(data: Data, report: Report, state: dict):
    cfg = data.cfg
    ev = data.events[(data.events["variante"] == "todas") & data.events["computa_imprevista"]]
    zonal, municipal, individual, monthly = [], [], [], []
    last = ev["inicio_ts"].max() if len(ev) else None

    for year in years_of(data):
        sub = ev[ev["anio"] == year]
        inst_m = data.installed(year, ["distribuidora_id", "provincia", "zona", "municipio_id", "municipio_nombre"])
        if inst_m.empty:
            continue
        monthly += monthly_zone_quality(sub, inst_m, year, cfg.get("limites_mensuales_extremadura") or {},
                                        last.month if last is not None and last.year == year else 12)

        k = sub.groupby(["distribuidora_id", "municipio_id"])["kva_h"].sum().rename("kva_h").reset_index()
        n = niepi_numerator(sub, ["distribuidora_id", "municipio_id"])
        m = inst_m.merge(k, on=["distribuidora_id", "municipio_id"], how="left").merge(
            n, on=["distribuidora_id", "municipio_id"], how="left")
        m[["kva_h", "kva_niepi"]] = m[["kva_h", "kva_niepi"]].fillna(0.0)
        m = m[m["p_total_kva"] > 0]
        m["anio"] = year
        m["tiepi_h"] = m["kva_h"] / m["p_total_kva"]
        m["niepi"] = m["kva_niepi"] / m["p_total_kva"]
        m["limite_p80_h"] = m["zona"].map(lambda z: cfg["limites_zonales"].get(str(z), [None, None, None])[1])
        m["supera_p80"] = m["tiepi_h"] > m["limite_p80_h"]
        municipal.append(m)

        for (dist, prov, zone), g in m.groupby(["distribuidora_id", "provincia", "zona"]):
            lim = cfg["limites_zonales"].get(str(zone), [np.nan, np.nan, np.nan])
            p_total = g["p_total_kva"].sum()
            zonal.append({
                "distribuidora": data.databases.get(dist, dist), "provincia": prov,
                "zona": ZONE_NAMES.get(int(zone), zone), "anio": year, "municipios": len(g),
                "tiepi_h": g["kva_h"].sum() / p_total, "limite_tiepi_h": lim[0],
                "p80_tiepi_h": percentile80(g["tiepi_h"]), "limite_p80_h": lim[1],
                "niepi": g["kva_niepi"].sum() / p_total, "limite_niepi": lim[2],
            })

        # Individual quality: every customer of the CT has the interruptions of the CT.
        per_ct = sub.groupby(["distribuidora_id", "ct_id"]).agg(n_interr=("dur_h", "size"), horas=("dur_h", "sum"))
        per_ct = per_ct.reset_index().merge(
            data.dim[["distribuidora_id", "ct_id", "zona", "num_abonados", "municipio_nombre"]],
            on=["distribuidora_id", "ct_id"], how="left")
        per_ct["anio"] = year
        individual.append(per_ct)

    if not zonal:
        report.section("2. Límites del RD 1955/2000", "_Sin datos._")
        return

    zonal = pd.DataFrame(zonal)
    zonal["supera_tiepi"] = zonal["tiepi_h"] > zonal["limite_tiepi_h"]
    zonal["supera_p80"] = zonal["p80_tiepi_h"] > zonal["limite_p80_h"]
    zonal["supera_niepi"] = zonal["niepi"] > zonal["limite_niepi"]
    report.csv("limites_zonales.csv", zonal)

    municipal = pd.concat(municipal, ignore_index=True).sort_values(["distribuidora_id", "municipio_id", "anio"])

    def streak(flags):
        best = run = 0
        for flag in flags:
            run = run + 1 if flag else 0
            best = max(best, run)
        return best

    streaks = municipal.groupby(["distribuidora_id", "municipio_id", "municipio_nombre", "zona"])["supera_p80"].agg(
        anios_supera="sum", racha_max=streak).reset_index()
    streaks["distribuidora"] = streaks["distribuidora_id"].map(data.databases)
    report.csv("municipios_p80.csv", municipal)
    report.csv("municipios_p80_rachas.csv", streaks)

    individual = pd.concat(individual, ignore_index=True)
    lim_h = individual["zona"].map(lambda z: cfg["limites_individuales_bt"].get(str(z), [np.nan, np.nan])[0])
    lim_n = individual["zona"].map(lambda z: cfg["limites_individuales_bt"].get(str(z), [np.nan, np.nan])[1])
    individual["limite_horas"] = lim_h
    individual["limite_interr"] = lim_n
    power, price = float(cfg["cliente_potencia_kw"]), float(cfg["precio_kwh_eur"])
    cap = 0.10 * float(cfg["factura_anual_eur"])
    by_hours = power * (individual["horas"] - lim_h).clip(lower=0) * 5.0 * price
    by_number = power * individual["horas"] * price * (individual["n_interr"] - lim_n).clip(lower=0) / 8.0
    individual["descuento_cliente_eur"] = np.minimum(np.maximum(by_hours, by_number), cap)
    individual["descuento_total_eur"] = individual["descuento_cliente_eur"] * individual["num_abonados"]
    exceed = individual[(individual["horas"] > lim_h) | (individual["n_interr"] > lim_n)]
    report.csv("calidad_individual.csv", exceed)

    per_year = individual.groupby("anio").apply(
        lambda g: pd.Series({
            "cts_supera": int(((g["horas"] > g["limite_horas"]) | (g["n_interr"] > g["limite_interr"])).sum()),
            "abonados_afectados": int(g.loc[(g["horas"] > g["limite_horas"]) | (g["n_interr"] > g["limite_interr"]),
                                            "num_abonados"].sum()),
            "descuentos_eur": float(g["descuento_total_eur"].sum()),
            "horas_ct_max": float(g["horas"].max()),
            "interr_ct_max": int(g["n_interr"].max()),
        })
    ).reset_index()
    report.csv("calidad_individual_anual.csv", per_year)

    monthly_text = ""
    if monthly:
        monthly = pd.DataFrame(monthly)
        runs = consecutive_months(monthly)
        for frame in (monthly, runs):
            frame["distribuidora"] = frame["distribuidora_id"].map(data.databases)
            frame["zona"] = frame["zona"].map(lambda z: ZONE_NAMES.get(int(z), z))
        report.csv("limites_mensuales_extremadura.csv", monthly)
        report.csv("limites_mensuales_extremadura_rachas.csv", runs)
        monthly_text = (
            "\nLímites mensuales de Extremadura (art. 9.6 de la Ley 2/2002, del Decreto-ley 1/2018: TIEPI de 0,88 h "
            "y NIEPI de 0,74 en zona rural concentrada, 1,32 h y 1,10 en rural dispersa; dos meses seguidos por "
            f"encima obligan a presentar medidas). Meses por encima: {int(monthly['supera'].sum())} de {len(monthly)}.\n\n"
            + md_table(runs, ["distribuidora", "provincia", "zona", "meses", "meses_supera", "racha_max_meses"]))

    over = zonal[zonal["supera_tiepi"] | zonal["supera_p80"] | zonal["supera_niepi"]]
    worst = streaks.sort_values(["racha_max", "anios_supera"], ascending=False).head(10)
    worst = worst.assign(zona=worst["zona"].map(lambda z: ZONE_NAMES.get(int(z), z)))

    report.section(
        "2. Límites del RD 1955/2000",
        "Calidad zonal (art. 106, solo imprevistas): TIEPI, percentil 80 del TIEPI de los municipios y NIEPI "
        "por provincia y tipo de zona.\n\n"
        + md_table(zonal, ["distribuidora", "provincia", "zona", "anio", "municipios", "tiepi_h", "limite_tiepi_h",
                           "p80_tiepi_h", "limite_p80_h", "niepi", "limite_niepi"], max_rows=80)
        + f"\nZonas y años por encima de algún límite: {len(over)} de {len(zonal)}.\n\n"
        "Municipios que más años superan el valor límite del percentil 80 (ninguno puede superarlo más de dos "
        "años seguidos):\n\n"
        + md_table(worst, ["distribuidora", "municipio_nombre", "zona", "anios_supera", "racha_max"])
        + "\nCalidad individual (art. 104, baja tensión, todos los clientes de un CT reciben sus interrupciones; "
        "es un mínimo porque faltan las de salida y abonado) y descuentos del art. 105 con "
        f"{fmt(power, 1)} kW de potencia media y {fmt(price, 4)} EUR/kWh:\n\n"
        + md_table(per_year)
        + monthly_text
    )
    state["individual"] = per_year


# ---------------------------------------------------------------------------
# 3. Causes
# ---------------------------------------------------------------------------

def incident_table(data: Data, variant: str = "todas", mask_column: str = "computa_tiepi") -> pd.DataFrame:
    ev = data.events[(data.events["variante"] == variant) & data.events[mask_column]]

    if ev.empty:
        return pd.DataFrame()

    first = ev.sort_values("inicio_ts").groupby("incidente").agg(
        distribuidora_id=("distribuidora_id", "first"),
        grupo=("grupo", "first"),
        factor=("factor", "first"),
        tipo_equipo_id=("tipo_equipo_id", "first"),
        texto=("texto", "first"),
        inicio_ts=("inicio_ts", "min"),
        fin_ts=("fin_ts", "max"),
        n_cts=("ct_id", "nunique"),
        kva_h=("kva_h", "sum"),
        dur_max_h=("dur_h", "max"),
        anio=("anio", "first"),
    )
    return first.reset_index()


def classify_cause(text: str, patterns: list) -> str:
    if not text:
        return "sin_texto"

    for name, regex in patterns:
        if regex.search(text):
            return name

    return "otras"


def section_causes(data: Data, report: Report, state: dict):
    inc = incident_table(data)

    if inc.empty:
        report.section("3. Causas", "_Sin incidencias._")
        return

    patterns = [(name, re.compile(regex)) for name, regex in data.cfg["causas"]]
    inc["causa"] = inc["texto"].fillna("").map(lambda t: classify_cause(t, patterns))
    inc["tipo_equipo_id"] = inc["tipo_equipo_id"].fillna("sin_dato")
    total = inc["kva_h"].sum()

    def summarize(by):
        g = inc.groupby(by, dropna=False).agg(
            incidencias=("incidente", "size"), kva_h=("kva_h", "sum"),
            cts_medio=("n_cts", "mean"), dur_max_media_h=("dur_max_h", "mean"),
        ).reset_index()
        g["parte_kva_h"] = g["kva_h"] / total
        return g.sort_values("kva_h", ascending=False)

    by_cause = summarize(["causa"])
    by_cause_factor = summarize(["grupo", "factor", "causa"])
    by_equipment = summarize(["grupo", "factor", "tipo_equipo_id"])
    report.csv("causas_texto.csv", by_cause_factor)
    report.csv("causas_equipo.csv", by_equipment)

    # Most frequent words and pairs of the unplanned incidents, to check the patterns.
    words, pairs = Counter(), Counter()
    for text in inc.loc[inc["grupo"] == "imprevista", "texto"].fillna(""):
        tokens = [t for t in text.split() if len(t) >= 4 and t not in STOPWORDS and not t.isdigit()]
        words.update(set(tokens))
        pairs.update(set(zip(tokens, tokens[1:])))

    minimum, top = int(data.cfg["tokens_min"]), int(data.cfg["tokens_top"])
    tokens = pd.DataFrame(
        [("palabra", w, c) for w, c in words.most_common(top) if c >= minimum]
        + [("par", " ".join(p), c) for p, c in pairs.most_common(top) if c >= minimum],
        columns=["tipo", "texto", "incidencias"],
    )
    report.csv("causas_palabras.csv", tokens)

    with_text = (inc["causa"] != "sin_texto").mean()
    by_cause_show = by_cause.assign(parte_kva_h=by_cause["parte_kva_h"].map(pct))
    equip = by_equipment[by_equipment["grupo"] == "imprevista"].head(15)
    equip = equip.assign(parte_kva_h=equip["parte_kva_h"].map(pct))

    report.section(
        "3. Causas",
        f"Incidencias que cuentan para el TIEPI: {len(inc)}. Con texto: {pct(with_text)}. La causa sale del texto "
        "libre de la incidencia con los patrones de analisis_red.causas (el primero que encaja, en ese orden), "
        "así que es orientativa; causas_palabras.csv tiene las palabras más frecuentes para revisarlos.\n\n"
        + md_table(by_cause_show, ["causa", "incidencias", "kva_h", "parte_kva_h", "cts_medio", "dur_max_media_h"])
        + "\nTipo de equipo de las imprevistas (tipo_equipo_id de Calser):\n\n"
        + md_table(equip, ["factor", "tipo_equipo_id", "incidencias", "kva_h", "parte_kva_h", "cts_medio"])
    )
    state["causas"] = by_cause


# ---------------------------------------------------------------------------
# 4. When
# ---------------------------------------------------------------------------

def factor_family(factor: str) -> str:
    return factor if factor in ("propias", "terceros", "fuerza_mayor", "transporte") else "otros"


def section_when(data: Data, report: Report, state: dict):
    inc = incident_table(data, mask_column="computa_imprevista")

    if inc.empty:
        report.section("4. Cuándo", "_Sin incidencias imprevistas._")
        return

    inc["familia"] = inc["factor"].map(factor_family)
    inc["mes"] = inc["inicio_ts"].dt.month
    inc["hora"] = inc["inicio_ts"].dt.hour
    inc["dia_semana"] = inc["inicio_ts"].dt.dayofweek
    inc["laborable_8_18"] = (inc["dia_semana"] < 5) & inc["hora"].between(8, 17)

    parts = []
    for column in ("mes", "hora", "dia_semana"):
        g = inc.groupby(["familia", column]).agg(incidencias=("incidente", "size"), kva_h=("kva_h", "sum"))
        g = g.reset_index().rename(columns={column: "valor"})
        g.insert(0, "dimension", column)
        parts.append(g)

    when = pd.concat(parts, ignore_index=True)
    report.csv("cuando.csv", when)

    working = inc.groupby("familia").agg(
        incidencias=("incidente", "size"), en_horario_laboral=("laborable_8_18", "mean"),
        kva_h=("kva_h", "sum"),
    ).reset_index()
    working["en_horario_laboral"] = working["en_horario_laboral"].map(pct)
    months = inc.groupby("mes")["kva_h"].sum()
    months = (months / months.sum()).map(pct).to_dict()

    report.section(
        "4. Cuándo",
        "Incidencias imprevistas por familia de factor y parte que empieza en horario laboral (lunes a viernes, "
        "de 8 a 18 h, el 30 % de las horas de la semana). Si las de terceros se concentran en ese horario, encaja "
        "con obras y maquinaria (el horario solo no lo prueba), y una fuente pública de obras tendría sentido.\n\n"
        + md_table(working)
        + "\nParte de los kVA·h imprevistos por mes: "
        + ", ".join(f"{m}: {v}" for m, v in months.items()) + "\n"
    )


# ---------------------------------------------------------------------------
# 5. Where: concentration and persistence
# ---------------------------------------------------------------------------

ENTITIES = {
    "ct": ["distribuidora_id", "ct_id"],
    "grupo_red": ["distribuidora_id", "grupo_red_id"],
    "cabecera": ["distribuidora_id", "cabecera"],
    "municipio": ["distribuidora_id", "municipio_id"],
}


def entity_values(events: pd.DataFrame, universe: pd.DataFrame, keys: list, years: list) -> pd.DataFrame:
    sub = events[events["anio"].isin(years)]
    g = sub.groupby(keys).agg(kva_h=("kva_h", "sum"), eventos=("ct_id", "size")).reset_index()
    out = universe.merge(g, on=keys, how="left")
    out[["kva_h", "eventos"]] = out[["kva_h", "eventos"]].fillna(0.0)
    return out


def top_share(values: pd.Series, fraction: float) -> float:
    ordered = values.sort_values(ascending=False)
    k = max(1, int(math.ceil(fraction * len(ordered))))
    total = ordered.sum()
    return share(ordered.head(k).sum(), total)


def section_where(data: Data, report: Report, state: dict):
    ev = data.events[data.events["variante"] == "principal"]
    years = years_of(data)
    fractions = [float(f) for f in data.cfg["fracciones_top"]]
    pareto, persistence = [], []

    for name, keys in ENTITIES.items():
        universe = data.dim[keys].drop_duplicates()
        values = entity_values(ev, universe, keys, years)
        row = {"nivel": name, "entidades": len(values),
               "con_eventos": int((values["eventos"] > 0).sum()), "kva_h": values["kva_h"].sum()}
        for f in [0.01] + fractions:
            row[f"top_{int(round(100 * f))}pct"] = top_share(values["kva_h"], f)
        pareto.append(row)

        for year in data.cfg["anios_persistencia"]:
            past = entity_values(ev, universe, keys, [year - 2, year - 1]).rename(
                columns={"kva_h": "kva_h_prev", "eventos": "eventos_prev"})
            now = entity_values(ev, universe, keys, [year])
            both = past.merge(now, on=keys)
            total = both["kva_h"].sum()
            if total <= 0:
                continue

            ranked = both.sort_values(["kva_h_prev", "eventos_prev"], ascending=False).reset_index(drop=True)
            spearman = both["kva_h_prev"].rank().corr(both["kva_h"].rank())
            for f in fractions:
                k = max(1, int(math.ceil(f * len(ranked))))
                captured = share(ranked.head(k)["kva_h"].sum(), total)
                oracle = top_share(both["kva_h"], f)
                persistence.append({
                    "nivel": name, "anio": year, "top": f, "entidades_top": k, "captura": captured,
                    "oraculo": oracle, "azar": f, "lift": share(captured, f), "spearman": spearman,
                    "entidades_con_eventos": int((both["kva_h"] > 0).sum()),
                })

    pareto = pd.DataFrame(pareto)
    persistence = pd.DataFrame(persistence)
    report.csv("pareto.csv", pareto)
    report.csv("persistencia.csv", persistence)
    state["persistencia"] = persistence

    shown = pareto.copy()
    for column in [c for c in shown.columns if c.startswith("top_")]:
        shown[column] = shown[column].map(pct)

    mean_persistence = pd.DataFrame()
    if not persistence.empty:
        mean_persistence = persistence.groupby(["nivel", "top"]).agg(
            captura=("captura", "mean"), oraculo=("oraculo", "mean"), lift=("lift", "mean"),
            spearman=("spearman", "mean")).reset_index()
        mean_persistence["top"] = mean_persistence["top"].map(pct)
        mean_persistence["captura"] = mean_persistence["captura"].map(pct)
        mean_persistence["oraculo"] = mean_persistence["oraculo"].map(pct)

    report.section(
        "5. Dónde: concentración y persistencia",
        "Parte de los kVA·h imprevistos (variante principal) que se lleva el top de cada nivel en todo el "
        "periodo. La cabecera es el elemento aguas arriba que más veces ha cortado el trafo del CT (su salida "
        "de línea en la práctica).\n\n"
        + md_table(shown)
        + "\nPersistencia: ranking por los kVA·h de los dos años anteriores y parte de los kVA·h del año "
        f"siguiente que cae en su top (media de {', '.join(str(y) for y in data.cfg['anios_persistencia'])}). "
        "El oráculo es el máximo posible con ese tamaño de top y el azar, el propio tamaño.\n\n"
        + md_table(mean_persistence)
    )


# ---------------------------------------------------------------------------
# 6. Staged restoration
# ---------------------------------------------------------------------------

def annuity_factor(rate: float, years: int) -> float:
    if rate <= 0:
        return 1.0 / years
    return rate / (1.0 - (1.0 + rate) ** (-years))


def section_restoration(data: Data, report: Report, state: dict):
    cfg = data.cfg
    ev = data.events[(data.events["variante"] == "principal") & data.events["incidente"].notna()].copy()

    if ev.empty:
        report.section("6. Reposición escalonada", "_Sin incidencias._")
        return

    margin = pd.Timedelta(minutes=float(cfg["margen_maniobra_min"]))
    last_end = ev.groupby("incidente")["fin_ts"].transform("max")
    n_cts = ev.groupby("incidente")["ct_id"].transform("nunique")
    ev["repuesto_por_maniobra"] = (n_cts > 1) & (ev["fin_ts"] < last_end - margin)
    ev["kva_h_maniobra"] = np.where(ev["repuesto_por_maniobra"], ev["kva_h"], 0.0)

    per_year = ev.groupby(["distribuidora_id", "anio"]).agg(
        incidencias=("incidente", "nunique"), kva_h=("kva_h", "sum"), kva_h_maniobra=("kva_h_maniobra", "sum"),
        cts_maniobra=("repuesto_por_maniobra", "sum"),
    ).reset_index()
    switched = ev[ev["repuesto_por_maniobra"]]
    durations = switched.groupby(["distribuidora_id", "anio"])["dur_h"].median().rename("dur_maniobra_p50_h")
    per_year = per_year.merge(durations.reset_index(), on=["distribuidora_id", "anio"], how="left")
    inst = pd.concat([data.installed(y, ["distribuidora_id"]).assign(anio=y) for y in per_year["anio"].unique()])
    per_year = per_year.merge(inst[["distribuidora_id", "anio", "p_total_kva"]], on=["distribuidora_id", "anio"])
    per_year["parte_maniobra"] = per_year["kva_h_maniobra"] / per_year["kva_h"]
    per_year["tiepi_maniobra_h"] = per_year["kva_h_maniobra"] / per_year["p_total_kva"]
    per_year["distribuidora"] = per_year["distribuidora_id"].map(data.databases)
    report.csv("reposicion_anual.csv", per_year)
    state["reposicion_anual"] = per_year

    # By feeder: the switching time of the last three years and what a remote
    # controlled switch must avoid to pay for itself.
    recent = sorted(ev["anio"].unique())[-3:]
    feeders = ev[ev["anio"].isin(recent)].groupby(["distribuidora_id", "cabecera", "zona"]).agg(
        kva_h_maniobra=("kva_h_maniobra", "sum"), kva_h=("kva_h", "sum"), incidencias=("incidente", "nunique"),
        cts=("ct_id", "nunique"),
    ).reset_index()
    feeders["kva_h_maniobra_anual"] = feeders["kva_h_maniobra"] / len(recent)
    state["cabeceras"] = feeders
    state["anios_recientes"] = recent

    shown = per_year.copy()
    shown["parte_maniobra"] = shown["parte_maniobra"].map(pct)
    report.section(
        "6. Reposición escalonada (potencial del telemando)",
        "En una incidencia con varios CT, los que vuelven antes que los últimos (con un margen de "
        f"{fmt(float(cfg['margen_maniobra_min']), 0)} min) se repusieron con maniobras, no con la reparación. "
        "Con un elemento telemandado en el punto adecuado esa reposición baja de 3 minutos y deja de contar. Es "
        "un máximo: supone que siempre había un punto de maniobra entre el CT y la avería.\n\n"
        + md_table(shown, ["distribuidora", "anio", "incidencias", "kva_h", "kva_h_maniobra", "parte_maniobra",
                           "tiepi_maniobra_h", "dur_maniobra_p50_h"], max_rows=60)
    )


# ---------------------------------------------------------------------------
# 7. Weather: gust bands
# ---------------------------------------------------------------------------

def daily_weather(spark) -> pd.DataFrame:
    weather = read_table(spark, WEATHER_TABLE, ["celda_id", "hora", "met_racha_1h", "met_lluvia_1h"])
    cells = read_table(spark, CELL_TABLE, ["distribuidora_id", "ct_id", "celda_id"])

    if weather is None or cells is None:
        return pd.DataFrame()

    per_cell = weather.groupby("celda_id", F.to_date("hora").alias("dia")).agg(
        F.max("met_racha_1h").alias("racha_max"), F.sum("met_lluvia_1h").alias("lluvia")
    )
    dist_cells = cells.where(F.col("celda_id").isNotNull()).select("distribuidora_id", "celda_id").distinct()
    daily = per_cell.join(dist_cells, "celda_id").groupBy("distribuidora_id", "dia").agg(
        F.max("racha_max").alias("racha_max_kmh"), F.max("lluvia").alias("lluvia_max_mm"),
        F.avg("lluvia").alias("lluvia_media_mm"),
    )
    pdf = daily.toPandas()
    pdf["dia"] = pd.to_datetime(pdf["dia"])
    return pdf


def band_labels(edges: list) -> list:
    labels = []
    for lo, hi in zip(edges, edges[1:] + [None]):
        labels.append(f"{lo}-{hi}" if hi is not None else f">={lo}")
    return labels


def section_weather(data: Data, report: Report, state: dict):
    cfg = data.cfg
    days = daily_weather(data.spark)

    if days.empty:
        report.section("7. Meteorología", "_Sin meteorología en Gold._")
        return

    inc = incident_table(data, variant="principal", mask_column="computa_imprevista")
    if inc.empty:
        report.section("7. Meteorología", "_Sin incidencias._")
        return

    inc["dia"] = inc["inicio_ts"].dt.normalize()
    per_day = inc.groupby(["distribuidora_id", "dia"]).agg(
        incidencias=("incidente", "size"), kva_h=("kva_h", "sum")).reset_index()
    days = days.merge(per_day, on=["distribuidora_id", "dia"], how="left")
    days[["incidencias", "kva_h"]] = days[["incidencias", "kva_h"]].fillna(0.0)
    days = days[(days["dia"].dt.year >= int(cfg["anio_desde"])) & (days["dia"].dt.year <= int(cfg["anio_hasta"]))]
    days = days[days["dia"] <= inc["dia"].max()]
    days["distribuidora"] = days["distribuidora_id"].map(data.databases)
    report.csv("dias_meteo.csv", days)

    gust_edges = [float(x) for x in cfg["rachas_kmh"]]
    rain_edges = [float(x) for x in cfg["lluvias_mm"]]
    days["banda_racha"] = pd.cut(days["racha_max_kmh"], gust_edges + [np.inf], right=False,
                                 labels=band_labels([int(e) for e in gust_edges]))
    days["banda_lluvia"] = pd.cut(days["lluvia_max_mm"], rain_edges + [np.inf], right=False,
                                  labels=band_labels([int(e) for e in rain_edges]))
    total_kvah = days["kva_h"].sum()

    def bands(column):
        g = days.groupby(column, observed=False).agg(
            dias=("dia", "size"), dias_con_incidencia=("incidencias", lambda s: int((s > 0).sum())),
            kva_h=("kva_h", "sum"), incidencias=("incidencias", "sum"),
        ).reset_index().rename(columns={column: "banda"})
        g["p_incidencia"] = g["dias_con_incidencia"] / g["dias"].where(g["dias"] > 0)
        g["parte_dias"] = g["dias"] / g["dias"].sum()
        g["parte_kva_h"] = g["kva_h"] / total_kvah if total_kvah else np.nan
        g.insert(0, "variable", column)
        return g

    dose = pd.concat([bands("banda_racha"), bands("banda_lluvia")], ignore_index=True)
    dose["banda"] = dose["banda"].astype(str)
    report.csv("dosis_respuesta.csv", dose)

    threshold = float(cfg["umbral_racha_alerta_kmh"])
    alert = days["racha_max_kmh"] >= threshold
    alert_days = float(alert.mean())
    alert_kvah = share(days.loc[alert, "kva_h"].sum(), total_kvah)
    alert_incidents = share(days.loc[alert, "incidencias"].sum(), days["incidencias"].sum())
    base_p = float((days["incidencias"] > 0).mean())
    alert_p = float((days.loc[alert, "incidencias"] > 0).mean()) if alert.any() else float("nan")
    state["alerta"] = {"umbral": threshold, "parte_dias": alert_days, "parte_kva_h": alert_kvah,
                       "parte_incidencias": alert_incidents, "p_base": base_p, "p_alerta": alert_p}

    shown = dose.copy()
    for column in ("p_incidencia", "parte_dias", "parte_kva_h"):
        shown[column] = shown[column].map(pct)

    report.section(
        "7. Meteorología: probabilidad de incidencia por racha y lluvia",
        "Un día por distribuidora con la racha máxima y la lluvia de su peor celda. p_incidencia es la parte de "
        "días con al menos una incidencia imprevista; parte_kva_h, la parte del TIEPI imprevisto que cae en esos "
        "días.\n\n"
        + md_table(shown, ["variable", "banda", "dias", "parte_dias", "p_incidencia", "incidencias", "parte_kva_h"])
        + f"\nRegla de aviso con racha >= {fmt(threshold, 0)} km/h: marca el {pct(alert_days)} de los días, "
        f"recoge el {pct(alert_incidents)} de las incidencias y el {pct(alert_kvah)} de los kVA·h. La probabilidad "
        f"de incidencia pasa de {pct(base_p)} en un día cualquiera a {pct(alert_p)} en un día marcado.\n"
    )


# ---------------------------------------------------------------------------
# 8. Transformer load and voltage
# ---------------------------------------------------------------------------

LOAD_FAMILIES = ["carga_trafo", "potencia_activa", "potencia_reactiva", "tension"]
PHASE_PATTERN = r"(?i)(L[123]\b|FASE [123]|\b[RST]$|\b[RST]-[RST]$)"
TOTAL_PATTERN = r"(?i)(TOTAL|SUMA|^AI\.P\.(RE)?ACTIVA$|^AI\.POTENCIA (RE)?ACTIVA$|FLOAT)"
NOMINAL_KV = [0.23, 0.4, 11.0, 13.2, 15.0, 20.0, 22.0, 25.0, 30.0]


def anchor_ratings(dim: pd.DataFrame) -> pd.DataFrame:
    rated = pd.to_numeric(dim["potencia_nominal_kva"], errors="coerce")
    rated = rated.where(rated > 0, pd.to_numeric(dim["potencia_kva"], errors="coerce"))
    rated = rated.where(rated > 0, dim["kva"])
    out = dim.assign(kva_nominal=rated).dropna(subset=["anchor_id"])
    return out.groupby("anchor_id").agg(
        kva_nominal=("kva_nominal", "sum"), distribuidora_id=("distribuidora_id", "first"),
        ct_ids=("ct_id", lambda s: ",".join(sorted(set(s)))), n_trafos=("ct_id", "nunique"),
        municipio_nombre=("municipio_nombre", "first"), zona=("zona", "first"),
        grupo_red_id=("grupo_red_id", "first"), cabecera=("cabecera", "first"),
    ).reset_index()


def hourly_load(spark, ratings: pd.DataFrame):
    agg = read_table(spark, AGG_TABLE)
    tags = read_table(spark, TAG_MAP_TABLE, ["anchor_id", "familia_medida", "clase_nombre", "tiene_serie", "unidades"])

    if agg is None or ratings.empty:
        return None

    agg = agg.where((F.col("ambito") == F.lit("LOCAL")) & F.col("familia").isin(LOAD_FAMILIES))
    agg = agg.withColumnRenamed("clave_id", "anchor_id")

    if tags is not None:
        kinds = (
            tags.where(F.col("anchor_id").isNotNull() & F.col("familia_medida").isin(LOAD_FAMILIES)
                       & F.coalesce(F.col("tiene_serie"), F.lit(True)))
            .groupBy("anchor_id", F.col("familia_medida").alias("familia"))
            .agg(
                F.max((F.col("clase_nombre").rlike(TOTAL_PATTERN) & ~F.col("clase_nombre").rlike(PHASE_PATTERN))
                      .cast("int")).alias("tiene_total"),
                F.concat_ws(" | ", F.array_sort(F.collect_set("clase_nombre"))).alias("clases"),
                F.concat_ws(" | ", F.array_sort(F.collect_set(
                    F.col("unidades") if "unidades" in tags.columns else F.lit(None).cast("string")))).alias("unidades"),
            )
        )
        agg = agg.join(kinds, ["anchor_id", "familia"], "left")
    else:
        agg = (agg.withColumn("tiene_total", F.lit(None).cast("int")).withColumn("clases", F.lit(None).cast("string"))
               .withColumn("unidades", F.lit(None).cast("string")))

    mean = F.try_divide(F.col("suma"), F.col("n_muestras"))
    n_tags = F.coalesce(F.col("n_tags"), F.lit(1))
    top_tag = F.coalesce(F.col("media_tag_max"), mean) if "media_tag_max" in agg.columns else mean
    low_tag = F.coalesce(F.col("media_tag_min"), mean) if "media_tag_min" in agg.columns else mean
    power = F.when(F.col("tiene_total") == 1, top_tag).otherwise(mean * n_tags)

    diagnostics = agg.groupBy("anchor_id", "familia").agg(
        F.count(F.lit(1)).alias("n_horas"),
        F.max("tiene_total").alias("tiene_total"),
        F.first("clases", ignorenulls=True).alias("clases"),
        F.first("unidades", ignorenulls=True).alias("unidades"),
        F.avg(n_tags).alias("n_tags_medio"),
        F.percentile_approx(mean, 0.5).alias("p50_media_hora"),
        F.percentile_approx(mean, 0.99).alias("p99_media_hora"),
        F.percentile_approx(top_tag, 0.99).alias("p99_tag_max"),
    )

    hourly = agg.groupBy("anchor_id", "hora").agg(
        F.max(F.when(F.col("familia") == "carga_trafo", top_tag)).alias("carga"),
        F.max(F.when(F.col("familia") == "potencia_activa", F.abs(power))).alias("p_kw"),
        F.max(F.when(F.col("familia") == "potencia_reactiva", F.abs(power))).alias("q_kvar"),
        F.max(F.when(F.col("familia") == "tension", mean)).alias("v_media"),
        F.max(F.when(F.col("familia") == "tension", F.col("min"))).alias("v_min"),
        F.max(F.when(F.col("familia") == "tension", F.col("max"))).alias("v_max"),
        F.max(F.when(F.col("familia") == "tension", F.try_divide(top_tag, low_tag))).alias("v_ratio_tags"),
    )

    rated_pdf = ratings[["anchor_id", "kva_nominal"]].dropna()
    rated_pdf = rated_pdf.assign(anchor_id=rated_pdf["anchor_id"].astype("int64"),
                                 kva_nominal=rated_pdf["kva_nominal"].astype(float))
    rated = spark.createDataFrame(rated_pdf, "anchor_id long, kva_nominal double")
    scale = hourly.groupBy("anchor_id").agg(F.max("carga").alias("_carga_max"))
    hourly = hourly.join(F.broadcast(rated), "anchor_id", "left").join(scale, "anchor_id", "left")

    carga_pu = F.when(F.col("_carga_max") > 2, F.col("carga") / 100.0).otherwise(F.col("carga"))
    apparent = F.when(F.col("q_kvar").isNotNull(), F.sqrt(F.col("p_kw") ** 2 + F.col("q_kvar") ** 2)).otherwise(
        F.col("p_kw") / F.lit(0.95))
    load = F.when(F.col("carga").isNotNull(), carga_pu).otherwise(
        F.when(F.col("kva_nominal") > 0, F.try_divide(apparent, F.col("kva_nominal"))))

    hourly = hourly.withColumn("carga_pu", load).withColumn(
        "fuente", F.when(F.col("carga").isNotNull(), F.lit("carga_trafo")).when(
            F.col("p_kw").isNotNull(), F.lit("potencia")))
    return hourly, diagnostics


def feeder_currents(data: Data, report: Report, state: dict) -> str:
    """Peak current of every feeder bay of the substations (AGUAS_ARRIBA scope) by year, and its trend."""
    cfg = data.cfg
    agg = read_table(data.spark, AGG_TABLE)
    upstream = read_table(data.spark, UPSTREAM_TABLE, ["trafo_elemento_id", "posicion_id", "elemento_corte_nombre",
                                                        "n_cortes"])
    if agg is None or upstream is None:
        return "_Sin series de las posiciones de cabecera._\n"

    current = agg.where((F.col("ambito") == F.lit("AGUAS_ARRIBA")) & (F.col("familia") == F.lit("intensidad")))
    hourly = current.select(F.col("clave_id").alias("posicion_id"), "hora",
                            F.try_divide(F.col("suma"), F.col("n_muestras")).alias("amperios"))
    yearly = hourly.where(F.col("amperios").isNotNull() & (F.col("amperios") >= 0)).groupBy(
        "posicion_id", F.year("hora").alias("anio")).agg(
        F.count(F.lit(1)).alias("n_horas"),
        F.percentile_approx("amperios", 0.5).alias("p50_a"),
        F.percentile_approx("amperios", 0.95).alias("p95_a"),
        F.percentile_approx("amperios", 0.99).alias("p99_a"),
        F.max("amperios").alias("max_a"),
    ).toPandas()
    if yearly.empty:
        return "_Ninguna posición de cabecera con serie de intensidad._\n"

    links = upstream.toPandas()
    links = links.merge(data.dim[["trafo_elemento_id", "distribuidora_id", "ct_id", "kva", "num_abonados"]],
                        on="trafo_elemento_id", how="inner")
    names = links.sort_values("n_cortes", ascending=False).drop_duplicates("posicion_id")[
        ["posicion_id", "elemento_corte_nombre", "distribuidora_id"]]
    fed = links.drop_duplicates(["posicion_id", "ct_id"]).groupby("posicion_id").agg(
        cts_alimentados=("ct_id", "nunique"), kva_alimentado=("kva", "sum"), abonados=("num_abonados", "sum"))
    yearly = yearly.merge(names, on="posicion_id", how="left").merge(fed.reset_index(), on="posicion_id", how="left")
    yearly["distribuidora"] = yearly["distribuidora_id"].map(data.databases)
    report.csv("carga_cabeceras_anual.csv", yearly)

    # A year compares with another only when it has most of its hours (seasons), and a bay that starts near 0 A
    # was being commissioned or mapped late: its growth is not load.
    complete = yearly[(yearly["n_horas"] >= int(cfg.get("min_horas_anio_cabecera", 6500)))]
    trends = []
    for position, g in complete.groupby("posicion_id"):
        g = g[g["p99_a"] >= float(cfg.get("min_amperios_cabecera", 5.0))]
        if g.empty:
            continue
        g = g.sort_values("anio")
        last = g.iloc[-1]
        slope = float(np.polyfit(g["anio"].astype(float), g["p99_a"].astype(float), 1)[0]) if len(g) >= 3 else np.nan
        first = g.iloc[0]
        growth = (float(last["p99_a"]) / float(first["p99_a"])) ** (1.0 / max(int(last["anio"] - first["anio"]), 1)) - 1.0 \
            if first["p99_a"] and first["p99_a"] > 0 and last["anio"] > first["anio"] else np.nan
        trends.append({
            "posicion_id": position, "distribuidora": last["distribuidora"], "cabecera": last["elemento_corte_nombre"],
            "cts_alimentados": last["cts_alimentados"], "kva_alimentado": last["kva_alimentado"], "anios": len(g),
            "primer_anio": int(first["anio"]), "ultimo_anio": int(last["anio"]), "p99_primero_a": float(first["p99_a"]),
            "p99_ultimo_a": float(last["p99_a"]), "max_ultimo_a": float(last["max_a"]),
            "pendiente_p99_a_anual": slope, "crecimiento_anual": growth,
        })
    trends = pd.DataFrame(trends)
    if trends.empty:
        return "_Ninguna posición con años completos de intensidad._\n"

    trends["aumento_p99_a"] = trends["p99_ultimo_a"] - trends["p99_primero_a"]
    trends = trends.sort_values(["crecimiento_anual", "aumento_p99_a"], ascending=False)
    report.csv("carga_cabeceras_tendencia.csv", trends)
    state["carga_cabeceras"] = trends
    compared = trends[trends["anios"] >= 2]
    growing = compared[compared["crecimiento_anual"] > 0.03]
    # Three or more years of growth is demand; a jump between two years may be a line that took CTs of another.
    steady = growing[growing["anios"] >= 3]
    shown = steady.sort_values("aumento_p99_a", ascending=False).head(12)
    shown = shown.assign(crecimiento_anual=shown["crecimiento_anual"].map(pct))
    return (
        f"Intensidad de las posiciones de cabecera de las subestaciones (ámbito AGUAS_ARRIBA de agg_medida_hora): "
        f"{len(compared)} posiciones con dos o más años casi completos (al menos "
        f"{int(cfg.get('min_horas_anio_cabecera', 6500))} horas) y una punta de más de "
        f"{fmt(float(cfg.get('min_amperios_cabecera', 5.0)), 0)} A; {len(growing)} crecen más de un 3 % al año en su "
        f"p99, {len(steady)} de ellas durante tres años o más. Es la carga de la línea entera, no la de cada trafo, y "
        "sin la intensidad admisible del conductor solo podemos ordenar por crecimiento y por punta. Un salto grande "
        "entre dos años suele ser una línea que ha pasado a alimentar CT de otra (cambio de esquema); un crecimiento "
        "de tres años o más ya es demanda. Las que más amperios ganan con tres años o más:\n\n"
        + md_table(shown, ["distribuidora", "cabecera", "cts_alimentados", "kva_alimentado", "primer_anio",
                           "ultimo_anio", "p99_primero_a", "p99_ultimo_a", "aumento_p99_a", "crecimiento_anual"])
    )


def customers_per_kva(data: Data, report: Report) -> str:
    """CTs with the fewest kVA installed per customer: the ones a growing demand fills first."""
    dim = data.dim[(data.dim["kva"] > 0) & (data.dim["num_abonados"] >= 20)].copy()
    if dim.empty:
        return ""
    dim["kva_por_abonado"] = dim["kva"] / dim["num_abonados"]
    dim["distribuidora"] = dim["distribuidora_id"].map(data.databases)
    # Below this the installed power of Calser is not credible (10 kVA for 330 customers): a data error to fix
    # in the inventory, not a transformer to replace.
    floor = float(data.cfg.get("min_kva_por_abonado_plausible", 0.3))
    dim["revisar_potencia"] = dim["kva_por_abonado"] < floor
    ranking = dim.sort_values("kva_por_abonado")[
        ["distribuidora", "ct_id", "municipio_nombre", "kva", "num_abonados", "kva_por_abonado", "n_trafos_ct",
         "cabecera", "revisar_potencia"]]
    report.csv("cts_kva_por_abonado.csv", ranking)
    plausible = dim[~dim["revisar_potencia"]]
    q = plausible["kva_por_abonado"].quantile([0.1, 0.5, 0.9])
    below = int((plausible["kva_por_abonado"] < 2.0).sum())
    doubtful = ranking[ranking["revisar_potencia"]]
    return (
        f"\nkVA instalados por cliente en los CT con al menos 20 clientes (Calser): p10 {fmt(q[0.1])}, mediana "
        f"{fmt(q[0.5])}, p90 {fmt(q[0.9])}. {below} CT tienen menos de 2 kVA por cliente: con la potencia media "
        "contratada de 4,3 kW, son los primeros que se quedan cortos si crece la demanda (bomba de calor, coche "
        "eléctrico). Sin la curva de carga no podemos confirmarlo; los diez primeros:\n\n"
        + md_table(ranking[~ranking["revisar_potencia"]].head(10),
                   ["distribuidora", "ct_id", "municipio_nombre", "kva", "num_abonados", "kva_por_abonado",
                    "n_trafos_ct"])
        + f"\n{len(doubtful)} CT tienen menos de {fmt(floor, 1)} kVA por cliente, una potencia que no es creíble: "
        "hay que revisar su ficha en el inventario antes de sacar conclusiones ("
        + ", ".join(f"{r.ct_id} {r.municipio_nombre} {fmt(float(r.kva), 0)} kVA y {int(r.num_abonados)} clientes"
                    for r in doubtful.head(5).itertuples()) + ").\n"
    )


def section_load(data: Data, report: Report, state: dict):
    cfg = data.cfg
    ratings = anchor_ratings(data.dim)
    built = hourly_load(data.spark, ratings)
    feeders_text = feeder_currents(data, report, state)
    customers_text = customers_per_kva(data, report)

    if built is None:
        report.section("8. Carga de la red", "_Sin agg_medida_hora._\n\n" + feeders_text + customers_text)
        return

    hourly, diagnostics = built
    diagnostics = diagnostics.toPandas().merge(ratings, on="anchor_id", how="left")
    report.csv("carga_diagnostico.csv", diagnostics)
    hourly = hourly.withColumn("anio", F.year("hora")).localCheckpoint(eager=True)
    lo, hi = [float(x) for x in cfg["umbral_carga"]]
    yearly = hourly.where(F.col("carga_pu").isNotNull()).groupBy("anchor_id", "anio").agg(
        F.count(F.lit(1)).alias("n_horas"),
        F.first("fuente", ignorenulls=True).alias("fuente"),
        F.percentile_approx("carga_pu", 0.5).alias("p50"),
        F.percentile_approx("carga_pu", 0.95).alias("p95"),
        F.percentile_approx("carga_pu", 0.99).alias("p99"),
        F.max("carga_pu").alias("max"),
        F.sum((F.col("carga_pu") >= lo).cast("int")).alias("horas_80"),
        F.sum((F.col("carga_pu") >= hi).cast("int")).alias("horas_100"),
        F.avg(F.when(F.month("hora").isin(6, 7, 8), F.col("carga_pu"))).alias("media_verano"),
        F.avg(F.when(F.month("hora").isin(12, 1, 2), F.col("carga_pu"))).alias("media_invierno"),
    ).toPandas()

    volt = hourly.where(F.col("v_media").isNotNull() & (F.coalesce(F.col("v_ratio_tags"), F.lit(1.0)) < 1.5))
    nominal = volt.groupBy("anchor_id").agg(F.percentile_approx("v_media", 0.5).alias("v_mediana")).toPandas()

    if not nominal.empty:
        nominal["v_nominal"] = nominal["v_mediana"].map(
            lambda v: min(NOMINAL_KV, key=lambda n: abs(math.log(max(v, 1e-6) / n))) if v and v > 0 else np.nan)
        nominal = nominal[(nominal["v_mediana"] / nominal["v_nominal"]).between(0.85, 1.15)]

    voltage = pd.DataFrame()
    if not nominal.empty:
        vn = data.spark.createDataFrame(
            nominal[["anchor_id", "v_nominal"]].astype({"anchor_id": "int64", "v_nominal": float}),
            "anchor_id long, v_nominal double")
        voltage = volt.join(F.broadcast(vn), "anchor_id").groupBy("anchor_id", "anio").agg(
            F.count(F.lit(1)).alias("n_horas_tension"),
            F.sum((F.col("v_min") < 0.93 * F.col("v_nominal")).cast("int")).alias("horas_bajo_93"),
            F.sum((F.col("v_max") > 1.07 * F.col("v_nominal")).cast("int")).alias("horas_sobre_107"),
            F.first("v_nominal").alias("v_nominal"),
        ).toPandas()
        voltage = voltage.merge(ratings[["anchor_id", "distribuidora_id", "ct_ids", "municipio_nombre"]],
                                on="anchor_id", how="left")
        report.csv("tension_ct.csv", voltage)

    trafo_text = "_Ningún CT con serie de carga o potencia en el SCADA._\n"
    if not yearly.empty:
        yearly = yearly.merge(ratings, on="anchor_id", how="left")
        plausible = yearly.groupby("anchor_id")["p99"].transform("max") <= float(cfg["carga_max_plausible"])
        yearly["plausible"] = plausible | (yearly["fuente"] == "carga_trafo")
        report.csv("carga_trafo_anual.csv", yearly)
        good = yearly[yearly["plausible"] & (yearly["n_horas"] >= int(cfg["min_horas_carga"]))]
        trends = []
        for anchor, g in good.groupby("anchor_id"):
            g = g.sort_values("anio")
            last = g.iloc[-1]
            slope = float(np.polyfit(g["anio"].astype(float), g["p95"].astype(float), 1)[0]) if len(g) >= 3 \
                else float("nan")
            reach = float("nan")
            if slope > 0 and last["p95"] < 1.0:
                reach = float(last["anio"]) + (1.0 - float(last["p95"])) / slope
            trends.append({
                "anchor_id": anchor, "distribuidora_id": last["distribuidora_id"], "ct_ids": last["ct_ids"],
                "municipio_nombre": last["municipio_nombre"], "cabecera": last["cabecera"],
                "kva_nominal": last["kva_nominal"], "fuente": last["fuente"], "anios": len(g),
                "ultimo_anio": int(last["anio"]), "p95_ultimo": float(last["p95"]), "p99_ultimo": float(last["p99"]),
                "max_ultimo": float(last["max"]), "horas_80_ultimo": int(last["horas_80"]),
                "horas_100_ultimo": int(last["horas_100"]), "pendiente_p95_anual": slope, "anio_llega_100": reach,
            })
        trends = pd.DataFrame(trends)
        if not trends.empty:
            trends["prioridad"] = np.select(
                [trends["p99_ultimo"] >= hi, trends["p95_ultimo"] >= lo,
                 trends["anio_llega_100"].notna() & (trends["anio_llega_100"] <= trends["ultimo_anio"] + 5)],
                ["ya_al_limite", "cerca_del_limite", "llega_en_5_anios"], default="con_margen",
            )
            trends = trends.sort_values(["p99_ultimo"], ascending=False)
        report.csv("carga_trafo_tendencia.csv", trends)
        state["carga"] = trends
        counts = trends["prioridad"].value_counts().to_dict() if not trends.empty else {}
        trafo_text = (
            f"CT con serie de carga o potencia en el SCADA (ámbito LOCAL): {yearly['anchor_id'].nunique()}. Con serie "
            f"plausible y al menos {cfg['min_horas_carga']} horas en algún año: {good['anchor_id'].nunique()}. La carga "
            "sale de la señal de % de carga del trafo cuando existe y, si no, de la potencia aparente frente a la "
            "nominal. La pendiente es la del p95 anual y el año en que llega al 100 % la extrapola en línea recta.\n\n"
            f"Clasificación: {', '.join(f'{k}: {v}' for k, v in counts.items()) or '-'}.\n\n"
            + md_table(trends.head(15) if not trends.empty else trends,
                       ["ct_ids", "municipio_nombre", "kva_nominal", "fuente", "ultimo_anio", "p95_ultimo", "p99_ultimo",
                        "horas_100_ultimo", "pendiente_p95_anual", "anio_llega_100", "prioridad"])
        )

    volt_line = ""
    if not voltage.empty:
        worst_v = voltage.groupby("anchor_id")[["horas_bajo_93", "horas_sobre_107"]].sum()
        volt_line = (f"\nTensión: {len(worst_v)} CT con serie de tensión utilizable; "
                     f"{int((worst_v['horas_bajo_93'] > 0).sum())} con alguna hora por debajo del 93 % de la nominal y "
                     f"{int((worst_v['horas_sobre_107'] > 0).sum())} por encima del 107 % (límite de +-7 % del art. 104).\n")

    report.section("8. Carga de la red", trafo_text + volt_line + "\n" + feeders_text + customers_text)


# ---------------------------------------------------------------------------
# 9. Age of the CTs
# ---------------------------------------------------------------------------

def section_age(data: Data, report: Report, state: dict):
    cts = read_table(data.spark, SILVER_CT_TABLE, ["distribuidora_id", "id", "fecha_pes"])

    if cts is None:
        report.section("9. Edad de los CT", "_Sin l2_silver.d_ct._")
        return

    pes = cts.where(F.col("fecha_pes").isNotNull()).groupBy("distribuidora_id", F.col("id").alias("ct_id")).agg(
        F.min("fecha_pes").alias("fecha_pes")).toPandas()

    if pes.empty:
        report.section("9. Edad de los CT", "_Ningún CT con fecha de puesta en servicio._")
        return

    pes["ct_id"] = pes["ct_id"].astype(str)
    pes["anio_pes"] = pd.to_datetime(pes["fecha_pes"], errors="coerce").dt.year
    dim = data.dim.merge(pes[["distribuidora_id", "ct_id", "anio_pes"]], on=["distribuidora_id", "ct_id"], how="left")
    ev = data.events[data.events["variante"] == "principal"]
    own = ev[(ev["factor"] == "propias") & (ev["tipo_equipo_id"].astype(str).str.upper().str.startswith("CT"))]

    rows = []
    for year in data.cfg["anios_persistencia"]:
        lo, hi = pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year + 1, month=1, day=1)
        valid = dim[(dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi))
                    & (dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo))].copy()
        valid["edad"] = year - valid["anio_pes"]
        n_all = ev[ev["anio"] == year].groupby(["distribuidora_id", "ct_id"]).size().rename("interr")
        n_own = own[own["anio"] == year].groupby(["distribuidora_id", "ct_id"]).size().rename("interr_equipo_ct")
        valid = valid.merge(n_all.reset_index(), on=["distribuidora_id", "ct_id"], how="left").merge(
            n_own.reset_index(), on=["distribuidora_id", "ct_id"], how="left")
        valid[["interr", "interr_equipo_ct"]] = valid[["interr", "interr_equipo_ct"]].fillna(0)
        valid["anio"] = year
        rows.append(valid[["distribuidora_id", "ct_id", "anio", "edad", "interr", "interr_equipo_ct"]])

    table = pd.concat(rows, ignore_index=True)
    table["banda_edad"] = pd.cut(table["edad"], [-1, 10, 20, 30, 40, 200],
                                 labels=["0-10", "10-20", "20-30", "30-40", ">40"])
    table["banda_edad"] = table["banda_edad"].cat.add_categories(["sin_fecha"]).fillna("sin_fecha")
    g = table.groupby("banda_edad", observed=False).agg(
        ct_anio=("ct_id", "size"), interr_por_ct=("interr", "mean"),
        interr_equipo_ct_por_100ct=("interr_equipo_ct", lambda s: 100.0 * s.mean()),
    ).reset_index()
    report.csv("edad_ct.csv", g)
    known = table["edad"].notna().mean()

    report.section(
        "9. Edad de los CT",
        f"Edad desde la fecha de puesta en servicio de Calser (conocida en el {pct(known)} de los CT-año). "
        "interr_equipo_ct cuenta las interrupciones propias cuyo equipo es del CT.\n\n" + md_table(g)
    )


# ---------------------------------------------------------------------------
# 10. Money
# ---------------------------------------------------------------------------

def incentive_q(means: dict, weights: dict, refs: dict, cfg: dict) -> tuple:
    """(CTIEPI, Q as a share of the remuneration, marginal share per unit of CTIEPI, region) of Circular 8/2025."""
    rate = float(cfg["incentivo_pct"])
    bonus_cap, penalty_cap = float(cfg["incentivo_tope_bonificacion"]), float(cfg["incentivo_tope_penalizacion"])
    c = sum(w * (means.get(z, 0.0) - refs.get(z, np.nan)) / refs.get(z, np.nan) for z, w in weights.items())

    if c >= 0:
        q_pct = -rate * min(c / penalty_cap, 1.0)
        slope = rate / penalty_cap if c < penalty_cap else 0.0
        region = "penalizacion" if c < penalty_cap else "penalizacion_tope"
    else:
        q_pct = rate * min(c / bonus_cap, 1.0)
        slope = rate / abs(bonus_cap) if c > bonus_cap else 0.0
        region = "bonificacion" if c > bonus_cap else "bonificacion_tope"

    return c, q_pct, slope, region


def incentive_by_distributor(data: Data, state: dict) -> pd.DataFrame:
    cfg = data.cfg
    zones = state.get("tiepi_zona")

    if zones is None or zones.empty:
        return pd.DataFrame()

    years = [int(y) for y in cfg["anios_incentivo"]]
    refs = {int(k): float(v) for k, v in cfg["tiepi_sector_h"].items()}
    rows = []

    for dist, history in zones.groupby("distribuidora_id"):
        g = history[history["anio"].isin(years)]
        if g.empty:
            continue
        db = data.databases.get(dist, str(dist))
        remuneration = float(cfg["retribucion_eur"].get(data.sources.get(dist, db), float("nan")))
        last = g[g["anio"] == g["anio"].max()]
        p_total = last["p_total_kva"].sum()
        if p_total <= 0:
            continue

        weights = (last.set_index("zona")["p_total_kva"] / p_total).to_dict()
        means = g.groupby("zona")["tiepi_incentivo_h"].mean().to_dict()
        c, q_pct, slope, region = incentive_q(means, weights, refs, cfg)

        # Risk: the worst year of the history takes the place of the last year of the window.
        yearly = history.assign(_w=history["zona"].map(weights).fillna(0.0) * history["tiepi_incentivo_h"])
        per_year = yearly.groupby("anio")["_w"].sum()
        worst_year = int(per_year.idxmax()) if not per_year.empty else None
        bad = g[g["anio"] != g["anio"].max()]
        if worst_year is not None:
            bad = pd.concat([bad, history[history["anio"] == worst_year]])
        bad_means = bad.groupby("zona")["tiepi_incentivo_h"].sum().div(len(years)).to_dict()
        _, q_bad, _, _ = incentive_q(bad_means, weights, refs, cfg)

        per_hour = sum(w / refs.get(z, np.nan) for z, w in weights.items())
        tiepi_mean = sum(w * means.get(z, 0.0) for z, w in weights.items())
        rows.append({
            "distribuidora_id": dist, "distribuidora": db, "anios": f"{min(years)}-{max(years)}",
            "tiepi_incentivo_medio_h": tiepi_mean, "ctiepi": c, "q_pct": q_pct, "retribucion_eur": remuneration,
            "q_eur": q_pct * remuneration, "region": region, "p_total_kva": p_total,
            "eur_por_hora_tiepi": slope * remuneration * per_hour,
            "eur_por_minuto_tiepi": slope * remuneration * per_hour / 60.0,
            "margen_hasta_tope_eur": max(float(cfg["incentivo_pct"]) - q_pct, 0.0) * remuneration,
            "peor_anio": worst_year, "q_peor_anio_eur": q_bad * remuneration,
            "perdida_peor_anio_eur": (q_pct - q_bad) * remuneration,
            "eur_por_kvah_por_zona": {z: slope * remuneration / (p_total * refs.get(z, np.nan)) for z in weights},
        })

    return pd.DataFrame(rows)


def section_money(data: Data, report: Report, state: dict):
    cfg = data.cfg
    incentive = incentive_by_distributor(data, state)
    lines = []

    if not incentive.empty:
        shown = incentive.drop(columns=["eur_por_kvah_por_zona"]).copy()
        shown["q_pct"] = shown["q_pct"].map(lambda v: pct(v, 2))
        lines.append(
            "Incentivo de calidad de la Circular 8/2025 de la CNMC estimado con nuestro TIEPI de incentivo "
            f"(media de {incentive['anios'].iloc[0]}), la referencia del sector por tipo de zona de 2026 - 2028 y la "
            "retribución de 2023 (sin el término AT de tendencia ni el reparto ω). eur_por_minuto_tiepi es lo que "
            "vale un minuto menos de TIEPI medio al año mientras la empresa no llegue al tope.\n\n"
            + md_table(shown, ["distribuidora", "tiepi_incentivo_medio_h", "ctiepi", "q_pct", "q_eur", "region",
                               "eur_por_minuto_tiepi", "margen_hasta_tope_eur", "peor_anio",
                               "perdida_peor_anio_eur"])
            + "\nmargen_hasta_tope_eur es lo que aún se puede ganar hasta el +4 %; perdida_peor_anio_eur, lo que se "
            "perdería al año si el último año de la ventana fuera como el peor año de la serie.\n"
        )
        report.csv("economia_incentivo.csv", incentive.drop(columns=["eur_por_kvah_por_zona"]))

    summary = state.get("tiepi_resumen")
    ev = data.events[(data.events["variante"] == "todas") & data.events["computa_tiepi"]]
    if not ev.empty:
        ens = ev.groupby(["distribuidora_id", "anio"])["kva_h"].sum().reset_index()
        ens["ens_kwh"] = ens["kva_h"] * float(cfg["factor_utilizacion"]) * float(cfg["factor_potencia"])
        low, high = [float(v) for v in cfg["voll_eur_kwh"]]
        ens["coste_social_bajo_eur"] = ens["ens_kwh"] * low
        ens["coste_social_alto_eur"] = ens["ens_kwh"] * high
        ens["distribuidora"] = ens["distribuidora_id"].map(data.databases)
        report.csv("economia_ens.csv", ens)
        recent = ens[ens["anio"].isin([int(y) for y in cfg["anios_incentivo"]])]
        per_year = recent.groupby("distribuidora")[["ens_kwh", "coste_social_bajo_eur", "coste_social_alto_eur"]].mean()
        lines.append(
            f"\nEnergía no suministrada (kVA·h que cuentan para el TIEPI x {fmt(float(cfg['factor_utilizacion']))} de "
            f"utilización x {fmt(float(cfg['factor_potencia']))} de factor de potencia) y su coste para los clientes con el VoLL de "
            f"{fmt(low, 2)} EUR/kWh (CEPA para ACER, doméstico) y {fmt(high, 2)} EUR/kWh (DGPEM 2025), media anual:\n\n"
            + md_table(per_year.reset_index())
        )

    feeders = state.get("cabeceras")
    if feeders is not None and not feeders.empty and not incentive.empty:
        values = []
        per_kvah = {row["distribuidora_id"]: row["eur_por_kvah_por_zona"] for _, row in incentive.iterrows()}
        for _, row in feeders.iterrows():
            rates = per_kvah.get(row["distribuidora_id"], {})
            values.append(row["kva_h_maniobra_anual"] * rates.get(row["zona"], float("nan")))
        feeders = feeders.assign(valor_anual_max_eur=values)
        per_feeder = feeders.groupby(["distribuidora_id", "cabecera"]).agg(
            kva_h_maniobra_anual=("kva_h_maniobra_anual", "sum"), valor_anual_max_eur=("valor_anual_max_eur", "sum"),
            incidencias=("incidencias", "sum"), cts=("cts", "sum"),
        ).reset_index()
        annual_cost = float(cfg["coste_interruptor_telemando_eur"]) * annuity_factor(
            float(cfg["tasa_descuento"]), int(cfg["vida_util_anios"])) + float(cfg["om_interruptor_telemando_eur"])
        low, high = [float(v) for v in cfg["voll_eur_kwh"]]
        energy = per_feeder["kva_h_maniobra_anual"] * float(cfg["factor_utilizacion"]) * float(cfg["factor_potencia"])
        per_feeder["valor_social_anual_eur"] = energy * low
        per_feeder["coste_anual_interruptor_eur"] = annual_cost
        per_feeder["parte_necesaria"] = annual_cost / per_feeder["valor_anual_max_eur"].where(
            per_feeder["valor_anual_max_eur"] > 0)
        per_feeder["distribuidora"] = per_feeder["distribuidora_id"].map(data.databases)
        per_feeder = per_feeder.sort_values("valor_anual_max_eur", ascending=False)
        report.csv("economia_telemando_cabeceras.csv", per_feeder)
        state["prioridades"] = per_feeder
        worth = per_feeder[per_feeder["parte_necesaria"] <= 0.25]
        lines.append(
            f"\nTelemando por cabecera (años {', '.join(str(y) for y in state.get('anios_recientes', []))}): valor "
            "anual máximo de dejar bajo 3 minutos la reposición por maniobra, al precio del minuto de TIEPI de cada "
            f"distribuidora, frente al coste anual de un interruptor-seccionador telemandado ({fmt(annual_cost, 0)} "
            f"EUR al año con el VUR TI-187A, {cfg['vida_util_anios']} años y {pct(float(cfg['tasa_descuento']), 2)}). "
            "parte_necesaria es la fracción de ese máximo que el interruptor tiene que evitar para pagarse; "
            f"{len(worth)} cabeceras lo hacen con un 25 % o menos.\n\n"
            + md_table(per_feeder.head(15), ["distribuidora", "cabecera", "incidencias", "kva_h_maniobra_anual",
                                             "valor_anual_max_eur", "valor_social_anual_eur", "parte_necesaria"])
            + "\nvalor_social_anual_eur valora la misma energía no suministrada al VoLL bajo. El valor para la "
            "empresa nunca supera, sumado, el margen hasta el tope del incentivo.\n"
        )

    alert = state.get("alerta")
    if alert and not incentive.empty and summary is not None:
        lines.append(
            f"\nAviso por racha (>= {fmt(alert['umbral'], 0)} km/h): {pct(alert['parte_dias'])} de los días con el "
            f"{pct(alert['parte_kva_h'])} del TIEPI imprevisto. Con brigadas preparadas esos días, cada 10 % menos de "
            "duración de las incidencias que caen en ellos vale, al precio del minuto de TIEPI, lo que muestra "
            "economia_aviso.csv.\n"
        )
        rows = []
        recent = summary[summary["anio"].isin([int(y) for y in cfg["anios_incentivo"]])]
        for _, row in incentive.iterrows():
            mean_imp = recent.loc[recent["distribuidora"] == row["distribuidora"], "tiepi_imprevista_h"].mean()
            saved_h = mean_imp * alert["parte_kva_h"] * 0.10
            rows.append({"distribuidora": row["distribuidora"], "tiepi_imprevista_medio_h": mean_imp,
                         "horas_tiepi_por_10pct": saved_h, "eur_anuales_por_10pct": saved_h * row["eur_por_hora_tiepi"]})
        report.csv("economia_aviso.csv", pd.DataFrame(rows))
        lines.append(md_table(pd.DataFrame(rows)))

    report.section("10. Valor económico", "\n".join(lines) if lines else "_Sin datos para el cálculo._")


# ---------------------------------------------------------------------------
# 11. Public works, building and third-party damage
# ---------------------------------------------------------------------------

def ct_towns(data: Data) -> pd.DataFrame:
    """INE code of the municipality of every CT, as contexto_ct_anio matched it (code, else name)."""
    ctx = read_table(data.spark, CONTEXT_TABLE, ["distribuidora_id", "ct_id", "codigo_ine_padron"])
    if ctx is not None and "codigo_ine_padron" in ctx.columns:
        towns = ctx.dropDuplicates(["distribuidora_id", "ct_id"]).toPandas().rename(
            columns={"codigo_ine_padron": "codigo_ine"})
    else:
        towns = data.dim[["distribuidora_id", "ct_id", "municipio_id"]].copy()
        digits = towns["municipio_id"].astype(str).str.replace(r"\D", "", regex=True)
        towns["codigo_ine"] = np.where(digits.str.len() >= 6, digits.str[:5], digits.str.zfill(5))
        towns = towns.drop(columns=["municipio_id"])
    towns["ct_id"] = towns["ct_id"].astype(str)
    return towns.dropna(subset=["codigo_ine"])


def works_panel(data: Data, works: pd.DataFrame, towns: pd.DataFrame) -> pd.DataFrame:
    """
    One row per distribuidora, municipality and year with works data: CTs and
    kVA of the distribuidora there, its unplanned incidents of third parties
    (factor), of machines and works (cause read from the text) and its own
    (propias, the control), and the works and building of the municipality.
    """
    years = sorted(int(y) for y in works.loc[works["obras_en_ejecucion"].notna(), "anio"].unique())
    years = [y for y in years if int(data.cfg["anio_desde"]) <= y <= int(data.cfg["anio_hasta"])]
    if not years:
        return pd.DataFrame()

    dim = data.dim.merge(towns, on=["distribuidora_id", "ct_id"], how="inner")
    rows = []
    for year in years:
        lo, hi = pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year + 1, month=1, day=1)
        valid = dim[(dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi))
                    & (dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo))]
        g = valid.groupby(["distribuidora_id", "codigo_ine"]).agg(
            n_cts=("ct_id", "nunique"), kva=("kva", "sum"), abonados=("num_abonados", "sum")).reset_index()
        rows.append(g.assign(anio=year))
    panel = pd.concat(rows, ignore_index=True)

    ev = data.events[(data.events["variante"] == "todas") & data.events["computa_imprevista"]]
    ev = ev[ev["anio"].isin(years)].merge(towns, on=["distribuidora_id", "ct_id"], how="inner")
    outcomes = ["inc_terceros", "inc_maquinaria", "inc_propias", "kva_h_terceros", "kva_h_propias"]
    if not ev.empty:
        patterns = [(name, re.compile(regex)) for name, regex in data.cfg["causas"]]
        causes = ev.groupby("incidente")["texto"].first().fillna("").map(lambda t: classify_cause(t, patterns))
        ev = ev.assign(causa=ev["incidente"].map(causes))
        # An incident counts once in every municipality where it left some CT without supply.
        pairs = ev.groupby(["distribuidora_id", "codigo_ine", "anio", "incidente"]).agg(
            factor=("factor", "first"), causa=("causa", "first"), kva_h=("kva_h", "sum")).reset_index()
        third = pairs["factor"] == "terceros"
        own = pairs["factor"] == "propias"
        pairs = pairs.assign(
            inc_terceros=third.astype(int), inc_maquinaria=(pairs["causa"] == "terceros").astype(int),
            inc_propias=own.astype(int), kva_h_terceros=pairs["kva_h"].where(third, 0.0),
            kva_h_propias=pairs["kva_h"].where(own, 0.0))
        counts = pairs.groupby(["distribuidora_id", "codigo_ine", "anio"])[outcomes].sum().reset_index()
        panel = panel.merge(counts, on=["distribuidora_id", "codigo_ine", "anio"], how="left")
    for column in outcomes:
        panel[column] = panel[column].fillna(0.0) if column in panel else 0.0

    columns = ["obras_publicadas", "obras_publicadas_importe", "obras_en_ejecucion", "obras_meses_ejecucion",
               "obras_mov_tierras_meses", "obras_previstas_meses", "obras_previstas_mov_tierras_meses",
               "edificios_nuevos", "viviendas_nuevas", "viviendas_parque"]
    panel = panel.merge(works[["codigo_ine", "anio", *[c for c in columns if c in works]]],
                        on=["codigo_ine", "anio"], how="left")
    # A building finished in Y or Y + 1 had machines on site in Y.
    following = works[["codigo_ine", "anio", "edificios_nuevos"]].assign(anio=works["anio"] - 1).rename(
        columns={"edificios_nuevos": "_edificios_siguiente"})
    panel = panel.merge(following, on=["codigo_ine", "anio"], how="left")
    panel["edificios_en_obra"] = panel["edificios_nuevos"] + panel["_edificios_siguiente"]
    panel = panel.drop(columns=["_edificios_siguiente"])
    panel["obra_calle"] = (panel["obras_mov_tierras_meses"] > 0).astype(int)
    panel["obra_prevista"] = (panel["obras_previstas_mov_tierras_meses"] > 0).astype(int)
    panel["distribuidora"] = panel["distribuidora_id"].map(data.databases)
    return panel


def works_bands(panel: pd.DataFrame) -> pd.DataFrame:
    months = panel["obras_mov_tierras_meses"].fillna(0.0)
    band = np.select([months <= 0, months <= 6], ["0 (sin obras que abren calle)", "hasta 6 meses"], "más de 6 meses")
    g = panel.assign(banda=band).groupby("banda").agg(
        municipio_anios=("codigo_ine", "size"), ct_anios=("n_cts", "sum"), terceros=("inc_terceros", "sum"),
        maquinaria=("inc_maquinaria", "sum"), propias=("inc_propias", "sum"),
        kva_h_terceros=("kva_h_terceros", "sum")).reset_index()
    for column in ("terceros", "maquinaria", "propias"):
        g[f"{column}_por_100_ct"] = 100.0 * g[column] / g["ct_anios"].where(g["ct_anios"] > 0)
    return g


def poisson_ratios(panel: pd.DataFrame, outcome: str, flags: list, fixed: list, reps: int, seed: int) -> dict:
    """
    Rate ratio of every flag in a Poisson model of the incidents per CT-year
    (exposure: CTs of the row) with the fixed effects of the list, and its 90 %
    interval resampling municipalities (the rows of a municipality move
    together, so years of the same town do not count as independent).
    """
    from sklearn.linear_model import PoissonRegressor

    data = panel[panel["n_cts"] > 0]
    if "codigo_ine" in fixed:
        # With a fixed effect per town, a town without any incident in any year says nothing about the flag (its
        # effect goes to minus infinity): out, as in a conditional Poisson. It also keeps every fit short.
        data = data[data.groupby("codigo_ine")[outcome].transform("sum") > 0]
    data = data.reset_index(drop=True)
    if data.empty:
        return {flag: {"razon": np.nan, "ic90_bajo": np.nan, "ic90_alto": np.nan, "p_mayor_que_1": np.nan}
                for flag in flags}
    dummies = pd.get_dummies(data[fixed].astype(str), drop_first=True, dtype=float) if fixed else pd.DataFrame(
        index=data.index)
    x = pd.concat([data[flags].astype(float), dummies], axis=1).to_numpy()
    y = (data[outcome] / data["n_cts"]).to_numpy()
    w = data["n_cts"].astype(float).to_numpy()

    def fit(index):
        if y[index].sum() <= 0:
            return [np.nan] * len(flags)
        model = PoissonRegressor(alpha=1e-6, max_iter=3000)
        model.fit(x[index], y[index], sample_weight=w[index])
        return list(model.coef_[:len(flags)])

    point = fit(np.arange(len(data)))
    groups = data.groupby("codigo_ine").indices
    names = list(groups)
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(reps):
        chosen = rng.choice(len(names), size=len(names), replace=True)
        draws.append(fit(np.concatenate([groups[names[i]] for i in chosen])))
    draws = np.array(draws, dtype=float)
    out = {}
    for i, flag in enumerate(flags):
        values = draws[:, i][np.isfinite(draws[:, i])]
        low, high = (np.exp(np.percentile(values, [5, 95])) if len(values) >= 20 else (np.nan, np.nan))
        out[flag] = {"razon": float(np.exp(point[i])) if np.isfinite(point[i]) else np.nan,
                     "ic90_bajo": float(low), "ic90_alto": float(high),
                     "p_mayor_que_1": float(np.mean(values > 0)) if len(values) else np.nan}
    return out


def growth_table(data: Data, works: pd.DataFrame, towns: pd.DataFrame) -> pd.DataFrame:
    """
    Dwellings built in the last five complete years of the Catastro against the
    transformer kVA of the distribuidora in the municipality: the kVA the same
    pace would need in the next five years at today's kVA per customer.
    """
    known = works.dropna(subset=["viviendas_parque"])
    if known.empty:
        return pd.DataFrame()
    last = int(datetime.now(timezone.utc).year) - 2
    last = min(last, int(known["anio"].max()))
    stock = known.pivot_table(index="codigo_ine", columns="anio", values="viviendas_parque", aggfunc="first")
    new = known[known["anio"].between(last - 4, last)].groupby("codigo_ine")["viviendas_nuevas"].sum()
    out = pd.DataFrame({"viviendas_parque": stock.get(last), "viviendas_nuevas_5a": new})
    base = stock.get(last - 10)
    out["crec_viviendas_10a"] = out["viviendas_parque"] / base - 1 if base is not None else np.nan
    out = out.reset_index().rename(columns={"index": "codigo_ine"})

    lo, hi = pd.Timestamp(year=last + 1, month=1, day=1), pd.Timestamp(year=last + 2, month=1, day=1)
    dim = data.dim.merge(towns, on=["distribuidora_id", "ct_id"], how="inner")
    valid = dim[(dim["vigente_desde"].isna() | (dim["vigente_desde"] < hi))
                & (dim["vigente_hasta_excl"].isna() | (dim["vigente_hasta_excl"] > lo))]
    network = valid.groupby(["distribuidora_id", "codigo_ine"]).agg(
        municipio=("municipio_nombre", "first"), cts=("ct_id", "nunique"), kva=("kva", "sum"),
        abonados=("num_abonados", "sum")).reset_index()
    out = network.merge(out, on="codigo_ine", how="left")
    out["kva_por_abonado"] = out["kva"] / out["abonados"].where(out["abonados"] > 0)
    # Below half the dwellings the distribuidora is not the only one in the town: the pace is not all ours.
    out["abonados_por_vivienda"] = out["abonados"] / out["viviendas_parque"].where(out["viviendas_parque"] > 0)
    out["kva_nuevos_5a"] = out["viviendas_nuevas_5a"] * out["kva_por_abonado"]
    out["kva_nuevos_5a_pct"] = out["kva_nuevos_5a"] / out["kva"].where(out["kva"] > 0)
    out["distribuidora"] = out["distribuidora_id"].map(data.databases)
    out["anio_catastro"] = last
    return add_projection(data, out).sort_values("kva_nuevos_5a_pct", ascending=False)


def add_projection(data: Data, out: pd.DataFrame) -> pd.DataFrame:
    """Population projected by the IEEX for the group of municipalities (AME) of every municipality."""
    members = read_table(data.spark, AME_TABLE, ["codigo_ine", "ame_codigo", "ame_nombre"])
    projection = read_table(data.spark, PROJECTION_TABLE, ["ame_codigo", "anio", "poblacion"])
    if members is None or projection is None:
        return out
    members, projection = members.toPandas(), projection.toPandas()
    if projection.empty:
        return out
    first, last = int(projection["anio"].min()), int(projection["anio"].max())
    wide = projection.pivot_table(index="ame_codigo", columns="anio", values="poblacion", aggfunc="first")
    ame = pd.DataFrame({"pob_ame_inicio": wide.get(first), "pob_ame_fin": wide.get(last)}).reset_index()
    ame["crec_pob_ame"] = ame["pob_ame_fin"] / ame["pob_ame_inicio"] - 1
    ame["proyeccion"] = f"{first}-{last}"
    return out.merge(members.merge(ame, on="ame_codigo", how="left"), on="codigo_ine", how="left")


def section_works(data: Data, report: Report, state: dict):
    works_df = read_table(data.spark, WORKS_TABLE)
    if works_df is None:
        report.section("11. Obras públicas, edificación y daños de terceros",
                       "_Sin l3_gold.obras_municipio_anio: falta cargar PLACSP y el Catastro._")
        return
    works = works_df.toPandas()
    towns = ct_towns(data)
    lines = []

    panel = works_panel(data, works, towns)
    if not panel.empty:
        report.csv("obras_terceros_panel.csv", panel)
        bands = works_bands(panel)
        report.csv("obras_terceros_bandas.csv", bands)
        reps, seed = int(data.cfg.get("bootstrap_reps", 200)), int(data.cfg.get("semilla", 20261005))
        models = []
        for outcome in ("inc_terceros", "inc_maquinaria", "inc_propias"):
            for name, flags, fixed in (("entre municipios", ["obra_calle"], ["distribuidora", "anio"]),
                                       ("dentro de cada municipio", ["obra_calle"], ["codigo_ine", "anio"]),
                                       ("aviso: obra licitada antes del año", ["obra_prevista"],
                                        ["distribuidora", "anio"])):
                if panel[flags[0]].nunique() < 2:
                    continue
                result = poisson_ratios(panel, outcome, flags, fixed, reps, seed)[flags[0]]
                models.append({"incidencias": outcome, "modelo": name, "variable": flags[0], **result})
        models = pd.DataFrame(models)
        report.csv("obras_terceros_poisson.csv", models)

        with_works = panel["obra_calle"] == 1
        share_ct = share(panel.loc[with_works, "n_cts"].sum(), panel["n_cts"].sum())
        share_third = share(panel.loc[with_works, "inc_terceros"].sum(), panel["inc_terceros"].sum())
        share_machine = share(panel.loc[with_works, "inc_maquinaria"].sum(), panel["inc_maquinaria"].sum())
        shown_bands = bands.copy()
        shown_models = models.copy()
        for column in ("p_mayor_que_1",):
            if column in shown_models:
                shown_models[column] = shown_models[column].map(pct)
        lines.append(
            f"Panel de {len(panel)} filas (distribuidora, municipio y año, {panel['anio'].min()} - "
            f"{panel['anio'].max()}) con las incidencias imprevistas que dejaron sin servicio algún CT del "
            "municipio: de terceros (factor de Calser), de máquinas y obras (causa leída del texto: excavadora, "
            "camión, obra, tractor, grúa...) y propias, que sirven de control: una obra no debería cambiarlas. "
            "obra_calle vale 1 cuando el municipio tiene ese año alguna obra pública en ejecución que abre zanja o "
            "calle (PLACSP).\n\n"
            + md_table(shown_bands, ["banda", "municipio_anios", "ct_anios", "terceros", "terceros_por_100_ct",
                                     "maquinaria", "maquinaria_por_100_ct", "propias", "propias_por_100_ct"])
            + f"\nLos años con obra que abre calle son el {pct(share_ct)} de los CT-año y tienen el "
            f"{pct(share_third)} de las incidencias de terceros y el {pct(share_machine)} de las de máquinas y obras.\n"
            "\nRazón de tasas (Poisson, exposición = CT, intervalo del 90 % remuestreando municipios). Entre "
            "municipios compara pueblos con y sin obra (efectos de distribuidora y año); dentro de cada municipio "
            "compara los años con obra y sin obra del mismo pueblo, que es la comparación más limpia; el aviso usa "
            "solo obras licitadas antes de empezar el año, lo que una distribuidora sabría a tiempo.\n\n"
            + md_table(shown_models, ["incidencias", "modelo", "razon", "ic90_bajo", "ic90_alto", "p_mayor_que_1"])
        )
        state["obras"] = {"bandas": bands, "modelos": models, "parte_ct": share_ct, "parte_terceros": share_third}

    growth = growth_table(data, works, towns)
    if not growth.empty:
        report.csv("crecimiento_municipios.csv", growth)
        partial = growth["abonados_por_vivienda"] < 0.5
        watch = growth[~partial & (growth["kva_nuevos_5a_pct"] >= 0.05)]
        lines.append(
            f"\nCrecimiento y potencia (Catastro hasta {int(growth['anio_catastro'].iloc[0])}): viviendas construidas "
            "en los últimos cinco años completos y los kVA que pediría el mismo ritmo en los cinco siguientes con los "
            "kVA por cliente de hoy, frente a los kVA instalados de la distribuidora en el municipio. Los municipios "
            "donde la distribuidora tiene menos clientes que la mitad de las viviendas (otra empresa da servicio al "
            f"resto) quedan aparte ({int(partial.sum())}). {len(watch)} municipios pedirían un 5 % o más de potencia "
            "nueva.\n\n"
            + md_table(growth[~partial].head(15), ["distribuidora", "municipio", "cts", "kva", "abonados",
                                                   "viviendas_parque", "crec_viviendas_10a", "viviendas_nuevas_5a",
                                                   "kva_nuevos_5a", "kva_nuevos_5a_pct"])
        )
        if "crec_pob_ame" in growth:
            ames = growth.dropna(subset=["ame_codigo"]).groupby(["ame_codigo", "ame_nombre"]).agg(
                municipios=("codigo_ine", "nunique"), cts=("cts", "sum"), kva=("kva", "sum"),
                crec_pob_ame=("crec_pob_ame", "first")).reset_index().sort_values("crec_pob_ame")
            report.csv("crecimiento_ame.csv", ames)
            lines.append(
                f"\nProyección de población del IEEX ({growth['proyeccion'].dropna().iloc[0]}) para las agrupaciones de "
                "municipios (AME) donde están los CT. Es la única proyección oficial por debajo de la provincia; la "
                "demanda que no venga de la población vendrá de la electrificación (bomba de calor, coche eléctrico), "
                "que solo mide la curva de carga.\n\n"
                + md_table(ames.assign(crec_pob_ame=ames["crec_pob_ame"].map(pct)),
                           ["ame_nombre", "municipios", "cts", "kva", "crec_pob_ame"])
            )
        state["crecimiento"] = growth

    report.section("11. Obras públicas, edificación y daños de terceros",
                   "\n".join(lines) if lines else "_Sin datos de obras ni de edificación en los años del análisis._")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SECTIONS = [
    ("tiepi", section_tiepi),
    ("sector", section_sector),
    ("limites", section_limits),
    ("causas", section_causes),
    ("cuando", section_when),
    ("donde", section_where),
    ("reposicion", section_restoration),
    ("meteo", section_weather),
    ("carga", section_load),
    ("edad", section_age),
    ("economia", section_money),
    ("obras", section_works),
]


def save_tables(spark, run_id: str, state: dict):
    stamp = {"run_id": run_id}

    for key, table in (("tiepi_resumen", TIEPI_TABLE), ("carga", LOAD_TABLE), ("prioridades", RANKING_TABLE)):
        pdf = state.get(key)
        if pdf is None or pdf.empty:
            continue
        try:
            out = pdf.copy()
            out.insert(0, "run_id", stamp["run_id"])
            for column in out.columns:
                if out[column].dtype == object:
                    out[column] = out[column].astype(str)
            save_frame(spark, out, table, run_id)
            logger.info("Saved %s rows in %s", len(out), table)
        except Exception as error:  # noqa: BLE001
            logger.error("Table %s not saved: %s", table, error)


def main(argv=None):
    args = parse_args(argv)
    config = load_config(args.config)
    cfg = settings(config)
    spark = get_spark(config)
    wanted = set(args.secciones.split(",")) if args.secciones else None

    logger.info("Network analysis %s, years %s - %s", args.run_id, cfg["anio_desde"], cfg["anio_hasta"])
    started = datetime.now(timezone.utc)
    data = Data(spark, cfg)
    logger.info("Events: %s rows; CTs: %s", len(data.events), len(data.dim))

    report = Report(config.get("exportar_dir"), args.run_id)
    state = {}
    failed = []

    for name, function in SECTIONS:
        if wanted and name not in wanted:
            continue
        t0 = datetime.now(timezone.utc)
        try:
            function(data, report, state)
            logger.info("Section %s done in %.0f s", name, (datetime.now(timezone.utc) - t0).total_seconds())
        except Exception as error:  # noqa: BLE001
            failed.append(name)
            logger.error("Section %s failed: %s\n%s", name, error, traceback.format_exc())
            report.section(f"Error en {name}", f"`{type(error).__name__}: {error}`")

    save_tables(spark, args.run_id, state)
    if args.dashboard_dir and state.get("sector_json"):
        if os.path.isdir(args.dashboard_dir):
            rz.write_json_atomic(os.path.join(args.dashboard_dir, "sector.json"), state["sector_json"], ensure_ascii=False,
                                 indent=1)
            logger.info("sector.json left in %s", args.dashboard_dir)
        else:
            logger.warning("Dashboard folder %s not found: sector.json only in the export folder", args.dashboard_dir)
    text = report.write()
    logger.info("Summary of %s\n%s", args.run_id, text)
    logger.info("Network analysis finished in %.0f s; failed sections: %s",
                (datetime.now(timezone.utc) - started).total_seconds(), failed or "none")
    return 1 if failed and len(failed) == len([s for s in SECTIONS if not wanted or s[0] in wanted]) else 0


if __name__ == "__main__":
    sys.exit(main())
