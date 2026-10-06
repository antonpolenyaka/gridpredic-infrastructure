"""
End to end test of the Gold layer on a local Spark, without Docker.

Builds a small but complete Bronze (two distribuidoras, CTs with two
transformers, a feeder bay that cuts them, measurement series, precursor
signals, Calser interruptions of every kind), runs every Silver job and then
every Gold job, and checks the decisions of Gold one by one: which
interruptions are the target, at which exact hours the labels are 1, which
data each feature sees, the split of the dataset and its version.

The last test cuts every source at an hour and recomputes the features: the
rows up to that hour must not change. That is the leakage test of the layer.

Requirements (same versions as the stack):
    pip install pyspark==4.2.0 delta-spark==4.4.0 pytest
Run from the root of the repository:
    python -m pytest tests/03_gold -q
"""

import importlib
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_DIR = os.path.join(ROOT, "etl", "jobs", "02_silver")
GOLD_DIR = os.path.join(ROOT, "etl", "jobs", "03_gold")
CONFIG = os.path.join(ROOT, "etl", "config", "03_gold", "config_gold.json")

for path in (SILVER_DIR, GOLD_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


RUN_ID = "test_gold"

os.environ["TZ"] = "UTC"
time.tzset()


def ts(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def tm(value):
    return datetime.strptime(f"1970-01-01 {value}", "%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Session and configuration
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def workdir():
    path = tempfile.mkdtemp(prefix="gold_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="module")
def config_path(workdir):
    """
    The real configuration with a one week window, so the test runs in
    minutes. Everything else (families, horizons, rules) is the real one.
    """
    with open(CONFIG, encoding="utf-8") as file:
        config = json.load(file)

    params = config["parametros"]
    params["ventana"] = {"desde": "2026-01-05", "hasta": "2026-01-12"}
    params["dataset"].update({
        "train_hasta": "2026-01-09",
        "valid_hasta": "2026-01-11",
        "tasa_negativos_train": 0.5,
    })

    path = os.path.join(workdir, "config_gold_test.json")

    with open(path, "w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False)

    return path


@pytest.fixture(scope="module")
def spark(workdir):
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(workdir, "warehouse"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()

    for database in ("l1_bronze", "l2_silver", "l3_gold"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    load_calser(session)
    load_tedisnet(session)

    yield session

    session.stop()


def save(spark, name, rows, schema):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(f"l1_bronze.{name}")


# ---------------------------------------------------------------------------
# Calser
# ---------------------------------------------------------------------------

# DEFAULT is the open period. It starts inside the window of the test so
# that a CT that is not listed in it stops being valid (ct_vigente) at 10.01.
PERIODS = [
    ("202511", date(2025, 11, 1)),
    ("202512", date(2025, 12, 1)),
    ("202601", date(2026, 1, 1)),
    ("DEFAULT", date(2026, 1, 10)),
]

ALL_PERIODS = [p for p, _ in PERIODS]

INCIDENT_SCHEMA = (
    "INCIDENCIA_ID string, INCIDENCIA_REFERENCIA string, INCIDENCIA_DESC string, INCIDENCIA_OBS string, "
    "INCIDENCIA_FECHA_INICIO date, INCIDENCIA_HORA_INICIO timestamp, INCIDENCIA_FECHA_FIN date, "
    "INCIDENCIA_HORA_FIN timestamp, INCIDENCIA_DURACION int, INCIDENCIA_FACTOR string, "
    "INCIDENCIA_CLASIFICACION string, INCIDENCIA_TIPO_EQUIPO string, INCIDENCIA_RESOLUCION string, "
    "INCIDENCIA_EG_ID string, INCIDENCIA_OPERADOR_CC string, INCIDENCIA_OPERARIO_CAMPO string, "
    "INCIDENCIA_FECHA_ALTA date, INCIDENCIA_PERIODO_ID string, INCIDENCIA_TS string, "
    "INCIDENCIA_USUARIO_ID string"
)

INTERRUPTION_SCHEMA = (
    "INT_ID string, INT_REFERENCIA string, INT_DESC string, INT_OBS string, INT_FECHA_INICIO date, "
    "INT_HORA_INICIO timestamp, INT_FECHA_FIN date, INT_HORA_FIN timestamp, INT_DURACION int, "
    "INT_TIPO_EVENTO_ID string, INT_ELEMENTO_ID string, INT_ACOMETIDA_ID string, INT_SALIDA_ID string, "
    "INT_EG_ID string, INT_FECHA_ALTA date, INT_INCIDENCIA_ID string, INT_USUARIO_ID string, "
    "INT_ABONADO_ID string, INT_PERIODO_ID string, INT_TS string, INT_FECHA_FIN_OPTIMIZADA date, "
    "INT_HORA_FIN_OPTIMIZADA timestamp, INT_DURACION_OPTIMIZADA int"
)


def incident(inc_id, period, classification, factor, start):
    return (
        inc_id, "R" + inc_id, "Averia", None,
        date.fromisoformat(start[:10]), tm(start[11:]), date.fromisoformat(start[:10]), tm("23:00:00"),
        3600, factor, classification, "CTRAN", None, "ACTIVO", "1", None,
        date(2026, 1, 20), period, "2026-01-20 00:00:00", "1",
    )


def interruption(int_id, ct, start, end, incidencia, period="202601", alta=None):
    duration = int((ts(end) - ts(start)).total_seconds())
    return (
        int_id, int_id, "SCADA: apertura", None,
        date.fromisoformat(start[:10]), tm(start[11:]),
        date.fromisoformat(end[:10]), tm(end[11:]),
        duration, "TI_DETEC", ct, None, None, "ACTIVO",
        alta or ts(end).date(), incidencia, "1", None, period, "2026-01-20 00:00:00",
        None, None, None,
    )


def load_calser_db(spark, db, distributor, municipio, cts, incidents, interruptions):
    save(spark, f"{db}_parametros_configuracion", [
        ("DistributorId", str(distributor)),
        ("MinimalDurationImportInterruptions", "180"),
    ], "PARAM_CONFIG_CODIGO string, PARAM_CONFIG_VALOR string")

    save(spark, f"{db}_periodos", [
        (p, p, start, "2026-01-01 00:00:00") for p, start in PERIODS
    ], "PERIODO_ID string, PERIODO_NOMBRE string, PERIODO_FECHA_INICIO date, PERIODO_TS string")

    save(spark, f"{db}_municipios", [
        (municipio, "10", None, "Municipio " + municipio, None, "TZ_RUCON", "TZ_RUCON", p, "x")
        for p, _ in PERIODS
    ], "MUNICIPIO_ID string, MUNICIPIO_PROVINCIA_ID string, MUNICIPIO_COMARCA_ID string, "
       "MUNICIPIO_NOMBRE string, MUNICIPIO_NOMBRE_MIGRACION string, MUNICIPIO_TIPO_ZONA_COMUN string, "
       "MUNICIPIO_TIPO_ZONA_ESTAT string, MUNICIPIO_PERIODO_ID string, MUNICIPIO_TS string")

    # (CT_ID, installed power, administrative power, periods that list it).
    # CT_FECHA_PES and CT_FECHA_BAJA stay empty, as in the real databases.
    save(spark, f"{db}_cts", [
        (ct, instal, admin, 0, instal, municipio, None, None, "1", p, "x", 10, None)
        for ct, instal, admin, periods in cts for p in periods
    ], "CT_ID string, CT_POTENCIA_INSTAL int, CT_POTENCIA_INSTAL_ADMIN int, CT_POTENCIA_CONTRA_MT int, "
       "CT_POTENCIA_TOTAL int, CT_MUNICIPIO_ID string, CT_FECHA_PES date, CT_FECHA_BAJA date, "
       "CT_USUARIO_ALTA_ID string, CT_PERIODO_ID string, CT_TS string, CT_NUM_ABONADOS int, "
       "CT_NUMERO_SERIE_TRAFO string")

    save(spark, f"{db}_salidas", [
        ("S" + ct, "A", "Salida", "1", ct, p, "2026-01-01 00:00:00")
        for ct, _, _, periods in cts for p in periods
    ], "SALIDA_ID string, SALIDA_ABREVIATURA string, SALIDA_NOMBRE string, SALIDA_USUARIO_ALTA_ID string, "
       "SALIDA_CT_ID string, SALIDA_PERIODO_ID string, SALIDA_TS string")

    save(spark, f"{db}_tipo_generico", [
        (value, value, None, "x")
        for value in ["CL_IMPRE", "CL_PROGR", "FA_CLIEN", "FA_DISTR", "TI_DETEC", "TI_MANDO"]
    ], "TG_ID string, TG_TIPO string, TG_DESC string, TG_TS string")

    save(spark, f"{db}_incidencias", incidents, INCIDENT_SCHEMA)
    save(spark, f"{db}_interrupciones", interruptions, INTERRUPTION_SCHEMA)


def load_calser(spark):
    load_calser_db(
        spark, "calser_eosa", 3, "10001",
        cts=[
            ("06011", 400, 400, ALL_PERIODS),
            # listed from December 2025 on: created then
            ("06012", 0, 250, ["202512", "202601", "DEFAULT"]),
            # not listed in the open period: removed from the topology on 10.01.2026
            ("06021", 0, 0, ["202511", "202512", "202601"]),
            ("09999", 100, 100, ALL_PERIODS),
        ],
        incidents=[
            incident("I1", "202601", "CL_IMPRE", "FA_DISTR", "2026-01-08 10:30:00"),   # two CTs: systemic
            incident("I2", "202601", "CL_PROGR", "FA_DISTR", "2026-01-07 09:00:00"),   # planned works
            incident("I3", "202601", "CL_IMPRE", "FA_CLIEN", "2026-01-10 05:00:00"),   # customer side
            incident("I4", "202601", "CL_IMPRE", "FA_DISTR", "2026-01-09 15:20:00"),   # one CT: local
            incident("I9", "202512", "CL_IMPRE", "FA_DISTR", "2025-12-20 10:00:00"),
            incident("I10", "202511", "CL_IMPRE", "FA_DISTR", "2025-11-20 10:00:00"),
        ],
        interruptions=[
            interruption("1", "06011", "2026-01-08 10:30:00", "2026-01-08 11:30:00", "I1"),
            interruption("2", "06012", "2026-01-08 10:30:00", "2026-01-08 12:00:00", "I1"),
            # overlaps with 1: merged into 10:30 - 12:30. Loaded two days later, so
            # the final duration of the event is known from 11.01 on
            interruption("3", "06011", "2026-01-08 11:00:00", "2026-01-08 12:30:00", "I1",
                         alta=date(2026, 1, 10)),
            interruption("4", "06021", "2026-01-09 15:20:00", "2026-01-09 16:00:00", "I4"),
            interruption("5", "06021", "2026-01-07 09:00:00", "2026-01-07 13:00:00", "I2"),
            # 120 s: microcut, never a target
            interruption("6", "06021", "2026-01-07 20:00:00", "2026-01-07 20:02:00", "I4"),
            interruption("7", "06011", "2026-01-10 05:00:00", "2026-01-10 06:00:00", "I3"),
            # without incident: only the amplia variant
            interruption("8", "06011", "2026-01-10 18:00:00", "2026-01-10 19:00:00", None),
            # history: loaded in Calser on 06.01.2026, known from 07.01.2026
            interruption("9", "06011", "2025-12-20 10:00:00", "2025-12-20 11:00:00", "I9", "202512",
                         alta=date(2026, 1, 6)),
            interruption("10", "06011", "2025-11-20 10:00:00", "2025-11-20 11:00:00", "I10", "202511"),
        ],
    )

    load_calser_db(
        spark, "calser_pitarch", 2, "10002",
        cts=[("33031", 630, 630, ALL_PERIODS)],
        incidents=[incident("I5", "202601", "CL_IMPRE", "FA_DISTR", "2026-01-09 03:10:00")],
        interruptions=[interruption("1", "33031", "2026-01-09 03:10:00", "2026-01-09 04:00:00", "I5")],
    )

    spark.createDataFrame(
        [("1", "Municipio 10001", "40.2", "-5.9", "10001"), ("2", "Municipio 10002", "39.9", "-6.5", "10002")],
        "distribuidora string, municipio string, latitud string, longitud string, codigo_ine string",
    ).write.format("delta").mode("overwrite").saveAsTable("l1_bronze.reference_municipios")


# ---------------------------------------------------------------------------
# TedisNet
# ---------------------------------------------------------------------------

VALUE_SCHEMA = (
    "Id bigint, TagId int, ValueBool boolean, ValueInt int, ValueFloat double, ValueStr string, "
    "ValueEnumId int, Comments string, SourceTimestamp timestamp, UpdateTimestamp timestamp, "
    "QualityId int, QualityDetailId int, QualitySourceId int"
)

# EOSA has no samples between 02:00 and 04:00 of 10.01.2026 (history gap).
GAP = (ts("2026-01-10 02:00:00"), ts("2026-01-10 04:00:00"))


def series(tag, first, last, value_fn, start_id):
    rows = []
    t = ts(first)
    end = ts(last)
    i = start_id

    while t <= end:
        value = value_fn(t)

        if value is not None:
            rows.append((i, tag, None, None, float(value), None, None, None,
                         t - timedelta(seconds=30), t, 1, None, 1))
            i += 1

        t += timedelta(minutes=10)

    return rows


def intensity_1000(t):
    if GAP[0] <= t < GAP[1]:
        return None
    if ts("2026-01-08 09:00:00") <= t < ts("2026-01-08 10:00:00"):
        return 10 * (t.minute // 10 + 1)          # 10, 20, ..., 60: mean 35
    return 50 + t.minute // 10


def intensity_1001(t):
    if ts("2026-01-08 09:00:00") <= t < ts("2026-01-08 10:00:00"):
        return 30
    return None


def feeder_1005(t):
    if GAP[0] <= t < GAP[1]:
        return None
    return 200


def load_tedisnet(spark):
    p = "tedis_net_eosa"

    save(spark, f"{p}_system_elements", [
        (3, "/EODSLU:DIS", "EODSLU", 115, None, True),
        (2, "/EPDSLU:DIS", "EPDSLU", 115, None, True),
        (6005, "/IBERDROLA:DIS", "IBERDROLA", 115, None, True),
        (10, "/EODSLU/TORRE:STR", "TORRE", 116, 3, True),
        (20, "/EODSLU/TORRE/MONTANCHEZ:LIN", "MONTANCHEZ", 146, 10, True),
        (30, "/EODSLU/TORRE/MONTANCHEZ/0601:CT", "0601", 144, 20, True),
        (100, "/EODSLU/TORRE/MONTANCHEZ/0601/06011:TRA", "06011", 145, 30, True),
        (101, "/EODSLU/TORRE/MONTANCHEZ/0601/06012:TRA", "06012", 145, 30, True),
        (31, "/EODSLU/TORRE/MONTANCHEZ/0601/C1:SEC", "C1", 138, 30, True),
        (40, "/EODSLU/TORRE/MONTANCHEZ/0602:CT", "0602", 144, 20, True),
        (102, "/EODSLU/TORRE/MONTANCHEZ/0602/06021:TRA", "06021", 145, 40, True),
        (50, "/EODSLU/S.T.R. TORRE/L1:CEL", "L1", 124, 10, True),
        (51, "/EODSLU/S.T.R. TORRE/L1/INT1:INT", "INT1", 136, 50, True),
        (60, "/EPDSLU/CORIA/CANAVERAL:LIN", "CANAVERAL", 146, 2, True),
        (61, "/EPDSLU/CORIA/CANAVERAL/3303:CT", "3303", 144, 60, True),
        (62, "/EPDSLU/CORIA/CANAVERAL/3303/SW:SEC", "SW", 138, 61, True),
        (200, "/EPDSLU/CORIA/CANAVERAL/3303/33031:TRA", "33031", 145, 61, True),
        (300, "/IBERDROLA/FRONTERA:INT", "FRONTERA", 120, 6005, True),
    ], "Id int, Name string, ShortName string, ElementTypeId int, ParentElementId int, IsEnabled boolean")

    save(spark, f"{p}_system_nodes", [(1, 100), (2, 101), (3, 102), (4, 200)], "Id int, ElementId int")

    save(spark, f"{p}_system_electric_electrical_transformers", [
        (100, 400.0, 20.0, 0.4, 4.0, True, False),
        (101, 250.0, 20.0, 0.4, 4.0, True, False),
        (102, 160.0, 20.0, 0.4, 4.0, True, False),
        (200, 630.0, 13.2, 0.4, 4.0, True, False),
    ], "ElementId int, RatedPower double, RatedPrimaryVoltage double, RatedSecondaryVoltage double, "
       "Ucc double, IsPowerCut boolean, IsGenerator boolean")

    save(spark, f"{p}_system_devices", [(1, "RTU 0601"), (2, "RTU TORRE"), (3, "RTU 3303")],
         "Id int, Name string")

    save(spark, f"{p}_lib_tag_classes", [
        (438, "AI.INTENSIDAD", "A", None),
        (456, "AI.INT. FASE L2", "A", None),
        (305, "DI.DEFECTO DE TIERRA", None, 3),
        (549, "ES.1 POSICIÓN", None, None),
        (118, "DI.DISPARO TEMPORIZADO NEUTRO", None, 4),
        (9, "SYS.Estado Dispositivo", None, None),
        (435, "AI.TENSION", "kV", None),
        (302, "DI.PASO DE FALTA", None, None),
    ], "Id int, Name string, Units string, EventTypeId int")

    save(spark, f"{p}_system_tags", [
        (1000, "RTU 0601/I", "I", 1, 438, 31, 600, None, None),
        (1001, "RTU 0601/IL2", "IL2", 1, 456, 31, 600, None, None),
        (1002, "RTU 0601/DT", "DT", 1, 305, 31, None, None, None),
        (1003, "CC/Estados/L1/INT1 Estado", "POS", 2, 549, 51, None, None, None),
        (1004, "RTU TORRE/DISP N", "DN", 2, 118, 51, None, None, None),
        (1005, "RTU TORRE/I L1", "I", 2, 438, 50, 600, None, None),
        (1006, "RTU 0601/SYS.CONNECTION", "SYS", 1, 9, None, None, None, None),
        (1007, "RTU 3303/V", "V", 3, 435, 200, 600, None, None),
        (1008, "LINEA MONTANCHEZ/PASO FALTA", "PF", 2, 302, 20, None, None, None),
        (1009, "CC/Estados/3303/SW Estado", "POS", 3, 549, 62, None, None, None),
    ], "Id int, Name string, ShortName string, DeviceId int, TagClassId int, ElementId int, "
       "StoreInterval int, SummaryStoreInterval int, ExportCode int")

    def change(i, tag, when, quality=1, detail=None, enum=None, value_bool=None, value_float=None,
               arrived=None):
        return (i, tag, value_bool, None, value_float, None, enum, None, ts(when), ts(arrived or when),
                quality, detail, 1)

    save(spark, f"{p}_historic_tag_value_changes", [
        change(1, 1002, "2026-01-08 07:15:00", value_bool=True, enum=1),
        change(2, 1002, "2026-01-08 07:20:00", value_bool=False),
        change(3, 1004, "2026-01-08 06:30:00", value_bool=True),
        change(4, 1008, "2026-01-08 05:05:00", value_bool=True),
        change(5, 1006, "2026-01-08 06:00:00", enum=21),
        change(6, 1006, "2026-01-08 06:10:00", enum=38),
        # communication failure of a reading: goes to f_tag_quality_event
        change(7, 1000, "2026-01-08 05:30:00", quality=2, detail=5, value_float=1.0),
        # earth fault buffered by the RTU during a communication loss: field
        # time 08:20, stored by the server at 09:40, known from 10:00
        change(8, 1002, "2026-01-08 08:20:00", value_bool=True, arrived="2026-01-08 09:40:00"),
        # state read again months after it happened: never a signal
        change(9, 1002, "2025-09-01 10:00:00", value_bool=True, arrived="2026-01-08 06:40:00"),
        # changes of the feeder breaker that trigger the cuts (copied value)
        change(50, 1003, "2026-01-06 12:00:00", detail=14, enum=169),
        change(51, 1003, "2026-01-06 12:30:00", detail=14, enum=170),
        # the Off of 08.01 reached the SCADA at 11:20
        change(52, 1003, "2026-01-08 10:30:30", detail=14, enum=169, arrived="2026-01-08 11:20:00"),
        change(53, 1003, "2026-01-08 12:30:00", detail=14, enum=170),
        change(54, 1003, "2026-01-09 15:20:10", detail=14, enum=169),
        change(55, 1003, "2026-01-09 16:00:00", detail=14, enum=170),
        change(56, 1009, "2026-01-09 03:10:00", detail=14, enum=169),
        change(57, 1009, "2026-01-09 03:50:00", detail=14, enum=170),
        change(58, 1003, "2026-01-10 08:00:00", detail=14, enum=169),
        change(59, 1003, "2026-01-10 08:10:00", detail=14, enum=170),
        change(60, 1003, "2026-01-07 20:00:05", detail=14, enum=169),
        change(61, 1003, "2026-01-07 20:01:30", detail=14, enum=170),
    ], VALUE_SCHEMA)

    rows = series(1000, "2026-01-04 00:00:00", "2026-01-11 23:50:00", intensity_1000, 1)
    rows += series(1001, "2026-01-08 09:00:00", "2026-01-08 09:50:00", intensity_1001, 100000)
    rows += series(1005, "2026-01-04 00:00:00", "2026-01-11 23:50:00", feeder_1005, 200000)
    rows += series(1007, "2026-01-04 00:00:00", "2026-01-11 23:50:00", lambda t: 20, 300000)

    # one held value of the 09:00 - 10:00 bucket is two hours old
    rows = [
        r[:8] + (ts("2026-01-08 07:20:00"),) + r[9:] if (r[1] == 1000 and r[9] == ts("2026-01-08 09:50:00")) else r
        for r in rows
    ]

    save(spark, f"{p}_historic_tag_interval_values_big", rows, VALUE_SCHEMA)

    cut_schema = ("Id int, TagValueChangeId int, Timestamp timestamp, ProcessedTimestamp timestamp, "
                  "CutStateId int, IsCommand boolean, RootElementId int")

    save(spark, f"{p}_historic_electric_power_cut_events", [
        (1, 50, ts("2026-01-06 12:00:00"), None, 1, False, None),
        (2, 51, ts("2026-01-06 12:30:00"), None, 2, False, None),
        (3, 52, ts("2026-01-08 10:30:30"), None, 1, False, None),
        (4, 53, ts("2026-01-08 12:30:00"), None, 2, False, None),
        (5, 54, ts("2026-01-09 15:20:10"), None, 1, False, None),
        (6, 55, ts("2026-01-09 16:00:00"), None, 2, False, None),
        (7, 60, ts("2026-01-07 20:00:05"), None, 1, False, None),
        (8, 61, ts("2026-01-07 20:01:30"), None, 2, False, None),
        (9, None, ts("2026-01-08 04:00:00"), None, 3, False, None),      # communication error
        (10, 56, ts("2026-01-09 03:10:00"), None, 1, False, None),
        (11, 57, ts("2026-01-09 03:50:00"), None, 2, False, None),
        (12, 62, ts("2026-03-13 13:21:19"), ts("2026-01-29 11:10:09"), 2, False, None),  # RTU clock
        (13, 58, ts("2026-01-10 08:00:00"), None, 1, False, None),      # manoeuvre
        (14, 59, ts("2026-01-10 08:10:00"), None, 2, False, None),
    ], cut_schema)

    save(spark, f"{p}_historic_electric_power_cut_element_events", [
        (1, 100, 1), (1, 101, 1), (1, 102, 1),
        (2, 100, 1), (2, 101, 1), (2, 102, 1),
        (3, 100, 1), (3, 101, 1),
        (4, 100, 1), (4, 101, 1),
        (5, 102, 1), (6, 102, 1),
        (7, 102, 1), (8, 102, 1),
        (9, 100, 1),
        (10, 200, 1), (11, 200, 1),
        (12, 100, 1),
        (13, 102, 1), (14, 102, 1),
    ], "CutEventId int, ElementId int, ElectricalElementType int")

    save(spark, f"{p}_historic_command_executions", [
        (1, 1, None, None, None, None, None, None, ts("2026-01-10 07:59:00"), ts("2026-01-10 08:10:00"),
         None, ts("2026-01-10 07:59:30"), None, ts("2026-01-10 08:00:00"), None, None, None, True, 58,
         ts("2026-01-10 08:00:01")),
    ], "Id int, CommandId int, ValueEnumValueId int, TargetCommandId int, TargetValueEnumValueId int, "
       "StateEnumValueId int, UserId int, FieldUserId int, Created timestamp, ExpiresBy timestamp, "
       "SelectStart timestamp, Selected timestamp, ExecuteStart timestamp, Executed timestamp, "
       "Expired timestamp, Cancelled timestamp, SetByUser timestamp, IsFinished boolean, "
       "TagValueChangeId int, UpdateTimestamp timestamp")

    save(spark, f"{p}_historic_events", [(1, 1, None, None)],
         "Id int, TagValueChangeId int, AckUserId int, AckTimestamp timestamp")

    save(spark, f"{p}_lib_tag_class_enum_values_event_levels", [(305, 1, 100)],
         "TagClassId int, EnumValueId int, EventLevelId int")

    save(spark, f"{p}_lib_event_levels", [(1, "Normal", 1), (2, "Aviso", 2), (3, "Alarma", 3), (100, "Alarma-Rojo", 3)],
         "Id int, Name string, Level int")


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_module(name, argv):
    module = importlib.import_module(name)
    old_argv = sys.argv
    sys.argv = [f"{name}.py", *argv]

    try:
        module.main()
    finally:
        sys.argv = old_argv


def run_d_elemento():
    old_argv = sys.argv
    sys.argv = ["job_silver_d_elemento.py"]

    try:
        if "job_silver_d_elemento" in sys.modules:
            importlib.reload(sys.modules["job_silver_d_elemento"])
        else:
            importlib.import_module("job_silver_d_elemento")
    finally:
        sys.argv = old_argv


SILVER_ORDER = [
    "d_periodo", "d_municipio", "d_tipo_generico", "d_ct", "d_salida",
    "f_incidencia", "f_interrupcion", "d_tag", "f_tag_value_change", "f_evento",
    "f_command_execution", "f_corte", "f_tag_interval_value",
]

GOLD_ORDER = [
    "dim_ct", "fact_interrupciones_mt", "fact_cortes_scada", "labels_ct_hora",
    "agg_medida_hora", "features_ct_hora", "dataset_train", "dq_checks",
]


def run_gold(name, config_path, *extra):
    run_module(f"job_gold_{name}", ["--run-id", RUN_ID, "--config", config_path, *extra])


@pytest.fixture(scope="module")
def gold(spark, config_path):
    run_d_elemento()

    for name in SILVER_ORDER:
        run_module(f"job_silver_{name}", ["--run-id", RUN_ID])

    for name in GOLD_ORDER:
        run_gold(name, config_path)

    return spark


def table(spark, name):
    return spark.table(f"l3_gold.{name}")


def rows_by(df, *keys):
    return {tuple(r[k] for k in keys): r for r in df.collect()}


def feature_row(spark, ct, when):
    return table(spark, "features_ct_hora").where(
        (F.col("ct_id") == ct) & (F.col("hora") == F.lit(when).cast("timestamp"))
    ).first()


def label_row(spark, ct, when):
    return table(spark, "labels_ct_hora").where(
        (F.col("ct_id") == ct) & (F.col("hora") == F.lit(when).cast("timestamp"))
    ).first()


# ---------------------------------------------------------------------------
# Dimension and maps
# ---------------------------------------------------------------------------

def test_dim_ct(gold):
    dim = rows_by(table(gold, "dim_ct"), "distribuidora_id", "ct_id")

    assert {k for k, r in dim.items() if r["en_estudio"]} == {
        (3, "06011"), (3, "06012"), (3, "06021"), (2, "33031"),
    }
    assert dim[(3, "09999")]["motivo_exclusion"] == "SIN_TRAFO_SCADA"

    ct = dim[(3, "06011")]
    assert ct["anchor_id"] == 30 and ct["grupo_red_id"] == 20 and ct["n_trafos_ct"] == 2
    assert ct["tiene_medidas"] and ct["familias_medida_local"] == "intensidad"
    assert ct["n_posiciones_aguas_arriba"] == 1
    assert ct["latitud"] == pytest.approx(40.2)

    # power: installed, administrative, rated power of the SCADA
    assert (ct["potencia_kva"], ct["potencia_fuente"]) == (400, "CALSER_INSTAL")
    assert (dim[(3, "06012")]["potencia_kva"], dim[(3, "06012")]["potencia_fuente"]) == (250, "CALSER_ADMIN")
    assert (dim[(3, "06021")]["potencia_kva"], dim[(3, "06021")]["potencia_fuente"]) == (160, "SCADA_NOMINAL")
    assert dim[(3, "06021")]["potencia_imputada"]
    assert not dim[(3, "06021")]["tiene_medidas"]

    # validity read from the periods that list the CT (the dates of Calser
    # are empty): from the first period, until the first one without it
    assert (ct["vigente_desde"], ct["vigente_hasta_excl"], ct["n_periodos"]) == (date(2025, 11, 1), None, 4)
    assert dim[(3, "06012")]["vigente_desde"] == date(2025, 12, 1)
    assert dim[(3, "06012")]["vigente_hasta_excl"] is None
    assert dim[(3, "06021")]["vigente_desde"] == date(2025, 11, 1)
    assert dim[(3, "06021")]["vigente_hasta_excl"] == date(2026, 1, 10)
    assert dim[(2, "33031")]["vigente_hasta_excl"] is None


def test_rebuild_with_a_month_range_is_refused(monkeypatch):
    import gold_common

    monkeypatch.setattr(sys, "argv", ["job.py", "--rebuild", "--desde", "2026-01"])

    with pytest.raises(SystemExit):
        gold_common.parse_args()


def test_maps(gold):
    tags = rows_by(table(gold, "map_tag_ct"), "tag_id")

    assert tags[(1000,)]["anchor_id"] == 30 and tags[(1000,)]["familia_medida"] == "intensidad"
    assert tags[(1002,)]["familia_evento"] == "defecto_tierra"
    assert tags[(1004,)]["familia_evento"] == "defecto_tierra"    # DISPARO TEMPORIZADO NEUTRO
    # a tag without element reaches the CT through its device
    assert (tags[(1006,)]["origen_mapeo"], tags[(1006,)]["anchor_id"]) == ("DISPOSITIVO", 30)
    # a signal of the line itself belongs to the network group
    assert (tags[(1008,)]["origen_mapeo"], tags[(1008,)]["grupo_red_id"]) == ("GRUPO", 20)

    upstream = rows_by(table(gold, "map_aguas_arriba"), "trafo_elemento_id", "elemento_corte_id")
    assert upstream[(100, 51)]["posicion_id"] == 50
    assert upstream[(200, 62)]["posicion_id"] == 61
    assert upstream[(102, 51)]["n_cortes"] == 4                   # 06.01, 07.01, 09.01 and the manoeuvre
    # known from the arrival of the first cut of each pair
    assert upstream[(100, 51)]["primer_conocido_ts"] == ts("2026-01-06 12:00:00")
    assert upstream[(200, 62)]["primer_conocido_ts"] == ts("2026-01-09 03:10:00")


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

def test_fact_interrupciones(gold):
    fact = table(gold, "fact_interrupciones_mt")
    principal = rows_by(fact.where("variante = 'principal'"), "distribuidora_id", "ct_id", "inicio_ts")

    merged = principal[(3, "06011", ts("2026-01-08 10:30:00"))]
    assert merged["fin_ts"] == ts("2026-01-08 12:30:00") and merged["n_registros"] == 2
    assert merged["es_sistemico"] and merged["n_cts_incidencia"] == 2

    local = principal[(3, "06021", ts("2026-01-09 15:20:00"))]
    assert not local["es_sistemico"]

    starts = {(k[1], k[2]) for k in principal}
    assert ("06021", ts("2026-01-07 09:00:00")) not in starts      # planned
    assert ("06021", ts("2026-01-07 20:00:00")) not in starts      # microcut
    assert ("06011", ts("2026-01-10 05:00:00")) not in starts      # customer side
    assert ("06011", ts("2026-01-10 18:00:00")) not in starts      # without incident
    assert (("33031", ts("2026-01-09 03:10:00"))) in starts

    history = principal[(3, "06011", ts("2025-12-20 10:00:00"))]
    assert history["conocido_ts"] == ts("2026-01-07 00:00:00")

    # first record loaded on 08.01, the one that extends it on 10.01
    assert merged["conocido_ts"] == ts("2026-01-09 00:00:00")
    assert merged["conocido_fin_ts"] == ts("2026-01-11 00:00:00")

    amplia = {(r["ct_id"], r["inicio_ts"]) for r in fact.where("variante = 'amplia'").collect()}
    assert ("06011", ts("2026-01-10 18:00:00")) in amplia
    assert ("06011", ts("2026-01-10 05:00:00")) not in amplia

    todas = {(r["ct_id"], r["inicio_ts"]) for r in fact.where("variante = 'todas'").collect()}
    assert ("06021", ts("2026-01-07 09:00:00")) in todas


def test_fact_cortes_scada(gold):
    cuts = rows_by(table(gold, "fact_cortes_scada"), "ct_id", "inicio_ts")

    assert cuts[("06011", ts("2026-01-08 10:30:30"))]["fin_ts"] == ts("2026-01-08 12:30:00")
    assert cuts[("06011", ts("2026-01-08 10:30:30"))]["es_corte_etiqueta"]
    assert cuts[("06011", ts("2026-01-08 10:30:30"))]["inicio_conocido_ts"] == ts("2026-01-08 11:20:00")
    assert cuts[("06011", ts("2026-01-08 10:30:30"))]["fin_conocido_ts"] == ts("2026-01-08 12:30:00")

    micro = cuts[("06021", ts("2026-01-07 20:00:05"))]
    assert micro["duracion_s"] == 85 and micro["es_microcorte"] and not micro["es_corte_etiqueta"]

    manoeuvre = cuts[("06021", ts("2026-01-10 08:00:00"))]
    assert manoeuvre["es_maniobra"] and not manoeuvre["es_corte_etiqueta"]

    # the On stamped two months after the backup is not paired
    assert all(r["fin_ts"] is None or r["fin_ts"] < ts("2026-02-01 00:00:00")
               for r in table(gold, "fact_cortes_scada").collect())


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

def test_labels_exact_hours(gold):
    labels = table(gold, "labels_ct_hora")
    positives = {
        (r["ct_id"], r["hora"]) for r in labels.where("y_1_3h = 1").collect()
    }

    # start 10:30 -> predicted at 08:00 and 09:00 (1 to 3 hours ahead)
    assert positives == {
        ("06011", ts("2026-01-08 08:00:00")), ("06011", ts("2026-01-08 09:00:00")),
        ("06012", ts("2026-01-08 08:00:00")), ("06012", ts("2026-01-08 09:00:00")),
        ("06021", ts("2026-01-09 13:00:00")), ("06021", ts("2026-01-09 14:00:00")),
        ("33031", ts("2026-01-09 01:00:00")), ("33031", ts("2026-01-09 02:00:00")),
    }

    assert label_row(gold, "06011", "2026-01-08 10:00:00")["y_0_1h"] == 1
    assert label_row(gold, "06011", "2026-01-08 08:00:00")["y_0_3h"] == 1
    assert label_row(gold, "06011", "2026-01-08 07:00:00")["y_0_3h"] == 0      # 10:30 is 3.5 h away
    assert label_row(gold, "06011", "2026-01-08 07:00:00")["y_0_6h"] == 1
    assert label_row(gold, "06011", "2026-01-08 07:00:00")["y_1_3h"] == 0

    # systemic event: not in the local label; the one CT incident is
    assert label_row(gold, "06012", "2026-01-08 08:00:00")["y_1_3h_local"] == 0
    assert label_row(gold, "06021", "2026-01-09 13:00:00")["y_1_3h_local"] == 1

    assert label_row(gold, "06011", "2026-01-10 15:00:00")["y_1_3h_amplia"] == 1
    assert label_row(gold, "06011", "2026-01-06 09:00:00")["y_1_3h_scada"] == 1

    assert label_row(gold, "06011", "2026-01-08 08:00:00")["horas_hasta_proximo_evento"] == pytest.approx(2.5)


def test_labels_ct_vigente(gold):
    labels = table(gold, "labels_ct_hora")

    # 06021 leaves the topology on 10.01: its rows stay in the grid, flagged
    assert label_row(gold, "06021", "2026-01-09 23:00:00")["ct_vigente"] == 1
    assert label_row(gold, "06021", "2026-01-10 00:00:00")["ct_vigente"] == 0
    assert label_row(gold, "06021", "2026-01-11 12:00:00")["ct_vigente"] == 0

    not_valid = {r["ct_id"] for r in labels.where("ct_vigente = 0").select("ct_id").distinct().collect()}
    assert not_valid == {"06021"}

    # no interruption of a CT falls in an hour where Calser did not list it
    assert labels.where("ct_vigente = 0 AND y_1_3h = 1").count() == 0


def test_labels_outage_hours(gold):
    outage = {
        (r["ct_id"], r["hora"]) for r in table(gold, "labels_ct_hora").where("en_corte = 1").collect()
    }

    assert {("06011", ts("2026-01-08 11:00:00")), ("06011", ts("2026-01-08 12:00:00"))} <= outage
    # planned works 09:00 - 13:00: not a label, but the CT has no supply
    assert {("06021", ts(f"2026-01-07 {h:02d}:00:00")) for h in (9, 10, 11, 12)} <= outage
    assert ("06021", ts("2026-01-07 13:00:00")) not in outage


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------

def test_measurement_features(gold):
    # bucket [09:00, 10:00): tag 1000 mean 35 (6 samples), tag 1001 mean 30
    row = feature_row(gold, "06011", "2026-01-08 10:00:00")

    assert row["med_intensidad_n_muestras"] == 12
    assert row["med_intensidad_media"] == pytest.approx(32.5)
    assert row["med_intensidad_max"] == 60 and row["med_intensidad_min"] == 10
    assert row["med_intensidad_desequilibrio"] == pytest.approx(5 / 32.5)
    assert row["med_intensidad_frac_rancio"] == pytest.approx(1 / 12)

    # same CT element, same measurements
    assert feature_row(gold, "06012", "2026-01-08 10:00:00")["med_intensidad_media"] == pytest.approx(32.5)

    # the CT without RTU gets the feeder current of the bay that cuts it
    other = feature_row(gold, "06021", "2026-01-08 10:00:00")
    assert other["med_intensidad_media"] is None
    assert other["medaa_intensidad_media"] == pytest.approx(200)

    assert feature_row(gold, "33031", "2026-01-08 10:00:00")["med_tension_media"] == pytest.approx(20)

    # 33031 is cut for the first time on 09.01 at 03:10: before that nobody
    # knew which bay feeds it
    before = feature_row(gold, "33031", "2026-01-08 10:00:00")
    after = feature_row(gold, "33031", "2026-01-09 06:00:00")
    assert before["medaa_tension_media"] is None and before["aa_n_posiciones"] == 0
    assert after["medaa_tension_media"] == pytest.approx(20) and after["aa_n_posiciones"] == 1

    assert feature_row(gold, "06011", "2026-01-06 12:00:00")["aa_n_posiciones"] == 0
    assert feature_row(gold, "06011", "2026-01-06 13:00:00")["aa_n_posiciones"] == 1


def test_event_features(gold):
    at8 = feature_row(gold, "06011", "2026-01-08 08:00:00")
    at9 = feature_row(gold, "06011", "2026-01-08 09:00:00")

    assert at8["ev_defecto_tierra_1h"] == 2 and at9["ev_defecto_tierra_1h"] == 0
    # the change read again months late does not count
    assert at9["ev_defecto_tierra_6h"] == 2
    assert feature_row(gold, "06011", "2026-01-08 07:00:00")["ev_defecto_tierra_1h"] == 0
    # the buffered change counts when it arrived, not at its field time
    assert feature_row(gold, "06011", "2026-01-08 10:00:00")["ev_defecto_tierra_1h"] == 1
    assert at8["ev_alarmas_1h"] == 1
    assert at8["evaa_defecto_tierra_6h"] == 1         # trip of the feeder that feeds the CT
    assert at8["calidad_fallo_comm_24h"] == 1
    assert at8["corte_error_comm_24h"] == 1
    assert feature_row(gold, "06011", "2026-01-08 07:00:00")["ev_comunicaciones_1h"] == 2

    for ct in ("06011", "06012", "06021"):
        assert feature_row(gold, ct, "2026-01-08 08:00:00")["evred_paso_falta_6h"] == 1

    assert feature_row(gold, "33031", "2026-01-08 08:00:00")["evred_paso_falta_6h"] == 0


def test_scada_and_history_features(gold):
    assert feature_row(gold, "06011", "2026-01-07 00:00:00")["scada_cortes_24h"] == 1
    assert feature_row(gold, "06021", "2026-01-08 00:00:00")["scada_microcortes_24h"] == 1
    assert feature_row(gold, "06011", "2026-01-08 00:00:00")["hist_scada_cortes_30d"] == 1

    # the Off of 10:30:30 arrived at 11:20
    assert feature_row(gold, "06011", "2026-01-08 11:00:00")["scada_cortes_24h"] == 0
    assert feature_row(gold, "06011", "2026-01-08 12:00:00")["scada_cortes_24h"] == 1

    # the December interruption was loaded on 06.01.2026: known from 07.01
    assert feature_row(gold, "06011", "2026-01-06 12:00:00")["hist_interr_30d"] == 0
    assert feature_row(gold, "06011", "2026-01-07 12:00:00")["hist_interr_30d"] == 1
    assert feature_row(gold, "06011", "2026-01-07 12:00:00")["hist_interr_90d"] == 2

    # the merged event of 08.01 counts from 09.01, its duration (2 h) only
    # from 11.01, when its last record is loaded
    assert feature_row(gold, "06011", "2026-01-09 12:00:00")["hist_interr_30d"] == 2
    assert feature_row(gold, "06011", "2026-01-10 12:00:00")["hist_duracion_media_365d"] == pytest.approx(3600)
    assert feature_row(gold, "06011", "2026-01-11 01:00:00")["hist_duracion_media_365d"] == pytest.approx(4800)


def test_calendar_static_and_activity(gold):
    assert feature_row(gold, "06011", "2026-01-06 12:00:00")["es_festivo"] == 1      # Epifania
    assert feature_row(gold, "06011", "2026-01-05 12:00:00")["es_vispera_festivo"] == 1
    assert feature_row(gold, "06012", "2026-01-05 12:00:00")["ct_potencia_kva"] == 250

    # history gap of EOSA between 02:00 and 04:00
    assert feature_row(gold, "06011", "2026-01-10 03:00:00")["scada_activo"] == 0
    assert feature_row(gold, "06011", "2026-01-10 05:00:00")["scada_activo"] == 1
    assert feature_row(gold, "33031", "2026-01-10 03:00:00")["scada_activo"] == 1


def test_feature_metadata(gold):
    features = table(gold, "features_ct_hora").columns
    metadata = {r["feature"]: r for r in table(gold, "feature_metadata").collect()}

    assert set(metadata) <= set(features)
    assert not [name for name in metadata if name.startswith("y_") or name in ("en_corte", "ts")]
    assert "pct_nulos_train" in table(gold, "feature_metadata").columns


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

def test_dataset(gold):
    dataset = table(gold, "dataset_train")

    assert dataset.where("en_corte = 1 OR scada_activo = 0 OR ct_vigente = 0").count() == 0
    # the hours of 06021 after it was removed from Calser are out
    assert dataset.where("ct_id = '06021' AND hora >= TIMESTAMP'2026-01-10 00:00:00'").count() == 0
    assert dataset.where("ct_id = '06021' AND hora < TIMESTAMP'2026-01-10 00:00:00'").count() > 0

    splits = {r["split"]: r for r in dataset.groupBy("split").agg(
        F.min("hora").alias("desde"), F.max("hora").alias("hasta"), F.sum("y_1_3h").alias("pos"),
    ).collect()}

    # the label of a train row must end by the boundary: 6 h of purge
    assert splits["train"]["hasta"] == ts("2026-01-08 18:00:00")
    assert splits["valid"]["desde"] >= ts("2026-01-09 00:00:00")
    assert splits["valid"]["hasta"] == ts("2026-01-10 18:00:00")
    assert splits["test"]["desde"] >= ts("2026-01-11 00:00:00")
    assert splits["train"]["pos"] == 4 and splits["valid"]["pos"] == 4

    purge = {r["hora"] for r in dataset.where("split = 'purga'").select("hora").distinct().collect()}
    assert purge == {ts(f"2026-01-08 {h}:00:00") for h in range(19, 24)} | \
        {ts(f"2026-01-10 {h}:00:00") for h in range(19, 24)}

    train = dataset.where("split = 'train'")
    assert train.where("y_1_3h = 1 AND NOT en_muestra_train").count() == 0
    assert dataset.where("split <> 'train' AND en_muestra_train").count() == 0
    assert "ct_n_posiciones_aa" not in dataset.columns

    versions = table(gold, "dataset_versions").collect()
    assert len(versions) == 1
    assert json.loads(versions[0]["filas_json"])["train"]["positivos"] == 4
    assert versions[0]["dataset_version"] == dataset.select("dataset_version").distinct().first()[0]


def test_dq_checks(gold):
    metrics = gold.table("l3_gold.dq_metrics").where(f"run_id = '{RUN_ID}'")

    offset = metrics.where("metrica = 'desfase_mediana_min'").first()
    assert offset is not None and abs(offset["valor"]) <= 1

    leaked = metrics.where("metrica = 'columnas_prohibidas_como_feature'").first()
    assert leaked["valor"] == 0

    validity = metrics.where("metrica = 'positivos_en_horas_no_vigentes:y_1_3h'").first()
    assert validity is not None and validity["valor"] == 0 and validity["estado"] == "OK"

    hours = metrics.where("metrica = 'horas_ct_no_vigente'").first()
    assert hours["valor"] == 2 * 24      # 06021 from 10.01 00:00 to the end of the window (12.01)


# ---------------------------------------------------------------------------
# Leakage: the past does not change when the future is removed
# ---------------------------------------------------------------------------

CUT = "2026-01-08 09:00:00"


def test_no_leakage(gold, config_path):
    keys = ["distribuidora_id", "ct_id", "hora"]
    metadata = [r["feature"] for r in table(gold, "feature_metadata").collect()]

    def snapshot():
        return {
            tuple(r[k] for k in keys): r
            for r in table(gold, "features_ct_hora")
            .where((F.col("hora") >= F.lit("2026-01-08 00:00:00").cast("timestamp"))
                   & (F.col("hora") <= F.lit(CUT).cast("timestamp")))
            .select(*keys, *metadata).collect()
        }

    before = snapshot()

    # Remove everything that was known at or after the cut: the later of the
    # field time and the arrival at the server (for a sample, the field time
    # of its value).
    for name, arrival in (("f_tag_interval_value", "ts_origen"), ("f_tag_value_change", "ts_actualizacion"),
                          ("f_evento", "ts_actualizacion"), ("f_tag_quality_event", "ts_actualizacion"),
                          ("f_corte_elemento", "ts_actualizacion"), ("f_corte_evento", "ts_actualizacion")):
        source = gold.table(f"l2_silver.{name}")
        known = F.greatest(F.col("ts"), F.coalesce(F.col(arrival), F.col("ts")))
        df = source.where(known < F.lit(CUT).cast("timestamp")).localCheckpoint(eager=True)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if name == "f_tag_interval_value":
            writer = writer.partitionBy("fecha_mes")
        writer.saveAsTable(f"l2_silver.{name}")

    # The map of the upstream bays is learnt from the cuts: rebuilt too.
    for name in ("dim_ct", "fact_cortes_scada", "agg_medida_hora"):
        run_gold(name, config_path)

    run_gold("features_ct_hora", config_path, "--desde", "2026-01", "--hasta", "2026-01")

    after = snapshot()

    assert before.keys() == after.keys()

    differences = []

    for key, row in before.items():
        other = after[key]
        for name in metadata:
            a, b = row[name], other[name]
            if isinstance(a, float) and isinstance(b, float):
                if abs(a - b) > 1e-9:
                    differences.append((key, name, a, b))
            elif a != b:
                differences.append((key, name, a, b))

    assert not differences, differences[:10]
