"""
End to end smoke test of the Silver layer on a local Spark, without Docker.

Creates small Bronze tables with the same names and columns the Bronze jobs
write, with the data problems found in the real databases planted on
purpose (duplicates, overlaps, bad quality, orphans, repeated power cut
events...), runs every Silver job and checks the result.

Requirements (same versions as the stack):
    pip install pyspark==4.2.0 delta-spark==4.4.0 pytest

Run from the root of the repository:
    python -m pytest tests/02_silver -q
The first run downloads the Delta jars from Maven Central.
"""

import importlib
import os
import shutil
import sys
import tempfile
import time
from datetime import date, datetime

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
JOBS_DIR = os.path.join(ROOT, "etl", "jobs", "02_silver")
sys.path.insert(0, JOBS_DIR)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


RUN_ID = "test_run"

# Python turns naive datetimes into timestamps with the local time zone of
# the process. Pinning it to UTC, like the Spark session, keeps the month of
# every fixture where it is written.
os.environ["TZ"] = "UTC"
time.tzset()


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    warehouse = tempfile.mkdtemp(prefix="silver_wh_")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("silver-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()

    session.sql("CREATE DATABASE IF NOT EXISTS l1_bronze")
    session.sql("CREATE DATABASE IF NOT EXISTS l2_silver")

    load_bronze(session)

    yield session

    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def save(spark, name, rows, schema):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite") \
        .saveAsTable(f"l1_bronze.{name}")


def ts(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def tm(value):
    # SQL Server TIME arrives through JDBC as a timestamp on 1970-01-01.
    return datetime.strptime(f"1970-01-01 {value}", "%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Bronze fixtures
# ---------------------------------------------------------------------------

CALSER = {
    "calser_eosa": 3,
    "calser_pitarch": 2,
}


def load_calser(spark):
    for db, distributor in CALSER.items():
        save(spark, f"{db}_parametros_configuracion", [
            ("DistributorId", str(distributor)),
            ("MinimalDurationImportInterruptions", "180"),
        ], "PARAM_CONFIG_CODIGO string, PARAM_CONFIG_VALOR string")

        save(spark, f"{db}_periodos", [
            ("202601", "Enero 2026", date(2026, 1, 1), "2026-01-01 00:00:00"),
            ("202602", "Febrero 2026", date(2026, 2, 1), "2026-02-01 00:00:00"),
            ("DEFAULT", "Abierto", date(2026, 3, 1), "2026-03-01 00:00:00"),
        ], "PERIODO_ID string, PERIODO_NOMBRE string, PERIODO_FECHA_INICIO date, PERIODO_TS string")

        save(spark, f"{db}_municipios", [
            ("10001", "10", None, "Jerte ", "Jerte", "TZ_RUCON", "TZ_RUCON", p, "x")
            for p in ["202601", "202602", "DEFAULT"]
        ], "MUNICIPIO_ID string, MUNICIPIO_PROVINCIA_ID string, MUNICIPIO_COMARCA_ID string, "
           "MUNICIPIO_NOMBRE string, MUNICIPIO_NOMBRE_MIGRACION string, MUNICIPIO_TIPO_ZONA_COMUN string, "
           "MUNICIPIO_TIPO_ZONA_ESTAT string, MUNICIPIO_PERIODO_ID string, MUNICIPIO_TS string")

        cts = []
        for p in ["202601", "202602", "DEFAULT"]:
            cts.append(("01011", 250, 250, 0, 250, "10001", None, None, "1", p, "x", 12, None))
            cts.append(("01012", 0, 160, 0, 160, "10001", None, None, "1", p, "x", 5, None))
        # CT pointing to a municipality that does not exist
        cts.append(("09999", 100, 100, 0, 100, "99999", None, None, "1", "202601", "x", 1, None))

        save(spark, f"{db}_cts", cts,
             "CT_ID string, CT_POTENCIA_INSTAL int, CT_POTENCIA_INSTAL_ADMIN int, "
             "CT_POTENCIA_CONTRA_MT int, CT_POTENCIA_TOTAL int, CT_MUNICIPIO_ID string, "
             "CT_FECHA_PES date, CT_FECHA_BAJA date, CT_USUARIO_ALTA_ID string, CT_PERIODO_ID string, "
             "CT_TS string, CT_NUM_ABONADOS int, CT_NUMERO_SERIE_TRAFO string")

        save(spark, f"{db}_salidas", [
            ("S1", "A1", " Salida 1 ", "1", "01011", "202601", "2026-01-01 00:00:00"),
            ("S2", "A2", "", "1", "05555", "202601", "2026-01-01 00:00:00"),
        ], "SALIDA_ID string, SALIDA_ABREVIATURA string, SALIDA_NOMBRE string, SALIDA_USUARIO_ALTA_ID string, "
           "SALIDA_CT_ID string, SALIDA_PERIODO_ID string, SALIDA_TS string")

        save(spark, f"{db}_tipo_generico", [
            (value, value, None, "x")
            for value in ["CL_IMPRE", "CL_PROGR", "FA_CLIEN", "FA_DISTR", "TI_DETEC", "TI_MANDO"]
        ], "TG_ID string, TG_TIPO string, TG_DESC string, TG_TS string")

        save(spark, f"{db}_incidencias", [
            ("I1", "R1", "Avería", None, date(2026, 1, 10), tm("10:00:00"), date(2026, 1, 10), tm("11:00:00"),
             3600, "FA_DISTR", "CL_IMPRE", "CTRAN", None, "ACTIVO", "1", None, date(2026, 1, 11), "202601",
             "2026-01-11 08:00:00", "1"),
        ], "INCIDENCIA_ID string, INCIDENCIA_REFERENCIA string, INCIDENCIA_DESC string, INCIDENCIA_OBS string, "
           "INCIDENCIA_FECHA_INICIO date, INCIDENCIA_HORA_INICIO timestamp, INCIDENCIA_FECHA_FIN date, "
           "INCIDENCIA_HORA_FIN timestamp, INCIDENCIA_DURACION int, INCIDENCIA_FACTOR string, "
           "INCIDENCIA_CLASIFICACION string, INCIDENCIA_TIPO_EQUIPO string, INCIDENCIA_RESOLUCION string, "
           "INCIDENCIA_EG_ID string, INCIDENCIA_OPERADOR_CC string, INCIDENCIA_OPERARIO_CAMPO string, "
           "INCIDENCIA_FECHA_ALTA date, INCIDENCIA_PERIODO_ID string, INCIDENCIA_TS string, "
           "INCIDENCIA_USUARIO_ID string")

        def interruption(int_id, element, start, end, duration, desc="Corte", incidencia="I1",
                         salida=None, period="202601"):
            return (
                int_id, int_id, desc, None,
                date.fromisoformat(start[:10]), tm(start[11:]),
                date.fromisoformat(end[:10]) if end else None, tm(end[11:]) if end else None,
                duration, "TI_DETEC", element, None, salida, "ACTIVO", date(2026, 1, 20),
                incidencia, "1", None, period, "2026-01-20 00:00:00",
                None, None, None,
            )

        save(spark, f"{db}_interrupciones", [
            # normal cut, imported from the SCADA
            interruption("1", "01011", "2026-01-10 10:00:00", "2026-01-10 11:00:00", 3600, "SCADA: apertura"),
            # same cut recorded twice with another id
            interruption("2", "01011", "2026-01-10 10:00:00", "2026-01-10 11:00:00", 3600, "SCADA: apertura", None),
            # overlaps with the first one
            interruption("3", "01011", "2026-01-10 10:30:00", "2026-01-10 12:00:00", 5400),
            # touching interval: not an overlap
            interruption("4", "01011", "2026-01-10 12:00:00", "2026-01-10 12:10:00", 600),
            # zero duration and microcut
            interruption("5", "01012", "2026-01-12 08:00:00", "2026-01-12 08:00:00", 0, "INCOMPLETA. SCADA"),
            interruption("6", "01012", "2026-01-13 08:00:00", "2026-01-13 08:02:00", 120),
            # end before start: impossible
            interruption("7", "01012", "2026-01-14 08:00:00", "2026-01-14 07:00:00", 3600),
            # CT that does not exist in the period
            interruption("8", "05555", "2026-01-15 08:00:00", "2026-01-15 09:00:00", 3600),
            # same start and duration on another output of the CT: kept
            interruption("9", "01011", "2026-01-10 10:00:00", "2026-01-10 11:00:00", 3600, salida="S1"),
            # incident that does not exist
            interruption("10", "01012", "2026-01-16 08:00:00", "2026-01-16 09:00:00", 3600, incidencia="NOPE"),
        ], "INT_ID string, INT_REFERENCIA string, INT_DESC string, INT_OBS string, INT_FECHA_INICIO date, "
           "INT_HORA_INICIO timestamp, INT_FECHA_FIN date, INT_HORA_FIN timestamp, INT_DURACION int, "
           "INT_TIPO_EVENTO_ID string, INT_ELEMENTO_ID string, INT_ACOMETIDA_ID string, INT_SALIDA_ID string, "
           "INT_EG_ID string, INT_FECHA_ALTA date, INT_INCIDENCIA_ID string, INT_USUARIO_ID string, "
           "INT_ABONADO_ID string, INT_PERIODO_ID string, INT_TS string, INT_FECHA_FIN_OPTIMIZADA date, "
           "INT_HORA_FIN_OPTIMIZADA timestamp, INT_DURACION_OPTIMIZADA int")


def load_tedisnet(spark):
    p = "tedis_net_eosa"

    save(spark, f"{p}_system_elements", [
        (3, "/EODSLU:DIS", "EODSLU", 115, None, True),
        (2, "/EPDSLU:DIS", "EPDSLU", 115, None, True),
        (6005, "/IBERDROLA:DIS", "IBERDROLA", 115, None, True),
        (10, "/EODSLU/JERTE:SUB", "JERTE", 100, 3, True),
        (100, "/EODSLU/JERTE/01011:TRA", "01011", 145, 10, True),
        (101, "/EODSLU/JERTE/01012:TRA", "01012", 145, 10, True),
        (200, "/EPDSLU/01011:TRA", "01011", 145, 2, True),
        (300, "/IBERDROLA/FRONTERA:INT", "FRONTERA", 120, 6005, True),
    ], "Id int, Name string, ShortName string, ElementTypeId int, ParentElementId int, IsEnabled boolean")

    save(spark, f"{p}_system_nodes", [(1, 100), (2, 200)], "Id int, ElementId int")
    save(spark, f"{p}_system_devices", [(1, "RTU1"), (2, "RTU2")], "Id int, Name string")

    save(spark, f"{p}_lib_tag_classes", [
        (1, "AI.INTENS L1", "A", None),
        (2, "ES.POSICIÓN", None, None),
        (3, "DI.DEFECTO DE TIERRA.2", None, 4),
    ], "Id int, Name string, Units string, EventTypeId int")

    save(spark, f"{p}_system_tags", [
        (1000, "I L1 01011", "IL1", 1, 1, 100, 300, None, None),
        (1001, "POS 01011", "POS", 1, 2, 100, None, None, None),
        (1002, "TIERRA 01011", "DT", 1, 3, 100, None, None, None),
        (1003, "I L1 EPD", "IL1", 2, 1, 200, 300, None, None),
        (1004, "SIN ELEMENTO", "X", 1, 1, None, None, None, None),
        (1005, "DISPOSITIVO FANTASMA", "X", 99, 1, 100, None, None, None),
    ], "Id int, Name string, ShortName string, DeviceId int, TagClassId int, ElementId int, "
       "StoreInterval int, SummaryStoreInterval int, ExportCode int")

    value_schema = (
        "Id bigint, TagId int, ValueBool boolean, ValueInt int, ValueFloat double, ValueStr string, "
        "ValueEnumId int, Comments string, SourceTimestamp timestamp, UpdateTimestamp timestamp, "
        "QualityId int, QualityDetailId int, QualitySourceId int"
    )

    save(spark, f"{p}_historic_tag_value_changes", [
        (1, 1001, True, None, None, None, None, None, ts("2026-01-10 09:59:00"), ts("2026-01-10 09:59:01"), 1, 14, 1),
        (2, 1002, True, None, None, None, 1, None, ts("2026-01-10 09:58:00"), ts("2026-01-10 09:58:01"), 1, None, 1),
        (3, 1000, None, None, 10.5, None, None, None, ts("2026-01-10 09:00:00"), None, 2, 5, 1),     # bad quality, comm failure
        (4, 1000, None, None, 11.0, None, None, None, ts("2026-01-10 09:05:00"), None, 1, None, 3),  # manual
        (5, 1000, None, None, None, None, None, None, ts("2026-01-10 09:06:00"), None, 1, None, 1),  # no value
        (6, 1004, None, None, 1.0, None, None, None, ts("2026-01-10 09:07:00"), None, 1, None, 1),   # tag rejected
        (7, 1000, None, None, 12.0, None, None, None, ts("2026-01-10 09:08:00"), None, 1, None, 4),  # estimated
        (8, 1000, None, None, 12.0, None, None, None, ts("2026-01-10 09:08:00"), None, 1, None, 4),  # same change, other id
        (9, 1000, None, None, 13.0, None, None, None, None, None, 1, None, 1),                       # no timestamp
    ], value_schema)

    save(spark, f"{p}_system_tag_value_changes", [
        (1, 1001, True, None, None, None, None, None, ts("2026-01-10 09:59:00"), ts("2026-01-10 09:59:05"), 1, 14, 1),
    ], value_schema)

    save(spark, f"{p}_historic_tag_interval_values_big", [
        (1, 1000, None, None, 50.0, None, None, None, ts("2026-01-10 10:00:00"), ts("2026-01-10 10:00:01"), 1, None, 1),
        (2, 1000, None, None, 51.0, None, None, None, ts("2026-01-10 10:05:00"), ts("2026-01-10 10:05:01"), 1, None, 1),
        (3, 1000, None, None, 51.5, None, None, None, ts("2026-01-10 10:05:00"), ts("2026-01-10 10:05:09"), 1, None, 1),
        (4, 1000, None, None, 0.0, None, None, None, ts("2026-01-10 10:10:00"), None, 3, 15, 1),
        (5, 1003, None, None, float("nan"), None, None, None, ts("2026-01-10 10:10:00"), None, 1, None, 1),
        (6, 1003, None, None, 20.0, None, None, None, ts("2026-02-01 00:00:00"), None, 1, 11, 1),
    ], value_schema)

    save(spark, f"{p}_system_tag_interval_values_big", [
        (900, 1000, None, None, 50.0, None, None, None, ts("2026-01-10 10:00:00"), ts("2026-01-10 10:00:01"), 1, None, 1),
    ], value_schema)

    cut_schema = ("Id int, TagValueChangeId int, Timestamp timestamp, ProcessedTimestamp timestamp, "
                  "CutStateId int, IsCommand boolean, RootElementId int")

    save(spark, f"{p}_historic_electric_power_cut_events", [
        (1, 1, ts("2026-01-10 10:00:00"), None, 1, False, 3),
        (2, 1, ts("2026-01-10 10:00:00"), None, 1, False, 3),   # repeated event
        (3, 1, ts("2026-01-10 10:00:00"), None, 1, False, 3),   # repeated event
        (4, None, ts("2026-01-10 11:00:00"), None, 2, False, 3),
        (5, None, ts("2026-01-10 11:30:00"), None, None, False, 3),  # empty
        (6, 2, ts("2026-01-10 12:00:00"), None, 3, False, 3),   # comm error
        (7, 50, ts("2026-01-11 08:00:00"), None, 1, False, 3),  # manoeuvre, no elements
    ], cut_schema)

    save(spark, f"{p}_historic_electric_power_cut_element_events", [
        (1, 100, 1),
        (2, 100, 1),    # same element, moved to event 1
        (3, 101, 113),  # invalid electrical type
        (4, 100, 1),
        (5, 100, 1),    # event rejected
        (6, 100, 1),
        (4, 999, 1),    # element that does not exist
    ], "CutEventId int, ElementId int, ElectricalElementType int")

    save(spark, f"{p}_historic_command_executions", [
        (1, 1, None, None, None, None, None, None, ts("2026-01-11 07:59:00"), ts("2026-01-11 08:10:00"),
         None, None, None, None, None, None, None, False, 50, ts("2026-01-11 07:59:10")),
        (1, 1, None, None, None, None, None, None, ts("2026-01-11 07:59:00"), ts("2026-01-11 08:10:00"),
         None, ts("2026-01-11 07:59:30"), None, ts("2026-01-11 08:00:00"), None, None, None, True, 50,
         ts("2026-01-11 08:00:01")),
    ], "Id int, CommandId int, ValueEnumValueId int, TargetCommandId int, TargetValueEnumValueId int, "
       "StateEnumValueId int, UserId int, FieldUserId int, Created timestamp, ExpiresBy timestamp, "
       "SelectStart timestamp, Selected timestamp, ExecuteStart timestamp, Executed timestamp, "
       "Expired timestamp, Cancelled timestamp, SetByUser timestamp, IsFinished boolean, "
       "TagValueChangeId int, UpdateTimestamp timestamp")

    save(spark, f"{p}_historic_events", [
        (1, 2, None, None),
        (2, 3, None, None),   # its change was rejected
    ], "Id int, TagValueChangeId int, AckUserId int, AckTimestamp timestamp")

    save(spark, f"{p}_lib_tag_class_enum_values_event_levels", [(3, 1, 4)],
         "TagClassId int, EnumValueId int, EventLevelId int")


def load_bronze(spark):
    load_calser(spark)
    load_tedisnet(spark)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_job(name, *extra):
    module = importlib.import_module(f"job_silver_{name}")
    old_argv = sys.argv
    sys.argv = [f"job_silver_{name}.py", "--run-id", RUN_ID, *extra]

    try:
        module.main()
    finally:
        sys.argv = old_argv


def run_d_elemento():
    # job_silver_d_elemento runs at import time (no main()), as in the repo.
    old_argv = sys.argv
    sys.argv = ["job_silver_d_elemento.py"]

    try:
        if "job_silver_d_elemento" in sys.modules:
            importlib.reload(sys.modules["job_silver_d_elemento"])
        else:
            importlib.import_module("job_silver_d_elemento")
    finally:
        sys.argv = old_argv


ORDER = [
    "d_periodo", "d_municipio", "d_tipo_generico", "d_ct", "d_salida",
    "f_incidencia", "f_interrupcion", "d_tag", "f_tag_value_change", "f_evento",
    "f_command_execution", "f_corte",
]


@pytest.fixture(scope="module")
def silver(spark):
    run_d_elemento()

    for name in ORDER:
        run_job(name)

    run_job("f_tag_interval_value")
    run_job("dq_checks")

    return spark


def table(spark, entity):
    return spark.table(f"l2_silver.{entity}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_distribuidora_from_parameters(silver):
    rows = {(r["source_database"], r["distribuidora_id"])
            for r in table(silver, "d_periodo").select("source_database", "distribuidora_id").distinct().collect()}
    assert rows == {("Calser_EOSA", 3), ("Calser_Pitarch", 2)}


def test_periodo_end_and_default(silver):
    df = table(silver, "d_periodo").where("distribuidora_id = 3")
    row = df.where("periodo_id = '202601'").first()
    assert row["fecha_fin_excl"] == date(2026, 2, 1)
    assert df.where("es_default").count() == 1


def test_ct_orphan_municipio_rejected(silver):
    assert table(silver, "d_ct").where("id = '09999'").count() == 0
    rejected = table(silver, "d_ct_rejected").where("id = '09999'")
    assert rejected.where("_motivo = 'ORPHAN_FK'").count() == 2


def test_ct_flags_and_scada_map(silver):
    ct = table(silver, "d_ct")
    assert ct.where("id = '01012' AND NOT potencia_cero").count() == 0
    assert ct.where("id = '01011' AND sin_telemetria").count() == 0

    ct_map = {(r["distribuidora_id"], r["ct_id"]): r
              for r in table(silver, "d_ct_scada").collect()}
    # Same CT code in two distribuidoras maps to two different transformers.
    assert ct_map[(3, "01011")]["elemento_id"] == 100
    assert ct_map[(2, "01011")]["elemento_id"] == 200
    assert ct_map[(3, "01011")]["tiene_nodo"] is True
    assert ct_map[(3, "01012")]["tiene_nodo"] is False


def test_salida_orphan_ct(silver):
    salidas = table(silver, "d_salida")
    assert salidas.where("id = 'S2'").count() == 0
    assert salidas.where("id = 'S1'").first()["nombre"] == "Salida 1"


def test_interrupcion_rules(silver):
    df = table(silver, "f_interrupcion").where("distribuidora_id = 3")
    ids = {r["id"] for r in df.select("id").collect()}

    assert "2" not in ids          # duplicate by natural key, the copy without incident goes
    assert "1" in ids
    assert "7" not in ids          # end before start
    assert "8" not in ids          # CT that does not exist
    assert "9" in ids              # same time on another output is another interruption

    rejected = table(silver, "f_interrupcion_rejected").where("distribuidora_id = 3")
    reasons = {r["id"]: r["_motivo"] for r in rejected.select("id", "_motivo").collect()}
    assert reasons == {"2": "DUPLICATE_NATURAL_KEY", "7": "INVALID_INTERVAL", "8": "ORPHAN_FK"}

    rows = {r["id"]: r for r in df.collect()}
    assert rows["1"]["en_solape"] and rows["3"]["en_solape"]
    assert rows["1"]["grupo_solape_id"] == rows["3"]["grupo_solape_id"]
    assert not rows["4"]["en_solape"]
    assert rows["5"]["duracion_cero"] and rows["5"]["es_incompleta"]
    assert rows["6"]["es_microcorte"]
    assert rows["1"]["es_origen_scada"]
    assert rows["9"]["nivel_afectacion"] == "SALIDA"
    assert rows["10"]["incidencia_inexistente"]
    assert rows["1"]["inicio_ts"] == ts("2026-01-10 10:00:00")
    assert "INT_DURACION_OPTIMIZADA" not in df.columns


def test_tag_orphans(silver):
    ids = {r["id"] for r in table(silver, "d_tag").select("id").collect()}
    assert ids == {1000, 1001, 1002, 1003}
    tag = table(silver, "d_tag").where("id = 1003").first()
    assert tag["distribuidora_id"] == 2
    assert tag["clase_nombre"] == "AI.INTENS L1"
    assert tag["tiene_serie"]


def test_tag_value_change_rules(silver):
    df = table(silver, "f_tag_value_change")
    ids = sorted(r["id"] for r in df.select("id").collect())
    # 1 copied state (Historic wins over System), 2 good, 7 estimated
    assert ids == [1, 2, 7]
    assert df.where("id = 1").first()["_origen"] == "Historic"
    assert df.where("id = 1").first()["es_copiado"]
    assert df.where("id = 7").first()["es_estimado"]

    reasons = {r["id"]: r["_motivo"]
               for r in table(silver, "f_tag_value_change_rejected").select("id", "_motivo").collect()
               if r["_motivo"] != "DUPLICATE_ID"}
    assert reasons == {
        3: "QUALITY_DETAIL_NOT_REAL",
        4: "SOURCE_MANUAL",
        5: "EMPTY_VALUE",
        6: "ORPHAN_FK",
        8: "DUPLICATE_NATURAL_KEY",
        9: "NO_TIMESTAMP",
    }

    quality = table(silver, "f_tag_quality_event").collect()
    assert len(quality) == 1 and quality[0]["calidad_detalle_id"] == 5


def test_evento(silver):
    df = table(silver, "f_evento")
    assert [r["id"] for r in df.collect()] == [1]
    assert df.first()["nivel_evento_id"] == 4


def test_interval_values(silver):
    df = table(silver, "f_tag_interval_value")
    rows = {(r["tag_id"], r["ts"]): r for r in df.collect()}

    # one sample per tag and instant, the latest update wins
    assert rows[(1000, ts("2026-01-10 10:05:00"))]["valor_float"] == 51.5
    assert rows[(1000, ts("2026-01-10 10:00:00"))]["_origen"] == "Historic"
    assert (1000, ts("2026-01-10 10:10:00")) not in rows   # bad quality
    assert (1003, ts("2026-01-10 10:10:00")) not in rows   # NaN
    assert rows[(1003, ts("2026-02-01 00:00:00"))]["fuera_rango_egu"]
    assert df.select("fecha_mes").distinct().count() == 2

    daily = table(silver, "f_tag_interval_value_rechazo_diario")
    motives = {r["motivo"]: r["filas"] for r in
               daily.groupBy("motivo").agg(F.sum("filas").alias("filas")).collect()}
    assert motives == {"QUALITY_DETAIL_NOT_REAL": 1, "NOT_A_NUMBER": 1, "DUPLICATE_NATURAL_KEY": 2}


def test_interval_values_rerun_is_idempotent(silver):
    before = table(silver, "f_tag_interval_value").count()
    run_job("f_tag_interval_value", "--desde", "2026-01", "--hasta", "2026-01")
    assert table(silver, "f_tag_interval_value").count() == before


def test_command_final_state(silver):
    rows = table(silver, "f_command_execution").collect()
    assert len(rows) == 1 and rows[0]["resultado"] == "EJECUTADO"


def test_power_cuts(silver):
    events = {r["evento_id"]: r for r in table(silver, "f_corte_evento").collect()}

    assert set(events) == {1, 4, 6, 7}
    assert events[1]["n_eventos_origen"] == 3
    assert events[1]["n_elementos"] == 2       # 100 and 101 gathered from the copies
    assert events[6]["es_error_comm"]
    assert events[7]["es_maniobra"] and events[7]["sin_elementos"]
    assert events[1]["distribuidora_id"] == 3

    elements = table(silver, "f_corte_elemento")
    e101 = elements.where("elemento_id = 101").first()
    assert e101["tipo_electrico"] is None and e101["tipo_electrico_invalido"]
    assert e101["ct_id"] == "01012"
    assert elements.where("elemento_id = 999").count() == 0

    rejected = {r["_motivo"] for r in table(silver, "f_corte_evento_rejected").collect()}
    assert rejected == {"EMPTY_EVENT", "DUPLICATE_NATURAL_KEY"}


def test_dq_metrics_and_alignment(silver):
    metrics = table(silver, "dq_metrics").where(f"run_id = '{RUN_ID}'")
    assert metrics.where("entidad = 'integridad' AND metrica LIKE 'huerfanos:%' AND valor > 0").count() == 0

    offset = metrics.where("metrica = 'desfase_mediana_min'").first()
    assert offset is not None and offset["valor"] == 0

    vocab = metrics.where("metrica LIKE 'vocabulario_ausente:%'").count()
    assert vocab == 0
