"""
Shared helpers and data quality rules of the Silver layer.

Silver takes the Bronze tables, which are a faithful copy of SQL Server, and
leaves a single valid version of every record. The rules follow the design of
the Trusted Zone (docs/silver-layer.md) and the course guideline of the UPC
Big Data Architecture lab: Silver only applies generic corrections that are
true for any later analysis. Anything debatable is flagged, not deleted, and
the decision is left to Gold.

Two kinds of action:

- Reject: rows that are useless for everybody (bad quality, no value, no
  timestamp, broken keys, exact duplicates). They are written to a
  <entity>_rejected table with a reason code, so a rule that turns out to be
  too aggressive can be audited and reverted.
- Flag: rows that someone may want to treat differently (zero duration,
  overlaps, microcuts, estimated values, manoeuvres, communication errors).
  They are kept with a boolean column.

The module is imported by the jobs of this folder. spark-submit puts the
folder of the application on sys.path, so a plain import is enough; the DAG
also ships it with py_files for the executors.
"""

import argparse
import logging
import re
import uuid
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

BRONZE = "l1_bronze"
SILVER = "l2_silver"

DQ_METRICS_TABLE = f"{SILVER}.dq_metrics"

TEDISNET_DATABASE = "TedisNet_EOSA"

# to_snake() turns "TedisNet_EOSA" into "tedis_net_eosa". The second prefix
# is kept as a fallback in case the ingestion is renamed (same criterion as
# job_silver_d_elemento.py).
TEDISNET_PREFIXES = ["tedis_net_eosa", "tedisnet_eosa"]

# Calser keeps one database per distribuidora.
CALSER_DATABASES = [
    "Calser_EOSA",
    "Calser_Pitarch",
    "Calser_ValleSantaAna",
]

DISTRIBUTOR_PARAM_CODE = "DistributorId"


# ---------------------------------------------------------------------------
# Business constants (verified against the TedisNet 3.4 catalogs and the
# Calser 6.4.3 source code, see docs/silver-layer.md)
# ---------------------------------------------------------------------------

# LibQualities
QUALITY_GOOD = 1

# LibQualityDetails whose value is not a real reading even if the quality
# says Good: 1 not connected, 2 configuration error, 3 device failure,
# 4 sensor failure, 5 communication failure, 6 last known, 7 out of service,
# 8 waiting for initial values, 9 last usable, 10 sensor calibrating,
# 15 starting, 16 stopped, 17 activating, 18 invalid, 19 blocked,
# 20 not topical.
QUALITY_DETAIL_NOT_REAL = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20]

# 11 exceeded (EGU) and 12 insufficient: real reading out of the configured
# range. Kept and flagged, they may be precursors of a fault.
QUALITY_DETAIL_OUT_OF_RANGE = [11, 12]

# 14 copied: value copied from the RTU signal to the element state tag. It is
# the exact value the SCADA uses to detect power cuts, so it must survive.
QUALITY_DETAIL_COPIED = 14

# LibQualitySources: 1 telemetered, 2 calculated, 3 manual, 4 estimated
QUALITY_SOURCE_CALCULATED = 2
QUALITY_SOURCE_MANUAL = 3
QUALITY_SOURCE_ESTIMATED = 4

# LibElectricPowerCutStates
CUT_STATE_OFF = 1
CUT_STATE_ON = 2
CUT_STATE_COMM_ERROR = 3

# ElectricalElementType (SharedLibrary/Core/ElectricalElementType.cs):
# 1 transformer, 2 LV line, 3 LV service connection, 4 LV subscriber.
VALID_ELECTRICAL_ELEMENT_TYPES = [1, 2, 3, 4]

# LibElementTypes
ELEMENT_TYPE_DISTRIBUTOR = 115
ELEMENT_TYPE_TRAFO_CT = 145

# Calser: interruptions up to this duration are microcuts. Same value as the
# MinimalDurationImportInterruptions parameter active in the three databases.
MICROCUT_MAX_SECONDS = 180

# Calser: interruptions longer than this are flagged as extreme. They are not
# winsorized here: that is a modelling decision and belongs to Gold.
EXTREME_DURATION_SECONDS = 7 * 24 * 3600

# Tolerance between INT_DURACION and the difference of the two timestamps.
DURATION_TOLERANCE_SECONDS = 60

# Calser vocabulary the target depends on. If one disappears the label of
# the model silently changes, so its absence is reported.
CALSER_REQUIRED_VOCABULARY = ["CL_IMPRE", "CL_PROGR", "FA_CLIEN", "TI_DETEC", "TI_MANDO"]

# Maximum share of rejected rows before a metric is marked for review.
# TagIntervalValuesBig loses around 22 % on the real database, the other
# entities should lose almost nothing.
DEFAULT_MAX_REJECTED_PCT = 5.0
MAX_REJECTED_PCT = {
    "f_tag_interval_value": 30.0,
    "f_tag_value_change": 30.0,
    "f_corte_evento": 60.0,
    "f_corte_elemento": 60.0,
}


# ---------------------------------------------------------------------------
# Session, logging and arguments
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("silver")


def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_args(extra=None) -> argparse.Namespace:
    """
    Common arguments of every Silver job. --run-id groups the metrics of one
    DAG run so the final check can evaluate them together.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)

    if extra:
        extra(parser)

    args, _ = parser.parse_known_args()

    if not args.run_id:
        args.run_id = (
            "manual_"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "_"
            + uuid.uuid4().hex[:6]
        )

    return args


# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------

def to_snake(value: str) -> str:
    """
    Same conversion as the Bronze jobs, so Silver finds the tables they write.
    """
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)

    return value.lower()


def bronze_table(database: str, table: str) -> str:
    return f"{BRONZE}.{to_snake(database)}_{to_snake(table)}"


def silver_table(entity: str) -> str:
    return f"{SILVER}.{entity}"


def tedisnet_table(spark: SparkSession, table: str):
    """
    Returns the Bronze name of a TedisNet table, or None if it has not been
    ingested yet (System* tables only exist once the streaming job has run).
    """
    for prefix in TEDISNET_PREFIXES:
        name = f"{BRONZE}.{prefix}_{to_snake(table)}"

        if spark.catalog.tableExists(name):
            return name

    return None


def require_table(spark: SparkSession, table: str) -> str:
    if not spark.catalog.tableExists(table):
        raise ValueError(
            f"Table {table} does not exist, run the job that builds it first"
        )

    return table


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def project(df: DataFrame, columns: dict) -> DataFrame:
    """
    Selects a fixed list of columns with a fixed type. A column missing in
    the source is created as null, so every union has the same schema no
    matter whether it comes from the batch or the CDC ingestion.
    """
    return df.select(
        *[
            (
                F.col(f"`{name}`").cast(data_type)
                if name in df.columns
                else F.lit(None).cast(data_type)
            ).alias(name)
            for name, data_type in columns.items()
        ]
    )


def read_tedisnet_union(
    spark: SparkSession,
    base_name: str,
    columns: dict,
    tables=None,
    required: bool = True,
):
    """
    Reads the Historic* and System* twins of a TedisNet entity and returns a
    single dataframe with the technical columns _origen (Historic / System),
    _origen_rank (2 / 1, Historic is the definitive copy) and _tabla_origen.

    System* keeps only the last 7 days in the source and arrives through CDC;
    Historic* is the complete history and arrives in batch. The same row can
    exist in both, which is why every job deduplicates after this union.
    """
    if tables is None:
        tables = [
            (f"Historic{base_name}", "Historic", 2),
            (f"System{base_name}", "System", 1),
        ]

    parts = []

    for source_table, origin, rank in tables:
        name = tedisnet_table(spark, source_table)

        if name is None:
            logger.warning("Bronze table not found, skipped: %s", source_table)
            continue

        parts.append(
            project(spark.table(name), columns)
            .withColumn("_origen", F.lit(origin))
            .withColumn("_origen_rank", F.lit(rank))
            .withColumn("_tabla_origen", F.lit(source_table))
        )

    if not parts:
        if required:
            raise ValueError(
                f"No Bronze table available for the TedisNet entity {base_name}"
            )

        return None

    return reduce(lambda a, b: a.unionByName(b), parts)


def read_distributor_map(spark: SparkSession) -> DataFrame:
    """
    Maps every Calser database to its distribuidora. The value of the
    DistributorId parameter is the Id of the distribuidora root element in
    TedisNet (3 EOSA, 2 Pitarch, 1366 Valle de Santa Ana), so it is also the
    key that joins both systems. It is not an ordinal: never map by order.
    """
    parts = []

    for database in CALSER_DATABASES:
        table = bronze_table(database, "parametros_configuracion")

        if not spark.catalog.tableExists(table):
            logger.warning("Parameters table not found: %s", table)
            continue

        parts.append(
            spark.table(table)
            .where(F.trim(F.col("PARAM_CONFIG_CODIGO")) == F.lit(DISTRIBUTOR_PARAM_CODE))
            .select(
                F.lit(database).alias("source_database"),
                F.trim(F.col("PARAM_CONFIG_VALOR"))
                .try_cast("bigint")
                .alias("distribuidora_id"),
            )
        )

    if not parts:
        raise ValueError(
            "No parametros_configuracion table in Bronze, the Calser rows "
            "cannot be attributed to a distribuidora"
        )

    return reduce(lambda a, b: a.unionByName(b), parts).distinct()


def read_calser_union(spark: SparkSession, table: str, required: bool = True):
    """
    Reads the same table from the three Calser databases, adds
    source_database and distribuidora_id and returns the union.

    A database with rows but without DistributorId stops the job: CT codes
    repeat between distribuidoras, so a row without distribuidora would be
    joined to the wrong CT later on.
    """
    parts = []

    for database in CALSER_DATABASES:
        name = bronze_table(database, table)

        if not spark.catalog.tableExists(name):
            logger.warning("Bronze table not found, skipped: %s", name)
            continue

        parts.append(
            spark.table(name).withColumn("source_database", F.lit(database))
        )

    if not parts:
        if required:
            raise ValueError(f"No Bronze table available for Calser {table}")

        return None

    union = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        parts,
    )

    mapping = read_distributor_map(spark)

    missing = [
        row["source_database"]
        for row in (
            union.select("source_database").distinct()
            .join(mapping, "source_database", "left_anti")
            .collect()
        )
    ]

    if missing:
        raise ValueError(
            f"Calser databases without {DISTRIBUTOR_PARAM_CODE}: {missing}"
        )

    return union.join(F.broadcast(mapping), "source_database", "left")


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------

def clean_str(column) -> Column:
    """
    Trims a text column and turns blank values into null.
    """
    value = F.trim(F.col(column) if isinstance(column, str) else column)

    return F.when(value == F.lit(""), F.lit(None).cast("string")).otherwise(value)


def _date_str(df: DataFrame, name: str) -> Column:
    data_type = df.schema[name].dataType

    if isinstance(data_type, (DateType, TimestampType, TimestampNTZType)):
        return F.date_format(F.col(name), "yyyy-MM-dd")

    return F.substring(F.trim(F.col(name).cast("string")), 1, 10)


def _time_str(df: DataFrame, name: str) -> Column:
    """
    SQL Server TIME can reach Spark as a timestamp on 1970-01-01, as a
    TimeType or as text depending on the driver and the Spark version. The
    three cases end up as HH:mm:ss.
    """
    data_type = df.schema[name].dataType

    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return F.date_format(F.col(name), "HH:mm:ss")

    return F.substring(F.trim(F.col(name).cast("string")), 1, 8)


def combine_date_time(df: DataFrame, date_col: str, time_col: str) -> Column:
    """
    Calser stores the date and the time of an event in two columns. Returns
    a single timestamp, or null if any part is missing or malformed.
    """
    return F.when(
        F.col(date_col).isNotNull() & F.col(time_col).isNotNull(),
        F.try_to_timestamp(
            F.concat_ws(" ", _date_str(df, date_col), _time_str(df, time_col)),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        ),
    )


def to_date_col(df: DataFrame, name: str) -> Column:
    data_type = df.schema[name].dataType

    if isinstance(data_type, DateType):
        return F.col(name)

    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return F.to_date(F.col(name))

    return F.try_to_date(F.substring(F.trim(F.col(name).cast("string")), 1, 10))


# ---------------------------------------------------------------------------
# Rejects and deduplication
# ---------------------------------------------------------------------------

def split_rejects(df: DataFrame, rules: list):
    """
    Applies an ordered list of (reason, condition) rules. The first rule that
    matches gives the reason of the rejection. Returns (kept, rejected); the
    rejected dataframe carries the _motivo column.

    Conditions must be null safe: a null condition does not reject.
    """
    if not rules:
        return df, None

    reason = None

    for code, condition in rules:
        clause = F.coalesce(condition, F.lit(False))

        reason = (
            F.when(clause, F.lit(code))
            if reason is None
            else reason.when(clause, F.lit(code))
        )

    tagged = df.withColumn("_motivo", reason)

    return (
        tagged.where(F.col("_motivo").isNull()).drop("_motivo"),
        tagged.where(F.col("_motivo").isNotNull()),
    )


def keep_first(df: DataFrame, keys: list, order: list, reason: str):
    """
    Keeps one row per key: the first one according to `order`. Returns
    (kept, dropped) and the dropped rows carry `reason` in _motivo.
    """
    window = Window.partitionBy(*keys).orderBy(*order)

    ranked = df.withColumn("_rn", F.row_number().over(window))

    kept = ranked.where(F.col("_rn") == 1).drop("_rn")
    dropped = (
        ranked.where(F.col("_rn") > 1)
        .drop("_rn")
        .withColumn("_motivo", F.lit(reason))
    )

    return kept, dropped


def union_rejects(parts: list):
    parts = [part for part in parts if part is not None]

    if not parts:
        return None

    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        parts,
    )


def orphan_condition(df: DataFrame, reference: DataFrame, keys: list, flag: str) -> DataFrame:
    """
    Adds a boolean column `flag` that is true when the row has no match in
    `reference` for `keys` (left anti join semantics, but keeping the rows).
    Rows whose keys are null are orphans too.
    """
    ref = reference.select(*keys).distinct().withColumn(f"_ref_{flag}", F.lit(True))

    joined = df.join(ref, on=keys, how="left")

    return joined.withColumn(
        flag,
        F.col(f"_ref_{flag}").isNull(),
    ).drop(f"_ref_{flag}")


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_table(
    df: DataFrame,
    table: str,
    partition_by=None,
    replace_where: str = None,
) -> None:
    """
    Writes a Silver table. Every job rebuilds its output from Bronze, so the
    write is an overwrite and the process is idempotent: running it twice
    gives the same result. Big tables overwrite only the partitions of the
    processed window with replaceWhere.
    """
    spark = df.sparkSession

    if partition_by:
        # The metastore expects partition columns at the end of the schema.
        # Without this, a second write with replaceWhere on the same table
        # fails with "Corrupted table metadata".
        df = df.select(
            *[name for name in df.columns if name not in partition_by],
            *partition_by,
        )

    writer = df.write.format("delta").mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if replace_where and spark.catalog.tableExists(table):
        writer = writer.option("replaceWhere", replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")

    writer.saveAsTable(table)

    logger.info("Written %s", table)


def write_rejected(
    df,
    entity: str,
    run_id: str,
    replace_where: str = None,
    partition_by=None,
) -> None:
    """
    Writes the rejected rows of an entity to <entity>_rejected. The table is
    rebuilt with the entity, so it always describes the current Silver.
    An empty result still writes the table: an empty rejected table is a
    useful answer ("nothing was discarded").
    """
    if df is None:
        return

    out = (
        df.withColumn("_run_id", F.lit(run_id))
        .withColumn("_rejected_at", F.current_timestamp())
    )

    write_table(
        out,
        silver_table(f"{entity}_rejected"),
        partition_by=partition_by,
        replace_where=replace_where,
    )


# ---------------------------------------------------------------------------
# Data quality metrics
# ---------------------------------------------------------------------------

DQ_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("job", StringType(), False),
    StructField("entidad", StringType(), False),
    StructField("ambito", StringType(), True),
    StructField("metrica", StringType(), False),
    StructField("valor", LongType(), True),
    StructField("total", LongType(), True),
    StructField("pct", DoubleType(), True),
    StructField("umbral_pct", DoubleType(), True),
    StructField("estado", StringType(), False),
    StructField("detalle", StringType(), True),
    StructField("ts", TimestampType(), False),
])


class DQCollector:
    """
    Collects the quality metrics of a job and appends them to dq_metrics.

    estado is OK, REVISAR (a threshold was exceeded) or INFO (the metric has
    no threshold). The last job of the DAG reads the REVISAR rows of the run
    and decides whether the batch can move on to Gold.
    """

    def __init__(self, spark: SparkSession, job: str, run_id: str):
        self.spark = spark
        self.job = job
        self.run_id = run_id
        self.rows = []

    def add(
        self,
        entidad: str,
        metrica: str,
        valor,
        total=None,
        umbral_pct: float = None,
        ambito: str = None,
        detalle: str = None,
        estado: str = None,
    ) -> None:
        valor = None if valor is None else int(valor)
        total = None if total is None else int(total)

        pct = None

        if valor is not None and total:
            pct = round(100.0 * valor / total, 4)

        if estado is None:
            if umbral_pct is None:
                estado = "INFO"
            elif pct is not None and pct > umbral_pct:
                estado = "REVISAR"
            else:
                estado = "OK"

        if estado == "REVISAR":
            logger.warning(
                "DQ REVISAR %s %s %s: %s of %s (%s %%) threshold %s %s",
                entidad, ambito or "", metrica, valor, total, pct,
                umbral_pct, detalle or "",
            )
        else:
            logger.info(
                "DQ %s %s %s: %s of %s (%s %%)",
                entidad, ambito or "", metrica, valor, total, pct,
            )

        self.rows.append((
            self.run_id,
            self.job,
            entidad,
            ambito,
            metrica,
            valor,
            total,
            pct,
            None if umbral_pct is None else float(umbral_pct),
            estado,
            detalle,
            datetime.now(timezone.utc).replace(tzinfo=None),
        ))

    def add_entity_counts(
        self,
        entidad: str,
        total_in: int,
        kept: int,
        rejected_df=None,
        flags_df: DataFrame = None,
        flags: list = None,
        ambito: str = None,
    ) -> None:
        """
        Standard block of metrics of an entity: input rows, output rows,
        rejected rows by reason (with the threshold of the entity) and
        flagged rows by flag.
        """
        threshold = MAX_REJECTED_PCT.get(entidad, DEFAULT_MAX_REJECTED_PCT)

        self.add(entidad, "filas_entrada", total_in, ambito=ambito)
        self.add(entidad, "filas_salida", kept, total_in, ambito=ambito)

        rejected_total = 0

        if rejected_df is not None:
            for row in rejected_df.groupBy("_motivo").count().collect():
                rejected_total += row["count"]

                self.add(
                    entidad,
                    f"rechazo:{row['_motivo']}",
                    row["count"],
                    total_in,
                    ambito=ambito,
                )

        self.add(
            entidad,
            "rechazo_total",
            rejected_total,
            total_in,
            umbral_pct=threshold,
            ambito=ambito,
        )

        if flags_df is not None and flags:
            sums = flags_df.agg(
                *[
                    F.sum(F.col(flag).cast("long")).alias(flag)
                    for flag in flags
                ]
            ).first()

            for flag in flags:
                self.add(
                    entidad,
                    f"flag:{flag}",
                    sums[flag] or 0,
                    kept,
                    ambito=ambito,
                )

    def flush(self) -> None:
        if not self.rows:
            return

        df = self.spark.createDataFrame(self.rows, DQ_SCHEMA)

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(DQ_METRICS_TABLE)
        )

        logger.info(
            "Written %s quality metrics to %s",
            len(self.rows),
            DQ_METRICS_TABLE,
        )

        self.rows = []
