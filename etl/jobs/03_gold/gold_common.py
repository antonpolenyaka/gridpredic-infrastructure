"""
Shared helpers and parameters of the Gold layer (Exploitation Zone).

Gold is where the business and modelling decisions live: what counts as an
interruption for the label, how overlapping records are merged, how the power
of a CT is imputed, which windows the features use and how the dataset is
split. Everything that Silver leaves flagged is decided here, and every
decision is a parameter of etl/config/03_gold/config_gold.json, so it can be
changed and the dataset rebuilt without touching Silver.

Conventions shared by every Gold job:

- Grain: (distribuidora_id, ct_id, hora). ct_id is the Calser CT_ID, which is
  the ShortName of the TRAFO CT element in TedisNet.
- hora is the prediction instant, an exact hour. A row only uses data with a
  timestamp strictly earlier than hora: the hourly aggregates of the bucket
  [hora - 1 h, hora) are stored with hora at the end of the bucket. Labels look
  forward from hora: y_1_3h = an interruption starts in (hora + 1 h, hora + 3 h].
- "Timestamp" means the moment the data could be known, not the moment the
  thing happened. Labels use the field time of the event; features use the
  arrival time (known_time: the later of the field time and the time the
  SCADA server stored it), the load date of a Calser record (conocido_ts)
  and, for the topology learnt from the cuts, the first cut that revealed it.
- Timestamps are naive wall clock times, as both sources store them. The
  session time zone is fixed to UTC so that hour arithmetic has no daylight
  saving jumps. If TedisNet turns out to store UTC (the Silver alignment check
  measures it), the parameters tiempo.tedisnet_en_utc and
  tiempo.desfase_tedisnet_min correct it in one place (tedisnet_time).
- Big tables are partitioned by fecha_mes and written month by month with
  replaceWhere, so a month can be rebuilt without touching the rest.

The module is imported by the jobs of this folder. spark-submit puts the
folder of the application on sys.path; the DAG also ships it with py_files.
"""

import argparse
import calendar
import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

BRONZE = "l1_bronze"
SILVER = "l2_silver"
GOLD = "l3_gold"

DQ_METRICS_TABLE = f"{GOLD}.dq_metrics"

# Where the configuration is looked for when --config is not given: the
# Airflow container, the Spark master container and the repository itself.
CONFIG_CANDIDATES = [
    "/opt/airflow/config/03_gold/config_gold.json",
    "/app/config/03_gold/config_gold.json",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "config", "03_gold", "config_gold.json",
    ),
]

HOUR = 3600


# ---------------------------------------------------------------------------
# Session, logging, arguments and parameters
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("gold")


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # Naive wall clock timestamps: UTC as session zone means no DST jumps in
    # date_trunc, hour sequences or windows by epoch seconds.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def parse_args(extra=None) -> argparse.Namespace:
    """
    Common arguments of every Gold job. --desde / --hasta (YYYY-MM) limit the
    months a job processes; without them the job covers the whole window of
    the configuration.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--desde", default=None, help="First month, YYYY-MM")
    parser.add_argument("--hasta", default=None, help="Last month, YYYY-MM (inclusive)")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop the output tables first instead of replacing month by month",
    )

    if extra:
        extra(parser)

    args, _ = parser.parse_known_args()

    # --rebuild drops the whole table; with a month range it would leave only
    # those months in it.
    if args.rebuild and (args.desde or args.hasta):
        parser.error("--rebuild can not be combined with --desde / --hasta")

    if not args.run_id:
        args.run_id = (
            "manual_"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "_"
            + uuid.uuid4().hex[:6]
        )

    return args


def load_config(path: str = None) -> dict:
    candidates = [path] if path else CONFIG_CANDIDATES

    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            with open(candidate, encoding="utf-8") as file:
                config = json.load(file)

            logger.info("Gold configuration read from %s", os.path.abspath(candidate))
            config["_path"] = os.path.abspath(candidate)
            return config

    raise FileNotFoundError(
        f"config_gold.json not found in {candidates}. Pass --config or mount "
        f"etl/config in the container"
    )


def load_params(args) -> dict:
    return load_config(args.config)["parametros"]


def params_hash(params: dict) -> str:
    import hashlib

    text = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

def silver_table(entity: str) -> str:
    return f"{SILVER}.{entity}"


def gold_table(entity: str) -> str:
    return f"{GOLD}.{entity}"


def require_table(spark: SparkSession, table: str) -> str:
    if not spark.catalog.tableExists(table):
        raise ValueError(
            f"Table {table} does not exist, run the job that builds it first"
        )

    return table


def optional_table(spark: SparkSession, table: str):
    if spark.catalog.tableExists(table):
        return spark.table(table)

    logger.warning("Optional table not found, its features stay null: %s", table)
    return None


def delta_version(spark: SparkSession, table: str):
    """
    Current version of a Delta table, or None. dataset_versions stores it so
    a training set can be rebuilt exactly with time travel.
    """
    if not spark.catalog.tableExists(table):
        return None

    try:
        row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
        return None if row is None else int(row["version"])
    except Exception as exc:  # noqa: BLE001 - a missing history is not fatal
        logger.warning("No Delta history for %s: %s", table, exc)
        return None


def drop_table(spark: SparkSession, table: str) -> None:
    if spark.catalog.tableExists(table):
        spark.sql(f"DROP TABLE {table}")
        logger.info("Dropped %s", table)


def write_table(
    df: DataFrame,
    table: str,
    partition_by=None,
    replace_where: str = None,
) -> None:
    """
    Writes a Gold table. Small tables are rebuilt whole; the big ones
    (labels, aggregates, features, dataset) overwrite only the months of the
    batch with replaceWhere. mergeSchema lets a new feature appear without
    rebuilding the months already written (they read it as null); --rebuild
    gives a clean table.
    """
    spark = df.sparkSession

    if partition_by:
        # The metastore expects partition columns at the end of the schema.
        df = df.select(
            *[name for name in df.columns if name not in partition_by],
            *partition_by,
        )

    writer = df.write.format("delta").mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if replace_where and spark.catalog.tableExists(table):
        writer = writer.option("replaceWhere", replace_where).option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")

    writer.saveAsTable(table)

    logger.info("Written %s%s", table, f" ({replace_where})" if replace_where else "")


# ---------------------------------------------------------------------------
# Data quality metrics (same schema as l2_silver.dq_metrics)
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
    Collects the quality metrics of a job and appends them to
    l3_gold.dq_metrics. estado is OK, REVISAR (a threshold was exceeded) or
    INFO. job_gold_dq_checks reads the REVISAR rows of the run.
    """

    def __init__(self, spark: SparkSession, job: str, run_id: str, table: str = DQ_METRICS_TABLE):
        self.spark = spark
        self.job = job
        self.run_id = run_id
        self.table = table
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
        minimo_pct: float = None,
    ) -> None:
        """
        umbral_pct: REVISAR when pct is above it. minimo_pct: REVISAR when pct
        is below it (coverage metrics).
        """
        valor = None if valor is None else int(valor)
        total = None if total is None else int(total)

        pct = None

        if valor is not None and total:
            pct = round(100.0 * valor / total, 4)

        if estado is None:
            if umbral_pct is None and minimo_pct is None:
                estado = "INFO"
            elif pct is None:
                estado = "OK"
            elif umbral_pct is not None and pct > umbral_pct:
                estado = "REVISAR"
            elif minimo_pct is not None and pct < minimo_pct:
                estado = "REVISAR"
            else:
                estado = "OK"

        level = logging.WARNING if estado == "REVISAR" else logging.INFO
        logger.log(
            level,
            "DQ %s %s %s %s: %s of %s (%s %%) %s",
            estado, entidad, ambito or "", metrica, valor, total, pct, detalle or "",
        )

        threshold = umbral_pct if umbral_pct is not None else minimo_pct

        self.rows.append((
            self.run_id,
            self.job,
            entidad,
            ambito,
            metrica,
            valor,
            total,
            pct,
            None if threshold is None else float(threshold),
            estado,
            detalle,
            datetime.now(timezone.utc).replace(tzinfo=None),
        ))

    def flush(self) -> None:
        if not self.rows:
            return

        df = self.spark.createDataFrame(self.rows, DQ_SCHEMA)

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.table)
        )

        logger.info("Written %s quality metrics to %s", len(self.rows), self.table)

        self.rows = []


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------

def ts_lit(value) -> Column:
    """
    Timestamp literal. Strings are 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.
    """
    if isinstance(value, datetime):
        value = value.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(value, date):
        value = value.isoformat()

    return F.lit(value).cast("timestamp")


def to_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    return datetime.fromisoformat(value)


def epoch(value: datetime) -> int:
    """
    Seconds of a naive wall clock datetime, read as UTC like the session.
    """
    return calendar.timegm(to_datetime(value).timetuple())


def tedisnet_time(column, params: dict) -> Column:
    """
    Puts a TedisNet timestamp on the same clock as Calser. With the default
    parameters (both sources in local time) it returns the column unchanged.
    """
    c = F.col(column) if isinstance(column, str) else column
    tiempo = params["tiempo"]

    if tiempo.get("tedisnet_en_utc"):
        c = F.from_utc_timestamp(c, tiempo.get("zona_horaria_local", "Europe/Madrid"))

    minutes = int(tiempo.get("desfase_tedisnet_min") or 0)

    if minutes:
        c = c + F.expr(f"INTERVAL {minutes} MINUTES")

    return c


def known_time(df: DataFrame, params: dict, column: str = "ts", arrival: str = "ts_actualizacion") -> Column:
    """
    First moment a TedisNet row could be known, on the Calser clock: the
    later of its field time (ts) and the time the server stored it
    (ts_actualizacion). A change buffered by an RTU during a communication
    loss has an old field time but only exists from its arrival on; using the
    field time would put it in hours that could not see it. Tables without
    an arrival column fall back to the field time.
    """
    c = F.col(column)

    if arrival in df.columns:
        c = F.greatest(c, F.coalesce(F.col(arrival), c))

    return tedisnet_time(c, params)


def arrival_delay_s(df: DataFrame, column: str = "ts", arrival: str = "ts_actualizacion") -> Column:
    """
    Seconds between the field time and the arrival at the server (null when
    the table has no arrival column).
    """
    if arrival not in df.columns:
        return F.lit(None).cast("long")

    return F.unix_timestamp(arrival) - F.unix_timestamp(column)


def bucket_hour(column) -> Column:
    """
    Hour at which the data of a timestamp becomes available: the end of its
    one hour bucket. A sample at 09:40 belongs to [09:00, 10:00) and can be
    used from hora = 10:00 on.
    """
    c = F.col(column) if isinstance(column, str) else column
    return F.date_trunc("hour", c) + F.expr("INTERVAL 1 HOUR")


def ceil_hour(column) -> Column:
    c = F.col(column) if isinstance(column, str) else column
    truncated = F.date_trunc("hour", c)
    return F.when(truncated == c, truncated).otherwise(truncated + F.expr("INTERVAL 1 HOUR"))


def hours_in(start, end_exclusive) -> Column:
    """
    Array with the exact hours h such that start <= h < end_exclusive, empty
    when there is none. Used to turn an event into the hours it labels.
    """
    first = ceil_hour(start)
    last = ceil_hour(end_exclusive) - F.expr("INTERVAL 1 HOUR")

    return F.when(
        first <= last,
        F.sequence(first, last, F.expr("INTERVAL 1 HOUR")),
    ).otherwise(F.array().cast("array<timestamp>"))


def era_expr(column, params: dict) -> Column:
    """
    Era of the label (Calser import regime): 1 file imports until 06.02.2018,
    2 manual / import without source until October 2025, 3 SCADA via TedisNet
    since November 2025. Only era 3 has a guaranteed fine match between
    features and labels.
    """
    c = F.col(column) if isinstance(column, str) else column
    expr = None

    for era in params["eras"]:
        if era["hasta"] is None:
            value = F.lit(era["era"])
            expr = value if expr is None else expr.otherwise(value)
            break

        condition = c < ts_lit(era["hasta"])
        expr = F.when(condition, F.lit(era["era"])) if expr is None else expr.when(condition, F.lit(era["era"]))

    return expr


def month_start(value) -> date:
    if isinstance(value, str):
        parts = value.split("-")
        return date(int(parts[0]), int(parts[1]), 1)

    return date(value.year, value.month, 1)


def next_month(value: date) -> date:
    return date(value.year + (value.month // 12), value.month % 12 + 1, 1)


def add_months(value: date, months: int) -> date:
    for _ in range(months):
        value = next_month(value)

    return value


def window_bounds(params: dict):
    """
    (first hour, end exclusive) of the dataset window.
    """
    return to_datetime(params["ventana"]["desde"]), to_datetime(params["ventana"]["hasta"])


def batches(args, params: dict, months_per_batch: int) -> list:
    """
    Month batches to process: list of (start datetime, end datetime
    exclusive), clipped to the dataset window and to --desde / --hasta.
    """
    start, end = window_bounds(params)

    first = month_start(args.desde) if args.desde else month_start(start)
    last = month_start(args.hasta) if args.hasta else month_start(end - timedelta(seconds=1))

    out = []
    current = first

    while current <= last:
        batch_end = min(add_months(current, max(1, months_per_batch)), next_month(last))

        lo = max(to_datetime(current), start)
        hi = min(to_datetime(batch_end), end)

        if lo < hi:
            out.append((lo, hi))

        current = batch_end

    return out


def month_range_condition(column: str, lo: datetime, hi: datetime) -> str:
    """
    replaceWhere condition on a fecha_mes partition column for the months
    touched by [lo, hi).
    """
    first = month_start(lo)
    last_exclusive = next_month(month_start(hi - timedelta(seconds=1)))
    return f"{column} >= DATE'{first.isoformat()}' AND {column} < DATE'{last_exclusive.isoformat()}'"


def hour_grid(spark: SparkSession, entities: DataFrame, lo: datetime, hi: datetime) -> DataFrame:
    """
    Every entity row times every hour in [lo, hi). The grid makes the hours
    without data exist, so that windows do not skip gaps and "no events" can
    be told apart from "no row".
    """
    hours = spark.range(epoch(lo), epoch(hi), HOUR).select(
        F.timestamp_seconds(F.col("id")).alias("hora")
    )

    return entities.crossJoin(F.broadcast(hours))


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

def easter_sunday(year: int) -> date:
    """
    Gregorian Easter (anonymous Gregorian algorithm, Meeus/Jones/Butcher).
    """
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month = (h + m - 7 * n + 114) // 31
    day = (h + m - 7 * n + 114) % 31 + 1
    return date(year, month, day)


def holidays(first_year: int, last_year: int, params: dict) -> set:
    """
    National holidays plus the regional ones of Extremadura that the
    configuration lists. LibElectricCalendarDates only has 9 rows in EOSA, so
    the calendar is computed here. Local holidays of each municipality are
    not included.
    """
    feats = params["features"]
    out = set()

    for year in range(first_year, last_year + 1):
        for month_day in feats.get("festivos_fijos", []):
            month, day = month_day.split("-")
            out.add(date(year, int(month), int(day)))

        easter = easter_sunday(year)

        if "jueves" in feats.get("festivos_semana_santa", []):
            out.add(easter - timedelta(days=3))

        if "viernes" in feats.get("festivos_semana_santa", []):
            out.add(easter - timedelta(days=2))

    return out


def calendar_days(spark: SparkSession, lo: datetime, hi: datetime, params: dict) -> DataFrame:
    first = to_datetime(lo).date() - timedelta(days=1)
    last = to_datetime(hi).date() + timedelta(days=1)

    festivos = holidays(first.year, last.year, params)

    rows = []
    day = first

    while day <= last:
        rows.append((
            day,
            day in festivos,
            (day + timedelta(days=1)) in festivos,
            day.weekday() >= 5,
        ))
        day += timedelta(days=1)

    schema = StructType([
        StructField("fecha", DateType(), False),
        StructField("es_festivo", BooleanType(), False),
        StructField("es_vispera_festivo", BooleanType(), False),
        StructField("es_fin_de_semana", BooleanType(), False),
    ])

    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# Signal families
# ---------------------------------------------------------------------------

def family_expr(column, families: dict) -> Column:
    """
    Name of the first family whose regular expression matches the tag class,
    or null. The order of the configuration matters: DI.DISPARO TEMPORIZADO
    NEUTRO is an earth fault before it is a generic trip. Matching ignores
    case and accents are written as character classes in the patterns.
    """
    c = F.col(column) if isinstance(column, str) else column
    expr = None

    for name, pattern in families.items():
        condition = c.rlike(f"(?iu){pattern}")
        expr = F.when(condition, F.lit(name)) if expr is None else expr.when(condition, F.lit(name))

    return expr if expr is not None else F.lit(None).cast("string")


def scale_factor(units_column) -> Column:
    """
    Values in V, W, VAr or VA are taken to kV, kW, kVAr and kVA, so tags of
    the same family that come from different devices share a scale.
    """
    units = F.upper(F.trim(F.col(units_column) if isinstance(units_column, str) else units_column))

    return F.when(units.isin("V", "W", "VAR", "VA"), F.lit(0.001)).otherwise(F.lit(1.0))


# ---------------------------------------------------------------------------
# Feature registry
# ---------------------------------------------------------------------------

FEATURE_METADATA_SCHEMA = StructType([
    StructField("feature", StringType(), False),
    StructField("bloque", StringType(), False),
    StructField("ambito", StringType(), True),
    StructField("familia", StringType(), True),
    StructField("ventana", StringType(), True),
    StructField("fuente", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("descripcion", StringType(), True),
    StructField("segura_leakage", BooleanType(), False),
])


class FeatureRegistry:
    """
    Keeps the description of every feature column while it is built. The
    features job writes it to l3_gold.feature_metadata: it is the contract
    between the batch computation, the dataset and, later, serving.
    """

    def __init__(self):
        self.rows = {}

    def add(self, feature, bloque, descripcion, ambito=None, familia=None,
            ventana=None, fuente=None, tipo="double"):
        self.rows[feature] = (
            feature, bloque, ambito, familia, ventana, fuente, tipo, descripcion, True,
        )
        return feature

    def names(self) -> list:
        return list(self.rows)

    def dataframe(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame(list(self.rows.values()), FEATURE_METADATA_SCHEMA)
