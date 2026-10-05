"""
Gold features per (distribuidora_id, ct_id, hora).

Every feature of a row is computed only with data that could be known before
hora: hourly counts and aggregates are stored at the end of their bucket and
the windows only look back. The time of a TedisNet row is its arrival at the
SCADA server (known_time), not its field time; changes that arrived more than
tiempo.retraso_max_llegada_h late are left out of the event counts (states
read again after a reconnection, old history loaded late). The upstream bays
of a CT (map_aguas_arriba) are used only after the first cut that revealed
them, and a Calser interruption only after it would have been loaded.
tests/03_gold checks it by removing everything known after an hour, rebuilding
the map and recomputing: the rows up to that hour must not change.

Blocks (the prefix of the column says which one):

- med_*: measurement series of the CT element (LOCAL scope of map_tag_ct):
  mean, max, min, standard deviation, samples and share of stale values of
  the last hour, imbalance between tags (phases) and 24 h windows and lags.
- medaa_*: the same for the bays that feed the CT (map_aguas_arriba), which
  is where the feeder currents are. The only measurements a CT without RTU
  can have. aa_n_posiciones: how many of those bays were known at hora.
- ev_*, evred_*, evaa_*: value changes of the precursor signal families
  (earth fault, phase fault, fault passage, trips, reclosures, loss of
  voltage, communication...) in the CT, in its network group and in the
  bays that feed it, counted over windows of hours.
- ev_alarmas_* / ev_avisos_*: events the SCADA raised as alarm or warning in
  the CT (severity of LibEventLevels).
- calidad_* and corte_error_comm_*: readings the SCADA marked as not real
  (communication failure and the like) and communication errors of the cut
  detection on the transformer.
- scada_*: Off events and microcuts of the transformer and of its group in
  the SCADA (fact_cortes_scada).
- hist_*: history of the CT in Calser and in the SCADA over 30, 90 and 365
  days. A Calser interruption only counts once it would have been loaded
  (conocido_ts), so the feature is what serving would see.
- Calendar (hora_del_dia, dia_semana, mes, festivo, vispera) and static data
  of the CT (ct_*).
- scada_activo: the SCADA history has samples in the last hour for the
  distribuidora. Without it, an hour with no events cannot be told apart
  from an hour with no data.

The job runs by batches of months over a grid with lookback hours in front,
so the windows of the first hours of a batch are complete, and writes the
months of the batch with replaceWhere. Every column is described in
l3_gold.feature_metadata.
"""

from datetime import timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from gold_common import (
    DQCollector,
    FeatureRegistry,
    arrival_delay_s,
    batches,
    bucket_hour,
    calendar_days,
    drop_table,
    get_spark,
    gold_table,
    hour_grid,
    known_time,
    load_params,
    logger,
    month_range_condition,
    optional_table,
    parse_args,
    require_table,
    silver_table,
    ts_lit,
    write_table,
)


TARGET_TABLE = gold_table("features_ct_hora")
METADATA_TABLE = gold_table("feature_metadata")

KEYS = ["distribuidora_id", "ct_id", "hora"]

# Measurement families turned into features, by scope. The families of the
# configuration that are not listed here stay in agg_medida_hora.
LOCAL_MEASURES = ["intensidad", "intensidad_neutro", "tension", "potencia_activa",
                  "potencia_reactiva", "factor_potencia", "temperatura"]
UPSTREAM_MEASURES = ["intensidad", "intensidad_neutro", "tension", "potencia_activa"]

LOCAL_STATS = ["media", "max", "min", "std", "n_muestras", "frac_rancio"]
UPSTREAM_STATS = ["media", "max", "std", "n_muestras"]

# Columns that must never be features (leakage or keys of the label).
# n_posiciones_aguas_arriba counts the bays of the whole history, future cuts
# included: aa_n_posiciones is its version known at hora.
FORBIDDEN = {"fecha_alta", "ts", "conocido_ts", "conocido_fin_ts", "inicio_ts", "fin_ts",
             "inicio_conocido_ts", "fin_conocido_ts", "primer_conocido_ts", "en_corte", "ct_vigente",
             "n_posiciones_aguas_arriba"}

# Delay of arrival accepted by default for a change to count as a signal.
DEFAULT_MAX_DELAY_H = 24


def lookback_hours(params: dict) -> int:
    feats = params["features"]
    return max(feats["ventanas_local_h"] + feats["ventanas_red_h"] + [25])


def in_range(column: str, lo, hi):
    return (F.col(column) >= ts_lit(lo)) & (F.col(column) < ts_lit(hi))


def zero_fill(df: DataFrame, columns: list) -> DataFrame:
    return df.fillna(0, subset=columns)


# ---------------------------------------------------------------------------
# Measurements
# ---------------------------------------------------------------------------

def measure_stats(agg: DataFrame, keys: list, families: list, stats: list,
                  imbalance: list, prefix: str) -> DataFrame:
    """
    From the sums of agg_medida_hora to one row per key and hour with a
    column per family and statistic.
    """
    df = (
        agg.where(F.col("familia").isin(families))
        .groupBy(*keys, "familia", "hora")
        .agg(
            F.sum("n_muestras").alias("n_muestras"),
            F.sum("n_tags").alias("n_tags"),
            F.sum("suma").alias("suma"),
            F.sum("suma2").alias("suma2"),
            F.max("max").alias("max"),
            F.min("min").alias("min"),
            F.sum("n_rancio").alias("n_rancio"),
            F.max("media_tag_max").alias("media_tag_max"),
            F.min("media_tag_min").alias("media_tag_min"),
        )
        .withColumn("media", F.col("suma") / F.col("n_muestras"))
        .withColumn(
            "std",
            F.sqrt(F.greatest(F.col("suma2") / F.col("n_muestras") - F.col("media") * F.col("media"), F.lit(0.0))),
        )
        .withColumn("frac_rancio", F.col("n_rancio") / F.col("n_muestras"))
        .withColumn(
            "desequilibrio",
            F.when(
                (F.col("n_tags") >= 2) & (F.abs(F.col("media")) > F.lit(1e-9)),
                (F.col("media_tag_max") - F.col("media_tag_min")) / F.abs(F.col("media")),
            ),
        )
    )

    aggs = [F.first(stat).alias(stat) for stat in stats]

    if imbalance:
        aggs.append(F.first("desequilibrio").alias("desequilibrio"))

    wide = df.groupBy(*keys, "hora").pivot("familia", families).agg(*aggs)

    renamed = []

    for family in families:
        for stat in stats:
            renamed.append(F.col(f"{family}_{stat}").alias(f"{prefix}_{family}_{stat}"))

        if family in imbalance:
            renamed.append(F.col(f"{family}_desequilibrio").alias(f"{prefix}_{family}_desequilibrio"))

    return wide.select(*keys, "hora", *renamed)


def register_measures(registry: FeatureRegistry, prefix: str, scope: str, families, stats, imbalance):
    descriptions = {
        "media": "mean of the samples of the last hour",
        "max": "maximum sample of the last hour",
        "min": "minimum sample of the last hour",
        "std": "standard deviation of the samples of the last hour",
        "n_muestras": "number of valid samples in the last hour",
        "frac_rancio": "share of samples whose held value is more than an hour old",
    }

    for family in families:
        for stat in stats:
            registry.add(f"{prefix}_{family}_{stat}", "medidas", descriptions[stat],
                         ambito=scope, familia=family, ventana="1h", fuente="f_tag_interval_value")

        if family in imbalance:
            registry.add(f"{prefix}_{family}_desequilibrio", "medidas",
                         "(max - min) of the mean of each tag over the mean of the family, last hour",
                         ambito=scope, familia=family, ventana="1h", fuente="f_tag_interval_value")


def add_measure_windows(df: DataFrame, registry: FeatureRegistry, prefix: str, scope: str,
                        families: list, with_max: bool) -> DataFrame:
    ordered = Window.partitionBy("distribuidora_id", "ct_id").orderBy("hora")
    last_24 = ordered.rowsBetween(-23, 0)

    columns = []

    for family in families:
        mean = f"{prefix}_{family}_media"

        columns.append(F.avg(mean).over(last_24).alias(
            registry.add(f"{prefix}_{family}_media_24h", "medidas", "mean of the hourly means of the last 24 h",
                         ambito=scope, familia=family, ventana="24h", fuente="f_tag_interval_value")))

        if with_max:
            columns.append(F.max(f"{prefix}_{family}_max").over(last_24).alias(
                registry.add(f"{prefix}_{family}_max_24h", "medidas", "maximum sample of the last 24 h",
                             ambito=scope, familia=family, ventana="24h", fuente="f_tag_interval_value")))

        columns.append((F.col(mean) - F.lag(mean, 1).over(ordered)).alias(
            registry.add(f"{prefix}_{family}_delta_1h", "medidas", "hourly mean minus the one of the hour before",
                         ambito=scope, familia=family, ventana="1h", fuente="f_tag_interval_value")))

        columns.append((F.col(mean) - F.lag(mean, 24).over(ordered)).alias(
            registry.add(f"{prefix}_{family}_delta_24h", "medidas",
                         "hourly mean minus the one of the same hour the day before",
                         ambito=scope, familia=family, ventana="24h", fuente="f_tag_interval_value")))

    return df.select("*", *columns)


# ---------------------------------------------------------------------------
# Counts of events
# ---------------------------------------------------------------------------

def hourly_counts(df: DataFrame, keys: list, families: list, family_col: str = "familia_evento") -> DataFrame:
    return (
        df.where(F.col(family_col).isin(families))
        .groupBy(*keys, "hora")
        .pivot(family_col, families)
        .agg(F.count(F.lit(1)))
    )


def rolling_sums(df: DataFrame, registry: FeatureRegistry, columns: dict, windows: list,
                 bloque: str, scope: str, fuente: str) -> DataFrame:
    """
    columns: {hourly count column: (feature prefix, family, description)}.
    Adds the sum over the last w hours (hours [hora - w, hora)) and drops the
    hourly columns.
    """
    ordered = Window.partitionBy("distribuidora_id", "ct_id").orderBy("hora")

    exprs = []

    for column, (prefix, family, description) in columns.items():
        for w in windows:
            name = registry.add(f"{prefix}_{w}h", bloque, f"{description}, last {w} h",
                                ambito=scope, familia=family, ventana=f"{w}h", fuente=fuente)
            exprs.append(F.sum(column).over(ordered.rowsBetween(-(w - 1), 0)).alias(name))

    return df.select("*", *exprs).drop(*columns.keys())


# ---------------------------------------------------------------------------
# History over long windows
# ---------------------------------------------------------------------------

def calser_history(grid: DataFrame, events: DataFrame, registry: FeatureRegistry, params: dict) -> DataFrame:
    days = params["features"]["ventanas_historico_d"]
    longest = max(days)

    joined = (
        grid.select(*KEYS)
        .join(events, ["distribuidora_id", "ct_id"])
        .where(F.col("conocido_ts") <= F.col("hora"))
        .where(F.col("inicio_ts") >= F.col("hora") - F.expr(f"INTERVAL {longest} DAYS"))
    )

    aggs = []

    for d in days:
        aggs.append(F.sum((F.col("inicio_ts") >= F.col("hora") - F.expr(f"INTERVAL {d} DAYS")).cast("int")).alias(
            registry.add(f"hist_interr_{d}d", "historico",
                         f"principal interruptions of the CT known at hora, last {d} days",
                         ambito="CT", ventana=f"{d}d", fuente="fact_interrupciones_mt")))

    aggs += [
        F.sum(F.coalesce(F.col("es_sistemico"), F.lit(False)).cast("int")).alias(
            registry.add(f"hist_interr_sistemicas_{longest}d", "historico",
                         f"systemic interruptions (more than one CT in the incident), last {longest} days",
                         ambito="CT", ventana=f"{longest}d", fuente="fact_interrupciones_mt")),
        # The duration is final only once every record of the event is
        # loaded and the event has ended (conocido_fin_ts).
        F.avg(F.when(F.col("conocido_fin_ts") <= F.col("hora"), F.col("duracion_winsor_s"))).alias(
            registry.add(f"hist_duracion_media_{longest}d", "historico",
                         f"mean winsorized duration of the interruptions of the last {longest} days whose "
                         "duration is known at hora (s)",
                         ambito="CT", ventana=f"{longest}d", fuente="fact_interrupciones_mt")),
        ((F.unix_timestamp(F.max("hora")) - F.unix_timestamp(F.max("inicio_ts"))) / F.lit(86400.0)).alias(
            registry.add("hist_dias_desde_ultima", "historico",
                         f"days since the last known interruption (null if none in {longest} days)",
                         ambito="CT", ventana=f"{longest}d", fuente="fact_interrupciones_mt")),
    ]

    return joined.groupBy(*KEYS).agg(*aggs)


def scada_history(grid: DataFrame, cuts: DataFrame, registry: FeatureRegistry, params: dict) -> DataFrame:
    days = params["features"]["ventanas_historico_d"]
    longest = max(days)
    micro = int(params["scada"]["microcorte_max_s"])

    # Offs that had reached the SCADA before hora.
    joined = (
        grid.select(*KEYS)
        .join(cuts, ["distribuidora_id", "ct_id"])
        .where(F.col("inicio_conocido_ts") < F.col("hora"))
        .where(F.col("inicio_ts") >= F.col("hora") - F.expr(f"INTERVAL {longest} DAYS"))
    )

    # At hora a cut is known to be longer than a microcut only once its Off
    # has arrived and microcorte_max_s have passed without an On. Using the
    # final duration of the episode would let the future in.
    known_cut = (
        (F.greatest(F.col("inicio_conocido_ts"), F.col("inicio_ts") + F.expr(f"INTERVAL {micro} SECONDS"))
         <= F.col("hora"))
        & ~F.col("es_microcorte")
    )

    aggs = []

    for d in days:
        recent = F.col("inicio_ts") >= F.col("hora") - F.expr(f"INTERVAL {d} DAYS")
        aggs.append(F.sum((recent & known_cut).cast("int")).alias(
            registry.add(f"hist_scada_cortes_{d}d", "historico",
                         f"cuts of the transformer in the SCADA longer than 180 s, not manoeuvres, last {d} days",
                         ambito="CT", ventana=f"{d}d", fuente="fact_cortes_scada")))
        # A microcut is known when its On arrives.
        aggs.append(F.sum((recent & F.col("es_microcorte") & (F.col("fin_conocido_ts") < F.col("hora"))).cast("int")).alias(
            registry.add(f"hist_scada_microcortes_{d}d", "historico",
                         f"microcuts (Off -> On in 180 s or less) of the transformer, last {d} days",
                         ambito="CT", ventana=f"{d}d", fuente="fact_cortes_scada")))

    aggs.append(
        ((F.unix_timestamp(F.max("hora")) - F.unix_timestamp(F.max("inicio_ts"))) / F.lit(86400.0)).alias(
            registry.add("hist_scada_dias_desde_ultimo", "historico",
                         f"days since the last Off of the transformer (null if none in {longest} days)",
                         ambito="CT", ventana=f"{longest}d", fuente="fact_cortes_scada"))
    )

    return joined.groupBy(*KEYS).agg(*aggs)


def municipality_history(spark, cts: DataFrame, all_cts: DataFrame, events: DataFrame, lo, hi,
                         registry: FeatureRegistry) -> DataFrame:
    """
    Interruptions of every CT of the municipality known at hora, last 30 days.
    The events of the CTs outside the study count too (all_cts places them
    in their municipality). Computed on a municipality x hour grid and joined
    to the CTs of the study.
    """
    municipalities = cts.select("distribuidora_id", "municipio_id").where(F.col("municipio_id").isNotNull()).distinct()

    grid = hour_grid(spark, municipalities, lo, hi)

    per_mun = events.join(
        all_cts.select("distribuidora_id", "ct_id", "municipio_id"), ["distribuidora_id", "ct_id"]
    ).select("distribuidora_id", "municipio_id", "inicio_ts", "conocido_ts")

    name = registry.add("hist_municipio_interr_30d", "historico",
                        "principal interruptions of all the CTs of the municipality known at hora, last 30 days",
                        ambito="MUNICIPIO", ventana="30d", fuente="fact_interrupciones_mt")

    counts = (
        grid.join(per_mun, ["distribuidora_id", "municipio_id"])
        .where(F.col("conocido_ts") <= F.col("hora"))
        .where(F.col("inicio_ts") >= F.col("hora") - F.expr("INTERVAL 30 DAYS"))
        .groupBy("distribuidora_id", "municipio_id", "hora")
        .agg(F.count(F.lit(1)).alias(name))
    )

    return (
        grid.join(counts, ["distribuidora_id", "municipio_id", "hora"], "left")
        .fillna(0, subset=[name])
        .join(cts.select("distribuidora_id", "ct_id", "municipio_id"), ["distribuidora_id", "municipio_id"])
        .select(*KEYS, name)
    )


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

class Sources:
    def __init__(self, spark, params: dict):
        dim = spark.table(require_table(spark, gold_table("dim_ct")))

        self.dim = dim.where(F.col("en_estudio")).localCheckpoint(eager=True)
        self.all_cts = dim.select("distribuidora_id", "ct_id", "municipio_id").localCheckpoint(eager=True)
        self.tag_map = spark.table(require_table(spark, gold_table("map_tag_ct"))).localCheckpoint(eager=True)
        self.agg = optional_table(spark, gold_table("agg_medida_hora"))
        self.activity = optional_table(spark, gold_table("actividad_scada_hora"))
        self.changes = spark.table(require_table(spark, silver_table("f_tag_value_change")))
        self.events = optional_table(spark, silver_table("f_evento"))
        self.quality = optional_table(spark, silver_table("f_tag_quality_event"))
        self.cut_elements = optional_table(spark, silver_table("f_corte_elemento"))
        self.cuts = spark.table(require_table(spark, gold_table("fact_cortes_scada")))
        self.calser = (
            spark.table(require_table(spark, gold_table("fact_interrupciones_mt")))
            .where(F.col("variante") == F.lit("principal"))
            .select("distribuidora_id", "ct_id", "inicio_ts", "conocido_ts", "conocido_fin_ts",
                    "es_sistemico", "duracion_winsor_s")
            .localCheckpoint(eager=True)
        )

        upstream = spark.table(require_table(spark, gold_table("map_aguas_arriba")))

        if "primer_conocido_ts" not in upstream.columns:
            raise ValueError("map_aguas_arriba has no primer_conocido_ts: run job_gold_dim_ct again")

        # (transformer, bay, first moment the pair was known).
        self.upstream = (
            upstream.where(F.col("posicion_id").isNotNull())
            .groupBy("trafo_elemento_id", "posicion_id")
            .agg(F.min("primer_conocido_ts").alias("desde_ts"))
            .localCheckpoint(eager=True)
        )

        self.max_delay_h = params["tiempo"].get("retraso_max_llegada_h", DEFAULT_MAX_DELAY_H)


def changes_in(df: DataFrame, params: dict, lo, hi, max_delay_h=None) -> DataFrame:
    """
    Rows of a TedisNet fact placed at the moment they became known
    (known_time, on the Calser clock), with hora = end of their bucket and
    restricted to the hours [lo, hi).

    With max_delay_h, the rows that reached the server more than that after
    their field time are left out: states read again after a reconnection or
    old history loaded late are not a signal of what is happening. Quality
    events and cut events keep them, their arrival is the signal.
    """
    # Coarse filter on the raw columns first (the known time is never earlier
    # than the field time), with a day of margin for the clock correction.
    out = df.where(F.col("ts") < ts_lit(hi + timedelta(days=1)))

    if max_delay_h is not None:
        out = out.where(F.col("ts") >= ts_lit(lo - timedelta(hours=1 + max_delay_h, days=1)))
        out = out.where(F.coalesce(arrival_delay_s(df) <= F.lit(int(max_delay_h * 3600)), F.lit(True)))

    return (
        out.withColumn("ts", known_time(df, params))
        .where((F.col("ts") >= ts_lit(lo - timedelta(hours=1))) & (F.col("ts") < ts_lit(hi)))
        .withColumn("hora", bucket_hour("ts"))
        .where(in_range("hora", lo, hi))
    )


def upstream_members(tag_map: DataFrame, upstream: DataFrame) -> DataFrame:
    """
    (tag_id, trafo_elemento_id, familia_evento, desde_ts) for the tags of the
    bays that feed each transformer; desde_ts is when the first of those bays
    became known for it.
    """
    siblings = tag_map.select("tag_id", F.col("posicion_id").alias("p"), "familia_evento")
    itself = tag_map.select("tag_id", F.col("elemento_id").alias("p"), "familia_evento")

    return (
        siblings.unionByName(itself)
        .where(F.col("p").isNotNull() & F.col("familia_evento").isNotNull())
        .join(upstream.withColumnRenamed("posicion_id", "p"), "p")
        .groupBy("tag_id", "trafo_elemento_id", "familia_evento")
        .agg(F.min("desde_ts").alias("desde_ts"))
    )


def build_batch(spark, src: Sources, params: dict, lo, hi, registry: FeatureRegistry) -> DataFrame:
    feats = params["features"]
    lookback = lookback_hours(params)
    ext_lo = lo - timedelta(hours=lookback)

    local_windows = feats["ventanas_local_h"]
    red_windows = feats["ventanas_red_h"]
    event_families = list(feats["familias_evento"])
    red_families = [f for f in feats["familias_red"] if f in event_families]
    measure_families = list(feats["familias_medida"])
    local_measures = [f for f in LOCAL_MEASURES if f in measure_families]
    upstream_measures = [f for f in UPSTREAM_MEASURES if f in measure_families]
    imbalance = [f for f in feats["familias_desequilibrio"] if f in local_measures]

    entities = src.dim.select(
        "distribuidora_id", "ct_id", "trafo_elemento_id", "anchor_id", "grupo_red_id",
    )

    grid = hour_grid(spark, entities, ext_lo, hi)

    # Hourly count columns whose source table does not exist: they stay null
    # (unknown), not 0 (nothing happened).
    missing = set()

    # --- measurements ------------------------------------------------------
    if src.agg is not None:
        agg = src.agg.where(in_range("hora", ext_lo, hi))

        local = measure_stats(
            agg.where(F.col("ambito") == F.lit("LOCAL")).withColumnRenamed("clave_id", "anchor_id"),
            ["anchor_id"], local_measures, LOCAL_STATS, imbalance, "med",
        )
        grid = grid.join(local, ["anchor_id", "hora"], "left")

        # A bay feeds a CT from the first cut that showed it, not before.
        remote = measure_stats(
            agg.where(F.col("ambito") == F.lit("AGUAS_ARRIBA"))
            .withColumnRenamed("clave_id", "posicion_id")
            .join(F.broadcast(src.upstream), "posicion_id")
            .where(F.col("desde_ts") < F.col("hora")),
            ["trafo_elemento_id"], upstream_measures, UPSTREAM_STATS, [], "medaa",
        )
        grid = grid.join(remote, ["trafo_elemento_id", "hora"], "left")
    else:
        for family in local_measures:
            for stat in LOCAL_STATS:
                grid = grid.withColumn(f"med_{family}_{stat}", F.lit(None).cast("double"))
            if family in imbalance:
                grid = grid.withColumn(f"med_{family}_desequilibrio", F.lit(None).cast("double"))
        for family in upstream_measures:
            for stat in UPSTREAM_STATS:
                grid = grid.withColumn(f"medaa_{family}_{stat}", F.lit(None).cast("double"))

    register_measures(registry, "med", "CT", local_measures, LOCAL_STATS, imbalance)
    register_measures(registry, "medaa", "AGUAS_ARRIBA", upstream_measures, UPSTREAM_STATS, [])

    # --- value changes of the signal families -------------------------------
    recent_changes = changes_in(src.changes, params, ext_lo, hi, src.max_delay_h)

    changes = recent_changes.join(
        F.broadcast(src.tag_map.select(
            "tag_id", "anchor_id", "grupo_red_id", "familia_evento",
        ).where(F.col("familia_evento").isNotNull())),
        "tag_id",
    )

    local_counts = hourly_counts(changes.where(F.col("anchor_id").isNotNull()), ["anchor_id"], event_families)
    local_counts = local_counts.select(
        "anchor_id", "hora", *[F.col(f).alias(f"_ev_{f}") for f in event_families]
    )
    grid = grid.join(local_counts, ["anchor_id", "hora"], "left")

    red_counts = hourly_counts(changes.where(F.col("grupo_red_id").isNotNull()), ["grupo_red_id"], red_families)
    red_counts = red_counts.select(
        "grupo_red_id", "hora", *[F.col(f).alias(f"_evred_{f}") for f in red_families]
    )
    grid = grid.join(red_counts, ["grupo_red_id", "hora"], "left")

    hourly = {f"_ev_{f}": (f"ev_{f}", f, f"value changes of {f} signals in the CT") for f in event_families}
    hourly_red = {f"_evred_{f}": (f"evred_{f}", f, f"value changes of {f} signals in the network group of the CT")
                  for f in red_families}

    members = upstream_members(src.tag_map, src.upstream)
    aa_counts = hourly_counts(
        recent_changes.select("tag_id", "hora")
        .join(F.broadcast(members), "tag_id")
        .where(F.col("desde_ts") < F.col("hora")),
        ["trafo_elemento_id"], red_families,
    )
    aa_counts = aa_counts.select(
        "trafo_elemento_id", "hora", *[F.col(f).alias(f"_evaa_{f}") for f in red_families]
    )
    grid = grid.join(aa_counts, ["trafo_elemento_id", "hora"], "left")

    hourly_aa = {f"_evaa_{f}": (f"evaa_{f}", f, f"value changes of {f} signals in the bays that feed the CT")
                 for f in red_families}

    # --- events raised by the SCADA ----------------------------------------
    if src.events is not None:
        severity = F.col("nivel_severidad") if "nivel_severidad" in src.events.columns else F.col("nivel_evento_id")
        events = (
            changes_in(src.events.withColumn("_sev", severity), params, ext_lo, hi, src.max_delay_h)
            .join(F.broadcast(src.tag_map.select("tag_id", "anchor_id").where(F.col("anchor_id").isNotNull())), "tag_id")
            .groupBy("anchor_id", "hora")
            .agg(
                F.sum((F.col("_sev") >= 3).cast("int")).alias("_alarmas"),
                F.sum((F.col("_sev") == 2).cast("int")).alias("_avisos"),
            )
        )
        grid = grid.join(events, ["anchor_id", "hora"], "left")
    else:
        grid = grid.withColumn("_alarmas", F.lit(None).cast("long")).withColumn("_avisos", F.lit(None).cast("long"))
        missing.update(["_alarmas", "_avisos"])

    hourly_sev = {
        "_alarmas": ("ev_alarmas", None, "events of the CT raised as alarm by the SCADA"),
        "_avisos": ("ev_avisos", None, "events of the CT raised as warning by the SCADA"),
    }

    # --- quality and communications -----------------------------------------
    # No limit of delay here: the moment the server marks a reading as not
    # real is the signal, whatever the field time of the value.
    if src.quality is not None:
        quality = (
            changes_in(src.quality, params, ext_lo, hi)
            .join(F.broadcast(src.tag_map.select("tag_id", "anchor_id").where(F.col("anchor_id").isNotNull())), "tag_id")
            .groupBy("anchor_id", "hora")
            .agg(
                F.sum((F.col("calidad_detalle_id") == 5).cast("int")).alias("_cal_comm"),
                F.sum((F.col("calidad_detalle_id") != 5).cast("int")).alias("_cal_no_real"),
            )
        )
        grid = grid.join(quality, ["anchor_id", "hora"], "left")
    else:
        grid = grid.withColumn("_cal_comm", F.lit(None).cast("long")).withColumn("_cal_no_real", F.lit(None).cast("long"))
        missing.update(["_cal_comm", "_cal_no_real"])

    if src.cut_elements is not None:
        comm = (
            changes_in(src.cut_elements.where(F.col("es_error_comm")), params, ext_lo, hi)
            .groupBy(F.col("elemento_id").alias("trafo_elemento_id"), "hora")
            .agg(F.count(F.lit(1)).alias("_corte_comm"))
        )
        grid = grid.join(comm, ["trafo_elemento_id", "hora"], "left")
    else:
        grid = grid.withColumn("_corte_comm", F.lit(None).cast("long"))
        missing.add("_corte_comm")

    hourly_quality = {
        "_cal_comm": ("calidad_fallo_comm", None, "readings of the CT marked as communication failure"),
        "_cal_no_real": ("calidad_no_real", None, "readings of the CT marked as not real (not connected, "
                         "device or sensor failure, last known value...)"),
        "_corte_comm": ("corte_error_comm", None, "communication errors of the cut detection on the transformer"),
    }

    # --- cuts of the SCADA ---------------------------------------------------
    # Counted when the Off (or, for a microcut, the On) reached the SCADA.
    cuts = src.cuts.where(~F.col("es_maniobra"))

    starts = (
        cuts.withColumn("hora", bucket_hour("inicio_conocido_ts"))
        .where(in_range("hora", ext_lo, hi))
        .groupBy("distribuidora_id", "ct_id", "hora")
        .agg(F.count(F.lit(1)).alias("_scada_cortes"))
    )
    micro = (
        cuts.where(F.col("es_microcorte"))
        .withColumn("hora", bucket_hour("fin_conocido_ts"))
        .where(in_range("hora", ext_lo, hi))
        .groupBy("distribuidora_id", "ct_id", "hora")
        .agg(F.count(F.lit(1)).alias("_scada_micro"))
    )
    group_starts = (
        cuts.withColumn("hora", bucket_hour("inicio_conocido_ts"))
        .where(in_range("hora", ext_lo, hi))
        .join(src.dim.select("distribuidora_id", "ct_id", "grupo_red_id"), ["distribuidora_id", "ct_id"])
        .where(F.col("grupo_red_id").isNotNull())
        .groupBy("grupo_red_id", "hora")
        .agg(F.count(F.lit(1)).alias("_scada_grupo"))
    )

    grid = (
        grid.join(starts, KEYS, "left")
        .join(micro, KEYS, "left")
        .join(group_starts, ["grupo_red_id", "hora"], "left")
    )

    hourly_scada = {
        "_scada_cortes": ("scada_cortes", None, "Off events of the transformer in the SCADA (no manoeuvres)"),
        "_scada_micro": ("scada_microcortes", None, "microcuts of the transformer (known when the On arrives)"),
    }
    hourly_scada_group = {
        "_scada_grupo": ("scada_cortes_grupo", None, "Off events of the transformers of the same network group"),
    }

    # --- activity of the SCADA history ---------------------------------------
    if src.activity is not None:
        activity = (
            src.activity.where(in_range("hora", ext_lo, hi))
            .groupBy("distribuidora_id", "hora")
            .agg(F.sum("n_muestras_serie").alias("_n_muestras_distrib"))
        )
        grid = grid.join(activity, ["distribuidora_id", "hora"], "left")
    else:
        grid = grid.withColumn("_n_muestras_distrib", F.lit(None).cast("long"))

    # --- windows over the extended grid -------------------------------------
    count_columns = list(hourly) + list(hourly_red) + list(hourly_aa) + list(hourly_sev) \
        + list(hourly_quality) + list(hourly_scada) + list(hourly_scada_group)

    grid = zero_fill(grid, [c for c in count_columns if c not in missing]).localCheckpoint(eager=True)

    local_measure_cols = [f"med_{f}_n_muestras" for f in local_measures]

    grid = grid.withColumn(
        registry.add("ct_con_datos_1h", "actividad",
                     "the CT element had at least one sample or one value change in the last hour",
                     ambito="CT", ventana="1h", fuente="agg_medida_hora, f_tag_value_change", tipo="int"),
        (
            (F.coalesce(*[F.col(c) for c in local_measure_cols], F.lit(0)) > 0)
            | (sum(F.col(c) for c in hourly) > 0)
        ).cast("int") if local_measure_cols else (sum(F.col(c) for c in hourly) > 0).cast("int"),
    )

    grid = grid.withColumn(
        registry.add("scada_activo", "actividad",
                     "the SCADA history of the distribuidora has series samples in the last hour",
                     ambito="DISTRIBUIDORA", ventana="1h", fuente="actividad_scada_hora", tipo="int"),
        (F.coalesce(F.col("_n_muestras_distrib"), F.lit(0)) > 0).cast("int"),
    ).drop("_n_muestras_distrib")

    grid = add_measure_windows(grid, registry, "med", "CT", local_measures, with_max=True)
    grid = add_measure_windows(grid, registry, "medaa", "AGUAS_ARRIBA", upstream_measures, with_max=False)

    grid = rolling_sums(grid, registry, hourly, local_windows, "eventos", "CT", "f_tag_value_change")
    grid = rolling_sums(grid, registry, hourly_red, red_windows, "eventos", "GRUPO_RED", "f_tag_value_change")
    grid = rolling_sums(grid, registry, hourly_aa, red_windows, "eventos", "AGUAS_ARRIBA", "f_tag_value_change")
    grid = rolling_sums(grid, registry, hourly_sev, local_windows, "eventos", "CT", "f_evento")
    grid = rolling_sums(grid, registry, hourly_quality, [24, 168], "calidad", "CT",
                        "f_tag_quality_event, f_corte_elemento")
    grid = rolling_sums(grid, registry, hourly_scada, [24, 168], "cortes_scada", "CT", "fact_cortes_scada")
    grid = rolling_sums(grid, registry, hourly_scada_group, red_windows, "cortes_scada", "GRUPO_RED",
                        "fact_cortes_scada")

    # Back to the hours of the batch.
    batch_rows = grid.where(in_range("hora", lo, hi))

    # Bays known to feed the CT at hora (the static count of dim_ct would
    # include the bays revealed by future cuts).
    bays_name = registry.add("aa_n_posiciones", "topologia",
                             "bays whose switches had cut the CT before hora (map_aguas_arriba as known at hora)",
                             ambito="AGUAS_ARRIBA", ventana="historico", fuente="map_aguas_arriba", tipo="int")

    known_bays = (
        batch_rows.select("trafo_elemento_id", "hora")
        .join(F.broadcast(src.upstream), "trafo_elemento_id")
        .where(F.col("desde_ts") < F.col("hora"))
        .groupBy("trafo_elemento_id", "hora")
        .agg(F.countDistinct("posicion_id").alias(bays_name))
    )

    out = (
        batch_rows.join(known_bays, ["trafo_elemento_id", "hora"], "left")
        .fillna(0, subset=[bays_name])
        .drop("trafo_elemento_id", "anchor_id", "grupo_red_id")
    )

    # --- history over long windows, calendar and static data -----------------
    batch_grid = hour_grid(spark, src.dim.select("distribuidora_id", "ct_id"), lo, hi)

    hist_calser = calser_history(batch_grid, src.calser, registry, params)
    hist_scada = scada_history(batch_grid, src.cuts.where(~F.col("es_maniobra")), registry, params)
    hist_mun = municipality_history(spark, src.dim, src.all_cts, src.calser, lo, hi, registry)

    count_hist = [c for c in hist_calser.columns if c.startswith("hist_interr_")] + \
        [c for c in hist_scada.columns if c.startswith(("hist_scada_cortes_", "hist_scada_microcortes_"))]

    out = (
        out.join(hist_calser, KEYS, "left")
        .join(hist_scada, KEYS, "left")
        .join(hist_mun, KEYS, "left")
    )
    out = out.fillna(0, subset=count_hist + ["hist_municipio_interr_30d"])

    calendar = calendar_days(spark, lo, hi, params)

    out = (
        out.withColumn("_fecha", F.to_date("hora"))
        .join(F.broadcast(calendar.withColumnRenamed("fecha", "_fecha")), "_fecha", "left")
        .drop("_fecha")
        .withColumn("hora_del_dia", F.hour("hora"))
        .withColumn("dia_semana", F.dayofweek("hora"))
        .withColumn("mes", F.month("hora"))
        .withColumn("es_festivo", F.col("es_festivo").cast("int"))
        .withColumn("es_vispera_festivo", F.col("es_vispera_festivo").cast("int"))
        .withColumn("es_fin_de_semana", F.col("es_fin_de_semana").cast("int"))
    )

    for name, description in [
        ("hora_del_dia", "hour of the prediction instant (0 - 23)"),
        ("dia_semana", "day of the week (1 Sunday - 7 Saturday, Spark dayofweek)"),
        ("mes", "month"),
        ("es_festivo", "national or Extremadura holiday (computed calendar)"),
        ("es_vispera_festivo", "the next day is a holiday"),
        ("es_fin_de_semana", "Saturday or Sunday"),
    ]:
        registry.add(name, "calendario", description, ventana="0h", fuente="calendar_days", tipo="int")

    static = src.dim.select(
        "distribuidora_id",
        "ct_id",
        F.col("potencia_kva").alias("ct_potencia_kva"),
        F.col("potencia_imputada").cast("int").alias("ct_potencia_imputada"),
        F.col("num_abonados").cast("double").alias("ct_num_abonados"),
        F.col("n_salidas").cast("double").alias("ct_n_salidas"),
        F.col("tipo_zona_codigo").alias("ct_tipo_zona"),
        F.col("n_trafos_ct").cast("double").alias("ct_n_trafos"),
        F.col("n_ct_grupo").cast("double").alias("ct_n_ct_grupo"),
        F.col("tiene_medidas").cast("int").alias("ct_tiene_medidas"),
        F.col("n_tags_medida_local").cast("double").alias("ct_n_tags_medida"),
        F.col("tension_primaria_kv").alias("ct_tension_primaria_kv"),
        F.col("latitud").alias("ct_latitud"),
        F.col("longitud").alias("ct_longitud"),
    )

    for name, description in [
        ("ct_potencia_kva", "power of the CT, imputed as Calser does when the installed power is 0"),
        ("ct_potencia_imputada", "the power does not come from CT_POTENCIA_INSTAL"),
        ("ct_num_abonados", "customers of the CT (Calser)"),
        ("ct_n_salidas", "LV outputs of the CT (Calser)"),
        ("ct_tipo_zona", "zone type of the municipality: 1 urban, 2 semiurban, 3 rural concentrated, 4 rural dispersed"),
        ("ct_n_trafos", "transformers of the same CT element in TedisNet"),
        ("ct_n_ct_grupo", "CTs of the study in the same network group"),
        ("ct_tiene_medidas", "the CT element has at least one measurement series"),
        ("ct_n_tags_medida", "measurement tags with series in the CT element"),
        ("ct_tension_primaria_kv", "rated primary voltage of the transformer (TedisNet)"),
        ("ct_latitud", "latitude of the municipality"),
        ("ct_longitud", "longitude of the municipality"),
    ]:
        registry.add(name, "estatica", description, ambito="CT", fuente="dim_ct")

    return (
        out.join(static, ["distribuidora_id", "ct_id"], "left")
        .withColumn("fecha_mes", F.trunc("hora", "month").cast("date"))
        .withColumn("audit_loaded_at", F.current_timestamp())
    )


def main():
    args = parse_args()
    params = load_params(args)
    spark = get_spark("job-gold-features_ct_hora")
    dq = DQCollector(spark, "job_gold_features_ct_hora", args.run_id)

    if args.rebuild:
        drop_table(spark, TARGET_TABLE)

    src = Sources(spark, params)
    registry = FeatureRegistry()

    for lo, hi in batches(args, params, params["lotes"]["features_meses"]):
        logger.info("Features %s - %s", lo, hi)

        out = build_batch(spark, src, params, lo, hi, registry)

        leaked = FORBIDDEN.intersection(registry.names())

        if leaked:
            raise ValueError(f"Columns that can not be features: {sorted(leaked)}")

        write_table(
            out,
            TARGET_TABLE,
            partition_by=["fecha_mes"],
            replace_where=month_range_condition("fecha_mes", lo, hi),
        )

        written = spark.table(TARGET_TABLE).where(in_range("hora", lo, hi))
        stats = written.agg(
            F.count(F.lit(1)).alias("filas"),
            F.sum("scada_activo").alias("activas"),
            F.sum("ct_con_datos_1h").alias("con_datos"),
        ).first()

        ambito = f"{lo:%Y-%m}"
        dq.add("features_ct_hora", "filas", stats["filas"], ambito=ambito)
        dq.add("features_ct_hora", "filas_scada_activo", stats["activas"], stats["filas"], minimo_pct=95.0,
               ambito=ambito)
        dq.add("features_ct_hora", "filas_ct_con_datos", stats["con_datos"], stats["filas"], ambito=ambito)
        dq.flush()

    if registry.names():
        write_table(registry.dataframe(spark).withColumn("audit_loaded_at", F.current_timestamp()), METADATA_TABLE)

    logger.info("Gold completed: %s (%s features)", TARGET_TABLE, len(registry.names()))


if __name__ == "__main__":
    main()
