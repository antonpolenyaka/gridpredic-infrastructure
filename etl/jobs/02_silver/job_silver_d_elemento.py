"""
Builds the element dimension of the lakehouse.

Every TedisNet element is resolved to the distribuidora it belongs to by
climbing SystemElements.ParentElementId until an element of type
DISTRIBUTOR_ELEMENT_TYPE_ID is reached. That element id is the same value
Calser stores in parametros_configuracion under the DistributorId code, so
the dimension is also the bridge between both systems: TedisNet keeps the
three distribuidoras in a single database and Calser keeps one database per
distribuidora.

Not every element of type 115 belongs to the TFM: TedisNet_EOSA also holds
the upstream utility the three distribuidoras hang from. Those roots have no
DistributorId in any Calser database, so they are identified by the absence of
a mapping and reported with their own status instead of being counted as one
of the distribuidoras under study.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    element_at,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    split,
    trim,
    when,
)


TARGET_TABLE = "l2_silver.d_elemento"

# LibElementTypes id of a distribuidora ("DIS"). The climb stops here and not
# on ParentElementId IS NULL: TedisNet_EOSA has roots of other types
# (100, 141, 163) that are not distribuidoras, and every element of type 115
# is already a root, so this condition is both sufficient and safe.
DISTRIBUTOR_ELEMENT_TYPE_ID = 115

# Safety bound for the climb. ParentElementId is not guaranteed to be acyclic,
# so the loop has to terminate on its own.
MAX_HIERARCHY_DEPTH = 40

# to_snake() in the Bronze jobs turns "TedisNet_EOSA" into "tedis_net_eosa".
# The second name is kept as a fallback in case the ingestion is renamed.
ELEMENTS_TABLE_CANDIDATES = [
    "l1_bronze.tedis_net_eosa_system_elements",
    "l1_bronze.tedisnet_eosa_system_elements",
]

# Calser keeps one database per distribuidora.
PARAMETROS_TABLES = {
    "Calser_EOSA": "l1_bronze.calser_eosa_parametros_configuracion",
    "Calser_Pitarch": "l1_bronze.calser_pitarch_parametros_configuracion",
    "Calser_ValleSantaAna": "l1_bronze.calser_valle_santa_ana_parametros_configuracion",
}

DISTRIBUTOR_PARAM_CODE = "DistributorId"

# SystemElements.Name holds the full hierarchical path of the element, not a
# short name: "/EODSLU/CASAS DEL MONTE/.../26011:TRA". Only the last segment
# carries the ":<type>" suffix, so comparing two paths means stripping it.
# The short name lives in its own column, ShortName.
PATH_SEPARATOR = "/"
TYPE_SUFFIX_PATTERN = ":[^:]*$"
TYPE_CODE_PATTERN = r":([^:/]+)$"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


def resolve_table(spark: SparkSession, candidates: list[str]) -> str:
    """
    Returns the first table of the list that exists in the catalog.
    """
    for candidate in candidates:
        if spark.catalog.tableExists(candidate):
            return candidate

    raise ValueError(
        f"None of the expected Bronze tables exists: {candidates}"
    )


def first_path_segment(path):
    """
    Returns the first segment of a hierarchical path, without the leading
    separator and without the type suffix a leaf segment carries. A
    distribuidora root is "/EODSLU:DIS" while its elements hang from
    "/EODSLU/...", so both only line up once the suffix is removed.
    """
    segment = element_at(
        split(
            regexp_replace(path, f"^{PATH_SEPARATOR}", ""),
            PATH_SEPARATOR,
        ),
        1,
    )

    return regexp_replace(segment, TYPE_SUFFIX_PATTERN, "")


def read_elements(spark: SparkSession, table: str) -> DataFrame:
    """
    Reads and normalises SystemElements. Each call issues an independent read
    on purpose: the dimension joins the table against itself several times and
    reusing the same dataframe makes those joins ambiguous.
    """
    ruta = trim(col("Name"))

    # Type code of the leaf segment ("DIS", "TRA", "SEC", "CEL", ...). It is
    # the textual counterpart of ElementTypeId and comes for free in the path.
    tipo_codigo = regexp_extract(ruta, TYPE_CODE_PATTERN, 1)

    return spark.table(table).select(
        col("Id").cast("bigint").alias("id"),
        trim(col("ShortName")).alias("nombre"),
        ruta.alias("ruta"),
        col("ElementTypeId").cast("int").alias("tipo_elemento_id"),
        when(tipo_codigo == lit(""), lit(None).cast("string"))
        .otherwise(tipo_codigo)
        .alias("tipo_codigo"),
        col("ParentElementId").cast("bigint").alias("parent_id"),
    )


def resolve_distributor(nodes: DataFrame, parents: DataFrame) -> DataFrame:
    """
    Climbs the element hierarchy and returns one row per element with the
    distribuidora it belongs to, the number of levels traversed and the
    outcome of the resolution.

    An element that cannot be attributed is kept with a null distribuidora and
    a status explaining why, so nothing is silently dropped or attributed to
    the wrong distribuidora.
    """
    lookup = parents.select(
        col("id").alias("padre_id"),
        col("tipo_elemento_id").alias("padre_tipo_elemento_id"),
        col("parent_id").alias("padre_parent_id"),
    ).cache()

    pending = nodes.select(
        col("id"),
        col("id").alias("node_id"),
        col("tipo_elemento_id").alias("node_tipo_elemento_id"),
        col("parent_id").alias("node_parent_id"),
        lit(0).alias("profundidad"),
    ).localCheckpoint(eager=True)

    parts = []
    depth = 0

    while True:
        parts.append(
            pending
            .where(col("node_tipo_elemento_id") == DISTRIBUTOR_ELEMENT_TYPE_ID)
            .select(
                col("id"),
                col("node_id").alias("distribuidora_id"),
                col("profundidad"),
                lit("RESUELTO").alias("estado_resolucion"),
            )
        )

        remaining = pending.where(
            col("node_tipo_elemento_id").isNull()
            | (col("node_tipo_elemento_id") != DISTRIBUTOR_ELEMENT_TYPE_ID)
        )

        # The climb reached a root that is not a distribuidora.
        parts.append(
            remaining
            .where(col("node_parent_id").isNull())
            .select(
                col("id"),
                lit(None).cast("bigint").alias("distribuidora_id"),
                col("profundidad"),
                lit("RAIZ_NO_DISTRIBUIDORA").alias("estado_resolucion"),
            )
        )

        climbing = remaining.where(col("node_parent_id").isNotNull())

        if climbing.isEmpty():
            break

        if depth >= MAX_HIERARCHY_DEPTH:
            logger.warning(
                "Maximum hierarchy depth reached at level %s",
                depth,
            )

            parts.append(
                climbing.select(
                    col("id"),
                    lit(None).cast("bigint").alias("distribuidora_id"),
                    col("profundidad"),
                    lit("PROFUNDIDAD_EXCEDIDA").alias("estado_resolucion"),
                )
            )

            break

        depth += 1

        climbed = (
            climbing
            .join(
                lookup,
                col("node_parent_id") == col("padre_id"),
                "left",
            )
            .select(
                col("id"),
                col("padre_id").alias("node_id"),
                col("padre_tipo_elemento_id").alias("node_tipo_elemento_id"),
                col("padre_parent_id").alias("node_parent_id"),
                lit(depth).alias("profundidad"),
            )
            .localCheckpoint(eager=True)
        )

        # ParentElementId pointing to an element that is not in SystemElements.
        parts.append(
            climbed
            .where(col("node_id").isNull())
            .select(
                col("id"),
                lit(None).cast("bigint").alias("distribuidora_id"),
                col("profundidad"),
                lit("PADRE_INEXISTENTE").alias("estado_resolucion"),
            )
        )

        pending = climbed.where(col("node_id").isNotNull())

    resolution = parts[0]

    for part in parts[1:]:
        resolution = resolution.unionByName(part)

    logger.info(
        "Hierarchy climbed up to level %s",
        depth,
    )

    return resolution.localCheckpoint(eager=True)


def read_distributor_map(spark: SparkSession) -> DataFrame:
    """
    Reads the DistributorId parameter of every Calser database and returns the
    mapping between the TedisNet distribuidora element and its Calser source.
    """
    mapping = None

    for source_database, table in PARAMETROS_TABLES.items():
        if not spark.catalog.tableExists(table):
            logger.warning(
                "Calser parameters table not found, its distribuidora will "
                "not be mapped: %s",
                table,
            )

            continue

        df = (
            spark.table(table)
            .where(
                trim(col("PARAM_CONFIG_CODIGO")) == lit(DISTRIBUTOR_PARAM_CODE)
            )
            .select(
                trim(col("PARAM_CONFIG_VALOR"))
                .cast("bigint")
                .alias("distribuidora_id"),
                lit(source_database).alias("source_database"),
            )
            .where(col("distribuidora_id").isNotNull())
            .distinct()
        )

        mapping = df if mapping is None else mapping.unionByName(df)

    if mapping is None:
        raise ValueError(
            "No Calser parameters table is available in Bronze, ingest "
            "parametros_configuracion before building the dimension"
        )

    return mapping


spark = (
    SparkSession.builder
    .appName("job-silver-d_elemento")
    .getOrCreate()
)


elements_table = resolve_table(spark, ELEMENTS_TABLE_CANDIDATES)

logger.info(
    "Building %s from %s",
    TARGET_TABLE,
    elements_table,
)

df_elements = read_elements(spark, elements_table)

df_resolution = resolve_distributor(
    read_elements(spark, elements_table),
    read_elements(spark, elements_table),
)

df_distribuidoras = read_elements(spark, elements_table).select(
    col("id").alias("distribuidora_id"),
    col("nombre").alias("distribuidora_nombre"),
    col("ruta").alias("distribuidora_ruta"),
)

df_map = read_distributor_map(spark)

df_joined = (
    df_elements
    .join(df_resolution, on="id", how="left")
    .join(df_distribuidoras, on="distribuidora_id", how="left")
    .join(df_map, on="distribuidora_id", how="left")
)

# The first segment of an element path names the distribuidora it hangs from,
# so comparing it against the resolved distribuidora's own first segment is an
# independent check of the climb.
df_out = (
    df_joined
    .withColumn(
        "coincide_ruta",
        when(
            col("ruta").isNull() | col("distribuidora_ruta").isNull(),
            lit(None).cast("boolean"),
        ).otherwise(
            lower(first_path_segment(col("ruta")))
            == lower(first_path_segment(col("distribuidora_ruta")))
        ),
    )
    .select(
        col("id"),
        col("nombre"),
        col("tipo_elemento_id"),
        col("tipo_codigo"),
        col("parent_id"),
        col("ruta"),
        col("distribuidora_id"),
        col("distribuidora_nombre"),
        col("source_database"),
        col("profundidad"),
        # A climb that ends on a type 115 root with no Calser database behind
        # it is correct, but the element does not belong to any of the three
        # distribuidoras and must not reach the training set as if it did.
        when(
            (col("estado_resolucion") == lit("RESUELTO"))
            & col("source_database").isNull(),
            lit("DISTRIBUIDORA_SIN_CALSER"),
        )
        .otherwise(col("estado_resolucion"))
        .alias("estado_resolucion"),
        col("coincide_ruta"),
        current_timestamp().alias("audit_loaded_at"),
    )
    .localCheckpoint(eager=True)
)


for row in (
    df_out
    .groupBy("estado_resolucion")
    .count()
    .orderBy(col("count").desc())
    .collect()
):
    logger.info(
        "Resolution: %s -> %s elements",
        row["estado_resolucion"],
        row["count"],
    )

for row in (
    df_out
    .where(col("estado_resolucion") == lit("RESUELTO"))
    .groupBy("distribuidora_id", "distribuidora_nombre", "source_database")
    .count()
    .orderBy(col("count").desc())
    .collect()
):
    logger.info(
        "Distribuidora: id=%s nombre=%s calser=%s -> %s elements",
        row["distribuidora_id"],
        row["distribuidora_nombre"],
        row["source_database"],
        row["count"],
    )

mismatches = df_out.where(col("coincide_ruta") == lit(False)).count()

if mismatches:
    logger.warning(
        "%s elements whose path does not match the resolved distribuidora",
        mismatches,
    )


(
    df_out.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

logger.info(
    "Dimension completed: %s",
    TARGET_TABLE,
)
