import json
import logging
import re

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_add,
    lit,
    row_number,
    timestamp_micros,
    timestamp_millis,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


LANDING_PATH = "s3a://datalake/00_landing/tedisnet-eosa"
CHECKPOINT_PATH = "s3a://datalake/_checkpoints/01_bronze/tedisnet-eosa"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


def to_snake(value: str) -> str:
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)

    return value.lower()


def get_spark_type(field: dict):
    connect_type = field["type"]

    primitive_types = {
        "int8": ByteType(),
        "int16": ShortType(),
        "int32": IntegerType(),
        "int64": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "string": StringType(),
        "bytes": BinaryType(),
    }

    if connect_type in primitive_types:
        return primitive_types[connect_type]

    if connect_type == "struct":
        return StructType([
            StructField(
                child["field"],
                get_spark_type(child),
                child.get("optional", True),
            )
            for child in field.get("fields", [])
        ])

    if connect_type == "array":
        return ArrayType(
            get_spark_type(field["items"]),
            containsNull=True,
        )

    if connect_type == "map":
        return MapType(
            get_spark_type(field["keys"]),
            get_spark_type(field["values"]),
            valueContainsNull=True,
        )

    raise ValueError(
        f"Unsupported Kafka Connect JSON type: {connect_type}"
    )


def build_struct_schema(fields: list[dict]) -> StructType:
    return StructType(
        [
            StructField(
                field["field"],
                get_spark_type(field),
                field.get("optional", True),
            )
            for field in fields
        ]
    )


def get_field(schema: dict, name: str) -> dict:
    return next(
        field
        for field in schema["fields"]
        if field.get("field") == name
    )


def apply_logical_types(df, fields: list[dict]):
    """
    Converts common Debezium logical temporal types to Spark SQL types.
    """
    for field in fields:
        name = field["field"]
        logical_type = field.get("name")

        if logical_type == "io.debezium.time.Timestamp":
            df = df.withColumn(
                name,
                timestamp_millis(col(name)),
            )

        elif logical_type == "io.debezium.time.MicroTimestamp":
            df = df.withColumn(
                name,
                timestamp_micros(col(name)),
            )

        elif logical_type == "io.debezium.time.NanoTimestamp":
            df = df.withColumn(
                name,
                timestamp_micros(
                    (col(name) / 1000).cast("long")
                ),
            )

        elif logical_type == "io.debezium.time.Date":
            df = df.withColumn(
                name,
                date_add(
                    to_date(lit("1970-01-01")),
                    col(name).cast("int"),
                ),
            )

        elif logical_type == "io.debezium.time.ZonedTimestamp":
            df = df.withColumn(
                name,
                to_timestamp(col(name)),
            )

    return df


def get_debezium_schemas(df):
    """
    Reads the Debezium schemas embedded in one event of the topic.
    """
    value_row = (
        df
        .where(col("value").isNotNull())
        .select("value")
        .first()
    )

    if value_row is None:
        return None

    key_row = (
        df
        .where(col("key").isNotNull())
        .select("key")
        .first()
    )

    if key_row is None:
        raise ValueError(
            "CDC event does not contain a Kafka key"
        )

    value_json = json.loads(value_row["value"])
    key_json = json.loads(key_row["key"])

    envelope_schema = value_json["schema"]
    key_schema = key_json["schema"]

    before_field = get_field(envelope_schema, "before")
    after_field = get_field(envelope_schema, "after")

    row_fields = after_field["fields"]
    key_fields = key_schema["fields"]

    row_schema = build_struct_schema(row_fields)
    key_struct_schema = build_struct_schema(key_fields)

    payload_schema = StructType(
        [
            StructField("before", row_schema, True),
            StructField("after", row_schema, True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
        ]
    )

    value_schema = StructType(
        [
            StructField(
                "payload",
                payload_schema,
                True,
            )
        ]
    )

    kafka_key_schema = StructType(
        [
            StructField(
                "payload",
                key_struct_schema,
                True,
            )
        ]
    )

    return (
        value_schema,
        kafka_key_schema,
        row_fields,
        [field["field"] for field in key_fields],
    )


def process_topic(df, topic: str):
    parts = topic.split(".")

    database = parts[-3]
    table = parts[-1]

    target_table = (
        f"l1_bronze."
        f"{to_snake(database)}_{to_snake(table)}"
    )

    logger.info(
        "Processing CDC: topic=%s target=%s",
        topic,
        target_table,
    )

    schemas = get_debezium_schemas(df)

    if schemas is None:
        return

    (
        value_schema,
        key_schema,
        row_fields,
        key_fields,
    ) = schemas

    from pyspark.sql.functions import from_json

    parsed_df = (
        df
        .where(col("value").isNotNull())
        .withColumn(
            "_event",
            from_json(col("value"), value_schema),
        )
        .withColumn(
            "_key",
            from_json(col("key"), key_schema),
        )
    )

    for key in key_fields:
        parsed_df = parsed_df.withColumn(
            f"_key_{key}",
            col(f"_key.payload.`{key}`"),
        )

    # A Kafka key remains in the same partition, so offset determines
    # the order of changes for that key.
    window = (
        Window
        .partitionBy(
            *[
                col(f"_key_{key}")
                for key in key_fields
            ]
        )
        .orderBy(
            col("offset").desc(),
        )
    )

    latest_df = (
        parsed_df
        .withColumn(
            "_row_number",
            row_number().over(window),
        )
        .where(col("_row_number") == 1)
    )

    # For DELETE use before.
    # For CREATE / UPDATE / SNAPSHOT use after.
    changes_df = (
        latest_df
        .withColumn(
            "_op",
            col("_event.payload.op"),
        )
        .withColumn(
            "_row",
            when(
                col("_event.payload.op") == "d",
                col("_event.payload.before"),
            ).otherwise(
                col("_event.payload.after"),
            ),
        )
        .select(
            "_op",
            "_row.*",
            *[
                col(f"_key_{key}")
                for key in key_fields
            ],
        )
    )

    # Ensure PK values always come from Kafka key.
    for key in key_fields:
        changes_df = changes_df.withColumn(
            key,
            col(f"_key_{key}"),
        )

    changes_df = apply_logical_types(
        changes_df,
        row_fields,
    )

    row_columns = [
        field["field"]
        for field in row_fields
    ]

    target_exists = spark.catalog.tableExists(
        target_table
    )

    if not target_exists:
        initial_df = (
            changes_df
            .where(col("_op").isin("c", "u", "r"))
            .select(*row_columns)
        )

        if not initial_df.isEmpty():
            (
                initial_df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(target_table)
            )

            logger.info(
                "Created Bronze table: %s",
                target_table,
            )

        return

    merge_condition = " AND ".join(
        f"target.`{key}` = source.`{key}`"
        for key in key_fields
    )

    source_df = changes_df.select(
        "_op",
        *row_columns,
    )

    delta_table = DeltaTable.forName(
        spark,
        target_table,
    )

    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition,
        )
        .whenMatchedDelete(
            condition="source._op = 'd'"
        )
        .whenMatchedUpdateAll(
            condition="source._op IN ('c', 'u', 'r')"
        )
        .whenNotMatchedInsertAll(
            condition="source._op IN ('c', 'u', 'r')"
        )
        .execute()
    )

    logger.info(
        "CDC processing completed: %s",
        target_table,
    )


def process_batch(batch_df, batch_id: int):
    logger.info(
        "Processing microbatch: %s",
        batch_id,
    )
    topics = [
        row["topic"]
        for row in (
            batch_df
            .select("topic")
            .distinct()
            .collect()
        )
    ]

    for topic in topics:
        process_topic(
            batch_df.where(
                col("topic") == topic
            ),
            topic,
        )


spark = (
    SparkSession.builder
    .appName("job-bronze-sqlserver-streaming")
    .config("spark.executor.memory", "4g")
    .config("spark.cores.max", "2")
    .getOrCreate()
)

landing_schema = (
    spark.read
    .parquet(LANDING_PATH)
    .schema
)

landing_df = (
    spark.readStream
    .schema(landing_schema)
    .format("parquet")
    .option("maxFilesPerTrigger", 10)
    .load(LANDING_PATH)
)

query = (
    landing_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)

query.awaitTermination()
