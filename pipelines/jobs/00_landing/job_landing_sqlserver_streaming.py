from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Todos los topics de tablas de TedisNet_EOSA.
TOPIC_PATTERN = r".*TedisNet_EOSA.*"

LANDING_PATH = "s3a://datalake/00_landing/tedisnet-eosa"
CHECKPOINT_PATH = "s3a://datalake/_checkpoints/00_landing/tedisnet-eosa"


spark = (
    SparkSession.builder
    .appName("job-landing-sqlserver-streaming")
    .config("spark.executor.memory", "2g")
    .config("spark.cores.max", "2")
    .getOrCreate()
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribePattern", TOPIC_PATTERN)
    .option("startingOffsets", "earliest")
    .option("includeHeaders", "true")
    .load()
)

landing_df = kafka_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
    col("headers"),
    col("key").cast("string"),
    col("value").cast("string"),
    current_timestamp().alias("ingested_at")
)

query = (
    landing_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(LANDING_PATH)
)

query.awaitTermination()
