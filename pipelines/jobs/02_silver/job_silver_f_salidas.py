from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, trim
from pyspark.sql.types import TimestampType


TARGET_TABLE = "l2_silver.salidas"


spark = (
    SparkSession.builder
    .appName("job-silver-f_salidas")
    .getOrCreate()
)


df_salidas_eosa = (
    spark.table("l1_bronze.calser_eosa_salidas")
    .withColumn("source_database", lit("Calser_EOSA"))
)

df_salidas_pitarch = (
    spark.table("l1_bronze.calser_pitarch_salidas")
    .withColumn("source_database", lit("Calser_Pitarch"))
)

df_salidas_vsa = (
    spark.table("l1_bronze.calser_valle_santa_ana_salidas")
    .withColumn("source_database", lit("Calser_ValleSantaAna"))
)


df_in = (
    df_salidas_eosa
    .unionByName(df_salidas_pitarch)
    .unionByName(df_salidas_vsa)
)


df_out = df_in.select(
    col("SALIDA_ID").alias("id"),
    trim(col("SALIDA_ABREVIATURA")).alias("abreviatura"),
    trim(col("SALIDA_NOMBRE")).alias("nombre"),
    col("SALIDA_USUARIO_ALTA_ID").alias("usuario_alta_id"),
    col("SALIDA_CT_ID").alias("ct_id"),
    col("SALIDA_PERIODO_ID").alias("periodo_id"),
    col("SALIDA_TS").alias("ts").cast(TimestampType()),
    col("source_database"),
    current_timestamp().alias("audit_loaded_at"),
)


(
    df_out.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)
