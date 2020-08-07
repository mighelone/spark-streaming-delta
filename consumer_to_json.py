import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from pyspark.sql.functions import current_timestamp
from streaming.spark import get_spark_context
import datetime as dt

spark = get_spark_context(__name__)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "prices")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr(
        "CAST(key AS STRING)", 
        "CAST(value AS STRING)",
        "timestamp",
    )
    .select(
        psf.col("key").cast(pst.IntegerType()).alias("match_id"),
        psf.col("value").alias("body"),
        "timestamp",
        psf.to_date(psf.col("timestamp")).alias("arrival_date")
    )
    .coalesce(1)
    .writeStream.format("json")
    .outputMode("append")
    .option("checkpointLocation", f"./_checkpoints/raw")
    .partitionBy("arrival_date")
    .trigger(processingTime='1 minute')
    .start("./raw/prices")
)

spark.streams.awaitAnyTermination()