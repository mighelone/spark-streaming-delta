import pyspark.sql.functions as psf
import pyspark.sql.types as pst

from streaming.spark import get_spark_context, enable_auto_compact, DELTA_FORMAT

schema = (
    pst.StructType()
    .add("match_id", pst.IntegerType())
    .add("price", pst.DoubleType())
    .add("ts", pst.TimestampType())
    .add("score", pst.ArrayType(pst.IntegerType()))
)

spark = get_spark_context("consumePrices")
# enable_auto_compact(spark)


(
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "prices")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value", psf.from_json("value", schema))
    .selectExpr(
        "value.match_id AS match_id",
        "value.price AS price",
        "value.ts AS ts",
        "value.score[0] AS home_score",
        "value.score[1] AS away_score",
    )
    .withColumn("exec_date", psf.to_date("ts"))
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "./_checkpoints/streaming")
    # .option("mergeSchema", "true")
    .partitionBy("exec_date")
    # .trigger(processingTime='5 minute')
    .trigger(once=True)
    .start("./prices")
)


spark.streams.awaitAnyTermination()
