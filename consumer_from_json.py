import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from pyspark.sql.types import LongType

from streaming.spark import get_spark_context, enable_auto_compact, DELTA_FORMAT

RAW_DATA = "./raw/prices"

body_schema = (
    pst.StructType()
    # .add("arrival_date", pst.DateType())
    .add("match_id", pst.IntegerType())
    .add("price", pst.DoubleType())
    .add("ts", pst.TimestampType())
    .add("score", pst.ArrayType(pst.IntegerType()))
)

schema = (
    pst.StructType()
    .add("body", pst.StringType())
    .add("match_id", pst.LongType())
    .add("timestamp", pst.TimestampType())
    .add("arrival_date", pst.DateType())
)

spark = get_spark_context("consumePrices")
# enable_auto_compact(spark)


df = (
    spark
    .readStream
    # .read
    .format("json")
    .schema(schema)
    .load(RAW_DATA)
    .withColumn("body", psf.from_json("body", body_schema))
    .selectExpr(
        # "value.match_id AS match_id",
        "body.price AS price",
        "body.ts AS ts",
        "body.score[0] AS home_score",
        "body.score[1] AS away_score",
        "timestamp AS ts_kafka" "arrival_date",
    )
    .withColumn("exec_date", psf.to_date("ts"))
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "./_checkpoints/streaming_from_json")
    .option("mergeSchema", "true")
    .partitionBy("exec_date")
    .trigger(processingTime='1 minute')
    # .trigger(once=True)
    .start("./prices")
)


# df.printSchema()


spark.streams.awaitAnyTermination()
