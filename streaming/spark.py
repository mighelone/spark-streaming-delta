from pyspark.sql import SparkSession


DELTA_FORMAT = "delta"

def get_spark_context(app_name="myApp") -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # .config("spark.databricks.delta.optimizeWrite.enabled", True)
        # .config("spark.databricks.delta.autoCompact.enabled", True)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", False)
        .appName(app_name)
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("ERROR")
    return spark


def enable_auto_compact(spark:SparkSession):
    # https://docs.databricks.com/delta/optimizations/auto-optimize.html
    spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
