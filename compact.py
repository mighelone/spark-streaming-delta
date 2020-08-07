from streaming.spark import get_spark_context, DELTA_FORMAT


data_path = "./prices"
num_files_partition = 6

partition = "exec_date='2020-07-23'"


spark = get_spark_context("compact")

# https://docs.delta.io/latest/best-practices.html#language-python
df = (
    spark.read.format(DELTA_FORMAT)
    .load(data_path)
    .where(partition)
    # .dropna()
    # .repartition(num_files_partition)
    .repartitionByRange(num_files_partition, "match_id")
    .write.option("dataChange", "false")
    .format(DELTA_FORMAT)
    .mode("overwrite")
    .option("replaceWhere", partition)
    .save(data_path)
)