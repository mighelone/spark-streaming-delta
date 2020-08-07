from streaming.spark import get_spark_context
from delta import DeltaTable


spark = get_spark_context(app_name="vacumm")

table = DeltaTable.forPath(sparkSession=spark, path="prices")

table.vacuum(retentionHours=0)
