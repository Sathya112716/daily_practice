from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("PivotExplodeExample").getOrCreate()

data = [
    ("A", ["x1", "x2"], [1, 2]),
    ("B", ["y1", "y2", "y3"], [3, 4, 5]),
    ("C", ["z1"], [6])
]

columns = ["id", "labels", "values"]
df = spark.createDataFrame(data, columns)
df.show(truncate=False)
df_exploded = df.withColumn("label", explode("labels")).withColumn("value", explode("values"))

df_exploded.show(truncate=False)
df_pivoted = df_exploded.groupBy("id").pivot("label").sum("value")

df_pivoted.show(truncate=False)


