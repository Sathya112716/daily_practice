from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

data = [
    (1, "Sathya", 5000),
    (2, "Priya", 6000),
    (3, "Anu", 5500),
    (4, "Sandhya", 7000),
    (5, "Maha", 7500)
]

df = spark.createDataFrame(data, ["id", "name", "salary"])
window_spec = Window.orderBy("salary").partitionBy()

df = df.withColumn("row_number", row_number().over(window_spec))

df = df.withColumn("rank", rank().over(window_spec))

df = df.withColumn("dense_rank", dense_rank().over(window_spec))

df = df.withColumn("next_salary", lead("salary", 1).over(window_spec))

df = df.withColumn("prev_salary", lag("salary", 1).over(window_spec))

df.show()

spark.stop()
