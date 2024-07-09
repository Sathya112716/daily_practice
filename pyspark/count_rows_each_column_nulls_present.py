from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("NullCountExample").getOrCreate()

data = [
    (1, None, 10),
    (2, 3, None),
    (None, 4, 6),
    (4, None, 8)
]

columns = ["col1", "col2", "col3"]

df = spark.createDataFrame(data, columns)

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

null_counts.show()
