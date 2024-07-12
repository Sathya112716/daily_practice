from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Date and Time Functions").getOrCreate()

data = [(date,) for date in ["2022-01-01", "2022-02-15", "2022-03-20", "2022-04-25"]]
columns = ["date"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("current_date", current_date())
df = df.withColumn("current_timestamp", current_timestamp())
df = df.withColumn("date_add_5_days", date_add(df["date"], 5))
df = df.withColumn("date_difference", datediff(df["date"], current_date()))
df = df.withColumn("year", year(df["date"]))
df = df.withColumn("month", month(df["date"]))
df = df.withColumn("day", dayofmonth(df["date"]))
df = df.withColumn("formatted_date", to_date(df["date"]))
df = df.withColumn("date_formatted", date_format(df["date"], "MM/dd/yyyy"))

df.show()

spark.stop()
