# Databricks notebook source
from pyspark.sql.functions import *
data = [(i,) for i in range(400)]
df = spark.createDataFrame(data, ["number"])
df.display()


# COMMAND ----------

df1=df.rdd.getNumPartitions()
print(df1)

# COMMAND ----------

df2=df.withColumn("Partition_id",spark_partition_id())
df2.display()

# COMMAND ----------

df3=df2.groupBy("partition_id").count()
df3.display()

# COMMAND ----------

df4=df.coalesce(4)
df5=df4.rdd.getNumPartitions()
display(df5)

# COMMAND ----------

df6=df4.withColumn("new_Partition_id",spark_partition_id())
df6.display()

# COMMAND ----------

df7=df6.groupBy("new_partition_id").count()
df7.display()
