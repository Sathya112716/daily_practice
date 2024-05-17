from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Handling NULL Values") .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

data = [(1, "Sathya"),(2, None),(3, "Ravi"),(None, "Eniyan")]

df = spark.createDataFrame(data, schema)
df.show()

#filtering Null
df_filtered=df.filter(df.id.isNotNull() & df.name.isNotNull())
print("Dataframe Filtered Nulls:")
df_filtered.show()

#Replacing Null
df_replaced=df.fillna({"name":"Sandhya"} , {4:"id"})
print("Dataframe Replaced Null:")
df_replaced.show()

#Coalesce Method
df_coalesced=df.withColumn("coalesce",coalesce(df.id,df.name))
print("Dataframe using Coalesce")
df_coalesced.show()

#Dropping rows with Null values
df_dropped=df.na.drop()
print("Dataframe after using na.drop")
df_dropped.show()

#fillna() function
df_filledna=df.fillna({'id':2,'name':'Unknown'})
print("Dataframe using fillna")
df_filledna.show()

#fill() function
df_fill=df.fillna('Unknown',subset=['name'])
print("Dataframe after using fill")
df_fill.show()
