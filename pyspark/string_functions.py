from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("String Functions").getOrCreate()
sc = spark.sparkContext
data= [("Sathya","Computer Science"),("Ravi","Business"),("Bhuva","Electronics")]

rdd=sc.parallelize(data)
print(rdd)
df=spark.createDataFrame(rdd,["Name","Department"])
df.printSchema()
df.show()
#translate function
df_translate = df.withColumn("translated_text", translate(df["Name"], "thyvi nes", "234"))
df_translate.printSchema()
df_translate.show()

#substring function
df_substring= df.withColumn("substring", substring(df["Name"], 2, 5))
df_substring.show()

#substring index function
df_transformed = df.withColumn("substring", substring_index(df["Name"], " ", 1))
df_transformed.show()

#split function
df_split= df.withColumn("split", split(df["Name"], " "))
df_split.show()

#repeat function
df_repeat= df.withColumn("repeated", expr("repeat(Name,3)"))
df_repeat.show()

# rpad function
df_rpad = df.withColumn("right padded", rpad(df["Name"], 7, "0"))
df_rpad.show()

# lpad function
df_lpad = df.withColumn("left padded", lpad(df["Name"], 7, "*"))
df_lpad.show()

#regexp_replace function
df_replace = df.withColumn("Replace", regexp_replace(df["Department"], "-", "/"))
df_replace.show()

#length function
df_length = df.withColumn("Name_Length", length(df["Department"]))
df_length.show()

