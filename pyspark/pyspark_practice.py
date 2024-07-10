# Databricks notebook source
#Count rows in each column where Nulls present

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])
# Sample data
data = [
    (1, None, 25, "NY"),
    (2, "Priya", None, None),
    (3, None, 35, None),
    (None, "Sandhya", 45, "SF"),
    (5, None, 55, "TX")
]
# Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum


# Initialize an empty dictionary to store the count of nulls
null_counts = {}

# Iterate over the columns of interest
for column in df.columns:
    # Count the number of nulls in the column
    null_count = df.filter(col(column).isNull()).count()
    null_counts[column] = null_count

null_counts


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

data = [("a",), ("b",), ("c",), ("d",)]
df = spark.createDataFrame(data, ["value"])

indexed_rdd = df.rdd.zipWithIndex()
indexed_df = indexed_rdd.map(lambda row: (row[1], row[0][0])).toDF(["index", "value"])

indexed_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("zipWithIndexExample").getOrCreate()

data = [("Sathya", 23, "Barugur"),
        ("Priya", 19, "Krishnagiri"),
        ("Ravi", 29, "Chennai"),
        ("Lalitha", 40, "Hosur")]

df = spark.createDataFrame(data, ["Name", "Age", "City"])
print("Original DataFrame:")
df.show()
indexed_rdd = df.rdd.zipWithIndex()
indexed_df = indexed_rdd.map(lambda row: (row[1],) + row[0]).toDF(["Index", "Name", "Age", "City"])
print("DataFrame with Index:")
indexed_df.show()

# COMMAND ----------

#Use monotonically_increasing_id() for unique, but not consecutive numbers

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_with_id = df.withColumn("UniqueID", monotonically_increasing_id())
print("DataFrame with Unique ID:")
df_with_id.show()


# COMMAND ----------

#Combine monotonically_increasing_id() with row_number() for two columns

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("UniqueID")

df_with_row_number = df_with_id.withColumn("RowNumber", row_number().over(window_spec))
df_with_row_number.show()



# COMMAND ----------

#python string functions
str="hello"
length=len(str)
print(length)


# COMMAND ----------

str.upper()

# COMMAND ----------

str.lower()

# COMMAND ----------

"hello world".capitalize() 

# COMMAND ----------

str2="hello world"
str2.title()

# COMMAND ----------

"  hello  ".strip() 

# COMMAND ----------

"hello world".replace("world", "Python")

# COMMAND ----------

"hello world".split(" ")

# COMMAND ----------

"hello world".find("world")  

# COMMAND ----------

" ".join(["hello", "world"])

# COMMAND ----------

text = "hello world, hello Python"
count_hello = text.count("hello")
print(count_hello)

# COMMAND ----------

text2= "hello world"
starts_with_hello = text2.startswith("hello")
print(starts_with_hello)
ends_with_world = text2.endswith("world") 
print(ends_with_world)

 

jahd  hdadad

# COMMAND ----------

text = "abracadabra"
substring = "a"
start = 0
while start != -1:
    start = text.find(substring, start)
    if start != -1:
        print(f"Found at index {start}")
        start += 1  




# COMMAND ----------


