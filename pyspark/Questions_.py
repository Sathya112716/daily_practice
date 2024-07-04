# Databricks notebook source
pip install requests


# COMMAND ----------

import requests
import json
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, current_date

# COMMAND ----------

response_data = requests.get('https://reqres.in/api/users?page=2')
json_data = response_data.json()
display(json_data)

# COMMAND ----------

data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# COMMAND ----------

custom_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("data", ArrayType(data_schema), True),
    StructField("support", MapType(StringType(), StringType()), True)
])

# COMMAND ----------

json_df=json_data
df = spark.createDataFrame([json_df], custom_schema)

# COMMAND ----------

df = df.drop('page', 'per_page', 'total', 'total_pages', 'support')

# COMMAND ----------

df = df.withColumn('data', explode('data'))

# COMMAND ----------

df = df.withColumn("id", df.data.id).withColumn('email', df.data.email).withColumn('first_name', df.data.first_name).withColumn('last_name', df.data.last_name).withColumn('aatar', df.data.avatar).drop(df.data)


# COMMAND ----------

derived_site_address_df = df.withColumn("site_address",split(df["email"],"@")[1])

# COMMAND ----------

loaded_date = derived_site_address_df.withColumn('load_date', current_date())

# COMMAND ----------

loaded_date.write.format('delta').mode('overwrite').save('dbfs:/FileStore/src/question2/site_info/person_info')


# COMMAND ----------

path=spark.read.load("dbfs:/FileStore/src/question2/site_info/person_info")

# COMMAND ----------

display(path)


# COMMAND ----------

#question_2
#How can you convert this dataframe into list without using rdd and collect (like without converting the datafame into rdd) using pyspark 
data = [("Sathya", "Priya", "India", "Barugur"),
        ("Ravi", "Chandran", "India", "Krishnagiri"),
        ("Maha", "Lakshmi", "India", "Hosur"),
        ("Negha", "Ravi", "India", "Karnataka")]

columns = ["firstname", "lastname", "country", "state"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

# Convert to Pandas DataFrame
pandas_df = df.toPandas()

# COMMAND ----------

# Convert to list
data_list = pandas_df.values.tolist()

print(data_list)


# COMMAND ----------

#question 3
# Sample DataFrame
data_1= [
    ("Alice", "Smith", "F", "NY", "alice@gmail.com", "Engineer", "2012-01-01", 30, 2000, 50000.5),
    ("Bob", "Brown", "M", "CA", "bob@gmail.com", "Doctor", "2013-01-01", 40, 3000, 60000.0),
    ("Cathy", "Davis", "F", "TX", "cathy@gmail.com", "Teacher", "2014-01-01", 35, 2500, 55000.0)
]

columns = ["first_name", "last_name", "gender", "state", "email", "profession", "hire_date", "age", "experience", "salary"]

df = spark.createDataFrame(data_1, columns)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

json_df = df.withColumn("json", to_json(struct([col(c) for c in df.columns])))
json_df = json_df.select("json")
display(json_df)


# COMMAND ----------

# Collect JSON strings
json_strings = json_df.rdd.map(lambda row: row.json).collect()


# COMMAND ----------

# Function to manipulate JSON strings
def format_json(json_str):
    data = json.loads(json_str)
    formatted_items = []
    for key, value in data.items():
        if isinstance(value, str):
            formatted_items.append(f'"{key}":"{value}"')
        else:
            formatted_items.append(f'"{key}":{value}')
    return "{" + ",".join(formatted_items) + "}"


# COMMAND ----------

#Apply the function  to each JSON string
formatted_json_strings = [format_json(json_str) for json_str in json_strings]

# COMMAND ----------

# Print formatted JSON strings
for json_str in formatted_json_strings:
    print(json_str)

# COMMAND ----------

help(from_json)

# COMMAND ----------


