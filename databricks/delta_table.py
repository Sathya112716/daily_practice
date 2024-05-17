# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Define schema for DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Experience", IntegerType(), True)
])

# Create DataFrame with additional columns
data = [
    ("Sathya Priya", 25, "Engineer", "New York", "USA", 80000, 3),
    ("Ambika", 30, "Data Scientist", "San Francisco", "USA", 100000, 5),
    ("ravi Chandran", 35, "Analyst", "London", "UK", 60000, 7)
]

df = spark.createDataFrame(data, schema)
display(df)
df.write.format('delta').saveAsTable('default.table1')




