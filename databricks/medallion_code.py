# Databricks notebook source file.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Medallion Data Processing").getOrCreate()

data = spark.read.csv("dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/house_prediction-5.csv", header=True, inferSchema=True)

# Perform data transformations and analysis
transformed_data = data.groupBy("date").agg({"price": "sum"})

# Show results
transformed_data.show()

# Stop SparkSession
spark.stop()

