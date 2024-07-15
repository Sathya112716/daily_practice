from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("from json").getOrCreate()
json_data = [
    '{"name": "Sathya", "age": 30}',
    '{"name": "Eniyan", "age": 25}',
    '{"name": "Sandhya", "age": 35}'
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

json_df = spark.createDataFrame(json_data, StringType()).toDF("json")


parsed_df = json_df.withColumn("parsed_json", from_json(col("json"), schema))

parsed_df.show(truncate=False)

