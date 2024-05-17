# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DBFS file system and dbutils.fs command").getOrCreate()
# File location and type
file_location = "/FileStore/tables/house_prediction-2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

temp_table_name = "house_prediction_2_csv"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from house_prediction_2_csv

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "house_prediction_2_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from house_prediction_2_csv where _c3 > 2
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/'))


df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/house_prediction.csv")


display(df1)


dbutils.help()

dbutils.fs.help()


dbutils.data.help()


data=[(1,'Sathya'),(2,'Priya'),(3,'Sandhya')]

cols=['id','name']

df=spark.createDataFrame(data,cols)

display(df)

dbutils.data.summarize(df)
#Copy files or directories across the file systems
dbutils.fs.cp('dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/house_prediction-2.csv', 'dbfs:/destination_path/house_prediction_2.csv')
#Returns up to the first 'maxBytes' bytes of the given file as a String encoded in UTF-8
dbutils.fs.head('dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/house_prediction-2.csv',4)
#create the directory if it not t6here it will create the parent directory
dbutils.fs.mkdirs('dbfs:/FileStore/shared_uploads/data')#to add new file
#list the contents of the directory
dbutils.fs.ls('/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/')



