# Databricks notebook source
dbfs_path="dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/house_prediction-4.csv"
df = spark.read.csv(dbfs_path, header=True, inferSchema=True)

# COMMAND ----------

df.show() #csv file

# COMMAND ----------

dbfs_parquet_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz-1.parquet"
parquet_df = spark.read.parquet(dbfs_parquet_path) #parquet file
parquet_df.show()

# COMMAND ----------


dbfs_text_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/samplefile-1.txt"

text_rdd = sc.textFile(dbfs_text_path)

text_contents = text_rdd.collect()

# COMMAND ----------

for line in text_contents:
    print(line)

# COMMAND ----------

# Specify the DataFrame
data = [("Sathya", 30), ("Priya", 25), ("Sandhya", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Specify the DBFS output path
dbfs_output_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/creates a dataframe and writes the parquet in the specified.parquet"

# Write the DataFrame to the DBFS path
df.write.parquet(dbfs_output_path)

# COMMAND ----------


dbfs_json_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/file4.json"

json_df = spark.read.json(dbfs_json_path)

json_df.show()


# COMMAND ----------

dbfs_text_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/samplefile-1.txt"

text_content = "This is a sample text content to be written to the file."
# Write text content to file in DBFS
dbutils.fs.put(dbfs_text_path, text_content, overwrite=True)


# COMMAND ----------

# Specify the DBFS paths for CSV and Parquet files
dbfs_csv_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/sampledemo.csv"
dbfs_parquet_path = "dbfs:/FileStore/shared_uploads/sathyapriya.r@diggibyte.com/file11.parquet"

# Write DataFrame to CSV file
df.write.csv(dbfs_csv_path, header=True)

# Write DataFrame to Parquet file
df.write.parquet(dbfs_parquet_path)


