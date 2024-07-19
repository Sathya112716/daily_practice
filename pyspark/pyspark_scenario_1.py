# Databricks notebook source
df=spark.read.csv('dbfs:/FileStore/csv_pyspark.csv')
df.show()

# COMMAND ----------

rdd=sc.textFile('dbfs:/FileStore/csv_pyspark.csv').zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(','))
rdd.collect()

# COMMAND ----------

column_rdd=rdd.first()
column_rdd

# COMMAND ----------

main_rdd=rdd.filter(lambda a:a!=column_rdd)
df=main_rdd.toDF(column_rdd)

# COMMAND ----------

display(df)

# COMMAND ----------

#Challenge :Data validation between source and target table
source_data=[(1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E')]
source_schema=['id','name']
source_df=spark.createDataFrame(source_data,source_schema)
source_df.show()

target_data=[(1,'A'),(2,'B'),(3,'X'),(4,'F'),(6,'G')]
target_schema=['id','name']
target_df=spark.createDataFrame(target_data,target_schema)
target_df.show()

# COMMAND ----------

from pyspark.sql.functions import col,when,isnull
final_df=source_df.join(target_df,source_df.id==target_df.id,'full')
final_df=final_df.withColumn('result',when(((source_df.id==target_df.id)&(source_df.name==target_df.name)),'matching').\
    when(((source_df.id==target_df.id)&(source_df.name!=target_df.name)),'non-matching').when((source_df.id.isnull()),'target not present'))
display(final_df)

# COMMAND ----------


