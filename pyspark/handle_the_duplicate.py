from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Handling Duplicates").getOrCreate()

data = [("Sathya", 25),("Riya", 30),("Anu", 25),("Sathya", 25),("Ravi", 30)]

columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()
#distinct function
df_distinct=df.distinct()
print("Distinct DataFrame:")
df_distinct.show()

#drop Duplicates
df_drop=df.dropDuplicates(['age'])
print("Drop Duplicates:")
df_drop.show()

#remove duplicates using the specific column
df_remove_duplicate=df.dropDuplicates(['age'])
print("Remove Duplicates using a specific column")
df_remove_duplicate.show()

#Remove duplicates using multiple columns
df_remove_multiple=df.dropDuplicates(['name','age'])
print("Remove Duplicates using multiple columns:")
df_remove_multiple.show()