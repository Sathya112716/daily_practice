from pyspark.sql import SparkSession#important necessary libraraies
from pyspark.sql import Row
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Aggregate Functions Example") .getOrCreate()

data = [
    Row(name='Sathya', age=24, department='Marketing', salary=50000),
    Row(name='Eniyan', age=16, department='Sales', salary=60000),
    Row(name='Ravi', age=50, department='Agriculture', salary=70000),
    Row(name='Sandhya', age=22, department='IT', salary=80000),
    Row(name='Anu', age=50, department='Finance', salary=90000)
]
df = spark.createDataFrame(data)

df.show()

mean_salary = df.select(mean("salary"))
print(mean_salary)
mean_salary.show()
avg_age = df.select(avg("age"))
print(avg_age)
avg_age.show()
name_list = df.select(collect_list("name"))
print(name_list)
name_list.show(truncate=False)
unique_departments = df.select(collect_set("department"))
print(unique_departments)
unique_departments.show(truncate=False)
distinct_departments = df.select(countDistinct("department"))
print(distinct_departments)
distinct_departments.show()
non_null_age_count = df.select(count("age"))
print(non_null_age_count)
non_null_age_count.show()
first_name = df.select(first("name"))
print(first_name)
first_name.show()
last_name = df.select(last("name"))
print(last_name)
last_name.show()

spark.stop()
