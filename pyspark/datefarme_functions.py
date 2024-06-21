from pyspark.sql.functions import col #
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame Functions").getOrCreate()
sc=spark.sparkContext
data=[("Sathya",24,1,),("Ravi",45,2),("priya",17,3),(None,15,4)]
rdd=sc.parallelize(data)
print(rdd)
df1=spark.createDataFrame(rdd,["Name","Age","Id"])
df1.printSchema()

#Used count function
count_result=df1.count()
print(count_result)

#select the particular column
select_result=df1.select("Name")
print(select_result)
select_result.show()

#filter function
filter_result=df1.filter(df1['Id']>1)
print(filter_result)
filter_result.show()

#where condition
where_result = df1.na.fill({'Name': 'Unknown'})
where_result.show()

#like function
like_result=df1.filter(df1['Name'].like('%a'))
like_result.show()

#describe function
describe_result=df1.describe()
describe_result.show()

# Assuming df1 is your DataFrame
columns_result = df1.columns
print(columns_result)

#alias function
alias_df=df1.select(col("Name").alias("Full Name"),"Age","Id")
alias_df.show()

#Orderby function
ordered_df=df1.orderBy("Age")
ordered_df.show()

#sort function
sorted_df=df1.sort("Age")
sorted_df.show()

#groupBy function
grouped_df=df1.groupBy("Id").count()
grouped_df.show()

#when and otherwise
df1_with_condition=df1.withColumn("Category", when(df1["Age"]<30,"Young").otherwise("Old"))
df1_with_condition.show()