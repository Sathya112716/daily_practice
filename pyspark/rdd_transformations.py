import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Transformation").getOrCreate()
sc=spark.sparkContext
#added the text file which is created in the working directory:
rdd=sc.textFile(r"C:\Users\SathyaPriyaR\Desktop\create_a_file.txt")
rdd2=rdd.flatMap(lambda x:x.split(" "))
print(rdd2.collect())
rdd3=rdd2.map(lambda x:(x,1))
print(rdd3.collect())
rdd4=rdd3.reduceByKey(lambda a,b:a+b)
print(rdd4.collect())
rdd5=rdd4.map(lambda x:(x[1],x[0])).sortByKey()
print(rdd5.collect())
