import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName('test').master('local[5]').getOrCreate()
data = [('him',1)]

schema =  StructType([StructField('name',StringType(),True),\
    StructField('id',IntegerType(),True)])
df = spark.createDataFrame(data = data,schema = schema)
df.show()

rdd1 = spark.sparkContext.parallelize((0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))
rdd3 = rdd1.coalesce(10)
print("Repartition size : "+str(rdd3.getNumPartitions()))

df1 = spark.range(0,20)
print(df1.rdd.getNumPartitions())
df1.write.mode('overwrite').csv('c://tmp//abc.csv')