
# Import pySpark
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data1 = [(1,'him'),(2,'pra')]
schema = StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True)])
df = spark.createDataFrame(data = data1,schema =schema)
df.show()

data2 = [(1,'him'),(3,'ram'),(4,'sam')]
schema = StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True)])
df2 = spark.createDataFrame(data = data2,schema =schema)
df2.show()
df3 = df.join(df2,df.id == df2.id,'leftanti').show() # 2,pra not match record from left with the left column 
df3 = df.join(df2,df.id == df2.id,'leftsemi').show() #1,him like inner join but select left columns