'''
To run the code place the file in local and change the path in line no.11 and 13
'''

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession  
from pyspark.sql.functions import *
#Creating SparkSession
spark = SparkSession.builder.getOrCreate()
#reading cusomter data
customer = spark.read.option('header',True).csv('C:\\Users\\himanshu.g.verma\\Downloads\\customer.csv')
#reading transaction data
transactions = spark.read.option('header',True).csv('C:\\Users\\himanshu.g.verma\\Downloads\\transactions.csv')
#creating temp view of dataframe
customer.createOrReplaceTempView('customer_tbl')
transactions.createOrReplaceTempView('transactions_tbl')
#tBucketed table on age
customer.write\
  .mode('overwrite')\
  .bucketBy(5, "age")\
  .sortBy("age")\
  .saveAsTable("customer_bucketed")

##Masking postcode using given condition

df = customer.withColumn("postcode",when(length(concat(customer.state,customer.gender,customer.age))>5,'******')\
                                  .otherwise(customer.postcode))

##Filter data based on age
df = df.filter(df.age>20)

##Loyalty Flag based on Total
loyalty_flag = transactions.join(customer, customer.person_id == transactions.CustomerID,"left")

loyalty_flag = loyalty_flag.withColumn("loyal_customer",when(loyalty_flag.Total>1000,'True')\
                                  .otherwise('False')).distinct()
loyalty_flag = loyalty_flag.select("person_id","postcode","state","gender","age","account_type","loyal_customer").distinct()

#selecting fields from transaction dataframe

df = transactions.select("CustomerID","Date","Total")

##Function to get the weekday for spend analysis
def get_weekday(date):
    import datetime
    import calendar
    month, day, year = (int(x) for x in date.split('/'))    
    weekday = datetime.date(year, month, day)
    return calendar.day_name[weekday.weekday()]

#registering UDF
spark.udf.register('get_weekday', get_weekday)

df.createOrReplaceTempView("weekdays")
weekday_df = spark.sql("select CustomerID,get_weekday(Date) as Weekday,round(sum(Total),2) from weekdays group by CustomerID,get_weekday(Date)")

#Time round down by 15 mins period
t_time = transactions.select("Time")
df = t_time.withColumn("new_time",regexp_replace(regexp_replace(round((regexp_replace("time",':',''))/15)*15, "(\\d{2})(\\d{2})","$1:$2"),'\..*$',''))\

'''
Linkage Attack - To mask sensitive data/PII data like Customer name,address,account number etc.
We can achieve by masking these information by using some fields like age and replace name and other PII 
fields with *
'''