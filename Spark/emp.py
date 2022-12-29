import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName('test').master('local').getOrCreate()
emp_data = [  (7369, "SMITH", "JUNIOR", 7902, 800, None, 20),
  (7499, "ALLY", "SALESWOMAN", 7698, 1600, 300, 30),
  (7521, "WARDY", "SALESWOMAN", 7698, 1250, 500, 30),
  (7566, "JONES", "SUPERVISOR", 7839, 2975, None, 20),
  (7654, "MARTINI", "SALESWOMAN", 7698, 1250, 1400, 30),
  (7698, "BLAKE", "SUPERVISOR", 7839, 2850, None, 30)]
cols = ["empno", "ename", "job", "mgr", "sal", "comm", "deptno"]
df = spark.createDataFrame(data = emp_data,schema = cols)
df.show()

#df.groupBy('deptno').pivot('job').agg(sum('sal')).show()
