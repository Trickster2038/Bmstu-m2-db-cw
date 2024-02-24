from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# import json
import time

sparkSession = SparkSession.builder.appName("spark_sql_app_2").config("spark.eventLog.enabled", "true").getOrCreate()

data = sparkSession.read.load("hdfs://namenode:9000/master.csv", format="csv",sep=",", inferSchema="true", header="true")
data.registerTempTable("master")

data = sparkSession.read.load("hdfs://namenode:9000/order.csv", format="csv",sep=",", inferSchema="true", header="true")
data.registerTempTable("order")

data = sparkSession.read.load("hdfs://namenode:9000/customer.csv", format="csv",sep=",", inferSchema="true", header="true")
data.registerTempTable("customer")

result = sparkSession.sql("""select m.master_id, m.master_desc, o.order_id, o.order_date, o.order_due_date, o.order_fact_completion_date, c.order_customer_id, c.order_customer_desc 
from master m JOIN order o ON o.order_master_id = m.master_id LEFT JOIN customer c ON o.order_customer_id = c.order_customer_id
where o.order_fact_completion_date > o.order_due_date
""")
result.show()
# result.explain()
result.write.csv(path='hdfs://namenode:9000/result.csv',mode='overwrite', header=True)

print("=== before stop ===")
# input()
print("=== after stop ===")
# sparkSession.stop()
# time.sleep(10)
print("=== after stop ===")

# /spark/bin/pyspark  --master spark://spark-master:7077
# data = sparkSession.read.load("hdfs://namenode:9000/result.csv", format="csv",sep=",", inferSchema="true", header="true")
# data.show(20, False)


# sparkSession.sql("""select count(1)
# from master m JOIN order o ON o.order_master_id = m.master_id LEFT JOIN customer c ON o.order_customer_id = c.order_customer_id
# where o.order_fact_completion_date <= o.order_due_date
# """).show()