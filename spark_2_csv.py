from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

sparkSession = SparkSession.builder.appName("cw_loader").getOrCreate()


with open('spark_masters.json') as f:
    masters = json.load(f)

with open('spark_orders.json') as f:
    orders = json.load(f)

MasterSchema = StructType([
    StructField("master_id", IntegerType(), False),
    StructField("master_desc", StringType(), False),
    StructField("master_feedbacks", StringType(), False)
])
CustomerSchema = StructType([
    StructField("order_customer_id", IntegerType(), False),
    StructField("order_customer_desc", StringType(), False)
])
OrderSchema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_date", StringType(), False),
    StructField("order_customer_id", IntegerType(), False),
    # StructField("order_customer_desc", StringType(), False),
    StructField("order_details_desc", StringType(), False),
    StructField("order_due_date", StringType(), False),
    StructField("order_fact_completion_date", StringType(), False),
    StructField("order_parts", StringType(), False),
    StructField("repair_types", StringType(), False),
    StructField("order_price", FloatType(), False),
    StructField("order_master_id", IntegerType(), False)
])

MasterTable = []
CustomerTable = []
OrderTable = []

CustomerSet = set()

for master in masters:
    MasterTable.append((
        master['_source']['master_id'],
        master['_source']['master_desc'],
        str(master['_source']['master_feedbacks'])
    ))
for order in orders:
    OrderTable.append((
        order['_source']['order_id'],
        order['_source']['order_date'],
        order['_source']['order_customer_id'],
        order['_source']['order_details_desc'],
        order['_source']['order_due_date'],
        order['_source']['order_fact_completion_date'],
        str(order['_source']['order_parts']),
        str(order['_source']['repair_types']),
        order['_source']['order_price'],
        order['_source']['order_master_id']
    ))
    if not order['_source']['order_id'] in CustomerSet:
        CustomerSet.add(order['_source']['order_id'])
        CustomerTable.append((
            order['_source']['order_customer_id'],
            order['_source']['order_customer_desc']
        ))

MasterDF = sparkSession.createDataFrame(MasterTable, MasterSchema)
OrderDF = sparkSession.createDataFrame(OrderTable, OrderSchema)
CustomerDF = sparkSession.createDataFrame(CustomerTable,CustomerSchema)

MasterDF.write.csv(path='hdfs://namenode:9000/master.csv',mode='overwrite', header=True)
OrderDF.write.csv(path='hdfs://namenode:9000/order.csv',mode='overwrite', header=True)
CustomerDF.write.csv(path='hdfs://namenode:9000/customer.csv',mode='overwrite', header=True)

sparkSession.stop()

