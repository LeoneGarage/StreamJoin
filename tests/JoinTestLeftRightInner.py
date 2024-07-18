# Databricks notebook source
# MAGIC %run "./SetupInputStream"

# COMMAND ----------

j = (
  c.join(t, 'left')
  .onKeys('customer_id').partitionBy(prune('date'))
  .join(o, 'right')
  .onKeys('transaction_id').partitionBy(prune('date'))
  .join(p)
  .onKeys('order_id')
  .writeToPath(f'{gold_path}/joined')
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .queryName(f'{gold_path}/joined')
  .start()
)

# COMMAND ----------

awaitInputTermination()
j.awaitAllProcessedAndStop(shutdownLatencySecs = streamShutdownLatencySecs)

# COMMAND ----------

cc = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
tt = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
pp = spark.read.format('delta').load(f'{silver_path}/products').withColumnRenamed('id', 'product_id').withColumnRenamed('item_name', 'product_name')
cc_tt = cc.join(tt, tt['customer_id'] == cc['customer_id'], 'left').drop(tt['customer_id'])
cc_tt_oo = cc_tt.join(oo, oo['transaction_id'] == cc_tt['transaction_id'], 'right').drop(cc_tt['transaction_id'])
jj = cc_tt_oo.join(pp, pp['order_id'] == cc_tt_oo['order_id']).drop(pp['order_id'])
jj.count()

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/joined')
df.count()

# COMMAND ----------

compare_dataframes(df, jj)
