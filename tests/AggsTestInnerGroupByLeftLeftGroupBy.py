# Databricks notebook source
# MAGIC %run "./SetupInputStream"

# COMMAND ----------

j = (
  c.join(t)
  .onKeys('customer_id').partitionBy(prune('date'))
  .groupBy("customer_id")
  .agg(F.sum("amount").alias("total_amount"), F.avg("amount").alias("avg"), F.count("amount").alias("count"))
  .reduce(column = "avg", update = (F.col("u.total_amount") + F.col("staged_updates.total_amount")) / (F.col("u.count") + F.col("staged_updates.count")))
  .join(t, 'left')
  .onKeys("customer_id")
  .join(o, 'left')
  .onKeys("transaction_id")
  .groupBy("order_id")
  .agg(F.sum("total_amount").alias("total_amount_per_order"))
  .writeToPath(f'{gold_path}/aggs')
  .option("checkpointLocation", f'{checkpointLocation}/gold/aggs')
  .queryName(f'{gold_path}/aggs')
  .start()
)

# COMMAND ----------

awaitInputTermination()
j.awaitAllProcessedAndStop(shutdownLatencySecs = streamShutdownLatencySecs)

# COMMAND ----------

cc = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
tt = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
cc_tt = cc.join(tt, tt['customer_id'] == cc['customer_id']).drop(cc['customer_id'])
cc_tt_g = cc_tt.groupBy("customer_id").agg(F.sum("amount").alias("total_amount"), F.avg("amount").alias("avg"), F.count("amount").alias("count"))
cc_tt_g_tt = cc_tt_g.join(tt, cc_tt_g['customer_id'] == tt['customer_id'], 'left').drop(tt['customer_id'])
cc_tt_g_tt_oo = cc_tt_g_tt.join(oo, cc_tt_g_tt['transaction_id'] == oo['transaction_id'], 'left').drop(oo['transaction_id'])
jj = cc_tt_g_tt_oo.groupBy("order_id").agg(F.sum("total_amount").alias("total_amount_per_order"))
jj.count()

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/aggs')
df.count()

# COMMAND ----------

compare_dataframes(df, jj)
