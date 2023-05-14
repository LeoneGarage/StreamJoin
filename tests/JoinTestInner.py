# Databricks notebook source
# MAGIC %run "./SetupInputStream"

# COMMAND ----------

j = (
  c.join(t)
  .onKeys('customer_id')
  .writeToPath(f'{gold_path}/joined')
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .queryName(f'{gold_path}/joined')
  .start()
)

# COMMAND ----------

awaitInputTermination()
j.awaitAllProcessedAndStop()

# COMMAND ----------

cc = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
tt = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
jj = cc.join(tt, tt['customer_id'] == cc['customer_id']).drop(cc['customer_id'])
jj.count()

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/joined')
df.count()

# COMMAND ----------

compare_dataframes(df, jj)
