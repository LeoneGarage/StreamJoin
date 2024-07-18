# Databricks notebook source
# MAGIC %run "./SetupInputStream"

# COMMAND ----------

j = (
  t.groupBy("customer_id")
   .agg(F.sum("amount").alias("amount"), F.avg("amount").alias("avg"), F.count("amount").alias("count"))
   .reduce(column = "avg", update = (F.col("u.amount") + F.col("staged_updates.amount")) / (F.col("u.count") + F.col("staged_updates.count")))
   .writeToPath(f'{gold_path}/aggs')
   .option("checkpointLocation", f'{checkpointLocation}/gold/aggs')
   .queryName(f'{gold_path}/aggs')
   .start()
)

# COMMAND ----------

awaitInputTermination()
j.awaitAllProcessedAndStop(shutdownLatencySecs = streamShutdownLatencySecs)

# COMMAND ----------

tt = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
jj = tt.groupBy("customer_id").agg(F.sum("amount").alias("amount"), F.avg("amount").alias("avg"), F.count("amount").alias("count"))
jj.count()

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/aggs')
df.count()

# COMMAND ----------

compare_dataframes(df, jj)
