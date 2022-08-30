# Databricks notebook source
# MAGIC %run ./StreamJoin

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

user = 'leon.eller@databricks.com'#dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

root_path = f"/Users/{user}/tmp/demo/cdc_raw"
customer_path = f"{root_path}/customers"
transaction_path = f"{root_path}/transactions"
orders_path = f"{root_path}/orders"
checkpointLocation = f"/Users/{user}/tmp/demo/cp"
schemaLocation = f"/Users/{user}/tmp/demo/schema"
# bronze_path = f"/Users/{user}/tmp/demo/bronze"
silver_path = f"/Users/{user}/tmp/demo/silver"
gold_path = f"/Users/{user}/tmp/demo/gold"

# COMMAND ----------

dbutils.fs.rm(checkpointLocation, True)
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)

# COMMAND ----------

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/transactions` ( amount STRING, customer_id STRING, id STRING, item_count STRING, operation STRING, operation_date STRING, transaction_date STRING) USING delta 
''')

# COMMAND ----------

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/customers` ( address STRING, email STRING, firstname STRING, id STRING, lastname STRING, operation STRING, operation_date STRING)
USING delta
''')

# COMMAND ----------

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/orders` ( delivery_date string,
id string,
item_name string,
operation string,
operation_date string,
transaction_id string) USING delta
''')

# COMMAND ----------

customer_stream = (
       spark.readStream.format("cloudFiles")
          .option('cloudFiles.format', 'json')
          .option('cloudFiles.schemaLocation', f'{schemaLocation}/customers')
          .option('cloudFiles.maxBytesPerTrigger', 20000)
          .load(customer_path)
          .drop('_rescued_data')
     )

# COMMAND ----------

transaction_stream = (
       spark.readStream.format("cloudFiles")
          .option('cloudFiles.format', 'json')
          .option('cloudFiles.schemaLocation', f'{schemaLocation}/transactions')
          .option('cloudFiles.maxBytesPerTrigger', 100)
          .load(transaction_path)
          .drop('_rescued_data')
     )

# COMMAND ----------

orders_stream = (
       spark.readStream.format("cloudFiles")
          .option('cloudFiles.format', 'json')
          .option('cloudFiles.schemaLocation', f'{schemaLocation}/orders')
          .option('cloudFiles.maxBytesPerTrigger', 2000000)
          .load(orders_path)
          .drop('_rescued_data')
     )

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
(customer_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/customers')
  .start(f'{silver_path}/customers'))

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
(transaction_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/transactions')
  .start(f'{silver_path}/transactions'))

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
(orders_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/orders')
  .start(f'{silver_path}/orders'))

# COMMAND ----------

aa = spark.readStream.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
bb = spark.readStream.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id')
oo = spark.readStream.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
aa = aa.withColumn('cts', F.current_timestamp()).withWatermark("cts", "1 days")
bb = bb.withColumn('tts', F.current_timestamp()).withWatermark("tts", "1 days")
oo = oo.withColumn('ots', F.current_timestamp()).withWatermark("ots", "1 days")
aa_bb = aa.join(bb, ((bb['customer_id'] == aa['customer_id']) & ((aa['cts'] >= bb['tts']) & (F.expr('cts <= tts + INTERVAL 20 day')))), 'left').drop(bb['customer_id'])
cc = aa_bb.join(oo, ((oo['transaction_id'] == aa_bb['transaction_id']) & ((aa_bb['cts'] >= oo['ots']) & (F.expr('cts <= ots + INTERVAL 20 days')))), 'left').drop(oo['transaction_id'])

# COMMAND ----------

spark.conf.set('spark.sql.streaming.statefulOperator.checkCorrectness.enabled', False)

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
(cc
  .writeStream
  .format('delta')
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .outputMode("update")
  .start(f'{gold_path}/joined'))
