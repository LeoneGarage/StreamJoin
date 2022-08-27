# Databricks notebook source
# MAGIC %run ./StreamJoin

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

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

a = (
      Stream.fromPath(f'{silver_path}/customers')
        .to(lambda df: df.withColumn('customer_id', df['id']).drop('id')) # drop duplicate id columns and rename customer's id to customer_id
        .to(lambda df: df.withColumn('customer_operation', df['operation']).drop('operation')) # drop duplicate operation columns and rename customer's oeration to customer_operation
        .to(lambda df: df.withColumn('customer_operation_date', df['operation_date']).drop('operation_date')) # drop duplicate operation_date columns and rename customer's operation_date to customer_operation_date
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )

b = (
      Stream.fromPath(f'{silver_path}/transactions')
      .to(lambda df: df.withColumnRenamed('id', 'transaction_id'))
      .primaryKeys('transaction_id')
      .sequenceBy('operation_date')
    )

c = (
      Stream.fromPath(f'{silver_path}/orders')
            .to(lambda df: df.withColumnRenamed('id', 'order_id'))
            .to(lambda df: df.withColumnRenamed('operation', 'order_operation'))
            .to(lambda df: df.withColumnRenamed('operation_date', 'order_operation_date'))
            .primaryKeys('order_id')
            .sequenceBy('order_operation_date')
    )

j = (
  a.join(b)
  .onKeys('customer_id')
#  .select('*')
  .join(c)
  .onKeys('transaction_id')
#  .select('*')
  .writeToPath(f'{gold_path}/joined')
#  .foreachBatch(mergeGold)
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .start()
)

# COMMAND ----------

aa = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
bb = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id')
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
cc = bb.join(aa, bb['customer_id'] == aa['customer_id']).drop(aa['customer_id']).join(oo, oo['transaction_id'] == bb['transaction_id']).drop(bb['transaction_id'])

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/joined').select(cc.columns)
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

print(df.exceptAll(cc).count())
print(cc.exceptAll(df).count())
