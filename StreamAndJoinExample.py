# Databricks notebook source
# MAGIC %run ./StreamJoin

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "2GB")

# COMMAND ----------

user = 'leon.eller@databricks.com'#dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

root_path = f"/Users/{user}/tmp/demo/cdc_raw"
customer_path = f"{root_path}/customers"
transaction_path = f"{root_path}/transactions"
orders_path = f"{root_path}/orders"
products_path = f"{root_path}/products"
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

dbutils.fs.rm(f'/Users/{user}/tmp/error', True)

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

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/products` (
id STRING,
item_name STRING,
item_operation STRING,
item_operation_date STRING,
order_id STRING,
price STRING) USING delta
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

products_stream = (
       spark.readStream.format("cloudFiles")
          .option('cloudFiles.format', 'json')
          .option('cloudFiles.schemaLocation', f'{schemaLocation}/products')
          .option('cloudFiles.maxBytesPerTrigger', 2000000)
          .load(products_path)
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

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
(products_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/products')
  .start(f'{silver_path}/products'))

# COMMAND ----------

def mergeGold(batchDf, batchId):
  deltaTable = DeltaTable.forPath(spark, f'{gold_path}/joined')
  deltaTable.alias("u").merge(
    source = batchDf.alias("staged_updates"),
    condition = F.expr("u.id = staged_updates.id AND u.customer_id = staged_updates.customer_id")) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

a = (
      Stream.fromPath(f'{silver_path}/customers')
        .to(lambda df: df.withColumnRenamed('id', 'customer_id')) # drop duplicate id columns and rename customer's id to customer_id
        .to(lambda df: df.withColumnRenamed('operation', 'customer_operation')) # drop duplicate operation columns and rename customer's oeration to customer_operation
        .to(lambda df: df.withColumnRenamed('operation_date', 'customer_operation_date')) # drop duplicate operation_date columns and rename customer's operation_date to customer_operation_date
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )

b = (
      Stream.fromPath(f'{silver_path}/transactions')
      .to(lambda df: df.withColumnRenamed('id', 'transaction_id'))
      .to(lambda df: df.withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100))
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

d = (
      Stream.fromPath(f'{silver_path}/products')
            .to(lambda df: df.withColumnRenamed('id', 'product_id'))
            .to(lambda df: df.withColumnRenamed('item_name', 'product_name'))
            .to(lambda df: df.withColumnRenamed('order_id', 'product_order_id'))
            .primaryKeys('product_id')
            .sequenceBy('item_operation_date')
    )

j = (
  a.join(b, 'right')
  .onKeys('customer_id').partitionBy(prune('date'))
  .join(c, 'right')
  .onKeys('transaction_id').partitionBy(prune('date'))
  .join(d, 'left')
  .on((d['product_name'] == c['item_name']) & (d['product_name'] == F.lit('Small Towels')))
#   .on(lambda l, r: (r['product_name'] == l['item_name']) & (r['product_name'] == F.lit('Small Towels')))
#  .drop(d['order_id'])
#   .to(lambda f, l, r: f.drop(r['order_id']))
#   .on(lambda l, r: l['item_name'] == r['product_name'])
#  .hintJoinKeys('item_name', 'product_name')
#  .onKeys('order_id')
  .writeToPath(f'{gold_path}/joined')
#  .foreachBatch(mergeGold)
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .queryName('gold')
  .start()
)

# COMMAND ----------

# path = (a.join(b, 'right')
#   .onKeys('customer_id')).stagingPath()
# display(DeltaTable.forPath(spark, f'{path}/data').history())

# COMMAND ----------

# dbutils.fs.rm(f'{checkpointLocation}/test/cp', True)
# dbutils.fs.rm(f'{silver_path}/test', True)

# inDf = (
#          spark.readStream
# #         .option('ignoreChanges', True)
#            .option("readChangeFeed", "true")
#            .format('delta')
#            .load('/Users/leon.eller@databricks.com/tmp/demo/silver/$$_customers_transactions/right/b72b7fd1451c20a2f17f376784a9a5ad718c0a58fbfe344f642902c191eca371/data')
#        )

# inDf = inDf.drop('_change_type', '_commit_timestamp', '_commit_version').where('date = 20220300')

# (
#   inDf.writeStream.format('delta')
#       .option('checkpointLocation', f'{checkpointLocation}/test/cp')
#       .start(f'{silver_path}/test')
# )

# COMMAND ----------

aa = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
bb = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
pp = spark.read.format('delta').load(f'{silver_path}/products').withColumnRenamed('id', 'product_id').withColumnRenamed('item_name', 'product_name')
aa_bb = aa.join(bb, bb['customer_id'] == aa['customer_id']).drop(aa['customer_id'])
aa_bb_oo = aa_bb.join(oo, oo['transaction_id'] == aa_bb['transaction_id']).drop(aa_bb['transaction_id'])
cc = aa_bb_oo.join(pp, pp['order_id'] == aa_bb_oo['order_id']).drop(pp['order_id'])
cc.count()

# COMMAND ----------

aa = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
bb = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id').withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100)
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
pp = spark.read.format('delta').load(f'{silver_path}/products').withColumnRenamed('id', 'product_id').withColumnRenamed('item_name', 'product_name').withColumnRenamed('order_id', 'product_order_id')
aa_bb = aa.join(bb, bb['customer_id'] == aa['customer_id'], 'right').drop(aa['customer_id'])
aa_bb_oo = aa_bb.join(oo, oo['transaction_id'] == aa_bb['transaction_id'], 'right').drop(aa_bb['transaction_id'])
cc = aa_bb_oo.join(pp, (pp['product_name'] == aa_bb_oo['item_name']) & (pp['product_name'] == F.lit('Small Towels')), 'left')#.drop(pp['order_id'])
cc.count()

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/joined')
df.count()

# COMMAND ----------

df_cols = df.columns
df_cols.sort()
cc_cols = cc.columns
cc_cols.sort()

# COMMAND ----------

print(df.select(df_cols).exceptAll(cc.select(cc_cols)).count())
print(cc.select(cc_cols).exceptAll(df.select(df_cols)).count())

# COMMAND ----------

# display(DeltaTable.forPath(spark, f'{gold_path}/joined').history())

# COMMAND ----------

# a = df.withColumn('_rn', F.row_number().over(Window.partitionBy(['customer_id', 'transaction_id', 'order_id', 'product_id']).orderBy(['customer_id']))).where('_rn = 1')

# COMMAND ----------

# display(a.select(df_cols).exceptAll(cc.select(cc_cols)))

# COMMAND ----------

# path = (a.join(b, 'right')).stagingPath()
# datatdf = spark.read.format('delta').load(f'{path}/data')

# COMMAND ----------

# display(datatdf.where("transaction_id = '91c71211-576a-46c1-bcde-20572e490809'"))

# COMMAND ----------

# path = (a.join(b, 'right')
#   .onKeys('customer_id')
#   .join(c)).stagingPath()
# dataodf = spark.read.format('delta').option('versionAsOf', 5).load(f'{path}/data')

# COMMAND ----------

# DeltaTable.forPath(spark, (a.join(b, 'right')
#   .onKeys('customer_id')
#   .join(c)).stagingPath() + '/data').history().display()

# COMMAND ----------

# dataodf.createOrReplaceTempView('test')

# COMMAND ----------

# %sql

# SELECT *, row_number() over(partition by transaction_id, order_id, CASE WHEN customer_id is null then max else customer_id end order by customer_id DESC) as rn FROM (
# SELECT customer_id, transaction_id, order_id, max(customer_id) over(partition by transaction_id, order_id order by customer_id DESC ) as max
#   from test
# )
# where order_id = 'a60833dd-9df1-401c-9d90-97c4daa1a70e'

# COMMAND ----------

#display(dataodf.where("order_id = '2e949785-1ffe-4fdf-b814-9f1d09cb3ae2'"))

# COMMAND ----------

#display(aa_bb_oo.where("order_id = 'a60833dd-9df1-401c-9d90-97c4daa1a70e'"))

# COMMAND ----------

# df = spark.read.format('delta').option('versionAsOf', 10).load(f'{gold_path}/joined')
# display(df.select(df_cols).where("product_id = '6a7ba336-932e-4024-99ae-75f3c99acd47'"))

# COMMAND ----------

display(df.select(df_cols).where("customer_id = '9b977761-628e-486f-a7f8-9433ac1cd0b2' and product_id = '7f60df08-0f03-4e2e-a43f-3b43c1687a15'"))

# COMMAND ----------

display(cc.select(cc_cols).where("customer_id = '9b977761-628e-486f-a7f8-9433ac1cd0b2' and product_id = '7f60df08-0f03-4e2e-a43f-3b43c1687a15'"))

# COMMAND ----------

# display(cc.select(cc_cols).exceptAll(df.select(df_cols)))

# COMMAND ----------

# display(df.select(df_cols).exceptAll(cc.select(cc_cols)))

# COMMAND ----------

# display(df.select(df_cols).where("customer_id = 'cd3b7176-ef20-4120-91c0-ff470718d419' and order_id = '411173c8-337f-441c-9586-d68418fc3c66'"))

# COMMAND ----------

# display(cc.select(cc_cols).where("customer_id = 'cd3b7176-ef20-4120-91c0-ff470718d419' and order_id = '411173c8-337f-441c-9586-d68418fc3c66'"))

# COMMAND ----------

# display(df.select('transaction_id').exceptAll(cc.select('transaction_id')))

# COMMAND ----------

# display(df.where("order_id = '4f17f32c-2cc2-4a03-a97b-a164fab8e266'"))

# COMMAND ----------

# display(cc.where("order_id = '4f17f32c-2cc2-4a03-a97b-a164fab8e266'"))

# COMMAND ----------

# display(df.select(cc_cols).where("transaction_id = '32dcfab0-6039-4c85-9f83-14a9f66bd0f6'"))

# COMMAND ----------

# display(cc.select(cc_cols).where("transaction_id = '32dcfab0-6039-4c85-9f83-14a9f66bd0f6'"))
