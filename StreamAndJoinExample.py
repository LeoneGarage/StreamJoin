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

d = (
      Stream.fromPath(f'{silver_path}/products')
            .to(lambda df: df.withColumnRenamed('id', 'product_id'))
            .to(lambda df: df.withColumnRenamed('item_name', 'product_name'))
            .primaryKeys('product_id')
            .sequenceBy('item_operation_date')
    )

j = (
  a.join(b, 'right')
  .onKeys('customer_id')
  .join(c)
  .onKeys('transaction_id')
  .join(d, 'left')
  .onKeys('order_id')
  .writeToPath(f'{gold_path}/joined')
#  .foreachBatch(mergeGold)
  .option("checkpointLocation", f'{checkpointLocation}/gold/joined')
  .queryName('gold')
  .start()
)

# COMMAND ----------

# %sql

# SELECT * FROM ( SELECT row_number() over(partition by 1 order by 1) as data_rn, * FROM data) u
# JOIN (SELECT row_number() over(partition by 1 order by 1) as batch_rn, * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch`) staged_updates ON ((u.transaction_id = staged_updates.transaction_id) AND ((((staged_updates.__rn = 1) AND (u.order_id IS NULL)) AND ((u.customer_id IS NULL) OR (u.customer_id = staged_updates.customer_id))) OR ((u.order_id = staged_updates.order_id) AND (((u.customer_id IS NULL) AND (staged_updates.__rn = 1)) OR (u.customer_id = staged_updates.customer_id)))))
# WHERE staged_updates.transaction_id = 'a94998e8-4eb6-4919-89e2-996fedced5a6'--u.data_rn=29894
# -- GROUP BY u.data_rn
# -- order by count desc

# COMMAND ----------

# %sql

# SELECT *, row_number() OVER(partition by customer_id, transaction_id, order_id ORDER BY customer_id) as rn FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch` WHERE transaction_id = 'a94998e8-4eb6-4919-89e2-996fedced5a6'

# COMMAND ----------

aa = spark.read.format('delta').load(f'{silver_path}/customers').withColumnRenamed('id', 'customer_id').withColumnRenamed('operation', 'customer_operation').withColumnRenamed('operation_date', 'customer_operation_date')
bb = spark.read.format('delta').load(f'{silver_path}/transactions').withColumnRenamed('id', 'transaction_id')
oo = spark.read.format('delta').load(f'{silver_path}/orders').withColumnRenamed('id', 'order_id').withColumnRenamed('operation', 'order_operation').withColumnRenamed('operation_date', 'order_operation_date')
pp = spark.read.format('delta').load(f'{silver_path}/products').withColumnRenamed('id', 'product_id').withColumnRenamed('item_name', 'product_name')
aa_bb = aa.join(bb, bb['customer_id'] == aa['customer_id'], 'right').drop(aa['customer_id'])
aa_bb_oo = aa_bb.join(oo, oo['transaction_id'] == aa_bb['transaction_id']).drop(aa_bb['transaction_id'])
cc = aa_bb_oo.join(pp, pp['order_id'] == aa_bb_oo['order_id'], 'left').drop(pp['order_id'])
cc.count()

# COMMAND ----------

# ab = spark.read.format('delta').load(f"{a.join(b, 'left').stagingPath()}/data")
# ab_cols = ab.columns
# ab_cols.sort()
# aa_bb_cols = aa_bb.columns
# aa_bb_cols.sort()
# print(ab.select(ab_cols).exceptAll(aa_bb.select(aa_bb_cols)).count())
# print(aa_bb.select(aa_bb_cols).exceptAll(ab.select(ab_cols)).count())

# COMMAND ----------

df = spark.read.format('delta').load(f'{gold_path}/joined').select(cc.columns)
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

# display(df.select(df_cols).where("product_id = 'e87324a3-1e78-4835-a117-bbb3fc36bb35'"))

# COMMAND ----------

# display(cc.select(cc_cols).where("product_id = 'e87324a3-1e78-4835-a117-bbb3fc36bb35'"))

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
