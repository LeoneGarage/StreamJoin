# Databricks notebook source
# MAGIC %run ./StreamJoin

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS
# MAGIC (
# MAGIC SELECT * FROM VALUES(1,2,3,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,3,5)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,null,3,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,null,3,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,3,3,null)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,null,3,null)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(null,null,3,null)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,3,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,3,null)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,4,6,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,5,6,4)
# MAGIC )
# MAGIC SELECT customer_id, transaction_id, order_id, product_id, ROW_NUMBER() over(partition by order_id, count order by customer_id desc, transaction_id desc, product_id desc) as rn
# MAGIC FROM (
# MAGIC SELECT *,
# MAGIC count(CASE WHEN (customer_id <=> lag_customer_id or customer_id is null) AND (transaction_id <=> lag_transaction_id or transaction_id is null) AND (product_id <=> lag_product_id or product_id is null) then null else 1 END) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as count
# MAGIC FROM (
# MAGIC SELECT *,
# MAGIC lag(customer_id, 1, customer_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_customer_id,
# MAGIC lag(transaction_id, 1, transaction_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_transaction_id,
# MAGIC lag(product_id, 1, product_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_product_id
# MAGIC FROM (
# MAGIC SELECT col1 as customer_id, col2 as transaction_id, col3 as order_id, col4 as product_id FROM data
# MAGIC )
# MAGIC )
# MAGIC )
# MAGIC ORDER BY customer_id desc, transaction_id desc, product_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW data AS
# MAGIC with batch AS (
# MAGIC SELECT customer_id, transaction_id, order_id, product_id, ROW_NUMBER() over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as rn_order_id, ROW_NUMBER() over(partition by order_id, count_customer_id order by customer_id desc, transaction_id desc, product_id desc) as rn_customer_id, ROW_NUMBER() over(partition by order_id, count_transaction_id order by customer_id desc, transaction_id desc, product_id desc) as rn_transaction_id, ROW_NUMBER() over(partition by order_id, count_product_id order by customer_id desc, transaction_id desc, product_id desc) as rn_product_id
# MAGIC FROM (
# MAGIC SELECT *,
# MAGIC count(CASE WHEN (customer_id <=> lag_customer_id or customer_id is null) THEN NULL else 1 END) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as count_customer_id, count(CASE WHEN (transaction_id <=> lag_transaction_id or transaction_id is null) THEN null else 1 END) as count_transaction_id, count(CASE WHEN (product_id <=> lag_product_id or product_id is null) then null else 1 END) as count_product_id over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as count
# MAGIC FROM (
# MAGIC SELECT *,
# MAGIC lag(customer_id, 1, customer_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_customer_id,
# MAGIC lag(transaction_id, 1, transaction_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_transaction_id,
# MAGIC lag(product_id, 1, product_id) over(partition by order_id order by customer_id desc, transaction_id desc, product_id desc) as lag_product_id
# MAGIC FROM (
# MAGIC SELECT customer_id, transaction_id, order_id, product_id FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch0`
# MAGIC )
# MAGIC )
# MAGIC )
# MAGIC )
# MAGIC SELECT order_id, customer_id, transaction_id, product_id FROM batch

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT order_id, count(*) as count FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch1`
# MAGIC GROUP BY order_id
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT order_id, customer_id, transaction_id, product_id, __rn FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch1`
# MAGIC WHERE order_id = '2e9e6a05-2b83-4d03-82f0-6cfb646510ed'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT order_id, customer_id, transaction_id, product_id FROM delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined`
# MAGIC WHERE order_id = '2e9e6a05-2b83-4d03-82f0-6cfb646510ed'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT a.order_id, a.customer_id, a.transaction_id, a.product_id FROM delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined` a
# MAGIC JOIN delta.`/Users/leon.eller@databricks.com/tmp/error/batch1` b ON a.order_id = b.order_id
# MAGIC WHERE a.product_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS
# MAGIC (
# MAGIC SELECT * FROM VALUES(1,null,3, 'a')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,3, 'a')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,4, 'b')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,4, 'c')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,4, 'd')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,4, 'e')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,3, 'f')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,3, 'g')
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,3,4, 'h')
# MAGIC )
# MAGIC SELECT *, CASE WHEN col1 = 1 then row_number() over(partition by col1, col2, col3 order by col2, col3) else 1000 end as rn FROM data ORDER BY col1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS
# MAGIC (
# MAGIC SELECT * FROM VALUES(1,2,3)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(1,2,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(2,2,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(null,2,4)
# MAGIC UNION ALL
# MAGIC SELECT * FROM VALUES(null,3,4)
# MAGIC )
# MAGIC SELECT * FROM data a
# MAGIC WHERE NOT EXISTS(SELECT * FROM data where a.col1 is null and col1 is not null and a.col2 = col2 and a.col3 = col3)

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
products_path = f"{root_path}/products"
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

d = (
      Stream.fromPath(f'{silver_path}/products')
            .to(lambda df: df.withColumnRenamed('id', 'product_id'))
            .to(lambda df: df.withColumnRenamed('item_name', 'product_name'))
            .primaryKeys('product_id')
            .sequenceBy('item_operation_date')
    )

# COMMAND ----------

def _mergeCondition(nonNullableKeys, nullableKeys, extraCond = ''):
  arr = []
  for i in range(0, len(nullableKeys)+1):
    nullKeys = nullableKeys.copy()
    t = list(itertools.combinations(nullKeys, i))
    for ii in range(0, len(t)):
      item = nonNullableKeys.copy()
      out = [f'u.{pk} = staged_updates.{pk}' for pk in item]
      for iii in range(0, len(t[ii])):
        item += [t[ii][iii]]
        out += [f'u.{t[ii][iii]} = staged_updates.{t[ii][iii]}']
      hasNullable = False
      for pk in nullKeys:
        if pk not in item:
          out += [f'(u.{pk} is null OR staged_updates.{pk} is null)']
          hasNullable = True
      arr += [f"({' AND '.join(out)}{extraCond if len(nullableKeys) > 0 else ''})"]
  return ' OR '.join(arr)

def _dedupBatch(batchDf, windowSpec, primaryKeys):
    if windowSpec is not None:
      batchDf = batchDf.withColumn('__row_number', F.row_number().over(windowSpec)).where('__row_number = 1')
    else:
      batchDf = batchDf.dropDuplicates(primaryKeys)
    return batchDf

# COMMAND ----------

primaryKeys = ['order_id', 'customer_id', 'transaction_id', 'product_id']
pks = [['customer_id'], ['customer_id', 'transaction_id', 'order_id', 'product_id']]
sequenceColumns = ['customer_operation_date', 'operation_date', 'order_operation_date', 'item_operation_date']

# COMMAND ----------

_mergeCondition(pks[0], pks[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE history delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE history delta.`/Users/leon.eller@databricks.com/tmp/error/batch0`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined` VERSION AS OF 2 WHERE order_id = 'f2242ba6-9587-47f6-9a9d-61d8f1b5fbd5'

# COMMAND ----------

deltaDF = spark.read.format('delta').option('versionAsOf', 1).load('/Users/leon.eller@databricks.com/tmp/demo/gold/joined')

# COMMAND ----------

cond = ' AND '.join([f'u.{pk} = staged_updates.{pk}' for pk in pks[0]] + [f'u.{pk} <=> staged_updates.{pk}' for pk in pks[1]])
if len(pks[1]) > 0:
  outerCondStr = _mergeCondition(pks[0], pks[1])
  outerCond = F.expr(outerCondStr)
  insertFilter = ' AND '.join([f'u.{pk} is null' for pk in pks[0]])
  updateFilter = ' AND '.join([f'u.{pk} is not null' for pk in pks[0]])
  outerWindowSpec = Window.partitionBy([f'__operation_flag'] + [f'u.{pk}' for pk in primaryKeys]).orderBy([f'u.{pk}' for pk in primaryKeys] + [F.desc(f'staged_updates.{sc}') for sc in sequenceColumns])
  dedupWindowSpec = Window.partitionBy([f'__u_{pk}' for pk in primaryKeys]).orderBy([F.desc(f'{pk}') for pk in primaryKeys])
  cond = '__rn = 1 AND ' + cond
  updateCols = {c: F.col(f'staged_updates.__u_{c}') for c in deltaDF.columns}
else:
  updateCols = {c: F.col(f'staged_updates.{c}') for c in deltaDF.columns}

windowSpec = None
if sequenceColumns is not None and len(sequenceColumns) > 0:
  windowSpec = Window.partitionBy(primaryKeys).orderBy([F.desc(sc) for sc in sequenceColumns])
  matchCondition = ' AND '.join([f'(u.{sc} is null OR u.{sc} <= staged_updates.{"__u_" if len(pks[1]) > 0 else ""}{sc})' for sc in sequenceColumns])
deltaTableColumns = deltaDF.columns
if outerCond is not None:
  batchSelect = [F.col(f'staged_updates.{c}').alias(f'__u_{c}') for c in deltaTableColumns] + [F.expr(f'CASE WHEN __operation_flag = 2 THEN staged_updates.{c} WHEN __operation_flag = 1 THEN u.{c} END AS {c}') for c in primaryKeys] + [F.when(F.expr('__operation_flag = 1'), F.row_number().over(outerWindowSpec)).otherwise(F.lit(2)).alias('__rn')]
  operationFlag = F.expr(f'CASE WHEN {updateFilter} THEN 1 WHEN {insertFilter} THEN 2 END').alias('__operation_flag')
  nullsCol = F.expr(' + '.join([f'CASE WHEN {pk} is not null THEN 0 ELSE 1 END' for pk in pks[1]]))
  stagedNullsCol = F.expr(' + '.join([f'CASE WHEN __u_{pk} is not null THEN 0 ELSE 1 END' for pk in pks[1]]))
  antiJoinCond = F.expr(' AND '.join([f'({outerCondStr})', '((u.__rn != 1 AND (u.__pk_nulls_count > staged_updates.__pk_nulls_count OR u.__u_pk_nulls_count > staged_updates.__u_pk_nulls_count)))']))

# COMMAND ----------

batchDf = spark.read.format('delta').option('versionAsOf', 1).load('/Users/leon.eller@databricks.com/tmp/error/batch0')
targetDf = deltaDF
u = targetDf.alias('u')
su = F.broadcast(batchDf).alias('staged_updates')
mergeDf = u.join(su, outerCond, 'right').select(F.col('*'), operationFlag).select(batchSelect).drop('__operation_flag').select(F.col('*'),
                                                                                                                               nullsCol.alias('__pk_nulls_count'),
                                                                                                                               stagedNullsCol.alias('__u_pk_nulls_count'))

# COMMAND ----------

display(mergeDf.where("product_id = '977d1a01-a0f9-4c0e-b17b-79b444311dc4'"))

# COMMAND ----------

batchDf = mergeDf.alias('u').join(mergeDf.alias('staged_updates'), antiJoinCond, 'left_anti')

# COMMAND ----------

display(batchDf.where("product_id = '977d1a01-a0f9-4c0e-b17b-79b444311dc4'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select u.order_id, u.customer_id, u.transaction_id, u.product_id, staged_updates.order_id, staged_updates.customer_id, staged_updates.transaction_id, staged_updates.product_id from delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined` VERSION AS OF 5 u
# MAGIC RIGHT JOIN delta.`/Users/leon.eller@databricks.com/tmp/error/batch0` VERSION AS OF 5 staged_updates ON (u.customer_id = staged_updates.customer_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.order_id is null OR staged_updates.order_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.order_id is null OR staged_updates.order_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND (u.order_id is null OR staged_updates.order_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.order_id = staged_updates.order_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.product_id = staged_updates.product_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.order_id is null OR staged_updates.order_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND (u.order_id is null OR staged_updates.order_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.order_id = staged_updates.order_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.product_id = staged_updates.product_id AND (u.transaction_id is null OR staged_updates.transaction_id is null) AND (u.order_id is null OR staged_updates.order_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.order_id = staged_updates.order_id AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.product_id = staged_updates.product_id AND (u.order_id is null OR staged_updates.order_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.order_id = staged_updates.order_id AND u.product_id = staged_updates.product_id AND (u.transaction_id is null OR staged_updates.transaction_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.order_id = staged_updates.order_id AND (u.product_id is null OR staged_updates.product_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.product_id = staged_updates.product_id AND (u.order_id is null OR staged_updates.order_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.order_id = staged_updates.order_id AND u.product_id = staged_updates.product_id AND (u.transaction_id is null OR staged_updates.transaction_id is null)) OR (u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.order_id = staged_updates.order_id AND u.product_id = staged_updates.product_id) OR (u.customer_id = staged_updates.customer_id AND u.customer_id = staged_updates.customer_id AND u.transaction_id = staged_updates.transaction_id AND u.order_id = staged_updates.order_id AND u.product_id = staged_updates.product_id)
# MAGIC where staged_updates.order_id = 'f2242ba6-9587-47f6-9a9d-61d8f1b5fbd5'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined` VERSION AS OF 3 where product_id = 'b1fc15c2-ab81-4575-bea5-e55917db8c31'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/Users/leon.eller@databricks.com/tmp/error/merge` VERSION AS OF 5 where __u_order_id = 'f2242ba6-9587-47f6-9a9d-61d8f1b5fbd5'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT row_number() over(partition by order_id, COALESCE(customer_id, f_customer_id), COALESCE(transaction_id, f_transaction_id), COALESCE(product_id, f_product_id) order by customer_id DESC, transaction_id DESC, product_id DESC) as rn, * FROM (
# MAGIC select first_value(customer_id) over(partition by order_id order by customer_id DESC, transaction_id DESC, product_id DESC) as f_customer_id, first_value(transaction_id) over(partition by order_id order by customer_id DESC, transaction_id DESC, product_id DESC) as f_transaction_id, first_value(product_id) over(partition by order_id order by customer_id DESC, transaction_id DESC, product_id DESC) as f_product_id, * from (
# MAGIC select customer_id, transaction_id, order_id, product_id from delta.`/Users/leon.eller@databricks.com/tmp/error/batch0` VERSION AS OF 5
# MAGIC UNION ALL
# MAGIC SELECT col1 as customer_id, col2 as transaction_id, col3 as order_id, col4 as product_id FROM VALUES('64506617-d7bc-4584-94cb-c5ebb2e6d6db','70be8862-514d-4a0f-93db-4cde24a2cc43','0d6dd69e-3fdb-4edd-ab6c-29762c87713c', /*'426d1e10-61a7-4f56-af33-403f781a7ea6'*/null)
# MAGIC UNION ALL
# MAGIC SELECT col1 as customer_id, col2 as transaction_id, col3 as order_id, col4 as product_id FROM VALUES(/*'64506617-d7bc-4584-94cb-c5ebb2e6d6db'*/null,'70be8862-514d-4a0f-93db-4cde24a2cc43','0d6dd69e-3fdb-4edd-ab6c-29762c87713c', '426d1e10-61a7-4f56-af33-403f781a7ea6')
# MAGIC UNION ALL
# MAGIC SELECT col1 as customer_id, col2 as transaction_id, col3 as order_id, col4 as product_id FROM VALUES(/*'64506617-d7bc-4584-94cb-c5ebb2e6d6db'*/null,/*'70be8862-514d-4a0f-93db-4cde24a2cc43'*/null,'0d6dd69e-3fdb-4edd-ab6c-29762c87713c', '426d1e10-61a7-4f56-af33-403f781a7ea6')
# MAGIC )
# MAGIC )
# MAGIC where order_id = '0d6dd69e-3fdb-4edd-ab6c-29762c87713c'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/Users/leon.eller@databricks.com/tmp/error/batch0` --VERSION AS OF 5 where order_id = '0d6dd69e-3fdb-4edd-ab6c-29762c87713c'

# COMMAND ----------

# batch0 = spark.read.format('delta').load('/Users/leon.eller@databricks.com/tmp/error/batch0')
batch2 = spark.read.format('delta').load('/Users/leon.eller@databricks.com/tmp/error/batch2')

# COMMAND ----------

display(batch0.exceptAll(batch.select(batch0.columns)))

# COMMAND ----------

display(batch.where("order_id = '6938d765-5932-471b-a29a-7f03843b6563'"))

# COMMAND ----------

display(batch.select(batch0.columns).exceptAll(batch0))

# COMMAND ----------

spark.read.format('delta').load(f'{gold_path}/joined').createOrReplaceTempView('data')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined`

# COMMAND ----------

display(DeltaTable.forPath(spark, f"{gold_path}/joined").history())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/demo/gold/joined` VERSION AS OF 4 where order_id = '0b5a4df9-af99-40c0-87d0-f91b7d7d0087'

# COMMAND ----------

path = (a.join(b, 'right')
  .onKeys('customer_id')
  .join(c, 'left').stagingPath())
print(path)
spark.read.format('delta').load(f"{path}/data").createOrReplaceTempView('data')

# COMMAND ----------

display(DeltaTable.forPath(spark, f"{path}/data").history())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch0`  VERSION AS OF 4 WHERE order_id = '0b5a4df9-af99-40c0-87d0-f91b7d7d0087'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch` VERSION AS OF 9 WHERE order_id = '0b5a4df9-af99-40c0-87d0-f91b7d7d0087'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM data
# MAGIC WHERE order_id = '0b5a4df9-af99-40c0-87d0-f91b7d7d0087'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC SELECT *, count(*) OVER(PARTITION BY u.customer_id, u.transaction_id, u.order_id, u.product_id ORDER BY u.customer_id, u.transaction_id, u.order_id, u.product_id) as count FROM data u
# MAGIC JOIN delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch` staged_updates ON ((((staged_updates.__rn = 1) AND (u.customer_id <=> staged_updates.customer_id)) AND (u.transaction_id <=> staged_updates.transaction_id)) AND ((u.order_id <=> staged_updates.order_id) AND (u.product_id <=> staged_updates.product_id)))
# MAGIC )
# MAGIC WHERE count > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM data where order_id = '1c98b8e9-237f-43a1-984b-a3321ab67ee0'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch` where order_id = '1c98b8e9-237f-43a1-984b-a3321ab67ee0'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC SELECT *, count(*) OVER(PARTITION BY customer_id, transaction_id, order_id, product_id order by customer_id, transaction_id, order_id, product_id) as count FROM delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch0`
# MAGIC )
# MAGIC WHERE count > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC SELECT *, count(*) OVER(PARTITION BY customer_id, transaction_id, order_id, product_id order by customer_id, transaction_id, order_id, product_id) as count FROM delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch` WHERE __rn = 1
# MAGIC )
# MAGIC WHERE count > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DeSCRIBE HISTORY delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch0`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DeSCRIBE HISTORY delta.`/Users/leon.eller@databricks.com/tmp/error/gold_batch`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select customer_id, transaction_id, order_id, count(*) as count from delta.`/Users/leon.eller@databricks.com/tmp/error/batch` where __rn_target = 1 and __rn_source = 1
# MAGIC GROUP BY customer_id, transaction_id, order_id
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/Users/leon.eller@databricks.com/tmp/error/batch0`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data u
# MAGIC RIGHT JOIN delta.`/Users/leon.eller@databricks.com/tmp/error/batch` staged_updates ON ((u.transaction_id = staged_updates.transaction_id) AND (((((u.order_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1)) AND ((u.customer_id IS NULL)
# MAGIC OR
# MAGIC (u.customer_id = staged_updates.customer_id)))
# MAGIC OR ((u.order_id = staged_updates.order_id) AND ((((u.customer_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1))
# MAGIC OR (u.customer_id = staged_updates.customer_id)))))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT u.customer_id, u.transaction_id, u.order_id, staged_updates.customer_id, staged_updates.transaction_id, staged_updates.order_id, count(*) as count FROM data u
# MAGIC RIGHT JOIN delta.`/Users/leon.eller@databricks.com/tmp/error/batch` staged_updates ON ((u.transaction_id = staged_updates.transaction_id) AND (((((u.order_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1)) AND ((u.customer_id IS NULL)
# MAGIC OR
# MAGIC (u.customer_id = staged_updates.customer_id)))
# MAGIC OR ((u.order_id = staged_updates.order_id) AND ((((u.customer_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1))
# MAGIC OR (u.customer_id = staged_updates.customer_id)))))
# MAGIC 
# MAGIC --WHERE staged_updates.customer_id = '0003fd38-39b2-4b2d-82d3-11fc1549ba3e'
# MAGIC --WHERE u.transaction_id is not null OR u.customer_id is not null OR u.order_id is not null
# MAGIC -- EXCEPT ALL
# MAGIC -- SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch`
# MAGIC GROUP BY staged_updates.customer_id, staged_updates.transaction_id, staged_updates.order_id, u.customer_id, u.transaction_id, u.order_id
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY count DESC, staged_updates.customer_id, staged_updates.transaction_id, staged_updates.order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM data u
# MAGIC RIGHT JOIN (SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch`) staged_updates ON ((u.transaction_id = staged_updates.transaction_id) AND (((((u.order_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1)) AND ((u.customer_id IS NULL)
# MAGIC OR
# MAGIC (u.customer_id = staged_updates.customer_id)))
# MAGIC OR ((u.order_id = staged_updates.order_id) AND ((((u.customer_id IS NULL) AND (staged_updates.__rn_target = 1)) AND (staged_updates.__rn_source = 1))
# MAGIC OR (u.customer_id = staged_updates.customer_id)))))
# MAGIC 
# MAGIC WHERE staged_updates.customer_id = '00891de1-fbe7-42aa-8bb7-e095f6eedcbc' and staged_updates.transaction_id = 'f81efeb9-9bb3-4048-b480-8e391d9c1835' and staged_updates.order_id = 'e47d6ecb-f67b-4adc-a720-b6b6149fe307'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM data
# MAGIC WHERE transaction_id = 'f81efeb9-9bb3-4048-b480-8e391d9c1835'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch0`
# MAGIC WHERE customer_id = '0003fd38-39b2-4b2d-82d3-11fc1549ba3e' and transaction_id = '79c40129-69d4-47ca-8148-8667fb17b368' and order_id = 'f369cc74-fae0-4360-8c7f-6987e4751b16'--AND __rn_target = 1 AND __rn_source = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/tmp/error/batch`
# MAGIC WHERE customer_id = '0003fd38-39b2-4b2d-82d3-11fc1549ba3e' and transaction_id = '79c40129-69d4-47ca-8148-8667fb17b368' and order_id = 'f369cc74-fae0-4360-8c7f-6987e4751b16' --AND __rn_target = 1 AND __rn_source = 1
