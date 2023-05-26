# Databricks notebook source
# MAGIC %run ../StreamJoin

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pathlib import Path

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = Path(dbutils.notebook.entry_point.getDbutils().notebook().getContext().extraContext().apply('notebook_path'))
notebook_name = path.parts[len(path.parts)-1]

# COMMAND ----------

root_path = f"/Users/{user}/tmp/demo/cdc_raw"
customer_path = f"{root_path}/customers"
transaction_path = f"{root_path}/transactions"
orders_path = f"{root_path}/orders"
products_path = f"{root_path}/products"
checkpointLocation = f"/Users/{user}/tmp/demo/{notebook_name}/cp"
schemaLocation = f"/Users/{user}/tmp/demo/schema"
# bronze_path = f"/Users/{user}/tmp/demo/bronze"
silver_path = f"/Users/{user}/tmp/demo/{notebook_name}/silver"
gold_path = f"/Users/{user}/tmp/demo/{notebook_name}/gold"

# COMMAND ----------

dbutils.fs.rm(checkpointLocation, True)
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE delta.`{silver_path}/transactions` ( amount STRING, customer_id STRING, id STRING, item_count STRING, operation STRING, operation_date STRING, transaction_date STRING) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE delta.`{silver_path}/customers` ( address STRING, email STRING, firstname STRING, id STRING, lastname STRING, operation STRING, operation_date STRING)
USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)
"""
)

# COMMAND ----------

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/orders` ( delivery_date string,
id string,
item_name string,
operation string,
operation_date string,
transaction_id string) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)
''')

# COMMAND ----------

spark.sql(f'''
CREATE TABLE delta.`{silver_path}/products` (
id STRING,
item_name STRING,
item_operation STRING,
item_operation_date STRING,
order_id STRING,
price STRING) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)
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
customer_query = (customer_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/customers')
  .trigger(availableNow=True)
  .queryName(f'{silver_path}/customers')
  .start(f'{silver_path}/customers'))

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
transaction_query = (transaction_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/transactions')
  .trigger(availableNow=True)
  .queryName(f'{silver_path}/transactions')
  .start(f'{silver_path}/transactions'))

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
orders_query = (orders_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/orders')
  .trigger(availableNow=True)
  .queryName(f'{silver_path}/orders')
  .start(f'{silver_path}/orders'))

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
products_query = (products_stream
  .writeStream
  .format('delta')
  .option('checkpointLocation', f'{checkpointLocation}/silver/products')
  .trigger(availableNow=True)
  .queryName(f'{silver_path}/products')
  .start(f'{silver_path}/products'))

# COMMAND ----------

# DBTITLE 1,Streams to Test
c = (
      Stream.fromPath(f'{silver_path}/customers')
        .to(lambda df: df.withColumnRenamed('id', 'customer_id')) # drop duplicate id columns and rename customer's id to customer_id
        .to(lambda df: df.withColumnRenamed('operation', 'customer_operation')) # drop duplicate operation columns and rename customer's oeration to customer_operation
        .to(lambda df: df.withColumnRenamed('operation_date', 'customer_operation_date')) # drop duplicate operation_date columns and rename customer's operation_date to customer_operation_date
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )

t = (
      Stream.fromPath(f'{silver_path}/transactions')
      .to(lambda df: df.withColumnRenamed('id', 'transaction_id'))
      .to(lambda df: df.withColumn('date', F.year(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 10000 + F.month(F.to_date('operation_date', 'MM-dd-yyyy HH:mm:ss')) * 100))
      .primaryKeys('transaction_id')
      .sequenceBy('operation_date')
    )

o = (
      Stream.fromPath(f'{silver_path}/orders')
            .to(lambda df: df.withColumnRenamed('id', 'order_id'))
            .to(lambda df: df.withColumnRenamed('operation', 'order_operation'))
            .to(lambda df: df.withColumnRenamed('operation_date', 'order_operation_date'))
            .primaryKeys('order_id')
            .sequenceBy('order_operation_date')
    )

p = (
      Stream.fromPath(f'{silver_path}/products')
            .to(lambda df: df.withColumnRenamed('id', 'product_id'))
            .to(lambda df: df.withColumnRenamed('item_name', 'product_name'))
            .primaryKeys('product_id')
            .sequenceBy('item_operation_date')
    )

# COMMAND ----------

def awaitInputTermination():
  customer_query.awaitTermination()
  transaction_query.awaitTermination()
  orders_query.awaitTermination()
  products_query.awaitTermination()

# COMMAND ----------

def compare_dataframes(resultDf, expectedDf):
  result_cols = resultDf.columns
  result_cols.sort()
  expected_cols = expectedDf.columns
  expected_cols.sort()

  t1 = resultDf.select(result_cols).exceptAll(expectedDf.select(expected_cols)).count()
  t2 = expectedDf.select(expected_cols).exceptAll(resultDf.select(result_cols)).count()

  assert t1 == 0, f"result rows compared to expected rows have {t1} unmatched rows in result"
  assert t2 == 0, f"expected rows compared to result rows have {t2} unmatched rows in expected"

  print("Passed")
  return True
