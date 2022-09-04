# Databricks notebook source
# MAGIC %run ./StreamJoin

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

# batch0 = spark.read.format('delta').load('/Users/leon.eller@databricks.com/tmp/error/batch0')
batch = spark.read.format('delta').load('/Users/leon.eller@databricks.com/tmp/error/batch')

# COMMAND ----------

display(batch0.exceptAll(batch.select(batch0.columns)))

# COMMAND ----------

display(batch.where("order_id = '6938d765-5932-471b-a29a-7f03843b6563'"))

# COMMAND ----------

display(batch.select(batch0.columns).exceptAll(batch0))

# COMMAND ----------

spark.read.format('delta').load(f'{gold_path}/joined').createOrReplaceTempView('data')

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
