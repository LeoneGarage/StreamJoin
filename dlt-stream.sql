-- Databricks notebook source
CREATE STREAMING LIVE TABLE customer
AS SELECT * FROM cloud_files('/Users/leon.eller@databricks.com/tmp/demo/cdc_raw/customers', 'json')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE transaction
AS SELECT * FROM cloud_files('/Users/leon.eller@databricks.com/tmp/demo/cdc_raw/transactions', 'json')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE order
AS SELECT * FROM cloud_files('/Users/leon.eller@databricks.com/tmp/demo/cdc_raw/orders', 'json')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE joined (
)
AS SELECT c.address, c.email, c.firstname, c.id as customer_id, t.amount, t.id as transaction_id, t.item_count, o.id as order_id, o.item_name FROM stream(LIVE.customer) c
  JOIN stream(live.transaction) t ON c.id = t.customer_id
  JOIN stream(live.order) o ON t.id = o.transaction_id

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT * FROM json.`/Users/leon.eller@databricks.com/tmp/demo/cdc_raw/orders`
