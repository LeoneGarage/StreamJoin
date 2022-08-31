# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %pip install faker_commerce

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
import faker_commerce
from collections import OrderedDict 
import uuid
from functools import reduce

numCustomers = 1000000
numTransactions = 500000
numOrders = 5000000
rowsPerPartition = 10000
numProducts = 50000000

folder = f"/Users/{user}/tmp/demo/cdc_raw"

# COMMAND ----------

fake = Faker()
fake.add_provider(faker_commerce.Provider)
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_this_year(before_today=True).strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()))

# COMMAND ----------

print("Generating the data...")  

df = spark.range(0, numCustomers)
df = df.withColumn("id", fake_id())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("operation", fake_operation())
df = df.withColumn("operation_date", fake_date())

df.repartition(int(numCustomers / rowsPerPartition)).write.format("json").mode("overwrite").save(folder+"/customers")

df = spark.range(0, numTransactions)
df = df.withColumn("id", fake_id())
df = df.withColumn("transaction_date", fake_date())
df = df.withColumn("amount", F.round(F.rand()*1000))
df = df.withColumn("item_count", F.round(F.rand()*10))
df = df.withColumn("operation", fake_operation())
df = df.withColumn("operation_date", fake_date())
#Join with the customer to get the same IDs generated.
df = df.withColumn("t_id", F.monotonically_increasing_id()).join(spark.read.json(folder+"/customers").sample(withReplacement = True, fraction = 1.0).selectExpr("id as customer_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
df.repartition(int(numTransactions / rowsPerPartition)).write.format("json").mode("overwrite").save(folder+"/transactions")

df = spark.range(0, numOrders)
fake_date_later = F.udf(lambda:fake.date_this_year(before_today=False, after_today=True).strftime("%m-%d-%Y %H:%M:%S"))
fake_product_name = F.udf(lambda: fake.ecommerce_name())
df = df.withColumn("id", fake_id())
df = df.withColumn("delivery_date", fake_date_later())
df = df.withColumn("item_name", fake_product_name())
df = df.withColumn("operation", fake_operation())
df = df.withColumn("operation_date", fake_date())
transDf = spark.read.json(folder+"/transactions").sample(withReplacement = True, fraction = 1.0)
for c in range(int(numOrders / numTransactions) - 1):
  transDf = transDf.unionByName(spark.read.json(folder+"/transactions").sample(withReplacement = True, fraction = 1.0))
df = df.withColumn("t_id", F.monotonically_increasing_id()).join(transDf.selectExpr("id as transaction_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
df.repartition(int(numOrders / rowsPerPartition)).write.format("json").mode("overwrite").save(folder+"/orders")

# COMMAND ----------

df = spark.range(0, numProducts)
fake_product_name = F.udf(lambda: fake.ecommerce_name())
df = df.withColumn("id", fake_id())
df = df.withColumn("item_name", fake_product_name())
df = df.withColumn("item_operation", fake_operation())
df = df.withColumn("item_operation_date", fake_date())
df = df.withColumn("price", F.round(F.rand()*10))
ordersDf = spark.read.json(folder+"/orders").sample(withReplacement = True, fraction = 1.0)
for c in range(int(numProducts / numOrders) - 1):
  ordersDf = ordersDf.unionByName(spark.read.json(folder+"/orders").sample(withReplacement = True, fraction = 1.0))
df = df.withColumn("t_id", F.monotonically_increasing_id()).join(ordersDf.selectExpr("id as order_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
df.repartition(int(numProducts / rowsPerPartition)).write.format("json").mode("overwrite").save(folder+"/products")

# COMMAND ----------

spark.read.json(folder+"/customers").display()

# COMMAND ----------

t = spark.read.json(folder+"/transactions")
c = spark.read.json(folder+"/customers")
o = spark.read.json(folder+"/orders")
p = spark.read.json(folder+"/products")

# COMMAND ----------

j1 = t.join(c, t['customer_id'] == c['id'])
display(j1.groupBy('customer_id').agg(F.count('*').alias('count')).where('count > 1'))

# COMMAND ----------

#display(t.groupBy('customer_id').agg(F.count('*').alias('count')).where('count > 1'))
j2 = j1.join(o, t['id'] == p['transaction_id'])
display(j2.groupBy('transaction_id').agg(F.count('*').alias('count')).where('count > 1'))
