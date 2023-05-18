# StreamJoin

A framework for running incremental joins and aggregation over structured streaming CDF feeds from Delta tables.
So you can have data streaming in from anywhere and landing as tables in Delta in Bronze or Silver layers and then have streaming incremental joins and aggregation happening downstream from that.

An example in python:
```
%run "StreamJoin"

c = (
      Stream.fromPath(f'{silver_path}/customers')
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )
t = (
    Stream.fromPath(f'{silver_path}/transactions')
    .primaryKeys('transaction_id')
    .sequenceBy('operation_date')
  )

j = (
  t.join(c, 'left')
  .onKeys('customer_id')
  .groupBy("customer_id")
  .agg(F.sum("amount").alias("total_amount"))
  .writeToPath(f'{gold_path}/aggs')
  .option("checkpointLocation", f'{checkpointLocation}/gold/aggs')
  .queryName(f'{gold_path}/aggs')
  .start()
)
```
or
```
%run "StreamJoin"

c = (
      Stream.fromPath(f'{silver_path}/customers')
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )
t = (
    Stream.fromPath(f'{silver_path}/transactions')
    .primaryKeys('transaction_id')
    .sequenceBy('operation_date')
  )

j = (
  t.join(c, 'left')
  .on(t['customer_id'] == c['customer_id'])
  .groupBy("customer_id")
  .agg(F.sum("amount").alias("total_amount"))
  .writeToPath(f'{gold_path}/aggs')
  .option("checkpointLocation", f'{checkpointLocation}/gold/aggs')
  .queryName(f'{gold_path}/aggs')
  .start()
)
```
Unique primary keys (.primaryKeys()) are required per table for joins to ensure incremental merges have unique keys to merge on.
Sequence columns (.sequenceBy()) is required to ensure correct ordered processing/merging on rows from CDF.
To use it put
```%run "StreamJoin"```
at the top of your Notebook.

The diagram below depicts the operation steps assuming the following conceptual example:
```
c = (
      Stream.fromPath(f'{silver_path}/customers')
        .primaryKeys('customer_id')
        .sequenceBy('customer_operation_date')
    )
t = (
    Stream.fromPath(f'{silver_path}/transactions')
    .primaryKeys('transaction_id')
    .sequenceBy('operation_date')
  )
o = (
    Stream.fromPath(f'{silver_path}/orders')
    .primaryKeys('order_id')
    .sequenceBy('order_operation_date')
  )
p = (
    Stream.fromPath(f'{silver_path}/products')
    .primaryKeys('product_id')
    .sequenceBy('product_operation_date')
  )
j = (
  c.join(t, 'right')
  .onKeys('customer_id')
  .join(o, 'left')
  .onKeys('transaction_id')
  .join(p)
  .onKeys('product_id')
  .groupBy("customer_id")
  .agg(F.sum("amount").alias("total_amount"))
  .writeToPath(f'{gold_path}/aggs')
  .option("checkpointLocation", f'{checkpointLocation}/gold/aggs')
  .queryName(f'{gold_path}/aggs')
  .start()
)
```
![Conceptual Diagram of join and aggregation steps](https://raw.githubusercontent.com/LeoneGarage/StreamJoin/main/StreamJoin.png)

If you want to run tests, run GenerateData Notebook to generate customer, transaction, orders, and products tables first.
