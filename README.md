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
