# Databricks notebook source
dbutils.widgets.text("num_parallel_tests", "4", "Number of tests to run in parallel")

# COMMAND ----------

# MAGIC %run "./GenerateData"

# COMMAND ----------

num_parallel_tests = int(dbutils.widgets.get("num_parallel_tests"))

# COMMAND ----------

def generateNewData():
  generateCustomers()
  generateTransactions(0.8)
  generateOrders(0.8)
  generateProducts(0.8)

# COMMAND ----------

generateNewData()

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

test_notebooks = [
"./tests/JoinTestInner",
"./tests/JoinTestRight",
"./tests/JoinTestLeft",
"./tests/JoinTestInnerRight",
"./tests/JoinTestRightInner",
"./tests/JoinTestInnerLeft",
"./tests/JoinTestLeftInner",
"./tests/JoinTestLeftLeft",
"./tests/JoinTestRightRight",
"./tests/JoinTestRightLeft",
"./tests/JoinTestLeftRight",
"./tests/JoinTestInnerInnerInner",
"./tests/JoinTestLeftRightInner",
"./tests/JoinTestInnerInnerLeft",
"./tests/JoinTestRightRightLeft",
"./tests/JoinTestLeftInnerRight",
"./tests/JoinTestLeftRightLeft",
"./tests/JoinTestComplex1",
"./tests/AggsTestGroupBy",
"./tests/AggsTestRightGroupBy",
"./tests/AggsTestInnerGroupByLeft",
"./tests/AggsTestInnerGroupByLeftLeftGroupBy",
"./tests/AggsTestRightGroupByInnerGroupBy",
"./tests/AggsTestRightGroupByInnerGroupByMax"
]

index = 0
lock = Lock()
def runTest(test, total):
  global index
  curIndex = None
  with lock:
    index += 1
    curIndex = index
    print(f'Running Test "{test}", {index} of {total}')
  try:
    dbutils.notebook.run(test, 0)
  except BaseException as e:
    print(f'Failed Test "{test}", {curIndex} of {total}')
    raise e

with ThreadPoolExecutor(max_workers=num_parallel_tests) as ec:
  runs = [ec.submit(runTest, test, len(test_notebooks)) for test in test_notebooks]
  [i.result() for i in runs]

print(f"{len(test_notebooks)} Tests completed succesfully")
