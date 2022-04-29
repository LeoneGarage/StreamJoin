# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType, struct, lit, col,count
import uuid
from  pyspark import StorageLevel
from functools import reduce
from pyspark.sql import Column
from delta.tables import *
import time
import os
import hashlib

# COMMAND ----------

import pyspark.sql.types
from pyspark.sql.types import _parse_datatype_string

def toDDL(self):
    """
    Returns a string containing the schema in DDL format.
    """
    from pyspark import SparkContext
    sc = SparkContext._active_spark_context
    dt = sc._jvm.__getattr__("org.apache.spark.sql.types.DataType$").__getattr__("MODULE$")
    json = self.json()
    return dt.fromJson(json).toDDL()
pyspark.sql.types.DataType.toDDL = toDDL
pyspark.sql.types.StructType.fromDDL = _parse_datatype_string

# COMMAND ----------

class MicrobatchJoin:
  _leftMicrobatch = None
  _leftStatic = None
  _leftPrimaryKeys = None
  _rightMicrobatch = None
  _rightStatic = None
  _rightPrimaryKeys = None
  _persisted = []

  def __init__(self,
               leftMicrobatch,
               leftStatic,
               leftPrimaryKeys,
               rightMicrobatch,
               rightStatic,
               rightPrimaryKeys):
    self._leftMicrobatch = leftMicrobatch
    self._leftStatic = leftStatic
    self._leftPrimaryKeys = leftPrimaryKeys
    self._rightMicrobatch = rightMicrobatch
    self._rightStatic = rightStatic
    self._rightPrimaryKeys = rightPrimaryKeys
  
  @staticmethod
  def _dropDupKeys(pk, f, l, r):
    if pk is not None:
      for k in pk:
        f = f.drop(r[k])
    return f

  def join(self,
           joinExpr,
           joinKeyColumnNames,
           selectCols,
           finalSelectCols):
    if isinstance(selectCols, tuple) or isinstance(selectCols, str):
      dropDupKeys = MicrobatchJoin._dropDupKeys
      selectFunc = lambda f, l, r: f.selectExpr(*selectCols)
      finalSelectFunc = lambda f, l, r: f.selectExpr(*selectCols)
    else:
      dropDupKeys = lambda pk, f, l, r: f
      selectFunc = lambda f, l, r: f.select(*selectCols(l, r))
      finalSelectFunc = lambda f, l, r: f.select(*finalSelectCols(l, r))

    newLeft = self._leftMicrobatch.join(self._rightStatic, joinExpr(self._leftMicrobatch, self._rightStatic))
    newLeft = dropDupKeys(joinKeyColumnNames, newLeft, self._leftMicrobatch, self._rightStatic)
    newLeft = selectFunc(newLeft, self._leftMicrobatch, self._rightStatic)

    newRight = self._rightMicrobatch.join(self._leftStatic, joinExpr(self._leftStatic, self._rightMicrobatch))
    newRight = dropDupKeys(joinKeyColumnNames, newRight, self._leftStatic, self._rightMicrobatch)
    newRight = selectFunc(newRight, self._leftStatic, self._rightMicrobatch)

    primaryJoinExpr = reduce(lambda e, pk: e & pk, [newLeft[k] == newRight[k] for k in list(dict.fromkeys(self._leftPrimaryKeys + self._rightPrimaryKeys))])   
    joinedOuter = newLeft.join(newRight, joinExpr(newLeft, newRight) & primaryJoinExpr, 'outer').persist(StorageLevel.MEMORY_AND_DISK)
    self._persisted.append(joinedOuter)
    left = joinedOuter.where(reduce(lambda e, pk: e & pk, [newRight[pk].isNull() for pk in joinKeyColumnNames])).select(newLeft['*'])
    right = joinedOuter.where(reduce(lambda e, pk: e & pk, [newLeft[pk].isNull() for pk in joinKeyColumnNames])).select(newRight['*'])
    both = joinedOuter.where(reduce(lambda e, pk: e & pk, [newLeft[pk].isNotNull() & newRight[pk].isNotNull() for pk in joinKeyColumnNames]))
    both = dropDupKeys(joinKeyColumnNames, both, newRight, newLeft)
    both = selectFunc(both, newRight, newLeft)
    unionDf = left.unionByName(right).unionByName(both)
    finalDf = finalSelectFunc(unionDf, unionDf, unionDf).persist(StorageLevel.MEMORY_AND_DISK)
    self._persisted.append(finalDf)
    return finalDf
  
  def __enter__(self):
    return self
  
  def __exit__(self, exc_type, exc_value, traceback):
    for df in self._persisted:
      df.unpersist()
    self._persisted.clear()
    
class StreamingJoin:
  _left = None
  _right = None
  _mergeFunc = None

  def __init__(self,
               left,
               right,
               mergeFunc):
    self._left = left
    self._right = right
    self._mergeFunc = mergeFunc

  def _merge(self,
             joinExpr,
             joinKeyColumnNames,
             selectCols,
             finalSelectCols):
    leftStatic = self._left.static()
    rightStatic = self._right.static()
    mergeFunc = self._mergeFunc
    def _mergeJoin(batchDf, batchId):
      left = batchDf.where('left is not null').select('left.*')
      right = batchDf.where('right is not null').select('right.*')
      with MicrobatchJoin(left, leftStatic, self._left.getPrimaryKeys(), right, rightStatic, self._right.getPrimaryKeys()) as mj:
        joinedBatchDf = mj.join(joinExpr,
                              joinKeyColumnNames,
                              selectCols,
                              finalSelectCols)
        return mergeFunc(joinedBatchDf, batchId)
    return _mergeJoin

  def join(self,
           joinExpr,
           joinKeyColumnNames,
           selectCols,
           finalSelectCols):
    packed = self._left.stream().select(struct('*').alias('left'), lit(None).alias('right')).unionByName(self._right.stream().select(lit(None).alias('left'), struct('*').alias('right')))
    return (
      packed
        .writeStream 
        .foreachBatch(self._merge(joinExpr, joinKeyColumnNames, selectCols, finalSelectCols))
  )
  
class StreamToStreamJoinWithConditionForEachBatch:
  _left = None
  _right = None
  _joinExpr = None
  _joinKeys = None
  _selectCols = None
  _finalSelectCols = None

  def __init__(self,
               left,
               right,
               onCondition,
               joinKeys,
               selectCols,
               finalSelectCols):
    self._left = left
    self._right = right
    self._joinExpr = onCondition
    self._joinKeys = joinKeys
    self._selectCols = selectCols
    self._finalSelectCols = finalSelectCols
    
  def foreachBatch(self, mergeFunc):
    return StreamingJoin(self._left,
               self._right,
               mergeFunc).join(self._joinExpr,
                               self._joinKeys,
                               self._selectCols,
                               self._finalSelectCols)

  def _doMerge(self, deltaTable, cond, batchDf, batchId):
    deltaTable.alias("u").merge(
        source = batchDf.alias("staged_updates"),
        condition = F.expr(cond)) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

  def join(self, right, storagePath = None):
    if storagePath is None:
      dir = os.path.dirname(right.path())
      name = f'{os.path.basename(self._left.path())}_{os.path.basename(self._right.path())}_{os.path.basename(right.path())}'
      m = hashlib.sha256()
      m.update(self._left.path().encode('ascii'))
      m.update(self._right.path().encode('ascii'))
      m.update(right.path().encode('ascii'))
      storagePath = f'{dir}/{name}/{m.hexdigest()}'
    (
      self.writeToPath(f'{storagePath}/data')
          .option('checkpointLocation', f'{storagePath}/cp')
          .start()
    )
    return Stream.fromPath(f'{storagePath}/data').primaryKeys(*list(dict.fromkeys(self._left.getPrimaryKeys() + self._right.getPrimaryKeys()))).join(right)

  def writeToPath(self, path):
    ddl = self._left.static().join(self._right.static(),
                                                   self._joinExpr(self._left.static(), self._right.static())).select(self._finalSelectCols(self._left.static(),
                          self._right.static())).schema.toDDL()
    spark.sql(f'DROP TABLE IF EXISTS delta.`{path}`')
    spark.sql(f'CREATE TABLE delta.`{path}`({ddl}) USING DELTA')
    cond = ' AND '.join([f'u.{k} = staged_updates.{k}' for k in list(dict.fromkeys(self._left.getPrimaryKeys() + self._right.getPrimaryKeys()))])
    def mergeFunc(batchDf, batchId):
      self._doMerge(DeltaTable.forPath(spark, path), cond, batchDf, batchId)

    return StreamingJoin(self._left,
               self._right,
               mergeFunc).join(self._joinExpr,
                               self._joinKeys,
                               self._selectCols,
                               self._finalSelectCols)

  def writeToTable(self, tableName):
    cond = ' AND '.join([f'u.{k} = staged_updates.{k}' for k in list(dict.fromkeys(self._left.getPrimaryKeys() + self._right.getPrimaryKeys()))])
    def mergeFunc(batchDf, batchId):
      self._doMerge(DeltaTable.forName(spark, tableName), cond, batchDf, batchId)

    return StreamingJoin(self._left,
               self._right,
               mergeFunc).join(self._joinExpr,
                               self._joinKeys,
                               self._selectCols,
                               self._finalSelectCols)

class ColumnSelector:
  _stream = None
  _columnName = None
  _func = None

  def __init__(self,
               stream,
               columnName):
    self._stream = stream
    self._columnName = columnName

  def stream(self):
    return self._stream.stream()

  def columnName(self):
    return self._columnName
  
  def transform(self, col):
    if self._func is None:
      return col
    return self._func(col)

  def to(self, func):
    self._func = func
    return self
    
class StreamToStreamJoinWithCondition:
  _left = None
  _right = None
  _joinExpr = None
  _joinKeys = None

  def __init__(self,
               left,
               right,
               onCondition,
               joinKeys = None):
    self._left = left
    self._right = right
    self._joinExpr = onCondition
    self._joinKeys = joinKeys

  def _onKeys(self, keys):
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               self._joinExpr,
               keys)
    
  def withJoinKeys(self, *keys):
    return self._onKeys(keys)
  
  def select(self, *selectCols):
    if isinstance(selectCols[0], ColumnSelector):
      leftDict = {}
      expandedCols = []
      for c in selectCols:
        if c.columnName() == '*':
          if c.stream() is self._left.stream():
            for col in self._left.stream().columns:
              expandedCols.append(ColumnSelector(self._left, col))
          elif c.stream() is self._right.stream():
            for col in self._right.stream().columns:
              expandedCols.append(ColumnSelector(self._right, col))
        else:
          expandedCols.append(c)
      selectCols = tuple(expandedCols)
      for c in selectCols:
        if c.stream() is self._left.stream():
          leftDict[c.columnName()] = c.columnName()
      def selectCol(c):
        cn = c.columnName()
        lc = leftDict.get(cn)
        if lc is not None:
          return lambda l, r: l[cn]
        return lambda l, r: r[cn]
      def finalSelectCol(c):
        cn = c.columnName()
        lc = leftDict.get(cn)
        if lc is not None:
          return lambda l, r: c.transform(l[cn])
        return lambda l, r: c.transform(r[cn])
      selectFuncs = [selectCol(c) for c in selectCols]
      selectFunc = lambda l, r: [f(l, r) for f in selectFuncs]
      finalSelectFuncs = [finalSelectCol(c) for c in selectCols]
      finalSelectFunc = lambda l, r: [f(l, r) for f in finalSelectFuncs]
    else:
      if isinstance(selectCols, tuple):
        # if '*' is specified convert to columns from left and right minus primary keys on right to avoid dups
        leftStars = [[ColumnSelector(self._left, lc) for lc in self._left.stream().columns] for c in selectCols if c == '*']
        rightStars = [[ColumnSelector(self._right, lc) for lc in self._right.stream().columns] for c in selectCols if c == '*']
        leftCols = [lc for arr in leftStars for lc in arr]
        rightCols = [lc for arr in rightStars for lc in arr if lc.columnName() not in self._joinKeys]
        allCols = leftCols + rightCols
        if len(allCols) > 0:
          return self.select(*allCols)
      else:
        selectFunc = selectCols
        finalSelectFunc = selectFunc
    return StreamToStreamJoinWithConditionForEachBatch(self._left,
               self._right,
               self._joinExpr,
               self._joinKeys,
               selectFunc,
               finalSelectFunc)

class StreamToStreamJoin:
  _left = None
  _right = None

  def __init__(self,
               left,
               right):
    self._left = left
    self._right = right
    
  def on(self,
           joinExpr):
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               joinExpr)
 
  def onKeys(self, *keys):
    joinExpr = lambda l, r: reduce(lambda c, e: c & e, [(l[k] == r[k]) for k in keys])
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               joinExpr)._onKeys(keys)

class Stream:
  _stream = None
  _static = None
  _primaryKeys = None
  _path = None

  def __init__(self,
               stream,
               static):
    self._stream = stream
    self._static = static
  
  @staticmethod
  def fromPath(path):
    stream = Stream(spark.readStream.format('delta').option("readChangeFeed", "true").load(path).where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp'),
                 spark.read.format('delta').load(path))
    stream.setPath(path)
    return stream

  @staticmethod
  def fromTable(tableName):
    return Stream(spark.readStream.format('delta').option("readChangeFeed", "true").table(tableName).where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp'),
                 spark.read.format('delta').table(tableName))

  def __getitem__(self, key):
    return ColumnSelector(self, key)
  
  def setPath(self, path):
    self._path = path
    return self
  
  def path(self):
    return self._path

  def stream(self):
    return self._stream
  def static(self):
    return self._static

  def primaryKeys(self, *keys):
    self._primaryKeys = keys
    return self
  
  def getPrimaryKeys(self):
    return self._primaryKeys

  def join(self, right):
    return StreamToStreamJoin(self, right)
  
  def to(self, func):
    self._stream = func(self._stream)
    self._static = func(self._static)
    return self
