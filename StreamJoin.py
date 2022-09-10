# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid
from  pyspark import StorageLevel
from functools import reduce
from pyspark.sql import Column
from delta.tables import *
import time
import os
import hashlib
import concurrent.futures
import itertools

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

class StreamToStreamJoin:
  _left = None
  _right = None
  _joinType = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self,
               left,
               right,
               joinType):
    self._left = left
    self._right = right
    self._joinType = joinType
  
  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

  def stagingPath(self):
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               self._joinType,
               [],
               [])._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond).stagingPath()

  def on(self,
           joinExpr):
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               self._joinType,
               joinExpr,
               [])._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)
 
  def onKeys(self, *keys):
    joinExpr = lambda l, r: reduce(lambda c, e: c & e, [(l[k] == r[k]) for k in keys])
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               self._joinType,
               joinExpr)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)._onKeys(keys)

# COMMAND ----------

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

# COMMAND ----------

class Stream:
  _stream = None
  _staticReader = None
  _static = None
  _primaryKeys = None
  _sequenceColumns = None
  _path = None
  _name = None
  excludedColumns = ['_commit_version']

  def __init__(self,
               stream,
               staticReader):
    self._stream = stream
    self._staticReader = staticReader
  
  @staticmethod
  def readAtVersion(reader, version = None):
    if version is not None:
      loader = reader.option('versionAsOf', version)
    else:
      loader = reader
    return loader
    
  @staticmethod
  def fromPath(path, startingVersion = None):
    cdfStream = spark.readStream.format('delta').option("readChangeFeed", "true")
    if startingVersion is not None:
      cdfStream.option("startingVersion", "{startingVersion}")
    cdfStream = cdfStream.load(path).where("_change_type != 'update_preimage' and _change_type != 'delete'").drop('_change_type', '_commit_timestamp')
    reader = spark.read.format('delta')
    return Stream(cdfStream, lambda v: Stream.readAtVersion(reader, v).load(path)).setPath(path)

  @staticmethod
  def fromTable(tableName, startingVersion = None):
    cdfStream = spark.readStream.format('delta').option("readChangeFeed", "true")
    if startingVersion is not None:
      cdfStream.option("startingVersion", "{startingVersion}")
    cdfStream = cdfStream.table(tableName).where("_change_type != 'update_preimage' and _change_type != 'delete'").drop('_change_type', '_commit_timestamp')
    reader = spark.read.format('delta')
    return Stream(cdfStream, lambda v: Stream.readAtVersion(reader, v).table(tableName)).setName(tableName).setPath(tableName)

  def __getitem__(self, key):
    return ColumnSelector(self, key)
  
  def setName(self, name):
    self._name = name
    return self

  def name(self):
    if self._name is None or len(self._name) == 0:
      self._name = os.path.basename(self.path())
    return self._name

  def setPath(self, path):
    self._path = path
    return self
  
  def path(self):
    return self._path

  def columns(self):
    return [c for c in self._stream.columns if c not in Stream.excludedColumns]

  def stream(self):
    return self._stream

  def static(self, version = None):
    if version is None:
      if self._static is None:
        self._static = self._staticReader(version)
      return self._static
    return self._staticReader(version)

  def primaryKeys(self, *keys):
    self._primaryKeys = keys
    return self
  
  def getPrimaryKeys(self):
    return self._primaryKeys

  def sequenceBy(self, *columns):
    self._sequenceColumns = columns
    return self
  
  def getSequenceColumns(self):
    return self._sequenceColumns

  def join(self, right, joinType = 'inner'):
    return StreamToStreamJoin(self, right, joinType)
  
  def to(self, func):
    self._stream = func(self._stream)
#     self._static = func(self._static)
    reader = self._staticReader
    self._staticReader = lambda v: func(reader(v))
    return self

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
  def _dropDupKeys(pk, f, r):
    if pk is not None:
      for k in pk:
        f = f.drop(r[k])
    return f

  def join(self,
           joinType,
           joinExpr,
           joinKeyColumnNames,
           selectCols,
           finalSelectCols):
    if isinstance(selectCols, tuple) or isinstance(selectCols, str):
      dropDupKeys = MicrobatchJoin._dropDupKeys
      selectFunc = lambda f, l, r, extraCols: f.selectExpr(*selectCols, *extraCols)
      finalSelectFunc = lambda f, l, r, extraCols: f.selectExpr(*selectCols, *extraCols)
    else:
      dropDupKeys = lambda pk, f, r: f
      selectFunc = lambda f, l, r, extraCols: f.select(*selectCols(l, r), *extraCols)
      finalSelectFunc = lambda f, l, r, extraCols: f.select(*finalSelectCols(l, r), *extraCols)

    newLeft = F.broadcast(self._leftMicrobatch).join(self._rightStatic, joinExpr(self._leftMicrobatch, self._rightStatic), 'left' if joinType == 'left' else 'inner')
    newLeft = dropDupKeys(joinKeyColumnNames, newLeft, self._rightStatic if joinType == 'inner' or joinType == 'left' else self._leftMicrobatch)
    newLeft = selectFunc(newLeft, self._leftMicrobatch, self._rightStatic, [self._leftMicrobatch[c].alias(c) for c in Stream.excludedColumns])

    newRight = F.broadcast(self._rightMicrobatch).join(self._leftStatic, joinExpr(self._leftStatic, self._rightMicrobatch), 'left' if joinType == 'right' else 'inner')
    newRight = dropDupKeys(joinKeyColumnNames, newRight, self._rightMicrobatch if joinType == 'inner' or joinType == 'left' else self._leftStatic)
    newRight = selectFunc(newRight, self._leftStatic, self._rightMicrobatch, [self._rightMicrobatch[c].alias(c) for c in Stream.excludedColumns])

    primaryKeys = list(dict.fromkeys(self._leftPrimaryKeys + self._rightPrimaryKeys))
    primaryJoinExpr = reduce(lambda e, pk: e & pk, [newLeft[k] == newRight[k] for k in primaryKeys])

    joinedOuter = newLeft.join(newRight, joinExpr(newLeft, newRight) & primaryJoinExpr, 'outer').persist(StorageLevel.MEMORY_AND_DISK)
    self._persisted.append(joinedOuter)
    if joinType == 'inner':
      left = joinedOuter.where(reduce(lambda e, pk: e & pk, [newRight[pk].isNull() for pk in joinKeyColumnNames])).select(newLeft['*'])
      right = joinedOuter.where(reduce(lambda e, pk: e & pk, [newLeft[pk].isNull() for pk in joinKeyColumnNames])).select(newRight['*'])
    elif joinType == 'right':
      left = joinedOuter.where(reduce(lambda e, pk: e & pk, [newRight[pk].isNull() for pk in joinKeyColumnNames])).select(newLeft['*'])
      right = joinedOuter.where(reduce(lambda e, pk: e & pk, [(newLeft[pk].isNull()) for pk in joinKeyColumnNames])).select(newRight['*'])
    elif joinType == 'left':
      left = joinedOuter.where(reduce(lambda e, pk: e & pk, [newRight[pk].isNull() for pk in joinKeyColumnNames])).select(newLeft['*'])
      right = joinedOuter.where(reduce(lambda e, pk: e & pk, [newLeft[pk].isNull() for pk in joinKeyColumnNames])).select(newRight['*'])
    else:
      raise Exception(f'{joinType} join type is not supported')
    both = joinedOuter.where(reduce(lambda e, pk: e & pk, [newLeft[pk].isNotNull() & newRight[pk].isNotNull() for pk in joinKeyColumnNames]))
    both = dropDupKeys(joinKeyColumnNames, both, newLeft)
    both = selectFunc(both, newLeft, newRight, [F.greatest(newLeft[c], newRight[c]).alias(c) for c in Stream.excludedColumns])
    unionDf = left.unionByName(right).unionByName(both)
    unionDf = unionDf.where(reduce(lambda e, pk: e | pk, [unionDf[pk].isNotNull() for pk in primaryKeys]))
    finalDf = finalSelectFunc(unionDf, unionDf, unionDf, [unionDf[c] for c in Stream.excludedColumns])
    finalDf = finalDf.persist(StorageLevel.MEMORY_AND_DISK)
    self._persisted.append(finalDf)
    return finalDf
  
  def __enter__(self):
    return self
  
  def __exit__(self, exc_type, exc_value, traceback):
    for df in self._persisted:
      df.unpersist()
    self._persisted.clear()

class StreamingQuery:
  _streamingQuery = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self,
               streamingQuery):
    self._streamingQuery = streamingQuery
  
  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self
    
  def option(self, name, value):
    self._streamingQuery = self._streamingQuery.option(name, value)
    return self
    
  def trigger(self, availableNow=None, processingTime=None, once=None, continuous=None):
    self._streamingQuery = self._streamingQuery.trigger(availableNow=availableNow, processingTime=processingTime, once=once, continuous=continuous)
    if self._dependentQuery is not None:
      self._dependentQuery.trigger(availableNow=availableNow, processingTime=processingTime, once=once, continuous=continuous)
    return self
  
  def queryName(self, name):
    self._streamingQuery = self._streamingQuery.queryName(name)
    return self
  
  @property
  def stream(self):
    return self._streamingQuery

  def start(self):
    if self._dependentQuery is not None:
      self._dependentQuery.start()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
    return self.stream.start()

class StreamingJoin:
  _left = None
  _right = None
  _joinType = None
  _mergeFunc = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self,
               left,
               right,
               joinType,
               mergeFunc):
    self._left = left
    self._right = right
    self._joinType = joinType
    self._mergeFunc = mergeFunc

  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

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
      maxCommitVersions = (
                                left.agg(F.max('_commit_version').alias('_left_commit_version'), F.lit(None).alias('_right_commit_version'))
                                    .unionByName(right.agg(F.lit(None).alias('_left_commit_version'), F.max('_commit_version').alias('_right_commit_version')))
                                    .agg(F.sum('_left_commit_version').alias('_left_commit_version'), F.sum('_right_commit_version').alias('_right_commit_version'))
                                    .collect()[0]
                             )
      # We want to grab the max commit version in the microbatch so we do a consistent read of left and right static pinned at that version
      # otherwise the read may be non-deterministic due to lazy spark evaluation
      leftMaxCommitVersion = maxCommitVersions[0]
      rightMaxCommitVersion = maxCommitVersions[1]
      leftStaticLocal = leftStatic
      rightStaticLocal = rightStatic
      if leftMaxCommitVersion is not None:
        leftStaticLocal = self._left.static(leftMaxCommitVersion)
      if rightMaxCommitVersion is not None:
        rightStaticLocal = self._right.static(rightMaxCommitVersion)
      with MicrobatchJoin(left, leftStaticLocal, self._left.getPrimaryKeys(), right, rightStaticLocal, self._right.getPrimaryKeys()) as mj:
        joinedBatchDf = mj.join(self._joinType,
                                joinExpr,
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
    packed = self._left.stream().select(F.struct('*').alias('left'), F.lit(None).alias('right')).unionByName(self._right.stream().select(F.lit(None).alias('left'), F.struct('*').alias('right')))
    return StreamingQuery(
      (packed
        .writeStream 
        .foreachBatch(self._merge(joinExpr, joinKeyColumnNames, selectCols, finalSelectCols))
      )
    )._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)
  
class StreamToStreamJoinWithConditionForEachBatch:
  _left = None
  _right = None
  _joinType = None
  _joinExpr = None
  _joinKeys = None
  _partitionColumns = None
  _selectCols = None
  _finalSelectCols = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self,
               left,
               right,
               joinType,
               onCondition,
               joinKeys,
               partitionColumns,
               selectCols,
               finalSelectCols):
    self._left = left
    self._right = right
    self._joinType = joinType
    self._joinExpr = onCondition
    self._joinKeys = joinKeys
    self._partitionColumns = partitionColumns
    self._selectCols = selectCols
    self._finalSelectCols = finalSelectCols
  
  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

  def _safeMergeLists(self, l, r):
    a = l
    if r is not None:
      if a is None:
        a = r
      else:
        a = a + r
    if a is not None:
      a = list(dict.fromkeys(a))
    return a

  def partitionBy(self, *columns):
    self._partitionColumns = columns
    return self

  def foreachBatch(self, mergeFunc):
    windowSpec = None
    primaryKeys = self._safeMergeLists(self._left.getPrimaryKeys(), self._right.getPrimaryKeys())
    sequenceColumns = self._safeMergeLists(self._left.getSequenceColumns(), self._right.getSequenceColumns())
    if primaryKeys is not None and len(primaryKeys) > 0 and sequenceColumns is not None and len(sequenceColumns) > 0:
      windowSpec = Window.partitionBy(primaryKeys).orderBy([F.desc(sc) for sc in sequenceColumns])
    def mergeTransformFunc(batchDf, batchId):
      return mergeFunc(self._dedupBatch(batchDf, windowSpec, primaryKeys), batchId)
    return StreamingJoin(self._left,
               self._right,
               self._joinType,
               mergeTransformFunc).join(self._joinExpr,
                               self._joinKeys,
                               self._selectCols,
                               self._finalSelectCols)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)

  def _dedupBatch(self, batchDf, windowSpec, primaryKeys):
    if windowSpec is not None:
      batchDf = batchDf.withColumn('__row_number', F.row_number().over(windowSpec)).where('__row_number = 1')
    else:
      batchDf = batchDf.dropDuplicates(primaryKeys)
    return batchDf

  def _doMerge(self, deltaTable, cond, primaryKeys, sequenceWindowSpec, updateCols, matchCondition, batchDf, batchId):
#    print(f'****** {cond} ******')
    mergeChain = deltaTable.alias("u").merge(
        source = batchDf.alias("staged_updates"),
        condition = F.expr(cond))
    mergeChain.whenMatchedUpdate(condition = matchCondition, set = updateCols) \
        .whenNotMatchedInsert(values = updateCols) \
        .execute()

  def _mergeCondition(self, nonNullableKeys, nullableKeys, extraCond = ''):
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

  def _mergeNonNullKeysForJoin(self, joinType, nonNullKeys, nullKeys, nonNullCandidateKeys, nullCandidateKeys):
    if joinType == 'inner':
      return list(dict.fromkeys(nonNullKeys + [pk for pk in nonNullCandidateKeys if pk not in nullKeys]))
    elif joinType == 'left':
      return list(dict.fromkeys([pk for pk in nonNullKeys if pk not in nullCandidateKeys] + [pk for pk in nonNullCandidateKeys if pk not in nullKeys]))
    elif joinType == 'right':
      return list(dict.fromkeys([pk for pk in nonNullCandidateKeys if pk not in nullKeys] + [pk for pk in nonNullKeys if pk not in nullCandidateKeys]))

  def _mergeNullKeysForJoin(self, joinType, nonNullKeys, nullKeys, nonNullCandidateKeys, nullCandidateKeys):
    if joinType == 'inner':
      return list(dict.fromkeys(nullKeys + [pk for pk in nullCandidateKeys if pk not in nonNullKeys and pk not in nullKeys and pk not in nonNullCandidateKeys]))
    elif joinType == 'left':
      return list(dict.fromkeys([pk for pk in nullKeys if pk not in nonNullKeys] + [pk for pk in nullCandidateKeys if pk not in nonNullKeys]))
    elif joinType == 'right':
      return list(dict.fromkeys([pk for pk in nullKeys if pk not in nonNullKeys] + [pk for pk in nullCandidateKeys if pk not in nullKeys]))

  def _writeToTarget(self, deltaTableForFunc, tableName, path):
    ddl = self._left.static().join(self._right.static(),
                                                   self._joinExpr(self._left.static(), self._right.static())).select(self._finalSelectCols(self._left.static(),
                          self._right.static())).schema.toDDL()
    createSql = f'CREATE TABLE IF NOT EXISTS {tableName}({ddl}) USING DELTA'
    if path is not None:
      createSql = f"{createSql} LOCATION '{path}'"
    if self._partitionColumns is not None:
      createSql = f"{createSql} PARTITIONED BY ({', '.join([pc for pc in self._partitionColumns])})"
    spark.sql(createSql)

    primaryKeys = self._safeMergeLists(self._left.getPrimaryKeys(), self._right.getPrimaryKeys())
    sequenceColumns = self._safeMergeLists(self._left.getSequenceColumns(), self._right.getSequenceColumns())
    pks = [[], []]
    if self._upstreamJoinCond is not None:
      pks = self._upstreamJoinCond()
#       print(f'%%%%%%%%%% nonNullKeys = {pks[0]}')
#       print(f'%%%%%%%%%% nullKeys = {pks[1]}')
    pks1 = self._nonNullAndNullPrimaryKeys(self._joinType,
                                           [pk for pk in primaryKeys if pk in self._left.getPrimaryKeys()],
                                           [pk for pk in primaryKeys if pk in self._right.getPrimaryKeys()])
#     print(f'%%%%%%%%%% self._joinType = {self._joinType}')
#     print(f'%%%%%%%%%% nonNullCandidateKeys = {pks1[0]}')
#     print(f'%%%%%%%%%% nullCandidateKeys = {pks1[1]}')
    pks = [self._mergeNonNullKeysForJoin(self._joinType, pks[0], pks[1], pks1[0], pks1[1]), self._mergeNullKeysForJoin(self._joinType, pks[0], pks[1], pks1[0], pks1[1])]
#     print(f'%%%%%%%%%% pks[0] = {pks[0]}')
#     print(f'%%%%%%%%%% pks[1] = {pks[1]}')
    cond = ' AND '.join([f'u.{pk} = staged_updates.{pk}' for pk in pks[0]] + [f'u.{pk} <=> staged_updates.{pk}' for pk in pks[1]])
    outerCond = None
    dedupWindowSpec = None
    outerWindowSpec = None
    matchCondition = None
    insertFilter = None
    updateFilter = None
    if len(pks[1]) > 0:
      outerCondStr = self._mergeCondition(pks[0], pks[1])
      outerCond = F.expr(outerCondStr)
      insertFilter = ' AND '.join([f'u.{pk} is null' for pk in pks[0]])
      updateFilter = ' AND '.join([f'u.{pk} is not null' for pk in pks[0]])
      outerWindowSpec = Window.partitionBy([f'__operation_flag'] + [f'u.{pk}' for pk in primaryKeys]).orderBy([f'u.{pk}' for pk in primaryKeys] + [F.desc(f'staged_updates.{sc}') for sc in sequenceColumns] + [F.expr('(' + ' + '.join([f'CASE WHEN staged_updates.{pk} is not null THEN 0 ELSE 1 END' for pk in pks[1]]) + ')')])
      dedupWindowSpec = Window.partitionBy([f'__u_{pk}' for pk in primaryKeys]).orderBy([F.desc(f'{pk}') for pk in primaryKeys])
      cond = '__rn = 1 AND ' + cond
      updateCols = {c: F.col(f'staged_updates.__u_{c}') for c in deltaTableForFunc().toDF().columns}
    else:
      updateCols = {c: F.col(f'staged_updates.{c}') for c in deltaTableForFunc().toDF().columns}
    windowSpec = None
    if sequenceColumns is not None and len(sequenceColumns) > 0:
      windowSpec = Window.partitionBy(primaryKeys).orderBy([F.desc(sc) for sc in sequenceColumns])
      matchCondition = ' AND '.join([f'(u.{sc} is null OR u.{sc} <= staged_updates.{"__u_" if len(pks[1]) > 0 else ""}{sc})' for sc in sequenceColumns])
    deltaTableColumns = deltaTableForFunc().toDF().columns
    if outerCond is not None:
      batchSelect = [F.col(f'staged_updates.{c}').alias(f'__u_{c}') for c in deltaTableColumns] + [F.expr(f'CASE WHEN __operation_flag = 2 THEN staged_updates.{c} WHEN __operation_flag = 1 THEN u.{c} END AS {c}') for c in primaryKeys] + [F.when(F.expr('__operation_flag = 1'), F.row_number().over(outerWindowSpec)).otherwise(F.lit(2)).alias('__rn')]
      operationFlag = F.expr(f'CASE WHEN {updateFilter} THEN 1 WHEN {insertFilter} THEN 2 END').alias('__operation_flag')
      nullsCol = F.expr(' + '.join([f'CASE WHEN {pk} is not null THEN 0 ELSE 1 END' for pk in pks[1]]))
      stagedNullsCol = F.expr(' + '.join([f'CASE WHEN __u_{pk} is not null THEN 0 ELSE 1 END' for pk in pks[1]]))
      antiJoinCond = F.expr(' AND '.join([f'({outerCondStr})', '((u.__rn != 1 AND (u.__pk_nulls_count > staged_updates.__pk_nulls_count OR u.__u_pk_nulls_count > staged_updates.__u_pk_nulls_count)))', ' AND '.join([f'(u.__u_{pk} <=> staged_updates.__u_{pk} OR u.__u_{pk} is null)' for pk in pks[1]])]))
    def mergeFunc(batchDf, batchId):
      batchDf._jdf.sparkSession().conf().set('spark.databricks.optimizer.adaptive.enabled', True)
      batchDf._jdf.sparkSession().conf().set('spark.sql.adaptive.forceApply', True)
      deltaTable = deltaTableForFunc()
      mergeDf = None
      if outerCond is None:
        batchDf = self._dedupBatch(batchDf, windowSpec, primaryKeys)
      else:
        batchDf = self._dedupBatch(batchDf, windowSpec, primaryKeys)
#         if 'product_id' in deltaTableColumns:
#           batchDf.withColumnRenamed('_commit_version', '__commit_version').write.format('delta').mode('overwrite').save('/Users/leon.eller@databricks.com/tmp/error/batch0')
        targetDf = deltaTable.toDF()
        u = targetDf.alias('u')
        su = F.broadcast(batchDf).alias('staged_updates')
        mergeDf = u.join(su, outerCond, 'right').select(F.col('*'), operationFlag).select(batchSelect).drop('__operation_flag').select(F.col('*'),
                                                                                                                                       nullsCol.alias('__pk_nulls_count'),
                                                                                                                                       stagedNullsCol.alias('__u_pk_nulls_count'))
        mergeDf = mergeDf.persist(StorageLevel.MEMORY_AND_DISK)
#         if 'product_id' in deltaTableColumns:
#           mergeDf.withColumnRenamed('_commit_version', '__commit_version').write.format('delta').mode('overwrite').save('/Users/leon.eller@databricks.com/tmp/error/merge')
        batchDf = mergeDf.alias('u').join(mergeDf.alias('staged_updates'), antiJoinCond, 'left_anti')
#         if 'product_id' in deltaTableColumns:
#           batchDf.withColumnRenamed('_commit_version', '__commit_version').write.format('delta').mode('overwrite').save('/Users/leon.eller@databricks.com/tmp/error/batch1')
      self._doMerge(deltaTable, cond, primaryKeys, windowSpec, updateCols, matchCondition, batchDf, batchId)
      if mergeDf is not None:
         mergeDf.unpersist()

    return StreamingJoin(self._left,
               self._right,
               self._joinType,
               mergeFunc).join(self._joinExpr,
                               self._joinKeys,
                               self._selectCols,
                               self._finalSelectCols)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)

  def generateJoinName(self):
    name = f'$$_{self._left.name()}_{self._right.name()}'
    m = hashlib.sha256()
    m.update(self._left.path().encode('ascii'))
    m.update(self._right.path().encode('ascii'))
    return f'{name}/{self._joinType}/{m.hexdigest()}'

  def generateJoinStagingPath(self):
    dir = os.path.dirname(self._right.path())
    return f'{dir}/{self.generateJoinName()}'

  def _nonNullAndNullPrimaryKeys(self, joinType, leftPrimaryKeys, rightPrimaryKeys):
      if joinType == 'left':
        return [leftPrimaryKeys, rightPrimaryKeys]
      elif joinType == 'right':
        return [rightPrimaryKeys, leftPrimaryKeys]
      else:
        return [(leftPrimaryKeys + rightPrimaryKeys), []]

  def join(self, right, joinType = 'inner', stagingPath = None):
    if stagingPath is None:
      stagingPath = self.generateJoinStagingPath()
    joinQuery = (
                  self.writeToPath(f'{stagingPath}/data')
                      .option('checkpointLocation', f'{stagingPath}/cp')
                      .queryName(self.generateJoinName())
                )
    primaryKeys = self._safeMergeLists(self._left.getPrimaryKeys(), self._right.getPrimaryKeys())
    if self._upstreamJoinCond is not None:
      def func():
        pks = self._upstreamJoinCond()
        pks1 = self._nonNullAndNullPrimaryKeys(self._joinType,
                                               [pk for pk in primaryKeys if pk in self._left.getPrimaryKeys()],
                                               [pk for pk in primaryKeys if pk in self._right.getPrimaryKeys()])
        return [self._mergeNonNullKeysForJoin(self._joinType, pks[0], pks[1], pks1[0], pks1[1]), self._mergeNullKeysForJoin(self._joinType, pks[0], pks[1], pks1[0], pks1[1])]
      joinCondFunc = func
    else:
      joinCondFunc = lambda: self._nonNullAndNullPrimaryKeys(self._joinType, [pk for pk in primaryKeys if pk in self._left.getPrimaryKeys()], [pk for pk in primaryKeys if pk in self._right.getPrimaryKeys()])
    return Stream.fromPath(f'{stagingPath}/data').setName(f'{self._left.name()}_{self._right.name()}').primaryKeys(*primaryKeys).join(right, joinType)._chainStreamingQuery(joinQuery, joinCondFunc)
    
  def writeToPath(self, path):
    return self._writeToTarget(lambda: DeltaTable.forPath(spark, path), f'delta.`{path}`', path)

  def writeToTable(self, tableName):
    return self._writeToTarget(lambda: DeltaTable.forName(spark, tableName), tableName, None)
    
class StreamToStreamJoinWithCondition:
  _left = None
  _right = None
  _joinType = None
  _joinExpr = None
  _joinKeys = None
  _dependentQuery = None
  _partitionColumns = None
  _upstreamJoinCond = None

  def __init__(self,
               left,
               right,
               joinType,
               onCondition,
               joinKeys = None,
               partitionColumns = None):
    self._left = left
    self._right = right
    self._joinType = joinType
    self._joinExpr = onCondition
    self._joinKeys = joinKeys
    self._partitionColumns = partitionColumns

  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

  def _onKeys(self, keys):
    return StreamToStreamJoinWithCondition(self._left,
               self._right,
               self._joinType,
               self._joinExpr,
               keys,
               self._partitionColumns)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)
  
  def stagingPath(self):
    return self.select('*').generateJoinStagingPath()

  def partitionBy(self, *columns):
    return self.select('*').partitionBy(*columns)

  def dedupJoinKeys(self, *keys):
    return self.hintJoinKeys(*keys)

  def hintJoinKeys(self, *keys):
    return self._onKeys(keys)
  
  def join(self, right, joinType = 'inner', stagingPath = None):
    return self.select('*').join(right, joinType, stagingPath)

  def foreachBatch(self, mergeFunc):
    return self.select('*').foreachBatch(mergeFunc)

  def writeToPath(self, path):
    return self.select('*').writeToPath(path)

  def writeToTable(self, tableName):
    return self.select('*').writeToTable(tableName)
    
  def select(self, *selectCols):
    if isinstance(selectCols[0], ColumnSelector):
      leftDict = {}
      expandedCols = []
      for c in selectCols:
        if c.columnName() == '*':
          if c.stream() is self._left.stream():
            for col in self._left.columns():
              expandedCols.append(ColumnSelector(self._left, col))
          elif c.stream() is self._right.stream():
            for col in self._right.columns():
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
        leftStars = [[ColumnSelector(self._left, lc) for lc in self._left.columns()] for c in selectCols if c == '*']
        rightStars = [[ColumnSelector(self._right, lc) for lc in self._right.columns()] for c in selectCols if c == '*']
        leftCols = [lc for arr in leftStars for lc in arr]
        rightCols = [lc for arr in rightStars for lc in arr if lc.columnName() not in self._joinKeys]
        allCols = leftCols + rightCols
        if len(allCols) > 0:
          return self.select(*allCols)
        else:
          return self.select(*([ColumnSelector(self._left, lc) for lc in self._left.columns() if lc in selectCols] + [ColumnSelector(self._right, lc) for lc in self._right.columns() if lc in selectCols]))
      else:
        selectFunc = selectCols
        finalSelectFunc = selectFunc
    return StreamToStreamJoinWithConditionForEachBatch(self._left,
               self._right,
               self._joinType,
               self._joinExpr,
               self._joinKeys,
               self._partitionColumns,
               selectFunc,
               finalSelectFunc)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)
