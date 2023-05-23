from databricks.sdk.runtime import *
from pyspark.sql import functions as F
import os
import hashlib
from delta.tables import *
from pyspark import StorageLevel

class GroupByWithAggs:
  _groupBy = None
  _aggCols = None
  _stream = None
  _partitionColumns = None
  _updateDict = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self, groupBy, aggCols, updateDict = None):
    self._groupBy = groupBy
    self._aggCols = aggCols
    self._updateDict = updateDict
    self._stream = groupBy.stream()

  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

  def stagingIndex(self):
    if self._dependentQuery is not None:
      return self._dependentQuery._depth(1)
    return 0

  def generateStagingName(self):
    name = f"{'.'.join([str(c) for c in self._groupBy.columns()])}_{'.'.join([str(c) for c in self._aggCols])}"
    m = hashlib.sha256()
    m.update(name.encode('ascii'))
    m.update(self._stream.path().encode('ascii'))
    return f'$$_agg_{m.hexdigest()}_{self.stagingIndex()}'

  def generateStagingPath(self):
    dir = os.path.dirname(self._stream.path())
    return f'{dir}/{self.generateStagingName()}'

  def _doMerge(self, deltaTable, cond, updateCols, insertCols, keyCols, aggCols, nullAggColsDf, deltaCalcs, batchDf, batchId):
    plusDf = batchDf.where("_change_type != 'update_preimage'").groupBy(*self._groupBy.columns()).agg(*self._aggCols).alias("p").persist(StorageLevel.MEMORY_AND_DISK)
    minusDf = batchDf.where("_change_type = 'update_preimage'").groupBy(*self._groupBy.columns()).agg(*self._aggCols).alias("m").persist(StorageLevel.MEMORY_AND_DISK)
    batchDf = F.broadcast(plusDf).join(minusDf, F.expr(" AND ".join([f"p.{k} <=> m.{k}" for k in keyCols])), how="left")
    batch_mdf = F.broadcast(minusDf).join(plusDf, F.expr(" AND ".join([f"p.{k} <=> m.{k}" for k in keyCols])), how="left_anti").crossJoin(nullAggColsDf.alias("p"))
    batchDf = batchDf.select([f"p.{k}" for k in keyCols] + [deltaCalcs[ac] for ac in deltaCalcs])
    batch_mdf = batch_mdf.select([f"m.{k}" for k in keyCols] + [deltaCalcs[ac] for ac in deltaCalcs])
    batchDf = batchDf.unionByName(batch_mdf)
    mergeChain = deltaTable.alias("u").merge(
        source = batchDf.alias("staged_updates"),
        condition = F.expr(cond))
    mergeChain.whenMatchedUpdate(set = updateCols) \
        .whenNotMatchedInsert(values = insertCols) \
        .execute()
    plusDf.unpersist()
    minusDf.unpersist()

  def _writeToTarget(self, deltaTableForFunc, tableName, path):
    from elzyme.streams import DataStreamWriter
    schemaDf = self._stream.static().groupBy(*self._groupBy.columns()).agg(*self._aggCols)
    keyCols = schemaDf.columns[:len(self._groupBy.columns())]
    aggCols = schemaDf.columns[len(self._groupBy.columns()):]
    if self._updateDict is not None:
      schemaDf = schemaDf.alias("u").join(schemaDf.alias("staged_updates")).select([f"u.{c}" for c in keyCols + aggCols if c not in self._updateDict] + [(self._updateDict[k][1]).alias(k) for k in self._updateDict])
    ddl = schemaDf.schema.toDDL()
    createSql = f'CREATE TABLE IF NOT EXISTS {tableName}({ddl}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.autoCompact = true, delta.autoOptimize.optimizeWrite = true)'
    if path is not None:
      createSql = f"{createSql} LOCATION '{path}'"
    if self._partitionColumns is not None:
      createSql = f"{createSql} PARTITIONED BY ({', '.join([pc.column() for pc in self._partitionColumns])})"
    spark.sql(createSql)
    cond = " AND ".join([f"u.{kc} <=> staged_updates.{kc}" for kc in keyCols])
    deltaCalcs = {ac: F.expr(f"CASE WHEN m.{ac} is not null THEN COALESCE(p.{ac}, 0) - m.{ac} ELSE p.{ac} END as {ac}") for ac in aggCols}
    updateCols = {ac: F.col(f'u.{ac}') + F.col(f'staged_updates.{ac}') for ac in aggCols}
    insertCols = {ic: F.col(f'staged_updates.{ic}') for ic in (keyCols + aggCols)}
    if self._updateDict is not None:
      for k in self._updateDict:
        updateCols[k] = self._updateDict[k][1]
        insertCols[k] = self._updateDict[k][0]
        deltaCalcs[k] = F.when(F.col(f"m.{k}").isNotNull(), self._updateDict[k][2]).otherwise(F.col(f"p.{k}")).alias(f"{k}")
    nullAggColsDf = spark.sql(f"SELECT {','.join([f'null as {a}' for a in aggCols])}")
    def mergeFunc(batchDf, batchId):
      batchDf._jdf.sparkSession().conf().set('spark.databricks.optimizer.adaptive.enabled', True)
      batchDf._jdf.sparkSession().conf().set('spark.sql.adaptive.forceApply', True)
      deltaTable = deltaTableForFunc()
      self._doMerge(deltaTable, cond, updateCols, insertCols, keyCols, aggCols, nullAggColsDf, deltaCalcs, batchDf, batchId)
    return DataStreamWriter(
      (
        self._stream.stream().writeStream.foreachBatch(mergeFunc)
      )
    )._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)

  def partitionBy(self, *columns):
    self._partitionColumns = [(c if isinstance(c, PartitionColumn) else PartitionColumn(c)) for c in columns]
    return self

  def reduce(self, column, update, delta_update = None, insert = None):
    if insert is None:
      insert = F.col(f"staged_updates.{column}")
    if delta_update is None:
      delta_update = F.coalesce(F.col(f"p.{column}"), F.lit(0)) - F.col(f"m.{column}")
    if update is None:
      update = F.col(f'u.{column}') + F.col(f'staged_updates.{column}')
    if self._updateDict is None:
      self._updateDict = {}
    self._updateDict[column] = (insert, update, delta_update)
    return self

  def join(self, right, joinType = 'inner', stagingPath = None):
    from elzyme.streams import Stream
    if stagingPath is None:
      stagingPath = self.generateStagingPath()
    query = (
                  self.writeToPath(f'{stagingPath}/data')
                      .option('checkpointLocation', f'{stagingPath}/cp')
                      .queryName(self.generateStagingName())
                )
    return ( Stream.fromPath(f'{stagingPath}/data').setName(self.generateStagingName()).primaryKeys(*self._groupBy.columns())
               .join(right, joinType)
               ._chainStreamingQuery(query, None) )
  

  def groupBy(self, *cols, stagingPath = None):
    if stagingPath is None:
      stagingPath = self.generateStagingPath()
    query = (
                  self.writeToPath(f'{stagingPath}/data')
                      .option('checkpointLocation', f'{stagingPath}/cp')
                      .queryName(self.generateStagingName())
                )
    return ( Stream.fromPath(f'{stagingPath}/data').setName(self.generateStagingName()).primaryKeys(*self._groupBy.columns())
               .groupBy(*cols)
               ._chainStreamingQuery(query, None) )

  def writeToPath(self, path):
      return self._writeToTarget(lambda: DeltaTable.forPath(spark, path), f'delta.`{path}`', path)

  def writeToTable(self, tableName):
    return self._writeToTarget(lambda: DeltaTable.forName(spark, tableName), tableName, None)

class GroupBy:
  _cols = None
  _stream = None
  _dependentQuery = None
  _upstreamJoinCond = None

  def __init__(self, stream, cols):
    self._cols = cols
    self._stream = stream

  def _chainStreamingQuery(self, dependentQuery, upstreamJoinCond):
    self._dependentQuery = dependentQuery
    self._upstreamJoinCond = upstreamJoinCond
    return self

  def agg(self, *aggCols):
    return GroupByWithAggs(self, aggCols)._chainStreamingQuery(self._dependentQuery, self._upstreamJoinCond)
  
  def stream(self):
    return self._stream
  
  def columns(self):
    return self._cols