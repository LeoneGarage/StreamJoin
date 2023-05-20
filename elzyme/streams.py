from databricks.sdk.runtime import *
from pyspark.sql import functions as F
from elzyme.joins import StreamToStreamJoin, ColumnRef
from elzyme.aggs import GroupBy
import uuid
import os
from delta.tables import *

spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "2GB")

class ColumnSelector:
  _stream = None
  _columnName = None
  _func = None

  def __init__(self,
               stream,
               columnName):
    self._stream = stream
    self._columnName = columnName

  def frame(self):
    return self._stream

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
  
  def __eq__(self, other):
    return ColumnRef(self) == other
  def __lt__(self, other):
    return ColumnRef(self) < other
  def __gt__(self, other):
    return ColumnRef(self) > other
  def __le__(self, other):
    return ColumnRef(self) <= other
  def __ge__(self, other):
    return ColumnRef(self) >= other
  def __ne__(self, other):
    return ColumnRef(self) != other

  def __and__(self, other):
    return ColumnRef(self) & other

class PartitionColumn:
  _column = None
  _staticPruned = False

  def __init__(self,
               column):
    if isinstance(column, prune):
      self._column = column.column()
      self._staticPruned = True
    else:
      self._column = column
      self._staticPruned = False
  
  def column(self):
    return self._column
  
  def isStaticPruned(self):
    return self._staticPruned

class prune:
  _column = None

  def __init__(self,
               column):
    self._column = column

  def column(self):
    return self._column

class Stream:
  _stream = None
  _staticReader = None
  _static = None
  _primaryKeys = None
  _sequenceColumns = None
  _path = None
  _name = None
  _isTable = None
  excludedColumns = ['_commit_version', '_change_type']

  def __init__(self,
               stream,
               staticReader,
               isTable):
    self._stream = stream
    self._staticReader = staticReader
    self._isTable = isTable
  
  @staticmethod
  def readAtVersion(reader, version = None):
    if version is not None:
      loader = reader.option('versionAsOf', version)
    else:
      loader = reader
    return loader
    
  @staticmethod
  def fromPath(path, startingVersion = None):
    cdfStream = spark.readStream.format('delta').option("readChangeFeed", "true").option("maxBytesPerTrigger", "1g")
    if startingVersion is not None:
      cdfStream = cdfStream.option("startingVersion", f"{startingVersion}")
    cdfStream = cdfStream.load(path)
    cdfStream = cdfStream.where("_change_type != 'delete'").drop('_commit_timestamp')
    reader = spark.read.format('delta')
    return Stream(cdfStream, lambda v: Stream.readAtVersion(reader, v).load(path), False).setPath(path)

  @staticmethod
  def fromTable(tableName, startingVersion = None):
    cdfStream = spark.readStream.format('delta').option("readChangeFeed", "true").option("maxBytesPerTrigger", "1g")
    if startingVersion is not None:
      cdfStream = cdfStream.option("startingVersion", f"{startingVersion}")
    cdfStream = cdfStream.table(tableName)
    cdfStream = cdfStream.where("_change_type != 'delete'").drop('_commit_timestamp')
    reader = spark.read.format('delta')
    return Stream(cdfStream, lambda v: Stream.readAtVersion(reader, v).table(tableName), True).setName(tableName).setPath(tableName)

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

  def getLatestVersion(self):
    if self._isTable is True:
      return DeltaTable.forName(spark, self.name()).history(1).select('version').collect()[0][0]
    return DeltaTable.forPath(spark, self.path()).history(1).select('version').collect()[0][0]

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
  
  def groupBy(self, *cols):
    return GroupBy(self, cols)
  
  def to(self, func):
    self._stream = func(self._stream)
#     self._static = func(self._static)
    reader = self._staticReader
    self._staticReader = lambda v: func(reader(v))
    return self

class StreamingQuery:
  _streamingQuery = None
  _dependentQuery = None

  def __init__(self,
               streamingQuery,
               dependentQuery):
    self._streamingQuery = streamingQuery
    self._dependentQuery = dependentQuery
  
  @property
  def lastProgress(self):
    pdict = {}
    if self._dependentQuery is not None:
      pdict.update(self._dependentQuery.lastProgress)
    pdict[self._streamingQuery.name] = self._streamingQuery.lastProgress
    return pdict

  @property
  def recentProgress(self):
    pdict = {}
    if self._dependentQuery is not None:
      pdict.update(self._dependentQuery.recentProgress)
    pdict[self._streamingQuery.name] = self._streamingQuery.recentProgress
    return pdict

  @property
  def isActive(self):
    if self._dependentQuery is not None:
      if self._dependentQuery.isActive is True:
        return True
    return self._streamingQuery.isActive

  def awaitTermination(self, timeout=None):
    if self._dependentQuery is not None:
      self._dependentQuery.awaitTermination(timeout)
    return self._streamingQuery.awaitTermination(timeout)

  def stop(self):
    if self._dependentQuery is not None:
      self._dependentQuery.stop()
    return self._streamingQuery.stop()
  
  def awaitAllProcessed(self, maxConsecutiveNoBytesOutstandingMicrobatchRetries = 6):
    lastBatches = {}
    batches = {}
    testTryCount = 0
    while(True):
      lp = self.lastProgress
      sources = [lp[k]['sources'][0] for k in lp if lp[k] is not None]
      if len(sources) == len(lp):
        bytes = [int(s['metrics']['numBytesOutstanding']) if s.get('metrics') is not None else 1 for s in sources]
        batches = {k: lp[k]['timestamp'] for k in lp}
        updatedBatches = [batches[bi] for bi in batches if bi in lastBatches and batches[bi] != lastBatches[bi]]
        if sum(bytes) == 0:
          if len(updatedBatches) > 0:
            if testTryCount >= maxConsecutiveNoBytesOutstandingMicrobatchRetries:
              break
            else:
              testTryCount += 1
        else:
          testTryCount = 0
      self.awaitTermination(5)
      lastBatches.update(batches)

  def awaitAllProcessedAndStop(self):
    self.awaitAllProcessed()
    self.stop()

class DataStreamWriter:
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

  def _depth(self, index):
    if self._dependentQuery is not None:
      return self._dependentQuery._depth(index + 1)
    return index
    
  def option(self, name, value):
    self._streamingQuery = self._streamingQuery.option(name, value)
    return self
    
  def trigger(self, availableNow=None, processingTime=None, once=None, continuous=None):
    if self._dependentQuery is not None:
      self._dependentQuery.trigger(availableNow=availableNow, processingTime=processingTime, once=once, continuous=continuous)
    self._streamingQuery = self._streamingQuery.trigger(availableNow=availableNow, processingTime=processingTime, once=once, continuous=continuous)
    return self
  
  def queryName(self, name):
    self._streamingQuery = self._streamingQuery.queryName(name)
    return self
  
  @property
  def stream(self):
    return self._streamingQuery

  def start(self):
    dq = None
    if self._dependentQuery is not None:
      dq = self._dependentQuery.start()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", str(uuid.uuid4()))
    sq = self.stream.start()
    return StreamingQuery(sq, dq)