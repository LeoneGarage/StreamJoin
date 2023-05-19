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