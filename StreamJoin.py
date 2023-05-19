# Databricks notebook source
import sys
from pathlib import Path
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path = Path(dbutils.notebook.entry_point.getDbutils().notebook().getContext().extraContext().apply('notebook_path')).parent
try:
  sys.path.remove(f'/Workspace/Repos/{user}/{path.name}')
except ValueError as e:
  pass
sys.path.insert(1, f'/Workspace/Repos/{user}/{path.name}')

# COMMAND ----------

from elzyme.streams import Stream, prune
