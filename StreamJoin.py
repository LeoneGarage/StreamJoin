# Databricks notebook source
dbutils.widgets.text("repo_subdir", "StreamJoin", "StreamJoin repo subdirectory")

# COMMAND ----------

repo_subdir = dbutils.widgets.get("repo_subdir")

# COMMAND ----------

import sys
from pathlib import Path
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
try:
  sys.path.remove(f'/Workspace/Repos/{user}/{repo_subdir}')
except ValueError as e:
  pass
sys.path.insert(1, f'/Workspace/Repos/{user}/{repo_subdir}')

# COMMAND ----------

from elzyme.streams import Stream, prune
