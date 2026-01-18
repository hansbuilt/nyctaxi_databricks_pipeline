# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0
# MAGIC
# MAGIC %reload_ext autoreload

# COMMAND ----------

import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from extract_bronze.extract_nyctaxi import extract_single_nyctaxi, extract_zonelookup_nyctaxi

raw_base_path = "/Volumes/nyc_project/raw/raw/"
bronze_base_path = "/Volumes/nyc_project/bronze/"

# COMMAND ----------

# DBTITLE 1,Cell 1
ingestion_plan = [
    {"color": "yellow", "year": 2025, "month": 1},
    {"color": "yellow", "year": 2025, "month": 2},
    {"color": "yellow", "year": 2025, "month": 3},
    {"color": "green",  "year": 2025, "month": 1},
    {"color": "green",  "year": 2025, "month": 2},
    {"color": "green",  "year": 2025, "month": 3},
]

for job in ingestion_plan:
    extract_single_nyctaxi(spark, job['color'], job['year'], job['month'], raw_base_path, bronze_base_path)
    print(f"Finished load for {job}")

# COMMAND ----------

extract_zonelookup_nyctaxi(spark, raw_base_path, bronze_base_path)
