# Databricks notebook source
import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from transform_silver.transform_silver_nycdata import transform_silver_yellow_nyctaxi, transform_silver_green_nyctaxi, transform_silver_zonelookup

bronze_base_path = "/Volumes/nyc_project/bronze/"
silver_base_path = "/Volumes/nyc_project/silver/"

# COMMAND ----------

# DBTITLE 1,Cell 1

transform_silver_yellow_nyctaxi(spark, bronze_base_path, silver_base_path)

transform_silver_green_nyctaxi(spark, bronze_base_path, silver_base_path)


# COMMAND ----------

transform_silver_zonelookup(spark, bronze_base_path, silver_base_path)
