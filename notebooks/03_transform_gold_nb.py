# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0
# MAGIC
# MAGIC %reload_ext autoreload

# COMMAND ----------

# DBTITLE 1,Untitled
import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from transform_gold.transform_gold_nycdata import transform_gold_consolidated_base, transform_gold_trip_revenue_daily_fact

silver_base_path = "/Volumes/nyc_project/silver/"
gold_base_path = "/Volumes/nyc_project/gold/"
gold_consol_base_path = "/Volumes/nyc_project/gold/gold_consolidated_trips_base/"

transform_gold_consolidated_base(spark, silver_base_path, gold_base_path)

transform_gold_trip_revenue_daily_fact(spark, gold_consol_base_path, gold_base_path)

# COMMAND ----------

# DBTITLE 1,Write Parquet to Managed Table
# read parquet
parquet_path = "/Volumes/nyc_project/gold/gold_consolidated_trips_base"
df = spark.read.parquet(parquet_path)

# drop table first, in the event we add columns
spark.sql("DROP TABLE IF EXISTS gold_consolidated_trips_base_fact")

# write table
df.write.mode("overwrite").saveAsTable("gold_consolidated_trips_base_fact")

# display table
display(spark.table("gold_consolidated_trips_base_fact"))
