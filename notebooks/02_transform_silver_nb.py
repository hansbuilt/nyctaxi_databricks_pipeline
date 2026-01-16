# Databricks notebook source
# DBTITLE 1,Cell 1
import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from transform_silver.transform_silver_nycdata import transform_silver_yellow_nyctaxi, transform_silver_green_nyctaxi

bronze_base_path = "/Volumes/nyc_project/bronze/"
silver_base_path = "/Volumes/nyc_project/silver/"

transform_silver_yellow_nyctaxi(spark, bronze_base_path, silver_base_path)

transform_silver_green_nyctaxi(spark, bronze_base_path, silver_base_path)

