# Databricks notebook source
# DBTITLE 1,Cell 1
import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from extract_bronze.extract_nyctaxi import extract_single_nyctaxi

raw_base_path = "/Volumes/nyc_project/raw/raw/"
bronze_base_path = "/Volumes/nyc_project/bronze/"

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