# Databricks notebook source
import sys
from pathlib import Path
repo_root = str(Path().resolve().parents[0])  #relative path so we don't expose email / username
sys.path.append(f"{repo_root}/src")

from transform_gold.transform_gold_nycdata import transform_gold_consolidated_base

silver_base_path = "/Volumes/nyc_project/silver/"
gold_base_path = "/Volumes/nyc_project/gold/"

transform_gold_consolidated_base(spark, silver_base_path, gold_base_path)
