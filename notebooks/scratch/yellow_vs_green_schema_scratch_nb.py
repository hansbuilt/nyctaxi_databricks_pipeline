# Databricks notebook source
yellow_path = "/Volumes/nyc_project/bronze/yellow/"
green_path = "/Volumes/nyc_project/bronze/green/"

yellow = spark.read.parquet(yellow_path)
green = spark.read.parquet(green_path)

# COMMAND ----------

yellow.printSchema()

# COMMAND ----------

green.printSchema()

# COMMAND ----------

greencol = list(green.columns)
yellowcol = list(yellow.columns)

#print(greencol)
#print(yellowcol)

gset = set(greencol)
yset = set(yellowcol)

unique = gset.symmetric_difference(yset)
print(unique)

# COMMAND ----------

green.display()

# COMMAND ----------

yellow_silver_path = "/Volumes/nyc_project/silver/yellow/"
green_silver_path = "/Volumes/nyc_project/silver/green/"

syellow = spark.read.parquet(yellow_silver_path)
sgreen = spark.read.parquet(green_silver_path)

syellow_col = syellow.columns
sgreen_col = sgreen.columns

print(syellow_col)
print(sgreen_col)

sgset = set(syellow_col)
syset = set(sgreen_col)

unique = sgset.symmetric_difference(syset)
print(unique)

#yellow only: airport_fee
#green only: ehail_fee, trip_type
