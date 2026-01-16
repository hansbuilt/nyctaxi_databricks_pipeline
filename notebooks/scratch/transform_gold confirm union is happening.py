# Databricks notebook source
from pyspark.sql.functions import col, to_date, year, month, row_number, lit
from pyspark.sql.window import Window

silver_base_path = "/Volumes/nyc_project/silver/"
gold_base_path = "/Volumes/nyc_project/gold/"
silver_yellow_path = f"{silver_base_path}/yellow"
silver_green_path = f"{silver_base_path}/green"
gold_path = f"{gold_base_path}/gold_consolidated_trips_base"

df_yellow = spark.read.parquet(silver_yellow_path)
df_green = spark.read.parquet(silver_green_path)



# COMMAND ----------

#yellow only: airport_fee
#green only: ehail_fee, trip_type

df_yellow = (df_yellow
            .withColumn('ehail_fee', lit(0).cast('double'))
            .withColumn('trip_type', lit(0).cast('int'))
            .withColumn('taxi_type', lit('yellow').cast('string'))
)

df_green = (df_green
            .withColumn('airport_fee', lit(0).cast('double'))
            .withColumn('taxi_type', lit('green').cast('string'))
)

display(df_yellow)
display(df_green)


# COMMAND ----------

df_unioned = (
    df_yellow
    .unionByName(df_green)
)


# COMMAND ----------

goldpath = '/Volumes/nyc_project/gold/gold_consolidated_trips_base'

df = spark.read.parquet(goldpath)

df.display()

#confirm we're getting both df's unioned
df.select("taxi_type").distinct().show()
