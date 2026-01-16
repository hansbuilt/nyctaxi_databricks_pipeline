# Databricks notebook source
bronze_path = "/Volumes/nyc_project/bronze/yellow/"

df = spark.read.parquet(bronze_path)

# COMMAND ----------

df.describe().show()

# COMMAND ----------

print(df)
display(df)
df.printSchema()

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#rename cols to set them all to snake case, cast datatypes explicitly
from pyspark.sql.functions import col, to_date, year, month

silver_clean = (
    df
    .select(
        col("VendorID").alias('vendor_id').cast('int'),
        col("tpep_pickup_datetime").alias('pickup_ts').cast('timestamp'),
        col("tpep_dropoff_datetime").alias('dropoff_ts').cast('timestamp'),
        col("passenger_count").cast('int'),
        col("trip_distance").cast('double'),
        col("RatecodeID").alias('ratecode_id').cast('int'),
        col("store_and_fwd_flag").cast('string'),
        col("PULocationID").alias('pu_location_id').cast('int'),
        col("DOLocationID").alias('do_location_id').cast('int'),
        col("payment_type").cast('int'),
        col("fare_amount").cast('double'),
        col("extra").cast('double'),
        col("mta_tax").cast('double'),
        col("tip_amount").cast('double'),
        col("tolls_amount").cast('double'),
        col("improvement_surcharge").cast('double'),
        col("total_amount").cast('double'),
        col("congestion_surcharge").cast('double'),
        col("Airport_fee").alias('airport_fee').cast('double'),
        col("cbd_congestion_fee").cast('double'),
        col("ingestion_ts").cast('timestamp'),
        col("year").cast('int'),
        col("month").cast('int')
    )
)

print(silver_clean)


# COMMAND ----------

#dedup rows, as we may be reloading the same files from time to time

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col


#build natural key cols, as we don't have a UID for each trip
natural_key = ['vendor_id', 'pickup_ts', 'dropoff_ts', 'pu_location_id', 'do_location_id']

#create groups within the df for each natural key, sorting on ingestion_ts so the most recent record is first
window_spec = (
    Window
    .partitionBy(*natural_key)
    .orderBy(col('ingestion_ts').desc())
)

#create a row num col for each window/group, then filter to just the first (most recent) rows, then drop the col
silver_dedup = (
    silver_clean
    .withColumn('row_num', row_number().over(window_spec))
    .filter(col('row_num') == 1)
    .drop('row_num')
)

# COMMAND ----------

#remove invalid data
starting_row_count = silver_dedup.count()
print(f"Starting row count: {starting_row_count}")

#remove anything with negative fare amount, negative trip distance, or null pickup/dropoff timestamps
silver_valid = (
    silver_dedup
    .filter(col("pickup_ts").isNotNull())
    .filter(col("dropoff_ts").isNotNull())
    .filter(col('fare_amount') > 0)
    .filter(col('trip_distance') > 0)

)

ending_row_count = silver_valid.count()
print(f"Ending row count: {ending_row_count}")



# COMMAND ----------

#write silver df to storage

silver_base_path = "/Volumes/nyc_project/silver/"
color = 'yellow'
silver_path = f"{silver_base_path}{color}/"

(
    silver_valid
    .write
    .mode('overwrite')
    .partitionBy('year', 'month')
    .parquet(silver_path)
)