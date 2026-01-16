from pyspark.sql.functions import lit, current_timestamp

raw_path =  "/Volumes/nyc_project/raw/raw/yellow_tripdata_2025-03.parquet"
bronze_path = "/Volumes/nyc_project/bronze/yellow"
year = 2025
month = 3

df_raw = spark.read.parquet(raw_path)

df_bronze = (
    df_raw
    .withColumn('ingestion_ts', current_timestamp())
    .withColumn('year', lit(year))
    .withColumn('month', lit(month))
    )

(
    df_bronze
    .write
    .mode('append')
    .partitionBy('year', 'month')
    .parquet(bronze_path)
)