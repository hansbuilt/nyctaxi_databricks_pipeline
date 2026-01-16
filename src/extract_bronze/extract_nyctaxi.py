from pyspark.sql.functions import lit, current_timestamp

def extract_single_nyctaxi(spark, color, year, month, raw_base_path, bronze_base_path):
    """
    Extracts a single month of NYC Taxi data from a raw parquet file and writes it to a bronze table.
    """

    raw_path = f"{raw_base_path}/{color}_tripdata_{year}-{month:02d}.parquet"
    bronze_path = f"{bronze_base_path}/{color}"

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