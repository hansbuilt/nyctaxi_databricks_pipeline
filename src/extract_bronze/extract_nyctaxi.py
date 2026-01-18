from pyspark.sql.functions import lit, current_timestamp

def extract_single_nyctaxi(spark, color, year, month, raw_base_path, bronze_base_path):
    """
    Extracts a single month of NYC Taxi data from a raw parquet file and writes it to a bronze volume.
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

def extract_zonelookup_nyctaxi(spark, raw_base_path, bronze_base_path):
    """
    Extracts the zone lookup csv file and writes to a bronze volume.
    """

    raw_path = f"{raw_base_path}/taxi_zone_lookup.csv"
    bronze_path = f"{bronze_base_path}/zone_lookup"

    df_raw = spark.read.csv(
        raw_path,
        header=True,
        inferSchema=True,
        )

    df_bronze = (
        df_raw
        .withColumn('ingestion_ts', current_timestamp())
    )

    (
        df_bronze
        .write
        .mode('overwrite')
        .parquet(bronze_path)
    )
