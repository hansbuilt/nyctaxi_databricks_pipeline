from pyspark.sql.functions import col, to_date, year, month, row_number, lit
from pyspark.sql.window import Window

def transform_gold_consolidated_base(spark, silver_base_path, gold_base_path):
    """
    Transforms silver yellow and silver yellow into a unioned, consolidated base table.
    """

    silver_yellow_path = f"{silver_base_path}/yellow"
    silver_green_path = f"{silver_base_path}/green"
    gold_path = f"{gold_base_path}/gold_consolidated_trips_base"

    df_yellow = spark.read.parquet(silver_yellow_path)
    df_green = spark.read.parquet(silver_green_path)
   
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
    
    df_unioned = (
        df_yellow
        .unionByName(df_green)
    )

    (
        df_unioned
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(gold_path)
    )