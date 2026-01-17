#from pyspark.sql.functions import col, to_date, year, month, row_number, lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Goals:
- consolidated yellow and green datasets

- trip revenue daily fact - trip ct, total rev, total tips, avg fare, avg tip, taxi type
- trip demand daily fact - hour AND day, zone/location, trip ct, avg distance, avg duration, total dist, total duration, taxi type
- trip efficiency fact - zone PU and DO pairings, taxi type, avg duration, avg distance, avg speed mph, trip count, hour/date
- taxi type utilization - PU zone/loc, yellow trip ct, green trip ct, avg fare of each, avg duration of each

""" 

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
                .withColumn('ehail_fee', F.lit(0).cast('double'))
                .withColumn('trip_type', F.lit(0).cast('int'))
                .withColumn('taxi_type', F.lit('yellow').cast('string'))
    )

    df_green = (df_green
                .withColumn('airport_fee', F.lit(0).cast('double'))
                .withColumn('taxi_type', F.lit('green').cast('string'))
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

def transform_gold_trip_revenue_daily_fact(spark, gold_consol_base_path, gold_base_path):

    """
    Transforms the gold consolidated base dataset into the trip revenue daily fact dataset.

    Meant more for financial use cases.
    """
    #test
    
    gold_path = f"{gold_base_path}/gold_trip_revenue_daily_fact"
    gold_consol_base_path = f"{gold_base_path}/gold_consolidated_trips_base"

    df = spark.read.parquet(gold_consol_base_path)

    df_day = (
        df
        .withColumn('pickup_date', F.to_date(F.col('pickup_ts')))
        )

    df_agg = (
        df_day
        .groupBy('pickup_date', 'taxi_type')
        .agg(F.count("*").alias('trip_count'),
            (F.sum('total_amount').alias('total_trip_revenue')),
            (F.avg('total_amount').alias('avg_revenue_per_trip')),
            (F.sum('tip_amount').alias('total_tips')),
            (F.avg('tip_amount').alias('avg_tip_per_trip')),
            (F.sum('trip_distance').alias('total_trip_distance')),
            (F.avg('trip_distance').alias('avg_trip_distance')),
            (F.sum('trip_duration_seconds').alias('total_trip_duration')),
            (F.avg('trip_duration_seconds').alias('avg_trip_duration')),

        )
        .orderBy("pickup_date")
    )

    (
        df_agg
        .write
        .mode('overwrite')
        .partitionBy('pickup_date')
        .parquet(gold_path)
    )
    