#from pyspark.sql.functions import col, to_date, year, month, row_number, lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window


"""
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

    Meant more for financial use cases using location, times, duration, distances.
    """
    
    gold_path = f"{gold_base_path}/gold_trip_revenue_daily_fact"

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
        #.orderBy("pickup_date")
    )

    (
        df_agg
        .write
        .mode('overwrite')
        .partitionBy('pickup_date')
        .parquet(gold_path)
    )

def transform_gold_trip_demand_fact(spark, gold_consol_base_path, gold_base_path):

    """
    Transforms the gold consolidated base dataset into the trip revenue demand fact dataset.

    Meant more for demand planning use cases.
    """

    """
    perf notes:
        - initial run with orderBy (which is worthless): 5m 6s
        - removing orderBy: 4m 30s
        - removing avg aggs: back up past 6m ???
    
    """

    #['vendor_id',  'passenger_count', 'trip_distance', 'pu_location_id', 'do_location_id',  'trip_duration_seconds', 'trip_type', 'taxi_type', 'pickup_ts', 'dropoff_ts',]
    
    gold_path = f"{gold_base_path}/gold_trip_demand_fact"

    df = spark.read.parquet(gold_consol_base_path)

    df_time = (
        df
        .withColumn("pickup_day", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))                    
        .withColumn("pickup_weekday_iso", F.date_format("pickup_ts", "u").cast("int"))
    )

    #removing averages given a ton of groupby levels here

    df_agg = (
        df_time
        .groupBy("pickup_day", 'pickup_hour', 'pickup_weekday_iso', 'taxi_type', 'pu_location_id', 'do_location_id')
        .agg(F.count("*").alias('trip_count'),
            (F.sum('trip_distance').alias('total_trip_distance')),
            #(F.avg('trip_distance').alias('avg_trip_distance')),
            (F.sum('trip_duration_seconds').alias('total_trip_duration')),
            #(F.avg('trip_duration_seconds').alias('avg_trip_duration')),
            (F.sum('passenger_count').alias('total_passenger_count')),
            #(F.avg('passenger_count').alias('avg_passenger_count')),
        )
    )

    #join in zone lookup dim for text descriptions of pu/do locations

    df_zone_lookup = (
        spark.read
            .parquet(f"{gold_base_path}/gold_zone_lookup_dim")
            .select(
                "location_id",
                "borough",
                "zone",
                "service_zone"
            )
    )

    df_pu_zone = F.broadcast(
        df_zone_lookup
            .withColumnRenamed("location_id", "pu_location_id")
            .withColumnRenamed("borough", "pu_borough")
            .withColumnRenamed("zone", "pu_zone")
            .withColumnRenamed("service_zone", "pu_service_zone")

        )

    df_do_zone = F.broadcast(
        df_zone_lookup
            .withColumnRenamed("location_id", "do_location_id")
            .withColumnRenamed("borough", "do_borough")
            .withColumnRenamed("zone", "do_zone")
            .withColumnRenamed("service_zone", "do_service_zone")
    )

    df_joined = (
        df_agg
            .join(df_pu_zone, "pu_location_id", "left")
            .join(df_do_zone, "do_location_id", "left")
    )

    (
        df_joined
        .write
        .mode('overwrite')
        .partitionBy("pickup_day")
        .parquet(gold_path)
    )
    
def transform_gold_zonelookup_dim(spark, silver_base_path, gold_base_path):
    """
    Transforms silver zone lookup dim table into gold.
    """

    silver_path = f"{silver_base_path}/zone_lookup"
    gold_path = f"{gold_base_path}/gold_zone_lookup_dim"

    df = spark.read.parquet(silver_path)

    #this dim table is pretty clean, no transforming needed

    (
        df
        .write
        .mode('overwrite')
        .parquet(gold_path)
    )

