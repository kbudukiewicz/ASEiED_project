"""
Test program to load data from Bucket S3 and get the average speed of the month.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType


def get_data_s3(spark_session: SparkSession, bucket: str):
    """Get data from S3 bucket.

    Args:
        spark_session: SparkSession
        bucket: name of the bucket in AWS S3
    Returns:
         Dataframe with data from AWS s3.
    """
    df = spark_session.read.option("header", "true").csv(bucket)
    return df


def operations_df(spark_session: SparkSession, bucket: str):
    """Preparing dataframe to get average speed of the month.

    Args:
        spark_session: SparkSession
        bucket: name of the bucket in AWS S3
    Returns:
        Dataframe with preparing data.
    """
    df = get_data_s3(spark_session=spark_session, bucket=bucket)
    df = df.select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")

    df.withColumn("lpep_pickup_datetime", df.lpep_pickup_datetime.cast(TimestampType()))
    df.withColumn("lpep_dropoff_datetime", df.lpep_dropoff_datetime.cast(TimestampType()))

    print(df)

    df = df.withColumn(
        "time_lpep",
        (
            to_timestamp(df["lpep_dropoff_datetime_1"])
            - to_timestamp(df["lpep_pickup_datetime_1"])
        ) / 3600
    )

    df = df.withColumn("speed", float(df["trip_distance"]) / float(df["time_lpep"]))

    return df


def average_speed(spark_session: SparkSession, bucket: str):
    """Get average speed of the month.

    Args:
        spark_session: SparkSession
        bucket: name of the bucket in AWS S3
    Returns:
        Average speed of the month.
    """
    d = operations_df(spark_session=spark_session, bucket=bucket)
    avg = d.agg({"trip_distance": "sum"}) / d.agg({"time_lpep": "sum"})

    return avg


if __name__ == "__main__":
    BUCKET = "s3://nyc-tlc/trip data/"
    list_avg_speed = []
    speed_green = []
    speed_yellow = []

    session = SparkSession.builder.appName(
        "Python Spark SQL basic example"
    ).getOrCreate()

    for num in range(1,12,1): # range to get good csv file
        if num >= 5:
            speed_green.append(
                average_speed(
                    spark_session=session,
                    bucket=f"{BUCKET}green_tripdata_2019-0{num-1}.csv",
                )
            )
            speed_yellow.append(
                average_speed(
                    spark_session=session,
                    bucket=f"{BUCKET}yellow_tripdata_2019-0{num-1}.csv",
                )
            )
        else:
            speed_green.append(
                average_speed(
                    spark_session=session, bucket=f"{BUCKET}green_tripdata_2020-0{num}.csv"
                )
            )
            speed_yellow.append(
                average_speed(
                    spark_session=session, bucket=f"{BUCKET}yellow_tripdata_2020-0{num}.csv"
                )
            )

    print(speed_green)
    print(speed_yellow)
