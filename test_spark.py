"""
Test program to load data from Bucket S3 and get the average speed of the month.
"""
import pandas as pd
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
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


def operations_df(
    spark_session: SparkSession,
    bucket: str,
    pickup_col: str,
    dropoff_col: str,
    trip_col: str,
):
    """Preparing dataframe to get average speed of the month.

    Args:
        spark_session: SparkSession
        bucket: name of the bucket in AWS S3
        pickup_col: name of the pick up column
        dropoff_col: name of the drop off column
        trip_col: name of the trip column
    Returns:
        Dataframe with preparing data.
    """
    df = get_data_s3(spark_session=spark_session, bucket=bucket)
    df = df.select(pickup_col, dropoff_col, trip_col)

    df.withColumn(pickup_col, df[pickup_col].cast(TimestampType()))
    df.withColumn(dropoff_col, df[dropoff_col].cast(TimestampType()))

    df = df.withColumn("time_lpep", (df[dropoff_col] - df[pickup_col]) / 3600)

    df = df.withColumn("speed", df[trip_col] / df["time_lpep"])

    return df


def average_speed(
    spark_session: SparkSession,
    bucket: str,
    pickup_col: str,
    dropoff_col: str,
    trip_col: str,
):
    """Get average speed of the month.

    Args:
        spark_session: SparkSession
        bucket: name of the bucket in AWS S3
        pickup_col: name of the pick up column
        dropoff_col: name of the drop off column
        trip_col: name of the trip column
    Returns:
        Average speed of the month.
    """
    d = operations_df(
        spark_session=spark_session,
        bucket=bucket,
        pickup_col=pickup_col,
        dropoff_col=dropoff_col,
        trip_col=trip_col,
    )
    avg = d.agg({trip_col: "sum"}) / d.agg({"time_lpep": "sum"})

    return avg


def plot(speed: list) -> None:
    """Plot the graph with average speed.

    Args:
        speed: list of the average speed to plot
    """
    dates = pd.date_range("2019-05", "2020-05", freq="M").tolist()
    plt.plot(speed, dates)
    plt.show()


if __name__ == "__main__":
    BUCKET = "s3://nyc-tlc/trip data/"

    speed_green = []
    speed_yellow = []

    session = SparkSession.builder.appName(
        "Python Spark SQL basic example"
    ).getOrCreate()

    for num in range(6, 13, 1):
        speed_green.append(
            average_speed(
                spark_session=session,
                bucket=f"{BUCKET}green_tripdata_2019-0{num}.csv",
                pickup_col="lpep_pickup_datetime",
                dropoff_col="lpep_dropoff_datetime",
                trip_col="trip_distance",
            )
        )
        speed_yellow.append(
            average_speed(
                spark_session=session,
                bucket=f"{BUCKET}yellow_tripdata_2019-0{num}.csv",
                pickup_col="tpep_pickup_datetime",
                dropoff_col="tpep_dropoff_datetime",
                trip_col="trip_distance",
            )
        )

    for num in range(1, 7, 1):
        speed_green.append(
            average_speed(
                spark_session=session,
                bucket=f"{BUCKET}green_tripdata_2020-0{num}.csv",
                pickup_col="lpep_pickup_datetime",
                dropoff_col="lpep_dropoff_datetime",
                trip_col="trip_distance",
            )
        )
        speed_yellow.append(
            average_speed(
                spark_session=session,
                bucket=f"{BUCKET}yellow_tripdata_2020-0{num}.csv",
                pickup_col="tpep_pickup_datetime",
                dropoff_col="tpep_dropoff_datetime",
                trip_col="trip_distance",
            )
        )

    print(speed_green)
    print(speed_yellow)
