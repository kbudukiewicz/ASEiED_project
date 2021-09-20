"""
Test program to load data from Bucket S3 and get the average speed of the month.
"""
import pandas as pd
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, unix_timestamp, psf
from pyspark.sql.types import DoubleType


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
    # time_lpep: str,
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

    df = df.withColumn(
        "lpep_pickup_datetime",
        to_timestamp("lpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"),
    )
    df = df.withColumn(
        "lpep_dropoff_datetime",
        to_timestamp("lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"),
    )

    df = df.withColumn(
        "time_lpep",
        (
            unix_timestamp("lpep_dropoff_datetime")
            - unix_timestamp("lpep_pickup_datetime")
        )
        / 3600,
    )

    df = df.withColumn(trip_col, df[trip_col].cast(DoubleType()))

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

    d.show()
    d.printSchema()

    avg = (
        d.agg(psf.sum("trip_distance")).collect()[0][0]
        / d.agg(psf.sum("time_lpep")).collect()[0][0]
    )
    return avg


def plot(green: list, yellow: list) -> None:
    """Plot the graph with average speed.

    Args:
        green: list of the average green taxi speed to plot
        yellow: list of the average yellow taxi speed to plot
    """
    dates = pd.date_range("2019-05", "2020-06", freq="M").strftime("%Y-%b").tolist()
    plt.plot(dates, green, label="Green taxi")
    plt.plot(dates, yellow, label="Yellow taxi")
    plt.xlabel("Dates")
    plt.ylabel("Average speed")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    BUCKET = "s3://nyc-tlc/trip data/"

    speed_green = []
    speed_yellow = []

    session = SparkSession.builder.appName(
        "Python Spark SQL basic example"
    ).getOrCreate()

    for num in range(6, 10, 1):
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

    print("średnia prędkość dla taksówek zielonych: ", speed_green)
    print("średnia prędkość dla taksówek żółtych: ", speed_yellow)

    plot(green=speed_green, yellow=speed_yellow)
