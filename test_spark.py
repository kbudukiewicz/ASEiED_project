"""
Test program to load data from Bucket S3 and get the average speed of the month.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


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

    df.withColumn("timestamp", to_timestamp(col("lpep_pickup_datetime"), "ss"))
    df.withColumn("timestamp", to_timestamp(col("lpep_dropoff_datetime"), "ss"))

    df = df.withColumn(
        "time_lpep",
        (df["lpep_dropoff_datetime"] - df["lpep_pickup_datetime"]) / 3600
    )

    df = df.withColumn("speed", df["trip_distance"] / df["time_lpep"])

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
    LIST_OF_FILES = [" "]
    BUCKET = "s3://nyc-tlc/trip data/"
    list_avg_speed = []

    session = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
    # file1 = "green_tripdata_2020-05.csv"
    # sc = session.sparkContext
    # sc.setLogLevel('ERROR')

    for file in LIST_OF_FILES:
        avg = average_speed(spark_session=session, bucket=(BUCKET+file))
        list_avg_speed.append(avg)

    print("WYNIK:", list_avg_speed)