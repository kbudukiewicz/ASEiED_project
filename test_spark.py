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
    df = (
        spark_session.read.format("csv")
        .options(header="true", inferSchema="true")
        .load(bucket)
    )
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

    df.withColumn("timestamp", to_timestamp("lpep_pickup_datetime"))
    df.withColumn("timestamp", to_timestamp("lpep_dropoff_datetime"))

    df = df.withColumn(
        "time_lpep",
        (df["lpep_dropoff_datetime"] - df["lpep_pickup_datetime"]).total_seconds()
        / 3600,
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
    df = operations_df(spark_session=spark_session, bucket=bucket)
    avg = df.agg({"trip_distance": "sum"}) / df.agg({"time_lpep": "sum"})

    return avg


if __name__ == "__main__":

    session =  SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()
    sc = session.sparkContext
    sc.setLogLevel('ERROR')
    dataFrameReader = session.read


    BUCKET = "s3://daneaseied/dane/green_tripdata_2020-05.csv"

    responses = dataFrameReader.option("header", "true").csv(BUCKET)

    print("WYNIK:")

    responses.printSchema()



    #data = get_data_s3(spark, bucket=BUCKET)

    #print(average_speed(spark_session=spark, bucket=BUCKET))
