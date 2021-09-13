# """
# Test program to load data from .csv and get the average speed of the month.
# """
# import pandas as pd
#
#
# def read_data(file: str) -> pd.DataFrame:
#     """Get data from csv file.
#
#     Args:
#         file: name of the file
#     Returns
#         Dataframe with use columns.
#     """
#     df = pd.read_csv(
#         file, usecols=["lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance"]
#     )
#
#     return df
#
#
# def prepare_df(file: str) -> pd.DataFrame:
#     """Args: Calculate average month speed.
#             Calculate time of the pick up, speed of the drive and average speed in the month.
#
#     Args:
#          file: name of the file
#     Returns:
#         Dataframe with important value.
#             df = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "time_lpep", "speed"]
#     """
#     df = read_data(file)
#
#     df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
#     df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
#
#     df["time_lpep"] = (
#         df["lpep_dropoff_datetime"] - df["lpep_pickup_datetime"]
#     ).dt.total_seconds() / 3600
#     df["speed"] = df["trip_distance"] / df["time_lpep"]
#
#     return df
#
#
# def get_average_speed(file: str):
#     """Get average of the speed.
#
#     Args:
#         file: name of the file
#     Returns:
#         Average of the speed.
#     """
#     df = prepare_df(file)
#     avg = df["trip_distance"].sum() / df["time_lpep"].sum()
#
#     return avg
#
#
# if __name__ == "__main__":
#     # FILE = 'D:\studia\Semestr_6\Autonomiczne Systemy Ekspertyzy i Eksploracji Danych\Projekt_2\Dane\green_tripdata_2019-05.csv'
#     FILEa = "/Users/konradbudukiewicz/Downloads/green_tripdata_2019-05.csv"
#     print(get_average_speed(FILEa))
for num in range(1, 14, 1):  # range to get good csv file
    if num >= 6:
        print(num-1)
    else:
        print(num)