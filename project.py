import pandas as pd
import numpy as np

def read_data(file: str) -> pd.DataFrame:
    """Get data from csv file.

    Args:
        file: name of the file
    Returns
        Dataframe with use columns.
    """
    df = pd.read_csv(file, usecols=['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance'])

    return df


def operations(file: str) -> pd.DataFrame:
    """

    :param file:
    :return:
    """
    df = read_data(file)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    df['time_lpep'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 3600
    df['speed'] = df['trip_distance'] / df['time_lpep']

    df.at['Average', 'time_lpep'] = df['time_lpep'].sum()/len(df)
    df.at['Average', 'trip_distance'] = df['trip_distance'].sum() / len(df)
    df.at['Average', 'speed'] = df['trip_distance'].sum() / df['time_lpep'].sum()

    return df


if __name__ == '__main__':
    FILE = 'D:\studia\Semestr_6\Autonomiczne Systemy Ekspertyzy i Eksploracji Danych\Projekt_2\Dane\green_tripdata_2019-05.csv'

    print(operations(FILE))
