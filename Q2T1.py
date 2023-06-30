from threading import Thread, Lock
import math
import numpy as np
import time
import pandas as pd

# Reference: for this assignment I used some parts of the code from comp6231 labs

# set the global variables here
number_of_rows = 6311871
num_of_threads = 5
all_diverted_flights = 0


def diverted_parallel(n_rows: int, skip_rows: int, lock):
    global all_diverted_flights
    df = pd.read_csv('C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2/Combined_Flights_2021.csv',
                     nrows=n_rows, skiprows=skip_rows, header=None)
    df = df[df.iloc[:, 5] == True]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.year == 2021]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.month == 11]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.day <= 30]
    df = df[pd.to_datetime(df.iloc[:, 0]).dt.day >= 20]
    num_of_diverted_flight = df.iloc[:, 5].count()
    lock.acquire()
    all_diverted_flights = all_diverted_flights + num_of_diverted_flight
    lock.release()


def thread_function():
    def distribute_rows(n_rows: int, num_threads: int):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows
        for _ in range(1, num_threads - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows
        reading_info.append([None, skip_rows])
        return reading_info

    lock = Lock()
    reading_info_fill = distribute_rows(number_of_rows//num_of_threads, num_of_threads)
    thread_handle = []
    for j in range(0, num_of_threads):
        t = Thread(target=diverted_parallel,
                   args=(reading_info_fill[j][0], reading_info_fill[j][1], lock))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()

    print(f'diverted flights: {all_diverted_flights}.')


if __name__ == "__main__":
    print('Q2: How many flights were diverted between the period of '
          '20th-30th November 2021? (T1. Multi-threading)')
    thread_function()
