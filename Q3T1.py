from threading import Thread, Lock
import math
import numpy as np
import time
import pandas as pd

# Reference: for this assignment I used some parts of the code from comp6231 labs

# set the global variables here
number_of_rows = 6311871
num_of_threads = 5
average_airtime_all = 0


def Q3_parallel(n_rows: int, skip_rows: int, lock):
    global average_airtime_all
    df = pd.read_csv('C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2/Combined_Flights_2021.csv',
                     nrows=n_rows, skiprows=skip_rows, header=None)
    df = df[df.iloc[:, 34] == 'Nashville, TN']
    df = df[df.iloc[:, 42] == 'Chicago, IL']
    average_airtime = df.iloc[:, 12].mean()
    lock.acquire()
    average_airtime_all = (average_airtime_all + average_airtime)/2
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
        t = Thread(target=Q3_parallel,
                   args=(reading_info_fill[j][0], reading_info_fill[j][1], lock))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()

    print(f'average airtime: {average_airtime_all}.')


if __name__ == "__main__":
    print('Q3: What was the average airtime for flights that were flying '
          'from Nashville to Chicago? (T1. Multi-threading)')
    thread_function()
