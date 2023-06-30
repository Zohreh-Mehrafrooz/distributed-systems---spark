from threading import Thread, Lock
import math
import numpy as np
import time
import pandas as pd

date_list_all = []
number_of_rows = 6311871
num_of_threads = 10


def Q4_parallel(n_rows: int, skip_rows: int, lock):
    global date_list_all
    df = pd.read_csv('C:/Users/zohre/OneDrive/Documents/6231/Assignments/Assignment2/Combined_Flights_2021.csv',
                     nrows=n_rows, skiprows=skip_rows, header=None)
    df = df[df.iloc[:, 7].isnull()]
    date_list = df.iloc[:, 0].values.tolist()
    lock.acquire()
    date_list_all.extend(date_list)
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
        t = Thread(target=Q4_parallel,
                   args=(reading_info_fill[j][0], reading_info_fill[j][1], lock))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()

    date_list_all2 = []
    for thing in date_list_all:
        thing = tuple(thing)
        date_list_all2.append(thing)

    date_set_all = set(date_list_all)

    print(f'Dates with missing DepTime : {date_set_all}')
    print(f'Number of the list of dates : {len(date_list_all)}')
    print(f'Number of the set of dates : {len(date_set_all)}')


if __name__ == "__main__":
    start_time = time.time()
    print('Q4: For which date was departure time (DepTime) not recorded/went missing?'
          ' (T1. Multi-threading)')
    thread_function()
    end_time = time.time()
    print("Time taken for multi-threading is: " + str(end_time - start_time))
